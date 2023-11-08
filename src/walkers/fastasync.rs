use crate::checksum::ChecksumTree;
use crate::errors::ChecksumError;
use crate::zarr::*;
use log::{trace, warn};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::channel;
use tokio::sync::Notify;

// We need to use Tokio's Notify instead of the standard Condvar so that this
// walker can function in a single-threaded runtime.
struct AsyncJobStack<T> {
    data: Mutex<AsyncJobStackData<T>>,
    cond: Notify,
}

struct AsyncJobStackData<T> {
    queue: Vec<T>,
    jobs: usize,
    shutdown: bool,
}

impl<T> AsyncJobStack<T> {
    fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
        let queue: Vec<T> = items.into_iter().collect();
        let jobs = queue.len();
        AsyncJobStack {
            data: Mutex::new(AsyncJobStackData {
                queue,
                jobs,
                shutdown: false,
            }),
            cond: Notify::new(),
        }
    }

    /*
    fn push(&self, item: T) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            data.queue.push(item);
            data.jobs += 1;
            trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_one();
        }
    }
    */

    fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            let prelen = data.queue.len();
            data.queue.extend(iter);
            data.jobs += data.queue.len() - prelen;
            trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_waiters();
        }
    }

    fn shutdown(&self) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            trace!("Shutting down stack");
            data.jobs -= data.queue.len();
            data.queue.clear();
            data.shutdown = true;
            self.cond.notify_waiters();
        }
    }

    fn is_shutdown(&self) -> bool {
        self.data.lock().unwrap().shutdown
    }

    async fn pop(&self) -> Option<T> {
        loop {
            trace!("Looping through pop()");
            {
                let mut data = self.data.lock().unwrap();
                if data.jobs == 0 || data.shutdown {
                    trace!("[pop] no jobs; returning None");
                    return None;
                }
                if let Some(v) = data.queue.pop() {
                    return Some(v);
                }
            }
            trace!("[pop] queue is empty; waiting");
            self.cond.notified().await;
        }
    }

    fn job_done(&self) {
        let mut data = self.data.lock().unwrap();
        data.jobs -= 1;
        trace!("Job count decremented to {}", data.jobs);
        if data.jobs == 0 {
            self.cond.notify_waiters();
        }
    }
}

/// Asynchronously traverse & checksum a Zarr directory using a stack of jobs
/// distributed over multiple worker tasks
///
/// The `workers` argument determines the number of worker tasks to use.
///
/// This builds an in-memory tree of all file checksums for computing the final
/// Zarr checksum.
pub async fn fastasync_checksum(
    zarr: &Zarr,
    workers: NonZeroUsize,
) -> Result<String, ChecksumError> {
    let stack = Arc::new(AsyncJobStack::new([ZarrEntry::Directory(zarr.root_dir())]));
    let (sender, mut receiver) = channel(64);
    for task_no in 0..workers.get() {
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        tokio::spawn(async move {
            trace!("[{task_no}] Starting worker");
            while let Some(entry) = stack.pop().await {
                trace!("[{task_no}] Popped {:?} from stack", entry);
                let output = match entry {
                    ZarrEntry::Directory(zd) => match zd.async_entries().await {
                        Ok(entries) => {
                            stack.extend(
                                entries
                                    .into_iter()
                                    .inspect(|n| trace!("[{task_no}] Pushing {n:?} onto stack")),
                            );
                            None
                        }
                        Err(e) => Some(Err(e)),
                    },
                    ZarrEntry::File(zf) => Some(zf.async_into_checksum().await),
                };
                stack.job_done();
                if let Some(v) = output {
                    // If we've shut down, don't send anything except Errs
                    if v.is_err() || !stack.is_shutdown() {
                        if v.is_err() {
                            stack.shutdown();
                        }
                        trace!("[{task_no}] Sending {v:?} to output");
                        match sender.send(v).await {
                            Ok(_) => (),
                            Err(_) => {
                                warn!("[{task_no}] Failed to send; exiting");
                                stack.shutdown();
                                return;
                            }
                        }
                    }
                }
            }
            trace!("[{task_no}] Ending worker");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all workers run to completion
    let mut tree = Ok(ChecksumTree::new());
    let mut err = None;
    while let Some(v) = receiver.recv().await {
        match v {
            Ok(i) => {
                tree = tree.and_then(|mut t| {
                    t.add_file(i)?;
                    Ok(t)
                });
            }
            Err(e) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e.into()),
        None => tree.map(ChecksumTree::into_checksum),
    }
}
