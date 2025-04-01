use super::util::Output;
use crate::checksum::ChecksumTree;
use crate::errors::ChecksumError;
use crate::zarr::*;
use std::future::Future;
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

impl<T: Send> AsyncJobStack<T> {
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

    fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        if !data.shutdown {
            let prelen = data.queue.len();
            data.queue.extend(iter);
            data.jobs += data.queue.len() - prelen;
            log::trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_waiters();
        }
    }

    fn shutdown(&self) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        if !data.shutdown {
            log::trace!("Shutting down stack");
            data.jobs -= data.queue.len();
            data.queue.clear();
            data.shutdown = true;
            self.cond.notify_waiters();
        }
    }

    fn is_shutdown(&self) -> bool {
        self.data
            .lock()
            .expect("Mutex should not have been poisoned")
            .shutdown
    }

    async fn handle_many_jobs<F, Fut, I, E>(&self, f: F) -> Result<(), E>
    where
        F: Fn(T) -> Fut + Send,
        Fut: Future<Output = Result<I, E>> + Send,
        I: IntoIterator<Item = T> + Send,
    {
        while let Some(value) = self.pop().await {
            match f(value).await {
                Ok(iter) => {
                    self.extend(iter);
                    self.job_done();
                }
                Err(e) => {
                    self.job_done();
                    self.shutdown();
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn pop(&self) -> Option<T> {
        loop {
            log::trace!("Looping through pop()");
            {
                let mut data = self
                    .data
                    .lock()
                    .expect("Mutex should not have been poisoned");
                if data.jobs == 0 || data.shutdown {
                    log::trace!("[pop] no jobs; returning None");
                    return None;
                }
                if let Some(v) = data.queue.pop() {
                    return Some(v);
                }
            }
            log::trace!("[pop] queue is empty; waiting");
            self.cond.notified().await;
        }
    }

    fn job_done(&self) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        data.jobs -= 1;
        log::trace!("Job count decremented to {}", data.jobs);
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
        tokio::spawn({
            let stack = Arc::clone(&stack);
            let sender = sender.clone();
            async move {
                log::trace!("[{task_no}] Starting worker");
                let _ = stack
                    .handle_many_jobs(|entry| {
                        let stack2 = stack.clone();
                        let sender = sender.clone();
                        async move {
                            log::trace!("[{task_no}] Popped {entry:?} from stack");
                            let output = match entry {
                                ZarrEntry::Directory(zd) => match zd.async_entries().await {
                                    Ok(entries) => {
                                        for n in &entries {
                                            log::trace!("[{task_no}] Pushing {n:?} onto stack");
                                        }
                                        Output::ToPush(entries)
                                    }
                                    Err(e) => Output::ToSend(Err(e)),
                                },
                                ZarrEntry::File(zf) => {
                                    Output::ToSend(zf.async_into_checksum().await)
                                }
                            };
                            match output {
                                Output::ToPush(to_push) => Ok(to_push),
                                Output::ToSend(to_send) => {
                                    // If we've shut down, don't send anything except Errs
                                    if to_send.is_err() || !stack2.is_shutdown() {
                                        if to_send.is_err() {
                                            stack2.shutdown();
                                        }
                                        log::trace!("[{task_no}] Sending {to_send:?} to output");
                                        if let Err(e) = sender.send(to_send).await {
                                            log::warn!("[{task_no}] Failed to send; exiting");
                                            return Err(e);
                                        }
                                    }
                                    Ok(Vec::new())
                                }
                                Output::Nil => Ok(Vec::new()),
                            }
                        }
                    })
                    .await;
                log::trace!("[{task_no}] Ending worker");
            }
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
