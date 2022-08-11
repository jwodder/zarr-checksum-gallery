use super::util::{async_listdir, DirEntry};
use crate::checksum::{compile_checksum, nodes::FileChecksumNode};
use crate::errors::ChecksumError;
use log::{trace, warn};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::{Mutex, Notify};

// We need to use Tokio's mutex et alii so that this walker can function in a
// single-threaded runtime.
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
    async fn push(&self, item: T) {
        let mut data = self.data.lock().await;
        if !data.shutdown {
            data.queue.push(item);
            data.jobs += 1;
            trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_one();
        }
    }
    */

    async fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
        let mut data = self.data.lock().await;
        if !data.shutdown {
            let prelen = data.queue.len();
            data.queue.extend(iter);
            data.jobs += data.queue.len() - prelen;
            trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_waiters();
        }
    }

    async fn shutdown(&self) {
        let mut data = self.data.lock().await;
        if !data.shutdown {
            trace!("Shutting down stack");
            data.jobs -= data.queue.len();
            data.queue.clear();
            data.shutdown = true;
            self.cond.notify_waiters();
        }
    }

    async fn is_shutdown(&self) -> bool {
        self.data.lock().await.shutdown
    }

    async fn pop(&self) -> Option<T> {
        loop {
            trace!("Looping through pop()");
            {
                let mut data = self.data.lock().await;
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

    async fn task_done(&self) {
        let mut data = self.data.lock().await;
        data.jobs -= 1;
        trace!("Job count decremented to {}", data.jobs);
        if data.jobs == 0 {
            self.cond.notify_waiters();
        }
    }
}

pub async fn fastasync_checksum<P: AsRef<Path>>(
    dirpath: P,
    workers: usize,
) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref();
    let stack = Arc::new(AsyncJobStack::new([DirEntry {
        path: dirpath.to_path_buf(),
        is_dir: true,
    }]));
    let (sender, mut receiver) = channel(64);
    for i in 0..workers {
        let basepath = dirpath.to_path_buf();
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        tokio::spawn(async move {
            trace!("[{i}] Starting worker");
            while let Some(entry) = stack.pop().await {
                trace!("[{i}] Popped {:?} from stack", entry);
                let output = if entry.is_dir {
                    match async_listdir(&entry.path).await {
                        Ok(entries) => {
                            stack
                                .extend(
                                    entries
                                        .into_iter()
                                        .inspect(|n| trace!("[{i}] Pushing {n:?} onto stack")),
                                )
                                .await;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    Some(FileChecksumNode::async_for_file(&entry.path, &basepath).await)
                };
                stack.task_done().await;
                if let Some(v) = output {
                    // If we've shut down, don't send anything except Errs
                    if v.is_err() || !stack.is_shutdown().await {
                        if v.is_err() {
                            stack.shutdown().await;
                        }
                        trace!("[{i}] Sending {v:?} to output");
                        match sender.send(v).await {
                            Ok(_) => (),
                            Err(_) => {
                                warn!("[{i}] Failed to send; exiting");
                                stack.shutdown().await;
                                return;
                            }
                        }
                    }
                }
            }
            trace!("[{i}] Ending worker");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all workers run to completion
    let mut infos = Vec::new();
    let mut err = None;
    while let Some(v) = receiver.recv().await {
        match v {
            Ok(i) => {
                infos.push(i);
            }
            Err(e) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e.into()),
        None => Ok(compile_checksum(infos)?),
    }
}
