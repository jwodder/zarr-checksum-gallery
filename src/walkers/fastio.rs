use super::util::{listdir, DirEntry};
use crate::checksum::{compile_checksum, FileInfo};
use crate::error::WalkError;
use log::{trace, warn};
use std::ops::Deref;
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

struct JobStack<T> {
    data: Mutex<JobStackData<T>>,
    cond: Condvar,
}

struct JobStackData<T> {
    queue: Vec<T>,
    jobs: usize,
    shutdown: bool,
}

impl<T> JobStack<T> {
    fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
        let queue: Vec<T> = items.into_iter().collect();
        let jobs = queue.len();
        JobStack {
            data: Mutex::new(JobStackData {
                queue,
                jobs,
                shutdown: false,
            }),
            cond: Condvar::new(),
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

    // We can't impl Extend, as that requires the receiver to be mut
    fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            let prelen = data.queue.len();
            data.queue.extend(iter);
            data.jobs += data.queue.len() - prelen;
            trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_all();
        }
    }

    fn shutdown(&self) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            trace!("Shutting down stack");
            data.jobs -= data.queue.len();
            data.queue.clear();
            data.shutdown = true;
            self.cond.notify_all();
        }
    }

    fn is_shutdown(&self) -> bool {
        self.data.lock().unwrap().shutdown
    }

    fn iter(&self) -> JobStackIterator<'_, T> {
        JobStackIterator { stack: self }
    }
}

struct JobStackIterator<'a, T> {
    stack: &'a JobStack<T>,
}

impl<'a, T> Iterator for JobStackIterator<'a, T> {
    type Item = JobStackItem<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut data = self.stack.data.lock().unwrap();
        loop {
            trace!("Looping through JobStackIterator::next");
            if data.jobs == 0 || data.shutdown {
                trace!("[JobStackIterator::next] no jobs; returning None");
                return None;
            }
            match data.queue.pop() {
                Some(value) => {
                    return Some(JobStackItem {
                        value,
                        stack: self.stack,
                    })
                }
                None => {
                    trace!("[JobStackIterator::next] queue is empty; waiting");
                    data = self.stack.cond.wait(data).unwrap();
                }
            }
        }
    }
}

struct JobStackItem<'a, T> {
    value: T,
    stack: &'a JobStack<T>,
}

impl<T> Deref for JobStackItem<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> Drop for JobStackItem<'_, T> {
    fn drop(&mut self) {
        let mut data = self.stack.data.lock().unwrap();
        data.jobs -= 1;
        trace!("Job count decremented to {}", data.jobs);
        if data.jobs == 0 {
            self.stack.cond.notify_all();
        }
    }
}

pub fn fastio_checksum<P: AsRef<Path>>(dirpath: P, threads: usize) -> Result<String, WalkError> {
    let dirpath = dirpath.as_ref();
    let stack = Arc::new(JobStack::new([DirEntry {
        path: dirpath.to_path_buf(),
        name: String::new(),
        is_dir: true,
    }]));
    let (sender, receiver) = channel();
    for i in 0..threads {
        let basepath = dirpath.to_path_buf();
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            trace!("[{i}] Starting thread");
            for entry in stack.iter() {
                trace!("[{i}] Popped {:?} from stack", *entry);
                let output = if entry.is_dir {
                    match listdir(&entry.path) {
                        Ok(entries) => {
                            stack.extend(
                                entries
                                    .into_iter()
                                    .inspect(|n| trace!("[{i}] Pushing {n:?} onto stack")),
                            );
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    Some(FileInfo::for_file(&entry.path, &basepath))
                };
                if let Some(v) = output {
                    // If we've shut down, don't send anything except Errs
                    if v.is_err() || !stack.is_shutdown() {
                        if v.is_err() {
                            stack.shutdown();
                        }
                        trace!("[{i}] Sending {v:?} to output");
                        match sender.send(v) {
                            Ok(_) => (),
                            Err(_) => {
                                warn!("[{i}] Failed to send; exiting");
                                stack.shutdown();
                                return;
                            }
                        }
                    }
                }
            }
            trace!("[{i}] Ending thread");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all threads run to completion
    let mut infos = Vec::new();
    let mut err = None;
    for v in receiver {
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
        Some(e) => Err(e),
        None => Ok(compile_checksum(infos)),
    }
}
