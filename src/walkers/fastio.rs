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
    tasks: usize,
    shutdown: bool,
}

impl<T> JobStack<T> {
    fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
        let queue: Vec<T> = items.into_iter().collect();
        let tasks = queue.len();
        JobStack {
            data: Mutex::new(JobStackData {
                queue,
                tasks,
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
            data.tasks += 1;
            trace!("Task count incremented to {}", data.tasks);
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
            data.tasks += data.queue.len() - prelen;
            trace!("Task count incremented to {}", data.tasks);
            self.cond.notify_one();
        }
    }

    fn shutdown(&self) {
        let mut data = self.data.lock().unwrap();
        trace!("Shutting down stack");
        data.tasks -= data.queue.len();
        data.queue.clear();
        data.shutdown = true;
        self.cond.notify_all();
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
            if data.tasks == 0 || data.shutdown {
                trace!("[JobStackIterator::next] no tasks; returning None");
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
        data.tasks -= 1;
        trace!("Task count decremented to {}", data.tasks);
        if data.tasks == 0 {
            self.stack.cond.notify_all();
        }
    }
}

pub fn fastio_checksum<P: AsRef<Path>>(dirpath: P, threads: usize) -> Result<String, WalkError> {
    let dirpath = dirpath.as_ref();
    let stack = Arc::new(JobStack::new([dirpath.to_path_buf()]));
    let (sender, receiver) = channel();
    for i in 0..threads {
        let basepath = dirpath.to_path_buf();
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            trace!("[{i}] Starting thread");
            for path in stack.iter() {
                trace!("[{i}] Popped {} from stack", path.display());
                let result = match listdir(&*path) {
                    Ok(entries) => {
                        let (dirs, files): (Vec<_>, Vec<_>) =
                            entries.into_iter().partition(|e| e.is_dir);
                        stack.extend(
                            dirs.into_iter()
                                .map(|d| d.path)
                                .inspect(|d| trace!("[{i}] Pushing {} onto stack", d.display())),
                        );
                        files
                            .into_iter()
                            .map(|DirEntry { path, .. }| FileInfo::for_file(path, &basepath))
                            .collect::<Result<Vec<FileInfo>, WalkError>>()
                    }
                    Err(e) => Err(e),
                };
                let output = match result {
                    Ok(infos) => {
                        // If we've shut down, don't send anything except Errs
                        if stack.is_shutdown() {
                            Vec::new()
                        } else {
                            infos.into_iter().map(Ok).collect()
                        }
                    }
                    Err(e) => {
                        stack.shutdown();
                        vec![Err(e)]
                    }
                };
                for v in output {
                    trace!("[{i}] Sending {v:?} to output");
                    match sender.send(v) {
                        Ok(_) => (),
                        Err(_) => {
                            warn!("[{i}] Failed to send; exiting");
                            return;
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
