use super::util::{listdir, DirEntry};
use crate::checksum::{try_compile_checksum, FileInfo};
use crate::error::ZarrError;
use log::{trace, warn};
use std::iter::once;
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
}

impl<T> JobStack<T> {
    fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
        let queue: Vec<T> = items.into_iter().collect();
        let tasks = queue.len();
        JobStack {
            data: Mutex::new(JobStackData { queue, tasks }),
            cond: Condvar::new(),
        }
    }

    fn push(&self, item: T) {
        let mut data = self.data.lock().unwrap();
        data.queue.push(item);
        data.tasks += 1;
        trace!("Task count incremented to {}", data.tasks);
        self.cond.notify_one();
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
            if data.tasks == 0 {
                trace!("[JobStackIterator::next] tasks == 0; returning None");
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

pub fn fastio_checksum<P: AsRef<Path>>(dirpath: P, threads: usize) -> Result<String, ZarrError> {
    let dirpath = dirpath.as_ref();
    let stack = Arc::new(JobStack::new(once(dirpath.to_path_buf())));
    let (sender, receiver) = channel();
    for i in 0..threads {
        let basepath = dirpath.to_path_buf();
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            trace!("[{i}] Starting thread");
            for path in stack.iter() {
                trace!("[{i}] Popped {} from stack", path.display());
                let output = match listdir(&*path) {
                    Ok(entries) => {
                        let (dirs, files): (Vec<_>, Vec<_>) =
                            entries.into_iter().partition(|e| e.is_dir);
                        for DirEntry { path: d, .. } in dirs {
                            trace!("[{i}] Pushing {} onto stack", d.display());
                            stack.push(d);
                        }
                        files
                            .into_iter()
                            .map(|DirEntry { path, .. }| FileInfo::for_file(&path, &basepath))
                            .collect()
                    }
                    Err(e) => vec![Err(e)],
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
    try_compile_checksum(receiver)
}
