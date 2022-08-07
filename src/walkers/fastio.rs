use super::util::listdir;
use crate::checksum::{try_compile_checksum, FileInfo};
use crate::error::ZarrError;
use log::{debug, info, warn};
use std::iter::once;
use std::ops::Deref;
use std::path::{Path, PathBuf};
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
        debug!("Task count incremented to {}", data.tasks);
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
            debug!("Looping through JobStackIterator::next");
            if data.tasks == 0 {
                debug!("[JobStackIterator::next] tasks == 0; returning None");
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
                    debug!("[JobStackIterator::next] queue is empty; waiting");
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
        debug!("Task count decremented to {}", data.tasks);
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
            info!("[{i}] Starting thread");
            for path in stack.iter() {
                info!("[{i}] Popped {} from stack", path.display());
                let output = match helper(i, &path, &basepath, &stack) {
                    Ok(infos) => infos.into_iter().map(Ok).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                };
                for v in output {
                    info!("[{i}] Sending {v:?} to output");
                    match sender.send(v) {
                        Ok(_) => (),
                        Err(_) => {
                            warn!("[{i}] Failed to send; exiting");
                            return;
                        }
                    }
                }
            }
            info!("[{i}] Ending thread");
        });
    }
    drop(sender);
    try_compile_checksum(receiver)
}

fn helper(
    i: usize,
    p: &PathBuf,
    basepath: &PathBuf,
    stack: &JobStack<PathBuf>,
) -> Result<Vec<FileInfo>, ZarrError> {
    let (files, dirs): (Vec<_>, Vec<_>) = listdir(p)?.into_iter().partition(|e| !e.is_dir());
    for d in dirs {
        info!("[{i}] Pushing {} onto stack", d.path().display());
        stack.push(d.path());
    }
    files
        .into_iter()
        .map(|f| FileInfo::for_file(&f.path(), basepath))
        .collect()
}
