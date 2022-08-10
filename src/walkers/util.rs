use crate::errors::FSError;
use log::trace;
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use tokio::fs as afs;
use tokio_stream::wrappers::ReadDirStream;
use tokio_stream::StreamExt;

#[derive(Debug)]
pub(crate) struct DirEntry {
    pub(crate) path: PathBuf,
    pub(crate) is_dir: bool,
}

pub(crate) fn listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, FSError> {
    let mut entries = Vec::new();
    for p in fs::read_dir(&dirpath).map_err(|e| FSError::readdir_error(&dirpath, e))? {
        let p = p.map_err(|e| FSError::readdir_error(&dirpath, e))?;
        let path = p.path();
        let ftype = p.file_type().map_err(|e| FSError::stat_error(&path, e))?;
        let is_dir = ftype.is_dir()
            || (ftype.is_symlink()
                && fs::metadata(&path)
                    .map_err(|e| FSError::stat_error(&path, e))?
                    .is_dir());
        entries.push(DirEntry { path, is_dir });
    }
    Ok(entries)
}

pub(crate) async fn async_listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, FSError> {
    let mut entries = Vec::new();
    let handle = afs::read_dir(&dirpath)
        .await
        .map_err(|e| FSError::readdir_error(&dirpath, e))?;
    let mut stream = ReadDirStream::new(handle);
    while let Some(p) = stream.next().await {
        let p = p.map_err(|e| FSError::readdir_error(&dirpath, e))?;
        let path = p.path();
        let ftype = p
            .file_type()
            .await
            .map_err(|e| FSError::stat_error(&path, e))?;
        let is_dir = ftype.is_dir()
            || (ftype.is_symlink()
                && afs::metadata(&path)
                    .await
                    .map_err(|e| FSError::stat_error(&path, e))?
                    .is_dir());
        entries.push(DirEntry { path, is_dir });
    }
    Ok(entries)
}

pub(crate) struct JobStack<T> {
    data: Mutex<JobStackData<T>>,
    cond: Condvar,
}

struct JobStackData<T> {
    queue: Vec<T>,
    jobs: usize,
    shutdown: bool,
}

impl<T> JobStack<T> {
    pub(crate) fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
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
    pub(crate) fn push(&self, item: T) {
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
    pub(crate) fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            let prelen = data.queue.len();
            data.queue.extend(iter);
            data.jobs += data.queue.len() - prelen;
            trace!("Job count incremented to {}", data.jobs);
            self.cond.notify_all();
        }
    }

    pub(crate) fn shutdown(&self) {
        let mut data = self.data.lock().unwrap();
        if !data.shutdown {
            trace!("Shutting down stack");
            data.jobs -= data.queue.len();
            data.queue.clear();
            data.shutdown = true;
            self.cond.notify_all();
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.data.lock().unwrap().shutdown
    }

    pub(crate) fn iter(&self) -> JobStackIterator<'_, T> {
        JobStackIterator { stack: self }
    }
}

pub(crate) struct JobStackIterator<'a, T> {
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

pub(crate) struct JobStackItem<'a, T> {
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
