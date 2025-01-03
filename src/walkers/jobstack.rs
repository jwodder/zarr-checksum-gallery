#![allow(dead_code)]
use std::ops::Deref;
use std::sync::{Condvar, Mutex};

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

    pub(crate) fn push(&self, item: T) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        if !data.shutdown {
            data.queue.push(item);
            data.jobs += 1;
            log::trace!("[JobStack] Job count incremented to {}", data.jobs);
            self.cond.notify_one();
        }
    }

    // We can't impl Extend, as that requires the receiver to be mut
    pub(crate) fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        if !data.shutdown {
            let prelen = data.queue.len();
            data.queue.extend(iter);
            data.jobs += data.queue.len() - prelen;
            log::trace!("[JobStack] Job count incremented to {}", data.jobs);
            self.cond.notify_all();
        }
    }

    pub(crate) fn shutdown(&self) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        if !data.shutdown {
            log::trace!("[JobStack] Shutting down stack");
            data.jobs -= data.queue.len();
            data.queue.clear();
            data.shutdown = true;
            self.cond.notify_all();
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.data
            .lock()
            .expect("Mutex should not have been poisoned")
            .shutdown
    }

    pub(crate) fn pop(&self) -> Option<T> {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        loop {
            log::trace!("[JobStack] Looping through stack");
            if data.jobs == 0 || data.shutdown {
                log::trace!("[JobStack] no jobs; returning None");
                return None;
            }
            if let value @ Some(_) = data.queue.pop() {
                return value;
            } else {
                log::trace!("[JobStack] queue is empty; waiting");
                data = self
                    .cond
                    .wait(data)
                    .expect("Mutex should not have been poisoned");
            }
        }
    }

    pub(crate) fn job_done(&self) {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        data.jobs -= 1;
        log::trace!("[JobStack] Job count decremented to {}", data.jobs);
        if data.jobs == 0 {
            self.cond.notify_all();
        }
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
        self.stack.pop().map(|value| JobStackItem {
            value,
            stack: self.stack,
        })
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
        self.stack.job_done();
    }
}
