#![allow(dead_code)]
use std::sync::{Condvar, Mutex};

#[derive(Debug)]
pub(crate) struct JobStack<T> {
    data: Mutex<JobStackData<T>>,
    cond: Condvar,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

    pub(crate) fn handle_job<F, I, E>(&self, f: F) -> Result<bool, E>
    where
        F: FnOnce(T) -> Result<I, E>,
        I: IntoIterator<Item = T>,
    {
        let Some(value) = self.pop() else {
            return Ok(false);
        };
        match f(value) {
            Ok(iter) => {
                self.extend(iter);
                self.job_done();
                Ok(true)
            }
            Err(e) => {
                self.job_done();
                self.shutdown();
                Err(e)
            }
        }
    }

    pub(crate) fn handle_many_jobs<F, I, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(T) -> Result<I, E>,
        I: IntoIterator<Item = T>,
    {
        while let Some(value) = self.pop() {
            match f(value) {
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

    fn pop(&self) -> Option<T> {
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

    fn job_done(&self) {
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

    // We can't impl Extend, as that requires the receiver to be mut
    fn extend<I: IntoIterator<Item = T>>(&self, iter: I) {
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
}
