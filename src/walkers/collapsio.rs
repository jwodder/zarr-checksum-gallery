use super::jobstack::JobStack;
use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;
use log::{error, trace, warn};
use std::fmt;
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

type ArcDirectory = Arc<Mutex<Directory>>;

#[derive(Debug)]
enum Job {
    Entry(ZarrEntry, Option<ArcDirectory>),
    CompletedDir(ArcDirectory),
}

struct Directory {
    dir: ZarrDirectory,
    nodes: Vec<EntryChecksum>,
    todo: usize,
    parent: Option<ArcDirectory>,
}

impl Directory {
    fn new(dir: ZarrDirectory, todo: usize, parent: Option<ArcDirectory>) -> Directory {
        trace!(
            "Directory {:?} has {} entries to checksum",
            dir.relpath(),
            todo
        );
        Directory {
            dir,
            nodes: Vec::new(),
            todo,
            parent,
        }
    }

    fn relpath(&self) -> &DirPath {
        self.dir.relpath()
    }

    fn checksum(self) -> DirChecksum {
        self.dir.get_checksum(self.nodes)
    }

    fn add(&mut self, node: EntryChecksum) {
        self.nodes.push(node);
        self.todo = self.todo.saturating_sub(1);
        trace!(
            "Directory {:?} now has {} entries left to checksum",
            self.relpath(),
            self.todo
        );
    }
}

impl fmt::Debug for Directory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Directory")
            .field("dir", &self.dir)
            .field("nodes", &format_args!("<{} nodes>", self.nodes.len()))
            .field("todo", &self.todo)
            .field(
                "parent",
                &self.parent.as_ref().map(|_| format_args!("<..>")),
            )
            .finish()
    }
}

/// Traverse & checksum a Zarr directory using a stack of jobs distributed over
/// multiple threads.  The checksum for each intermediate directory is computed
/// as a job as soon as possible.
///
/// The `threads` argument determines the number of worker threads to use.
pub fn collapsio_checksum(zarr: Zarr, threads: NonZeroUsize) -> Result<String, ChecksumError> {
    let stack = Arc::new(JobStack::new([Job::Entry(
        ZarrEntry::Directory(zarr.root_dir()),
        None,
    )]));
    let (sender, receiver) = channel();
    for i in 0..threads.get() {
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            trace!("[{i}] Starting thread");
            for entry in from_fn(|| stack.pop()) {
                trace!("[{i}] Popped {entry:?} from stack");
                let (to_push, to_send) = process(i, entry);
                stack.job_done();
                if let Some(v) = to_send {
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
                if !to_push.is_empty() {
                    stack.extend(to_push);
                }
            }
            trace!("[{i}] Ending thread");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all threads run to completion
    let mut chksum = None;
    let mut err = None;
    for v in receiver {
        match v {
            Ok(s) => {
                chksum.get_or_insert(s);
            }
            Err(e) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e.into()),
        None => match chksum {
            Some(s) => Ok(s),
            None => {
                error!("Neither checksum nor errors were received!");
                panic!("Neither checksum nor errors were received!");
            }
        },
    }
}

fn process(i: usize, entry: Job) -> (Vec<Job>, Option<Result<String, FSError>>) {
    match entry {
        Job::Entry(ZarrEntry::Directory(zd), parent) => match zd.entries() {
            Ok(entries) => {
                let thisdirpath = zd.relpath().clone();
                let arcdir = Arc::new(Mutex::new(Directory::new(zd, entries.len(), parent)));
                if entries.is_empty() {
                    trace!("[{i}] Directory {thisdirpath:?} is empty; pushing onto stack");
                    (vec![Job::CompletedDir(arcdir)], None)
                } else {
                    (
                        entries
                            .into_iter()
                            .inspect(|n| trace!("[{i}] Pushing {n:?} onto stack"))
                            .map(|n| Job::Entry(n, Some(Arc::clone(&arcdir))))
                            .collect(),
                        None,
                    )
                }
            }
            Err(e) => (Vec::new(), Some(Err(e))),
        },
        Job::Entry(ZarrEntry::File(zf), parent) => {
            let node = match zf.into_checksum() {
                Ok(n) => n,
                Err(e) => return (Vec::new(), Some(Err(e))),
            };
            let parent = parent.as_ref().expect("File without a parent directory");
            {
                let mut p = parent.lock().unwrap();
                p.add(node.into());
                if p.todo == 0 {
                    trace!(
                        "[{i}] Computed all checksums within directory {}; pushing onto stack",
                        p.relpath()
                    );
                    (vec![Job::CompletedDir(Arc::clone(parent))], None)
                } else {
                    (Vec::new(), None)
                }
            }
        }
        Job::CompletedDir(arcdir) => {
            let dir = match Arc::try_unwrap(arcdir) {
                Ok(dir) => dir.into_inner().unwrap(),
                Err(a) => {
                    error!("Expected CompletedDir to have only one strong reference, but there were {}!", Arc::strong_count(&a));
                    panic!("CompletedDir should have only one strong reference");
                }
            };
            let parent = dir.parent.as_ref().map(Arc::clone);
            let node = dir.checksum();
            if let Some(parent) = parent {
                let mut p = parent.lock().unwrap();
                p.add(node.into());
                if p.todo == 0 {
                    trace!(
                        "[{i}] Computed all checksums within directory {}; pushing onto stack",
                        p.relpath()
                    );
                    (vec![Job::CompletedDir(Arc::clone(&parent))], None)
                } else {
                    (Vec::new(), None)
                }
            } else {
                (Vec::new(), Some(Ok(node.into_checksum())))
            }
        }
    }
}
