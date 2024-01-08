use super::jobstack::JobStack;
use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;
use crossbeam_utils::sync::WaitGroup;
use std::fmt;
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug)]
enum Job {
    Entry(ZarrEntry, Option<SharedDirectory>),
    CompletedDir(SharedDirectory),
}

impl Job {
    fn mkroot(zarr: &Zarr) -> Job {
        Job::Entry(ZarrEntry::Directory(zarr.root_dir()), None)
    }

    fn process(self, thread_no: usize) -> Output {
        match self {
            Job::Entry(ZarrEntry::Directory(zd), parent) => match zd.entries() {
                Ok(entries) => {
                    let arcdir = SharedDirectory::new(Directory::new(zd, entries.len(), parent));
                    if entries.is_empty() {
                        log::trace!(
                            "[{thread_no}] Directory {:?} is empty; pushing onto stack",
                            arcdir.relpath()
                        );
                        Output::ToPush(vec![Job::CompletedDir(arcdir)])
                    } else {
                        Output::ToPush(
                            entries
                                .into_iter()
                                .inspect(|n| log::trace!("[{thread_no}] Pushing {n:?} onto stack"))
                                .map(|n| Job::Entry(n, Some(arcdir.clone())))
                                .collect(),
                        )
                    }
                }
                Err(e) => Output::ToSend(Err(e)),
            },
            Job::Entry(ZarrEntry::File(zf), parent) => {
                let node = match zf.into_checksum() {
                    Ok(n) => n,
                    Err(e) => return Output::ToSend(Err(e)),
                };
                let parent = parent.expect("File without a parent directory");
                if parent.add(node.into()) {
                    log::trace!(
                        "[{thread_no}] Computed all checksums within directory {}; pushing onto stack",
                        parent.relpath()
                    );
                    Output::ToPush(vec![Job::CompletedDir(parent)])
                } else {
                    // It is possible for the CPU to suspend the thread here
                    // (and also on the `Nil` branch to the other call to
                    // `add()` further down), before `parent` is dropped, and
                    // then switch to another thread that finishes `parent` and
                    // pushes its CompletedDir on the stack, where it is
                    // immediately popped off the stack despite the `Arc` in
                    // `parent` still having an outstanding "extra" strong
                    // reference — hence the need for waiting for the strong
                    // count to reach 1 with a WaitGroup.
                    Output::Nil
                }
            }
            Job::CompletedDir(arcdir) => {
                let dir = arcdir.unwrap();
                let parent = dir.parent.as_ref().cloned();
                let node = dir.checksum();
                if let Some(parent) = parent {
                    if parent.add(node.into()) {
                        log::trace!(
                            "[{thread_no}] Computed all checksums within directory {}; pushing onto stack",
                            parent.relpath()
                        );
                        Output::ToPush(vec![Job::CompletedDir(parent)])
                    } else {
                        Output::Nil
                    }
                } else {
                    Output::ToSend(Ok(node.into_checksum()))
                }
            }
        }
    }
}

enum Output {
    ToPush(Vec<Job>),
    ToSend(Result<String, FSError>),
    Nil,
}

#[derive(Debug)]
struct Directory {
    dir: ZarrDirectory,
    data: Mutex<DirectoryData>,
    parent: Option<SharedDirectory>,
}

impl Directory {
    fn new(dir: ZarrDirectory, todo: usize, parent: Option<SharedDirectory>) -> Directory {
        log::trace!(
            "Directory {:?} has {} entries to checksum",
            dir.relpath(),
            todo
        );
        Directory {
            dir,
            data: Mutex::new(DirectoryData {
                nodes: Vec::new(),
                todo,
            }),
            parent,
        }
    }

    fn relpath(&self) -> &DirPath {
        self.dir.relpath()
    }

    fn checksum(self) -> DirChecksum {
        self.dir.get_checksum(
            self.data
                .into_inner()
                .expect("Mutex should not have been poisoned")
                .nodes,
        )
    }

    /// Returns `true` if all to-dos are now done after adding
    fn add(&self, node: EntryChecksum) -> bool {
        let mut data = self
            .data
            .lock()
            .expect("Mutex should not have been poisoned");
        data.nodes.push(node);
        data.todo = data.todo.saturating_sub(1);
        log::trace!(
            "Directory {:?} now has {} entries left to checksum",
            self.relpath(),
            data.todo
        );
        data.todo == 0
    }
}

struct DirectoryData {
    nodes: Vec<EntryChecksum>,
    todo: usize,
}

impl fmt::Debug for DirectoryData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectoryData")
            .field("nodes", &format_args!("<{} nodes>", self.nodes.len()))
            .field("todo", &self.todo)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct SharedDirectory {
    data: Arc<Directory>,
    wg: WaitGroup,
}

impl SharedDirectory {
    fn new(dir: Directory) -> SharedDirectory {
        SharedDirectory {
            data: Arc::new(dir),
            wg: WaitGroup::new(),
        }
    }

    fn unwrap(self) -> Directory {
        self.wg.wait();
        match Arc::try_unwrap(self.data) {
            Ok(dir) => dir,
            Err(arcdir) => {
                log::error!("Expected SharedDirectory to have only one strong reference, but there were {}!", Arc::strong_count(&arcdir));
                panic!("SharedDirectory should have only one strong reference");
            }
        }
    }
}

impl std::ops::Deref for SharedDirectory {
    type Target = Directory;

    fn deref(&self) -> &Directory {
        &self.data
    }
}

/// Traverse & checksum a Zarr directory using a stack of jobs distributed over
/// multiple threads.  The checksum for each intermediate directory is computed
/// as a job as soon as possible.  Checksums for directory entries are passed
/// to parent jobs via shared memory implemented using `Arc` and `Mutex`.
///
/// The `threads` argument determines the number of worker threads to use.
pub fn collapsio_arc_checksum(zarr: &Zarr, threads: NonZeroUsize) -> Result<String, ChecksumError> {
    let stack = Arc::new(JobStack::new([Job::mkroot(zarr)]));
    let (sender, receiver) = channel();
    for thread_no in 0..threads.get() {
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            log::trace!("[{thread_no}] Starting thread");
            for entry in from_fn(|| stack.pop()) {
                log::trace!("[{thread_no}] Popped {entry:?} from stack");
                let out = entry.process(thread_no);
                stack.job_done();
                match out {
                    Output::ToPush(to_push) => stack.extend(to_push),
                    Output::ToSend(to_send) => {
                        // If we've shut down, don't send anything except Errs
                        if to_send.is_err() || !stack.is_shutdown() {
                            if to_send.is_err() {
                                stack.shutdown();
                            }
                            log::trace!("[{thread_no}] Sending {to_send:?} to output");
                            if sender.send(to_send).is_err() {
                                log::warn!("[{thread_no}] Failed to send; exiting");
                                stack.shutdown();
                                return;
                            }
                        }
                    }
                    Output::Nil => (),
                }
            }
            log::trace!("[{thread_no}] Ending thread");
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
        None => {
            if let Some(s) = chksum {
                Ok(s)
            } else {
                log::error!("Neither checksum nor errors were received!");
                panic!("Neither checksum nor errors were received!");
            }
        }
    }
}
