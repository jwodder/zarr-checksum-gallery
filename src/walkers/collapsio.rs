use super::jobstack::JobStack;
use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;
use either::{Either, Left, Right};
use log::{error, trace, warn};
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
enum Job {
    Entry(ZarrEntry, Option<Sender<EntryChecksum>>),
    CompletedDir(Directory),
}

#[derive(Debug)]
struct Directory {
    dir: ZarrDirectory,
    recv: Receiver<EntryChecksum>,
    parent: Option<Sender<EntryChecksum>>,
}

/// Traverse & checksum a Zarr directory using a stack of jobs distributed over
/// multiple threads.  The checksum for each intermediate directory is computed
/// as a job as soon as possible.
///
/// The `threads` argument determines the number of worker threads to use.
pub fn collapsio_checksum(zarr: &Zarr, threads: NonZeroUsize) -> Result<String, ChecksumError> {
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
                let out = process(i, entry);
                stack.job_done();
                match out {
                    Left(to_push) => {
                        if !to_push.is_empty() {
                            stack.extend(to_push);
                        }
                    }
                    Right(to_send) => {
                        // If we've shut down, don't send anything except Errs
                        if to_send.is_err() || !stack.is_shutdown() {
                            if to_send.is_err() {
                                stack.shutdown();
                            }
                            trace!("[{i}] Sending {to_send:?} to output");
                            if sender.send(to_send).is_err() {
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

fn process(i: usize, entry: Job) -> Either<Vec<Job>, Result<String, FSError>> {
    match entry {
        Job::Entry(ZarrEntry::Directory(zd), parent) => match zd.entries() {
            Ok(entries) => {
                let thisdirpath = zd.relpath().clone();
                trace!(
                    "Directory {:?} has {} entries to checksum",
                    thisdirpath,
                    entries.len(),
                );
                let (dirsend, recv) = channel();
                let completed_dir = Directory {
                    dir: zd,
                    recv,
                    parent,
                };
                let mut to_push = vec![Job::CompletedDir(completed_dir)];
                if entries.is_empty() {
                    trace!("[{i}] Directory {thisdirpath:?} is empty; pushing onto stack");
                } else {
                    to_push.extend(
                        entries
                            .into_iter()
                            .inspect(|n| trace!("[{i}] Pushing {n:?} onto stack"))
                            .map(|n| Job::Entry(n, Some(dirsend.clone()))),
                    )
                }
                Left(to_push)
            }
            Err(e) => Right(Err(e)),
        },
        Job::Entry(ZarrEntry::File(zf), parent) => {
            let node = match zf.into_checksum() {
                Ok(n) => n,
                Err(e) => return Right(Err(e)),
            };
            parent
                .expect("File without a parent directory")
                .send(node.into())
                .expect("Failed to send checksum to parent node");
            Left(Vec::new())
        }
        Job::CompletedDir(dir) => {
            let node = dir.dir.get_checksum(dir.recv);
            if let Some(parent) = dir.parent {
                parent
                    .send(node.into())
                    .expect("Failed to send checksum to parent node");
                Left(Vec::new())
            } else {
                Right(Ok(node.into_checksum()))
            }
        }
    }
}
