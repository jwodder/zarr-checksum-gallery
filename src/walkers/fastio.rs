use super::jobstack::JobStack;
use crate::checksum::ChecksumTree;
use crate::errors::ChecksumError;
use crate::zarr::*;
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;

/// Traverse & checksum a Zarr directory using a stack of jobs distributed over
/// multiple threads
///
/// The `threads` argument determines the number of worker threads to use.
///
/// This builds an in-memory tree of all file checksums for computing the final
/// Zarr checksum.
pub fn fastio_checksum(zarr: &Zarr, threads: NonZeroUsize) -> Result<String, ChecksumError> {
    Ok(fastio_checksum_tree(zarr, threads)?.into_checksum())
}

pub fn fastio_checksum_tree(
    zarr: &Zarr,
    threads: NonZeroUsize,
) -> Result<ChecksumTree, ChecksumError> {
    let stack = Arc::new(JobStack::new([ZarrEntry::Directory(zarr.root_dir())]));
    let (sender, receiver) = channel();
    for thread_no in 0..threads.get() {
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            log::trace!("[{thread_no}] Starting thread");
            for entry in from_fn(|| stack.pop()) {
                log::trace!("[{thread_no}] Popped {:?} from stack", entry);
                let output = match entry {
                    ZarrEntry::Directory(zd) => match zd.entries() {
                        Ok(entries) => {
                            stack.extend(entries.into_iter().inspect(|n| {
                                log::trace!("[{thread_no}] Pushing {n:?} onto stack")
                            }));
                            None
                        }
                        Err(e) => Some(Err(e)),
                    },
                    ZarrEntry::File(zf) => Some(zf.into_checksum()),
                };
                stack.job_done();
                if let Some(v) = output {
                    // If we've shut down, don't send anything except Errs
                    if v.is_err() || !stack.is_shutdown() {
                        if v.is_err() {
                            stack.shutdown();
                        }
                        log::trace!("[{thread_no}] Sending {v:?} to output");
                        match sender.send(v) {
                            Ok(_) => (),
                            Err(_) => {
                                log::warn!("[{thread_no}] Failed to send; exiting");
                                stack.shutdown();
                                return;
                            }
                        }
                    }
                }
            }
            log::trace!("[{thread_no}] Ending thread");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all threads run to completion
    let mut tree = Ok(ChecksumTree::new());
    let mut err = None;
    for v in receiver {
        match v {
            Ok(i) => {
                tree = tree.and_then(|mut t| {
                    t.add_file(i)?;
                    Ok(t)
                });
            }
            Err(e) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e.into()),
        None => tree,
    }
}
