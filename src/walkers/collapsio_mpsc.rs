use super::jobstack::JobStack;
use super::util::Output;
use crate::checksum::nodes::*;
use crate::errors::ChecksumError;
use crate::zarr::*;
use std::num::NonZeroUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
enum Job {
    Entry(ZarrEntry, Option<Sender<EntryChecksum>>),
    CompletedDir {
        dir: ZarrDirectory,
        recv: Receiver<EntryChecksum>,
        parent: Option<Sender<EntryChecksum>>,
    },
}

impl Job {
    fn mkroot(zarr: &Zarr) -> Job {
        Job::Entry(ZarrEntry::Directory(zarr.root_dir()), None)
    }

    fn process(self, thread_no: usize) -> Output<Job, String> {
        match self {
            Job::Entry(ZarrEntry::Directory(dir), parent) => match dir.entries() {
                Ok(entries) => {
                    log::trace!(
                        "[{thread_no}] Directory {:?} has {} entries to checksum",
                        dir.relpath(),
                        entries.len(),
                    );
                    let (sender, recv) = channel();
                    let mut to_push = vec![Job::CompletedDir { dir, recv, parent }];
                    to_push.extend(
                        entries
                            .into_iter()
                            .inspect(|n| log::trace!("[{thread_no}] Pushing {n:?} onto stack"))
                            .map(|n| Job::Entry(n, Some(sender.clone()))),
                    );
                    Output::ToPush(to_push)
                }
                Err(e) => Output::ToSend(Err(e)),
            },
            Job::Entry(ZarrEntry::File(zf), parent) => {
                let node = match zf.into_checksum() {
                    Ok(n) => n,
                    Err(e) => return Output::ToSend(Err(e)),
                };
                // If the send() fails, it must be because the job stack was
                // shut down, dropping the receiver, so do nothing.
                let _ = parent
                    .expect("File without a parent directory")
                    .send(node.into());
                Output::Nil
            }
            Job::CompletedDir { dir, recv, parent } => {
                let node = dir.get_checksum(recv);
                if let Some(parent) = parent {
                    // If the send() fails, it must be because the job stack
                    // was shut down, dropping the receiver, so do nothing.
                    let _ = parent.send(node.into());
                    Output::Nil
                } else {
                    Output::ToSend(Ok(node.into_checksum()))
                }
            }
        }
    }
}

/// Traverse & checksum a Zarr directory using a stack of jobs distributed over
/// multiple threads.  The checksum for each intermediate directory is computed
/// as a job as soon as possible.  Checksums for directory entries are passed
/// to parent jobs via MPSC channels.
///
/// The `threads` argument determines the number of worker threads to use.
pub fn collapsio_mpsc_checksum(
    zarr: &Zarr,
    threads: NonZeroUsize,
) -> Result<String, ChecksumError> {
    let stack = Arc::new(JobStack::new([Job::mkroot(zarr)]));
    let (sender, receiver) = channel();
    for thread_no in 0..threads.get() {
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            log::trace!("[{thread_no}] Starting thread");
            let _ = stack.handle_many_jobs(|entry| {
                log::trace!("[{thread_no}] Popped {entry:?} from stack");
                let out = entry.process(thread_no);
                match out {
                    Output::ToPush(to_push) => Ok(to_push),
                    Output::ToSend(to_send) => {
                        // If we've shut down, don't send anything except Errs
                        if to_send.is_err() || !stack.is_shutdown() {
                            if to_send.is_err() {
                                stack.shutdown();
                            }
                            log::trace!("[{thread_no}] Sending {to_send:?} to output");
                            if let Err(e) = sender.send(to_send) {
                                log::warn!("[{thread_no}] Failed to send; exiting");
                                return Err(e);
                            }
                        }
                        Ok(Vec::new())
                    }
                    Output::Nil => Ok(Vec::new()),
                }
            });
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
