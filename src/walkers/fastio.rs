use super::jobstack::JobStack;
use super::util::{listdir, DirEntry};
use crate::checksum::{compile_checksum, nodes::FileChecksumNode};
use crate::errors::ChecksumError;
use log::{trace, warn};
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;

/// Traverse & checksum a directory using a stack of jobs distributed over
/// multiple threads
///
/// The `threads` argument determines the number of worker threads to use.
///
/// This builds an in-memory tree of all file checksums for computing the final
/// Zarr checksum.
pub fn fastio_checksum<P: AsRef<Path>>(
    dirpath: P,
    threads: usize,
) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref();
    let stack = Arc::new(JobStack::new([DirEntry {
        path: dirpath.to_path_buf(),
        is_dir: true,
    }]));
    let (sender, receiver) = channel();
    for i in 0..threads {
        let basepath = dirpath.to_path_buf();
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        thread::spawn(move || {
            trace!("[{i}] Starting thread");
            for entry in stack.iter() {
                trace!("[{i}] Popped {:?} from stack", *entry);
                let output = if entry.is_dir {
                    match listdir(&entry.path) {
                        Ok(entries) => {
                            stack.extend(
                                entries
                                    .into_iter()
                                    .inspect(|n| trace!("[{i}] Pushing {n:?} onto stack")),
                            );
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    Some(FileChecksumNode::for_file(&entry.path, &basepath))
                };
                if let Some(v) = output {
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
            }
            trace!("[{i}] Ending thread");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all threads run to completion
    let mut infos = Vec::new();
    let mut err = None;
    for v in receiver {
        match v {
            Ok(i) => {
                infos.push(i);
            }
            Err(e) => {
                err.get_or_insert(e);
            }
        }
    }
    match err {
        Some(e) => Err(e.into()),
        None => Ok(compile_checksum(infos)?),
    }
}
