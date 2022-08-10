use super::util::{async_listdir, DirEntry, JobStack};
use crate::checksum::{compile_checksum, nodes::FileChecksumNode};
use crate::errors::ChecksumError;
use log::{trace, warn};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::channel;

pub async fn fastasync_checksum<P: AsRef<Path>>(
    dirpath: P,
    workers: usize,
) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref();
    let stack = Arc::new(JobStack::new([DirEntry {
        path: dirpath.to_path_buf(),
        is_dir: true,
    }]));
    let (sender, mut receiver) = channel(64);
    for i in 0..workers {
        let basepath = dirpath.to_path_buf();
        let stack = Arc::clone(&stack);
        let sender = sender.clone();
        tokio::spawn(async move {
            trace!("[{i}] Starting worker");
            for entry in stack.iter() {
                trace!("[{i}] Popped {:?} from stack", *entry);
                let output = if entry.is_dir {
                    match async_listdir(&entry.path).await {
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
                    Some(FileChecksumNode::async_for_file(&entry.path, &basepath).await)
                };
                if let Some(v) = output {
                    // If we've shut down, don't send anything except Errs
                    if v.is_err() || !stack.is_shutdown() {
                        if v.is_err() {
                            stack.shutdown();
                        }
                        trace!("[{i}] Sending {v:?} to output");
                        match sender.send(v).await {
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
            trace!("[{i}] Ending worker");
        });
    }
    drop(sender);
    // Force the receiver to receive everything (rather than breaking out early
    // on an Err) in order to ensure that all workers run to completion
    let mut infos = Vec::new();
    let mut err = None;
    while let Some(v) = receiver.recv().await {
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
