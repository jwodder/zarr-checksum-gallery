use super::util::{listdir, DirEntry};
use crate::checksum::{try_compile_checksum, FileInfo};
use crate::error::WalkError;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

pub fn breadth_first_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, WalkError> {
    let dirpath = dirpath.as_ref();
    try_compile_checksum(
        BreadthFirstIterator::new(dirpath).map(|r| r.and_then(|p| FileInfo::for_file(p, dirpath))),
    )
}

struct BreadthFirstIterator {
    queue: VecDeque<Result<DirEntry, WalkError>>,
}

impl BreadthFirstIterator {
    fn new<P: AsRef<Path>>(dirpath: P) -> Self {
        BreadthFirstIterator {
            queue: VecDeque::from([Ok(DirEntry {
                path: dirpath.as_ref().into(),
                name: String::new(),
                is_dir: true,
            })]),
        }
    }
}

impl Iterator for BreadthFirstIterator {
    type Item = Result<PathBuf, WalkError>;

    fn next(&mut self) -> Option<Result<PathBuf, WalkError>> {
        loop {
            let entry = self.queue.pop_front()?;
            match entry {
                Ok(DirEntry {
                    path, is_dir: true, ..
                }) => match listdir(path) {
                    Ok(entries) => self.queue.extend(entries.into_iter().map(Ok)),
                    Err(e) => self.queue.push_back(Err(e)),
                },
                Ok(DirEntry { path, .. }) => return Some(Ok(path)),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
