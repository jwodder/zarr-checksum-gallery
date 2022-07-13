use crate::checksum::{try_compile_checksum, FileInfo};
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

// TODO: Return a Result
pub fn depth_first_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    try_compile_checksum(
        DepthFirstIterator::new(dirpath.as_ref())
            .map(|r| r.map(|p| FileInfo::for_file(p, dirpath.as_ref().into()))),
    )
    .expect("Error walking Zarr")
}

struct DepthFirstIterator {
    queue: VecDeque<Result<PathBuf, io::Error>>,
}

impl DepthFirstIterator {
    fn new<P: AsRef<Path>>(dirpath: P) -> Self {
        DepthFirstIterator {
            queue: VecDeque::from([Ok(dirpath.as_ref().into())]),
        }
    }
}

impl Iterator for DepthFirstIterator {
    type Item = Result<PathBuf, io::Error>;

    fn next(&mut self) -> Option<Result<PathBuf, io::Error>> {
        loop {
            let path = self.queue.pop_front()?;
            // TODO: Try to simplify this code
            match path {
                Ok(path) => match fs::metadata(&path) {
                    Ok(m) => {
                        if m.is_dir() {
                            match fs::read_dir(&path) {
                                Ok(iter) => self.queue.extend(iter.map(|r| r.map(|e| e.path()))),
                                Err(e) => return Some(Err(e)),
                            }
                        } else {
                            return Some(Ok(path));
                        }
                    }
                    Err(e) => return Some(Err(e)),
                },
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
