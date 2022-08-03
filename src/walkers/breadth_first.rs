use crate::checksum::{try_compile_checksum, FileInfo};
use crate::error::ZarrError;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};

pub fn breadth_first_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ZarrError> {
    try_compile_checksum(
        BreadthFirstIterator::new(dirpath.as_ref())
            .map(|r| r.and_then(|p| FileInfo::for_file(p, dirpath.as_ref().into()))),
    )
}

struct BreadthFirstIterator {
    queue: VecDeque<Result<PathBuf, ZarrError>>,
}

impl BreadthFirstIterator {
    fn new<P: AsRef<Path>>(dirpath: P) -> Self {
        BreadthFirstIterator {
            queue: VecDeque::from([Ok(dirpath.as_ref().into())]),
        }
    }
}

impl Iterator for BreadthFirstIterator {
    type Item = Result<PathBuf, ZarrError>;

    fn next(&mut self) -> Option<Result<PathBuf, ZarrError>> {
        loop {
            let path = self.queue.pop_front()?;
            // TODO: Try to simplify this code
            match path {
                Ok(path) => match fs::metadata(&path) {
                    Ok(m) => {
                        if m.is_dir() {
                            match fs::read_dir(&path) {
                                Ok(iter) => self.queue.extend(iter.map(|r| {
                                    r.map_or_else(
                                        |exc| Err(ZarrError::readdir_error(&path, exc)),
                                        |e| Ok(e.path()),
                                    )
                                })),
                                Err(e) => return Some(Err(ZarrError::readdir_error(&path, e))),
                            }
                        } else {
                            return Some(Ok(path));
                        }
                    }
                    Err(e) => return Some(Err(ZarrError::stat_error(&path, e))),
                },
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
