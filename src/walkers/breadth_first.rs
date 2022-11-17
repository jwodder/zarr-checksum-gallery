use crate::checksum::try_compile_checksum;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;
use std::collections::VecDeque;

/// Traverse & checksum a Zarr directory breadth-first and iteratively
///
/// This builds an in-memory tree of all file checksums for computing the final
/// Zarr checksum.
pub fn breadth_first_checksum(zarr: &Zarr) -> Result<String, ChecksumError> {
    try_compile_checksum(
        BreadthFirstIterator::new(zarr.root_dir()).map(|r| r.and_then(ZarrFile::into_checksum)),
    )
}

struct BreadthFirstIterator {
    queue: VecDeque<Result<ZarrEntry, FSError>>,
}

impl BreadthFirstIterator {
    fn new(zd: ZarrDirectory) -> Self {
        BreadthFirstIterator {
            queue: VecDeque::from([Ok(zd.into())]),
        }
    }
}

impl Iterator for BreadthFirstIterator {
    type Item = Result<ZarrFile, FSError>;

    fn next(&mut self) -> Option<Result<ZarrFile, FSError>> {
        loop {
            let entry = self.queue.pop_front()?;
            match entry {
                Ok(ZarrEntry::Directory(zd)) => match zd.entries() {
                    Ok(entries) => self.queue.extend(entries.into_iter().map(Ok)),
                    Err(e) => self.queue.push_back(Err(e)),
                },
                Ok(ZarrEntry::File(zf)) => return Some(Ok(zf)),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
