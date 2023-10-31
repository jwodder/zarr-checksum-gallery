use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;

struct OpenDir {
    handle: Entries,
    summer: Dirsummer,
}

impl OpenDir {
    fn new(dir: ZarrDirectory) -> Result<OpenDir, FSError> {
        let handle = dir.iter_entries()?;
        Ok(OpenDir {
            handle,
            summer: dir.dirsummer(),
        })
    }
}

/// Traverse & checksum a Zarr directory tree depth-first and iteratively
///
/// The checksum for each directory is computed as soon as the checksums for
/// all of its entries are computed.
pub fn depth_first_checksum(zarr: &Zarr) -> Result<String, ChecksumError> {
    let mut dirstack = vec![OpenDir::new(zarr.root_dir())?];
    loop {
        let topdir = dirstack.last_mut().expect("dirstack should be nonempty");
        match topdir.handle.next() {
            Some(Ok(ZarrEntry::Directory(zd))) => dirstack.push(OpenDir::new(zd)?),
            Some(Ok(ZarrEntry::File(zf))) => topdir.summer.push(zf.into_checksum()?),
            Some(Err(e)) => return Err(e.into()),
            None => {
                let OpenDir { summer, .. } = dirstack.pop().expect("dirstack should be nonempty");
                match dirstack.last_mut() {
                    Some(od) => od.summer.push(summer.checksum()),
                    None => return Ok(summer.checksum().into_checksum()),
                }
            }
        }
    }
}
