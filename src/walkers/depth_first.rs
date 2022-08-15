use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;
use std::path::Path;

struct OpenDir {
    handle: Entries,
    entries: Directory,
}

impl OpenDir {
    fn new(dir: ZarrDirectory) -> Result<OpenDir, FSError> {
        let handle = dir.iter_entries()?;
        Ok(OpenDir {
            handle,
            entries: Directory::new(dir),
        })
    }
}

struct Directory {
    dir: ZarrDirectory,
    nodes: Vec<ZarrChecksumNode>,
}

impl Directory {
    fn new(dir: ZarrDirectory) -> Directory {
        Directory {
            dir,
            nodes: Vec::new(),
        }
    }

    fn checksum(self) -> DirChecksumNode {
        self.dir.get_checksum(self.nodes)
    }

    fn add_file(&mut self, node: FileChecksumNode) {
        self.nodes.push(node.into());
    }

    fn add_directory(&mut self, zdir: Directory) {
        self.nodes.push(zdir.checksum().into());
    }
}

/// Traverse & checksum a directory tree depth-first and iteratively
///
/// The checksum for each directory is computed as soon as the checksums for
/// all of its entries are computed.
pub fn depth_first_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let zarr = Zarr::new(dirpath)?;
    let mut dirstack = vec![OpenDir::new(zarr.root_dir())?];
    loop {
        let topdir = dirstack.last_mut().unwrap();
        match topdir.handle.next() {
            Some(Ok(ZarrEntry::Directory(zd))) => dirstack.push(OpenDir::new(zd)?),
            Some(Ok(ZarrEntry::File(zf))) => topdir.entries.add_file(zf.into_checksum()?),
            Some(Err(e)) => return Err(e.into()),
            None => {
                let OpenDir { entries, .. } = dirstack.pop().unwrap();
                match dirstack.last_mut() {
                    Some(od) => od.entries.add_directory(entries),
                    None => return Ok(entries.checksum().into_checksum()),
                }
            }
        }
    }
}
