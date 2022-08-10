use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::util::relative_to;
use relative_path::RelativePathBuf;
use std::fs;
use std::path::{Path, PathBuf};

struct OpenDir {
    handle: fs::ReadDir,
    path: PathBuf,
    entries: ZarrDirectory,
}

impl OpenDir {
    fn new<P: AsRef<Path>>(dirpath: P, relpath: RelativePathBuf) -> Result<OpenDir, FSError> {
        let handle = fs::read_dir(&dirpath).map_err(|e| FSError::readdir_error(&dirpath, e))?;
        Ok(OpenDir {
            handle,
            path: dirpath.as_ref().into(),
            entries: ZarrDirectory::new(relpath),
        })
    }
}

struct ZarrDirectory {
    relpath: RelativePathBuf,
    nodes: Vec<ZarrChecksumNode>,
}

impl ZarrDirectory {
    fn new(relpath: RelativePathBuf) -> ZarrDirectory {
        ZarrDirectory {
            relpath,
            nodes: Vec::new(),
        }
    }

    fn checksum(self) -> DirChecksumNode {
        get_checksum(self.relpath, self.nodes)
    }

    fn add_file(&mut self, node: FileChecksumNode) {
        self.nodes.push(node.into());
    }

    fn add_directory(&mut self, zdir: ZarrDirectory) {
        self.nodes.push(zdir.checksum().into());
    }
}

pub fn depth_first_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let dirpath = PathBuf::from(dirpath.as_ref());
    let mut dirstack = vec![OpenDir::new(&dirpath, "<root>".into())?];
    loop {
        let topdir = dirstack.last_mut().unwrap();
        match topdir.handle.next() {
            Some(Ok(p)) => {
                let path = p.path();
                let is_dir = p
                    .file_type()
                    .map_err(|e| FSError::stat_error(&p.path(), e))?
                    .is_dir();
                if is_dir {
                    let relpath = relative_to(&path, &dirpath)?;
                    dirstack.push(OpenDir::new(path, relpath)?);
                } else {
                    topdir
                        .entries
                        .add_file(FileChecksumNode::for_file(path, &dirpath)?);
                }
            }
            Some(Err(e)) => return Err(FSError::readdir_error(&topdir.path, e).into()),
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
