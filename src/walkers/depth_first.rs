use super::util::decode_filename;
use crate::checksum::{get_checksum, FileInfo, ZarrChecksum};
use crate::errors::{ChecksumError, WalkError};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

struct OpenDir {
    handle: fs::ReadDir,
    path: PathBuf,
    name: String,
    entries: ZarrDirectory,
}

impl OpenDir {
    fn new<P: AsRef<Path>>(dirpath: P, name: String) -> Result<OpenDir, WalkError> {
        let handle = fs::read_dir(&dirpath).map_err(|e| WalkError::readdir_error(&dirpath, e))?;
        Ok(OpenDir {
            handle,
            path: dirpath.as_ref().into(),
            name,
            entries: ZarrDirectory::new(),
        })
    }
}

struct ZarrDirectory {
    files: HashMap<String, ZarrChecksum>,
    directories: HashMap<String, ZarrChecksum>,
}

impl ZarrDirectory {
    fn new() -> ZarrDirectory {
        ZarrDirectory {
            files: HashMap::new(),
            directories: HashMap::new(),
        }
    }

    fn checksum(self) -> ZarrChecksum {
        get_checksum(self.files, self.directories)
    }

    fn add_file(&mut self, name: String, info: FileInfo) {
        self.files.insert(name, info.into());
    }

    fn add_directory(&mut self, name: String, zdir: ZarrDirectory) {
        let checksum = zdir.checksum();
        self.directories.insert(name, checksum);
    }
}

pub fn depth_first_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let dirpath = PathBuf::from(dirpath.as_ref());
    let mut dirstack = vec![OpenDir::new(&dirpath, String::new())?];
    loop {
        let topdir = dirstack.last_mut().unwrap();
        match topdir.handle.next() {
            Some(Ok(p)) => {
                let path = p.path();
                let name = decode_filename(p.file_name())?;
                let is_dir = p
                    .file_type()
                    .map_err(|e| WalkError::stat_error(&p.path(), e))?
                    .is_dir();
                if is_dir {
                    dirstack.push(OpenDir::new(path, name)?);
                } else {
                    topdir
                        .entries
                        .add_file(name, FileInfo::for_file(path, &dirpath)?);
                }
            }
            Some(Err(e)) => return Err(WalkError::readdir_error(&topdir.path, e).into()),
            None => {
                let OpenDir { name, entries, .. } = dirstack.pop().unwrap();
                match dirstack.last_mut() {
                    Some(od) => od.entries.add_directory(name, entries),
                    None => return Ok(entries.checksum().checksum),
                }
            }
        }
    }
}
