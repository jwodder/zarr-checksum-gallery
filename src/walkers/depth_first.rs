use crate::checksum::{get_checksum, FileInfo, ZarrChecksum};
use crate::error::ZarrError;
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
    fn new<P: AsRef<Path>>(dirpath: P, name: String) -> Result<OpenDir, ZarrError> {
        let handle = fs::read_dir(&dirpath).map_err(|e| ZarrError::readdir_error(&dirpath, e))?;
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

    fn is_empty(&self) -> bool {
        self.files.is_empty() && self.directories.is_empty()
    }

    fn add_file(&mut self, name: String, info: FileInfo) {
        self.files.insert(name, info.into());
    }

    fn add_directory(&mut self, name: String, zdir: ZarrDirectory) {
        if !zdir.is_empty() {
            let checksum = zdir.checksum();
            self.directories.insert(name, checksum);
        }
    }
}

pub fn depth_first_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ZarrError> {
    let dirpath = PathBuf::from(dirpath.as_ref());
    let mut dirstack = vec![OpenDir::new(&dirpath, String::new())?];
    loop {
        let topdir = dirstack.last_mut().unwrap();
        match topdir.handle.next() {
            Some(Ok(p)) => {
                let path = p.path();
                // TODO: Add a dedicated ZarrError variant for this failure:
                let name = p.file_name().to_str().unwrap().to_string();
                let is_dir = p
                    .file_type()
                    .map_err(|e| ZarrError::stat_error(&p.path(), e))?
                    .is_dir();
                if is_dir {
                    dirstack.push(OpenDir::new(path, name)?);
                } else {
                    topdir
                        .entries
                        .add_file(name, FileInfo::for_file(&path, &dirpath)?);
                }
            }
            Some(Err(e)) => return Err(ZarrError::readdir_error(&topdir.path, e)),
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
