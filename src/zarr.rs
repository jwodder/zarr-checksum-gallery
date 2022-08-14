//! General operations on Zarrs and their entries
mod entrypath;
use crate::errors::{EntryNameError, FSError};
pub use entrypath::*;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Zarr {
    path: PathBuf,
}

impl Zarr {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Zarr, FSError> {
        let path = path.as_ref();
        if !fs::metadata(&path)
            .map_err(|e| FSError::stat_error(&path, e))?
            .is_dir()
        {
            return Err(FSError::not_dir_root_error(path));
        }
        Ok(Zarr { path: path.into() })
    }

    pub fn root_dir(&self) -> ZarrDirectory {
        ZarrDirectory {
            path: self.path.clone(),
            relpath: DirPath::Root,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrFile {
    path: PathBuf,
    relpath: EntryPath,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrDirectory {
    path: PathBuf,
    relpath: DirPath,
}

impl ZarrDirectory {
    pub fn entries(&self) -> Result<Vec<ZarrEntry>, FSError> {
        let mut entries = Vec::new();
        for p in fs::read_dir(&self.path).map_err(|e| FSError::readdir_error(&self.path, e))? {
            let p = p.map_err(|e| FSError::readdir_error(&self.path, e))?;
            let path = p.path();
            let ftype = p.file_type().map_err(|e| FSError::stat_error(&path, e))?;
            let is_dir = ftype.is_dir()
                || (ftype.is_symlink()
                    && fs::metadata(&path)
                        .map_err(|e| FSError::stat_error(&path, e))?
                        .is_dir());
            let relpath = match p.file_name().to_str() {
                Some(s) => self
                    .relpath
                    .join1(s)
                    .expect("DirEntry.file_name() should not be . or .. nor contain /"),
                None => return Err(FSError::undecodable_name_error(path)),
            };
            entries.push(if is_dir {
                ZarrEntry::Directory(ZarrDirectory {
                    path,
                    relpath: relpath.into(),
                })
            } else {
                ZarrEntry::File(ZarrFile { path, relpath })
            })
        }
        Ok(entries)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ZarrEntry {
    File(ZarrFile),
    Directory(ZarrDirectory),
}

impl From<ZarrFile> for ZarrEntry {
    fn from(zf: ZarrFile) -> ZarrEntry {
        ZarrEntry::File(zf)
    }
}

impl From<ZarrDirectory> for ZarrEntry {
    fn from(zd: ZarrDirectory) -> ZarrEntry {
        ZarrEntry::Directory(zd)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DirPath {
    Root,
    Path(EntryPath),
}

impl DirPath {
    pub fn join1(&self, s: &str) -> Result<EntryPath, EntryNameError> {
        match self {
            DirPath::Root if is_path_name(s) => Ok(EntryPath::try_from(s).unwrap()),
            DirPath::Path(ep) => ep.join1(s),
            _ => Err(EntryNameError(String::from(s))),
        }
    }
}

impl fmt::Display for DirPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DirPath::Root => f.write_str("<root>"),
            DirPath::Path(ep) => <EntryPath as fmt::Display>::fmt(ep, f),
        }
    }
}

impl From<EntryPath> for DirPath {
    fn from(ep: EntryPath) -> DirPath {
        DirPath::Path(ep)
    }
}
