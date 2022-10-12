//! General operations on Zarrs and the entries within
mod entrypath;
use crate::checksum::nodes::*;
use crate::errors::{EntryNameError, FSError};
use crate::util::{async_md5_file, md5_file};
pub use entrypath::*;
use log::debug;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::fs as afs;
use tokio_stream::wrappers::ReadDirStream;
use tokio_stream::StreamExt;

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
            return Err(FSError::not_dir_root(path));
        }
        Ok(Zarr { path: path.into() })
    }

    pub fn root_dir(&self) -> ZarrDirectory {
        ZarrDirectory {
            path: self.path.clone(),
            relpath: DirPath::Root,
        }
    }

    pub(crate) fn checksum_file<P: AsRef<Path>>(&self, path: P) -> Result<FileChecksum, FSError> {
        FileChecksum::for_file(path, &self.path)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrFile {
    path: PathBuf,
    relpath: EntryPath,
}

impl ZarrFile {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn relpath(&self) -> &EntryPath {
        &self.relpath
    }

    pub fn into_checksum(self) -> Result<FileChecksum, FSError> {
        let size = fs::metadata(&self.path)
            .map_err(|e| FSError::stat_error(&self.path, e))?
            .len();
        let checksum = md5_file(self.path)?;
        debug!("Computed checksum for file {}: {checksum}", &self.relpath);
        Ok(FileChecksum::new(self.relpath, checksum, size))
    }

    pub async fn async_into_checksum(self) -> Result<FileChecksum, FSError> {
        let size = afs::metadata(&self.path)
            .await
            .map_err(|e| FSError::stat_error(&self.path, e))?
            .len();
        let checksum = async_md5_file(self.path).await?;
        debug!("Computed checksum for file {}: {checksum}", &self.relpath);
        Ok(FileChecksum::new(self.relpath, checksum, size))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrDirectory {
    path: PathBuf,
    relpath: DirPath,
}

impl ZarrDirectory {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn relpath(&self) -> &DirPath {
        &self.relpath
    }

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
                None => return Err(FSError::undecodable_name(path)),
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

    pub fn iter_entries(&self) -> Result<Entries, FSError> {
        let handle = fs::read_dir(&self.path).map_err(|e| FSError::readdir_error(&self.path, e))?;
        Ok(Entries {
            handle,
            basepath: self.path.clone(),
            baserelpath: self.relpath.clone(),
        })
    }

    pub async fn async_entries(&self) -> Result<Vec<ZarrEntry>, FSError> {
        let mut entries = Vec::new();
        let handle = afs::read_dir(&self.path)
            .await
            .map_err(|e| FSError::readdir_error(&self.path, e))?;
        let mut stream = ReadDirStream::new(handle);
        while let Some(p) = stream.next().await {
            let p = p.map_err(|e| FSError::readdir_error(&self.path, e))?;
            let path = p.path();
            let ftype = p
                .file_type()
                .await
                .map_err(|e| FSError::stat_error(&path, e))?;
            let is_dir = ftype.is_dir()
                || (ftype.is_symlink()
                    && afs::metadata(&path)
                        .await
                        .map_err(|e| FSError::stat_error(&path, e))?
                        .is_dir());
            let relpath = match p.file_name().to_str() {
                Some(s) => self
                    .relpath
                    .join1(s)
                    .expect("DirEntry.file_name() should not be . or .. nor contain /"),
                None => return Err(FSError::undecodable_name(path)),
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

    /// Compute the checksum for the directory from the given checksums for the
    /// directory's entries.
    ///
    /// It is the caller's responsibility to ensure that `nodes` contains all &
    /// only entries from the directory in question and that no two items in
    /// `nodes` have the same [`name`][Checksum::name].  If this condition is
    /// not met, `get_checksum()` will return an inaccurate value.
    pub fn get_checksum<I>(&self, nodes: I) -> DirChecksum
    where
        I: IntoIterator<Item = EntryChecksum>,
    {
        let relpath = match &self.relpath {
            // TODO: Replace this kludgy workaround with something better:
            DirPath::Root => EntryPath::try_from("<root>").unwrap(),
            DirPath::Path(ep) => ep.clone(),
        };
        get_checksum(relpath, nodes)
    }
}

pub struct Entries {
    handle: fs::ReadDir,
    basepath: PathBuf,
    baserelpath: DirPath,
}

impl Iterator for Entries {
    type Item = Result<ZarrEntry, FSError>;

    fn next(&mut self) -> Option<Self::Item> {
        let p = match self.handle.next()? {
            Ok(p) => p,
            Err(e) => return Some(Err(FSError::readdir_error(&self.basepath, e))),
        };
        let path = p.path();
        let ftype = match p.file_type() {
            Ok(ft) => ft,
            Err(e) => return Some(Err(FSError::stat_error(path, e))),
        };
        let is_dir = if ftype.is_dir() {
            true
        } else if ftype.is_symlink() {
            match fs::metadata(&path) {
                Ok(m) => m.is_dir(),
                Err(e) => return Some(Err(FSError::stat_error(path, e))),
            }
        } else {
            false
        };
        let relpath = match p.file_name().to_str() {
            Some(s) => self
                .baserelpath
                .join1(s)
                .expect("DirEntry.file_name() should not be . or .. nor contain /"),
            None => return Some(Err(FSError::undecodable_name(path))),
        };
        Some(Ok(if is_dir {
            ZarrEntry::Directory(ZarrDirectory {
                path,
                relpath: relpath.into(),
            })
        } else {
            ZarrEntry::File(ZarrFile { path, relpath })
        }))
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
pub enum DirPath {
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
