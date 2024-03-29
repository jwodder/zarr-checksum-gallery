//! General operations on Zarrs and the entries within
mod entrypath;
use crate::checksum::nodes::*;
use crate::errors::{EntryNameError, FSError};
use crate::util::{async_md5_file, md5_file};
pub use entrypath::*;
use fs_err::{metadata, read_dir, tokio as afs, DirEntry, ReadDir};
use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};

/// Names of files & directories that are excluded from consideration when
/// traversing a Zarr
static EXCLUDED_DOTFILES: &[&str] = &[
    // This list must be kept in sorted order (enforced by the test
    // `test_excluded_dotfiles_is_sorted()`)
    ".dandi",
    ".datalad",
    ".git",
    ".gitattributes",
    ".gitmodules",
];

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Zarr {
    path: PathBuf,
    exclude_dotfiles: bool,
}

impl Zarr {
    pub fn new<P: AsRef<Path>>(path: P) -> Zarr {
        Zarr {
            path: path.as_ref().into(),
            exclude_dotfiles: false,
        }
    }

    pub fn exclude_dotfiles(self, flag: bool) -> Zarr {
        Zarr {
            exclude_dotfiles: flag,
            ..self
        }
    }

    pub fn root_dir(&self) -> ZarrDirectory {
        ZarrDirectory {
            path: self.path.clone(),
            relpath: DirPath::Root,
            exclude_dotfiles: self.exclude_dotfiles,
        }
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
        let size = metadata(&self.path)?.len();
        let checksum = md5_file(self.path)?;
        log::debug!("Computed checksum for file {}: {checksum}", &self.relpath);
        Ok(FileChecksum::new(self.relpath, checksum, size))
    }

    pub async fn async_into_checksum(self) -> Result<FileChecksum, FSError> {
        let size = afs::metadata(&self.path).await?.len();
        let checksum = async_md5_file(self.path).await?;
        log::debug!("Computed checksum for file {}: {checksum}", &self.relpath);
        Ok(FileChecksum::new(self.relpath, checksum, size))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrDirectory {
    path: PathBuf,
    relpath: DirPath,
    exclude_dotfiles: bool,
}

impl ZarrDirectory {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn relpath(&self) -> &DirPath {
        &self.relpath
    }

    pub fn entries(&self) -> Result<Vec<ZarrEntry>, FSError> {
        self.iter_entries()?.collect()
    }

    pub fn iter_entries(&self) -> Result<Entries, FSError> {
        let handle = read_dir(&self.path)?;
        Ok(Entries {
            handle,
            baserelpath: self.relpath.clone(),
            exclude_dotfiles: self.exclude_dotfiles,
        })
    }

    pub async fn async_entries(&self) -> Result<Vec<ZarrEntry>, FSError> {
        let mut entries = Vec::new();
        let mut handle = afs::read_dir(&self.path).await?;
        while let Some(p) = handle.next_entry().await.transpose() {
            let p = p?;
            let path = p.path();
            if self.exclude_dotfiles && is_excluded_dotfile(&path) {
                log::debug!("Excluding special dotfile {path:?}");
                continue;
            }
            let ftype = p.file_type().await?;
            let is_dir =
                ftype.is_dir() || (ftype.is_symlink() && afs::metadata(&path).await?.is_dir());
            let relpath = match p.file_name().to_str() {
                Some(s) => self
                    .relpath
                    .join1(s)
                    .expect("DirEntry.file_name() should not be . or .. nor contain /"),
                None => return Err(FSError::UndecodableName { path }),
            };
            entries.push(if is_dir {
                ZarrEntry::Directory(ZarrDirectory {
                    path,
                    relpath: relpath.into(),
                    exclude_dotfiles: self.exclude_dotfiles,
                })
            } else {
                ZarrEntry::File(ZarrFile { path, relpath })
            });
        }
        Ok(entries)
    }

    pub fn dirsummer(&self) -> Dirsummer {
        let relpath = match &self.relpath {
            // TODO: Replace this kludgy workaround with something better:
            DirPath::Root => {
                let Ok(ep) = EntryPath::try_from("<root>") else {
                    unreachable!("<root> should be a valid EntryPath");
                };
                ep
            }
            DirPath::Path(ep) => ep.clone(),
        };
        Dirsummer::new(relpath)
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
        let mut ds = self.dirsummer();
        ds.extend(nodes);
        ds.checksum()
    }
}

#[derive(Debug)]
pub struct Entries {
    handle: ReadDir,
    baserelpath: DirPath,
    exclude_dotfiles: bool,
}

impl Entries {
    fn process_direntry(&self, p: DirEntry) -> Result<ZarrEntry, FSError> {
        let path = p.path();
        let ftype = p.file_type()?;
        let is_dir = ftype.is_dir() || (ftype.is_symlink() && metadata(&path)?.is_dir());
        let relpath = match p.file_name().to_str() {
            Some(s) => self
                .baserelpath
                .join1(s)
                .expect("DirEntry.file_name() should not be . or .. nor contain /"),
            None => return Err(FSError::UndecodableName { path }),
        };
        Ok(if is_dir {
            ZarrEntry::Directory(ZarrDirectory {
                path,
                relpath: relpath.into(),
                exclude_dotfiles: self.exclude_dotfiles,
            })
        } else {
            ZarrEntry::File(ZarrFile { path, relpath })
        })
    }
}

impl Iterator for Entries {
    type Item = Result<ZarrEntry, FSError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            return Some(match self.handle.next()? {
                Ok(p) => {
                    let path = p.path();
                    if self.exclude_dotfiles && is_excluded_dotfile(&path) {
                        log::debug!("Excluding special dotfile {path:?}");
                        continue;
                    }
                    self.process_direntry(p)
                }
                Err(e) => Err(e.into()),
            });
        }
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
            DirPath::Root if is_path_name(s) => {
                Ok(EntryPath::try_from(s).expect("path names should be valid EntryPaths"))
            }
            DirPath::Path(ep) => ep.join1(s),
            _ => Err(EntryNameError(String::from(s))),
        }
    }
}

impl fmt::Display for DirPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

pub fn is_excluded_dotfile<P: AsRef<Path>>(path: P) -> bool {
    if let Some(name) = path.as_ref().file_name().and_then(OsStr::to_str) {
        EXCLUDED_DOTFILES.binary_search(&name).is_ok()
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_excluded_dotfiles_is_sorted() {
        assert!(EXCLUDED_DOTFILES.windows(2).all(|ab| {
            assert!(ab.len() >= 2);
            ab[0] < ab[1]
        }));
    }

    #[rstest]
    #[case(".dandi", true)]
    #[case(".datalad", true)]
    #[case(".git", true)]
    #[case(".gitattributes", true)]
    #[case(".gitmodules", true)]
    #[case("foo/bar/.dandi", true)]
    #[case("foo/bar/.datalad", true)]
    #[case("foo/bar/.git", true)]
    #[case("foo/bar/.gitattributes", true)]
    #[case("foo/bar/.gitmodules", true)]
    #[case(".dandi/foo/bar", false)]
    #[case(".datalad/foo/bar", false)]
    #[case(".git/foo/bar", false)]
    #[case(".gitattributes/foo/bar", false)]
    #[case(".gitmodules/foo/bar", false)]
    fn test_is_excluded_dotfile(#[case] path: &str, #[case] b: bool) {
        assert_eq!(is_excluded_dotfile(path), b);
    }
}
