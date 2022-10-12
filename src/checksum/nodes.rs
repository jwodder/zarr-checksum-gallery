use super::json::get_checksum_json;
use crate::errors::FSError;
use crate::util::{md5_file, md5_string};
use crate::zarr::{relative_to, EntryPath};
use enum_dispatch::enum_dispatch;
use log::debug;
use std::fs;
use std::path::Path;

/// Trait for behavior shared by [`FileChecksum`] and [`DirChecksum`]
#[enum_dispatch]
pub trait Checksum {
    /// Return the path within the Zarr for which this is a checksum
    fn relpath(&self) -> &EntryPath;

    /// Return the final component of the path
    fn name(&self) -> &str;

    /// Return the checksum for the file or directory
    fn checksum(&self) -> &str;

    /// Consume the node and return the checksum for the file or directory
    fn into_checksum(self) -> String;

    /// Return the size of the file or the total size of all files within the
    /// directory
    fn size(&self) -> u64;

    /// Return the number of files within the directory, or 1 for a
    /// [`FileChecksum`]
    fn file_count(&self) -> u64;
}

/// An MD5 checksum computed for a file in a Zarr directory
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileChecksum {
    pub(super) relpath: EntryPath,
    pub(super) checksum: String,
    pub(super) size: u64,
}

impl FileChecksum {
    pub(crate) fn new(relpath: EntryPath, checksum: String, size: u64) -> Self {
        FileChecksum {
            relpath,
            checksum,
            size,
        }
    }

    /// Compute the checksum for the file `path` within the Zarr at `basepath`
    pub(crate) fn for_file<P, Q>(path: P, basepath: Q) -> Result<Self, FSError>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let relpath = relative_to(&path, &basepath)?;
        let size = fs::metadata(&path)
            .map_err(|e| FSError::stat_error(&path, e))?
            .len();
        let checksum = md5_file(&path)?;
        debug!("Computed checksum for file {relpath}: {checksum}");
        Ok(FileChecksum {
            relpath,
            checksum,
            size,
        })
    }
}

impl Checksum for FileChecksum {
    fn relpath(&self) -> &EntryPath {
        &self.relpath
    }

    fn name(&self) -> &str {
        self.relpath.file_name()
    }

    fn checksum(&self) -> &str {
        &self.checksum
    }

    fn into_checksum(self) -> String {
        self.checksum
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn file_count(&self) -> u64 {
        1
    }
}

/// A Zarr checksum computed for a directory inside a Zarr directory
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DirChecksum {
    pub(super) relpath: EntryPath,
    pub(super) checksum: String,
    pub(super) size: u64,
    pub(super) file_count: u64,
}

impl Checksum for DirChecksum {
    fn relpath(&self) -> &EntryPath {
        &self.relpath
    }

    fn name(&self) -> &str {
        self.relpath.file_name()
    }

    fn checksum(&self) -> &str {
        &self.checksum
    }

    fn into_checksum(self) -> String {
        self.checksum
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn file_count(&self) -> u64 {
        self.file_count
    }
}

/// An enum of [`FileChecksum`] and [`DirChecksum`]
#[enum_dispatch(Checksum)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum EntryChecksum {
    File(FileChecksum),
    Directory(DirChecksum),
}

impl EntryChecksum {
    /// True iff this node represents a directory checksum
    pub fn is_dir(&self) -> bool {
        matches!(self, EntryChecksum::Directory(_))
    }

    /// True iff this node represents a file checksum
    pub fn is_file(&self) -> bool {
        matches!(self, EntryChecksum::File(_))
    }
}

/// Compute the checksum for the directory at relative path `relpath` within a
/// Zarr, where the entries of the directory have the checksums supplied in
/// `iter`.
///
/// It is the caller's responsibility to ensure that `iter` contains all & only
/// entries from the given directory and that no two items in `iter` have the
/// same [`name`][Checksum::name].  If this condition is not met,
/// `get_checksum()` will return an inaccurate value.
pub(crate) fn get_checksum<I>(relpath: EntryPath, iter: I) -> DirChecksum
where
    I: IntoIterator<Item = EntryChecksum>,
{
    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut size = 0;
    let mut file_count = 0;
    for node in iter {
        size += node.size();
        file_count += node.file_count();
        match node {
            EntryChecksum::File(f) => files.push(f),
            EntryChecksum::Directory(d) => directories.push(d),
        }
    }
    let md5 = md5_string(&get_checksum_json(files, directories));
    let checksum = format!("{md5}-{file_count}--{size}");
    debug!("Computed checksum for directory {relpath}: {checksum}");
    DirChecksum {
        relpath,
        checksum,
        size,
        file_count,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::iter::empty;

    #[test]
    fn test_get_checksum_nothing() {
        let checksum = get_checksum("foo".try_into().unwrap(), empty());
        assert_eq!(checksum.checksum, "481a2f77ab786a0f45aafd5db0971caa-0--0");
    }

    #[test]
    fn test_get_checksum_one_file() {
        let nodes = vec![EntryChecksum::File(FileChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size: 1,
        })];
        let checksum = get_checksum("foo".try_into().unwrap(), nodes);
        assert_eq!(checksum.checksum, "f21b9b4bf53d7ce1167bcfae76371e59-1--1");
    }

    #[test]
    fn test_get_checksum_one_directory() {
        let nodes = vec![EntryChecksum::Directory(DirChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
            size: 1,
            file_count: 1,
        })];
        let checksum = get_checksum("foo".try_into().unwrap(), nodes);
        assert_eq!(checksum.checksum, "ea8b8290b69b96422a3ed1cca0390f21-1--1");
    }

    #[test]
    fn test_get_checksum_two_files() {
        let nodes = vec![
            EntryChecksum::File(FileChecksum {
                relpath: "bar".try_into().unwrap(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
            }),
            EntryChecksum::File(FileChecksum {
                relpath: "baz".try_into().unwrap(),
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                size: 1,
            }),
        ];
        let checksum = get_checksum("foo".try_into().unwrap(), nodes);
        assert_eq!(checksum.checksum, "8e50add2b46d3a6389e2d9d0924227fb-2--2");
    }

    #[test]
    fn test_get_checksum_two_directories() {
        let nodes = vec![
            EntryChecksum::Directory(DirChecksum {
                relpath: "bar".try_into().unwrap(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                size: 1,
                file_count: 1,
            }),
            EntryChecksum::Directory(DirChecksum {
                relpath: "baz".try_into().unwrap(),
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                size: 1,
                file_count: 1,
            }),
        ];
        let checksum = get_checksum("foo".try_into().unwrap(), nodes);
        assert_eq!(checksum.checksum, "4c21a113688f925240549b14136d61ff-2--2");
    }

    #[test]
    fn test_get_checksum_one_of_each() {
        let nodes = vec![
            EntryChecksum::File(FileChecksum {
                relpath: "baz".try_into().unwrap(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
            }),
            EntryChecksum::Directory(DirChecksum {
                relpath: "bar".try_into().unwrap(),
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                size: 1,
                file_count: 1,
            }),
        ];
        let checksum = get_checksum("foo".try_into().unwrap(), nodes);
        assert_eq!(checksum.checksum, "d5e4eb5dc8efdb54ff089db1eef34119-2--2");
    }
}
