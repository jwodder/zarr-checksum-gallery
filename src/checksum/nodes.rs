use super::json::get_checksum_json;
use crate::util::md5_string;
use crate::zarr::EntryPath;
use enum_dispatch::enum_dispatch;

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

/// Struct for computing the checksum for a directory.  After creation,
/// [`push()`][Dirsummer::push] the checksums for each directory entry and then
/// call [`checksum()`][Dirsummer::checksum] to fetch the directory's checksum.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Dirsummer {
    relpath: EntryPath,
    files: Vec<FileChecksum>,
    directories: Vec<DirChecksum>,
    size: u64,
    file_count: u64,
}

impl Dirsummer {
    pub fn new(relpath: EntryPath) -> Dirsummer {
        Dirsummer {
            relpath,
            files: Vec::new(),
            directories: Vec::new(),
            size: 0,
            file_count: 0,
        }
    }

    /// Return the path within the Zarr for the directory
    pub fn relpath(&self) -> &EntryPath {
        &self.relpath
    }

    /// Add a checksum for an entry of the directory.
    ///
    /// It is the caller's responsibility to ensure that `chksum` actually
    /// belongs to an entry in the on-disk directory and that `push()` is not
    /// called with two different checksums with the same
    /// [`name`][Checksum::name].  If these conditions are not met,
    /// [`checksum()`][Dirsummer::checksum] will return an inaccurate value.
    pub fn push<N: Into<EntryChecksum>>(&mut self, chksum: N) {
        let node = chksum.into();
        self.size += node.size();
        self.file_count += node.file_count();
        match node {
            EntryChecksum::File(f) => self.files.push(f),
            EntryChecksum::Directory(d) => self.directories.push(d),
        }
    }

    /// Compute the checksum for the directory based on the entry checksums
    /// added so far
    pub fn checksum(&self) -> DirChecksum {
        let md5 = md5_string(&get_checksum_json(
            self.files.iter(),
            self.directories.iter(),
        ));
        let checksum = format!("{}-{}--{}", md5, self.file_count, self.size);
        log::debug!(
            "Computed checksum for directory {}: {}",
            self.relpath,
            checksum
        );
        DirChecksum {
            relpath: self.relpath.clone(),
            checksum,
            size: self.size,
            file_count: self.file_count,
        }
    }
}

impl<N: Into<EntryChecksum>> Extend<N> for Dirsummer {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = N>,
    {
        for node in iter {
            self.push(node);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dirsummer_nothing() {
        let ds = Dirsummer::new("foo".try_into().unwrap());
        assert_eq!(
            ds.checksum().checksum,
            "481a2f77ab786a0f45aafd5db0971caa-0--0"
        );
    }

    #[test]
    fn test_dirsummer_one_file() {
        let mut ds = Dirsummer::new("foo".try_into().unwrap());
        ds.push(FileChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size: 1,
        });
        assert_eq!(
            ds.checksum().checksum,
            "f21b9b4bf53d7ce1167bcfae76371e59-1--1"
        );
    }

    #[test]
    fn test_dirsummer_one_directory() {
        let mut ds = Dirsummer::new("foo".try_into().unwrap());
        ds.push(DirChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
            size: 1,
            file_count: 1,
        });
        assert_eq!(
            ds.checksum().checksum,
            "ea8b8290b69b96422a3ed1cca0390f21-1--1"
        );
    }

    #[test]
    fn test_dirsummer_two_files() {
        let mut ds = Dirsummer::new("foo".try_into().unwrap());
        ds.push(FileChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size: 1,
        });
        ds.push(FileChecksum {
            relpath: "baz".try_into().unwrap(),
            checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            size: 1,
        });
        assert_eq!(
            ds.checksum().checksum,
            "8e50add2b46d3a6389e2d9d0924227fb-2--2"
        );
    }

    #[test]
    fn test_dirsummer_two_directories() {
        let mut ds = Dirsummer::new("foo".try_into().unwrap());
        ds.push(DirChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
            size: 1,
            file_count: 1,
        });
        ds.push(DirChecksum {
            relpath: "baz".try_into().unwrap(),
            checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
            size: 1,
            file_count: 1,
        });
        assert_eq!(
            ds.checksum().checksum,
            "4c21a113688f925240549b14136d61ff-2--2"
        );
    }

    #[test]
    fn test_dirsummer_one_of_each() {
        let mut ds = Dirsummer::new("foo".try_into().unwrap());
        ds.push(EntryChecksum::File(FileChecksum {
            relpath: "baz".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size: 1,
        }));
        ds.push(EntryChecksum::Directory(DirChecksum {
            relpath: "bar".try_into().unwrap(),
            checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
            size: 1,
            file_count: 1,
        }));
        assert_eq!(
            ds.checksum().checksum,
            "d5e4eb5dc8efdb54ff089db1eef34119-2--2"
        );
    }
}
