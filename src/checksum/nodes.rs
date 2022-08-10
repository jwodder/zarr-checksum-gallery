use super::json::get_checksum_json;
use crate::errors::FSError;
use crate::util::{async_md5_file, md5_file, md5_string};
use crate::zarr::{relative_to, EntryPath};
use enum_dispatch::enum_dispatch;
use log::debug;
use std::fs;
use std::path::Path;
use tokio::fs as afs;

#[enum_dispatch]
pub trait ChecksumNode {
    fn relpath(&self) -> &EntryPath;
    fn name(&self) -> &str;
    fn checksum(&self) -> &str;
    fn into_checksum(self) -> String;
    fn size(&self) -> u64;
    fn file_count(&self) -> u64;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileChecksumNode {
    pub(super) relpath: EntryPath,
    pub(super) checksum: String,
    pub(super) size: u64,
}

impl FileChecksumNode {
    pub fn for_file<P, Q>(path: P, basepath: Q) -> Result<Self, FSError>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        Ok(FileChecksumNode {
            relpath: relative_to(&path, &basepath)?,
            checksum: md5_file(&path)?,
            size: fs::metadata(&path)
                .map_err(|e| FSError::stat_error(&path, e))?
                .len(),
        })
    }

    pub async fn async_for_file<P, Q>(path: P, basepath: Q) -> Result<Self, FSError>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        Ok(FileChecksumNode {
            relpath: relative_to(&path, &basepath)?,
            checksum: async_md5_file(&path).await?,
            size: afs::metadata(&path)
                .await
                .map_err(|e| FSError::stat_error(&path, e))?
                .len(),
        })
    }
}

impl ChecksumNode for FileChecksumNode {
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DirChecksumNode {
    pub(super) relpath: EntryPath,
    pub(super) checksum: String,
    pub(super) size: u64,
    pub(super) file_count: u64,
}

impl ChecksumNode for DirChecksumNode {
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

#[enum_dispatch(ChecksumNode)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ZarrChecksumNode {
    File(FileChecksumNode),
    Directory(DirChecksumNode),
}

impl ZarrChecksumNode {
    pub fn is_dir(&self) -> bool {
        matches!(self, ZarrChecksumNode::Directory(_))
    }

    pub fn is_file(&self) -> bool {
        matches!(self, ZarrChecksumNode::File(_))
    }
}

pub fn get_checksum<I>(relpath: EntryPath, iter: I) -> DirChecksumNode
where
    I: IntoIterator<Item = ZarrChecksumNode>,
{
    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut size = 0;
    let mut file_count = 0;
    for node in iter {
        size += node.size();
        file_count += node.file_count();
        match node {
            ZarrChecksumNode::File(f) => files.push(f),
            ZarrChecksumNode::Directory(d) => directories.push(d),
        }
    }
    let md5 = md5_string(&get_checksum_json(files, directories));
    let checksum = format!("{md5}-{file_count}--{size}");
    debug!("Computed checksum for directory {relpath}: {checksum}");
    DirChecksumNode {
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
        let nodes = vec![ZarrChecksumNode::File(FileChecksumNode {
            relpath: "bar".try_into().unwrap(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size: 1,
        })];
        let checksum = get_checksum("foo".try_into().unwrap(), nodes);
        assert_eq!(checksum.checksum, "f21b9b4bf53d7ce1167bcfae76371e59-1--1");
    }

    #[test]
    fn test_get_checksum_one_directory() {
        let nodes = vec![ZarrChecksumNode::Directory(DirChecksumNode {
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
            ZarrChecksumNode::File(FileChecksumNode {
                relpath: "bar".try_into().unwrap(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
            }),
            ZarrChecksumNode::File(FileChecksumNode {
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
            ZarrChecksumNode::Directory(DirChecksumNode {
                relpath: "bar".try_into().unwrap(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                size: 1,
                file_count: 1,
            }),
            ZarrChecksumNode::Directory(DirChecksumNode {
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
            ZarrChecksumNode::File(FileChecksumNode {
                relpath: "baz".try_into().unwrap(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
            }),
            ZarrChecksumNode::Directory(DirChecksumNode {
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
