mod json;
use crate::errors::{ChecksumError, ChecksumTreeError, WalkError};
use crate::util::{md5_file, md5_string, relative_to};
use enum_dispatch::enum_dispatch;
use json::get_checksum_json;
use relative_path::{Component, RelativePath, RelativePathBuf};
use std::collections::{hash_map::Entry, HashMap};
use std::fs;
use std::path::Path;

const ROOT_PATH: &str = "<root>";

#[enum_dispatch]
pub trait ChecksumNode {
    fn relpath(&self) -> &RelativePath;
    fn name(&self) -> &str;
    fn checksum(&self) -> &str;
    fn into_checksum(self) -> String;
    fn size(&self) -> u64;
    fn file_count(&self) -> u64;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileChecksumNode {
    relpath: RelativePathBuf,
    checksum: String,
    size: u64,
}

impl FileChecksumNode {
    pub fn for_file<P, Q>(path: P, basepath: Q) -> Result<Self, WalkError>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        Ok(FileChecksumNode {
            relpath: relative_to(&path, &basepath)?,
            checksum: md5_file(&path)?,
            size: fs::metadata(&path)
                .map_err(|e| WalkError::stat_error(&path, e))?
                .len(),
        })
    }
}

impl ChecksumNode for FileChecksumNode {
    fn relpath(&self) -> &RelativePath {
        &self.relpath
    }

    fn name(&self) -> &str {
        self.relpath
            .file_name()
            .expect("Invariant violated: FileChecksumNode.relpath did not have file_name")
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
    relpath: RelativePathBuf,
    checksum: String,
    size: u64,
    file_count: u64,
}

impl ChecksumNode for DirChecksumNode {
    fn relpath(&self) -> &RelativePath {
        &self.relpath
    }

    fn name(&self) -> &str {
        self.relpath
            .file_name()
            .expect("Invariant violated: DirChecksumNode.relpath did not have file_name")
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChecksumTree {
    File {
        node: FileChecksumNode,
    },
    Directory {
        relpath: RelativePathBuf,
        children: HashMap<String, ChecksumTree>,
    },
}

impl ChecksumTree {
    pub fn new() -> Self {
        ChecksumTree::directory(ROOT_PATH)
    }

    fn directory<P: Into<RelativePathBuf>>(relpath: P) -> Self {
        ChecksumTree::Directory {
            relpath: relpath.into(),
            children: HashMap::new(),
        }
    }

    // TODO: Rename this in order to distinguish it from ChecksumNode::checksum
    pub fn checksum(&self) -> ZarrChecksumNode {
        match self {
            ChecksumTree::File { node } => node.clone().into(),
            ChecksumTree::Directory { relpath, children } => {
                get_checksum(relpath.clone(), children.values().map(|n| n.checksum())).into()
            }
        }
    }

    pub fn add_file(&mut self, node: FileChecksumNode) -> Result<(), ChecksumTreeError> {
        match self {
            ChecksumTree::File { .. } => Err(ChecksumTreeError::PathTypeConflict {
                path: RelativePathBuf::from(ROOT_PATH),
            }),
            ChecksumTree::Directory { children, .. } => {
                let mut parts = Vec::new();
                for p in node.relpath.components() {
                    match p {
                        Component::Normal(name) => parts.push(name.to_string()),
                        _ => return Err(ChecksumTreeError::InvalidPath { path: node.relpath }),
                    }
                }
                let basename = match parts.pop() {
                    Some(s) => s,
                    None => return Err(ChecksumTreeError::InvalidPath { path: node.relpath }),
                };
                let mut d = children;
                let mut dpath = RelativePathBuf::new();
                for dirname in parts {
                    dpath.push(&dirname);
                    match d
                        .entry(dirname.clone())
                        .or_insert_with(|| ChecksumTree::directory(&dpath))
                    {
                        ChecksumTree::File { .. } => {
                            return Err(ChecksumTreeError::PathTypeConflict { path: dpath })
                        }
                        ChecksumTree::Directory { children, .. } => d = children,
                    }
                }
                match d.entry(basename) {
                    Entry::Occupied(_) => {
                        return Err(ChecksumTreeError::DoubleAdd { path: node.relpath })
                    }
                    Entry::Vacant(v) => {
                        v.insert(ChecksumTree::File { node });
                    }
                }
                Ok(())
            }
        }
    }

    pub fn from_files<I: IntoIterator<Item = FileChecksumNode>>(
        iter: I,
    ) -> Result<ChecksumTree, ChecksumTreeError> {
        let mut zarr = ChecksumTree::new();
        for node in iter {
            zarr.add_file(node)?;
        }
        Ok(zarr)
    }
}

impl Default for ChecksumTree {
    fn default() -> Self {
        ChecksumTree::new()
    }
}

pub fn get_checksum<I>(relpath: RelativePathBuf, iter: I) -> DirChecksumNode
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
    DirChecksumNode {
        relpath,
        checksum,
        size,
        file_count,
    }
}

pub fn compile_checksum<I: IntoIterator<Item = FileChecksumNode>>(
    iter: I,
) -> Result<String, ChecksumTreeError> {
    Ok(ChecksumTree::from_files(iter)?.checksum().into_checksum())
}

pub fn try_compile_checksum<I>(iter: I) -> Result<String, ChecksumError>
where
    I: IntoIterator<Item = Result<FileChecksumNode, WalkError>>,
{
    let mut zarr = ChecksumTree::new();
    for node in iter {
        zarr.add_file(node?)?;
    }
    Ok(zarr.checksum().into_checksum())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::iter::empty;

    #[test]
    fn test_get_checksum_nothing() {
        let checksum = get_checksum("foo".into(), empty());
        assert_eq!(checksum.checksum, "481a2f77ab786a0f45aafd5db0971caa-0--0");
    }

    #[test]
    fn test_get_checksum_one_file() {
        let nodes = vec![ZarrChecksumNode::File(FileChecksumNode {
            relpath: "bar".into(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size: 1,
        })];
        let checksum = get_checksum("foo".into(), nodes);
        assert_eq!(checksum.checksum, "f21b9b4bf53d7ce1167bcfae76371e59-1--1");
    }

    #[test]
    fn test_get_checksum_one_directory() {
        let nodes = vec![ZarrChecksumNode::Directory(DirChecksumNode {
            relpath: "bar".into(),
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
            size: 1,
            file_count: 1,
        })];
        let checksum = get_checksum("foo".into(), nodes);
        assert_eq!(checksum.checksum, "ea8b8290b69b96422a3ed1cca0390f21-1--1");
    }

    #[test]
    fn test_get_checksum_two_files() {
        let nodes = vec![
            ZarrChecksumNode::File(FileChecksumNode {
                relpath: "bar".into(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
            }),
            ZarrChecksumNode::File(FileChecksumNode {
                relpath: "baz".into(),
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                size: 1,
            }),
        ];
        let checksum = get_checksum("foo".into(), nodes);
        assert_eq!(checksum.checksum, "8e50add2b46d3a6389e2d9d0924227fb-2--2");
    }

    #[test]
    fn test_get_checksum_two_directories() {
        let nodes = vec![
            ZarrChecksumNode::Directory(DirChecksumNode {
                relpath: "bar".into(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                size: 1,
                file_count: 1,
            }),
            ZarrChecksumNode::Directory(DirChecksumNode {
                relpath: "baz".into(),
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                size: 1,
                file_count: 1,
            }),
        ];
        let checksum = get_checksum("foo".into(), nodes);
        assert_eq!(checksum.checksum, "4c21a113688f925240549b14136d61ff-2--2");
    }

    #[test]
    fn test_get_checksum_one_of_each() {
        let nodes = vec![
            ZarrChecksumNode::File(FileChecksumNode {
                relpath: "baz".into(),
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
            }),
            ZarrChecksumNode::Directory(DirChecksumNode {
                relpath: "bar".into(),
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                size: 1,
                file_count: 1,
            }),
        ];
        let checksum = get_checksum("foo".into(), nodes);
        assert_eq!(checksum.checksum, "d5e4eb5dc8efdb54ff089db1eef34119-2--2");
    }

    #[test]
    fn test_checksum_tree() {
        let mut sample = ChecksumTree::new();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_0/.zarray".into(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_0/0".into(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_1/.zarray".into(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_1/0".into(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: ".zgroup".into(),
                checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            })
            .unwrap();
        assert_eq!(
            sample.checksum().checksum(),
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }

    #[test]
    fn test_from_files() {
        let files = vec![
            FileChecksumNode {
                relpath: "arr_0/.zarray".into(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksumNode {
                relpath: "arr_0/0".into(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileChecksumNode {
                relpath: "arr_1/.zarray".into(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksumNode {
                relpath: "arr_1/0".into(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileChecksumNode {
                relpath: ".zgroup".into(),
                checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let sample = ChecksumTree::from_files(files).unwrap();
        assert_eq!(
            sample.checksum().checksum(),
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }
}
