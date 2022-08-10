mod json;
pub mod nodes;
use self::nodes::*;
use crate::errors::{ChecksumError, ChecksumTreeError, WalkError};
use relative_path::{Component, RelativePathBuf};
use std::collections::{hash_map::Entry, HashMap};

const ROOT_PATH: &str = "<root>";

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

    // TODO: Rename this (`to_node()`?) in order to distinguish it from
    // ChecksumNode::checksum
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
