use super::nodes::*;
use crate::errors::ChecksumTreeError;
use crate::zarr::EntryPath;
use std::collections::{hash_map::Entry, HashMap};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumTree(DirTree);

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirTree {
    relpath: EntryPath,
    children: HashMap<String, TreeNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TreeNode {
    File(FileChecksumNode),
    Directory(DirTree),
}

impl ChecksumTree {
    pub fn new() -> Self {
        ChecksumTree(DirTree::new("<root>".try_into().unwrap()))
    }

    pub fn checksum(&self) -> String {
        self.0.to_checksum_node().into_checksum()
    }

    pub fn into_checksum(self) -> String {
        DirChecksumNode::from(self.0).into_checksum()
    }

    pub fn add_file(&mut self, node: FileChecksumNode) -> Result<(), ChecksumTreeError> {
        let mut d = &mut self.0.children;
        for parent in node.relpath().parents() {
            match d
                .entry(parent.file_name().to_string())
                .or_insert_with(|| TreeNode::directory(parent.clone()))
            {
                TreeNode::File(_) => {
                    return Err(ChecksumTreeError::PathTypeConflict { path: parent })
                }
                TreeNode::Directory(DirTree { children, .. }) => d = children,
            }
        }
        match d.entry(node.relpath().file_name().to_string()) {
            Entry::Occupied(_) => return Err(ChecksumTreeError::DoubleAdd { path: node.relpath }),
            Entry::Vacant(v) => {
                v.insert(TreeNode::File(node));
            }
        }
        Ok(())
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

impl DirTree {
    fn new(relpath: EntryPath) -> Self {
        DirTree {
            relpath,
            children: HashMap::new(),
        }
    }

    fn to_checksum_node(&self) -> DirChecksumNode {
        get_checksum(
            self.relpath.clone(),
            self.children.values().map(TreeNode::to_checksum_node),
        )
    }
}

impl From<DirTree> for DirChecksumNode {
    fn from(dirtree: DirTree) -> DirChecksumNode {
        get_checksum(
            dirtree.relpath,
            dirtree.children.into_values().map(ZarrChecksumNode::from),
        )
    }
}

impl TreeNode {
    fn directory(relpath: EntryPath) -> Self {
        TreeNode::Directory(DirTree::new(relpath))
    }

    fn to_checksum_node(&self) -> ZarrChecksumNode {
        match self {
            TreeNode::File(node) => node.clone().into(),
            TreeNode::Directory(dirtree) => dirtree.to_checksum_node().into(),
        }
    }
}

impl From<TreeNode> for ZarrChecksumNode {
    fn from(node: TreeNode) -> ZarrChecksumNode {
        match node {
            TreeNode::File(node) => node.into(),
            TreeNode::Directory(dirtree) => DirChecksumNode::from(dirtree).into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_checksum_tree() {
        let mut sample = ChecksumTree::new();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_0/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_0/0".try_into().unwrap(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_1/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: "arr_1/0".try_into().unwrap(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            })
            .unwrap();
        sample
            .add_file(FileChecksumNode {
                relpath: ".zgroup".try_into().unwrap(),
                checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            })
            .unwrap();
        assert_eq!(
            sample.checksum(),
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }

    #[test]
    fn test_from_files() {
        let files = vec![
            FileChecksumNode {
                relpath: "arr_0/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksumNode {
                relpath: "arr_0/0".try_into().unwrap(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileChecksumNode {
                relpath: "arr_1/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksumNode {
                relpath: "arr_1/0".try_into().unwrap(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileChecksumNode {
                relpath: ".zgroup".try_into().unwrap(),
                checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let sample = ChecksumTree::from_files(files).unwrap();
        assert_eq!(
            sample.checksum(),
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }
}
