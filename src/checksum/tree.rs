use super::nodes::*;
use crate::errors::ChecksumTreeError;
use crate::zarr::EntryPath;
use std::cell::RefCell;
use std::collections::{hash_map::Entry, HashMap};

/// A tree of [`FileChecksum`]s, for computing the final checksum for an entire
/// Zarr one file at a time
///
/// A `ChecksumTree` can be built up by creating an instance with
/// [`ChecksumTree::new`] and then adding [`FileChecksum`]s one at a time with
/// [`add_file()`][ChecksumTree::add_file], after which the final checksum can
/// be retrieved with [`checksum()`][ChecksumTree::checksum] or
/// [`into_checksum()`][ChecksumTree::into_checksum].  Alternatively, these
/// steps can be done all at once by calling [`ChecksumTree::from_files`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumTree(DirTree);

#[derive(Clone, Debug)]
struct DirTree {
    relpath: EntryPath,
    children: HashMap<String, TreeNode>,
    checksum_cache: RefCell<Option<DirChecksum>>,
}

impl PartialEq for DirTree {
    fn eq(&self, other: &DirTree) -> bool {
        // Don't compare checksum_cache
        (&self.relpath, &self.children) == (&other.relpath, &other.children)
    }
}

impl Eq for DirTree {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TreeNode {
    File(FileChecksum),
    Directory(DirTree),
}

impl ChecksumTree {
    /// Create a new `ChecksumTree`
    pub fn new() -> Self {
        ChecksumTree(DirTree::new("<root>".try_into().unwrap()))
    }

    /// Compute the Zarr checksum for the entire tree
    pub fn checksum(&self) -> String {
        self.0.to_checksum().into_checksum()
    }

    /// Consume the tree and return its Zarr checksum
    pub fn into_checksum(self) -> String {
        DirChecksum::from(self.0).into_checksum()
    }

    /// Add the checksum for a file to the tree
    pub fn add_file(&mut self, node: FileChecksum) -> Result<(), ChecksumTreeError> {
        let mut d = &mut self.0.children;
        let nodepath = node.relpath().clone();
        for parent in nodepath.parents() {
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
        match d.entry(nodepath.file_name().to_string()) {
            Entry::Occupied(_) => return Err(ChecksumTreeError::DoubleAdd { path: nodepath }),
            Entry::Vacant(v) => {
                v.insert(TreeNode::File(node));
            }
        }
        // TODO: Try to merge this into the loop above:
        let mut dt = &self.0;
        dt.clear_cache();
        for parent in nodepath.parents() {
            match dt.children.get(parent.file_name()) {
                None => panic!("Directory suddenly disappeared"),
                Some(TreeNode::File(_)) => panic!("Directory suddenly turned into a File"),
                Some(TreeNode::Directory(dt2)) => {
                    dt = dt2;
                    dt.clear_cache();
                }
            }
        }
        Ok(())
    }

    /// Construct a new `ChecksumTree` from an iterator of
    /// [`FileChecksum`]s
    pub fn from_files<I: IntoIterator<Item = FileChecksum>>(
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
            checksum_cache: RefCell::new(None),
        }
    }

    fn to_checksum(&self) -> DirChecksum {
        self.checksum_cache
            .borrow_mut()
            .get_or_insert_with(|| {
                get_checksum(
                    self.relpath.clone(),
                    self.children.values().map(TreeNode::to_checksum),
                )
            })
            .clone()
    }

    fn clear_cache(&self) {
        _ = self.checksum_cache.borrow_mut().take();
    }
}

impl From<DirTree> for DirChecksum {
    fn from(dirtree: DirTree) -> DirChecksum {
        dirtree.checksum_cache.take().unwrap_or_else(|| {
            get_checksum(
                dirtree.relpath,
                dirtree.children.into_values().map(EntryChecksum::from),
            )
        })
    }
}

impl TreeNode {
    fn directory(relpath: EntryPath) -> Self {
        TreeNode::Directory(DirTree::new(relpath))
    }

    fn to_checksum(&self) -> EntryChecksum {
        match self {
            TreeNode::File(node) => node.clone().into(),
            TreeNode::Directory(dirtree) => dirtree.to_checksum().into(),
        }
    }
}

impl From<TreeNode> for EntryChecksum {
    fn from(node: TreeNode) -> EntryChecksum {
        match node {
            TreeNode::File(node) => node.into(),
            TreeNode::Directory(dirtree) => DirChecksum::from(dirtree).into(),
        }
    }
}

impl From<ChecksumTree> for termtree::Tree<String> {
    fn from(chktree: ChecksumTree) -> termtree::Tree<String> {
        let chksum = chktree.checksum();
        let mut children = chktree.0.children.into_iter().collect::<Vec<_>>();
        children.sort_by_key(|(k, _)| k.clone());
        termtree::Tree::new(chksum).with_leaves(children.into_iter().map(|(_, v)| v))
    }
}

impl From<TreeNode> for termtree::Tree<String> {
    fn from(node: TreeNode) -> termtree::Tree<String> {
        match node {
            TreeNode::File(f) => termtree::Tree::new(format!("{} = {}", f.name(), f.checksum())),
            TreeNode::Directory(d) => {
                let chksum = d.to_checksum();
                let mut children = d.children.into_iter().collect::<Vec<_>>();
                children.sort_by_key(|(k, _)| k.clone());
                termtree::Tree::new(format!("{}/ = {}", chksum.name(), chksum.checksum()))
                    .with_leaves(children.into_iter().map(|(_, v)| v))
            }
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
            .add_file(FileChecksum {
                relpath: "arr_0/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            })
            .unwrap();
        sample
            .add_file(FileChecksum {
                relpath: "arr_0/0".try_into().unwrap(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            })
            .unwrap();
        sample
            .add_file(FileChecksum {
                relpath: "arr_1/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            })
            .unwrap();
        sample
            .add_file(FileChecksum {
                relpath: "arr_1/0".try_into().unwrap(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            })
            .unwrap();
        sample
            .add_file(FileChecksum {
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
            FileChecksum {
                relpath: "arr_0/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksum {
                relpath: "arr_0/0".try_into().unwrap(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileChecksum {
                relpath: "arr_1/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksum {
                relpath: "arr_1/0".try_into().unwrap(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileChecksum {
                relpath: ".zgroup".try_into().unwrap(),
                checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let mut sample = ChecksumTree::from_files(files).unwrap();
        assert_eq!(
            sample.checksum(),
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
        sample
            .add_file(FileChecksum {
                relpath: "arr_0/1".try_into().unwrap(),
                checksum: "d41d8cd98f00b204e9800998ecf8427e".into(),
                size: 0,
            })
            .unwrap();
        assert_eq!(
            sample.checksum(),
            "46bf6cacf13e20cd09eda687e367af3a-6--1516",
        );
    }

    #[test]
    fn test_dirtree_eq() {
        let a = DirTree {
            relpath: "foo/bar/baz".try_into().unwrap(),
            children: HashMap::from([(
                "glarch".into(),
                TreeNode::File(FileChecksum {
                    relpath: "foo/bar/baz/glarch".try_into().unwrap(),
                    checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                    size: 24,
                }),
            )]),
            checksum_cache: RefCell::new(None),
        };
        let b = DirTree {
            relpath: "foo/bar/baz".try_into().unwrap(),
            children: HashMap::from([(
                "glarch".into(),
                TreeNode::File(FileChecksum {
                    relpath: "foo/bar/baz/glarch".try_into().unwrap(),
                    checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                    size: 24,
                }),
            )]),
            checksum_cache: RefCell::new(Some(DirChecksum {
                relpath: "foo/bar/baz".try_into().unwrap(),
                checksum: "2606add1822870a6d0f892da6503e720-1--24".into(),
                size: 24,
                file_count: 1,
            })),
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_draw_tree() {
        let files = vec![
            FileChecksum {
                relpath: "arr_0/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksum {
                relpath: "arr_0/0".try_into().unwrap(),
                checksum: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileChecksum {
                relpath: "arr_1/.zarray".try_into().unwrap(),
                checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileChecksum {
                relpath: "arr_1/0".try_into().unwrap(),
                checksum: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileChecksum {
                relpath: ".zgroup".try_into().unwrap(),
                checksum: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let sample = ChecksumTree::from_files(files).unwrap();
        let drawing = termtree::Tree::from(sample).to_string();
        assert_eq!(
            drawing,
            concat!(
                "4313ab36412db2981c3ed391b38604d6-5--1516\n",
                "├── .zgroup = e20297935e73dd0154104d4ea53040ab\n",
                "├── arr_0/ = 51c74ec257069ce3a555bdddeb50230a-2--746\n",
                "│   ├── .zarray = 9e30a0a1a465e24220d4132fdd544634\n",
                "│   └── 0 = ed4e934a474f1d2096846c6248f18c00\n",
                "└── arr_1/ = 7b99a0ad9bd8bb3331657e54755b1a31-2--746\n",
                "    ├── .zarray = 9e30a0a1a465e24220d4132fdd544634\n",
                "    └── 0 = fba4dee03a51bde314e9713b00284a93\n",
            )
        );
    }

    #[test]
    fn test_draw_deeper_tree() {
        let files = vec![FileChecksum {
            relpath: "foo/bar/baz/quux.dat".try_into().unwrap(),
            checksum: "9e30a0a1a465e24220d4132fdd544634".into(),
            size: 315,
        }];
        let sample = ChecksumTree::from_files(files).unwrap();
        let drawing = termtree::Tree::from(sample).to_string();
        assert_eq!(
            drawing,
            concat!(
                "2dc73d60f44b42c168b0e0dc81aa44b8-1--315\n",
                "└── foo/ = 348db3d80ccdd9a74e792593760b0070-1--315\n",
                "    └── bar/ = 6b59406727cc70a04ae099b4fa4b8fea-1--315\n",
                "        └── baz/ = 8c5ad66123476879b63bc830f36c6baa-1--315\n",
                "            └── quux.dat = 9e30a0a1a465e24220d4132fdd544634\n",
            )
        );
    }
}
