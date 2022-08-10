mod json;
pub mod nodes;
pub mod tree;
use self::nodes::FileChecksumNode;
use self::tree::ChecksumTree;
use crate::errors::{ChecksumError, ChecksumTreeError, FSError};

pub fn compile_checksum<I: IntoIterator<Item = FileChecksumNode>>(
    iter: I,
) -> Result<String, ChecksumTreeError> {
    Ok(ChecksumTree::from_files(iter)?.into_checksum())
}

pub fn try_compile_checksum<I>(iter: I) -> Result<String, ChecksumError>
where
    I: IntoIterator<Item = Result<FileChecksumNode, FSError>>,
{
    let mut tree = ChecksumTree::new();
    for node in iter {
        tree.add_file(node?)?;
    }
    Ok(tree.into_checksum())
}
