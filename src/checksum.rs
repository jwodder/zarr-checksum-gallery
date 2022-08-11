//! Functionality for computing Zarr checksums
//!
//! The checksum for a file is respresented by a [`FileChecksumNode`], which
//! can be obtained via the [`FileChecksumNode::for_file`] function.  The
//! checksum for a directory whose entries have all had their checksums
//! calculated is respresented by a
//! [`DirChecksumNode`][nodes::DirChecksumNode], which can be computed with
//! [`get_checksum()`][nodes::get_checksum].  The checksum for an entire Zarr
//! can then be computed by building up these types, by building up a
//! [`ChecksumTree`] from [`FileChecksumNode`]s, or by using just
//! [`compile_checksum()`] or [`try_compile_checksum()`].
mod json;
pub(crate) mod nodes;
mod tree;
use crate::errors::{ChecksumError, ChecksumTreeError, FSError};
pub use nodes::*;
pub use tree::*;

/// Compute a checksum for a Zarr from an iterator of [`FileChecksumNode`]s for
/// each file within
pub fn compile_checksum<I: IntoIterator<Item = FileChecksumNode>>(
    iter: I,
) -> Result<String, ChecksumTreeError> {
    Ok(ChecksumTree::from_files(iter)?.into_checksum())
}

/// Compute a checksum for a Zarr from an iterator of `Result<FileChecksumNode,
/// FSError>` items
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
