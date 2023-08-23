//! Functionality for computing Zarr checksums
//!
//! The checksum for a file is respresented by a [`FileChecksum`], which
//! can be obtained via the
//! [`ZarrFile::into_checksum()`][crate::zarr::ZarrFile::into_checksum]
//! function.  The checksum for a directory whose entries have all had their
//! checksums calculated is represented by a [`DirChecksum`], which can be
//! computed with
//! [`ZarrDirectory::get_checksum()`][crate::zarr::ZarrDirectory::get_checksum].
//! The checksum for an entire Zarr can then be computed by building up these
//! types, by building up a [`ChecksumTree`] from [`FileChecksum`]s, or by
//! using just [`compile_checksum()`] or [`try_compile_checksum()`].
mod json;
pub(crate) mod nodes;
mod tree;
use crate::errors::{ChecksumError, ChecksumTreeError, FSError};
pub use nodes::*;
pub use tree::*;

/// Compute a checksum for a Zarr from an iterator of [`FileChecksum`]s for
/// each file within
pub fn compile_checksum<I: IntoIterator<Item = FileChecksum>>(
    iter: I,
) -> Result<String, ChecksumTreeError> {
    Ok(ChecksumTree::from_files(iter)?.into_checksum())
}

/// Compute a checksum for a Zarr from an iterator of `Result<FileChecksum,
/// FSError>` items
pub fn try_compile_checksum<I>(iter: I) -> Result<String, ChecksumError>
where
    I: IntoIterator<Item = Result<FileChecksum, FSError>>,
{
    let mut tree = ChecksumTree::new();
    for node in iter {
        tree.add_file(node?)?;
    }
    Ok(tree.into_checksum())
}
