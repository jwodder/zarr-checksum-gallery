use crate::checksum::try_compile_checksum;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::Zarr;
use std::path::Path;
use walkdir::WalkDir;

/// Traverse a directory tree using [the `walkdir`
/// crate](https://crates.io/crates/walkdir) and checksum it
///
/// This builds an in-memory tree of all file checksums for computing the final
/// Zarr checksum.
///
/// If `dirpath` is not a directory, this will return an
/// [`FSError::NotDirRootError`] immediately.  This is unlike the other
/// walkers, which return an [`FSError::ReaddirError`] in such a situation.
pub fn walkdir_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref();
    let zarr = Zarr::new(dirpath)?;
    try_compile_checksum(
        WalkDir::new(dirpath)
            .follow_links(true)
            .into_iter()
            // We can't use walkdir's filter_entry(), because that prevents
            // descending into directories that don't match the predicate.
            // We also can't use r.map() inside the filter(), as that takes
            // ownership of r.
            .filter(|r| match r {
                Ok(e) => !e.file_type().is_dir(),
                Err(_) => true,
            })
            .map(|r| {
                r.map_or_else(
                    |exc| Err(FSError::walkdir_error(exc)),
                    |e| zarr.checksum_file(e.path()),
                )
            }),
    )
}
