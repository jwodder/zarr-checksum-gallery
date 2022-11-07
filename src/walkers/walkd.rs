use crate::checksum::try_compile_checksum;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::{is_excluded_dotfile, Zarr};
use walkdir::WalkDir;

/// Traverse a Zarr directory tree using [the `walkdir`
/// crate](https://crates.io/crates/walkdir) and checksum it
///
/// This builds an in-memory tree of all file checksums for computing the final
/// Zarr checksum.
pub fn walkdir_checksum(zarr: Zarr) -> Result<String, ChecksumError> {
    try_compile_checksum(
        WalkDir::new(zarr.path.clone())
            .follow_links(true)
            .into_iter()
            .filter_entry(|e| !(zarr.exclude_dotfiles && is_excluded_dotfile(e.path())))
            // We can't use walkdir's filter_entry() below, because that
            // prevents descending into directories that don't match the
            // predicate.  We also can't use r.map() inside the filter(), as
            // that takes ownership of r.
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
