use crate::checksum::{try_compile_checksum, FileInfo};
use crate::error::ZarrError;
use std::path::Path;
use walkdir::WalkDir;

pub fn walkdir_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ZarrError> {
    try_compile_checksum(
        WalkDir::new(dirpath.as_ref())
            .into_iter()
            // We can't use walkdir's filter_entry(), because that prevents
            // descending into directories that don't match the predicate.
            // We also can't use r.map() inside the filter(), as that takes
            // ownership of r.
            .filter(|r| match r {
                Ok(e) => e.file_type().is_file(),
                Err(_) => true,
            })
            .map(|r| {
                r.map_or_else(
                    |exc| Err(ZarrError::walkdir_error(exc)),
                    |e| FileInfo::for_file(e.path(), dirpath.as_ref()),
                )
            }),
    )
}
