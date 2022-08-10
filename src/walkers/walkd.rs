use crate::checksum::{nodes::FileChecksumNode, try_compile_checksum};
use crate::errors::{ChecksumError, FSError};
use std::fs::metadata;
use std::path::Path;
use walkdir::WalkDir;

pub fn walkdir_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref();
    // Without this check, walkdir will return only the file `dirpath`, leading
    // to an empty relative path error
    if !metadata(&dirpath)
        .map_err(|e| FSError::stat_error(&dirpath, e))?
        .is_dir()
    {
        return Err(FSError::not_dir_root_error(dirpath).into());
    }
    try_compile_checksum(
        WalkDir::new(dirpath)
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
                    |exc| Err(FSError::walkdir_error(exc)),
                    |e| FileChecksumNode::for_file(e.path(), dirpath),
                )
            }),
    )
}
