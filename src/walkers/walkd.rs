use crate::checksum::{FileInfo, ZarrEntry};
use std::path::Path;
use walkdir::WalkDir;

// TODO: Return a Result
pub fn walkdir_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    let zarr: Result<ZarrEntry, _> = WalkDir::new(dirpath.as_ref())
        .into_iter()
        // We can't use walkdir's filter_entry(), because that prevents
        // descending into directories that don't match the predicate.
        // We also can't use r.map() inside the filter(), as that takes
        // ownership of r.
        .filter(|r| match r {
            Ok(e) => e.file_type().is_file(),
            Err(_) => true,
        })
        .map(|r| r.map(|e| FileInfo::for_file(e.path(), dirpath.as_ref())))
        .collect();
    match zarr {
        Ok(z) => z.digest().digest,
        Err(e) => panic!("Error walking Zarr: {e}"),
    }
}
