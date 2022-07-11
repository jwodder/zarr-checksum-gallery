pub mod checksum;
pub mod checksum_json;
use crate::checksum::{FileInfo, ZarrEntry};
use std::path::Path;
use walkdir::WalkDir;

pub fn walkdir_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    let zarr: Result<ZarrEntry, _> = WalkDir::new(dirpath.as_ref())
        .into_iter()
        .filter_entry(|e| e.file_type().is_file())
        .map(|r| r.map(|e| FileInfo::for_file(e.path(), dirpath.as_ref())))
        .collect();
    match zarr {
        Ok(z) => z.digest().digest,
        Err(e) => panic!("Error walking Zarr: {e}"),
    }
}
