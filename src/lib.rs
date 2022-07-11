pub mod checksum;
pub mod checksum_json;
use crate::checksum::{get_checksum, FileInfo, ZarrDigest, ZarrEntry};
use std::collections::HashMap;
use std::fs;
use std::io;
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

// TODO: Return a Result
pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    fn recurse<P: AsRef<Path>>(path: P, basepath: P) -> Result<ZarrDigest, io::Error> {
        let mut files = HashMap::new();
        let mut directories = HashMap::new();
        for p in fs::read_dir(path)? {
            let p = p?;
            let name = p.file_name().to_str().unwrap().to_string();
            if p.file_type()?.is_dir() {
                directories.insert(name, recurse(&p.path(), &basepath.as_ref().into())?);
            } else {
                files.insert(
                    name,
                    FileInfo::for_file(p.path(), basepath.as_ref().into()).to_zarr_digest(),
                );
            }
        }
        Ok(get_checksum(files, directories))
    }

    recurse(&dirpath, &dirpath).unwrap().digest
}
