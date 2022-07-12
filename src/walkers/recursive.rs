use crate::checksum::{get_checksum, FileInfo, ZarrDigest};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

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
