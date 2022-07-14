use crate::checksum::{get_checksum, FileInfo, ZarrDigest};
use crate::error::ZarrError;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ZarrError> {
    fn recurse<P: AsRef<Path>>(path: P, basepath: P) -> Result<ZarrDigest, ZarrError> {
        let mut files = HashMap::new();
        let mut directories = HashMap::new();
        for p in fs::read_dir(&path).map_err(|e| ZarrError::readdir_error(&path, e))? {
            let p = p.map_err(|e| ZarrError::readdir_error(&path, e))?;
            let name = p.file_name().to_str().unwrap().to_string();
            if p.file_type()
                .map_err(|e| ZarrError::stat_error(&p.path(), e))?
                .is_dir()
            {
                directories.insert(name, recurse(&p.path(), &basepath.as_ref().into())?);
            } else {
                files.insert(
                    name,
                    FileInfo::for_file(p.path(), basepath.as_ref().into())?.to_zarr_digest(),
                );
            }
        }
        Ok(get_checksum(files, directories))
    }

    Ok(recurse(&dirpath, &dirpath)?.digest)
}
