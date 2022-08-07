use super::listdir::listdir;
use crate::checksum::{get_checksum, FileInfo, ZarrChecksum};
use crate::error::ZarrError;
use std::collections::HashMap;
use std::path::Path;

pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ZarrError> {
    let dirpath = dirpath.as_ref().to_path_buf();
    Ok(recurse(&dirpath, &dirpath)?.checksum)
}

fn recurse(path: &Path, basepath: &Path) -> Result<ZarrChecksum, ZarrError> {
    let entries = listdir(path)?;
    let (files, directories): (Vec<_>, Vec<_>) = entries.into_iter().partition(|e| !e.is_dir());
    let files = files
        .into_iter()
        .map(|e| {
            FileInfo::for_file(e.path(), basepath.into())
                .map(|info| (e.name(), info.to_zarr_checksum()))
        })
        .collect::<Result<HashMap<String, ZarrChecksum>, ZarrError>>()?;
    let directories = directories
        .into_iter()
        .map(|e| recurse(&e.path(), basepath).map(|dgst| (e.name(), dgst)))
        .filter(|r| match r {
            Ok((_, dgst)) => dgst.file_count != 0,
            Err(_) => true,
        })
        .collect::<Result<HashMap<String, ZarrChecksum>, ZarrError>>()?;
    Ok(get_checksum(files, directories))
}
