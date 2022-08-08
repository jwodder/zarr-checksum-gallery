use super::util::{listdir, DirEntry};
use crate::checksum::{get_checksum, FileInfo, ZarrChecksum};
use crate::errors::{ChecksumError, WalkError};
use std::collections::HashMap;
use std::path::Path;

pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref().to_path_buf();
    Ok(recurse(&dirpath, &dirpath)?.checksum)
}

fn recurse(path: &Path, basepath: &Path) -> Result<ZarrChecksum, WalkError> {
    let entries = listdir(path)?;
    let (files, directories): (Vec<_>, Vec<_>) = entries.into_iter().partition(|e| !e.is_dir);
    let files = files
        .into_iter()
        .map(|DirEntry { path, name, .. }| {
            FileInfo::for_file(path, basepath).map(|info| (name, ZarrChecksum::from(info)))
        })
        .collect::<Result<HashMap<String, ZarrChecksum>, WalkError>>()?;
    let directories = directories
        .into_iter()
        .map(|DirEntry { path, name, .. }| recurse(&path, basepath).map(|dgst| (name, dgst)))
        .collect::<Result<HashMap<String, ZarrChecksum>, WalkError>>()?;
    Ok(get_checksum(files, directories))
}
