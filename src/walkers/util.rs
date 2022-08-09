use crate::errors::WalkError;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct DirEntry {
    pub(crate) path: PathBuf,
    pub(crate) name: String,
    pub(crate) is_dir: bool,
}

pub(crate) fn listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, WalkError> {
    let mut entries = Vec::new();
    for p in fs::read_dir(&dirpath).map_err(|e| WalkError::readdir_error(&dirpath, e))? {
        let p = p.map_err(|e| WalkError::readdir_error(&dirpath, e))?;
        let path = p.path();
        let name = decode_filename(p.file_name())?;
        let is_dir = p
            .file_type()
            .map_err(|e| WalkError::stat_error(&p.path(), e))?
            .is_dir();
        entries.push(DirEntry { path, name, is_dir });
    }
    Ok(entries)
}

pub(crate) fn decode_filename(name: OsString) -> Result<String, WalkError> {
    // TODO: Should the path to the containing directory be included in the
    // error?
    name.into_string().map_err(WalkError::path_decode_error)
}
