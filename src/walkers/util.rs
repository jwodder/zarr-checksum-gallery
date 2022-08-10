use crate::errors::FSError;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct DirEntry {
    pub(crate) path: PathBuf,
    pub(crate) is_dir: bool,
}

pub(crate) fn listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, FSError> {
    let mut entries = Vec::new();
    for p in fs::read_dir(&dirpath).map_err(|e| FSError::readdir_error(&dirpath, e))? {
        let p = p.map_err(|e| FSError::readdir_error(&dirpath, e))?;
        let path = p.path();
        let is_dir = p
            .file_type()
            .map_err(|e| FSError::stat_error(&p.path(), e))?
            .is_dir();
        entries.push(DirEntry { path, is_dir });
    }
    Ok(entries)
}
