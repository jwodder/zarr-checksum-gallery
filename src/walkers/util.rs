use crate::error::ZarrError;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) struct DirEntry {
    pub(crate) path: PathBuf,
    pub(crate) name: String,
    pub(crate) is_dir: bool,
}

pub(crate) fn listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, ZarrError> {
    let mut entries = Vec::new();
    for p in fs::read_dir(&dirpath).map_err(|e| ZarrError::readdir_error(&dirpath, e))? {
        let p = p.map_err(|e| ZarrError::readdir_error(&dirpath, e))?;
        let path = p.path();
        let name = decode_filename(p.file_name())?;
        let is_dir = p
            .file_type()
            .map_err(|e| ZarrError::stat_error(&p.path(), e))?
            .is_dir();
        entries.push(DirEntry { path, name, is_dir });
    }
    Ok(entries)
}

pub(crate) fn decode_filename(name: OsString) -> Result<String, ZarrError> {
    name.into_string().map_err(ZarrError::filename_decode_error)
}
