use crate::error::ZarrError;
use std::fs;
use std::path::{Path, PathBuf};

pub struct DirEntry {
    path: PathBuf,
    name: String,
    is_dir: bool,
}

impl DirEntry {
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn is_dir(&self) -> bool {
        self.is_dir
    }
}

pub fn listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, ZarrError> {
    let mut entries = Vec::new();
    for p in fs::read_dir(&dirpath).map_err(|e| ZarrError::readdir_error(&dirpath, e))? {
        let p = p.map_err(|e| ZarrError::readdir_error(&dirpath, e))?;
        let path = p.path();
        // TODO: Add a dedicated ZarrError variant for this failure:
        let name = p.file_name().to_str().unwrap().to_string();
        let is_dir = p
            .file_type()
            .map_err(|e| ZarrError::stat_error(&p.path(), e))?
            .is_dir();
        entries.push(DirEntry { path, name, is_dir });
    }
    Ok(entries)
}
