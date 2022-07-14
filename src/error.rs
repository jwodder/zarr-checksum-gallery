use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use walkdir::Error as WDError;

#[derive(Debug, Error)]
pub enum ZarrError {
    #[error("Error digesting file: {}: {source:?}", .path.display())]
    MD5FileError { path: PathBuf, source: io::Error },

    #[error("Path {path:?} is not a descendant of {basepath:?}")]
    StripPrefixError { path: PathBuf, basepath: PathBuf },

    #[error("Error stat'ing file: {}: {source:?}", .path.display())]
    StatError { path: PathBuf, source: io::Error },

    #[error("Error reading directory: {}: {source:?}", .path.display())]
    ReaddirError { path: PathBuf, source: io::Error },

    #[error("Error walking directory: {source}")]
    WalkdirError {
        #[from]
        source: WDError,
    },
}

impl ZarrError {
    pub fn md5_file_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        ZarrError::MD5FileError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn strip_prefix_error<P: AsRef<Path>>(path: P, basepath: P) -> Self {
        ZarrError::StripPrefixError {
            path: path.as_ref().into(),
            basepath: basepath.as_ref().into(),
        }
    }

    pub fn stat_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        ZarrError::StatError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn readdir_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        ZarrError::ReaddirError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn walkdir_error(e: WDError) -> Self {
        e.into()
    }
}
