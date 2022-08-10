use crate::zarr::EntryPath;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use walkdir::Error as WDError;

#[derive(Debug, Error)]
pub enum FSError {
    #[error("Error digesting file: {}: {source}", .path.display())]
    MD5FileError { path: PathBuf, source: io::Error },

    #[error("Path {path:?} is not a normalized & decodable descendant of {basepath:?}")]
    RelativePathError { path: PathBuf, basepath: PathBuf },

    #[error("Error stat'ing file: {}: {source}", .path.display())]
    StatError { path: PathBuf, source: io::Error },

    #[error("Error reading directory: {}: {source}", .path.display())]
    ReaddirError { path: PathBuf, source: io::Error },

    #[error("Error walking directory: {source}")]
    WalkdirError {
        #[from]
        source: WDError,
    },

    #[error("Root path of traversal is not a directory: {}", .path.display())]
    NotDirRootError { path: PathBuf },
}

impl FSError {
    pub fn md5_file_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        FSError::MD5FileError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn relative_path_error<P: AsRef<Path>>(path: P, basepath: P) -> Self {
        FSError::RelativePathError {
            path: path.as_ref().into(),
            basepath: basepath.as_ref().into(),
        }
    }

    pub fn stat_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        FSError::StatError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn readdir_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        FSError::ReaddirError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn walkdir_error(e: WDError) -> Self {
        e.into()
    }

    pub fn not_dir_root_error<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotDirRootError {
            path: path.as_ref().into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ChecksumTreeError {
    #[error("Path type conflict error for {path:?}")]
    PathTypeConflict { path: EntryPath },

    #[error("File {path:?} added to checksum tree twice")]
    DoubleAdd { path: EntryPath },
}

#[derive(Debug, Error)]
pub enum ChecksumError {
    #[error(transparent)]
    ChecksumTreeError(#[from] ChecksumTreeError),
    #[error(transparent)]
    FSError(#[from] FSError),
}

#[derive(Debug, Error)]
#[error("Invalid, unnormalized, or undecodable relative path: {0:?}")]
pub struct EntryPathError(pub PathBuf);
