use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use walkdir::Error as WDError;

#[derive(Debug, Error)]
pub enum WalkError {
    #[error("Error digesting file: {}: {source}", .path.display())]
    MD5FileError { path: PathBuf, source: io::Error },

    #[error("Path {path:?} is not a descendant of {basepath:?}")]
    StripPrefixError { path: PathBuf, basepath: PathBuf },

    #[error("Error stat'ing file: {}: {source}", .path.display())]
    StatError { path: PathBuf, source: io::Error },

    #[error("Error reading directory: {}: {source}", .path.display())]
    ReaddirError { path: PathBuf, source: io::Error },

    #[error("Error walking directory: {source}")]
    WalkdirError {
        #[from]
        source: WDError,
    },

    #[error("Could not decode filename {filename:?}")]
    // TODO: Should this include the path of the containing directory?
    FilenameDecodeError { filename: OsString },

    #[error("Root path of traversal is not a directory: {}", .path.display())]
    NotDirRootError { path: PathBuf },
}

impl WalkError {
    pub fn md5_file_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        WalkError::MD5FileError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn strip_prefix_error<P: AsRef<Path>>(path: P, basepath: P) -> Self {
        WalkError::StripPrefixError {
            path: path.as_ref().into(),
            basepath: basepath.as_ref().into(),
        }
    }

    pub fn stat_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        WalkError::StatError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn readdir_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        WalkError::ReaddirError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub fn walkdir_error(e: WDError) -> Self {
        e.into()
    }

    pub fn filename_decode_error(filename: OsString) -> Self {
        WalkError::FilenameDecodeError { filename }
    }

    pub fn not_dir_root_error<P: AsRef<Path>>(path: P) -> Self {
        WalkError::NotDirRootError {
            path: path.as_ref().into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ChecksumTreeError {
    #[error("Could not decode path {path:?}")]
    PathDecodeError { path: PathBuf },

    #[error("Invalid relative path {path:?}")]
    InvalidPath { path: PathBuf },

    #[error("Path type conflict error for {path:?}")]
    PathTypeConflict { path: PathBuf },

    #[error("File {path:?} added to checksum tree twice")]
    DoubleAdd { path: PathBuf },
}

impl ChecksumTreeError {
    pub fn path_decode_error<P: AsRef<Path>>(path: P) -> Self {
        ChecksumTreeError::PathDecodeError {
            path: path.as_ref().into(),
        }
    }

    pub fn invalid_path<P: AsRef<Path>>(path: P) -> Self {
        ChecksumTreeError::InvalidPath {
            path: path.as_ref().into(),
        }
    }

    pub fn path_type_conflict<P: AsRef<Path>>(path: P) -> Self {
        ChecksumTreeError::PathTypeConflict {
            path: path.as_ref().into(),
        }
    }

    pub fn double_add<P: AsRef<Path>>(path: P) -> Self {
        ChecksumTreeError::DoubleAdd {
            path: path.as_ref().into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ChecksumError {
    #[error(transparent)]
    ChecksumTreeError(#[from] ChecksumTreeError),
    #[error(transparent)]
    WalkError(#[from] WalkError),
}
