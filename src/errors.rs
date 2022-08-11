//! Error types
use crate::zarr::EntryPath;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use walkdir::Error as WDError;

/// Error type returned when something goes wrong while interacting with the
/// filesystem
#[derive(Debug, Error)]
pub enum FSError {
    /// Returned when an error occurs while trying to compute the MD5 digest of
    /// a filepath
    #[error("Error digesting file: {}: {source}", .path.display())]
    MD5FileError { path: PathBuf, source: io::Error },

    #[error("Path {path:?} is not a normalized & decodable descendant of {basepath:?}")]
    RelativePathError { path: PathBuf, basepath: PathBuf },

    /// Returned when an error occurs while trying to fetch a file's filesystem
    /// metadata
    #[error("Error stat'ing file: {}: {source}", .path.display())]
    StatError { path: PathBuf, source: io::Error },

    /// Returned when an error occurs while trying to list the contents of a
    /// directory
    #[error("Error reading directory: {}: {source}", .path.display())]
    ReaddirError { path: PathBuf, source: io::Error },

    /// Returned when an error occurs while walking a directory with [the
    /// `walkdir` crate](https://crates.io/crates/walkdir)
    #[error("Error walking directory: {source}")]
    WalkdirError {
        #[from]
        source: WDError,
    },

    /// Returned by [`walkdir_checksum()`][crate::walkdir_checksum] when given
    /// a path that does not point to a directory
    #[error("Root path of traversal is not a directory: {}", .path.display())]
    NotDirRootError { path: PathBuf },
}

impl FSError {
    pub(crate) fn md5_file_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        FSError::MD5FileError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub(crate) fn relative_path_error<P: AsRef<Path>>(path: P, basepath: P) -> Self {
        FSError::RelativePathError {
            path: path.as_ref().into(),
            basepath: basepath.as_ref().into(),
        }
    }

    pub(crate) fn stat_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        FSError::StatError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub(crate) fn readdir_error<P: AsRef<Path>>(path: P, source: io::Error) -> Self {
        FSError::ReaddirError {
            path: path.as_ref().into(),
            source,
        }
    }

    pub(crate) fn walkdir_error(e: WDError) -> Self {
        e.into()
    }

    pub(crate) fn not_dir_root_error<P: AsRef<Path>>(path: P) -> Self {
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

/// An enum of [`ChecksumTreeError`] and [`FSError`]
#[derive(Debug, Error)]
pub enum ChecksumError {
    #[error(transparent)]
    ChecksumTreeError(#[from] ChecksumTreeError),
    #[error(transparent)]
    FSError(#[from] FSError),
}

/// Error returned when trying to construct an
/// [`EntryPath`][crate::zarr::EntryPath] from an invalid, unnormalized, or
/// undecodable relative path
#[derive(Debug, Error)]
#[error("Invalid, unnormalized, or undecodable relative path: {0:?}")]
pub struct EntryPathError(pub PathBuf);
