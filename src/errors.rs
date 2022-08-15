//! Error types
use crate::zarr::EntryPath;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Error returned when something goes wrong while interacting with the
/// filesystem
#[derive(Debug, Error)]
pub enum FSError {
    /// Returned when an error occurs while trying to compute the MD5 digest of
    /// a filepath
    #[error("Error digesting file: {}: {source}", .path.display())]
    MD5FileError { path: PathBuf, source: io::Error },

    /// Returned when an attempt is made to compute `path` relative to
    /// `basepath` but the former is not a valid, normalized, UTF-8 decodable
    /// descendant of the latter
    #[error("Path {path:?} is not a normalized & decodable descendant of {basepath:?}")]
    RelativePathError { path: PathBuf, basepath: PathBuf },

    #[error("Final componenet of path {path:?} is not valid UTF-8")]
    UndecodableName { path: PathBuf },

    /// Returned when an error occurs while trying to fetch a path's filesystem
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
        source: walkdir::Error,
    },

    /// Returned by a walker when given a path that does not point to a
    /// directory
    #[error("Root path of traversal is not a directory: {}", .path.display())]
    NotDirRoot { path: PathBuf },
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

    pub(crate) fn undecodable_name<P: AsRef<Path>>(path: P) -> Self {
        FSError::UndecodableName {
            path: path.as_ref().into(),
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

    pub(crate) fn walkdir_error(e: walkdir::Error) -> Self {
        e.into()
    }

    pub(crate) fn not_dir_root<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotDirRoot {
            path: path.as_ref().into(),
        }
    }
}

/// Error for failure to construct a
/// [`ChecksumTree`][crate::checksum::ChecksumTree] due to invalid input
#[derive(Debug, Error)]
pub enum ChecksumTreeError {
    /// Returned when a node would be added to a `ChecksumTree` in which a
    /// parent path of the node is already present as a file
    #[error("Path type conflict error for {path:?}")]
    PathTypeConflict {
        /// The path of the node that would have been added
        path: EntryPath,
    },

    /// Returned when a node would be added to a `ChecksumTree` which already
    /// contains a file or directory at the node's path
    #[error("File {path:?} added to checksum tree twice")]
    DoubleAdd {
        /// The path of the node that would have been added
        path: EntryPath,
    },
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
///
/// The error contains the invalid path in question as a [`PathBuf`].
#[derive(Debug, Error)]
#[error("Invalid, unnormalized, or undecodable relative path: {0:?}")]
pub struct EntryPathError(pub PathBuf);

#[derive(Debug, Error)]
#[error("Invalid path name: {0:?}")]
pub struct EntryNameError(pub String);
