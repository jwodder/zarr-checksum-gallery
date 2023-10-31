//! Error types
use crate::zarr::EntryPath;
use std::path::PathBuf;
use thiserror::Error;

/// Error returned when something goes wrong while interacting with the
/// filesystem
#[derive(Debug, Error)]
pub enum FSError {
    /// Returned when an error occurs while trying to compute the MD5 digest of
    /// a filepath
    #[error("failed to digest contents of {}", .path.display())]
    Digest {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("final component of path {path:?} is not valid UTF-8")]
    UndecodableName { path: PathBuf },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Error for failure to construct a
/// [`ChecksumTree`][crate::checksum::ChecksumTree] due to invalid input
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ChecksumTreeError {
    /// Returned when a node would be added to a `ChecksumTree` in which a
    /// parent path of the node is already present as a file
    #[error("path type conflict error for {path:?}")]
    PathTypeConflict {
        /// The path of the node that would have been added
        path: EntryPath,
    },

    /// Returned when a node would be added to a `ChecksumTree` which already
    /// contains a file or directory at the node's path
    #[error("file {path:?} added to checksum tree twice")]
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

/// Error returned when trying to construct an [`EntryPath`] from an invalid,
/// unnormalized, or undecodable relative path
///
/// The error contains the invalid path in question as a [`PathBuf`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid, unnormalized, or undecodable relative path: {0:?}")]
pub struct EntryPathError(pub PathBuf);

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid path name: {0:?}")]
pub struct EntryNameError(pub String);
