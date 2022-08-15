//! Various implementations of directory traversal & checksumming
//!
//! Each directory checksumming function returns either `Ok(String)`,
//! containing the Zarr checksum for the specified directory, or
//! `Err(ChecksumError)`, which can wrap either an
//! [`FSError`][crate::errors::FSError] or a
//! [`ChecksumTreeError`][crate::errors::ChecksumError].  The latter error type
//! indicates a bug in the traversal function.
mod breadth_first;
mod collapsio;
mod depth_first;
mod fastasync;
mod fastio;
mod jobstack;
mod recursive;
mod walkd;
pub use breadth_first::*;
pub use collapsio::*;
pub use depth_first::*;
pub use fastasync::*;
pub use fastio::*;
pub use recursive::*;
pub use walkd::*;
