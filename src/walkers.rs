//! Various implementations of directory traversal & checksumming
//!
//! Each directory checksumming function returns either `Ok(String)`,
//! containing the Zarr checksum for the specified Zarr, or
//! `Err(ChecksumError)`, which can wrap either an
//! [`FSError`][crate::errors::FSError] or a
//! [`ChecksumTreeError`][crate::errors::ChecksumError].  The latter error type
//! indicates a bug in the traversal function.
mod breadth_first;
mod collapsio_arc;
mod collapsio_mpsc;
mod depth_first;
mod fastasync;
mod fastio;
mod jobstack;
mod recursive;
mod util;
pub use breadth_first::*;
pub use collapsio_arc::*;
pub use collapsio_mpsc::*;
pub use depth_first::*;
pub use fastasync::*;
pub use fastio::*;
pub use recursive::*;
