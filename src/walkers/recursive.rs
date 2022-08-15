use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;
use std::path::Path;

/// Traverse & checksum a directory tree recursively
///
/// The checksum for each directory is computed as soon as the checksums for
/// all of its entries are computed.
pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let zarr = Zarr::new(dirpath)?;
    Ok(recurse(zarr.root_dir())?.into_checksum())
}

fn recurse(zdir: ZarrDirectory) -> Result<DirChecksumNode, FSError> {
    let mut nodes: Vec<ZarrChecksumNode> = Vec::new();
    for entry in zdir.entries()? {
        match entry {
            ZarrEntry::File(f) => nodes.push(f.into_checksum()?.into()),
            ZarrEntry::Directory(d) => nodes.push(recurse(d)?.into()),
        }
    }
    Ok(zdir.get_checksum(nodes))
}
