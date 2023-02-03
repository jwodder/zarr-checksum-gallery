use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::*;

/// Traverse & checksum a Zarr directory tree recursively
///
/// The checksum for each directory is computed as soon as the checksums for
/// all of its entries are computed.
///
/// The checksumming of the Zarr is performed roughly in accordance with the
/// following pseudocode:
///
/// ```none
/// fn recursive_checksum(dirpath):
///     entry_checksums = new List()
///     for entry in dirpath.listdir():
///         if entry.is_dir():
///             entry_checksums.append(recursive_checksum(entry))
///         else:
///             entry_checksums.append(checksum_file(entry))
///     // This step weeds out checksums for empty directories:
///     return combine_checksums(entry_checksums)
/// ```
pub fn recursive_checksum(zarr: &Zarr) -> Result<String, ChecksumError> {
    Ok(recurse(zarr.root_dir())?.into_checksum())
}

fn recurse(zdir: ZarrDirectory) -> Result<DirChecksum, FSError> {
    let mut nodes: Vec<EntryChecksum> = Vec::new();
    for entry in zdir.entries()? {
        match entry {
            ZarrEntry::File(f) => nodes.push(f.into_checksum()?.into()),
            ZarrEntry::Directory(d) => nodes.push(recurse(d)?.into()),
        }
    }
    Ok(zdir.get_checksum(nodes))
}
