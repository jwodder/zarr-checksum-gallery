use super::util::{listdir, DirEntry};
use crate::checksum::nodes::*;
use crate::errors::{ChecksumError, FSError};
use crate::zarr::relative_to;
use std::path::Path;

pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> Result<String, ChecksumError> {
    let dirpath = dirpath.as_ref().to_path_buf();
    Ok(recurse(&dirpath, &dirpath)?.into_checksum())
}

fn recurse(path: &Path, basepath: &Path) -> Result<DirChecksumNode, FSError> {
    let relpath = if path == basepath {
        "<root>".try_into().unwrap()
    } else {
        relative_to(path, basepath)?
    };
    let entries = listdir(path)?;
    let (files, directories): (Vec<_>, Vec<_>) = entries.into_iter().partition(|e| !e.is_dir);
    let nodes = files
        .into_iter()
        .map(|DirEntry { path, .. }| {
            FileChecksumNode::for_file(path, basepath).map(ZarrChecksumNode::from)
        })
        .chain(
            directories
                .into_iter()
                .map(|DirEntry { path, .. }| recurse(&path, basepath).map(ZarrChecksumNode::from)),
        )
        .collect::<Result<Vec<ZarrChecksumNode>, FSError>>()?;
    Ok(get_checksum(relpath, nodes))
}
