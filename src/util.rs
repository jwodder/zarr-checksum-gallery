use crate::errors::WalkError;
use md5::{Digest, Md5};
use relative_path::RelativePathBuf;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn md5_string(s: &str) -> String {
    hex::encode(Md5::digest(s))
}

pub(crate) fn md5_file<P: AsRef<Path>>(path: P) -> Result<String, WalkError> {
    let mut file = fs::File::open(&path).map_err(|e| WalkError::md5_file_error(&path, e))?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher).map_err(|e| WalkError::md5_file_error(&path, e))?;
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn relative_to<P, Q>(path: P, basepath: Q) -> Result<RelativePathBuf, WalkError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let path = path.as_ref();
    let basepath = basepath.as_ref();
    let relpath = path
        .strip_prefix(PathBuf::from(basepath))
        .map_err(|_| WalkError::strip_prefix_error(&path, &basepath))?;
    // TODO: Verify that (utf8)relpath is entirely composed of normal
    // components
    if relpath.file_name().is_none() {
        return Err(WalkError::strip_prefix_error(path, basepath));
    }
    // Should we assert that this only ever fails with kind NonUtf8?
    RelativePathBuf::from_path(relpath).map_err(|_| WalkError::path_decode_error(&relpath))
}
