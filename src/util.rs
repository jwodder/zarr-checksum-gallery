use crate::errors::FSError;
use md5::{Digest, Md5};
use relative_path::{Component, RelativePathBuf};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn md5_string(s: &str) -> String {
    hex::encode(Md5::digest(s))
}

pub(crate) fn md5_file<P: AsRef<Path>>(path: P) -> Result<String, FSError> {
    let mut file = fs::File::open(&path).map_err(|e| FSError::md5_file_error(&path, e))?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher).map_err(|e| FSError::md5_file_error(&path, e))?;
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn relative_to<P, Q>(path: P, basepath: Q) -> Result<RelativePathBuf, FSError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let path = path.as_ref();
    let basepath = basepath.as_ref();
    let relpath = path
        .strip_prefix(PathBuf::from(basepath))
        .map_err(|_| FSError::strip_prefix_error(&path, &basepath))?;
    let mut normpath = RelativePathBuf::new();
    // Should we assert that this only ever fails with kind NonUtf8?
    for c in RelativePathBuf::from_path(relpath)
        .map_err(|_| FSError::path_decode_error(&relpath))?
        .components()
    {
        match c {
            Component::Normal(part) => normpath.push(part),
            _ => return Err(FSError::strip_prefix_error(path, basepath)),
        }
    }
    if normpath.file_name().is_none() {
        return Err(FSError::strip_prefix_error(path, basepath));
    }
    Ok(normpath)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("foo/bar/baz", "foo/bar", "baz")]
    #[case("foo/bar/./baz", "foo/bar", "baz")]
    #[case("foo/bar//baz", "foo/bar", "baz")]
    #[case("foo/bar/baz/", "foo/bar", "baz")]
    #[case("foo/bar/baz//", "foo/bar", "baz")]
    #[case("foo/bar/baz/.", "foo/bar", "baz")]
    #[case("foo/bar/baz/./quux", "foo/bar", "baz/quux")]
    #[case("foo/bar/baz/quux/gnusto", "foo/bar", "baz/quux/gnusto")]
    #[case("foo/bar/baz//quux/gnusto", "foo/bar", "baz/quux/gnusto")]
    fn test_relative_to(#[case] path: &str, #[case] basepath: &str, #[case] relpath: &str) {
        assert_eq!(relative_to(path, basepath).unwrap(), relpath);
    }

    #[rstest]
    #[case("baz", "foo/bar")]
    #[case("/foo/bar/baz", "foo/bar")]
    #[case("foo/bar/baz", "/foo/bar")]
    #[case("foo/bar", "foo/bar")]
    #[case("foo/bar/", "foo/bar")]
    #[case("foo/bar/.", "foo/bar")]
    #[case("foo/bar/..", "foo/bar")]
    #[case("foo/bar/baz/..", "foo/bar")]
    #[case("foo/bar/../baz", "foo/bar")]
    fn test_relative_to_invalid(#[case] path: &str, #[case] basepath: &str) {
        match relative_to(&path, &basepath) {
            Err(FSError::StripPrefixError {
                path: epath,
                basepath: ebasepath,
            }) if PathBuf::from(path) == epath && PathBuf::from(basepath) == ebasepath => (),
            r => panic!("r = {r:?}"),
        }
    }
}
