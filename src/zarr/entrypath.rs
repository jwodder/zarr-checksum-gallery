use crate::errors::{EntryNameError, EntryPathError, FSError};
use std::fmt;
use std::path::{Component, Path};

/// A normalized, nonempty, forward-slash-separated UTF-8 encoded relative path
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct EntryPath(Vec<String>);

impl EntryPath {
    /// Return the basename of the path
    pub fn file_name(&self) -> &str {
        self.0
            .last()
            .expect("Invariant violated: EntryPath is empty")
    }

    /// Return an iterator over the parent paths of the path, starting at the
    /// first component and stopping before the file name
    ///
    /// ```
    /// # use zarr_checksum_gallery::zarr::EntryPath;
    /// let path = EntryPath::try_from("foo/bar/baz").unwrap();
    /// let mut parents = path.parents();
    /// assert_eq!(parents.next().unwrap().to_string(), "foo");
    /// assert_eq!(parents.next().unwrap().to_string(), "foo/bar");
    /// assert_eq!(parents.next(), None);
    /// ```
    pub fn parents(&self) -> Parents<'_> {
        Parents {
            parts: &self.0,
            i: 0,
        }
    }

    pub fn join1(&self, s: &str) -> Result<EntryPath, EntryNameError> {
        if is_path_name(s) {
            let mut parts = self.0.clone();
            parts.push(String::from(s));
            Ok(EntryPath(parts))
        } else {
            Err(EntryNameError(String::from(s)))
        }
    }
}

impl fmt::Debug for EntryPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("\"")?;
        for (i, part) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            for c in part.chars() {
                f.write_str(&c.escape_debug().to_string())?;
            }
        }
        f.write_str("\"")?;
        Ok(())
    }
}

impl fmt::Display for EntryPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, part) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            f.write_str(part)?;
        }
        Ok(())
    }
}

impl TryFrom<&Path> for EntryPath {
    type Error = EntryPathError;

    fn try_from(path: &Path) -> Result<EntryPath, EntryPathError> {
        let mut output = Vec::new();
        for c in path.components() {
            match c {
                Component::Normal(part) => match part.to_str() {
                    Some(s) => output.push(String::from(s)),
                    None => return Err(EntryPathError(path.into())),
                },
                Component::CurDir => (),
                _ => return Err(EntryPathError(path.into())),
            }
        }
        if output.is_empty() {
            return Err(EntryPathError(path.into()));
        }
        Ok(EntryPath(output))
    }
}

impl TryFrom<&str> for EntryPath {
    type Error = EntryPathError;

    fn try_from(path: &str) -> Result<EntryPath, EntryPathError> {
        Path::new(path).try_into()
    }
}

/// Iterator over the parent paths of an [`EntryPath`]
///
/// The iterator's items are themselves [`EntryPath`]s.
///
/// This struct is returned by [`EntryPath::parents()`].
pub struct Parents<'a> {
    parts: &'a Vec<String>,
    i: usize,
}

impl<'a> Iterator for Parents<'a> {
    type Item = EntryPath;

    fn next(&mut self) -> Option<EntryPath> {
        if self.i + 1 < self.parts.len() {
            self.i += 1;
            Some(EntryPath(self.parts[0..self.i].to_vec()))
        } else {
            None
        }
    }
}

/// Compute `path` relative to `basepath` as an [`EntryPath`]
pub(crate) fn relative_to<P, Q>(path: P, basepath: Q) -> Result<EntryPath, FSError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let path = path.as_ref();
    let basepath = basepath.as_ref();
    path.strip_prefix(basepath)
        .map_err(|_| FSError::relative_path_error(&path, &basepath))?
        .try_into()
        .map_err(|_| FSError::relative_path_error(&path, &basepath))
}

pub(super) fn is_path_name(s: &str) -> bool {
    !s.is_empty() && s != "." && s != ".." && !s.contains('/')
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::path::PathBuf;

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
        assert_eq!(relative_to(path, basepath).unwrap().to_string(), relpath);
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
            Err(FSError::RelativePathError {
                path: epath,
                basepath: ebasepath,
            }) if PathBuf::from(path) == epath && PathBuf::from(basepath) == ebasepath => (),
            r => panic!("r = {r:?}"),
        }
    }

    #[test]
    fn test_parents() {
        let path = EntryPath::try_from("foo/bar/baz").unwrap();
        let mut parents = path.parents();
        assert_eq!(parents.next().unwrap().to_string(), "foo");
        assert_eq!(parents.next().unwrap().to_string(), "foo/bar");
        assert_eq!(parents.next(), None);
    }

    #[test]
    fn test_parents_len_1() {
        let path = EntryPath::try_from("foo").unwrap();
        let mut parents = path.parents();
        assert_eq!(parents.next(), None);
    }

    #[rstest]
    #[case("foo", r#""foo""#)]
    #[case("foo/bar", r#""foo/bar""#)]
    #[case("foo\n/\tbar", r#""foo\n/\tbar""#)]
    #[case("foo\x1B—🐐bar", r#""foo\u{1b}—🐐bar""#)]
    fn test_debug(#[case] path: &str, #[case] repr: &str) {
        let path = EntryPath::try_from(path).unwrap();
        assert_eq!(format!("{path:?}"), repr);
    }

    #[rstest]
    #[case("foo", "foo")]
    #[case("foo/bar", "foo/bar")]
    #[case("foo/.", "foo")]
    #[case("./foo", "foo")]
    #[case("foo/./bar", "foo/bar")]
    #[case("foo/", "foo")]
    #[case("foo//bar", "foo/bar")]
    fn test_entrypath_try_from(#[case] path: &str, #[case] displayed: &str) {
        assert_eq!(EntryPath::try_from(path).unwrap().to_string(), displayed);
    }

    #[rstest]
    #[case("")]
    #[case(".")]
    #[case("..")]
    #[case("/")]
    #[case("/foo")]
    #[case("foo/..")]
    #[case("../foo")]
    #[case("foo/../bar")]
    #[case("foo/bar/..")]
    fn test_entrypath_try_from_err(#[case] path: &str) {
        assert_eq!(
            EntryPath::try_from(path),
            Err(EntryPathError(PathBuf::from(path)))
        );
    }
}
