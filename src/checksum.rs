use crate::checksum_json::get_checksum_json;
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::iter::Iterator;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrDigest {
    pub digest: String,
    pub size: u64,
    pub file_count: usize,
}

impl fmt::Display for ZarrDigest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.digest)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZarrEntry {
    File {
        digest: ZarrDigest,
    },
    Directory {
        children: HashMap<String, ZarrEntry>,
    },
}

impl ZarrEntry {
    pub fn new() -> Self {
        ZarrEntry::directory()
    }

    pub fn file(digest: ZarrDigest) -> Self {
        ZarrEntry::File { digest }
    }

    pub fn directory() -> Self {
        ZarrEntry::Directory {
            children: HashMap::new(),
        }
    }

    pub fn digest(&self) -> ZarrDigest {
        match self {
            ZarrEntry::File { digest, .. } => digest.clone(),
            ZarrEntry::Directory { children, .. } => {
                let (files, directories): (Vec<_>, Vec<_>) =
                    children.iter().partition(|(_, v)| v.is_file());
                get_checksum(
                    files
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.digest()))
                        .collect(),
                    directories
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.digest()))
                        .collect(),
                )
            }
        }
    }

    pub fn is_file(&self) -> bool {
        match &self {
            ZarrEntry::File { .. } => true,
            ZarrEntry::Directory { .. } => false,
        }
    }

    // Should this return a Result?
    pub fn add_path<P: AsRef<Path>>(&mut self, path: P, digest: &str, size: u64) {
        match self {
            ZarrEntry::File { .. } => panic!("Cannot add a path to a file"),
            ZarrEntry::Directory { children, .. } => {
                let mut parts = Vec::new();
                for p in path.as_ref().components() {
                    match p {
                        Component::Normal(s) => match s.to_str() {
                            Some(name) => parts.push(name.to_string()),
                            None => panic!("Non-UTF-8 path: {:?}", path.as_ref()),
                        },
                        _ => panic!(
                            "Non-normalized or absolute path: {}",
                            path.as_ref().display()
                        ),
                    }
                }
                let basename = match parts.pop() {
                    Some(s) => s,
                    None => panic!("Empty path"),
                };
                let mut d = children;
                let mut dpath = PathBuf::new();
                for dirname in parts {
                    dpath.push(&dirname);
                    match d
                        .entry(dirname.clone())
                        .or_insert_with(ZarrEntry::directory)
                    {
                        ZarrEntry::File { .. } => {
                            panic!("Path type conflict for {}", dpath.display())
                        }
                        ZarrEntry::Directory { children, .. } => d = children,
                    }
                }
                let entry = ZarrEntry::file(ZarrDigest {
                    digest: digest.to_string(),
                    size,
                    file_count: 1,
                });
                if d.insert(basename, entry).is_some() {
                    panic!("File {} encountered twice", path.as_ref().display());
                }
            }
        }
    }

    pub fn add_file_info(&mut self, info: FileInfo) {
        self.add_path(info.path, &info.digest, info.size);
    }
}

impl Default for ZarrEntry {
    fn default() -> Self {
        ZarrEntry::new()
    }
}

impl FromIterator<FileInfo> for ZarrEntry {
    fn from_iter<I: IntoIterator<Item = FileInfo>>(iter: I) -> Self {
        let mut zarr = ZarrEntry::directory();
        for info in iter {
            zarr.add_file_info(info);
        }
        zarr
    }
}

pub struct FileInfo {
    pub path: PathBuf,
    pub digest: String,
    pub size: u64,
}

impl FileInfo {
    // TODO: Make this return a Result (and use Result::and_then() to call it
    // in the walker)
    pub fn for_file<P: AsRef<Path>>(path: P, basepath: P) -> FileInfo {
        let relpath = match path.as_ref().strip_prefix(PathBuf::from(basepath.as_ref())) {
            Ok(p) => p,
            Err(_) => panic!(
                "Path {:?} is not a descendant of {:?}",
                path.as_ref(),
                basepath.as_ref()
            ),
        };
        let size = match fs::metadata(path.as_ref()) {
            Ok(m) => m.len(),
            Err(e) => panic!("Could not get size of {:?}: {e}", path.as_ref()),
        };
        let digest = match md5_file(&path) {
            Ok(d) => d,
            Err(e) => panic!("Failed to digest {:?}: {e}", path.as_ref()),
        };
        FileInfo {
            path: relpath.into(),
            digest,
            size,
        }
    }

    pub fn to_zarr_digest(&self) -> ZarrDigest {
        ZarrDigest {
            digest: self.digest.clone(),
            size: self.size,
            file_count: 1,
        }
    }
}

pub fn get_checksum(
    files: HashMap<String, ZarrDigest>,
    directories: HashMap<String, ZarrDigest>,
) -> ZarrDigest {
    let md5 = md5_string(&get_checksum_json(&files, &directories));
    let mut size = 0;
    let mut file_count = 0;
    for zd in files.values().chain(directories.values()) {
        size += zd.size;
        file_count += zd.file_count;
    }
    let digest = format!("{md5}-{file_count}--{size}");
    ZarrDigest {
        digest,
        size,
        file_count,
    }
}

pub fn compile_checksum<I: Iterator<Item = FileInfo>>(iter: I) -> String {
    iter.collect::<ZarrEntry>().digest().digest
}

pub fn try_compile_checksum<I: Iterator<Item = Result<FileInfo, E>>, E>(
    iter: I,
) -> Result<String, E> {
    Ok(iter.collect::<Result<ZarrEntry, E>>()?.digest().digest)
}

pub fn md5_string(s: &str) -> String {
    hex::encode(&(Md5::new().chain_update(s).finalize()))
}

pub fn md5_file<P: AsRef<Path>>(path: P) -> Result<String, io::Error> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(hex::encode(&hasher.finalize()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_checksum_nothing() {
        let files = HashMap::new();
        let directories = HashMap::new();
        let digest = get_checksum(files, directories);
        assert_eq!(digest.digest, "481a2f77ab786a0f45aafd5db0971caa-0--0");
    }

    #[test]
    fn test_get_checksum_one_file() {
        let files = HashMap::from([(
            "bar".into(),
            ZarrDigest {
                digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let directories = HashMap::new();
        let digest = get_checksum(files, directories);
        assert_eq!(digest.digest, "f21b9b4bf53d7ce1167bcfae76371e59-1--1");
    }

    #[test]
    fn test_get_checksum_one_directory() {
        let files = HashMap::new();
        let directories = HashMap::from([(
            "bar".into(),
            ZarrDigest {
                digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let digest = get_checksum(files, directories);
        assert_eq!(digest.digest, "ea8b8290b69b96422a3ed1cca0390f21-1--1");
    }

    #[test]
    fn test_get_checksum_two_files() {
        let files = HashMap::from([
            (
                "bar".into(),
                ZarrDigest {
                    digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
            (
                "baz".into(),
                ZarrDigest {
                    digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
        ]);
        let directories = HashMap::new();
        let digest = get_checksum(files, directories);
        assert_eq!(digest.digest, "8e50add2b46d3a6389e2d9d0924227fb-2--2");
    }

    #[test]
    fn test_get_checksum_two_directories() {
        let files = HashMap::new();
        let directories = HashMap::from([
            (
                "bar".into(),
                ZarrDigest {
                    digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
            (
                "baz".into(),
                ZarrDigest {
                    digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
        ]);
        let digest = get_checksum(files, directories);
        assert_eq!(digest.digest, "4c21a113688f925240549b14136d61ff-2--2");
    }

    #[test]
    fn test_get_checksum_one_of_each() {
        let files = HashMap::from([(
            "baz".into(),
            ZarrDigest {
                digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let directories = HashMap::from([(
            "bar".into(),
            ZarrDigest {
                digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let digest = get_checksum(files, directories);
        assert_eq!(digest.digest, "d5e4eb5dc8efdb54ff089db1eef34119-2--2");
    }

    #[test]
    fn test_tree_digest() {
        let mut sample = ZarrEntry::directory();
        sample.add_path("arr_0/.zarray", "9e30a0a1a465e24220d4132fdd544634", 315);
        sample.add_path("arr_0/0", "ed4e934a474f1d2096846c6248f18c00", 431);
        sample.add_path("arr_1/.zarray", "9e30a0a1a465e24220d4132fdd544634", 315);
        sample.add_path("arr_1/0", "fba4dee03a51bde314e9713b00284a93", 431);
        sample.add_path(".zgroup", "e20297935e73dd0154104d4ea53040ab", 24);
        assert_eq!(
            sample.digest().digest,
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }

    #[test]
    fn test_from_iter() {
        let files = vec![
            FileInfo {
                path: "arr_0/.zarray".into(),
                digest: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileInfo {
                path: "arr_0/0".into(),
                digest: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileInfo {
                path: "arr_1/.zarray".into(),
                digest: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileInfo {
                path: "arr_1/0".into(),
                digest: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileInfo {
                path: ".zgroup".into(),
                digest: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let sample = ZarrEntry::from_iter(files);
        assert_eq!(
            sample.digest().digest,
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }
}
