use crate::checksum_json::get_checksum_json;
use base16ct::lower::encode_string as tohex;
use md5::{Digest, Md5};
use std::collections::HashMap;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZarrEntry {
    File {
        name: String,
        digest: ZarrDigest,
    },
    Directory {
        name: String,
        children: HashMap<String, ZarrEntry>,
    },
}

impl ZarrEntry {
    pub fn file(name: &str, digest: ZarrDigest) -> Self {
        ZarrEntry::File {
            name: name.into(),
            digest,
        }
    }

    pub fn directory(name: &str) -> Self {
        ZarrEntry::Directory {
            name: name.into(),
            children: HashMap::new(),
        }
    }

    pub fn name(&self) -> String {
        match self {
            ZarrEntry::File { name, .. } => name.clone(),
            ZarrEntry::Directory { name, .. } => name.clone(),
        }
    }

    pub fn digest(&self) -> ZarrDigest {
        match self {
            ZarrEntry::File { digest, .. } => digest.clone(),
            ZarrEntry::Directory { children, .. } => {
                let (files, directories): (Vec<_>, Vec<_>) =
                    children.values().partition(|e| e.is_file());
                get_checksum(
                    files.into_iter().map(|e| (e.name(), e.digest())).collect(),
                    directories
                        .into_iter()
                        .map(|e| (e.name(), e.digest()))
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

    pub fn add_file(&mut self, path: &PathBuf, digest: &str, size: u64) {
        match self {
            ZarrEntry::File { .. } => panic!("Cannot add a path to a file"),
            ZarrEntry::Directory { children, .. } => {
                let mut parts = Vec::new();
                for p in path.components() {
                    match p {
                        Component::Normal(s) => match s.to_str() {
                            Some(name) => parts.push(name.to_string()),
                            None => panic!("Non-UTF-8 path: {:?}", path),
                        },
                        _ => panic!("Non-normalized or absolute path: {}", path.display()),
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
                        .or_insert_with(|| ZarrEntry::directory(&dirname))
                    {
                        ZarrEntry::File { .. } => {
                            panic!("Path type conflict for {}", dpath.display())
                        }
                        ZarrEntry::Directory { children, .. } => d = children,
                    }
                }
                let entry = ZarrEntry::file(
                    &basename,
                    ZarrDigest {
                        digest: digest.to_string(),
                        size,
                        file_count: 1,
                    },
                );
                if d.insert(basename, entry).is_some() {
                    panic!("File {} encountered twice", path.display());
                }
            }
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

pub fn md5_string(s: &str) -> String {
    tohex(&(Md5::new().chain_update(s).finalize()))
}

pub fn md5_file<P: AsRef<Path>>(path: &P) -> String {
    let mut file = fs::File::open(path).unwrap();
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher).unwrap();
    tohex(&hasher.finalize())
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
}
