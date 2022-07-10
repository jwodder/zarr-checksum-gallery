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

    pub fn add_file_digest(&mut self, path: &PathBuf, digest: ZarrDigest) {
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
                let entry = ZarrEntry::file(&basename, digest);
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

#[allow(unused_variables)]
pub fn get_checksum_json(
    files: &HashMap<String, ZarrDigest>,
    directories: &HashMap<String, ZarrDigest>,
) -> String {
    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct ZarrChecksum {
        digest: String,
        name: String,
        size: u64,
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct ZarrChecksumCollection {
        files: Vec<ZarrChecksum>,
        directories: Vec<ZarrChecksum>,
    }

    todo!()
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
