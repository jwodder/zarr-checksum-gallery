use crate::checksum_json::get_checksum_json;
use crate::error::ZarrError;
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::iter::Iterator;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ZarrChecksum {
    pub checksum: String,
    pub size: u64,
    pub file_count: usize,
}

impl fmt::Display for ZarrChecksum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.checksum)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZarrEntry {
    File {
        checksum: ZarrChecksum,
    },
    Directory {
        children: HashMap<String, ZarrEntry>,
    },
}

impl ZarrEntry {
    pub fn new() -> Self {
        ZarrEntry::directory()
    }

    pub fn file(checksum: ZarrChecksum) -> Self {
        ZarrEntry::File { checksum }
    }

    pub fn directory() -> Self {
        ZarrEntry::Directory {
            children: HashMap::new(),
        }
    }

    pub fn checksum(&self) -> ZarrChecksum {
        match self {
            ZarrEntry::File { checksum, .. } => checksum.clone(),
            ZarrEntry::Directory { children, .. } => {
                let (files, directories): (Vec<_>, Vec<_>) =
                    children.iter().partition(|(_, v)| v.is_file());
                get_checksum(
                    files
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.checksum()))
                        .collect(),
                    directories
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.checksum()))
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
    pub fn add_path<P: AsRef<Path>>(&mut self, path: P, checksum: &str, size: u64) {
        let path = path.as_ref();
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
                        .or_insert_with(ZarrEntry::directory)
                    {
                        ZarrEntry::File { .. } => {
                            panic!("Path type conflict for {}", dpath.display())
                        }
                        ZarrEntry::Directory { children, .. } => d = children,
                    }
                }
                let entry = ZarrEntry::file(ZarrChecksum {
                    checksum: checksum.to_string(),
                    size,
                    file_count: 1,
                });
                if d.insert(basename, entry).is_some() {
                    panic!("File {} encountered twice", path.display());
                }
            }
        }
    }

    pub fn add_file_info(&mut self, info: FileInfo) {
        self.add_path(info.path, &info.md5_digest, info.size);
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileInfo {
    pub path: PathBuf,
    pub md5_digest: String,
    pub size: u64,
}

impl FileInfo {
    pub fn for_file<P: AsRef<Path>>(path: P, basepath: P) -> Result<FileInfo, ZarrError> {
        let path = path.as_ref();
        let basepath = basepath.as_ref();
        let relpath = path
            .strip_prefix(PathBuf::from(basepath))
            .map_err(|_| ZarrError::strip_prefix_error(&path, &basepath))?;
        Ok(FileInfo {
            path: relpath.into(),
            md5_digest: md5_file(&path)?,
            size: fs::metadata(&path)
                .map_err(|e| ZarrError::stat_error(&path, e))?
                .len(),
        })
    }

    pub fn to_zarr_checksum(&self) -> ZarrChecksum {
        ZarrChecksum {
            checksum: self.md5_digest.clone(),
            size: self.size,
            file_count: 1,
        }
    }
}

pub fn get_checksum(
    files: HashMap<String, ZarrChecksum>,
    directories: HashMap<String, ZarrChecksum>,
) -> ZarrChecksum {
    let md5 = md5_string(&get_checksum_json(&files, &directories));
    let mut size = 0;
    let mut file_count = 0;
    for zd in files.values().chain(directories.values()) {
        size += zd.size;
        file_count += zd.file_count;
    }
    let checksum = format!("{md5}-{file_count}--{size}");
    ZarrChecksum {
        checksum,
        size,
        file_count,
    }
}

pub fn compile_checksum<I: IntoIterator<Item = FileInfo>>(seq: I) -> String {
    seq.into_iter().collect::<ZarrEntry>().checksum().checksum
}

pub fn try_compile_checksum<I: IntoIterator<Item = Result<FileInfo, E>>, E>(
    seq: I,
) -> Result<String, E> {
    Ok(seq
        .into_iter()
        .collect::<Result<ZarrEntry, E>>()?
        .checksum()
        .checksum)
}

pub fn md5_string(s: &str) -> String {
    hex::encode(Md5::digest(s))
}

pub fn md5_file<P: AsRef<Path>>(path: P) -> Result<String, ZarrError> {
    let mut file = fs::File::open(&path).map_err(|e| ZarrError::md5_file_error(&path, e))?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher).map_err(|e| ZarrError::md5_file_error(&path, e))?;
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_checksum_nothing() {
        let files = HashMap::new();
        let directories = HashMap::new();
        let checksum = get_checksum(files, directories);
        assert_eq!(checksum.checksum, "481a2f77ab786a0f45aafd5db0971caa-0--0");
    }

    #[test]
    fn test_get_checksum_one_file() {
        let files = HashMap::from([(
            "bar".into(),
            ZarrChecksum {
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let directories = HashMap::new();
        let checksum = get_checksum(files, directories);
        assert_eq!(checksum.checksum, "f21b9b4bf53d7ce1167bcfae76371e59-1--1");
    }

    #[test]
    fn test_get_checksum_one_directory() {
        let files = HashMap::new();
        let directories = HashMap::from([(
            "bar".into(),
            ZarrChecksum {
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let checksum = get_checksum(files, directories);
        assert_eq!(checksum.checksum, "ea8b8290b69b96422a3ed1cca0390f21-1--1");
    }

    #[test]
    fn test_get_checksum_two_files() {
        let files = HashMap::from([
            (
                "bar".into(),
                ZarrChecksum {
                    checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
            (
                "baz".into(),
                ZarrChecksum {
                    checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
        ]);
        let directories = HashMap::new();
        let checksum = get_checksum(files, directories);
        assert_eq!(checksum.checksum, "8e50add2b46d3a6389e2d9d0924227fb-2--2");
    }

    #[test]
    fn test_get_checksum_two_directories() {
        let files = HashMap::new();
        let directories = HashMap::from([
            (
                "bar".into(),
                ZarrChecksum {
                    checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1--1".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
            (
                "baz".into(),
                ZarrChecksum {
                    checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                    size: 1,
                    file_count: 1,
                },
            ),
        ]);
        let checksum = get_checksum(files, directories);
        assert_eq!(checksum.checksum, "4c21a113688f925240549b14136d61ff-2--2");
    }

    #[test]
    fn test_get_checksum_one_of_each() {
        let files = HashMap::from([(
            "baz".into(),
            ZarrChecksum {
                checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let directories = HashMap::from([(
            "bar".into(),
            ZarrChecksum {
                checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-1--1".into(),
                size: 1,
                file_count: 1,
            },
        )]);
        let checksum = get_checksum(files, directories);
        assert_eq!(checksum.checksum, "d5e4eb5dc8efdb54ff089db1eef34119-2--2");
    }

    #[test]
    fn test_tree_checksum() {
        let mut sample = ZarrEntry::directory();
        sample.add_path("arr_0/.zarray", "9e30a0a1a465e24220d4132fdd544634", 315);
        sample.add_path("arr_0/0", "ed4e934a474f1d2096846c6248f18c00", 431);
        sample.add_path("arr_1/.zarray", "9e30a0a1a465e24220d4132fdd544634", 315);
        sample.add_path("arr_1/0", "fba4dee03a51bde314e9713b00284a93", 431);
        sample.add_path(".zgroup", "e20297935e73dd0154104d4ea53040ab", 24);
        assert_eq!(
            sample.checksum().checksum,
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }

    #[test]
    fn test_from_iter() {
        let files = vec![
            FileInfo {
                path: "arr_0/.zarray".into(),
                md5_digest: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileInfo {
                path: "arr_0/0".into(),
                md5_digest: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileInfo {
                path: "arr_1/.zarray".into(),
                md5_digest: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileInfo {
                path: "arr_1/0".into(),
                md5_digest: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileInfo {
                path: ".zgroup".into(),
                md5_digest: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let sample = ZarrEntry::from_iter(files);
        assert_eq!(
            sample.checksum().checksum,
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }
}
