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
pub enum ChecksumTree {
    File {
        checksum: ZarrChecksum,
    },
    Directory {
        children: HashMap<String, ChecksumTree>,
    },
}

impl ChecksumTree {
    pub fn new() -> Self {
        ChecksumTree::directory()
    }

    pub fn file(checksum: ZarrChecksum) -> Self {
        ChecksumTree::File { checksum }
    }

    pub fn directory() -> Self {
        ChecksumTree::Directory {
            children: HashMap::new(),
        }
    }

    pub fn checksum(&self) -> ZarrChecksum {
        match self {
            ChecksumTree::File { checksum, .. } => checksum.clone(),
            ChecksumTree::Directory { children, .. } => {
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
            ChecksumTree::File { .. } => true,
            ChecksumTree::Directory { .. } => false,
        }
    }

    // TODO: Should this return a Result?  (Error type name: Tree(Build?)Error?)
    pub fn add_file<P: AsRef<Path>>(&mut self, relpath: P, checksum: &str, size: u64) {
        let relpath = relpath.as_ref();
        match self {
            ChecksumTree::File { .. } => panic!("Cannot add a path to a file"),
            ChecksumTree::Directory { children, .. } => {
                let mut parts = Vec::new();
                for p in relpath.components() {
                    match p {
                        Component::Normal(s) => match s.to_str() {
                            Some(name) => parts.push(name.to_string()),
                            // TODO: Replace `P` with a relative_path type so
                            // that this is caught earlier:
                            None => panic!("Non-UTF-8 path: {:?}", relpath),
                        },
                        _ => panic!("Non-normalized or absolute path: {}", relpath.display()),
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
                        .or_insert_with(ChecksumTree::directory)
                    {
                        ChecksumTree::File { .. } => {
                            panic!("Path type conflict for {}", dpath.display())
                        }
                        ChecksumTree::Directory { children, .. } => d = children,
                    }
                }
                let entry = ChecksumTree::file(ZarrChecksum {
                    checksum: checksum.to_string(),
                    size,
                    file_count: 1,
                });
                if d.insert(basename, entry).is_some() {
                    panic!("File {} encountered twice", relpath.display());
                }
            }
        }
    }

    pub fn add_file_info(&mut self, info: FileInfo) {
        self.add_file(info.relpath, &info.md5_digest, info.size);
    }
}

impl Default for ChecksumTree {
    fn default() -> Self {
        ChecksumTree::new()
    }
}

impl FromIterator<FileInfo> for ChecksumTree {
    fn from_iter<I: IntoIterator<Item = FileInfo>>(iter: I) -> Self {
        let mut zarr = ChecksumTree::directory();
        for info in iter {
            zarr.add_file_info(info);
        }
        zarr
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileInfo {
    pub relpath: PathBuf,
    pub md5_digest: String,
    pub size: u64,
}

impl FileInfo {
    pub fn for_file<P, Q>(path: P, basepath: Q) -> Result<FileInfo, ZarrError>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let path = path.as_ref();
        let basepath = basepath.as_ref();
        // TODO: Also map empty relpaths to strip_prefix_error
        let relpath = path
            .strip_prefix(PathBuf::from(basepath))
            .map_err(|_| ZarrError::strip_prefix_error(&path, &basepath))?
            .to_path_buf();
        Ok(FileInfo {
            relpath,
            md5_digest: md5_file(&path)?,
            size: fs::metadata(&path)
                .map_err(|e| ZarrError::stat_error(&path, e))?
                .len(),
        })
    }
}

impl From<FileInfo> for ZarrChecksum {
    fn from(info: FileInfo) -> ZarrChecksum {
        ZarrChecksum {
            checksum: info.md5_digest,
            size: info.size,
            file_count: 1,
        }
    }
}

pub fn get_checksum(
    files: HashMap<String, ZarrChecksum>,
    directories: HashMap<String, ZarrChecksum>,
) -> ZarrChecksum {
    let mut size = 0;
    let mut file_count = 0;
    for zd in files.values().chain(directories.values()) {
        size += zd.size;
        file_count += zd.file_count;
    }
    let md5 = md5_string(&get_checksum_json(files, directories));
    let checksum = format!("{md5}-{file_count}--{size}");
    ZarrChecksum {
        checksum,
        size,
        file_count,
    }
}

pub fn compile_checksum<I: IntoIterator<Item = FileInfo>>(seq: I) -> String {
    seq.into_iter()
        .collect::<ChecksumTree>()
        .checksum()
        .checksum
}

pub fn try_compile_checksum<I: IntoIterator<Item = Result<FileInfo, E>>, E>(
    seq: I,
) -> Result<String, E> {
    Ok(seq
        .into_iter()
        .collect::<Result<ChecksumTree, E>>()?
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
    fn test_checksum_tree() {
        let mut sample = ChecksumTree::directory();
        sample.add_file("arr_0/.zarray", "9e30a0a1a465e24220d4132fdd544634", 315);
        sample.add_file("arr_0/0", "ed4e934a474f1d2096846c6248f18c00", 431);
        sample.add_file("arr_1/.zarray", "9e30a0a1a465e24220d4132fdd544634", 315);
        sample.add_file("arr_1/0", "fba4dee03a51bde314e9713b00284a93", 431);
        sample.add_file(".zgroup", "e20297935e73dd0154104d4ea53040ab", 24);
        assert_eq!(
            sample.checksum().checksum,
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }

    #[test]
    fn test_from_iter() {
        let files = vec![
            FileInfo {
                relpath: "arr_0/.zarray".into(),
                md5_digest: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileInfo {
                relpath: "arr_0/0".into(),
                md5_digest: "ed4e934a474f1d2096846c6248f18c00".into(),
                size: 431,
            },
            FileInfo {
                relpath: "arr_1/.zarray".into(),
                md5_digest: "9e30a0a1a465e24220d4132fdd544634".into(),
                size: 315,
            },
            FileInfo {
                relpath: "arr_1/0".into(),
                md5_digest: "fba4dee03a51bde314e9713b00284a93".into(),
                size: 431,
            },
            FileInfo {
                relpath: ".zgroup".into(),
                md5_digest: "e20297935e73dd0154104d4ea53040ab".into(),
                size: 24,
            },
        ];
        let sample = ChecksumTree::from_iter(files);
        assert_eq!(
            sample.checksum().checksum,
            "4313ab36412db2981c3ed391b38604d6-5--1516"
        );
    }
}
