pub mod checksum;
pub mod checksum_json;
use crate::checksum::{get_checksum, FileInfo, ZarrDigest, ZarrEntry};
use clap::ValueEnum;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Debug, Eq, Hash, PartialEq, ValueEnum)]
pub enum Walker {
    Walkdir,
    Recursive,
    DepthFirst,
}

impl Walker {
    pub fn run<P: AsRef<Path>>(&self, dirpath: P) -> String {
        match self {
            Walker::Walkdir => walkdir_checksum(dirpath),
            Walker::Recursive => recursive_checksum(dirpath),
            Walker::DepthFirst => depth_first_checksum(dirpath),
        }
    }
}

// TODO: Return a Result
pub fn walkdir_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    let zarr: Result<ZarrEntry, _> = WalkDir::new(dirpath.as_ref())
        .into_iter()
        // We can't use walkdir's filter_entry(), because that prevents
        // descending into directories that don't match the predicate.
        // We also can't use r.map() inside the filter(), as that takes
        // ownership of r.
        .filter(|r| match r {
            Ok(e) => e.file_type().is_file(),
            Err(_) => true,
        })
        .map(|r| r.map(|e| FileInfo::for_file(e.path(), dirpath.as_ref())))
        .collect();
    match zarr {
        Ok(z) => z.digest().digest,
        Err(e) => panic!("Error walking Zarr: {e}"),
    }
}

// TODO: Return a Result
pub fn recursive_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    fn recurse<P: AsRef<Path>>(path: P, basepath: P) -> Result<ZarrDigest, io::Error> {
        let mut files = HashMap::new();
        let mut directories = HashMap::new();
        for p in fs::read_dir(path)? {
            let p = p?;
            let name = p.file_name().to_str().unwrap().to_string();
            if p.file_type()?.is_dir() {
                directories.insert(name, recurse(&p.path(), &basepath.as_ref().into())?);
            } else {
                files.insert(
                    name,
                    FileInfo::for_file(p.path(), basepath.as_ref().into()).to_zarr_digest(),
                );
            }
        }
        Ok(get_checksum(files, directories))
    }

    recurse(&dirpath, &dirpath).unwrap().digest
}

// TODO: Return a Result
pub fn depth_first_checksum<P: AsRef<Path>>(dirpath: P) -> String {
    let zarr: Result<ZarrEntry, _> = DepthFirstIterator::new(dirpath.as_ref())
        .map(|r| r.map(|p| FileInfo::for_file(p, dirpath.as_ref().into())))
        .collect();
    match zarr {
        Ok(z) => z.digest().digest,
        Err(e) => panic!("Error walking Zarr: {e}"),
    }
}

struct DepthFirstIterator {
    queue: VecDeque<Result<PathBuf, io::Error>>,
}

impl DepthFirstIterator {
    fn new<P: AsRef<Path>>(dirpath: P) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back(Ok(dirpath.as_ref().into()));
        DepthFirstIterator { queue }
    }
}

impl Iterator for DepthFirstIterator {
    type Item = Result<PathBuf, io::Error>;

    fn next(&mut self) -> Option<Result<PathBuf, io::Error>> {
        loop {
            let path = self.queue.pop_front()?;
            // TODO: Try to simplify this code
            match path {
                Ok(path) => match fs::metadata(&path) {
                    Ok(m) => {
                        if m.is_dir() {
                            match fs::read_dir(&path) {
                                Ok(iter) => self.queue.extend(iter.map(|r| r.map(|e| e.path()))),
                                Err(e) => return Some(Err(e)),
                            }
                        } else {
                            return Some(Ok(path));
                        }
                    }
                    Err(e) => return Some(Err(e)),
                },
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
