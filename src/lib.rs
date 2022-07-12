pub mod checksum;
pub mod checksum_json;
pub mod walkers;
pub use crate::walkers::*;
use clap::ValueEnum;
use std::path::Path;

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
