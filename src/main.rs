use clap::{Parser, Subcommand};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread::available_parallelism;
use tokio::runtime::Builder;
use zarr_checksum_gallery::zarr::Zarr;
use zarr_checksum_gallery::*;

/// Compute the Dandi Zarr checksum for a directory
#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[clap(version)]
struct Arguments {
    /// Show DEBUG log messages
    #[clap(long)]
    debug: bool,

    /// Exclude special dotfiles from checksumming
    #[clap(short = 'E', long)]
    exclude_dotfiles: bool,

    /// Show TRACE log messages
    #[clap(long)]
    trace: bool,

    /// The tree-traversal implementation to use
    #[clap(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
enum Command {
    /// Traverse the directory breadth-first and build a tree of checksums
    BreadthFirst {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do a multithreaded directory traversal, computing directory checksums
    /// as soon as possible
    Collapsio {
        /// Set the number of threads to use
        #[clap(short, long, default_value_t = default_jobs())]
        threads: NonZeroUsize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Traverse the directory depth-first & iteratively, computing directory
    /// checksums as soon as possible
    DepthFirst {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do an asynchronous directory traversal and build a tree of checksums
    Fastasync {
        /// Set the number of threads for the async runtime to use
        #[clap(short, long, default_value_t = default_jobs())]
        threads: NonZeroUsize,

        /// Set the number of worker tasks to use
        #[clap(short, long, default_value_t = default_jobs())]
        workers: NonZeroUsize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do a multithreaded directory traversal and build a tree of checksums
    Fastio {
        /// Set the number of threads to use
        #[clap(short, long, default_value_t = default_jobs())]
        threads: NonZeroUsize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Traverse & checksum the directory depth-first & recursively
    Recursive {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do a multithreaded directory traversal and draw a tree of checksums
    Tree {
        /// Set the number of threads to use
        #[clap(short, long, default_value_t = default_jobs())]
        threads: NonZeroUsize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
}

impl Arguments {
    fn run(self) -> Result<String, ChecksumError> {
        let log_level = if self.trace {
            log::LevelFilter::Trace
        } else if self.debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Warn
        };
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("[{:<5}] {}", record.level(), message))
            })
            .level(log_level)
            .chain(std::io::stderr())
            .apply()
            .unwrap();
        match self.command {
            Command::BreadthFirst { dirpath } => {
                breadth_first_checksum(&Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles))
            }
            Command::Collapsio { threads, dirpath } => collapsio_checksum(
                &Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles),
                threads,
            ),
            Command::DepthFirst { dirpath } => {
                depth_first_checksum(&Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles))
            }
            Command::Fastasync {
                threads,
                workers,
                dirpath,
            } => {
                let threads = threads.get();
                let rt = if threads > 1 {
                    Builder::new_multi_thread()
                        .worker_threads(threads)
                        .enable_all()
                        .build()
                        .unwrap()
                } else {
                    Builder::new_current_thread().enable_all().build().unwrap()
                };
                rt.block_on(fastasync_checksum(
                    &Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles),
                    workers,
                ))
            }
            Command::Fastio { threads, dirpath } => fastio_checksum(
                &Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles),
                threads,
            ),
            Command::Recursive { dirpath } => {
                recursive_checksum(&Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles))
            }
            Command::Tree { threads, dirpath } => fastio_checksum_tree(
                &Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles),
                threads,
            )
            .map(|chktree| chktree.into_termtree().to_string()),
        }
    }
}

fn main() -> ExitCode {
    match Arguments::parse().run() {
        Ok(checksum) => {
            println!("{checksum}");
            ExitCode::SUCCESS
        }
        Err(ChecksumError::ChecksumTreeError(e)) => {
            eprintln!("INTERNAL ERROR: {e}");
            ExitCode::FAILURE
        }
        Err(ChecksumError::FSError(e)) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn default_jobs() -> NonZeroUsize {
    available_parallelism().expect("Could not determine number of available CPUs")
}
