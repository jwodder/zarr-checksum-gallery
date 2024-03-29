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
#[command(version)]
struct Arguments {
    /// Show DEBUG log messages
    #[arg(long)]
    debug: bool,

    /// Exclude special dotfiles from checksumming
    #[arg(short = 'E', long)]
    exclude_dotfiles: bool,

    /// Show TRACE log messages
    #[arg(long)]
    trace: bool,

    /// The tree-traversal implementation to use
    #[command(subcommand)]
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
    /// as soon as possible, with intermediate results reported using shared
    /// memory
    CollapsioArc {
        /// Set the number of threads to use
        #[arg(short, long, default_value_t = default_jobs())]
        threads: NonZeroUsize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do a multithreaded directory traversal, computing directory checksums
    /// as soon as possible, with intermediate results reported over
    /// synchronized channels
    CollapsioMpsc {
        /// Set the number of threads to use
        #[arg(short, long, default_value_t = default_jobs())]
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
        #[arg(short, long, default_value_t = default_jobs())]
        threads: NonZeroUsize,

        /// Set the number of worker tasks to use
        #[arg(short, long, default_value_t = default_jobs())]
        workers: NonZeroUsize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do a multithreaded directory traversal and build a tree of checksums
    Fastio {
        /// Set the number of threads to use
        #[arg(short, long, default_value_t = default_jobs())]
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
        #[arg(short, long, default_value_t = default_jobs())]
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
                out.finish(format_args!("[{:<5}] {}", record.level(), message));
            })
            .level(log_level)
            .chain(std::io::stderr())
            .apply()
            .expect("no other logger should have been previously initialized");
        match self.command {
            Command::BreadthFirst { dirpath } => {
                breadth_first_checksum(&Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles))
            }
            Command::CollapsioArc { threads, dirpath } => collapsio_arc_checksum(
                &Zarr::new(dirpath).exclude_dotfiles(self.exclude_dotfiles),
                threads,
            ),
            Command::CollapsioMpsc { threads, dirpath } => collapsio_mpsc_checksum(
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
                        .expect("Buiding a multithreaded tokio runtime should not fail")
                } else {
                    Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Buiding a single-threaded tokio runtime should not fail")
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
