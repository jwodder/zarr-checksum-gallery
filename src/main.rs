use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::runtime::Builder;
use zarr_checksum_gallery::*;

/// Compute the Dandi Zarr checksum for a directory
#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[clap(version)]
struct Arguments {
    /// Show DEBUG log messages
    #[clap(long)]
    debug: bool,

    /// Show TRACE log messages
    #[clap(long)]
    trace: bool,

    /// The tree-traversal implementation to use
    #[clap(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
enum Command {
    /// Traverse the directory breadth-first
    BreadthFirst {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Traverse the directory depth-first & iteratively
    DepthFirst {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do an asynchronous directory traversal
    Fastasync {
        /// Set the number of threads for the async runtime to use
        #[clap(short, long, default_value_t = num_cpus::get())]
        threads: usize,

        /// Set the number of worker tasks to use
        #[clap(short, long, default_value_t = num_cpus::get())]
        workers: usize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Do a multithreaded directory traversal
    Fastio {
        /// Set the number of threads to use
        #[clap(short, long, default_value_t = num_cpus::get())]
        threads: usize,

        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Traverse the directory depth-first & recursively
    Recursive {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
    /// Traverse the directory using the 'walkdir' crate
    Walkdir {
        /// Path to the directory to checksum
        dirpath: PathBuf,
    },
}

impl Command {
    fn run(self) -> Result<String, ChecksumError> {
        match self {
            Command::BreadthFirst { dirpath } => breadth_first_checksum(dirpath),
            Command::DepthFirst { dirpath } => depth_first_checksum(dirpath),
            Command::Fastasync {
                threads,
                workers,
                dirpath,
            } => {
                let rt = if threads > 1 {
                    Builder::new_multi_thread()
                        .worker_threads(threads)
                        .enable_all()
                        .build()
                        .unwrap()
                } else {
                    Builder::new_current_thread().enable_all().build().unwrap()
                };
                rt.block_on(fastasync_checksum(dirpath, workers))
            }
            Command::Fastio { threads, dirpath } => fastio_checksum(dirpath, threads),
            Command::Recursive { dirpath } => recursive_checksum(dirpath),
            Command::Walkdir { dirpath } => walkdir_checksum(dirpath),
        }
    }
}

fn main() -> ExitCode {
    let args = Arguments::parse();
    let log_level = if args.trace {
        log::LevelFilter::Trace
    } else if args.debug {
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
    match args.command.run() {
        Ok(checksum) => {
            println!("{}", checksum);
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
