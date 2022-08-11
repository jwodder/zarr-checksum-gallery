use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::runtime::Builder;
use zarr_checksum_gallery::*;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[clap(version)]
struct Arguments {
    #[clap(long)]
    debug: bool,

    #[clap(long)]
    trace: bool,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
enum Command {
    BreadthFirst {
        dirpath: PathBuf,
    },
    DepthFirst {
        dirpath: PathBuf,
    },
    Fastasync {
        #[clap(short, long, default_value_t = num_cpus::get())]
        threads: usize,
        #[clap(short, long, default_value_t = num_cpus::get())]
        workers: usize,
        dirpath: PathBuf,
    },
    Fastio {
        #[clap(short, long, default_value_t = num_cpus::get())]
        threads: usize,
        dirpath: PathBuf,
    },
    Recursive {
        dirpath: PathBuf,
    },
    Walkdir {
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
