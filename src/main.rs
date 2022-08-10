use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
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
    Walkdir {
        dirpath: PathBuf,
    },
    Recursive {
        dirpath: PathBuf,
    },
    BreadthFirst {
        dirpath: PathBuf,
    },
    DepthFirst {
        dirpath: PathBuf,
    },
    Fastio {
        #[clap(short, long, default_value_t = num_cpus::get())]
        threads: usize,
        dirpath: PathBuf,
    },
}

impl Command {
    fn run(self) -> Result<String, ChecksumError> {
        match self {
            Command::BreadthFirst { dirpath } => breadth_first_checksum(dirpath),
            Command::DepthFirst { dirpath } => depth_first_checksum(dirpath),
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
