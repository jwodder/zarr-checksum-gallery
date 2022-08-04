use clap::{Parser, Subcommand};
use dandi_zarr_checksum::*;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[clap(version)]
struct Arguments {
    #[clap(short, long)]
    debug: bool,

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

fn main() -> Result<(), ZarrError> {
    let args = Arguments::parse();
    if args.debug {
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("[{:<5}] {}", record.level(), message))
            })
            .level(log::LevelFilter::Debug)
            .chain(std::io::stderr())
            .apply()
            .unwrap();
    }
    let checksum = match args.command {
        Command::Walkdir { dirpath } => walkdir_checksum(dirpath),
        Command::Recursive { dirpath } => recursive_checksum(dirpath),
        Command::BreadthFirst { dirpath } => breadth_first_checksum(dirpath),
        Command::DepthFirst { dirpath } => depth_first_checksum(dirpath),
        Command::Fastio { threads, dirpath } => fastio_checksum(dirpath, threads),
    };
    println!("{}", checksum?);
    Ok(())
}
