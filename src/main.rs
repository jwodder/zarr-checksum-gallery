use clap::{Parser, Subcommand};
use dandi_zarr_checksum::{
    depth_first_checksum, fastio_checksum, recursive_checksum, walkdir_checksum, ZarrError,
};
use log::Level;
use std::path::PathBuf;
use stderrlog::ColorChoice;

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
    DepthFirst {
        dirpath: PathBuf,
    },
    Fastio {
        #[clap(short, long, default_value_t = 5)]
        threads: usize,
        dirpath: PathBuf,
    },
}

fn main() -> Result<(), ZarrError> {
    let args = Arguments::parse();
    if args.debug {
        stderrlog::new()
            .verbosity(Level::Debug)
            // The threading causes the colors to be applied to stdout as well
            .color(ColorChoice::Never)
            .init()
            .unwrap();
    }
    let checksum = match args.command {
        Command::Walkdir { dirpath } => walkdir_checksum(dirpath),
        Command::Recursive { dirpath } => recursive_checksum(dirpath),
        Command::DepthFirst { dirpath } => depth_first_checksum(dirpath),
        Command::Fastio { threads, dirpath } => fastio_checksum(dirpath, threads),
    };
    println!("{}", checksum?);
    Ok(())
}
