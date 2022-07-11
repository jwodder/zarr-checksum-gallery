use clap::Parser;
use dandi_zarr_checksum::Walker;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[clap(version)]
struct Arguments {
    #[clap(value_enum)]
    walker: Walker,
    dirpath: PathBuf,
}

fn main() {
    let args = Arguments::parse();
    println!("{}", args.walker.run(args.dirpath));
}
