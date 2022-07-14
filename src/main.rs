use clap::Parser;
use dandi_zarr_checksum::{Walker, ZarrError};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[clap(version)]
struct Arguments {
    #[clap(value_enum)]
    walker: Walker,
    dirpath: PathBuf,
}

fn main() -> Result<(), ZarrError> {
    let args = Arguments::parse();
    println!("{}", args.walker.run(args.dirpath)?);
    Ok(())
}
