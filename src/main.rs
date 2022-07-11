use dandi_zarr_checksum::walkdir_checksum;
use std::env::args;

fn main() {
    match args().nth(1) {
        Some(p) => println!("{}", walkdir_checksum(p)),
        None => panic!("No directory path provided"),
    }
}
