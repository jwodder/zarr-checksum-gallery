use dandi_zarr_checksum::{recursive_checksum, walkdir_checksum};
use std::path::PathBuf;

const SAMPLE_CHECKSUM: &str = "4313ab36412db2981c3ed391b38604d6-5--1516";

fn sample_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path.push("sample.zarr");
    path
}

#[test]
fn test_walkdir_checksum() {
    assert_eq!(walkdir_checksum(sample_path()), SAMPLE_CHECKSUM);
}

#[test]
fn test_recursive_checksum() {
    assert_eq!(recursive_checksum(sample_path()), SAMPLE_CHECKSUM);
}
