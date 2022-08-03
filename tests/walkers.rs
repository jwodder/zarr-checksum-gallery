use dandi_zarr_checksum::*;
use fs_extra::dir;
use std::fs::create_dir_all;
use std::path::PathBuf;
use tempfile::{tempdir, TempDir};

const SAMPLE_CHECKSUM: &str = "4313ab36412db2981c3ed391b38604d6-5--1516";

fn sample_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path.push("sample.zarr");
    path
}

fn sample2() -> TempDir {
    let tmp_path = tempdir().unwrap();
    dir::copy(sample_path(), tmp_path.path(), &dir::CopyOptions::new()).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("sample.zarr");
    path.push("arr_2");
    create_dir_all(path).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("sample.zarr");
    path.push("arr_3");
    path.push("foo");
    create_dir_all(path).unwrap();
    tmp_path
}

#[test]
fn test_walkdir_checksum() {
    assert_eq!(walkdir_checksum(sample_path()).unwrap(), SAMPLE_CHECKSUM);
}

#[test]
fn test_walkdir_checksum2() {
    assert_eq!(
        walkdir_checksum(sample2().path().join("sample.zarr")).unwrap(),
        SAMPLE_CHECKSUM
    );
}

#[test]
fn test_recursive_checksum() {
    assert_eq!(recursive_checksum(sample_path()).unwrap(), SAMPLE_CHECKSUM);
}

#[test]
fn test_recursive_checksum2() {
    assert_eq!(
        recursive_checksum(sample2().path().join("sample.zarr")).unwrap(),
        SAMPLE_CHECKSUM
    );
}

#[test]
fn test_breadth_first_checksum() {
    assert_eq!(
        breadth_first_checksum(sample_path()).unwrap(),
        SAMPLE_CHECKSUM
    );
}

#[test]
fn test_breadth_first_checksum2() {
    assert_eq!(
        breadth_first_checksum(sample2().path().join("sample.zarr")).unwrap(),
        SAMPLE_CHECKSUM
    );
}

#[test]
fn test_fastio_checksum() {
    assert_eq!(fastio_checksum(sample_path(), 5).unwrap(), SAMPLE_CHECKSUM);
}

#[test]
fn test_fastio_checksum2() {
    assert_eq!(
        fastio_checksum(sample2().path().join("sample.zarr"), 5).unwrap(),
        SAMPLE_CHECKSUM
    );
}
