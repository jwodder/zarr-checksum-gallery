extern crate rstest_reuse;

use dandi_zarr_checksum::*;
use fs_extra::dir;
use rstest::rstest;
use rstest_reuse::{apply, template};
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

const SAMPLE_CHECKSUM: &str = "4313ab36412db2981c3ed391b38604d6-5--1516";

enum TestCase {
    Permanent {
        path: PathBuf,
        checksum: &'static str,
    },
    Temporary {
        dir: TempDir,
        checksum: &'static str,
    },
}

impl TestCase {
    fn path(&self) -> &Path {
        match self {
            TestCase::Permanent { path, .. } => path,
            TestCase::Temporary { dir, .. } => dir.path(),
        }
    }

    fn checksum(&self) -> &'static str {
        match self {
            TestCase::Permanent { checksum, .. } => checksum,
            TestCase::Temporary { checksum, .. } => checksum,
        }
    }
}

fn sample1() -> TestCase {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path.push("sample.zarr");
    TestCase::Permanent {
        path,
        checksum: SAMPLE_CHECKSUM,
    }
}

fn sample2() -> TestCase {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    dir::copy(sample1().path(), tmp_path.path(), &opts).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_2");
    create_dir_all(path).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_3");
    path.push("foo");
    create_dir_all(path).unwrap();
    TestCase::Temporary {
        dir: tmp_path,
        checksum: SAMPLE_CHECKSUM,
    }
}

fn empty_dir() -> TestCase {
    TestCase::Temporary {
        dir: tempdir().unwrap(),
        checksum: "481a2f77ab786a0f45aafd5db0971caa-0--0",
    }
}

#[template]
#[rstest]
#[case(sample1())]
#[case(sample2())]
#[case(empty_dir())]
fn test_cases(#[case] case: TestCase) {}

#[apply(test_cases)]
fn test_walkdir_checksum(#[case] case: TestCase) {
    assert_eq!(walkdir_checksum(case.path()).unwrap(), case.checksum());
}

#[apply(test_cases)]
fn test_recursive_checksum(#[case] case: TestCase) {
    assert_eq!(recursive_checksum(case.path()).unwrap(), case.checksum());
}

#[apply(test_cases)]
fn test_breadth_first_checksum(#[case] case: TestCase) {
    assert_eq!(
        breadth_first_checksum(case.path()).unwrap(),
        case.checksum()
    );
}

#[apply(test_cases)]
fn test_fastio_checksum(#[case] case: TestCase) {
    assert_eq!(
        fastio_checksum(case.path(), num_cpus::get()).unwrap(),
        case.checksum()
    );
}

#[apply(test_cases)]
fn test_depth_first_checksum(#[case] case: TestCase) {
    assert_eq!(depth_first_checksum(case.path()).unwrap(), case.checksum());
}
