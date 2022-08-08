extern crate rstest_reuse;

use cfg_if::cfg_if;
use dandi_zarr_checksum::*;
use fs_extra::dir;
use rstest::rstest;
use rstest_reuse::{apply, template};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const SAMPLE_CHECKSUM: &str = "4313ab36412db2981c3ed391b38604d6-5--1516";

enum Input {
    Permanent(PathBuf),
    Temporary(TempDir),
}

enum Expected {
    Checksum(&'static str),
    Error(Box<dyn FnOnce(ZarrError)>),
}

struct TestCase {
    input: Input,
    expected: Expected,
}

impl TestCase {
    fn path(&self) -> &Path {
        match &self.input {
            Input::Permanent(path) => path,
            Input::Temporary(dir) => dir.path(),
        }
    }

    fn check(self, output: Result<String, ZarrError>) {
        match (self.expected, output) {
            (Expected::Checksum(s), Ok(t)) => assert_eq!(s, t),
            (Expected::Error(func), Err(e)) => func(e),
            (Expected::Checksum(_), Err(e)) => panic!("Expected checksum, but got error: {e}"),
            (Expected::Error(_), Ok(s)) => panic!("Expected error, but got checksum {s:?}"),
        }
    }
}

fn sample1() -> TestCase {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("data");
    path.push("sample.zarr");
    TestCase {
        input: Input::Permanent(path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
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
    fs::create_dir_all(path).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_3");
    path.push("foo");
    fs::create_dir_all(path).unwrap();
    TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
    }
}

fn empty_dir() -> TestCase {
    TestCase {
        input: Input::Temporary(tempdir().unwrap()),
        expected: Expected::Checksum("481a2f77ab786a0f45aafd5db0971caa-0--0"),
    }
}

#[cfg(unix)]
fn unreadable_file() -> TestCase {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    dir::copy(sample1().path(), tmp_path.path(), &opts).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_0");
    path.push("unreadable");
    fs::write(&path, "You will never see this.\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let checker = move |e| match e {
        ZarrError::MD5FileError { path: epath, .. } => assert_eq!(path, epath),
        e => panic!("Got unexpected error: {e}"),
    };
    TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
    }
}

#[cfg(unix)]
fn unreadable_dir() -> TestCase {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    dir::copy(sample1().path(), tmp_path.path(), &opts).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_0");
    path.push("unreadable");
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let checker = move |e| match e {
        ZarrError::ReaddirError { path: epath, .. } => assert_eq!(path, epath),
        ZarrError::WalkdirError { .. } => (),
        e => panic!("Got unexpected error: {e:?}"),
    };
    TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
    }
}

#[template]
#[rstest]
#[case(sample1())]
#[case(sample2())]
#[case(empty_dir())]
fn base_cases(#[case] case: TestCase) {}

cfg_if! {
    if #[cfg(unix)] {
        #[template]
        #[apply(base_cases)]
        #[case(unreadable_file())]
        #[case(unreadable_dir())]
        fn test_cases(#[case] case: TestCase) {}
    } else {
        #[template]
        #[apply(base_cases)]
        fn test_cases(#[case] case: TestCase) {}
    }
}

#[apply(test_cases)]
fn test_walkdir_checksum(#[case] case: TestCase) {
    let r = walkdir_checksum(case.path());
    case.check(r);
}

#[apply(test_cases)]
fn test_recursive_checksum(#[case] case: TestCase) {
    let r = recursive_checksum(case.path());
    case.check(r);
}

#[apply(test_cases)]
fn test_breadth_first_checksum(#[case] case: TestCase) {
    let r = breadth_first_checksum(case.path());
    case.check(r);
}

#[apply(test_cases)]
fn test_fastio_checksum(#[case] case: TestCase) {
    let r = fastio_checksum(case.path(), num_cpus::get());
    case.check(r);
}

#[apply(test_cases)]
fn test_depth_first_checksum(#[case] case: TestCase) {
    let r = depth_first_checksum(case.path());
    case.check(r);
}
