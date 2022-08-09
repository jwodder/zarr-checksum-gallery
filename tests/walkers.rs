extern crate rstest_reuse;

use cfg_if::cfg_if;
use fs_extra::dir;
use rstest::rstest;
use rstest_reuse::{apply, template};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, NamedTempFile, TempDir};
use zarr_checksum_gallery::*;

cfg_if! {
    if #[cfg(unix)] {
        use std::ffi::OsStr;
        use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};
    }
}

const SAMPLE_ZARR_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/sample.zarr");

const SAMPLE_CHECKSUM: &str = "4313ab36412db2981c3ed391b38604d6-5--1516";

enum Input {
    Permanent(PathBuf),
    Temporary(TempDir),
    TempFile(NamedTempFile),
    SubTemporary(TempDir, PathBuf),
}

enum Expected {
    Checksum(&'static str),
    Error(Box<dyn FnOnce(ChecksumError)>),
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
            Input::TempFile(f) => f.path(),
            Input::SubTemporary(_, path) => path,
        }
    }

    fn check(self, output: Result<String, ChecksumError>) {
        match (self.expected, output) {
            (Expected::Checksum(s), Ok(t)) => assert_eq!(s, t),
            (Expected::Error(func), Err(e)) => func(e),
            (Expected::Checksum(_), Err(e)) => panic!("Expected checksum, but got error: {e}"),
            (Expected::Error(_), Ok(s)) => panic!("Expected error, but got checksum {s:?}"),
        }
    }
}

fn sample1() -> Option<TestCase> {
    Some(TestCase {
        input: Input::Permanent(SAMPLE_ZARR_PATH.into()),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
    })
}

fn mksamplecopy() -> TempDir {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    dir::copy(SAMPLE_ZARR_PATH, tmp_path.path(), &opts).unwrap();
    tmp_path
}

fn sample2() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_2");
    fs::create_dir_all(path).unwrap();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_3");
    path.push("foo");
    fs::create_dir_all(path).unwrap();
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
    })
}

fn empty_dir() -> Option<TestCase> {
    Some(TestCase {
        input: Input::Temporary(tempdir().unwrap()),
        expected: Expected::Checksum("481a2f77ab786a0f45aafd5db0971caa-0--0"),
    })
}

fn file_arg() -> Option<TestCase> {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path().to_path_buf();
    let checker = move |e| match e {
        ChecksumError::WalkError(WalkError::ReaddirError { path: epath, .. }) => {
            assert_eq!(path, epath)
        }
        ChecksumError::WalkError(WalkError::NotDirRootError { path: epath }) => {
            assert_eq!(path, epath)
        }
        e => panic!("Got unexpected error: {e:?}"),
    };
    Some(TestCase {
        input: Input::TempFile(tmpfile),
        expected: Expected::Error(Box::new(checker)),
    })
}

#[cfg(unix)]
fn unreadable_file() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_0");
    path.push("unreadable");
    fs::write(&path, "You will never see this.\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let checker = move |e| match e {
        ChecksumError::WalkError(WalkError::MD5FileError { path: epath, .. }) => {
            assert_eq!(path, epath)
        }
        e => panic!("Got unexpected error: {e}"),
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
    })
}

#[cfg(unix)]
fn unreadable_dir() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let mut path = PathBuf::from(tmp_path.path());
    path.push("arr_0");
    path.push("unreadable");
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let checker = move |e| {
        // Make the directory readable again so that the temp dir can be
        // cleaned up:
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        match e {
            ChecksumError::WalkError(WalkError::ReaddirError { path: epath, .. }) => {
                assert_eq!(path, epath)
            }
            ChecksumError::WalkError(WalkError::WalkdirError { .. }) => (),
            e => panic!("Got unexpected error: {e:?}"),
        }
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
    })
}

#[cfg(unix)]
fn bad_filename() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let badname = OsStr::from_bytes(b"f\xF6\xF6");
    let mut relpath = PathBuf::new();
    relpath.push("arr_0");
    relpath.push(badname);
    let path = tmp_path.path().join(&relpath);
    if fs::write(path, "This is a file.\n").is_err() {
        // Some Unix OS's and/or filesystems (Looking at you, Apple) don't
        // allow non-UTF-8 pathnames at all.  Hence, we need to skip this test
        // on such platforms.
        return None;
    }
    let checker = move |e| match e {
        ChecksumError::WalkError(WalkError::PathDecodeError { path: epath }) => {
            assert!(epath == badname || epath == relpath, "epath = {epath:?}");
        }
        e => panic!("Got unexpected error: {e:?}"),
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
    })
}

#[cfg(unix)]
fn bad_dirname() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let badname = OsStr::from_bytes(b"f\xF6\xF6");
    let mut relpath = PathBuf::new();
    relpath.push("arr_0");
    relpath.push(badname);
    if fs::create_dir(tmp_path.path().join(&relpath)).is_err() {
        // Some Unix OS's and/or filesystems (Looking at you, Apple) don't
        // allow non-UTF-8 pathnames at all.  Hence, we need to skip this test
        // on such platforms.
        return None;
    }
    relpath.push("somefile");
    fs::write(tmp_path.path().join(&relpath), "This is a file.\n").unwrap();
    let checker = move |e| match e {
        ChecksumError::WalkError(WalkError::PathDecodeError { path: epath }) => {
            assert!(epath == badname || epath == relpath, "epath = {epath:?}");
        }
        e => panic!("Got unexpected error: {e:?}"),
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
    })
}

#[cfg(unix)]
fn bad_basedir() -> Option<TestCase> {
    let badname = OsStr::from_bytes(b"f\xF6\xF6");
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    let path = tmp_path.path().join(badname);
    if dir::copy(SAMPLE_ZARR_PATH, &path, &opts).is_err() {
        // Some Unix OS's and/or filesystems (Looking at you, Apple) don't
        // allow non-UTF-8 pathnames at all.  Hence, we need to skip this test
        // on such platforms.
        return None;
    }
    Some(TestCase {
        input: Input::SubTemporary(tmp_path, path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
    })
}

#[template]
#[rstest]
#[case(sample1())]
#[case(sample2())]
#[case(empty_dir())]
#[case(file_arg())]
fn base_cases(#[case] case: TestCase) {}

cfg_if! {
    if #[cfg(unix)] {
        #[template]
        #[apply(base_cases)]
        #[case(unreadable_file())]
        #[case(unreadable_dir())]
        #[case(bad_filename())]
        #[case(bad_dirname())]
        #[case(bad_basedir())]
        fn test_cases(#[case] case: TestCase) {}
    } else {
        #[template]
        #[apply(base_cases)]
        fn test_cases(#[case] case: TestCase) {}
    }
}

#[apply(test_cases)]
fn test_walkdir_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = walkdir_checksum(case.path());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_recursive_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = recursive_checksum(case.path());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_breadth_first_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = breadth_first_checksum(case.path());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_fastio_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = fastio_checksum(case.path(), num_cpus::get());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_depth_first_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = depth_first_checksum(case.path());
        case.check(r);
    }
}
