use assert_matches::assert_matches;
use cfg_if::cfg_if;
use fs_extra::dir;
use rstest::rstest;
use rstest_reuse::{self, apply, template};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread::available_parallelism;
use tempfile::{tempdir, NamedTempFile, TempDir};
use zarr_checksum_gallery::zarr::Zarr;
use zarr_checksum_gallery::*;

cfg_if! {
    if #[cfg(unix)] {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::{symlink, PermissionsExt};
    } else if #[cfg(windows)] {
        use std::os::windows::fs::{symlink_dir, symlink_file};
    }
}

static SAMPLE_ZARR_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/sample.zarr");

static SAMPLE_CHECKSUM: &str = "4313ab36412db2981c3ed391b38604d6-5--1516";

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
    exclude_dotfiles: bool,
}

impl TestCase {
    fn zarr(&self) -> Zarr {
        match &self.input {
            Input::Permanent(path) => Zarr::new(path).exclude_dotfiles(self.exclude_dotfiles),
            Input::Temporary(dir) => Zarr::new(dir.path()).exclude_dotfiles(self.exclude_dotfiles),
            Input::TempFile(f) => Zarr::new(f.path()).exclude_dotfiles(self.exclude_dotfiles),
            Input::SubTemporary(_, path) => Zarr::new(path).exclude_dotfiles(self.exclude_dotfiles),
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
        exclude_dotfiles: false,
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
        exclude_dotfiles: false,
    })
}

fn empty_dir() -> Option<TestCase> {
    Some(TestCase {
        input: Input::Temporary(tempdir().unwrap()),
        expected: Expected::Checksum("481a2f77ab786a0f45aafd5db0971caa-0--0"),
        exclude_dotfiles: false,
    })
}

fn file_arg() -> Option<TestCase> {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path().to_path_buf();
    let checker = move |e| {
        assert_matches!(e, ChecksumError::FSError(FSError::ReaddirError { path: epath, .. }) => {
            assert_eq!(path, epath);
        })
    };
    // Unstable:
    //assert_eq!(source.kind(), std::io::ErrorKind::NotADirectory);
    Some(TestCase {
        input: Input::TempFile(tmpfile),
        expected: Expected::Error(Box::new(checker)),
        exclude_dotfiles: false,
    })
}

fn file_symlink() -> Option<TestCase> {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    let path = tmp_path.path().join("sample.zarr");
    dir::copy(SAMPLE_ZARR_PATH, &path, &opts).unwrap();
    let linkpath = path.join("arr_0").join("0");
    fs::rename(&linkpath, tmp_path.path().join("0")).unwrap();
    cfg_if! {
        if #[cfg(unix)] {
            symlink(Path::new("../../0"), linkpath).unwrap()
        } else if #[cfg(windows)] {
            if symlink_file(Path::new("..\\..\\0"), linkpath).is_err() {
                // Assume symlinks aren't enabled for us and skip the test
                return None;
            }
        } else {
            return None;
        }
    }
    Some(TestCase {
        input: Input::SubTemporary(tmp_path, path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
        exclude_dotfiles: false,
    })
}

fn dir_symlink() -> Option<TestCase> {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    let path = tmp_path.path().join("sample.zarr");
    dir::copy(SAMPLE_ZARR_PATH, &path, &opts).unwrap();
    let linkpath = path.join("arr_0");
    fs::rename(&linkpath, tmp_path.path().join("arr_0")).unwrap();
    cfg_if! {
        if #[cfg(unix)] {
            symlink(Path::new("../arr_0"), linkpath).unwrap()
        } else if #[cfg(windows)] {
            if symlink_dir(Path::new("..\\arr_0"), linkpath).is_err() {
                // Assume symlinks aren't enabled for us and skip the test
                return None;
            }
        } else {
            return None;
        }
    }
    Some(TestCase {
        input: Input::SubTemporary(tmp_path, path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
        exclude_dotfiles: false,
    })
}

fn zarr_symlink() -> Option<TestCase> {
    let tmp_path = tempdir().unwrap();
    let opts = dir::CopyOptions {
        content_only: true,
        ..dir::CopyOptions::default()
    };
    let path = tmp_path.path().join("sample.zarr");
    dir::copy(SAMPLE_ZARR_PATH, &path, &opts).unwrap();
    let linkpath = tmp_path.path().join("link.zarr");
    cfg_if! {
        if #[cfg(unix)] {
            symlink("sample.zarr", linkpath).unwrap()
        } else if #[cfg(windows)] {
            if symlink_dir("sample.zarr", linkpath).is_err() {
                // Assume symlinks aren't enabled for us and skip the test
                return None;
            }
        } else {
            return None;
        }
    }
    Some(TestCase {
        input: Input::SubTemporary(tmp_path, path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
        exclude_dotfiles: false,
    })
}

fn excluded_dotfiles() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let path = tmp_path.path();
    fs::write(path.join(".git"), "gitdir: foo/bar\n").unwrap();
    fs::create_dir(path.join(".dandi")).unwrap();
    fs::write(path.join(".dandi").join("somefile.txt"), "Hello world!\n").unwrap();
    fs::write(path.join(".gitattributes"), "* eol=lf\n").unwrap();
    fs::write(path.join("arr_0").join(".gitmodules"), "# Empty\n").unwrap();
    fs::create_dir(path.join("arr_1").join(".datalad")).unwrap();
    fs::write(
        path.join("arr_1").join(".datalad").join("config"),
        "# Empty\n",
    )
    .unwrap();
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Checksum(SAMPLE_CHECKSUM),
        exclude_dotfiles: true,
    })
}

fn unexcluded_dotfiles() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let path = tmp_path.path();
    fs::write(path.join(".git"), "gitdir: foo/bar\n").unwrap();
    fs::create_dir(path.join(".dandi")).unwrap();
    fs::write(path.join(".dandi").join("somefile.txt"), "Hello world!\n").unwrap();
    fs::write(path.join(".gitattributes"), "* eol=lf\n").unwrap();
    fs::write(path.join("arr_0").join(".gitmodules"), "# Empty\n").unwrap();
    fs::create_dir(path.join("arr_1").join(".datalad")).unwrap();
    fs::write(
        path.join("arr_1").join(".datalad").join("config"),
        "# Empty\n",
    )
    .unwrap();
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Checksum("affe15acbc00d048debc9ba4f3834577-10--1570"),
        exclude_dotfiles: false,
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
    let checker = move |e| {
        assert_matches!(e, ChecksumError::FSError(FSError::MD5FileError { path: epath, .. }) => {
            assert_eq!(path, epath);
        })
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
        exclude_dotfiles: false,
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
        assert_matches!(e, ChecksumError::FSError(FSError::ReaddirError { path: epath, .. }) => {
            assert_eq!(path, epath);
        });
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
        exclude_dotfiles: false,
    })
}

#[cfg(unix)]
fn bad_filename() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let badname = OsStr::from_bytes(b"f\xF6\xF6");
    let path = tmp_path.path().join("arr_0").join(badname);
    if fs::write(&path, "This is a file.\n").is_err() {
        // Some Unix OS's and/or filesystems (Looking at you, Apple) don't
        // allow non-UTF-8 pathnames at all.  Hence, we need to skip this test
        // on such platforms.
        return None;
    }
    let checker = move |e| {
        assert_matches!(e, ChecksumError::FSError(FSError::UndecodableName { path: epath }) => {
            assert_eq!(path, epath);
        })
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
        exclude_dotfiles: false,
    })
}

#[cfg(unix)]
fn bad_dirname() -> Option<TestCase> {
    let tmp_path = mksamplecopy();
    let badname = OsStr::from_bytes(b"f\xF6\xF6");
    let badpath = tmp_path.path().join("arr_0").join(badname);
    if fs::create_dir(&badpath).is_err() {
        // Some Unix OS's and/or filesystems (Looking at you, Apple) don't
        // allow non-UTF-8 pathnames at all.  Hence, we need to skip this test
        // on such platforms.
        return None;
    }
    fs::write(badpath.join("somefile"), "This is a file.\n").unwrap();
    let checker = move |e| {
        assert_matches!(e, ChecksumError::FSError(FSError::UndecodableName { path: epath }) => {
            assert_eq!(epath, badpath);
        })
    };
    Some(TestCase {
        input: Input::Temporary(tmp_path),
        expected: Expected::Error(Box::new(checker)),
        exclude_dotfiles: false,
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
        exclude_dotfiles: false,
    })
}

#[template]
#[rstest]
#[case(sample1())]
#[case(sample2())]
#[case(empty_dir())]
#[case(file_arg())]
#[case(file_symlink())]
#[case(dir_symlink())]
#[case(zarr_symlink())]
#[case(excluded_dotfiles())]
#[case(unexcluded_dotfiles())]
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
fn test_recursive_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = recursive_checksum(&case.zarr());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_breadth_first_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = breadth_first_checksum(&case.zarr());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_fastio_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = fastio_checksum(&case.zarr(), available_parallelism().unwrap());
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_depth_first_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = depth_first_checksum(&case.zarr());
        case.check(r);
    }
}

#[apply(test_cases)]
#[tokio::test]
async fn test_fastasync_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        let r = fastasync_checksum(&case.zarr(), available_parallelism().unwrap()).await;
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_collapsio_arc_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        if fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("[{:<5}] {}", record.level(), message))
            })
            .level(log::LevelFilter::Trace)
            .chain(std::io::stderr())
            .apply()
            .is_err()
        {
            log::set_max_level(log::LevelFilter::Trace);
        }
        let r = collapsio_arc_checksum(&case.zarr(), available_parallelism().unwrap());
        log::set_max_level(log::LevelFilter::Off);
        case.check(r);
    }
}

#[apply(test_cases)]
fn test_collapsio_mpsc_checksum(#[case] case: Option<TestCase>) {
    if let Some(case) = case {
        if fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("[{:<5}] {}", record.level(), message))
            })
            .level(log::LevelFilter::Trace)
            .chain(std::io::stderr())
            .apply()
            .is_err()
        {
            log::set_max_level(log::LevelFilter::Trace);
        }
        let r = collapsio_mpsc_checksum(&case.zarr(), available_parallelism().unwrap());
        log::set_max_level(log::LevelFilter::Off);
        case.check(r);
    }
}
