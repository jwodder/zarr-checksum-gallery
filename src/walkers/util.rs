use crate::errors::FSError;
use std::path::{Path, PathBuf};
use tokio::fs as afs;
use tokio_stream::wrappers::ReadDirStream;
use tokio_stream::StreamExt;

#[derive(Debug)]
pub(crate) struct DirEntry {
    pub(crate) path: PathBuf,
    pub(crate) is_dir: bool,
}

pub(crate) async fn async_listdir<P: AsRef<Path>>(dirpath: P) -> Result<Vec<DirEntry>, FSError> {
    let mut entries = Vec::new();
    let handle = afs::read_dir(&dirpath)
        .await
        .map_err(|e| FSError::readdir_error(&dirpath, e))?;
    let mut stream = ReadDirStream::new(handle);
    while let Some(p) = stream.next().await {
        let p = p.map_err(|e| FSError::readdir_error(&dirpath, e))?;
        let path = p.path();
        let ftype = p
            .file_type()
            .await
            .map_err(|e| FSError::stat_error(&path, e))?;
        let is_dir = ftype.is_dir()
            || (ftype.is_symlink()
                && afs::metadata(&path)
                    .await
                    .map_err(|e| FSError::stat_error(&path, e))?
                    .is_dir());
        entries.push(DirEntry { path, is_dir });
    }
    Ok(entries)
}
