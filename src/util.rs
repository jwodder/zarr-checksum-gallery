use crate::errors::FSError;
use md5::{Digest, Md5};
use std::fs;
use std::io;
use std::path::Path;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

pub(crate) fn md5_string(s: &str) -> String {
    hex::encode(Md5::digest(s))
}

pub(crate) fn md5_file<P: AsRef<Path>>(path: P) -> Result<String, FSError> {
    let path = path.as_ref();
    let mut file = fs::File::open(&path).map_err(|e| FSError::md5_file_error(&path, e))?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher).map_err(|e| FSError::md5_file_error(&path, e))?;
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) async fn async_md5_file<P: AsRef<Path>>(path: P) -> Result<String, FSError> {
    let path = path.as_ref();
    let fp = tokio::fs::File::open(&path)
        .await
        .map_err(|e| FSError::md5_file_error(&path, e))?;
    let mut stream = ReaderStream::new(fp);
    let mut hasher = Md5::new();
    while let Some(chunk) = stream.next().await {
        hasher.update(chunk.map_err(|e| FSError::md5_file_error(&path, e))?);
    }
    Ok(hex::encode(hasher.finalize()))
}
