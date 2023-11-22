use crate::errors::FSError;
use fs_err::{tokio::File as TokioFile, File};
use md5::{Digest, Md5};
use std::path::Path;
use tokio::io::AsyncReadExt;

/// Compute the MD5 hash of a string (encoded in UTF-8) and return the hash as
/// a string of lowercase hexadecimal digits
pub(crate) fn md5_string(s: &str) -> String {
    hex::encode(Md5::digest(s))
}

/// Compute the MD5 hash of the contents of the given file, returning a string
/// of lowercase hexadecimal digits
pub(crate) fn md5_file<P: AsRef<Path>>(path: P) -> Result<String, FSError> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    let mut hasher = Md5::new();
    std::io::copy(&mut file, &mut hasher).map_err(|source| FSError::Digest {
        path: path.into(),
        source,
    })?;
    Ok(hex::encode(hasher.finalize()))
}

/// Compute the MD5 hash of the contents of the given file asynchronously,
/// returning a string of lowercase hexadecimal digits
pub(crate) async fn async_md5_file<P: AsRef<Path> + Send>(path: P) -> Result<String, FSError> {
    let path = path.as_ref();
    let mut fp = TokioFile::open(path).await?;
    let mut hasher = Md5::new();
    let mut buffer = bytes::BytesMut::with_capacity(4096);
    loop {
        match fp.read_buf(&mut buffer).await {
            Ok(0) => break,
            Ok(_) => {
                hasher.update(&buffer);
                buffer.clear();
            }
            Err(source) => {
                return Err(FSError::Digest {
                    path: path.into(),
                    source,
                })
            }
        }
    }
    Ok(hex::encode(hasher.finalize()))
}
