use crate::checksum::ZarrDigest;
use std::collections::HashMap;
use std::fmt::{Error, Write};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ZarrChecksum {
    name: String,
    digest: String,
    size: u64,
}

impl ZarrChecksum {
    fn write_json<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write_str(r#"{"digest":"#)?;
        write_json_str(&self.digest, writer)?;
        writer.write_str(r#","name":"#)?;
        write_json_str(&self.name, writer)?;
        write!(writer, r#","size":{}}}"#, self.size)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ZarrChecksumCollection {
    directories: Vec<ZarrChecksum>,
    files: Vec<ZarrChecksum>,
}

impl ZarrChecksumCollection {
    fn write_json<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write_str(r#"{"directories":["#)?;
        for (i, d) in self.directories.iter().enumerate() {
            if i > 0 {
                writer.write_char(',')?;
            }
            d.write_json(writer)?;
        }
        writer.write_str(r#"],"files":["#)?;
        for (i, f) in self.files.iter().enumerate() {
            if i > 0 {
                writer.write_char(',')?;
            }
            f.write_json(writer)?;
        }
        writer.write_str(r#"]}"#)?;
        Ok(())
    }
}

pub fn get_checksum_json(
    files: &HashMap<String, ZarrDigest>,
    directories: &HashMap<String, ZarrDigest>,
) -> String {
    let mut filevec = Vec::new();
    for (name, digest) in files.iter() {
        filevec.push(ZarrChecksum {
            name: name.clone(),
            digest: digest.digest.clone(),
            size: digest.size,
        });
    }
    filevec.sort();
    let mut dirvec = Vec::new();
    for (name, digest) in directories.iter() {
        dirvec.push(ZarrChecksum {
            name: name.clone(),
            digest: digest.digest.clone(),
            size: digest.size,
        });
    }
    dirvec.sort();
    let collection = ZarrChecksumCollection {
        directories: dirvec,
        files: filevec,
    };
    let mut buf = String::new();
    collection.write_json(&mut buf).unwrap();
    buf
}

fn write_json_str<W: Write>(s: &str, writer: &mut W) -> Result<(), Error> {
    writer.write_char('"')?;
    for c in s.chars() {
        match c {
            '"' => writer.write_str("\"")?,
            '\\' => writer.write_str(r"\\")?,
            '\x08' => writer.write_str("\\b")?,
            '\x0C' => writer.write_str("\\f")?,
            '\n' => writer.write_str("\\n")?,
            '\r' => writer.write_str("\\r")?,
            '\t' => writer.write_str("\\t")?,
            ' '..='~' => writer.write_char(c)?,
            c => {
                let mut buf = [0u16; 2];
                for b in c.encode_utf16(&mut buf) {
                    write!(writer, "\\u{:04x}", b)?;
                }
            }
        }
    }
    writer.write_char('"')?;
    Ok(())
}
