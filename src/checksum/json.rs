use super::nodes::*;
use std::fmt::{Error, Write};

pub(super) fn get_checksum_json(
    files: Vec<FileChecksumNode>,
    directories: Vec<DirChecksumNode>,
) -> String {
    let mut filevec = files.into_iter().map(JSONEntry::from).collect::<Vec<_>>();
    filevec.sort();
    let mut dirvec = directories
        .into_iter()
        .filter(|node| node.file_count > 0)
        .map(JSONEntry::from)
        .collect::<Vec<_>>();
    dirvec.sort();
    let collection = JSONEntryCollection {
        directories: dirvec,
        files: filevec,
    };
    let mut buf = String::new();
    collection.write_json(&mut buf).unwrap();
    buf
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct JSONEntry {
    name: String,
    digest: String,
    size: u64,
}

impl JSONEntry {
    fn write_json<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write_str(r#"{"digest":"#)?;
        write_json_str(&self.digest, writer)?;
        writer.write_str(r#","name":"#)?;
        write_json_str(&self.name, writer)?;
        write!(writer, r#","size":{}}}"#, self.size)?;
        Ok(())
    }
}

impl From<FileChecksumNode> for JSONEntry {
    fn from(node: FileChecksumNode) -> JSONEntry {
        JSONEntry {
            name: node.name().to_string(),
            digest: node.checksum,
            size: node.size,
        }
    }
}

impl From<DirChecksumNode> for JSONEntry {
    fn from(node: DirChecksumNode) -> JSONEntry {
        JSONEntry {
            name: node.name().to_string(),
            digest: node.checksum,
            size: node.size,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct JSONEntryCollection {
    directories: Vec<JSONEntry>,
    files: Vec<JSONEntry>,
}

impl JSONEntryCollection {
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

fn write_json_str<W: Write>(s: &str, writer: &mut W) -> Result<(), Error> {
    writer.write_char('"')?;
    for c in s.chars() {
        match c {
            '"' => writer.write_str("\\\"")?,
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

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("foobar", r#""foobar""#)]
    #[case("foo / bar", r#""foo / bar""#)]
    #[case("foo\"bar", r#""foo\"bar""#)]
    #[case("foo\\bar", r#""foo\\bar""#)]
    #[case("foo\x08\x0C\n\r\tbar", r#""foo\b\f\n\r\tbar""#)]
    #[case("foo\x0B\x1B\x7Fbar", r#""foo\u000b\u001b\u007fbar""#)]
    #[case("foo—bar", r#""foo\u2014bar""#)]
    #[case("foo🐐bar", r#""foo\ud83d\udc10bar""#)]
    fn test_write_json_str(#[case] s: &str, #[case] json: String) {
        let mut buf = String::new();
        write_json_str(s, &mut buf).unwrap();
        assert_eq!(buf, json);
    }

    #[test]
    fn test_get_checksum_json() {
        let files = vec![
            FileChecksumNode {
                relpath: "foo".into(),
                checksum: "0123456789abcdef0123456789abcdef".into(),
                size: 69105,
            },
            FileChecksumNode {
                relpath: "bar".into(),
                checksum: "abcdef0123456789abcdef0123456789".into(),
                size: 42,
            },
        ];
        let directories = Vec::from([DirChecksumNode {
            relpath: "quux".into(),
            checksum: "0987654321fedcba0987654321fedcba-23--65537".into(),
            size: 65537,
            file_count: 23,
        }]);
        let json = get_checksum_json(files, directories);
        assert_eq!(
            json,
            r#"{"directories":[{"digest":"0987654321fedcba0987654321fedcba-23--65537","name":"quux","size":65537}],"files":[{"digest":"abcdef0123456789abcdef0123456789","name":"bar","size":42},{"digest":"0123456789abcdef0123456789abcdef","name":"foo","size":69105}]}"#
        );
    }

    #[test]
    fn test_get_checksum_json_empty_dir() {
        let files = Vec::new();
        let directories = vec![DirChecksumNode {
            relpath: "quux".into(),
            checksum: "481a2f77ab786a0f45aafd5db0971caa-0--0".into(),
            size: 0,
            file_count: 0,
        }];
        let json = get_checksum_json(files, directories);
        assert_eq!(json, r#"{"directories":[],"files":[]}"#);
    }
}
