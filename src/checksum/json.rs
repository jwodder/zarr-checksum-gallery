use super::nodes::*;
use std::fmt::{Error, Write};

pub(super) fn get_checksum_json<'a, FI, DI>(files: FI, directories: DI) -> String
where
    FI: IntoIterator<Item = &'a FileChecksum>,
    DI: IntoIterator<Item = &'a DirChecksum>,
{
    let mut filevec = files.into_iter().map(JSONEntry::from).collect::<Vec<_>>();
    filevec.sort_unstable();
    let mut dirvec = directories
        .into_iter()
        .filter(|node| node.file_count > 0)
        .map(JSONEntry::from)
        .collect::<Vec<_>>();
    dirvec.sort_unstable();
    let collection = JSONEntryCollection {
        directories: dirvec,
        files: filevec,
    };
    let mut buf = String::new();
    collection.write_json(&mut buf).unwrap();
    buf
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct JSONEntry<'a> {
    name: &'a str,
    digest: &'a str,
    size: u64,
}

impl<'a> JSONEntry<'a> {
    fn write_json<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write_str(r#"{"digest":"#)?;
        write_json_str(self.digest, writer)?;
        writer.write_str(r#","name":"#)?;
        write_json_str(self.name, writer)?;
        write!(writer, r#","size":{}}}"#, self.size)?;
        Ok(())
    }
}

impl<'a> From<&'a FileChecksum> for JSONEntry<'a> {
    fn from(node: &'a FileChecksum) -> JSONEntry<'a> {
        JSONEntry {
            name: node.name(),
            digest: node.checksum(),
            size: node.size,
        }
    }
}

impl<'a> From<&'a DirChecksum> for JSONEntry<'a> {
    fn from(node: &'a DirChecksum) -> JSONEntry<'a> {
        JSONEntry {
            name: node.name(),
            digest: node.checksum(),
            size: node.size,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct JSONEntryCollection<'a> {
    directories: Vec<JSONEntry<'a>>,
    files: Vec<JSONEntry<'a>>,
}

impl<'a> JSONEntryCollection<'a> {
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
                    write!(writer, "\\u{b:04x}")?;
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
            FileChecksum {
                relpath: "foo".try_into().unwrap(),
                checksum: "0123456789abcdef0123456789abcdef".into(),
                size: 69105,
            },
            FileChecksum {
                relpath: "bar".try_into().unwrap(),
                checksum: "abcdef0123456789abcdef0123456789".into(),
                size: 42,
            },
        ];
        let directories = Vec::from([DirChecksum {
            relpath: "quux".try_into().unwrap(),
            checksum: "0987654321fedcba0987654321fedcba-23--65537".into(),
            size: 65537,
            file_count: 23,
        }]);
        let json = get_checksum_json(files.iter(), directories.iter());
        assert_eq!(
            json,
            r#"{"directories":[{"digest":"0987654321fedcba0987654321fedcba-23--65537","name":"quux","size":65537}],"files":[{"digest":"abcdef0123456789abcdef0123456789","name":"bar","size":42},{"digest":"0123456789abcdef0123456789abcdef","name":"foo","size":69105}]}"#
        );
    }

    #[test]
    fn test_get_checksum_json_empty_dir() {
        let files = Vec::new();
        let directories = vec![DirChecksum {
            relpath: "quux".try_into().unwrap(),
            checksum: "481a2f77ab786a0f45aafd5db0971caa-0--0".into(),
            size: 0,
            file_count: 0,
        }];
        let json = get_checksum_json(files.iter(), directories.iter());
        assert_eq!(json, r#"{"directories":[],"files":[]}"#);
    }
}
