use crate::checksum::ZarrDigest;
use std::collections::HashMap;

#[allow(unused_variables)]
pub fn get_checksum_json(
    files: &HashMap<String, ZarrDigest>,
    directories: &HashMap<String, ZarrDigest>,
) -> String {
    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct ZarrChecksum {
        digest: String,
        name: String,
        size: u64,
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct ZarrChecksumCollection {
        files: Vec<ZarrChecksum>,
        directories: Vec<ZarrChecksum>,
    }

    todo!()
}
