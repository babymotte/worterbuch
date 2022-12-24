mod client;
mod server;

pub use client::*;
pub use server::*;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub type MessageType = u8;
pub type TransactionId = u64;
pub type RequestPattern = String;
pub type RequestPatterns = Vec<RequestPattern>;
pub type Key = String;
pub type Value = String;
pub type KeyValuePairs = Vec<KeyValuePair>;
pub type TypedKeyValuePairs<T> = Vec<TypedKeyValuePair<T>>;
pub type ErrorCode = u8;
pub type MetaData = String;
pub type PathLength = u16;
pub type Path = String;
pub type Separator = char;
pub type Wildcard = char;
pub type MultiWildcard = char;
pub type ProtocolVersionSegment = u16;
pub type ProtocolVersions = Vec<ProtocolVersion>;
pub type LastWill = KeyValuePairs;
pub type GraveGoods = RequestPatterns;
pub type UniqueFlag = u8;

pub type RequestPatternLength = u16;
pub type KeyLength = u16;
pub type ValueLength = u32;
pub type MetaDataLength = u32;
pub type NumKeyValuePairs = u32;
pub type NumProtocolVersions = u8;
pub type NumLastWill = u8;
pub type NumGraveGoods = u8;

pub const TRANSACTION_ID_BYTES: usize = (TransactionId::BITS / 8) as usize;
pub const REQUEST_PATTERN_LENGTH_BYTES: usize = (RequestPatternLength::BITS / 8) as usize;
pub const KEY_LENGTH_BYTES: usize = (KeyLength::BITS / 8) as usize;
pub const VALUE_LENGTH_BYTES: usize = (ValueLength::BITS / 8) as usize;
pub const NUM_KEY_VALUE_PAIRS_BYTES: usize = (NumKeyValuePairs::BITS / 8) as usize;
pub const ERROR_CODE_BYTES: usize = (ErrorCode::BITS / 8) as usize;
pub const METADATA_LENGTH_BYTES: usize = (MetaDataLength::BITS / 8) as usize;
pub const PATH_LENGTH_BYTES: usize = (PathLength::BITS / 8) as usize;
pub const UNIQUE_FLAG_BYTES: usize = (UniqueFlag::BITS / 8) as usize;
pub const PROTOCOL_VERSION_SEGMENT_BYTES: usize = (ProtocolVersionSegment::BITS / 8) as usize;
pub const NUM_PROTOCOL_VERSION_BYTES: usize = (NumProtocolVersions::BITS / 8) as usize;
pub const NUM_LAST_WILL_BYTES: usize = (NumLastWill::BITS / 8) as usize;
pub const NUM_GRAVE_GOODS_BYTES: usize = (NumGraveGoods::BITS / 8) as usize;
pub const SEPARATOR_BYTES: usize = 1;
pub const WILDCARD_BYTES: usize = 1;
pub const MULTI_WILDCARD_BYTES: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolVersion {
    pub major: ProtocolVersionSegment,
    pub minor: ProtocolVersionSegment,
}

impl ProtocolVersion {
    pub fn new(major: ProtocolVersionSegment, minor: ProtocolVersionSegment) -> Self {
        ProtocolVersion { major, minor }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValuePair {
    pub key: Key,
    pub value: Value,
}

pub struct TypedKeyValuePair<T: DeserializeOwned> {
    pub key: Key,
    pub value: T,
}

impl From<(String, String)> for KeyValuePair {
    fn from((key, value): (String, String)) -> Self {
        KeyValuePair { key, value }
    }
}

impl From<(&str, &str)> for KeyValuePair {
    fn from((key, value): (&str, &str)) -> Self {
        KeyValuePair {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use crate::ProtocolVersion as PV;

    #[test]
    fn protocol_versions_are_sorted_correctly() {
        assert_eq!(PV::new(0, 1).cmp(&PV::new(0, 2)), Ordering::Less);
        assert_eq!(PV::new(0, 9).cmp(&PV::new(1, 0)), Ordering::Less);
        assert_eq!(PV::new(1, 2).cmp(&PV::new(1, 3)), Ordering::Less);

        assert_eq!(PV::new(0, 1).cmp(&PV::new(0, 1)), Ordering::Equal);
        assert_eq!(PV::new(0, 9).cmp(&PV::new(0, 9)), Ordering::Equal);
        assert_eq!(PV::new(1, 2).cmp(&PV::new(1, 2)), Ordering::Equal);

        assert_eq!(PV::new(0, 2).cmp(&PV::new(0, 1)), Ordering::Greater);
        assert_eq!(PV::new(1, 0).cmp(&PV::new(0, 9)), Ordering::Greater);
        assert_eq!(PV::new(1, 3).cmp(&PV::new(1, 2)), Ordering::Greater);

        assert_eq!(PV::new(0, 3), PV::new(0, 3).min(PV::new(0, 5)));
        assert_eq!(PV::new(0, 8), PV::new(0, 8).min(PV::new(1, 2)));
        assert_eq!(PV::new(2, 3), PV::new(2, 3).min(PV::new(3, 1)));

        assert_eq!(PV::new(0, 3), PV::new(0, 5).min(PV::new(0, 3)));
        assert_eq!(PV::new(0, 8), PV::new(1, 2).min(PV::new(0, 8)));
        assert_eq!(PV::new(2, 3), PV::new(3, 1).min(PV::new(2, 3)));

        let mut versions = vec![PV::new(1, 2), PV::new(0, 4), PV::new(9, 0), PV::new(3, 5)];
        versions.sort();
        assert_eq!(
            vec![PV::new(0, 4), PV::new(1, 2), PV::new(3, 5), PV::new(9, 0)],
            versions
        );
    }
}
