mod client;
mod decode;
mod encode;
pub mod error;
pub mod nonblocking;
mod server;

pub use client::*;
pub use decode::*;
pub use encode::*;
pub use server::*;

use serde::{Deserialize, Serialize};

pub type MessageType = u8;
pub type TransactionId = u64;
pub type RequestPattern = String;
pub type Key = String;
pub type Value = String;
pub type KeyValuePairs = Vec<KeyValuePair>;
pub type ErrorCode = u8;
pub type MetaData = String;
pub type PathLength = u16;
pub type Path = String;
pub type Separator = char;
pub type Wildcard = char;
pub type MultiWildcard = char;
pub type ProtocolVersionSegment = u16;
pub type ProtocolVersions = Vec<ProtocolVersion>;

pub type RequestPatternLength = u16;
pub type KeyLength = u16;
pub type ValueLength = u32;
pub type MetaDataLength = u32;
pub type NumKeyValuePairs = u32;
pub type NumProtocolVersions = u8;

pub const TRANSACTION_ID_BYTES: usize = 8;
pub const REQUEST_PATTERN_LENGTH_BYTES: usize = 2;
pub const KEY_LENGTH_BYTES: usize = 2;
pub const VALUE_LENGTH_BYTES: usize = 4;
pub const NUM_KEY_VALUE_PAIRS_BYTES: usize = 4;
pub const ERROR_CODE_BYTES: usize = 1;
pub const METADATA_LENGTH_BYTES: usize = 4;
pub const PATH_LENGTH_BYTES: usize = 2;
pub const UNIQUE_FLAG_BYTES: usize = 1;
pub const PROTOCOL_VERSION_SEGMENT_BYTES: usize = 2;
pub const NUM_PROTOCOL_VERSION_BYTES: usize = 1;
pub const SEPARATOR_BYTES: usize = 1;
pub const WILDCARD_BYTES: usize = 1;
pub const MULTI_WILDCARD_BYTES: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolVersion {
    pub major: ProtocolVersionSegment,
    pub minor: ProtocolVersionSegment,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValuePair {
    pub key: Key,
    pub value: Value,
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
