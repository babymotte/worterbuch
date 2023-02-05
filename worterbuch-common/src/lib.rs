mod client;
pub mod error;
mod server;

pub use client::*;
use error::WorterbuchResult;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub use server::*;
use std::{fmt, ops::Deref};

pub type TransactionId = u64;
pub type RequestPattern = String;
pub type RequestPatterns = Vec<RequestPattern>;
pub type Key = String;
pub type Value = serde_json::Value;
pub type KeyValuePairs = Vec<KeyValuePair>;
pub type TypedKeyValuePairs<T> = Vec<TypedKeyValuePair<T>>;
pub type ErrorCode = u8;
pub type MetaData = String;
pub type Path = String;
pub type Separator = char;
pub type Wildcard = char;
pub type MultiWildcard = char;
pub type ProtocolVersionSegment = u16;
pub type ProtocolVersions = Vec<ProtocolVersion>;
pub type LastWill = KeyValuePairs;
pub type GraveGoods = RequestPatterns;
pub type UniqueFlag = u8;

pub const SEPARATOR: &str = "/";

#[macro_export]
macro_rules! topic {
    ( $sep:expr, $( $x:expr ),+ ) => {
        {
            let mut segments = Vec::new();
            $(
                segments.push($x.to_string());
            )+
            segments.join(&$sep.to_string())
        }
    };
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValuePair {
    pub key: Key,
    pub value: Value,
}

impl From<KeyValuePair> for Option<Value> {
    fn from(kvp: KeyValuePair) -> Self {
        Some(kvp.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedKeyValuePair<T: DeserializeOwned> {
    pub key: Key,
    pub value: T,
}

impl<T: DeserializeOwned> TryFrom<KeyValuePair> for TypedKeyValuePair<T> {
    type Error = serde_json::Error;

    fn try_from(kvp: KeyValuePair) -> Result<Self, Self::Error> {
        let deserialized = serde_json::from_value(kvp.value)?;
        Ok(TypedKeyValuePair {
            key: kvp.key,
            value: deserialized,
        })
    }
}

impl From<(String, serde_json::Value)> for KeyValuePair {
    fn from((key, value): (String, serde_json::Value)) -> Self {
        KeyValuePair { key, value }
    }
}

impl From<(&str, serde_json::Value)> for KeyValuePair {
    fn from((key, value): (&str, serde_json::Value)) -> Self {
        KeyValuePair {
            key: key.to_owned(),
            value,
        }
    }
}

impl TryFrom<(String, &str)> for KeyValuePair {
    type Error = serde_json::Error;

    fn try_from((key, value): (String, &str)) -> Result<Self, Self::Error> {
        let value = serde_json::from_str(value)?;
        Ok(KeyValuePair { key, value })
    }
}

impl TryFrom<(&str, &str)> for KeyValuePair {
    type Error = serde_json::Error;

    fn try_from((key, value): (&str, &str)) -> Result<Self, Self::Error> {
        let value = serde_json::from_str(value)?;
        Ok(KeyValuePair {
            key: key.to_owned(),
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct RegularKeySegment(pub String);

impl RegularKeySegment {
    pub fn parse(pattern: &str) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let mut segments = Vec::new();
        for segment in pattern.split(SEPARATOR) {
            let ks: KeySegment = segment.into();
            match ks {
                KeySegment::Regular(reg) => segments.push(reg),
                KeySegment::Wildcard => {
                    return Err(error::WorterbuchError::IllegalWildcard(pattern.to_owned()))
                }
                KeySegment::MultiWildcard => {
                    return Err(error::WorterbuchError::IllegalMultiWildcard(
                        pattern.to_owned(),
                    ))
                }
            }
        }
        Ok(segments)
    }
}

impl Deref for RegularKeySegment {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for RegularKeySegment {
    fn from(str: String) -> Self {
        RegularKeySegment(str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeySegment {
    Regular(RegularKeySegment),
    Wildcard,
    MultiWildcard,
    // RegexWildcard(String),
}

impl From<RegularKeySegment> for KeySegment {
    fn from(reg: RegularKeySegment) -> Self {
        Self::Regular(reg)
    }
}

impl Deref for KeySegment {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            KeySegment::Regular(reg) => &reg,
            KeySegment::Wildcard => "?",
            KeySegment::MultiWildcard => "#",
        }
    }
}

impl fmt::Display for KeySegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeySegment::Regular(segment) => segment.fmt(f),
            KeySegment::Wildcard => write!(f, "?"),
            KeySegment::MultiWildcard => write!(f, "#"),
            // PathSegment::RegexWildcard(regex) => write!(f, "?{regex}?"),
        }
    }
}

impl From<&str> for KeySegment {
    fn from(str: &str) -> Self {
        match str {
            "?" => KeySegment::Wildcard,
            "#" => KeySegment::MultiWildcard,
            other => KeySegment::Regular(other.to_owned().into()),
        }
    }
}

impl KeySegment {
    pub fn parse(pattern: &str) -> Vec<KeySegment> {
        let segments = pattern.split(SEPARATOR);
        segments.map(KeySegment::from).collect()
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
