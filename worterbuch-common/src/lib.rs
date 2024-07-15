/*
 *  Worterbuch common modules library
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

pub mod benchmark;
mod client;
pub mod error;
mod server;
pub mod tcp;

pub use client::*;
use serde_json::json;
pub use server::*;

use error::WorterbuchResult;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::*;
use sha2::{Digest, Sha256};
use std::{fmt, ops::Deref};

pub const SYSTEM_TOPIC_ROOT: &str = "$SYS";
pub const SYSTEM_TOPIC_ROOT_PREFIX: &str = "$SYS/";
pub const SYSTEM_TOPIC_CLIENTS: &str = "clients";
pub const SYSTEM_TOPIC_VERSION: &str = "version";
pub const SYSTEM_TOPIC_LICENSE: &str = "license";
pub const SYSTEM_TOPIC_SOURCES: &str = "source-code";
pub const SYSTEM_TOPIC_SUBSCRIPTIONS: &str = "subscriptions";
pub const SYSTEM_TOPIC_CLIENTS_PROTOCOL: &str = "protocol";
pub const SYSTEM_TOPIC_CLIENTS_ADDRESS: &str = "address";
pub const SYSTEM_TOPIC_CLIENTS_KEEPALIVE_LAG: &str = "keepaliveLag";
pub const SYSTEM_TOPIC_LAST_WILL: &str = "lastWill";
pub const SYSTEM_TOPIC_GRAVE_GOODS: &str = "graveGoods";
pub const SYSTEM_TOPIC_CLIENT_NAME: &str = "clientName";
pub const SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION: &str = "protocolVersion";

pub type TransactionId = u64;
pub type RequestPattern = String;
pub type RequestPatterns = Vec<RequestPattern>;
pub type Key = String;
pub type Value = serde_json::Value;
pub type KeyValuePairs = Vec<KeyValuePair>;
pub type TypedKeyValuePairs<T> = Vec<TypedKeyValuePair<T>>;
pub type MetaData = String;
pub type Path = String;
pub type ProtocolVersionSegment = u16;
pub type ProtocolVersions = Vec<ProtocolVersion>;
pub type LastWill = KeyValuePairs;
pub type GraveGoods = RequestPatterns;
pub type UniqueFlag = bool;
pub type LiveOnlyFlag = bool;
pub type AuthToken = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Privilege {
    Read,
    Write,
    Delete,
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privilege::Read => "read".fmt(f),
            Privilege::Write => "write".fmt(f),
            Privilege::Delete => "delete".fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ErrorCode {
    IllegalWildcard = 0b00000000,
    IllegalMultiWildcard = 0b00000001,
    MultiWildcardAtIllegalPosition = 0b00000010,
    IoError = 0b00000011,
    SerdeError = 0b00000100,
    NoSuchValue = 0b00000101,
    NotSubscribed = 0b00000110,
    ProtocolNegotiationFailed = 0b00000111,
    InvalidServerResponse = 0b00001000,
    ReadOnlyKey = 0b00001001,
    AuthorizationFailed = 0b00001010,
    AuthorizationRequired = 0b00001011,
    AlreadyAuthorized = 0b00001100,
    MissingValue = 0b00001101,
    Unauthorized = 0b00001110,
    Other = 0b11111111,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.to_owned() as u8).fmt(f)
    }
}

#[macro_export]
macro_rules! topic {
    ($( $x:expr ),+ ) => {
        {
            let mut segments = Vec::new();
            $(
                segments.push($x.to_string());
            )+
            segments.join("/")
        }
    };
}

pub type Version = String;
pub type ProtocolVersion = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Hash, Deserialize)]
pub enum Protocol {
    TCP,
    WS,
    HTTP,
    UNIX,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValuePair {
    pub key: Key,
    pub value: Value,
}

impl fmt::Display for KeyValuePair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

impl From<KeyValuePair> for Option<Value> {
    fn from(kvp: KeyValuePair) -> Self {
        Some(kvp.value)
    }
}

impl From<KeyValuePair> for Value {
    fn from(kvp: KeyValuePair) -> Self {
        kvp.value
    }
}

impl KeyValuePair {
    pub fn new<S: Serialize>(key: String, value: S) -> Self {
        (key, value).into()
    }

    pub fn of<S: Serialize>(key: String, value: S) -> Self {
        KeyValuePair::new(key, value)
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

impl<S: Serialize> From<(String, S)> for KeyValuePair {
    fn from((key, value): (String, S)) -> Self {
        let value = json!(value);
        KeyValuePair { key, value }
    }
}

impl<S: Serialize> From<(&str, S)> for KeyValuePair {
    fn from((key, value): (&str, S)) -> Self {
        let value = json!(value);
        KeyValuePair {
            key: key.to_owned(),
            value,
        }
    }
}

// #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, Tags)]
pub type RegularKeySegment = String;

pub fn parse_segments(pattern: &str) -> WorterbuchResult<Vec<RegularKeySegment>> {
    let mut segments = Vec::new();
    for segment in pattern.split('/') {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeySegment {
    Regular(RegularKeySegment),
    Wildcard,
    MultiWildcard,
    // RegexWildcard(String),
}

pub fn format_path(path: &[KeySegment]) -> String {
    path.iter()
        .map(|seg| format!("{seg}"))
        .collect::<Vec<String>>()
        .join("/")
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
            KeySegment::Regular(reg) => reg,
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
            other => KeySegment::Regular(other.to_owned()),
        }
    }
}

impl KeySegment {
    pub fn parse(pattern: impl AsRef<str>) -> Vec<KeySegment> {
        let segments = pattern.as_ref().split('/');
        segments.map(KeySegment::from).collect()
    }
}

pub fn quote(str: impl AsRef<str>) -> String {
    let str_ref = str.as_ref();
    if str_ref.starts_with('\"') && str_ref.ends_with('\"') {
        str_ref.to_owned()
    } else {
        format!("\"{str_ref}\"")
    }
}

pub fn digest_token(auth_token: &Option<String>, client_id: String) -> Option<String> {
    auth_token.as_deref().map(|token| {
        let salted = client_id + token;
        let mut hasher = Sha256::new();
        hasher.update(salted.as_bytes());
        format!("{:x}", hasher.finalize())
    })
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use crate::{ClientMessage, ErrorCode, ServerMessage};

    #[test]
    fn protocol_versions_are_sorted_correctly() {
        assert_eq!("0.1".cmp("0.2"), Ordering::Less);
        assert_eq!("0.9".cmp("1.0"), Ordering::Less);
        assert_eq!("1.2".cmp("1.3"), Ordering::Less);
        assert_eq!("1.234".cmp("1.3"), Ordering::Less);

        assert_eq!("0.1".cmp("0.1"), Ordering::Equal);
        assert_eq!("0.9".cmp("0.9"), Ordering::Equal);
        assert_eq!("1.2".cmp("1.2"), Ordering::Equal);

        assert_eq!("0.2".cmp("0.1"), Ordering::Greater);
        assert_eq!("1.0".cmp("0.9"), Ordering::Greater);
        assert_eq!("1.3".cmp("1.2"), Ordering::Greater);
        assert_eq!("1.3".cmp("1.234"), Ordering::Greater);
        assert_eq!("13.45".cmp("1.345"), Ordering::Greater);

        assert_eq!("0.3", "0.3".min("0.5"));
        assert_eq!("0.8", "0.8".min("1.2"));
        assert_eq!("2.3", "2.3".min("3.1"));

        assert_eq!("0.3", "0.5".min("0.3"));
        assert_eq!("0.8", "1.2".min("0.8"));
        assert_eq!("2.34", "3.1".min("2.34"));

        let mut versions = vec!["1.2", "0.456", "9.0", "3.15"];
        versions.sort();
        assert_eq!(vec!["0.456", "1.2", "3.15", "9.0"], versions);
    }

    #[test]
    fn topic_macro_generates_topic_correctly() {
        assert_eq!(
            "hello/world/foo/bar",
            topic!("hello", "world", "foo", "bar")
        );
    }

    #[test]
    fn server_keepalive_can_be_serialized() {
        assert_eq!(
            r#""""#,
            serde_json::to_string(&ServerMessage::Keepalive).unwrap()
        );
    }

    #[test]
    fn server_keepalive_can_be_deserialized() {
        assert_eq!(
            ServerMessage::Keepalive,
            serde_json::from_str(r#""""#).unwrap()
        );
    }

    #[test]
    fn client_keepalive_can_be_serialized() {
        assert_eq!(
            r#""""#,
            serde_json::to_string(&ClientMessage::Keepalive).unwrap()
        );
    }

    #[test]
    fn client_keepalive_can_be_deserialized() {
        assert_eq!(
            ClientMessage::Keepalive,
            serde_json::from_str(r#""""#).unwrap()
        );
    }

    #[test]
    fn error_codes_are_serialized_as_numbers() {
        assert_eq!(
            "1",
            serde_json::to_string(&ErrorCode::IllegalMultiWildcard).unwrap()
        )
    }

    #[test]
    fn error_codes_are_deserialized_from_numbers() {
        assert_eq!(
            ErrorCode::ProtocolNegotiationFailed,
            serde_json::from_str("7").unwrap()
        )
    }
}
