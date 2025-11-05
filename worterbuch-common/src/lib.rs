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

#[cfg(feature = "benchmark")]
pub mod benchmark;
mod client;
pub mod error;
mod server;

pub use client::*;
use serde_json::json;
pub use server::*;

use error::WorterbuchResult;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_repr::*;
use sha2::{Digest, Sha256};
use std::{fmt, net::SocketAddr, ops::Deref};
use tokio::sync::{mpsc, oneshot};
use tracing::Span;
use uuid::Uuid;
#[cfg(feature = "jemalloc")]
mod jemalloc;
#[cfg(feature = "jemalloc")]
pub mod profiling;

pub const INTERNAL_CLIENT_ID: Uuid = Uuid::nil();

pub const SYSTEM_TOPIC_ROOT: &str = "$SYS";
pub const SYSTEM_TOPIC_ROOT_PREFIX: &str = "$SYS/";
pub const SYSTEM_TOPIC_CLIENTS: &str = "clients";
pub const SYSTEM_TOPIC_VERSION: &str = "version";
pub const SYSTEM_TOPIC_LICENSE: &str = "license";
pub const SYSTEM_TOPIC_SOURCES: &str = "source-code";
pub const SYSTEM_TOPIC_SUBSCRIPTIONS: &str = "subscriptions";
pub const SYSTEM_TOPIC_LOCKS: &str = "locks";
pub const SYSTEM_TOPIC_CLIENTS_PROTOCOL: &str = "protocol";
pub const SYSTEM_TOPIC_CLIENTS_PROTOCOL_VERSION: &str = "protocolVersion";
pub const SYSTEM_TOPIC_CLIENTS_ADDRESS: &str = "address";
pub const SYSTEM_TOPIC_CLIENTS_TIMESTAMP: &str = "connectedSince";
pub const SYSTEM_TOPIC_LAST_WILL: &str = "lastWill";
pub const SYSTEM_TOPIC_GRAVE_GOODS: &str = "graveGoods";
pub const SYSTEM_TOPIC_CLIENT_NAME: &str = "clientName";
pub const SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION: &str = "protocolVersion";
pub const SYSTEM_TOPIC_MODE: &str = "mode";

pub type TransactionId = u64;
pub type RequestPattern = String;
pub type RequestPatterns = Vec<RequestPattern>;
pub type Key = String;
pub type Value = serde_json::Value;
pub type KeyValuePairs = Vec<KeyValuePair>;
pub type TypedKeyValuePairs<T> = Vec<TypedKeyValuePair<T>>;
pub type MetaData = String;
pub type Path = String;
pub type ProtocolVersionSegment = u32;
pub type ProtocolMajorVersion = ProtocolVersionSegment;
pub type ProtocolVersions = Vec<ProtocolVersion>;
pub type LastWill = KeyValuePairs;
pub type GraveGoods = RequestPatterns;
pub type UniqueFlag = bool;
pub type LiveOnlyFlag = bool;
pub type AuthToken = String;
pub type AuthTokenKey = String;
pub type CasVersion = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueEntry {
    Cas(Value, u64),
    #[serde(untagged)]
    Plain(Value),
}

impl AsRef<Value> for ValueEntry {
    fn as_ref(&self) -> &Value {
        match self {
            ValueEntry::Plain(value) => value,
            ValueEntry::Cas(value, _) => value,
        }
    }
}

impl From<ValueEntry> for Value {
    fn from(value: ValueEntry) -> Self {
        match value {
            ValueEntry::Plain(value) => value,
            ValueEntry::Cas(value, _) => value,
        }
    }
}

impl From<Value> for ValueEntry {
    fn from(value: Value) -> Self {
        ValueEntry::Plain(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Privilege {
    Read,
    Write,
    Delete,
    Profile,
    WebLogin,
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privilege::Read => "read".fmt(f),
            Privilege::Write => "write".fmt(f),
            Privilege::Delete => "delete".fmt(f),
            Privilege::Profile => "profile".fmt(f),
            Privilege::WebLogin => "web-login".fmt(f),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthCheck<'a> {
    Pattern(&'a str),
    Flag,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthCheckOwned {
    Pattern(String),
    Flag,
}

impl<'a> From<AuthCheck<'a>> for AuthCheckOwned {
    fn from(value: AuthCheck<'a>) -> Self {
        match value {
            AuthCheck::Pattern(p) => AuthCheckOwned::Pattern(p.to_owned()),
            AuthCheck::Flag => AuthCheckOwned::Flag,
        }
    }
}

impl fmt::Display for AuthCheckOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthCheckOwned::Pattern(p) => p.fmt(f),
            AuthCheckOwned::Flag => true.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ErrorCode {
    IllegalWildcard = 0,
    IllegalMultiWildcard = 1,
    MultiWildcardAtIllegalPosition = 2,
    IoError = 3,
    SerdeError = 4,
    NoSuchValue = 5,
    NotSubscribed = 6,
    ProtocolNegotiationFailed = 7,
    InvalidServerResponse = 8,
    ReadOnlyKey = 9,
    AuthorizationFailed = 10,
    AuthorizationRequired = 11,
    AlreadyAuthorized = 12,
    MissingValue = 13,
    Unauthorized = 14,
    NoPubStream = 15,
    NotLeader = 16,
    Cas = 17,
    CasVersionMismatch = 18,
    NotImplemented = 19,
    KeyIsLocked = 20,
    KeyIsNotLocked = 21,
    LockAcquisitionCancelled = 22,
    FeatureDisabled = 23,
    ClientIDCollision = 24,
    Other = u8::MAX,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(ProtocolVersionSegment, ProtocolVersionSegment);

impl ProtocolVersion {
    pub const fn new(major: ProtocolVersionSegment, minor: ProtocolVersionSegment) -> Self {
        Self(major, minor)
    }

    pub const fn major(&self) -> ProtocolVersionSegment {
        self.0
    }

    pub const fn minor(&self) -> ProtocolVersionSegment {
        self.1
    }

    pub fn is_compatible_with_server(&self, server_version: &ProtocolVersion) -> bool {
        self.major() == server_version.major() && self.minor() <= server_version.minor()
    }

    pub fn is_compatible_with_client_version(&self, client_version: &ProtocolVersion) -> bool {
        self.major() == client_version.major() && self.minor() >= client_version.minor()
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0, self.1)
    }
}

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
    pub fn new(key: String, value: Value) -> Self {
        KeyValuePair { key, value }
    }

    pub fn of<S: Serialize>(key: impl Into<String>, value: S) -> Self {
        KeyValuePair::new(key.into(), json!(value))
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
                return Err(error::WorterbuchError::IllegalWildcard(pattern.to_owned()));
            }
            KeySegment::MultiWildcard => {
                return Err(error::WorterbuchError::IllegalMultiWildcard(
                    pattern.to_owned(),
                ));
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

impl AsRef<str> for KeySegment {
    fn as_ref(&self) -> &str {
        match self {
            KeySegment::Regular(segment) => segment.as_str(),
            KeySegment::Wildcard => "?",
            KeySegment::MultiWildcard => "#",
        }
    }
}

pub fn format_path(path: &[impl AsRef<str>]) -> String {
    let mut path = path.iter().fold(String::new(), |mut a, b| {
        let b = b.as_ref();
        a.reserve(b.len() + 1);
        a.push_str(b);
        a.push('/');
        a
    });
    path.pop();
    path
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SubscriptionId {
    pub client_id: Uuid,
    pub transaction_id: TransactionId,
}

impl SubscriptionId {
    pub fn new(client_id: Uuid, transaction_id: TransactionId) -> Self {
        SubscriptionId {
            client_id,
            transaction_id,
        }
    }
}

pub trait WbApi {
    fn supported_protocol_versions(&self) -> Vec<ProtocolVersion>;

    fn version(&self) -> &str;

    fn get(&self, key: Key) -> impl Future<Output = WorterbuchResult<Value>> + Send;

    fn cget(&self, key: Key) -> impl Future<Output = WorterbuchResult<(Value, CasVersion)>> + Send;

    fn pget(
        &self,
        pattern: RequestPattern,
    ) -> impl Future<Output = WorterbuchResult<KeyValuePairs>> + Send;

    fn set(
        &self,
        key: Key,
        value: Value,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn cset(
        &self,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn lock(&self, key: Key, client_id: Uuid) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn acquire_lock(
        &self,
        key: Key,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<oneshot::Receiver<()>>> + Send;

    fn release_lock(
        &self,
        key: Key,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn spub_init(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn spub(
        &self,
        transaction_id: TransactionId,
        value: Value,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn publish(&self, key: Key, value: Value) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn ls(
        &self,
        parent: Option<Key>,
    ) -> impl Future<Output = WorterbuchResult<Vec<RegularKeySegment>>> + Send;

    fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> impl Future<Output = WorterbuchResult<Vec<RegularKeySegment>>> + Send;

    fn subscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> impl Future<Output = WorterbuchResult<(mpsc::Receiver<StateEvent>, SubscriptionId)>> + Send;

    fn psubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
        live_only: bool,
    ) -> impl Future<Output = WorterbuchResult<(mpsc::Receiver<PStateEvent>, SubscriptionId)>> + Send;

    fn subscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        parent: Option<Key>,
    ) -> impl Future<
        Output = WorterbuchResult<(mpsc::Receiver<Vec<RegularKeySegment>>, SubscriptionId)>,
    > + Send;

    fn unsubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn unsubscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn delete(
        &self,
        key: Key,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<Value>> + Send;

    fn pdelete(
        &self,
        pattern: RequestPattern,
        client_id: Uuid,
    ) -> impl Future<Output = WorterbuchResult<KeyValuePairs>> + Send;

    fn connected(
        &self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn protocol_switched(
        &self,
        client_id: Uuid,
        protocol: ProtocolMajorVersion,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn disconnected(
        &self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn export(
        &self,
        span: Span,
    ) -> impl Future<Output = WorterbuchResult<(Value, GraveGoods, LastWill)>> + Send;

    fn import(
        &self,
        json: String,
    ) -> impl Future<Output = WorterbuchResult<Vec<(String, (ValueEntry, bool))>>> + Send;

    fn entries(&self) -> impl Future<Output = WorterbuchResult<usize>> + Send;
}

#[cfg(test)]
mod test {
    use crate::{ErrorCode, ProtocolVersion};
    use serde_json::json;

    #[test]
    fn protocol_versions_are_sorted_correctly() {
        assert!(ProtocolVersion::new(1, 2) < ProtocolVersion::new(3, 2));
        assert!(ProtocolVersion::new(1, 2) == ProtocolVersion::new(1, 2));
        assert!(ProtocolVersion::new(2, 1) > ProtocolVersion::new(1, 9));

        let mut versions = vec![
            ProtocolVersion::new(1, 2),
            ProtocolVersion::new(0, 456),
            ProtocolVersion::new(9, 0),
            ProtocolVersion::new(3, 15),
        ];
        versions.sort();
        assert_eq!(
            vec![
                ProtocolVersion::new(0, 456),
                ProtocolVersion::new(1, 2),
                ProtocolVersion::new(3, 15),
                ProtocolVersion::new(9, 0)
            ],
            versions
        );
    }

    #[test]
    fn topic_macro_generates_topic_correctly() {
        assert_eq!(
            "hello/world/foo/bar",
            topic!("hello", "world", "foo", "bar")
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

    #[test]
    fn protocol_version_get_serialized_correctly() {
        assert_eq!(&json!(ProtocolVersion::new(2, 1)).to_string(), "[2,1]")
    }

    #[test]
    fn protocol_version_get_formatted_correctly() {
        assert_eq!(&ProtocolVersion::new(2, 1).to_string(), "2.1")
    }

    #[test]
    fn compatible_version_is_selected_correctly() {
        let client_version = ProtocolVersion::new(1, 2);
        let server_versions = [
            ProtocolVersion::new(0, 11),
            ProtocolVersion::new(1, 6),
            ProtocolVersion::new(2, 0),
        ];
        let compatible_version = server_versions
            .iter()
            .find(|v| client_version.is_compatible_with_server(v));
        assert_eq!(compatible_version, Some(&server_versions[1]))
    }
}
