mod client;
mod server;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt;
use uuid::Uuid;

pub use client::*;
pub use server::*;

use crate::{ClientId, Protocol};

pub const SYSTEM_TOPIC_ROOT: &str = "$SYS";
pub const SYSTEM_TOPIC_ROOT_PREFIX: &str = "$SYS/";
pub const SYSTEM_TOPIC_NAME: &str = "name";
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
pub const SYSTEM_TOPIC_UPTIME: &str = "uptime";
pub const SYSTEM_TOPIC_STORE: &str = "store";
pub const SYSTEM_TOPIC_VALUES: &str = "values";
pub const SYSTEM_TOPIC_COUNT: &str = "count";
pub const SYSTEM_TOPIC_JEMALLOC: &str = "jemalloc";
pub const SYSTEM_TOPIC_RAW: &str = "raw";
pub const SYSTEM_TOPIC_FORMATTED: &str = "formatted";
pub const SYSTEM_TOPIC_CLUSTER: &str = "cluster";
pub const SYSTEM_TOPIC_LEADER: &str = "leader";

pub type TransactionId = u64;
pub type RequestPattern = String;
pub type RequestPatterns = Vec<RequestPattern>;
pub type Key = String;
pub type Value = serde_json::Value;
pub type KeyValuePairs = Vec<KeyValuePair>;
pub type ProtocolVersionSegment = u32;
pub type ProtocolMajorVersion = ProtocolVersionSegment;
pub type ProtocolVersions = Vec<ProtocolVersion>;
pub type UniqueFlag = bool;
pub type LiveOnlyFlag = bool;
pub type SendTracesFlag = bool;
pub type QuietFlag = bool;
pub type AggregationDuration = u64;
pub type AuthToken = String;
pub type AuthTokenKey = String;
pub type CasVersion = u64;
pub type MetaData = String;
pub type Version = String;
pub type LastWill = KeyValuePairs;
pub type GraveGoods = RequestPatterns;
pub type BorrowedLastWill = dyn AsRef<[KeyValuePair]>;
pub type BorrowedGraveGoods = dyn AsRef<[RequestPattern]>;
pub type ForceSet = bool;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

impl KeyValuePair {
    pub fn new(key: String, value: Value) -> Self {
        KeyValuePair { key, value }
    }

    pub fn of<S: Serialize>(key: impl Into<String>, value: S) -> Self {
        KeyValuePair::new(key.into(), json!(value))
    }
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema,
)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Interface {
    Protocol(Protocol),
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TraceData {
    pub transaction_id: TransactionId,
    pub client_id: ClientId,
    pub interface: Interface,
}

impl TraceData {
    pub fn new(client_id: ClientId, interface: Interface, transaction_id: TransactionId) -> Self {
        TraceData {
            transaction_id,
            client_id,
            interface,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Trace {
    #[serde(rename_all = "camelCase")]
    ClientRequest {
        client_id: ClientId,
        transaction_id: TransactionId,
        method: Method,
        interface: Interface,
    },
    #[serde(rename_all = "camelCase")]
    ProtocolSwitch {
        client_id: ClientId,
        protocol_version: ProtocolMajorVersion,
        interface: Interface,
    },
    #[serde(rename_all = "camelCase")]
    InternalAction(InternalAction),
}

impl Trace {
    pub fn client_request(method: Method, trace_data: &TraceData) -> Self {
        Trace::ClientRequest {
            client_id: trace_data.client_id,
            transaction_id: trace_data.transaction_id,
            method,
            interface: trace_data.interface.clone(),
        }
    }

    pub fn client_id(&self) -> Option<Uuid> {
        match self {
            Trace::ClientRequest { client_id, .. } => Some(*client_id),
            Trace::ProtocolSwitch { client_id, .. } => Some(*client_id),
            Trace::InternalAction(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Method {
    Set,
    CSet,
    Publish,
    Delete,
    PDelete,
    Import,
    Subscribe,
    PSubscribe,
    LsSubscribe,
    Unsubscribe,
    Lock,
    AcquireLock,
    ReleaseLock,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum InternalAction {
    #[serde(rename_all = "camelCase")]
    ClientConnected {
        client_id: ClientId,
        protocol: Protocol,
    },
    #[serde(rename_all = "camelCase")]
    ClientDisconnected {
        client_id: ClientId,
        protocol: Protocol,
    },
    #[serde(rename_all = "camelCase")]
    SubscriptionsChanged {
        cause: Box<Trace>,
    },
    #[serde(rename_all = "camelCase")]
    LocksChanged {
        cause: Box<Trace>,
    },
    #[serde(rename_all = "camelCase")]
    ApplyingGraveGoods {
        cause: Box<Trace>,
    },
    #[serde(rename_all = "camelCase")]
    ApplyingLastWill {
        cause: Box<Trace>,
    },
    Startup,
    LeaderSync,
    Shutdown,
}

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

    use super::*;

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
