/*
 *  Worterbuch server messages module
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

use crate::protocol::client_server::{
    CasVersion, KeyValuePair, KeyValuePairs, MetaData, ProtocolVersion, RequestPattern,
    TransactionId, Value, Version,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ServerMessage {
    Welcome(Welcome),
    PState(PState),
    Ack(Ack),
    State(State),
    CState(CState),
    Err(Err),
    Authorized(Ack),
    LsState(LsState),
}

impl ServerMessage {
    pub fn transaction_id(&self) -> Option<TransactionId> {
        match self {
            ServerMessage::Welcome(_) => None,
            ServerMessage::PState(msg) => Some(msg.transaction_id),
            ServerMessage::Ack(msg) => Some(msg.transaction_id),
            ServerMessage::State(msg) => Some(msg.transaction_id),
            ServerMessage::CState(msg) => Some(msg.transaction_id),
            ServerMessage::Err(msg) => Some(msg.transaction_id),
            ServerMessage::LsState(msg) => Some(msg.transaction_id),
            ServerMessage::Authorized(_) => Some(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Welcome {
    pub info: ServerInfo,
    pub client_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PState {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
    #[serde(flatten)]
    pub event: PStateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PStateEvent {
    KeyValuePairs(KeyValuePairs),
    Deleted(KeyValuePairs),
}

impl From<PStateEvent> for Vec<Option<Value>> {
    fn from(e: PStateEvent) -> Self {
        match e {
            PStateEvent::KeyValuePairs(kvps) => kvps.into_iter().map(KeyValuePair::into).collect(),
            PStateEvent::Deleted(keys) => keys.into_iter().map(|_| Option::None).collect(),
        }
    }
}

impl From<PState> for Vec<Option<Value>> {
    fn from(pstate: PState) -> Self {
        pstate.event.into()
    }
}

impl fmt::Display for PState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.event {
            PStateEvent::KeyValuePairs(key_value_pairs) => {
                let kvps: Vec<String> = key_value_pairs
                    .iter()
                    .map(|kvp| format!("{}={}", kvp.key, kvp.value))
                    .collect();
                let joined = kvps.join("\n");
                write!(f, "{joined}")
            }
            PStateEvent::Deleted(key_value_pairs) => {
                let kvps: Vec<String> = key_value_pairs
                    .iter()
                    .map(|kvp| format!("{}!={}", kvp.key, kvp.value))
                    .collect();
                let joined = kvps.join("\n");
                write!(f, "{joined}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Ack {
    pub transaction_id: TransactionId,
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ack {}", self.transaction_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct State {
    pub transaction_id: TransactionId,
    #[serde(flatten)]
    pub event: StateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum StateEvent {
    Value(Value),
    Deleted(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CState {
    pub transaction_id: TransactionId,
    #[serde(flatten)]
    pub event: CStateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CStateEvent {
    pub value: Value,
    pub version: CasVersion,
}

impl From<StateEvent> for Option<Value> {
    fn from(e: StateEvent) -> Self {
        match e {
            StateEvent::Value(v) => Some(v),
            StateEvent::Deleted(_) => None,
        }
    }
}

impl From<State> for Option<Value> {
    fn from(state: State) -> Self {
        state.event.into()
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.event {
            StateEvent::Value(v) => write!(f, "{v}"),
            StateEvent::Deleted(v) => write!(f, "!{v}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Err {
    pub transaction_id: TransactionId,
    pub error_code: ErrorCode,
    pub metadata: MetaData,
}

impl fmt::Display for Err {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "server error {}: {}", self.error_code, self.metadata)
    }
}

impl std::error::Error for Err {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Handshake {
    pub protocol_version: ProtocolVersion,
}

impl fmt::Display for Handshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "handshake: supported protocol versions: {}",
            self.protocol_version
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LsState {
    pub transaction_id: TransactionId,
    pub children: Vec<String>,
}

impl fmt::Display for LsState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.children
                .iter()
                .map(escape_path_segment)
                .fold(String::new(), |a, b| a + &b + "\t")
                .trim_end()
        )
    }
}

fn escape_path_segment(str: impl AsRef<str>) -> String {
    let str = str.as_ref();
    let white = str.contains(char::is_whitespace);
    let single_quote = str.contains('\'');
    let quote = str.contains('"');

    if (quote || white) && !single_quote {
        format!("'{str}'")
    } else if single_quote {
        str.replace('\'', r#"\'"#)
    } else {
        str.to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub version: Version,
    pub supported_protocol_versions: Box<[ProtocolVersion]>,
    #[deprecated(since = "1.1.0", note = "replaced by `supported_protocol_versions`")]
    protocol_version: String,
    pub authorization_required: bool,
}

#[allow(deprecated)]
impl ServerInfo {
    pub fn new(
        version: Version,
        supported_protocol_versions: Box<[ProtocolVersion]>,
        authorization_required: bool,
    ) -> Self {
        Self {
            version,
            supported_protocol_versions,
            protocol_version: "0.11".to_owned(),
            authorization_required,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr, JsonSchema)]
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
    EmptyKey = 25,
    Other = u8::MAX,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.to_owned() as u8).fmt(f)
    }
}

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

    use super::*;
    use serde_json::json;

    #[test]
    fn state_is_serialized_correctly() {
        let state = State {
            transaction_id: 1,
            event: StateEvent::Value(json!(2)),
        };

        let json = r#"{"transactionId":1,"value":2}"#;

        assert_eq!(json, &serde_json::to_string(&state).unwrap());

        let state = State {
            transaction_id: 1,
            event: StateEvent::Deleted(json!(2)),
        };

        let json = r#"{"transactionId":1,"deleted":2}"#;

        assert_eq!(json, &serde_json::to_string(&state).unwrap());
    }

    #[test]
    fn state_is_deserialized_correctly() {
        let state = State {
            transaction_id: 1,
            event: StateEvent::Value(json!(2)),
        };

        let json = r#"{"transactionId":1,"value":2}"#;

        assert_eq!(state, serde_json::from_str(json).unwrap());

        let state = State {
            transaction_id: 1,
            event: StateEvent::Deleted(json!(2)),
        };

        let json = r#"{"transactionId":1,"deleted":2}"#;

        assert_eq!(state, serde_json::from_str(json).unwrap());
    }

    #[test]
    fn pstate_is_serialized_correctly() {
        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::KeyValuePairs(vec![KeyValuePair::of("$SYS/clients", 2)]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","keyValuePairs":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(json, &serde_json::to_string(&pstate).unwrap());

        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::Deleted(vec![KeyValuePair::of("$SYS/clients", 2)]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","deleted":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(json, &serde_json::to_string(&pstate).unwrap());
    }

    #[test]
    fn pstate_is_deserialized_correctly() {
        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::KeyValuePairs(vec![KeyValuePair::of("$SYS/clients", 2)]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","keyValuePairs":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(pstate, serde_json::from_str(json).unwrap());

        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::Deleted(vec![KeyValuePair::of("$SYS/clients", 2)]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","deleted":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(pstate, serde_json::from_str(json).unwrap());
    }
}
