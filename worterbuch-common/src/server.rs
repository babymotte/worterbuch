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

use crate::{
    ErrorCode, KeyValuePair, KeyValuePairs, MetaData, ProtocolVersion, RequestPattern,
    TransactionId, TypedKeyValuePair, Value, Version,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ServerMessage {
    Welcome(Welcome),
    PState(PState),
    Ack(Ack),
    State(State),
    Err(Err),
    Authorized(Ack),
    LsState(LsState),
    #[serde(rename = "")]
    Keepalive,
}

impl ServerMessage {
    pub fn transaction_id(&self) -> Option<TransactionId> {
        match self {
            ServerMessage::Welcome(_) => None,
            ServerMessage::PState(msg) => Some(msg.transaction_id),
            ServerMessage::Ack(msg) => Some(msg.transaction_id),
            ServerMessage::State(msg) => Some(msg.transaction_id),
            ServerMessage::Err(msg) => Some(msg.transaction_id),
            ServerMessage::LsState(msg) => Some(msg.transaction_id),
            ServerMessage::Authorized(_) => Some(0),
            ServerMessage::Keepalive => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Welcome {
    pub info: ServerInfo,
    pub client_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PState {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
    #[serde(flatten)]
    pub event: PStateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PStateEvent {
    KeyValuePairs(KeyValuePairs),
    Deleted(KeyValuePairs),
}

impl From<PStateEvent> for Vec<StateEvent> {
    fn from(e: PStateEvent) -> Self {
        match e {
            PStateEvent::KeyValuePairs(kvps) => {
                kvps.into_iter().map(StateEvent::KeyValue).collect()
            }
            PStateEvent::Deleted(kvps) => kvps.into_iter().map(StateEvent::Deleted).collect(),
        }
    }
}

impl From<PState> for Vec<StateEvent> {
    fn from(pstate: PState) -> Self {
        pstate.event.into()
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ack {
    pub transaction_id: TransactionId,
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ack {}", self.transaction_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct State {
    pub transaction_id: TransactionId,
    #[serde(flatten)]
    pub event: StateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StateEvent {
    KeyValue(KeyValuePair),
    Deleted(KeyValuePair),
}

impl From<StateEvent> for Option<Value> {
    fn from(e: StateEvent) -> Self {
        match e {
            StateEvent::KeyValue(kv) => Some(kv.value),
            StateEvent::Deleted(_) => None,
        }
    }
}

impl From<State> for Option<Value> {
    fn from(state: State) -> Self {
        state.event.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedStateEvent<T: DeserializeOwned> {
    KeyValue(TypedKeyValuePair<T>),
    Deleted(TypedKeyValuePair<T>),
}

impl<T: DeserializeOwned> From<TypedStateEvent<T>> for Option<T> {
    fn from(e: TypedStateEvent<T>) -> Self {
        match e {
            TypedStateEvent::KeyValue(kvp) => Some(kvp.value),
            TypedStateEvent::Deleted(_) => None,
        }
    }
}

impl<T: DeserializeOwned> From<TypedKeyValuePair<T>> for TypedStateEvent<T> {
    fn from(kvp: TypedKeyValuePair<T>) -> Self {
        TypedStateEvent::KeyValue(kvp)
    }
}

impl<T: DeserializeOwned> TryFrom<StateEvent> for TypedStateEvent<T> {
    type Error = serde_json::Error;

    fn try_from(e: StateEvent) -> Result<Self, Self::Error> {
        match e {
            StateEvent::KeyValue(kvp) => Ok(TypedStateEvent::KeyValue(kvp.try_into()?)),
            StateEvent::Deleted(kvp) => Ok(TypedStateEvent::Deleted(kvp.try_into()?)),
        }
    }
}

pub type TypedStateEvents<T> = Vec<TypedStateEvent<T>>;

impl<T: DeserializeOwned> TryFrom<PStateEvent> for TypedStateEvents<T> {
    type Error = serde_json::Error;

    fn try_from(event: PStateEvent) -> Result<Self, Self::Error> {
        let state_events: Vec<StateEvent> = event.into();
        let mut typed_events = TypedStateEvents::new();
        for event in state_events {
            typed_events.push(event.try_into()?);
        }
        Ok(typed_events)
    }
}

impl<T: DeserializeOwned> TryFrom<PState> for TypedStateEvents<T> {
    type Error = serde_json::Error;

    fn try_from(pstate: PState) -> Result<Self, Self::Error> {
        pstate.event.try_into()
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.event {
            StateEvent::KeyValue(kvp) => write!(f, "{}={}", kvp.key, kvp.value),
            StateEvent::Deleted(kvp) => write!(f, "{}!={}", kvp.key, kvp.value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
                .reduce(|a, b| format!("{a}\t{b}"))
                .unwrap_or("".to_owned())
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub version: Version,
    pub protocol_version: ProtocolVersion,
    pub authorization_required: bool,
}

#[cfg(test)]
mod test {

    use serde_json::json;

    use super::*;

    #[test]
    fn state_is_serialized_correctly() {
        let state = State {
            transaction_id: 1,
            event: StateEvent::KeyValue(("$SYS/clients", json!(2)).into()),
        };

        let json = r#"{"transactionId":1,"keyValue":{"key":"$SYS/clients","value":2}}"#;

        assert_eq!(json, &serde_json::to_string(&state).unwrap());

        let state = State {
            transaction_id: 1,
            event: StateEvent::Deleted(("$SYS/clients", json!(2)).into()),
        };

        let json = r#"{"transactionId":1,"deleted":{"key":"$SYS/clients","value":2}}"#;

        assert_eq!(json, &serde_json::to_string(&state).unwrap());
    }

    #[test]
    fn state_is_deserialized_correctly() {
        let state = State {
            transaction_id: 1,
            event: StateEvent::KeyValue(("$SYS/clients", json!(2)).into()),
        };

        let json = r#"{"transactionId":1,"keyValue":{"key":"$SYS/clients","value":2}}"#;

        assert_eq!(state, serde_json::from_str(&json).unwrap());

        let state = State {
            transaction_id: 1,
            event: StateEvent::Deleted(("$SYS/clients", json!(2)).into()),
        };

        let json = r#"{"transactionId":1,"deleted":{"key":"$SYS/clients","value":2}}"#;

        assert_eq!(state, serde_json::from_str(&json).unwrap());
    }

    #[test]
    fn pstate_is_serialized_correctly() {
        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::KeyValuePairs(vec![("$SYS/clients", json!(2)).into()]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","keyValuePairs":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(json, &serde_json::to_string(&pstate).unwrap());

        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::Deleted(vec![("$SYS/clients", json!(2)).into()]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","deleted":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(json, &serde_json::to_string(&pstate).unwrap());
    }

    #[test]
    fn pstate_is_deserialized_correctly() {
        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::KeyValuePairs(vec![("$SYS/clients", json!(2)).into()]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","keyValuePairs":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(pstate, serde_json::from_str(&json).unwrap());

        let pstate = PState {
            transaction_id: 1,
            request_pattern: "$SYS/clients".to_owned(),
            event: PStateEvent::Deleted(vec![("$SYS/clients", json!(2)).into()]),
        };

        let json = r#"{"transactionId":1,"requestPattern":"$SYS/clients","deleted":[{"key":"$SYS/clients","value":2}]}"#;

        assert_eq!(pstate, serde_json::from_str(&json).unwrap());
    }
}
