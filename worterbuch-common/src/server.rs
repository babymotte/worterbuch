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
    CasVersion, ErrorCode, KeyValuePair, KeyValuePairs, MetaData, ProtocolVersion, RequestPattern,
    TransactionId, TypedKeyValuePair, TypedKeyValuePairs, Value, Version,
    error::{ConnectionError, ConnectionResult},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fmt::{self, Display},
    io,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWriteExt, BufReader, Lines},
    time::timeout,
};
use tracing::{debug, error, trace};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedPStateEvent<T: DeserializeOwned> {
    KeyValuePairs(TypedKeyValuePairs<T>),
    Deleted(TypedKeyValuePairs<T>),
}

impl<T: DeserializeOwned> TryFrom<PStateEvent> for TypedPStateEvent<T> {
    type Error = serde_json::Error;

    fn try_from(value: PStateEvent) -> Result<Self, Self::Error> {
        match value {
            PStateEvent::KeyValuePairs(kvps) => Ok(TypedPStateEvent::KeyValuePairs(
                try_to_typed_key_value_pairs(kvps)?,
            )),
            PStateEvent::Deleted(kvps) => Ok(TypedPStateEvent::KeyValuePairs(
                try_to_typed_key_value_pairs(kvps)?,
            )),
        }
    }
}

fn try_to_typed_key_value_pairs<T: DeserializeOwned>(
    kvps: KeyValuePairs,
) -> Result<TypedKeyValuePairs<T>, serde_json::Error> {
    let mut out = vec![];

    for kvp in kvps {
        out.push(kvp.try_into()?);
    }

    Ok(out)
}

pub type TypedPStateEvents<T> = Vec<TypedPStateEvent<T>>;

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
    Value(Value),
    Deleted(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CState {
    pub transaction_id: TransactionId,
    #[serde(flatten)]
    pub event: CStateEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedStateEvent<T: DeserializeOwned> {
    Value(T),
    Deleted(T),
}

impl<T: DeserializeOwned> From<TypedStateEvent<T>> for Option<T> {
    fn from(e: TypedStateEvent<T>) -> Self {
        match e {
            TypedStateEvent::Value(v) => Some(v),
            TypedStateEvent::Deleted(_) => None,
        }
    }
}

impl<T: DeserializeOwned> From<TypedKeyValuePair<T>> for TypedStateEvent<T> {
    fn from(kvp: TypedKeyValuePair<T>) -> Self {
        TypedStateEvent::Value(kvp.value)
    }
}

impl<T: DeserializeOwned + TryFrom<Value, Error = serde_json::Error>> TryFrom<StateEvent>
    for TypedStateEvent<T>
{
    type Error = serde_json::Error;

    fn try_from(e: StateEvent) -> Result<Self, Self::Error> {
        match e {
            StateEvent::Value(v) => Ok(TypedStateEvent::Value(v.try_into()?)),
            StateEvent::Deleted(v) => Ok(TypedStateEvent::Deleted(v.try_into()?)),
        }
    }
}

pub type TypedStateEvents<T> = Vec<TypedStateEvent<T>>;

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.event {
            StateEvent::Value(v) => write!(f, "{v}"),
            StateEvent::Deleted(v) => write!(f, "!{v}"),
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

pub async fn write_line_and_flush(
    msg: impl Serialize,
    mut tx: impl AsyncWriteExt + Unpin,
    send_timeout: Option<Duration>,
    remote: impl Display,
) -> ConnectionResult<()> {
    let mut json = serde_json::to_string(&msg)?;
    if json.contains('\n') {
        return Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON: '{json}' contains line break"),
        )));
    }
    if json.trim().is_empty() {
        return Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON: '{json}' is empty"),
        )));
    }

    json.push('\n');
    let bytes = json.as_bytes();

    debug!("Sending message with timeout {send_timeout:?}: {json}");
    trace!("Writing line …");
    for chunk in bytes.chunks(1024) {
        let mut written = 0;
        while written < chunk.len() {
            if let Some(send_timeout) = send_timeout {
                written += timeout(send_timeout, tx.write(&chunk[written..]))
                    .await
                    .map_err(|_| {
                        ConnectionError::Timeout(format!(
                            "timeout while sending tcp message to {remote}"
                        ))
                    })??;
            } else {
                written += tx.write(&chunk[written..]).await?;
            }
        }
    }
    trace!("Writing line done.");
    trace!("Flushing channel …");
    if let Some(send_timeout) = send_timeout {
        timeout(send_timeout, tx.flush()).await.map_err(|_| {
            ConnectionError::Timeout(format!("timeout while sending tcp message to {remote}"))
        })??;
    } else {
        tx.flush().await?;
    }
    trace!("Flushing channel done.");

    Ok(())
}

pub async fn receive_msg<T: DeserializeOwned, R: AsyncRead + Unpin>(
    rx: &mut Lines<BufReader<R>>,
) -> ConnectionResult<Option<T>> {
    let read = rx.next_line().await;
    match read {
        Ok(None) => Ok(None),
        Ok(Some(json)) => {
            debug!("Received message: {json}");
            let sm = serde_json::from_str(&json);
            if let Err(e) = &sm {
                error!("Error deserializing message '{json}': {e}")
            }
            Ok(sm?)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

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
