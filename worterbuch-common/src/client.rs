use crate::{
    GraveGoods, Key, LastWill, Path, ProtocolVersions, RequestPattern, TransactionId, Value,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientMessage {
    HandshakeRequest(HandshakeRequest),
    Get(Get),
    PGet(PGet),
    Set(Set),
    Publish(Publish),
    Subscribe(Subscribe),
    PSubscribe(PSubscribe),
    Export(Export),
    Import(Import),
    Unsubscribe(Unsubscribe),
    Delete(Delete),
    PDelete(PDelete),
}

impl ClientMessage {
    pub fn transaction_id(&self) -> TransactionId {
        match self {
            ClientMessage::HandshakeRequest(_) => 0,
            ClientMessage::Get(m) => m.transaction_id,
            ClientMessage::PGet(m) => m.transaction_id,
            ClientMessage::Set(m) => m.transaction_id,
            ClientMessage::Publish(m) => m.transaction_id,
            ClientMessage::Subscribe(m) => m.transaction_id,
            ClientMessage::PSubscribe(m) => m.transaction_id,
            ClientMessage::Export(m) => m.transaction_id,
            ClientMessage::Import(m) => m.transaction_id,
            ClientMessage::Unsubscribe(m) => m.transaction_id,
            ClientMessage::Delete(m) => m.transaction_id,
            ClientMessage::PDelete(m) => m.transaction_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeRequest {
    pub supported_protocol_versions: ProtocolVersions,
    pub last_will: LastWill,
    pub grave_goods: GraveGoods,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Get {
    pub transaction_id: TransactionId,
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PGet {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Set {
    pub transaction_id: TransactionId,
    pub key: Key,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Publish {
    pub transaction_id: TransactionId,
    pub key: Key,
    pub value: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscribe {
    pub transaction_id: TransactionId,
    pub key: RequestPattern,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PSubscribe {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Export {
    pub transaction_id: TransactionId,
    pub path: Path,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Import {
    pub transaction_id: TransactionId,
    pub path: Path,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Unsubscribe {
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delete {
    pub transaction_id: TransactionId,
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PDelete {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::ProtocolVersion;
    use serde_json::json;

    #[test]
    fn handshake_request_is_serialized_correctly() {
        let msg = ClientMessage::HandshakeRequest(HandshakeRequest {
            grave_goods: vec!["delete/this/stuff/#".to_owned()],
            last_will: vec![("set/this", json!("to this value")).into()],
            supported_protocol_versions: vec![ProtocolVersion { major: 0, minor: 1 }],
        });

        let json = r#"{"handshakeRequest":{"supportedProtocolVersions":[{"major":0,"minor":1}],"lastWill":[{"key":"set/this","value":"to this value"}],"graveGoods":["delete/this/stuff/#"]}}"#;

        assert_eq!(&serde_json::to_string(&msg).unwrap(), json);
    }

    #[test]
    fn handshake_request_is_deserialized_correctly() {
        let msg = ClientMessage::HandshakeRequest(HandshakeRequest {
            grave_goods: vec!["delete/this/stuff/#".to_owned()],
            last_will: vec![("set/this", json!("to this value")).into()],
            supported_protocol_versions: vec![ProtocolVersion { major: 0, minor: 1 }],
        });

        let json = r#"{
            "handshakeRequest": {
              "supportedProtocolVersions": [{ "major": 0, "minor": 1 }],
              "lastWill": [{ "key": "set/this", "value": "to this value" }],
              "graveGoods": ["delete/this/stuff/#"]
            }
          }"#;

        assert_eq!(serde_json::from_str::<ClientMessage>(&json).unwrap(), msg);
    }

    #[test]
    fn set_is_deserialized_correctly() {
        let json = r#"{"set": {"transactionId": 2, "key": "hello/world", "value": { "this value": "is a ", "complex": "JSON object"}}}"#;
        let msg = serde_json::from_str::<ClientMessage>(&json).unwrap();
        assert_eq!(
            msg,
            ClientMessage::Set(Set {
                transaction_id: 2,
                key: "hello/world".to_owned(),
                value: json!({ "this value": "is a ", "complex": "JSON object"}),
            })
        );
    }
}
