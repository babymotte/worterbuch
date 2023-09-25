use crate::{
    GraveGoods, Key, LastWill, ProtocolVersions, RequestPattern, TransactionId, UniqueFlag, Value,
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
    Unsubscribe(Unsubscribe),
    Delete(Delete),
    PDelete(PDelete),
    Ls(Ls),
    SubscribeLs(SubscribeLs),
    UnsubscribeLs(UnsubscribeLs),
    #[serde(rename = "")]
    Keepalive,
}

impl ClientMessage {
    pub fn transaction_id(&self) -> Option<TransactionId> {
        match self {
            ClientMessage::HandshakeRequest(_) => Some(0),
            ClientMessage::Get(m) => Some(m.transaction_id),
            ClientMessage::PGet(m) => Some(m.transaction_id),
            ClientMessage::Set(m) => Some(m.transaction_id),
            ClientMessage::Publish(m) => Some(m.transaction_id),
            ClientMessage::Subscribe(m) => Some(m.transaction_id),
            ClientMessage::PSubscribe(m) => Some(m.transaction_id),
            ClientMessage::Unsubscribe(m) => Some(m.transaction_id),
            ClientMessage::Delete(m) => Some(m.transaction_id),
            ClientMessage::PDelete(m) => Some(m.transaction_id),
            ClientMessage::Ls(m) => Some(m.transaction_id),
            ClientMessage::SubscribeLs(m) => Some(m.transaction_id),
            ClientMessage::UnsubscribeLs(m) => Some(m.transaction_id),
            ClientMessage::Keepalive => None,
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
    pub unique: UniqueFlag,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PSubscribe {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
    pub unique: UniqueFlag,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_events: Option<u64>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ls {
    pub transaction_id: TransactionId,
    pub parent: Option<Key>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeLs {
    pub transaction_id: TransactionId,
    pub parent: Option<Key>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeLs {
    pub transaction_id: TransactionId,
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

    #[test]
    fn psubscribe_without_aggregation_is_serialized_correctly() {
        let msg = ClientMessage::PSubscribe(PSubscribe {
            transaction_id: 1,
            request_pattern: "hello/world".to_owned(),
            unique: true,
            aggregate_events: None,
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true}}"#
        );
    }

    #[test]
    fn psubscribe_with_aggregation_is_serialized_correctly() {
        let msg = ClientMessage::PSubscribe(PSubscribe {
            transaction_id: 1,
            request_pattern: "hello/world".to_owned(),
            unique: true,
            aggregate_events: Some(10),
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true,"aggregateEvents":10}}"#
        );
    }

    #[test]
    fn psubscribe_without_aggregation_is_deserialized_correctly() {
        let json =
            r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true}}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();

        assert_eq!(
            msg,
            ClientMessage::PSubscribe(PSubscribe {
                transaction_id: 1,
                request_pattern: "hello/world".to_owned(),
                unique: true,
                aggregate_events: None,
            })
        );
    }

    #[test]
    fn psubscribe_with_aggregation_is_deserialized_correctly() {
        let json = r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true,"aggregateEvents":10}}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();

        assert_eq!(
            msg,
            ClientMessage::PSubscribe(PSubscribe {
                transaction_id: 1,
                request_pattern: "hello/world".to_owned(),
                unique: true,
                aggregate_events: Some(10),
            })
        );
    }
}
