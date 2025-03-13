/*
 *  Worterbuch client messages module
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
    AuthToken, Key, LiveOnlyFlag, ProtocolVersionSegment, RequestPattern, TransactionId,
    UniqueFlag, Value,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientMessage {
    ProtocolSwitchRequest(ProtocolSwitchRequest),
    AuthorizationRequest(AuthorizationRequest),
    Get(Get),
    CGet(Get),
    PGet(PGet),
    Set(Set),
    CSet(CSet),
    SPubInit(SPubInit),
    SPub(SPub),
    Publish(Publish),
    Subscribe(Subscribe),
    PSubscribe(PSubscribe),
    Unsubscribe(Unsubscribe),
    Delete(Delete),
    PDelete(PDelete),
    Ls(Ls),
    PLs(PLs),
    SubscribeLs(SubscribeLs),
    UnsubscribeLs(UnsubscribeLs),
    Lock(Lock),
    AcquireLock(Lock),
    ReleaseLock(Lock),
    Transform(Transform),
}

impl ClientMessage {
    pub fn transaction_id(&self) -> Option<TransactionId> {
        match self {
            ClientMessage::ProtocolSwitchRequest(_) | ClientMessage::AuthorizationRequest(_) => {
                Some(0)
            }
            ClientMessage::Get(m) | ClientMessage::CGet(m) => Some(m.transaction_id),
            ClientMessage::PGet(m) => Some(m.transaction_id),
            ClientMessage::Set(m) => Some(m.transaction_id),
            ClientMessage::CSet(m) => Some(m.transaction_id),
            ClientMessage::SPubInit(m) => Some(m.transaction_id),
            ClientMessage::SPub(m) => Some(m.transaction_id),
            ClientMessage::Publish(m) => Some(m.transaction_id),
            ClientMessage::Subscribe(m) => Some(m.transaction_id),
            ClientMessage::PSubscribe(m) => Some(m.transaction_id),
            ClientMessage::Unsubscribe(m) => Some(m.transaction_id),
            ClientMessage::Delete(m) => Some(m.transaction_id),
            ClientMessage::PDelete(m) => Some(m.transaction_id),
            ClientMessage::Ls(m) => Some(m.transaction_id),
            ClientMessage::PLs(m) => Some(m.transaction_id),
            ClientMessage::SubscribeLs(m) => Some(m.transaction_id),
            ClientMessage::UnsubscribeLs(m) => Some(m.transaction_id),
            ClientMessage::Lock(m) => Some(m.transaction_id),
            ClientMessage::AcquireLock(m) => Some(m.transaction_id),
            ClientMessage::ReleaseLock(m) => Some(m.transaction_id),
            ClientMessage::Transform(m) => Some(m.transaction_id),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolSwitchRequest {
    pub version: ProtocolVersionSegment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationRequest {
    pub auth_token: AuthToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Get {
    pub transaction_id: TransactionId,
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
pub struct CSet {
    pub transaction_id: TransactionId,
    pub key: Key,
    pub value: Value,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SPubInit {
    pub transaction_id: TransactionId,
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SPub {
    pub transaction_id: TransactionId,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Publish {
    pub transaction_id: TransactionId,
    pub key: Key,
    pub value: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscribe {
    pub transaction_id: TransactionId,
    pub key: RequestPattern,
    pub unique: UniqueFlag,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_only: Option<LiveOnlyFlag>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PSubscribe {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
    pub unique: UniqueFlag,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_events: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_only: Option<LiveOnlyFlag>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Unsubscribe {
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delete {
    pub transaction_id: TransactionId,
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PDelete {
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
    pub quiet: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ls {
    pub transaction_id: TransactionId,
    pub parent: Option<Key>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PLs {
    pub transaction_id: TransactionId,
    pub parent_pattern: Option<RequestPattern>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeLs {
    pub transaction_id: TransactionId,
    pub parent: Option<Key>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeLs {
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lock {
    pub transaction_id: TransactionId,
    pub key: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transform {
    pub transaction_id: TransactionId,
    pub key: Key,
    pub template: Value,
}

#[cfg(test)]
mod test {

    use super::*;
    use serde_json::json;

    #[test]
    fn auth_request_is_serialized_correctly() {
        let msg = ClientMessage::AuthorizationRequest(AuthorizationRequest {
            auth_token: "123456".to_owned(),
        });

        let json = r#"{"authorizationRequest":{"authToken":"123456"}}"#;

        assert_eq!(&serde_json::to_string(&msg).unwrap(), json);
    }

    #[test]
    fn auth_request_is_deserialized_correctly() {
        let msg = ClientMessage::AuthorizationRequest(AuthorizationRequest {
            auth_token: "123456".to_owned(),
        });

        let json = r#"{
            "authorizationRequest": {
              "authToken": "123456"
            }
          }"#;

        assert_eq!(serde_json::from_str::<ClientMessage>(json).unwrap(), msg);
    }

    #[test]
    fn set_is_deserialized_correctly() {
        let json = r#"{"set": {"transactionId": 2, "key": "hello/world", "value": { "this value": "is a ", "complex": "JSON object"}}}"#;
        let msg = serde_json::from_str::<ClientMessage>(json).unwrap();
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
            live_only: None,
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
            live_only: Some(true),
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true,"aggregateEvents":10,"liveOnly":true}}"#
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
                live_only: None,
            })
        );
    }

    #[test]
    fn psubscribe_with_aggregation_is_deserialized_correctly() {
        let json = r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true,"aggregateEvents":10,"liveOnly":false}}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();

        assert_eq!(
            msg,
            ClientMessage::PSubscribe(PSubscribe {
                transaction_id: 1,
                request_pattern: "hello/world".to_owned(),
                unique: true,
                aggregate_events: Some(10),
                live_only: Some(false),
            })
        );
    }

    #[test]
    fn transform_is_serialized_correctly() {
        let msg = ClientMessage::Transform(Transform {
            transaction_id: 123,
            key: "test/transformed/key".to_owned(),
            template: json!({
              "name": "@some/person/name",
              "email": "@some/person/email",
              "phone": "@some/person/phone",
              "meta": {
                "nested": "@some/completely/unrelated/key",
                "info": "this is not a key reference and will remain in the transformed state"
              }
            }),
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"transform":{"transactionId":123,"key":"test/transformed/key","template":{"email":"@some/person/email","meta":{"info":"this is not a key reference and will remain in the transformed state","nested":"@some/completely/unrelated/key"},"name":"@some/person/name","phone":"@some/person/phone"}}}"#
        );
    }

    #[test]
    fn transform_is_deserialized_correctly() {
        let json = r#"{
                "transform": {
                  "transactionId": 123,
                  "key": "test/transformed/key",
                  "template": {
                    "name": "@some/person/name",
                    "email": "@some/person/email",
                    "phone": "@some/person/phone",
                    "meta": {
                      "nested": "@some/completely/unrelated/key",
                      "info": "this is not a key reference and will remain in the transformed state"
                    }
                  }
                }
              }
              "#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();

        assert_eq!(
            msg,
            ClientMessage::Transform(Transform {
                transaction_id: 123,
                key: "test/transformed/key".to_owned(),
                template: json!({
                  "name": "@some/person/name",
                  "email": "@some/person/email",
                  "phone": "@some/person/phone",
                  "meta": {
                    "nested": "@some/completely/unrelated/key",
                    "info": "this is not a key reference and will remain in the transformed state"
                  }
                }),
            })
        );
    }

    #[test]
    fn spub_init_is_deserialized_correctly() {
        let json = r#"{"sPubInit": {"transactionId": 2, "key": "hello/world"}}"#;
        let expected = ClientMessage::SPubInit(SPubInit {
            transaction_id: 2,
            key: "hello/world".into(),
        });
        assert_eq!(
            serde_json::from_str::<ClientMessage>(json).unwrap(),
            expected
        );
    }

    #[test]
    fn spub_is_deserialized_correctly() {
        let json = r#"{"sPub": {"transactionId": 2, "value": 123}}"#;
        let expected = ClientMessage::SPub(SPub {
            transaction_id: 2,
            value: json!(123),
        });
        assert_eq!(
            serde_json::from_str::<ClientMessage>(json).unwrap(),
            expected
        );
    }

    #[test]
    fn spub_init_is_serialized_correctly() {
        let json = r#"{"sPubInit":{"transactionId":2,"key":"hello/world"}}"#;
        let expected = SPubInit {
            transaction_id: 2,
            key: "hello/world".into(),
        };
        assert_eq!(
            &serde_json::to_string(&ClientMessage::SPubInit(expected)).unwrap(),
            json
        );
    }

    #[test]
    fn spub_is_serialized_correctly() {
        let json = r#"{"sPub":{"transactionId":2,"value":123}}"#;
        let expected = SPub {
            transaction_id: 2,
            value: json!(123),
        };
        assert_eq!(
            &serde_json::to_string(&ClientMessage::SPub(expected)).unwrap(),
            json
        );
    }
}
