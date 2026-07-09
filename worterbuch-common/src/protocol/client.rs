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

use crate::protocol::{
    AggregationDuration, AuthToken, Key, LiveOnlyFlag, ProtocolVersionSegment, QuietFlag,
    RequestPattern, TransactionId, UniqueFlag, Value,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
    // Transform(Transform),
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
            // ClientMessage::Transform(m) => Some(m.transaction_id),
        }
    }
}

/// A message sent by a client to request switching to the specified protocol major version
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolSwitchRequest {
    pub version: ProtocolVersionSegment,
}

/// A message sent by a client to acquire authorization from the server
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationRequest {
    pub auth_token: AuthToken,
}

/// A message sent by a client to request the value of the provided key from the server
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Get {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    pub key: Key,
}

/// A message sent by a client to request the values of all keys matching the provided pattern from the server
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PGet {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    pub request_pattern: RequestPattern,
}

/// A message sent by a client to set a new value for a key
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Set {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The key for which to set the value
    pub key: Key,
    /// The new value for the key
    pub value: Value,
}

/// A message sent by a client to set a new value for a key using compare and swap
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CSet {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The key for which to set the value
    pub key: Key,
    /// The new value for the key
    pub value: Value,
    /// The expected current value version or 0 if the value is not expected to exist yet
    pub version: u64,
}

/// A message sent by a client to initiate a new publish stream
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SPubInit {
    /// A unique transaction ID. This transaction ID will be used instead of a key for all subsequent publish messages in this stream.
    pub transaction_id: TransactionId,
    /// The key this stream will publish to
    pub key: Key,
}

/// A message sent by a client to publish to an existing pub stream
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SPub {
    /// The transaction ID of the pub stream to publish to
    pub transaction_id: TransactionId,
    /// The value to be published
    pub value: Value,
}

/// A message sent by a client to publish a new value for a key. The value will not be persisted on the server
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Publish {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The key for which to publish the value
    pub key: Key,
    /// The value to be published for the key
    pub value: Value,
}

/// A message sent by a client to subscribe to values of the provided key
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Subscribe {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The key to subscribe to
    pub key: RequestPattern,
    /// Indicate whether all or only unique values should be received
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unique: Option<UniqueFlag>,
    /// Indicate whether there should be a callback for data already stored on the broker (false) or only for live events (true)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub live_only: Option<LiveOnlyFlag>,
}

/// A message sent by a client to subscribe to values of all keys matching the provided pattern
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PSubscribe {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The pattern to subscribe to
    pub request_pattern: RequestPattern,
    /// Indicate whether all or only unique values should be received
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unique: Option<UniqueFlag>,
    /// Indicate whether there should be a callback for data already stored on the broker (false) or only for live events (true)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub live_only: Option<LiveOnlyFlag>,
    /// Optionally aggregate events for the given number of milliseconds before sending them to the client to reduce network traffic
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub aggregate_events: Option<AggregationDuration>,
}

/// A message sent by a client to request the cancellation of the subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Unsubscribe {
    /// The transaction ID of the subscription to be cancelled
    pub transaction_id: TransactionId,
}

/// A message sent by a client to request the deletion of the value of the provided key
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Delete {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The key to subscribe to
    pub key: Key,
}

/// A message sent by a client to request the deletion of the values of all keys matching the provided pattern
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PDelete {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The deletion pattern
    pub request_pattern: RequestPattern,
    /// If true, the server will not send the deleted values back to the client
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub quiet: Option<QuietFlag>,
}

/// A message sent by a client to list all direct sub-key segments of the provided partial key
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Ls {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The parent partial key for which to list sub-key segments
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent: Option<Key>,
}

/// A message sent by a client to list all direct sub-key segments of all partial keys matching the provided pattern
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PLs {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// A pattern describing the parent partial keys for which to list sub-key segments
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent_pattern: Option<RequestPattern>,
}

/// A message sent by a client to request a subscription to all direct sub-key segments of the provided partial key
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeLs {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The parent partial key for which to list sub-key segments
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent: Option<Key>,
}

/// A message sent by a client to request the cancellation of an ls subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeLs {
    /// The transaction ID of the ls subscription to be cancelled
    pub transaction_id: TransactionId,
}

/// A message sent by a client to request a lock on the specified key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Lock {
    /// A unique transaction ID
    pub transaction_id: TransactionId,
    /// The key to get a lock on
    pub key: Key,
}

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
// #[serde(rename_all = "camelCase")]
// pub struct Transform {
//     pub transaction_id: TransactionId,
//     pub key: Key,
//     pub template: Value,
// }

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

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
            unique: Some(true),
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
            unique: Some(true),
            aggregate_events: Some(10),
            live_only: Some(true),
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"pSubscribe":{"transactionId":1,"requestPattern":"hello/world","unique":true,"liveOnly":true,"aggregateEvents":10}}"#
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
                unique: Some(true),
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
                unique: Some(true),
                aggregate_events: Some(10),
                live_only: Some(false),
            })
        );
    }

    // #[test]
    // fn transform_is_serialized_correctly() {
    //     let msg = ClientMessage::Transform(Transform {
    //         transaction_id: 123,
    //         key: "test/transformed/key".to_owned(),
    //         template: json!({
    //           "name": "@some/person/name",
    //           "email": "@some/person/email",
    //           "phone": "@some/person/phone",
    //           "meta": {
    //             "nested": "@some/completely/unrelated/key",
    //             "info": "this is not a key reference and will remain in the transformed state"
    //           }
    //         }),
    //     });

    //     let json = serde_json::to_string(&msg).unwrap();
    //     assert_eq!(
    //         json,
    //         r#"{"transform":{"transactionId":123,"key":"test/transformed/key","template":{"email":"@some/person/email","meta":{"info":"this is not a key reference and will remain in the transformed state","nested":"@some/completely/unrelated/key"},"name":"@some/person/name","phone":"@some/person/phone"}}}"#
    //     );
    // }

    // #[test]
    // fn transform_is_deserialized_correctly() {
    //     let json = r#"{
    //             "transform": {
    //               "transactionId": 123,
    //               "key": "test/transformed/key",
    //               "template": {
    //                 "name": "@some/person/name",
    //                 "email": "@some/person/email",
    //                 "phone": "@some/person/phone",
    //                 "meta": {
    //                   "nested": "@some/completely/unrelated/key",
    //                   "info": "this is not a key reference and will remain in the transformed state"
    //                 }
    //               }
    //             }
    //           }
    //           "#;
    //     let msg: ClientMessage = serde_json::from_str(json).unwrap();

    //     assert_eq!(
    //         msg,
    //         ClientMessage::Transform(Transform {
    //             transaction_id: 123,
    //             key: "test/transformed/key".to_owned(),
    //             template: json!({
    //               "name": "@some/person/name",
    //               "email": "@some/person/email",
    //               "phone": "@some/person/phone",
    //               "meta": {
    //                 "nested": "@some/completely/unrelated/key",
    //                 "info": "this is not a key reference and will remain in the transformed state"
    //               }
    //             }),
    //         })
    //     );
    // }

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
