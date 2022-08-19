use crate::{Key, MessageType, Path, RequestPattern, TransactionId, Value};
use serde::{Deserialize, Serialize};

pub const GET: MessageType = 0b00000000;
pub const SET: MessageType = 0b00000001;
pub const SUB: MessageType = 0b00000010;
pub const PGET: MessageType = 0b00000011;
pub const PSUB: MessageType = 0b00000100;
pub const EXP: MessageType = 0b00000101;
pub const IMP: MessageType = 0b00000110;
pub const USUB: MessageType = 0b00000111;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientMessage {
    Get(Get),
    PGet(PGet),
    Set(Set),
    Subscribe(Subscribe),
    PSubscribe(PSubscribe),
    Export(Export),
    Import(Import),
    Unsubscribe(Unsubscribe),
}

impl ClientMessage {
    pub fn transaction_id(&self) -> TransactionId {
        match self {
            ClientMessage::Get(m) => m.transaction_id,
            ClientMessage::PGet(m) => m.transaction_id,
            ClientMessage::Set(m) => m.transaction_id,
            ClientMessage::Subscribe(m) => m.transaction_id,
            ClientMessage::PSubscribe(m) => m.transaction_id,
            ClientMessage::Export(m) => m.transaction_id,
            ClientMessage::Import(m) => m.transaction_id,
            ClientMessage::Unsubscribe(m) => m.transaction_id,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Set {
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
