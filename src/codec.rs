use serde::{Deserialize, Serialize};

pub type TransactionId = u64;
pub type RequestPattern = String;
pub type Key = String;
pub type Value = String;
pub type KeyValuePairs = Vec<(String, String)>;
pub type ErrorCode = u8;
pub type MetaData = String;

pub type RequestPatternLength = u16;
pub type KeyLength = u16;
pub type ValueLength = u32;
pub type MetaDataLength = u32;
pub type NumKeyValuePairs = u32;

pub const GET: u8 = 0b00000000;
pub const SET: u8 = 0b00000001;
pub const SUB: u8 = 0b00000010;

pub const STA: u8 = 0b10000000;
pub const ACK: u8 = 0b10000001;
pub const EVE: u8 = 0b10000010;
pub const ERR: u8 = 0b10000011;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Message {
    // client messages
    Get(Get),
    Set(Set),
    Subscribe(Subscribe),
    // server messages
    State(State),
    Ack(Ack),
    Event(Event),
    Err(Err),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Get {
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Set {
    transaction_id: TransactionId,
    key: Key,
    value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Subscribe {
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct State {
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    key_value_pairs: KeyValuePairs,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ack {
    transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Event {
    transaction_id: TransactionId,
    key: Key,
    value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Err {
    transaction_id: TransactionId,
    error_code: ErrorCode,
    metadata: MetaData,
}

#[cfg(not(feature = "async"))]
mod blocking {
    use std::io::Read;

    use super::{
        Ack, Err, Event, Get, Message, Set, State, Subscribe, ACK, ERR, EVE, GET, SET, STA, SUB,
    };
    use crate::error::{Error, Result};

    pub fn read_message(mut data: impl Read) -> Result<Message> {
        let mut buf = [0];
        data.read_exact(&mut buf)?;
        match buf[0] {
            // client messages
            GET => read_get_message(data).map(Message::Get),
            SET => read_set_message(data).map(Message::Set),
            SUB => read_subscribe_message(data).map(Message::Subscribe),
            // server messages
            STA => read_state_message(data).map(Message::State),
            ACK => read_ack_message(data).map(Message::Ack),
            EVE => read_event_message(data).map(Message::Event),
            ERR => read_err_message(data).map(Message::Err),
            // undefined
            _ => Err(Error::DecodeError(format!(
                "type byte {:b} not defined",
                buf[0]
            ))),
        }
    }

    fn read_get_message(mut data: impl Read) -> Result<Get> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        let mut buf = [0; 2];
        data.read_exact(&mut buf)?;
        let request_pattern_length = u16::from_be_bytes(buf);

        let mut buf = vec![0u8; request_pattern_length as usize];
        data.read_exact(&mut buf)?;
        let request_pattern = String::from_utf8_lossy(&buf).to_string();

        Ok(Get {
            transaction_id,
            request_pattern,
        })
    }

    fn read_set_message(mut data: impl Read) -> Result<Set> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        let mut buf = [0; 2];
        data.read_exact(&mut buf)?;
        let key_length = u16::from_be_bytes(buf);

        let mut buf = [0; 4];
        data.read_exact(&mut buf)?;
        let value_length = u32::from_be_bytes(buf);

        let mut buf = vec![0u8; key_length as usize];
        data.read_exact(&mut buf)?;
        let key = String::from_utf8_lossy(&buf).to_string();

        let mut buf = vec![0u8; value_length as usize];
        data.read_exact(&mut buf)?;
        let value = String::from_utf8_lossy(&buf).to_string();

        Ok(Set {
            transaction_id,
            key,
            value,
        })
    }

    fn read_subscribe_message(mut data: impl Read) -> Result<Subscribe> {
        todo!()
    }

    fn read_state_message(mut data: impl Read) -> Result<State> {
        todo!()
    }

    fn read_ack_message(mut data: impl Read) -> Result<Ack> {
        todo!()
    }

    fn read_event_message(mut data: impl Read) -> Result<Event> {
        todo!()
    }

    fn read_err_message(mut data: impl Read) -> Result<Err> {
        todo!()
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn get_message_is_read_correctly() {
            let data = [
                GET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
                0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(
                result,
                Message::Get(Get {
                    transaction_id: 4,
                    request_pattern: "trolo".to_owned()
                })
            )
        }

        #[test]
        fn set_message_is_read_correctly() {
            let data = [
                SET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
                0b00000000, 0b00000000, 0b00000000, 0b00000111, 0b00000000, 0b00000000, 0b00000000,
                0b00000011, b'y', b'o', b'/', b'm', b'a', b'm', b'a', b'f', b'a', b't',
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(
                result,
                Message::Set(Set {
                    transaction_id: 0,
                    key: "yo/mama".to_owned(),
                    value: "fat".to_owned()
                })
            )
        }
    }
}

#[cfg(not(feature = "async"))]
pub use blocking::*;
