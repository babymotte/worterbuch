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
    request_pattern: RequestPattern,
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
    use crate::error::{DecodeError, DecodeResult};

    pub fn read_message(mut data: impl Read) -> DecodeResult<Message> {
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
            _ => Err(DecodeError::with_message(format!(
                "type byte {:b} not defined",
                buf[0]
            ))),
        }
    }

    fn read_get_message(mut data: impl Read) -> DecodeResult<Get> {
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

    fn read_set_message(mut data: impl Read) -> DecodeResult<Set> {
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

    fn read_subscribe_message(mut data: impl Read) -> DecodeResult<Subscribe> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        let mut buf = [0; 2];
        data.read_exact(&mut buf)?;
        let request_pattern_length = u16::from_be_bytes(buf);

        let mut buf = vec![0u8; request_pattern_length as usize];
        data.read_exact(&mut buf)?;
        let request_pattern = String::from_utf8_lossy(&buf).to_string();

        Ok(Subscribe {
            transaction_id,
            request_pattern,
        })
    }

    fn read_state_message(mut data: impl Read) -> DecodeResult<State> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        let mut buf = [0; 2];
        data.read_exact(&mut buf)?;
        let request_pattern_length = u16::from_be_bytes(buf);

        let mut buf = [0; 4];
        data.read_exact(&mut buf)?;
        let num_key_val_pairs = u32::from_be_bytes(buf);

        let mut key_value_lengths = Vec::new();

        for _ in 0..num_key_val_pairs {
            let mut buf = [0; 2];
            data.read_exact(&mut buf)?;
            let key_length = u16::from_be_bytes(buf);

            let mut buf = [0; 4];
            data.read_exact(&mut buf)?;
            let value_length = u32::from_be_bytes(buf);

            key_value_lengths.push((key_length, value_length));
        }

        let mut buf = vec![0u8; request_pattern_length as usize];
        data.read_exact(&mut buf)?;
        let request_pattern = String::from_utf8_lossy(&buf).to_string();

        let mut key_value_pairs = Vec::new();

        for (key_length, value_length) in key_value_lengths {
            let mut buf = vec![0u8; key_length as usize];
            data.read_exact(&mut buf)?;
            let key = String::from_utf8(buf)?;

            let mut buf = vec![0u8; value_length as usize];
            data.read_exact(&mut buf)?;
            let value = String::from_utf8_lossy(&buf).to_string();

            key_value_pairs.push((key, value));
        }

        Ok(State {
            transaction_id,
            request_pattern,
            key_value_pairs,
        })
    }

    fn read_ack_message(mut data: impl Read) -> DecodeResult<Ack> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        Ok(Ack { transaction_id })
    }

    fn read_event_message(mut data: impl Read) -> DecodeResult<Event> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        let mut buf = [0; 2];
        data.read_exact(&mut buf)?;
        let request_pattern_length = u16::from_be_bytes(buf);

        let mut buf = [0; 2];
        data.read_exact(&mut buf)?;
        let key_length = u16::from_be_bytes(buf);

        let mut buf = [0; 4];
        data.read_exact(&mut buf)?;
        let value_length = u32::from_be_bytes(buf);

        let mut buf = vec![0u8; request_pattern_length as usize];
        data.read_exact(&mut buf)?;
        let request_pattern = String::from_utf8_lossy(&buf).to_string();

        let mut buf = vec![0u8; key_length as usize];
        data.read_exact(&mut buf)?;
        let key = String::from_utf8_lossy(&buf).to_string();

        let mut buf = vec![0u8; value_length as usize];
        data.read_exact(&mut buf)?;
        let value = String::from_utf8_lossy(&buf).to_string();

        Ok(Event {
            transaction_id,
            request_pattern,
            key,
            value,
        })
    }

    fn read_err_message(mut data: impl Read) -> DecodeResult<Err> {
        let mut buf = [0; 8];
        data.read_exact(&mut buf)?;
        let transaction_id = u64::from_be_bytes(buf);

        let mut buf = [0; 1];
        data.read_exact(&mut buf)?;
        let error_code = buf[0];

        let mut buf = [0; 4];
        data.read_exact(&mut buf)?;
        let metadata_length = u32::from_be_bytes(buf);

        let mut buf = vec![0u8; metadata_length as usize];
        data.read_exact(&mut buf)?;
        let metadata = String::from_utf8_lossy(&buf).to_string();

        Ok(Err {
            transaction_id,
            error_code,
            metadata,
        })
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

        #[test]
        fn subscribe_message_is_read_correctly() {
            let data = [
                SUB, 0b00000000, 0b00000000, 0b00000101, 0b00001001, 0b00011100, 0b00100000,
                0b01110000, 0b10010111, 0b00000000, 0b00011001, b'l', b'e', b't', b'/', b'm', b'e',
                b'/', b'?', b'/', b'y', b'o', b'u', b'/', b'i', b't', b's', b'/', b'f', b'e', b'a',
                b't', b'u', b'r', b'e', b's',
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(
                result,
                Message::Subscribe(Subscribe {
                    transaction_id: 5536684732567,
                    request_pattern: "let/me/?/you/its/features".to_owned()
                })
            )
        }

        #[test]
        fn state_message_is_read_correctly() {
            let data = [
                STA, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
                0b11111111, 0b11111111, 0b00000000, 0b00001111, 0b00000000, 0b00000000, 0b00000000,
                0b00000010, 0b00000000, 0b00100010, 0b00000000, 0b00000000, 0b00000000, 0b00011010,
                0b00000000, 0b00010100, 0b00000000, 0b00000000, 0b00000000, 0b00011000, b'w', b'h',
                b'o', b'/', b'l', b'e', b't', b'/', b't', b'h', b'e', b'/', b'?', b'/', b'#', b'w',
                b'h', b'o', b'/', b'l', b'e', b't', b'/', b't', b'h', b'e', b'/', b'c', b'h', b'i',
                b'c', b'k', b'e', b'n', b'/', b'c', b'r', b'o', b's', b's', b'/', b't', b'h', b'e',
                b'/', b'r', b'o', b'a', b'd', b'y', b'e', b'a', b'h', b',', b' ', b't', b'h', b'a',
                b't', b' ', b'w', b'a', b's', b' ', b'm', b'e', b',', b' ', b'I', b' ', b'g', b'u',
                b'e', b's', b's', b'w', b'h', b'o', b'/', b'l', b'e', b't', b'/', b't', b'h', b'e',
                b'/', b'd', b'o', b'g', b's', b'/', b'o', b'u', b't', b'W', b'h', b'o', b'?', b' ',
                b'W', b'h', b'o', b'?', b' ', b'W', b'h', b'o', b'?', b' ', b'W', b'h', b'o', b'?',
                b' ', b'W', b'h', b'o', b'?',
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(
                result,
                Message::State(State {
                    transaction_id: u64::MAX,
                    request_pattern: "who/let/the/?/#".to_owned(),
                    key_value_pairs: vec![
                        (
                            "who/let/the/chicken/cross/the/road".to_owned(),
                            "yeah, that was me, I guess".to_owned()
                        ),
                        (
                            "who/let/the/dogs/out".to_owned(),
                            "Who? Who? Who? Who? Who?".to_owned()
                        )
                    ]
                })
            )
        }

        #[test]
        fn ack_message_is_read_correctly() {
            let data = [
                ACK, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
                0b00000000, 0b00101010,
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(result, Message::Ack(Ack { transaction_id: 42 }))
        }

        #[test]
        fn event_message_is_read_correctly() {
            let data = [
                EVE, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
                0b00000000, 0b00101010, 0b00000000, 0b00000101, 0b00000000, 0b00000101, 0b00000000,
                0b00000000, 0b00000000, 0b00000001, b'1', b'/', b'2', b'/', b'3', b'1', b'/', b'2',
                b'/', b'3', b'4',
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(
                result,
                Message::Event(Event {
                    transaction_id: 42,
                    request_pattern: "1/2/3".to_owned(),
                    key: "1/2/3".to_owned(),
                    value: "4".to_owned()
                })
            )
        }

        #[test]
        fn err_message_is_read_correctly() {
            let data = [
                ERR, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
                0b00000000, 0b00101010, 0b00000101, 0b00000000, 0b00000000, 0b00000000, 0b00010001,
                b'T', b'H', b'I', b'S', b' ', b'I', b'S', b' ', b'M', b'E', b'T', b'A', b'A', b'A',
                b'!', b'!', b'!',
            ];

            let result = read_message(&data[..]).unwrap();

            assert_eq!(
                result,
                Message::Err(Err {
                    transaction_id: 42,
                    error_code: 5,
                    metadata: "THIS IS METAAA!!!".to_owned()
                })
            )
        }
    }
}

#[cfg(not(feature = "async"))]
pub use blocking::*;
