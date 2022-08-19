use crate::{
    error::EncodeResult, ClientMessage, Export, Get, Import, PGet, PSubscribe, Set, Subscribe,
    Unsubscribe, EXP, GET, IMP, PGET, PSUB, SET, SUB, USUB,
};

use super::{get_key_length, get_path_length, get_request_pattern_length, get_value_length};

pub fn encode_message(msg: &ClientMessage) -> EncodeResult<Vec<u8>> {
    match msg {
        ClientMessage::Get(msg) => encode_get_message(msg),
        ClientMessage::PGet(msg) => encode_pget_message(msg),
        ClientMessage::Set(msg) => encode_set_message(msg),
        ClientMessage::Subscribe(msg) => encode_subscribe_message(msg),
        ClientMessage::PSubscribe(msg) => encode_psubscribe_message(msg),
        ClientMessage::Export(msg) => encode_export_message(msg),
        ClientMessage::Import(msg) => encode_import_message(msg),
        ClientMessage::Unsubscribe(msg) => encode_unsubscribe_message(msg),
    }
}

pub fn encode_get_message(msg: &Get) -> EncodeResult<Vec<u8>> {
    let key_length = get_key_length(&msg.key)?;

    let mut buf = vec![GET];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(key_length.to_be_bytes());
    buf.extend(msg.key.as_bytes());

    Ok(buf)
}

pub fn encode_pget_message(msg: &PGet) -> EncodeResult<Vec<u8>> {
    let request_pattern_length = get_request_pattern_length(&msg.request_pattern)?;

    let mut buf = vec![PGET];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(request_pattern_length.to_be_bytes());
    buf.extend(msg.request_pattern.as_bytes());

    Ok(buf)
}

pub fn encode_set_message(msg: &Set) -> EncodeResult<Vec<u8>> {
    let key_length = get_key_length(&msg.key)?;
    let value_length = get_value_length(&msg.value)?;

    let mut buf = vec![SET];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(key_length.to_be_bytes());
    buf.extend(value_length.to_be_bytes());
    buf.extend(msg.key.as_bytes());
    buf.extend(msg.value.as_bytes());

    Ok(buf)
}

pub fn encode_subscribe_message(msg: &Subscribe) -> EncodeResult<Vec<u8>> {
    let key_length = get_key_length(&msg.key)?;

    let mut buf = vec![SUB];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(key_length.to_be_bytes());
    buf.extend(msg.key.as_bytes());
    buf.push(if msg.unique { 1 } else { 0 });

    Ok(buf)
}

pub fn encode_psubscribe_message(msg: &PSubscribe) -> EncodeResult<Vec<u8>> {
    let request_pattern_length = get_request_pattern_length(&msg.request_pattern)?;

    let mut buf = vec![PSUB];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(request_pattern_length.to_be_bytes());
    buf.extend(msg.request_pattern.as_bytes());
    buf.push(if msg.unique { 1 } else { 0 });

    Ok(buf)
}

pub fn encode_export_message(msg: &Export) -> EncodeResult<Vec<u8>> {
    let path_length = get_path_length(&msg.path)?;

    let mut buf = vec![EXP];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(path_length.to_be_bytes());
    buf.extend(msg.path.as_bytes());

    Ok(buf)
}

pub fn encode_import_message(msg: &Import) -> EncodeResult<Vec<u8>> {
    let path_length = get_path_length(&msg.path)?;

    let mut buf = vec![IMP];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(path_length.to_be_bytes());
    buf.extend(msg.path.as_bytes());

    Ok(buf)
}

pub fn encode_unsubscribe_message(msg: &Unsubscribe) -> EncodeResult<Vec<u8>> {
    let mut buf = vec![USUB];

    buf.extend(msg.transaction_id.to_be_bytes());

    Ok(buf)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        Export, Get, Import, PGet, PSubscribe, Set, Subscribe, Unsubscribe, EXP, GET, IMP, PGET,
        PSUB, SET, SUB, USUB,
    };

    #[test]
    fn get_message_is_encoded_correctly() {
        let msg = Get {
            transaction_id: 4,
            key: "trolo".to_owned(),
        };

        let data = vec![
            GET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
        ];

        assert_eq!(data, encode_get_message(&msg).unwrap());
    }

    #[test]
    fn pget_message_is_encoded_correctly() {
        let msg = PGet {
            transaction_id: 4,
            request_pattern: "trolo".to_owned(),
        };

        let data = vec![
            PGET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
        ];

        assert_eq!(data, encode_pget_message(&msg).unwrap());
    }

    #[test]
    fn set_message_is_encoded_correctly() {
        let msg = Set {
            transaction_id: 0,
            key: "yo/mama".to_owned(),
            value: "fat".to_owned(),
        };

        let data = vec![
            SET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000000, 0b00000000, 0b00000111, 0b00000000, 0b00000000, 0b00000000,
            0b00000011, b'y', b'o', b'/', b'm', b'a', b'm', b'a', b'f', b'a', b't',
        ];

        assert_eq!(data, encode_set_message(&msg).unwrap());
    }

    #[test]
    fn subscribe_message_is_encoded_correctly() {
        let msg = Subscribe {
            transaction_id: 5536684732567,
            key: "let/me/?/you/its/features".to_owned(),
            unique: true,
        };

        let data = vec![
            SUB, 0b00000000, 0b00000000, 0b00000101, 0b00001001, 0b00011100, 0b00100000,
            0b01110000, 0b10010111, 0b00000000, 0b00011001, b'l', b'e', b't', b'/', b'm', b'e',
            b'/', b'?', b'/', b'y', b'o', b'u', b'/', b'i', b't', b's', b'/', b'f', b'e', b'a',
            b't', b'u', b'r', b'e', b's', 0b00000001,
        ];

        assert_eq!(data, encode_subscribe_message(&msg).unwrap());
    }

    #[test]
    fn psubscribe_message_is_encoded_correctly() {
        let msg = PSubscribe {
            transaction_id: 5536684732567,
            request_pattern: "let/me/?/you/its/features".to_owned(),
            unique: false,
        };

        let data = vec![
            PSUB, 0b00000000, 0b00000000, 0b00000101, 0b00001001, 0b00011100, 0b00100000,
            0b01110000, 0b10010111, 0b00000000, 0b00011001, b'l', b'e', b't', b'/', b'm', b'e',
            b'/', b'?', b'/', b'y', b'o', b'u', b'/', b'i', b't', b's', b'/', b'f', b'e', b'a',
            b't', b'u', b'r', b'e', b's', 0b00000000,
        ];

        assert_eq!(data, encode_psubscribe_message(&msg).unwrap());
    }

    #[test]
    fn export_message_is_encoded_correctly() {
        let msg = Export {
            transaction_id: 42,
            path: "/path/to/file".to_owned(),
        };

        let data = vec![
            EXP, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00001101, b'/', b'p', b'a', b't', b'h', b'/',
            b't', b'o', b'/', b'f', b'i', b'l', b'e',
        ];

        assert_eq!(data, encode_export_message(&msg).unwrap());
    }

    #[test]
    fn import_message_is_encoded_correctly() {
        let msg = Import {
            transaction_id: 42,
            path: "/path/to/file".to_owned(),
        };

        let data = vec![
            IMP, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00001101, b'/', b'p', b'a', b't', b'h', b'/',
            b't', b'o', b'/', b'f', b'i', b'l', b'e',
        ];

        assert_eq!(data, encode_import_message(&msg).unwrap());
    }

    #[test]
    fn unsubscribe_message_is_encoded_correctly() {
        let msg = Unsubscribe { transaction_id: 42 };

        let data = vec![
            USUB, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010,
        ];

        assert_eq!(data, encode_unsubscribe_message(&msg).unwrap());
    }
}
