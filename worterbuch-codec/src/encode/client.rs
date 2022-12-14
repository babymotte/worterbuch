use super::{
    get_key_length, get_num_grave_goods, get_num_last_will, get_num_protocol_versions,
    get_path_length, get_request_pattern_length, get_value_length,
};
use crate::{
    error::EncodeResult, ClientMessage, Export, Get, HandshakeRequest, Import, KeyValuePair, PGet,
    PSubscribe, ProtocolVersion, Set, Subscribe, Unsubscribe, EXP, GET, HSHKR, IMP, PGET, PSUB,
    SET, SUB, USUB,
};

pub fn encode_message(msg: &ClientMessage) -> EncodeResult<Vec<u8>> {
    match msg {
        ClientMessage::HandshakeRequest(msg) => encode_handshake_request_message(msg),
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

pub fn encode_handshake_request_message(msg: &HandshakeRequest) -> EncodeResult<Vec<u8>> {
    let num_protocol_versions = get_num_protocol_versions(&msg.supported_protocol_versions)?;
    let num_last_will = get_num_last_will(&msg.last_will)?;
    let num_grave_goods = get_num_grave_goods(&msg.grave_goods)?;

    let mut buf = vec![HSHKR];

    buf.extend(num_protocol_versions.to_be_bytes());
    buf.extend(num_last_will.to_be_bytes());
    buf.extend(num_grave_goods.to_be_bytes());

    for ProtocolVersion { major, minor } in &msg.supported_protocol_versions {
        buf.extend(major.to_be_bytes());
        buf.extend(minor.to_be_bytes());
    }

    for KeyValuePair { key, value } in &msg.last_will {
        let key_length = get_key_length(&key)?;
        let value_length = get_value_length(&value)?;
        buf.extend(key_length.to_be_bytes());
        buf.extend(value_length.to_be_bytes());
    }

    for grave_good in &msg.grave_goods {
        let key_length = get_key_length(&grave_good)?;
        buf.extend(key_length.to_be_bytes());
    }

    for KeyValuePair { key, value } in &msg.last_will {
        buf.extend(key.as_bytes());
        buf.extend(value.as_bytes());
    }

    for grave_good in &msg.grave_goods {
        buf.extend(grave_good.as_bytes());
    }

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

    #[test]
    fn handshake_request_message_is_encoded_correctly() {
        let msg = HandshakeRequest {
            supported_protocol_versions: vec![
                ProtocolVersion { major: 0, minor: 1 },
                ProtocolVersion { major: 0, minor: 5 },
                ProtocolVersion { major: 1, minor: 0 },
            ],
            last_will: vec![("last/will", "test").into()],
            grave_goods: vec!["grave/goods/1".into(), "grave/goods/2".into()],
        };

        let data = vec![
            HSHKR,      // message type
            0b00000011, // 3 protocol versions
            0b00000001, // 1 last will
            0b00000010, // 2 grave goods
            0b00000000, 0b00000000, 0b00000000, 0b00000001, // protocol version 0.1
            0b00000000, 0b00000000, 0b00000000, 0b00000101, // protocol version 0.5
            0b00000000, 0b00000001, 0b00000000, 0b00000000, // protocol version 1.0
            0b00000000, 0b00001001, // last will key length (9)
            0b00000000, 0b00000000, 0b00000000, 0b00000100, // last will value length (4)
            0b00000000, 0b00001101, // grave good 1 key length (13)
            0b00000000, 0b00001101, // grave good 2 key length (13)
            b'l', b'a', b's', b't', b'/', b'w', b'i', b'l', b'l', // last will key
            b't', b'e', b's', b't', // last will value
            b'g', b'r', b'a', b'v', b'e', b'/', b'g', b'o', b'o', b'd', b's', b'/',
            b'1', // grave goods 1 key
            b'g', b'r', b'a', b'v', b'e', b'/', b'g', b'o', b'o', b'd', b's', b'/',
            b'2', // grave goods 2 key
        ];

        assert_eq!(data, encode_handshake_request_message(&msg).unwrap());
    }
}
