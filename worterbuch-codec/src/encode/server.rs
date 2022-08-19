use crate::{
    error::EncodeResult, Ack, Err, Handshake, KeyValuePair, PState, ProtocolVersion, State, ACK,
    ERR, HSHK, PSTA, STA,
};

use super::{
    get_key_length, get_metadata_length, get_num_key_val_pairs, get_num_protocol_versions,
    get_request_pattern_length, get_value_length,
};

pub fn encode_pstate_message(msg: &PState) -> EncodeResult<Vec<u8>> {
    let request_pattern_length = get_request_pattern_length(&msg.request_pattern)?;
    let num_key_val_pairs = get_num_key_val_pairs(&msg.key_value_pairs)?;

    let mut buf = vec![PSTA];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(request_pattern_length.to_be_bytes());
    buf.extend(num_key_val_pairs.to_be_bytes());

    for KeyValuePair { key, value } in &msg.key_value_pairs {
        let key_length = get_key_length(&key)?;
        let value_length = get_value_length(&value)?;
        buf.extend(key_length.to_be_bytes());
        buf.extend(value_length.to_be_bytes());
    }

    buf.extend(msg.request_pattern.as_bytes());

    for KeyValuePair { key, value } in &msg.key_value_pairs {
        buf.extend(key.as_bytes());
        buf.extend(value.as_bytes());
    }

    Ok(buf)
}

pub fn encode_ack_message(msg: &Ack) -> EncodeResult<Vec<u8>> {
    let mut buf = vec![ACK];

    buf.extend(msg.transaction_id.to_be_bytes());

    Ok(buf)
}

pub fn encode_state_message(msg: &State) -> EncodeResult<Vec<u8>> {
    let KeyValuePair { key, value } = &msg.key_value;
    let key_length = get_key_length(key)?;
    let value_length = get_value_length(value)?;

    let mut buf = vec![STA];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.extend(key_length.to_be_bytes());
    buf.extend(value_length.to_be_bytes());

    buf.extend(key.as_bytes());
    buf.extend(value.as_bytes());

    Ok(buf)
}

pub fn encode_err_message(msg: &Err) -> EncodeResult<Vec<u8>> {
    let metadata_length = get_metadata_length(&msg.metadata)?;

    let mut buf = vec![ERR];

    buf.extend(msg.transaction_id.to_be_bytes());
    buf.push(msg.error_code);
    buf.extend(metadata_length.to_be_bytes());
    buf.extend(msg.metadata.as_bytes());

    Ok(buf)
}

pub fn encode_handshake_message(msg: &Handshake) -> EncodeResult<Vec<u8>> {
    let num_protocol_versions = get_num_protocol_versions(&msg.supported_protocol_versions)?;

    let mut buf = vec![HSHK];

    buf.extend(num_protocol_versions.to_be_bytes());

    for ProtocolVersion { major, minor } in &msg.supported_protocol_versions {
        buf.extend(major.to_be_bytes());
        buf.extend(minor.to_be_bytes());
    }

    buf.push(msg.separator as u8);
    buf.push(msg.wildcard as u8);
    buf.push(msg.multi_wildcard as u8);

    Ok(buf)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Ack, Err, Handshake, PState, ProtocolVersion, State, ACK, ERR, HSHK, PSTA, STA};

    #[test]
    fn handshake_message_is_encoded_correctly() {
        let msg = Handshake {
            supported_protocol_versions: vec![
                ProtocolVersion { major: 1, minor: 0 },
                ProtocolVersion { major: 1, minor: 1 },
                ProtocolVersion { major: 1, minor: 2 },
            ],
            separator: '/',
            wildcard: '?',
            multi_wildcard: '#',
        };

        let data = vec![
            HSHK, 0b00000011, 0b00000000, 0b00000001, 0b00000000, 0b00000000, 0b00000000,
            0b00000001, 0b00000000, 0b00000001, 0b00000000, 0b00000001, 0b00000000, 0b00000010,
            b'/', b'?', b'#',
        ];

        assert_eq!(data, encode_handshake_message(&msg).unwrap());
    }

    #[test]
    fn pstate_message_is_encoded_correctly() {
        let msg = PState {
            transaction_id: u64::MAX,
            request_pattern: "who/let/the/?/#".to_owned(),
            key_value_pairs: vec![
                (
                    "who/let/the/chicken/cross/the/road".to_owned(),
                    "yeah, that was me, I guess".to_owned(),
                )
                    .into(),
                (
                    "who/let/the/dogs/out".to_owned(),
                    "Who? Who? Who? Who? Who?".to_owned(),
                )
                    .into(),
            ],
        };

        let data = vec![
            PSTA, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
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

        assert_eq!(data, encode_pstate_message(&msg).unwrap());
    }

    #[test]
    fn ack_message_is_encoded_correctly() {
        let msg = Ack { transaction_id: 42 };

        let data = vec![
            ACK, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010,
        ];

        assert_eq!(data, encode_ack_message(&msg).unwrap());
    }

    #[test]
    fn state_message_is_encoded_correctly() {
        let msg = State {
            transaction_id: 42,
            key_value: ("1/2/3", "4").into(),
        };

        let data = vec![
            STA, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00000101, 0b00000000, 0b00000000, 0b00000000,
            0b00000001, b'1', b'/', b'2', b'/', b'3', b'4',
        ];

        assert_eq!(data, encode_state_message(&msg).unwrap());
    }

    #[test]
    fn empty_state_message_is_encoded_correctly() {
        let msg = State {
            transaction_id: 42,
            key_value: ("1/2/3", "").into(),
        };

        let data = vec![
            STA, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00000101, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, b'1', b'/', b'2', b'/', b'3',
        ];

        assert_eq!(data, encode_state_message(&msg).unwrap());
    }

    #[test]
    fn err_message_is_encoded_correctly() {
        let msg = Err {
            transaction_id: 42,
            error_code: 5,
            metadata: "THIS IS METAAA!!!".to_owned(),
        };

        let data = vec![
            ERR, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000101, 0b00000000, 0b00000000, 0b00000000, 0b00010001,
            b'T', b'H', b'I', b'S', b' ', b'I', b'S', b' ', b'M', b'E', b'T', b'A', b'A', b'A',
            b'!', b'!', b'!',
        ];

        assert_eq!(data, encode_err_message(&msg).unwrap());
    }
}
