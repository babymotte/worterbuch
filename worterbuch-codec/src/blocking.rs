use super::{
    ClientMessage as CM, Export, Get, Handshake, Import, Key, KeyLength, MetaData, MetaDataLength,
    NumKeyValuePairs, NumProtocolVersions, PGet, PSubscribe, Path, PathLength, ProtocolVersion,
    ProtocolVersionSegment, RequestPattern, RequestPatternLength, Set, Subscribe, TransactionId,
    Unsubscribe, Value, ValueLength, ERROR_CODE_BYTES, EXP, GET, HSHK, IMP, KEY_LENGTH_BYTES,
    METADATA_LENGTH_BYTES, MULTI_WILDCARD_BYTES, NUM_KEY_VALUE_PAIRS_BYTES,
    NUM_PROTOCOL_VERSION_BYTES, PATH_LENGTH_BYTES, PGET, PROTOCOL_VERSION_SEGMENT_BYTES, PSUB,
    REQUEST_PATTERN_LENGTH_BYTES, SEPARATOR_BYTES, SET, SUB, TRANSACTION_ID_BYTES,
    UNIQUE_FLAG_BYTES, USUB, VALUE_LENGTH_BYTES, WILDCARD_BYTES,
};
use crate::{
    error::{DecodeError, DecodeResult},
    {Ack, Err, PState, ServerMessage as SM, State, ACK, ERR, PSTA, STA},
};
use std::io::Read;

pub fn read_server_message(mut data: impl Read) -> DecodeResult<SM> {
    let mut buf = [0];
    data.read_exact(&mut buf)?;
    match buf[0] {
        PSTA => read_pstate_message(data).map(SM::PState),
        ACK => read_ack_message(data).map(SM::Ack),
        STA => read_state_message(data).map(SM::State),
        ERR => read_err_message(data).map(SM::Err),
        HSHK => read_handshake_message(data).map(SM::Handshake),
        _ => Err(DecodeError::UndefinedType(buf[0])),
    }
}

fn read_pstate_message(mut data: impl Read) -> DecodeResult<PState> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; REQUEST_PATTERN_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let request_pattern_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = [0; NUM_KEY_VALUE_PAIRS_BYTES];
    data.read_exact(&mut buf)?;
    let num_key_val_pairs = NumKeyValuePairs::from_be_bytes(buf);

    let mut key_value_lengths = Vec::new();

    for _ in 0..num_key_val_pairs {
        let mut buf = [0; KEY_LENGTH_BYTES];
        data.read_exact(&mut buf)?;
        let key_length = KeyLength::from_be_bytes(buf);

        let mut buf = [0; VALUE_LENGTH_BYTES];
        data.read_exact(&mut buf)?;
        let value_length = ValueLength::from_be_bytes(buf);

        key_value_lengths.push((key_length, value_length));
    }

    let mut buf = vec![0; request_pattern_length as usize];
    data.read_exact(&mut buf)?;
    let request_pattern = RequestPattern::from_utf8_lossy(&buf).to_string();

    let mut key_value_pairs = Vec::new();

    for (key_length, value_length) in key_value_lengths {
        let mut buf = vec![0; key_length as usize];
        data.read_exact(&mut buf)?;
        let key = Key::from_utf8(buf)?;

        let mut buf = vec![0; value_length as usize];
        data.read_exact(&mut buf)?;
        let value = Value::from_utf8_lossy(&buf).to_string();

        key_value_pairs.push((key, value).into());
    }

    Ok(PState {
        transaction_id,
        request_pattern,
        key_value_pairs,
    })
}

fn read_handshake_message(mut data: impl Read) -> DecodeResult<Handshake> {
    let mut buf = [0; NUM_PROTOCOL_VERSION_BYTES];
    data.read_exact(&mut buf)?;
    let num_protocol_versions = NumProtocolVersions::from_be_bytes(buf);

    let mut supported_protocol_versions = Vec::new();

    for _ in 0..num_protocol_versions {
        let mut buf = [0; PROTOCOL_VERSION_SEGMENT_BYTES];
        data.read_exact(&mut buf)?;
        let major = ProtocolVersionSegment::from_be_bytes(buf);

        let mut buf = [0; PROTOCOL_VERSION_SEGMENT_BYTES];
        data.read_exact(&mut buf)?;
        let minor = ProtocolVersionSegment::from_be_bytes(buf);

        supported_protocol_versions.push(ProtocolVersion { major, minor });
    }

    let mut buf = vec![0; SEPARATOR_BYTES];
    data.read_exact(&mut buf)?;
    let separator = buf[0] as char;

    let mut buf = vec![0; WILDCARD_BYTES];
    data.read_exact(&mut buf)?;
    let wildcard = buf[0] as char;

    let mut buf = vec![0; MULTI_WILDCARD_BYTES];
    data.read_exact(&mut buf)?;
    let multi_wildcard = buf[0] as char;

    Ok(Handshake {
        supported_protocol_versions,
        separator,
        wildcard,
        multi_wildcard,
    })
}

fn read_ack_message(mut data: impl Read) -> DecodeResult<Ack> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    Ok(Ack { transaction_id })
}

fn read_state_message(mut data: impl Read) -> DecodeResult<State> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let key_length = KeyLength::from_be_bytes(buf);

    let mut buf = [0; VALUE_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let value_length = ValueLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf)?;
    let key = Key::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0; value_length as usize];
    data.read_exact(&mut buf)?;
    let value = Value::from_utf8_lossy(&buf).to_string();

    Ok(State {
        transaction_id,
        key_value: (key, value).into(),
    })
}

fn read_err_message(mut data: impl Read) -> DecodeResult<Err> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; ERROR_CODE_BYTES];
    data.read_exact(&mut buf)?;
    let error_code = buf[0];

    let mut buf = [0; METADATA_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let metadata_length = MetaDataLength::from_be_bytes(buf);

    let mut buf = vec![0; metadata_length as usize];
    data.read_exact(&mut buf)?;
    let metadata = MetaData::from_utf8_lossy(&buf).to_string();

    Ok(Err {
        transaction_id,
        error_code,
        metadata,
    })
}

pub fn read_client_message(mut data: impl Read) -> DecodeResult<CM> {
    let mut buf = [0];
    data.read_exact(&mut buf)?;
    match buf[0] {
        GET => read_get_message(data).map(CM::Get),
        PGET => read_pget_message(data).map(CM::PGet),
        SET => read_set_message(data).map(CM::Set),
        SUB => read_subscribe_message(data).map(CM::Subscribe),
        PSUB => read_psubscribe_message(data).map(CM::PSubscribe),
        EXP => read_export_message(data).map(CM::Export),
        IMP => read_import_message(data).map(CM::Import),
        USUB => read_unsubscribe_message(data).map(CM::Unsubscribe),
        _ => Err(DecodeError::UndefinedType(buf[0])),
    }
}

fn read_get_message(mut data: impl Read) -> DecodeResult<Get> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let key_length = KeyLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf)?;
    let key = Key::from_utf8_lossy(&buf).to_string();

    Ok(Get {
        transaction_id,
        key,
    })
}

fn read_pget_message(mut data: impl Read) -> DecodeResult<PGet> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; REQUEST_PATTERN_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let request_pattern_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = vec![0; request_pattern_length as usize];
    data.read_exact(&mut buf)?;
    let request_pattern = RequestPattern::from_utf8_lossy(&buf).to_string();

    Ok(PGet {
        transaction_id,
        request_pattern,
    })
}

fn read_set_message(mut data: impl Read) -> DecodeResult<Set> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let key_length = KeyLength::from_be_bytes(buf);

    let mut buf = [0; VALUE_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let value_length = ValueLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf)?;
    let key = Key::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0; value_length as usize];
    data.read_exact(&mut buf)?;
    let value = Value::from_utf8_lossy(&buf).to_string();

    Ok(Set {
        transaction_id,
        key,
        value,
    })
}

fn read_subscribe_message(mut data: impl Read) -> DecodeResult<Subscribe> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let key_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf)?;
    let key = RequestPattern::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0; UNIQUE_FLAG_BYTES];
    data.read_exact(&mut buf)?;
    let unique = buf[0] != 0;

    Ok(Subscribe {
        transaction_id,
        key,
        unique,
    })
}

fn read_psubscribe_message(mut data: impl Read) -> DecodeResult<PSubscribe> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; REQUEST_PATTERN_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let request_pattern_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = vec![0; request_pattern_length as usize];
    data.read_exact(&mut buf)?;
    let request_pattern = RequestPattern::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0; UNIQUE_FLAG_BYTES];
    data.read_exact(&mut buf)?;
    let unique = buf[0] != 0;

    Ok(PSubscribe {
        transaction_id,
        request_pattern,
        unique,
    })
}

fn read_import_message(mut data: impl Read) -> DecodeResult<Import> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; PATH_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let path_length = PathLength::from_be_bytes(buf);

    let mut buf = vec![0; path_length as usize];
    data.read_exact(&mut buf)?;
    let path = Path::from_utf8_lossy(&buf).to_string();

    Ok(Import {
        transaction_id,
        path,
    })
}

fn read_unsubscribe_message(mut data: impl Read) -> DecodeResult<Unsubscribe> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    Ok(Unsubscribe { transaction_id })
}

fn read_export_message(mut data: impl Read) -> DecodeResult<Export> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf)?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; PATH_LENGTH_BYTES];
    data.read_exact(&mut buf)?;
    let path_length = PathLength::from_be_bytes(buf);

    let mut buf = vec![0; path_length as usize];
    data.read_exact(&mut buf)?;
    let path = Path::from_utf8_lossy(&buf).to_string();

    Ok(Export {
        transaction_id,
        path,
    })
}

#[cfg(test)]
mod test {
    use crate::encode_set_message;

    use super::*;

    #[test]
    fn get_message_is_read_correctly() {
        let data = [
            GET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::Get(Get {
                transaction_id: 4,
                key: "trolo".to_owned()
            })
        )
    }

    #[test]
    fn pget_message_is_read_correctly() {
        let data = [
            PGET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::PGet(PGet {
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

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::Set(Set {
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
            b't', b'u', b'r', b'e', b's', 0b00000000,
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::Subscribe(Subscribe {
                transaction_id: 5536684732567,
                // TODO this needs to be rejected!
                key: "let/me/?/you/its/features".to_owned(),
                unique: false
            })
        )
    }
    #[test]
    fn psubscribe_message_is_read_correctly() {
        let data = [
            PSUB, 0b00000000, 0b00000000, 0b00000101, 0b00001001, 0b00011100, 0b00100000,
            0b01110000, 0b10010111, 0b00000000, 0b00011001, b'l', b'e', b't', b'/', b'm', b'e',
            b'/', b'?', b'/', b'y', b'o', b'u', b'/', b'i', b't', b's', b'/', b'f', b'e', b'a',
            b't', b'u', b'r', b'e', b's', 0b00000001,
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::PSubscribe(PSubscribe {
                transaction_id: 5536684732567,
                request_pattern: "let/me/?/you/its/features".to_owned(),
                unique: true
            })
        )
    }

    #[test]
    fn export_message_is_read_correctly() {
        let data = [
            EXP, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00001101, b'/', b'p', b'a', b't', b'h', b'/',
            b't', b'o', b'/', b'f', b'i', b'l', b'e',
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::Export(Export {
                transaction_id: 42,
                path: "/path/to/file".to_owned(),
            })
        )
    }

    #[test]
    fn import_message_is_read_correctly() {
        let data = [
            IMP, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00001101, b'/', b'p', b'a', b't', b'h', b'/',
            b't', b'o', b'/', b'f', b'i', b'l', b'e',
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::Import(Import {
                transaction_id: 42,
                path: "/path/to/file".to_owned(),
            })
        )
    }

    #[test]
    fn handshake_message_is_read_correctly() {
        let data = vec![
            HSHK, 0b00000011, 0b00000000, 0b00000001, 0b00000000, 0b00000000, 0b00000000,
            0b00000001, 0b00000000, 0b00000001, 0b00000000, 0b00000001, 0b00000000, 0b00000010,
            b'/', b'?', b'#',
        ];

        let result = read_server_message(&data[..]).unwrap();

        assert_eq!(
            result,
            SM::Handshake(Handshake {
                supported_protocol_versions: vec![
                    ProtocolVersion { major: 1, minor: 0 },
                    ProtocolVersion { major: 1, minor: 1 },
                    ProtocolVersion { major: 1, minor: 2 },
                ],
                separator: '/',
                wildcard: '?',
                multi_wildcard: '#',
            })
        );
    }

    #[test]
    fn pstate_message_is_read_correctly() {
        let data = [
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

        let result = read_server_message(&data[..]).unwrap();

        assert_eq!(
            result,
            SM::PState(PState {
                transaction_id: u64::MAX,
                request_pattern: "who/let/the/?/#".to_owned(),
                key_value_pairs: vec![
                    (
                        "who/let/the/chicken/cross/the/road".to_owned(),
                        "yeah, that was me, I guess".to_owned()
                    )
                        .into(),
                    (
                        "who/let/the/dogs/out".to_owned(),
                        "Who? Who? Who? Who? Who?".to_owned()
                    )
                        .into()
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

        let result = read_server_message(&data[..]).unwrap();

        assert_eq!(result, SM::Ack(Ack { transaction_id: 42 }))
    }

    #[test]
    fn state_message_is_read_correctly() {
        let data = [
            STA, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00000101, 0b00000000, 0b00000000, 0b00000000,
            0b00000001, b'1', b'/', b'2', b'/', b'3', b'4',
        ];

        let result = read_server_message(&data[..]).unwrap();

        assert_eq!(
            result,
            SM::State(State {
                transaction_id: 42,
                key_value: ("1/2/3", "4").into()
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

        let result = read_server_message(&data[..]).unwrap();

        assert_eq!(
            result,
            SM::Err(Err {
                transaction_id: 42,
                error_code: 5,
                metadata: "THIS IS METAAA!!!".to_owned()
            })
        )
    }

    #[test]
    fn unsubscribe_message_is_read_correctly() {
        let data = [
            USUB, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010,
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(result, CM::Unsubscribe(Unsubscribe { transaction_id: 42 }))
    }

    #[test]
    fn utf_message_roundtrip_is_successful() {
        let msg = Set {
            transaction_id: 42,
            key: "🦀/🕸/😅".to_owned(),
            value: "…".to_owned(),
        };

        let data = encode_set_message(&msg).unwrap();

        let decoded = read_client_message(&*data).unwrap();

        assert_eq!(CM::Set(msg), decoded);
    }
}
