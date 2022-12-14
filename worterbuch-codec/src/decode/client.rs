use crate::{
    error::{DecodeError, DecodeResult},
    ClientMessage as CM, Export, Get, GraveGoods, HandshakeRequest, Import, Key, KeyLength,
    LastWill, NumGraveGoods, NumLastWill, NumProtocolVersions, PGet, PSubscribe, Path, PathLength,
    ProtocolVersion, ProtocolVersionSegment, ProtocolVersions, RequestPattern,
    RequestPatternLength, Set, Subscribe, TransactionId, Unsubscribe, Value, ValueLength, EXP, GET,
    HSHKR, IMP, KEY_LENGTH_BYTES, NUM_GRAVE_GOODS_BYTES, NUM_LAST_WILL_BYTES,
    NUM_PROTOCOL_VERSION_BYTES, PATH_LENGTH_BYTES, PGET, PROTOCOL_VERSION_SEGMENT_BYTES, PSUB,
    REQUEST_PATTERN_LENGTH_BYTES, SET, SUB, TRANSACTION_ID_BYTES, UNIQUE_FLAG_BYTES, USUB,
    VALUE_LENGTH_BYTES,
};
use std::io::Read;

pub fn read_client_message(mut data: impl Read) -> DecodeResult<CM> {
    let mut buf = [0];
    data.read_exact(&mut buf)?;
    match buf[0] {
        HSHKR => read_handshake_request_message(data).map(CM::HandshakeRequest),
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

fn read_handshake_request_message(mut data: impl Read) -> DecodeResult<HandshakeRequest> {
    let mut buf = [0; NUM_PROTOCOL_VERSION_BYTES];
    data.read_exact(&mut buf)?;
    let num_protocol_versions = NumProtocolVersions::from_be_bytes(buf);

    let mut buf = [0; NUM_LAST_WILL_BYTES];
    data.read_exact(&mut buf)?;
    let num_last_will = NumLastWill::from_be_bytes(buf);

    let mut buf = [0; NUM_GRAVE_GOODS_BYTES];
    data.read_exact(&mut buf)?;
    let num_grave_good = NumGraveGoods::from_be_bytes(buf);

    let mut supported_protocol_versions = ProtocolVersions::new();

    for _ in 0..num_protocol_versions {
        let mut buf = [0; PROTOCOL_VERSION_SEGMENT_BYTES];
        data.read_exact(&mut buf)?;
        let major = ProtocolVersionSegment::from_be_bytes(buf);

        let mut buf = [0; PROTOCOL_VERSION_SEGMENT_BYTES];
        data.read_exact(&mut buf)?;
        let minor = ProtocolVersionSegment::from_be_bytes(buf);

        supported_protocol_versions.push(ProtocolVersion { major, minor });
    }

    let mut last_will_key_value_lengths = Vec::new();

    for _ in 0..num_last_will {
        let mut buf = [0; KEY_LENGTH_BYTES];
        data.read_exact(&mut buf)?;
        let key_length = KeyLength::from_be_bytes(buf);

        let mut buf = [0; VALUE_LENGTH_BYTES];
        data.read_exact(&mut buf)?;
        let value_length = ValueLength::from_be_bytes(buf);

        last_will_key_value_lengths.push((key_length, value_length));
    }

    let mut grave_goods_key_lengths = Vec::new();

    for _ in 0..num_grave_good {
        let mut buf = [0; KEY_LENGTH_BYTES];
        data.read_exact(&mut buf)?;
        let key_length = KeyLength::from_be_bytes(buf);

        grave_goods_key_lengths.push(key_length);
    }

    let mut last_will = LastWill::new();

    for (key_length, value_length) in last_will_key_value_lengths {
        let mut buf = vec![0; key_length as usize];
        data.read_exact(&mut buf)?;
        let key = Key::from_utf8(buf)?;

        let mut buf = vec![0; value_length as usize];
        data.read_exact(&mut buf)?;
        let value = Value::from_utf8_lossy(&buf).to_string();

        last_will.push((key, value).into());
    }

    let mut grave_goods = GraveGoods::new();

    for key_length in grave_goods_key_lengths {
        let mut buf = vec![0; key_length as usize];
        data.read_exact(&mut buf)?;
        let key = Key::from_utf8(buf)?;

        grave_goods.push(key);
    }

    Ok(HandshakeRequest {
        supported_protocol_versions,
        last_will,
        grave_goods,
    })
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::encode_set_message;

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
    fn unsubscribe_message_is_read_correctly() {
        let data = [
            USUB, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010,
        ];

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(result, CM::Unsubscribe(Unsubscribe { transaction_id: 42 }))
    }

    #[test]
    fn handshake_request_message_is_read_correctly() {
        let data = [
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

        let result = read_client_message(&data[..]).unwrap();

        assert_eq!(
            result,
            CM::HandshakeRequest(HandshakeRequest {
                supported_protocol_versions: vec![
                    ProtocolVersion { major: 0, minor: 1 },
                    ProtocolVersion { major: 0, minor: 5 },
                    ProtocolVersion { major: 1, minor: 0 },
                ],
                last_will: vec![("last/will", "test").into(),],
                grave_goods: vec!["grave/goods/1".into(), "grave/goods/2".into(),]
            })
        )
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
