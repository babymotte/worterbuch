use super::{
    Ack, ClientMessage as CM, Err, Export, Get, Import, Key, KeyLength, MetaData, MetaDataLength,
    NumKeyValuePairs, PGet, PState, PSubscribe, Path, PathLength, RequestPattern,
    RequestPatternLength, ServerMessage as SM, Set, State, Subscribe, TransactionId, Value,
    ValueLength, ACK, ERR, ERROR_CODE_BYTES, EXP, GET, IMP, KEY_LENGTH_BYTES,
    METADATA_LENGTH_BYTES, NUM_KEY_VALUE_PAIRS_BYTES, PATH_LENGTH_BYTES, PGET, PSTA, PSUB,
    REQUEST_PATTERN_LENGTH_BYTES, SET, STA, SUB, TRANSACTION_ID_BYTES, VALUE_LENGTH_BYTES,
};
use crate::error::{DecodeError, DecodeResult};
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn read_client_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Option<CM>> {
    let mut buf = [0];
    if let Err(e) = data.read_exact(&mut buf).await {
        log::debug!("client disconnected: {e}");
        return Ok(None);
    }
    match buf[0] {
        GET => read_get_message(data).await.map(CM::Get),
        PGET => read_pget_message(data).await.map(CM::PGet),
        SET => read_set_message(data).await.map(CM::Set),
        SUB => read_subscribe_message(data).await.map(CM::Subscribe),
        PSUB => read_psubscribe_message(data).await.map(CM::PSubscribe),
        EXP => read_export_message(data).await.map(CM::Export),
        IMP => read_import_message(data).await.map(CM::Import),
        _ => Err(DecodeError::UndefinedType(buf[0])),
    }
    .map(Some)
}

pub async fn read_server_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Option<SM>> {
    let mut buf = [0];
    if let Err(e) = data.read_exact(&mut buf).await {
        log::debug!("client disconnected: {e}");
        return Ok(None);
    }
    match buf[0] {
        PSTA => read_pstate_message(data).await.map(SM::PState),
        ACK => read_ack_message(data).await.map(SM::Ack),
        STA => read_state_message(data).await.map(SM::State),
        ERR => read_err_message(data).await.map(SM::Err),
        _ => Err(DecodeError::UndefinedType(buf[0])),
    }
    .map(Some)
}

async fn read_get_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Get> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let key_length = KeyLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf).await?;
    let key = Key::from_utf8_lossy(&buf).to_string();

    Ok(Get {
        transaction_id,
        key,
    })
}

async fn read_pget_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<PGet> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; REQUEST_PATTERN_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = vec![0; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = RequestPattern::from_utf8_lossy(&buf).to_string();

    Ok(PGet {
        transaction_id,
        request_pattern,
    })
}

async fn read_set_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Set> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let key_length = KeyLength::from_be_bytes(buf);

    let mut buf = [0; VALUE_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let value_length = ValueLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf).await?;
    let key = Key::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0; value_length as usize];
    data.read_exact(&mut buf).await?;
    let value = Value::from_utf8_lossy(&buf).to_string();

    Ok(Set {
        transaction_id,
        key,
        value,
    })
}

async fn read_subscribe_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Subscribe> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let key_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = vec![0; key_length as usize];
    data.read_exact(&mut buf).await?;
    let key = RequestPattern::from_utf8_lossy(&buf).to_string();

    Ok(Subscribe {
        transaction_id,
        key,
    })
}

async fn read_psubscribe_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<PSubscribe> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; REQUEST_PATTERN_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = vec![0; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = RequestPattern::from_utf8_lossy(&buf).to_string();

    Ok(PSubscribe {
        transaction_id,
        request_pattern,
    })
}

async fn read_pstate_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<PState> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; REQUEST_PATTERN_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = RequestPatternLength::from_be_bytes(buf);

    let mut buf = [0; NUM_KEY_VALUE_PAIRS_BYTES];
    data.read_exact(&mut buf).await?;
    let num_key_val_pairs = NumKeyValuePairs::from_be_bytes(buf);

    let mut key_value_lengths = Vec::new();

    for _ in 0..num_key_val_pairs {
        let mut buf = [0; KEY_LENGTH_BYTES];
        data.read_exact(&mut buf).await?;
        let key_length = KeyLength::from_be_bytes(buf);

        let mut buf = [0; VALUE_LENGTH_BYTES];
        data.read_exact(&mut buf).await?;
        let value_length = ValueLength::from_be_bytes(buf);

        key_value_lengths.push((key_length, value_length));
    }

    let mut buf = vec![0; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = RequestPattern::from_utf8_lossy(&buf).to_string();

    let mut key_value_pairs = Vec::new();

    for (key_length, value_length) in key_value_lengths {
        let mut buf = vec![0; key_length as usize];
        data.read_exact(&mut buf).await?;
        let key = Key::from_utf8(buf)?;

        let mut buf = vec![0; value_length as usize];
        data.read_exact(&mut buf).await?;
        let value = Value::from_utf8_lossy(&buf).to_string();

        key_value_pairs.push((key, value).into());
    }

    Ok(PState {
        transaction_id,
        request_pattern,
        key_value_pairs,
    })
}

async fn read_ack_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Ack> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    Ok(Ack { transaction_id })
}

async fn read_state_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<State> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; KEY_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let key_length = KeyLength::from_be_bytes(buf);

    let mut buf = [0; VALUE_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let value_length = ValueLength::from_be_bytes(buf);

    if key_length == 0 {
        Ok(State {
            transaction_id,
            key_value: None,
        })
    } else {
        let mut buf = vec![0; key_length as usize];
        data.read_exact(&mut buf).await?;
        let key = Key::from_utf8_lossy(&buf).to_string();

        let mut buf = vec![0; value_length as usize];
        data.read_exact(&mut buf).await?;
        let value = Value::from_utf8_lossy(&buf).to_string();

        Ok(State {
            transaction_id,
            key_value: Some((key, value).into()),
        })
    }
}

async fn read_err_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Err> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; ERROR_CODE_BYTES];
    data.read_exact(&mut buf).await?;
    let error_code = buf[0];

    let mut buf = [0; METADATA_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let metadata_length = MetaDataLength::from_be_bytes(buf);

    let mut buf = vec![0; metadata_length as usize];
    data.read_exact(&mut buf).await?;
    let metadata = MetaData::from_utf8_lossy(&buf).to_string();

    Ok(Err {
        transaction_id,
        error_code,
        metadata,
    })
}

async fn read_export_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Export> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; PATH_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let path_length = PathLength::from_be_bytes(buf);

    let mut buf = vec![0; path_length as usize];
    data.read_exact(&mut buf).await?;
    let path = Path::from_utf8_lossy(&buf).to_string();

    Ok(Export {
        transaction_id,
        path,
    })
}

async fn read_import_message(mut data: impl AsyncRead + Unpin) -> DecodeResult<Import> {
    let mut buf = [0; TRANSACTION_ID_BYTES];
    data.read_exact(&mut buf).await?;
    let transaction_id = TransactionId::from_be_bytes(buf);

    let mut buf = [0; PATH_LENGTH_BYTES];
    data.read_exact(&mut buf).await?;
    let path_length = PathLength::from_be_bytes(buf);

    let mut buf = vec![0; path_length as usize];
    data.read_exact(&mut buf).await?;
    let path = Path::from_utf8_lossy(&buf).to_string();

    Ok(Import {
        transaction_id,
        path,
    })
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::codec::encode_set_message;

    #[test]
    fn get_message_is_read_correctly() {
        let data = [
            GET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
        ];

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

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
            b't', b'u', b'r', b'e', b's',
        ];

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            CM::Subscribe(Subscribe {
                transaction_id: 5536684732567,
                key: "let/me/?/you/its/features".to_owned()
            })
        )
    }

    #[test]
    fn psubscribe_message_is_read_correctly() {
        let data = [
            PSUB, 0b00000000, 0b00000000, 0b00000101, 0b00001001, 0b00011100, 0b00100000,
            0b01110000, 0b10010111, 0b00000000, 0b00011001, b'l', b'e', b't', b'/', b'm', b'e',
            b'/', b'?', b'/', b'y', b'o', b'u', b'/', b'i', b't', b's', b'/', b'f', b'e', b'a',
            b't', b'u', b'r', b'e', b's',
        ];

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            CM::PSubscribe(PSubscribe {
                transaction_id: 5536684732567,
                request_pattern: "let/me/?/you/its/features".to_owned()
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

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_client_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            CM::Import(Import {
                transaction_id: 42,
                path: "/path/to/file".to_owned(),
            })
        )
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

        let result = tokio_test::block_on(read_server_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_server_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(result, SM::Ack(Ack { transaction_id: 42 }))
    }

    #[test]
    fn state_message_is_read_correctly() {
        let data = [
            STA, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00000101, 0b00000000, 0b00000000, 0b00000000,
            0b00000001, b'1', b'/', b'2', b'/', b'3', b'4',
        ];

        let result = tokio_test::block_on(read_server_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            SM::State(State {
                transaction_id: 42,
                key_value: Some(("1/2/3".to_owned(), "4".to_owned()).into())
            })
        )
    }

    #[test]
    fn empty_state_message_is_read_correctly() {
        let data = [
            STA, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00101010, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000,
        ];

        let result = tokio_test::block_on(read_server_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            SM::State(State {
                transaction_id: 42,
                key_value: None
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

        let result = tokio_test::block_on(read_server_message(&data[..]))
            .unwrap()
            .unwrap();

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
    fn utf_message_roundtrip_is_successful() {
        let msg = Set {
            transaction_id: 42,
            key: "🦀/🕸/😅".to_owned(),
            value: "…".to_owned(),
        };

        let data = encode_set_message(&msg).unwrap();

        let decoded = tokio_test::block_on(read_client_message(&*data))
            .unwrap()
            .unwrap();

        assert_eq!(CM::Set(msg), decoded);
    }

    #[test]
    fn message_sequence_read_successful() {
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
            b' ', b'W', b'h', b'o', b'?', STA, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000000, 0b00000000, 0b00101010, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000000, 0b00000000, ACK, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00101010, PSTA, 0b11111111,
            0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
            0b00000000, 0b00001111, 0b00000000, 0b00000000, 0b00000000, 0b00000010, 0b00000000,
            0b00100010, 0b00000000, 0b00000000, 0b00000000, 0b00011010, 0b00000000, 0b00010100,
            0b00000000, 0b00000000, 0b00000000, 0b00011000, b'w', b'h', b'o', b'/', b'l', b'e',
            b't', b'/', b't', b'h', b'e', b'/', b'?', b'/', b'#', b'w', b'h', b'o', b'/', b'l',
            b'e', b't', b'/', b't', b'h', b'e', b'/', b'c', b'h', b'i', b'c', b'k', b'e', b'n',
            b'/', b'c', b'r', b'o', b's', b's', b'/', b't', b'h', b'e', b'/', b'r', b'o', b'a',
            b'd', b'y', b'e', b'a', b'h', b',', b' ', b't', b'h', b'a', b't', b' ', b'w', b'a',
            b's', b' ', b'm', b'e', b',', b' ', b'I', b' ', b'g', b'u', b'e', b's', b's', b'w',
            b'h', b'o', b'/', b'l', b'e', b't', b'/', b't', b'h', b'e', b'/', b'd', b'o', b'g',
            b's', b'/', b'o', b'u', b't', b'W', b'h', b'o', b'?', b' ', b'W', b'h', b'o', b'?',
            b' ', b'W', b'h', b'o', b'?', b' ', b'W', b'h', b'o', b'?', b' ', b'W', b'h', b'o',
            b'?',
        ];

        let mut messages = Vec::new();

        tokio_test::block_on(async {
            let mut data = AsyncReadExt::take(&data[..], data.len() as u64);
            messages.push(read_server_message(&mut data).await.unwrap());
            messages.push(read_server_message(&mut data).await.unwrap());
            messages.push(read_server_message(&mut data).await.unwrap());
            messages.push(read_server_message(&mut data).await.unwrap());
            messages.push(read_server_message(&mut data).await.unwrap());
        });

        assert_eq!(
            messages[0],
            Some(SM::PState(PState {
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
            }))
        );

        assert_eq!(
            messages[1],
            Some(SM::State(State {
                transaction_id: 42,
                key_value: None
            }))
        );

        assert_eq!(messages[2], Some(SM::Ack(Ack { transaction_id: 42 })));

        assert_eq!(
            messages[3],
            Some(SM::PState(PState {
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
            }))
        );

        assert_eq!(messages[4], None);
    }
}
