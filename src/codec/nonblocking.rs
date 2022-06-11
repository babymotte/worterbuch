
use super::{
    Ack, Err, Event, Get, Message, Set, State, Subscribe, ACK, ERR, EVE, GET, SET, STA, SUB,
};
use crate::error::{DecodeError, DecodeResult};
use tokio::io::AsyncReadExt;

pub async fn read_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Option<Message>> {
    let mut buf = [0];
    if let Err(e) = data.read_exact(&mut buf).await {
        log::debug!("client disconnected: {e}");
        return Ok(None);
    }
    match buf[0] {
        // client messages
        GET => read_get_message(data).await.map(Message::Get),
        SET => read_set_message(data).await.map(Message::Set),
        SUB => read_subscribe_message(data).await.map(Message::Subscribe),
        // server messages
        STA => read_state_message(data).await.map(Message::State),
        ACK => read_ack_message(data).await.map(Message::Ack),
        EVE => read_event_message(data).await.map(Message::Event),
        ERR => read_err_message(data).await.map(Message::Err),
        // undefined
        _ => Err(DecodeError::UndefinedType(buf[0])),
    }
    .map(Some)
}

async fn read_get_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Get> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    let mut buf = [0; 2];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = u16::from_be_bytes(buf);

    let mut buf = vec![0u8; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = String::from_utf8_lossy(&buf).to_string();

    Ok(Get {
        transaction_id,
        request_pattern,
    })
}

async fn read_set_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Set> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    let mut buf = [0; 2];
    data.read_exact(&mut buf).await?;
    let key_length = u16::from_be_bytes(buf);

    let mut buf = [0; 4];
    data.read_exact(&mut buf).await?;
    let value_length = u32::from_be_bytes(buf);

    let mut buf = vec![0u8; key_length as usize];
    data.read_exact(&mut buf).await?;
    let key = String::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0u8; value_length as usize];
    data.read_exact(&mut buf).await?;
    let value = String::from_utf8_lossy(&buf).to_string();

    Ok(Set {
        transaction_id,
        key,
        value,
    })
}

async fn read_subscribe_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Subscribe> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    let mut buf = [0; 2];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = u16::from_be_bytes(buf);

    let mut buf = vec![0u8; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = String::from_utf8_lossy(&buf).to_string();

    Ok(Subscribe {
        transaction_id,
        request_pattern,
    })
}

async fn read_state_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<State> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    let mut buf = [0; 2];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = u16::from_be_bytes(buf);

    let mut buf = [0; 4];
    data.read_exact(&mut buf).await?;
    let num_key_val_pairs = u32::from_be_bytes(buf);

    let mut key_value_lengths = Vec::new();

    for _ in 0..num_key_val_pairs {
        let mut buf = [0; 2];
        data.read_exact(&mut buf).await?;
        let key_length = u16::from_be_bytes(buf);

        let mut buf = [0; 4];
        data.read_exact(&mut buf).await?;
        let value_length = u32::from_be_bytes(buf);

        key_value_lengths.push((key_length, value_length));
    }

    let mut buf = vec![0u8; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = String::from_utf8_lossy(&buf).to_string();

    let mut key_value_pairs = Vec::new();

    for (key_length, value_length) in key_value_lengths {
        let mut buf = vec![0u8; key_length as usize];
        data.read_exact(&mut buf).await?;
        let key = String::from_utf8(buf)?;

        let mut buf = vec![0u8; value_length as usize];
        data.read_exact(&mut buf).await?;
        let value = String::from_utf8_lossy(&buf).to_string();

        key_value_pairs.push((key, value));
    }

    Ok(State {
        transaction_id,
        request_pattern,
        key_value_pairs,
    })
}

async fn read_ack_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Ack> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    Ok(Ack { transaction_id })
}

async fn read_event_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Event> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    let mut buf = [0; 2];
    data.read_exact(&mut buf).await?;
    let request_pattern_length = u16::from_be_bytes(buf);

    let mut buf = [0; 2];
    data.read_exact(&mut buf).await?;
    let key_length = u16::from_be_bytes(buf);

    let mut buf = [0; 4];
    data.read_exact(&mut buf).await?;
    let value_length = u32::from_be_bytes(buf);

    let mut buf = vec![0u8; request_pattern_length as usize];
    data.read_exact(&mut buf).await?;
    let request_pattern = String::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0u8; key_length as usize];
    data.read_exact(&mut buf).await?;
    let key = String::from_utf8_lossy(&buf).to_string();

    let mut buf = vec![0u8; value_length as usize];
    data.read_exact(&mut buf).await?;
    let value = String::from_utf8_lossy(&buf).to_string();

    Ok(Event {
        transaction_id,
        request_pattern,
        key,
        value,
    })
}

async fn read_err_message(mut data: impl AsyncReadExt + Unpin) -> DecodeResult<Err> {
    let mut buf = [0; 8];
    data.read_exact(&mut buf).await?;
    let transaction_id = u64::from_be_bytes(buf);

    let mut buf = [0; 1];
    data.read_exact(&mut buf).await?;
    let error_code = buf[0];

    let mut buf = [0; 4];
    data.read_exact(&mut buf).await?;
    let metadata_length = u32::from_be_bytes(buf);

    let mut buf = vec![0u8; metadata_length as usize];
    data.read_exact(&mut buf).await?;
    let metadata = String::from_utf8_lossy(&buf).to_string();

    Ok(Err {
        transaction_id,
        error_code,
        metadata,
    })
}

#[cfg(test)]
mod test {
    use crate::codec::encode_set_message;

    use super::*;

    #[test]
    fn get_message_is_read_correctly() {
        let data = [
            GET, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
            0b00000000, 0b00000100, 0b00000000, 0b00000101, b't', b'r', b'o', b'l', b'o',
        ];

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

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

        let result = tokio_test::block_on(read_message(&data[..]))
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            Message::Err(Err {
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

        let decoded = tokio_test::block_on(read_message(&*data)).unwrap().unwrap();

        assert_eq!(Message::Set(msg), decoded);
    }
}
