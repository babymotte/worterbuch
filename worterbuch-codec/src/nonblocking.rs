use super::{ClientMessage as CM, ServerMessage as SM};
use crate::{error::DecodeResult, MessageLength, MESSAGE_LENGTH_BYTES};
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn read_client_message(data: impl AsyncRead + Unpin) -> DecodeResult<Option<CM>> {
    let buf = match read_message_bytes(data).await {
        Some(it) => it,
        None => return Ok(None),
    };

    crate::read_client_message(&buf[..]).map(Some)
}

pub async fn read_server_message(data: impl AsyncRead + Unpin) -> DecodeResult<Option<SM>> {
    let buf = match read_message_bytes(data).await {
        Some(it) => it,
        None => return Ok(None),
    };

    crate::read_server_message(&buf[..]).map(Some)
}

async fn read_message_bytes(mut data: impl AsyncRead + Unpin) -> Option<Vec<u8>> {
    let mut len_buf = [0; MESSAGE_LENGTH_BYTES];
    if let Err(e) = data.read_exact(&mut len_buf).await {
        log::debug!("client disconnected: {e}");
        return None;
    }
    let length = MessageLength::from_be_bytes(len_buf);

    let mut buf = vec![0; length as usize];
    if let Err(e) = data.read_exact(&mut buf).await {
        log::debug!("client disconnected: {e}");
        return None;
    }

    let mut msg_data = len_buf.to_vec();
    msg_data.extend(buf);

    Some(msg_data)
}
