mod decode;
mod encode;
pub mod error;
pub mod nonblocking;
mod types;

pub use decode::*;
pub use encode::*;
use error::{EncodeError, EncodeResult};
pub use types::*;

pub(crate) fn prepend_buffer_length(buf: Vec<u8>) -> EncodeResult<Vec<u8>> {
    let len = buf.len();

    if len > MessageLength::MAX as usize {
        return Err(EncodeError::MessageTooLarge(len as u64));
    }

    let mut len_buf = (len as MessageLength).to_be_bytes().to_vec();
    len_buf.extend(buf);

    Ok(len_buf)
}
