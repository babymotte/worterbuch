use std::fmt;

use crate::codec::MessageType;

#[derive(Debug)]
pub enum DecodeError {
    UndefinedType(MessageType),
    Other(String),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::UndefinedType(mtype) => write!(f, "undefined message type: {mtype})",),
            DecodeError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl<E: std::error::Error> From<E> for DecodeError {
    fn from(e: E) -> Self {
        DecodeError::Other(format!("{e}"))
    }
}

pub type DecodeResult<T> = std::result::Result<T, DecodeError>;

#[derive(Debug)]
pub enum EncodeError {
    RequestPatternTooLong(usize),
    Other(String),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::RequestPatternTooLong(len) => write!(
                f,
                "request pattern is too long : {} bytes (max {} bytes allowed)",
                len,
                u16::MAX
            ),
            EncodeError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl<E: std::error::Error> From<E> for EncodeError {
    fn from(e: E) -> Self {
        EncodeError::Other(format!("{e}"))
    }
}

pub type EncodeResult<T> = std::result::Result<T, EncodeError>;
