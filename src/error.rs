use std::fmt;

use crate::codec::{
    KeyLength, MessageType, MetaDataLength, NumKeyValuePairs, RequestPatternLength, ValueLength,
};

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
    KeyTooLong(usize),
    ValueTooLong(usize),
    MetaDataTooLong(usize),
    TooManyKeyValuePairs(usize),
    Other(String),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::RequestPatternTooLong(len) => write!(
                f,
                "request pattern is too long : {} bytes (max {} bytes allowed)",
                len,
                RequestPatternLength::MAX
            ),
            EncodeError::KeyTooLong(len) => write!(
                f,
                "key is too long : {} bytes (max {} bytes allowed)",
                len,
                KeyLength::MAX
            ),
            EncodeError::ValueTooLong(len) => write!(
                f,
                "value is too long : {} bytes (max {} bytes allowed)",
                len,
                ValueLength::MAX
            ),
            EncodeError::MetaDataTooLong(len) => write!(
                f,
                "meta data is too long : {} bytes (max {} bytes allowed)",
                len,
                MetaDataLength::MAX
            ),
            EncodeError::TooManyKeyValuePairs(len) => write!(
                f,
                "too many key/value pairs: {} (max {} allowed)",
                len,
                NumKeyValuePairs::MAX
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
