use crate::{
    ErrorCode, KeyLength, MessageType, MetaDataLength, NumGraveGoods, NumKeyValuePairs, PathLength,
    RequestPatternLength, ValueLength,
};
use std::{fmt, io, string::FromUtf8Error};

#[derive(Debug)]
pub enum DecodeError {
    UndefinedType(MessageType),
    IoError(io::Error),
    FromUtf8Error(FromUtf8Error),
    UndefinedErrorCode(ErrorCode),
    SerDeError(serde_json::Error),
}

impl std::error::Error for DecodeError {}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::UndefinedType(mtype) => write!(f, "undefined message type: {mtype})",),
            DecodeError::IoError(e) => e.fmt(f),
            DecodeError::FromUtf8Error(e) => e.fmt(f),
            DecodeError::SerDeError(e) => e.fmt(f),
            DecodeError::UndefinedErrorCode(error_code) => {
                write!(f, "undefined error code: {error_code})",)
            }
        }
    }
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> Self {
        DecodeError::IoError(e)
    }
}

impl From<FromUtf8Error> for DecodeError {
    fn from(e: FromUtf8Error) -> Self {
        DecodeError::FromUtf8Error(e)
    }
}

impl From<serde_json::Error> for DecodeError {
    fn from(e: serde_json::Error) -> Self {
        DecodeError::SerDeError(e)
    }
}

pub type DecodeResult<T> = std::result::Result<T, DecodeError>;

#[derive(Debug)]
pub enum EncodeError {
    RequestPatternTooLong(usize),
    KeyTooLong(usize),
    ValueTooLong(usize),
    MetaDataTooLong(usize),
    PathTooLong(usize),
    TooManyKeyValuePairs(usize),
    TooManyProtocolVersions(usize),
    TooManyLastWills(usize),
    TooManyGraveGoods(usize),
    IoError(io::Error),
    MessageTooLarge(u64),
}

impl std::error::Error for EncodeError {}

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
            EncodeError::PathTooLong(len) => write!(
                f,
                "path is too long : {} bytes (max {} bytes allowed)",
                len,
                PathLength::MAX
            ),
            EncodeError::TooManyKeyValuePairs(len) => write!(
                f,
                "too many key/value pairs: {} (max {} allowed)",
                len,
                NumKeyValuePairs::MAX
            ),
            EncodeError::TooManyProtocolVersions(len) => write!(
                f,
                "too many supported protocol versions pairs: {} (max {} allowed)",
                len,
                NumKeyValuePairs::MAX
            ),
            EncodeError::TooManyLastWills(len) => write!(
                f,
                "too many last will key/value pairs: {} (max {} allowed)",
                len,
                NumGraveGoods::MAX
            ),
            EncodeError::TooManyGraveGoods(len) => write!(
                f,
                "too many grave goods: {} (max {} allowed)",
                len,
                NumGraveGoods::MAX
            ),
            EncodeError::MessageTooLarge(len) => write!(
                f,
                "message is too large: {} (max {} allowed)",
                len,
                MetaDataLength::MAX
            ),
            EncodeError::IoError(ioe) => ioe.fmt(f),
        }
    }
}

impl From<io::Error> for EncodeError {
    fn from(e: io::Error) -> Self {
        EncodeError::IoError(e)
    }
}

pub type EncodeResult<T> = std::result::Result<T, EncodeError>;
