use std::{fmt, io, string::FromUtf8Error};

use crate::codec::{
    KeyLength, MessageType, MetaDataLength, NumKeyValuePairs, RequestPattern, RequestPatternLength,
    ValueLength,
};

#[derive(Debug)]
pub enum DecodeError {
    UndefinedType(MessageType),
    IoError(io::Error),
    FromUtf8Error(FromUtf8Error),
}

impl std::error::Error for DecodeError {}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::UndefinedType(mtype) => write!(f, "undefined message type: {mtype})",),
            DecodeError::IoError(e) => e.fmt(f),
            DecodeError::FromUtf8Error(e) => e.fmt(f),
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

pub type DecodeResult<T> = std::result::Result<T, DecodeError>;

#[derive(Debug)]
pub enum EncodeError {
    RequestPatternTooLong(usize),
    KeyTooLong(usize),
    ValueTooLong(usize),
    MetaDataTooLong(usize),
    TooManyKeyValuePairs(usize),
    IoError(io::Error),
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
            EncodeError::TooManyKeyValuePairs(len) => write!(
                f,
                "too many key/value pairs: {} (max {} allowed)",
                len,
                NumKeyValuePairs::MAX
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

#[derive(Debug)]
pub enum WorterbuchError {
    IllegalWildcard(RequestPattern),
    IllegalMultiWildcard(RequestPattern),
    MultiWildcardAtIllegalPosition(RequestPattern),
    Other(anyhow::Error),
}

impl std::error::Error for WorterbuchError {}

impl fmt::Display for WorterbuchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorterbuchError::IllegalWildcard(rp) => {
                write!(f, "Key contains illegal wildcard: {rp}")
            }
            WorterbuchError::IllegalMultiWildcard(rp) => {
                write!(f, "Key contains illegal multi-wildcard: {rp}")
            }
            WorterbuchError::MultiWildcardAtIllegalPosition(rp) => {
                write!(f, "Key contains multi-wildcard at illegal position: {rp}")
            }
            WorterbuchError::Other(e) => e.fmt(f),
        }
    }
}

impl From<anyhow::Error> for WorterbuchError {
    fn from(e: anyhow::Error) -> Self {
        WorterbuchError::Other(e)
    }
}

pub type WorterbuchResult<T> = std::result::Result<T, WorterbuchError>;
