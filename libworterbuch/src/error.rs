use crate::codec::{
    KeyLength, MessageType, MetaDataLength, NumKeyValuePairs, PathLength, RequestPatternLength,
    ValueLength,
};
use std::{fmt, io, net::AddrParseError, num::ParseIntError, string::FromUtf8Error};

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
    PathTooLong(usize),
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
pub enum ConfigError {
    InvalidSeparator(String),
    InvalidWildcard(String),
    InvalidMultiWildcard(String),
    InvalidPort(ParseIntError),
    InvalidAddr(AddrParseError),
}

impl std::error::Error for ConfigError {}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::InvalidSeparator(str) => write!(
                f,
                "invalid separator: {str}; separator must be a single ASCII char"
            ),
            ConfigError::InvalidWildcard(str) => write!(
                f,
                "invalid wildcard: {str}; wildcard must be a single ASCII char"
            ),
            ConfigError::InvalidMultiWildcard(str) => write!(
                f,
                "invalid multi-wildcard: {str}; multi-wildcard must be a single ASCII char"
            ),
            ConfigError::InvalidPort(e) => write!(f, "invalid port: {e}"),
            ConfigError::InvalidAddr(e) => write!(f, "invalid address: {e}"),
        }
    }
}

pub type ConfigResult<T> = std::result::Result<T, ConfigError>;
