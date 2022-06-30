use crate::codec::{
    ErrorCode, Key, KeyLength, MessageType, MetaData, MetaDataLength, NumKeyValuePairs, PathLength,
    RequestPattern, RequestPatternLength, ValueLength,
};
#[cfg(feature = "graphql")]
use futures_channel::mpsc::TrySendError;
use std::{fmt, io, net::AddrParseError, num::ParseIntError, string::FromUtf8Error};
use tokio::sync::mpsc::error::SendError;
#[cfg(feature = "web")]
use tokio_tungstenite::tungstenite;
#[cfg(feature = "graphql")]
use url::ParseError;

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

impl From<ParseIntError> for ConfigError {
    fn from(e: ParseIntError) -> Self {
        ConfigError::InvalidPort(e)
    }
}

impl From<AddrParseError> for ConfigError {
    fn from(e: AddrParseError) -> Self {
        ConfigError::InvalidAddr(e)
    }
}

pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

pub trait Context<T, E: std::error::Error> {
    fn context(self, metadata: impl FnOnce() -> String) -> Result<T, WorterbuchError>;
}

#[derive(Debug)]
pub enum WorterbuchError {
    IllegalWildcard(RequestPattern),
    IllegalMultiWildcard(RequestPattern),
    MultiWildcardAtIllegalPosition(RequestPattern),
    NoSuchValue(Key),
    IoError(io::Error, MetaData),
    SerDeError(serde_json::Error, MetaData),
    Other(Box<dyn std::error::Error + Send + Sync>, MetaData),
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
            WorterbuchError::NoSuchValue(key) => write!(f, "no value for key '{key}'"),
            WorterbuchError::IoError(e, meta) => write!(f, "{meta}: {e}"),
            WorterbuchError::SerDeError(e, meta) => write!(f, "{meta}: {e}"),
            WorterbuchError::Other(e, meta) => write!(f, "{meta}: {e}"),
        }
    }
}

impl<T> Context<T, io::Error> for Result<T, io::Error> {
    fn context(self, metadata: impl FnOnce() -> String) -> Result<T, WorterbuchError> {
        self.map_err(|e| WorterbuchError::IoError(e, metadata()))
    }
}

impl<T> Context<T, serde_json::Error> for Result<T, serde_json::Error> {
    fn context(self, metadata: impl FnOnce() -> String) -> Result<T, WorterbuchError> {
        self.map_err(|e| WorterbuchError::SerDeError(e, metadata()))
    }
}

impl<T, V: fmt::Debug + 'static + Send + Sync> Context<T, SendError<V>>
    for Result<T, SendError<V>>
{
    fn context(self, metadata: impl FnOnce() -> String) -> Result<T, WorterbuchError> {
        self.map_err(|e| WorterbuchError::Other(Box::new(e), metadata()))
    }
}

pub type WorterbuchResult<T> = std::result::Result<T, WorterbuchError>;

#[derive(Debug)]
pub enum ConnectionError {
    IoError(io::Error),
    EncodeError(EncodeError),
    SendError(Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "web")]
    WebsocketError(tungstenite::Error),
    TrySendError(Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "graphql")]
    JsonError(serde_json::Error),
    #[cfg(feature = "graphql")]
    ParseError(ParseError),
}

impl std::error::Error for ConnectionError {}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => fmt::Display::fmt(&e, f),
            Self::EncodeError(e) => fmt::Display::fmt(&e, f),
            Self::SendError(e) => fmt::Display::fmt(&e, f),
            #[cfg(feature = "graphql")]
            Self::JsonError(e) => fmt::Display::fmt(&e, f),
            #[cfg(feature = "web")]
            Self::WebsocketError(e) => fmt::Display::fmt(&e, f),
            Self::TrySendError(e) => fmt::Display::fmt(&e, f),
            #[cfg(feature = "graphql")]
            Self::ParseError(e) => fmt::Display::fmt(&e, f),
        }
    }
}

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

impl From<EncodeError> for ConnectionError {
    fn from(e: EncodeError) -> Self {
        ConnectionError::EncodeError(e)
    }
}

impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::IoError(e)
    }
}

impl<T: fmt::Debug + 'static + Send + Sync> From<SendError<T>> for ConnectionError {
    fn from(e: SendError<T>) -> Self {
        ConnectionError::SendError(Box::new(e))
    }
}

#[cfg(feature = "web")]
impl From<tungstenite::Error> for ConnectionError {
    fn from(e: tungstenite::Error) -> Self {
        ConnectionError::WebsocketError(e)
    }
}

#[cfg(feature = "graphql")]
impl From<ParseError> for ConnectionError {
    fn from(e: ParseError) -> Self {
        ConnectionError::ParseError(e)
    }
}

#[cfg(feature = "graphql")]
impl From<serde_json::Error> for ConnectionError {
    fn from(e: serde_json::Error) -> Self {
        ConnectionError::JsonError(e)
    }
}

#[cfg(feature = "graphql")]
impl<T: 'static + Send + Sync> From<TrySendError<T>> for ConnectionError {
    fn from(e: TrySendError<T>) -> Self {
        ConnectionError::TrySendError(Box::new(e))
    }
}
