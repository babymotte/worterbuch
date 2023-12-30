use crate::{server::Err, ErrorCode, Key, MetaData, RequestPattern};
use std::{fmt, io, net::AddrParseError, num::ParseIntError};
use tokio::sync::{
    broadcast,
    mpsc::{self, error::SendError},
    oneshot,
};

#[derive(Debug)]
pub enum ConfigError {
    InvalidSeparator(String),
    InvalidWildcard(String),
    InvalidMultiWildcard(String),
    InvalidPort(ParseIntError),
    InvalidAddr(AddrParseError),
    InvalidInterval(ParseIntError),
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
            ConfigError::InvalidInterval(e) => write!(f, "invalid interval: {e}"),
        }
    }
}

pub trait ConfigIntContext<I> {
    fn as_port(self) -> Result<I, ConfigError>;
    fn as_interval(self) -> Result<I, ConfigError>;
}

impl<I> ConfigIntContext<I> for Result<I, ParseIntError> {
    fn as_port(self) -> Result<I, ConfigError> {
        self.map_err(ConfigError::InvalidPort)
    }
    fn as_interval(self) -> Result<I, ConfigError> {
        self.map_err(ConfigError::InvalidInterval)
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
    NotSubscribed,
    IoError(io::Error, MetaData),
    SerDeError(serde_json::Error, MetaData),
    InvalidServerResponse(MetaData),
    Other(Box<dyn std::error::Error + Send + Sync>, MetaData),
    ServerResponse(Err),
    ProtocolNegotiationFailed,
    ReadOnlyKey(Key),
    AuthenticationFailed,
    AuthenticationRequired(&'static str),
    AlreadyAuthenticated,
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
            WorterbuchError::NotSubscribed => write!(f, "no such subscription"),
            WorterbuchError::IoError(e, meta) => write!(f, "{meta}: {e}"),
            WorterbuchError::SerDeError(e, meta) => write!(f, "{meta}: {e}"),
            WorterbuchError::Other(e, meta) => write!(f, "{meta}: {e}"),
            WorterbuchError::ServerResponse(e) => {
                write!(f, "error {}: {}", e.error_code, e.metadata)
            }
            WorterbuchError::ProtocolNegotiationFailed => {
                write!(f, "The server does not implement any of the protocol versions supported by this client")
            }
            WorterbuchError::InvalidServerResponse(meta) => write!(
                f,
                "The server sent a response that is not valid for the issued request: {meta}"
            ),
            WorterbuchError::ReadOnlyKey(key) => {
                write!(f, "Tried to delete a read only key: {key}")
            }
            WorterbuchError::AuthenticationFailed => {
                write!(f, "Client failed to authenticate")
            }
            WorterbuchError::AuthenticationRequired(op) => {
                write!(f, "Operation {op} requires authentication")
            }
            WorterbuchError::AlreadyAuthenticated => {
                write!(f, "Handshake already done")
            }
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

impl<T: Send + Sync + 'static> From<mpsc::error::SendError<T>> for WorterbuchError {
    fn from(value: mpsc::error::SendError<T>) -> Self {
        WorterbuchError::Other(Box::new(value), "Internal server error".to_owned())
    }
}

impl From<oneshot::error::RecvError> for WorterbuchError {
    fn from(value: oneshot::error::RecvError) -> Self {
        WorterbuchError::Other(Box::new(value), "Internal server error".to_owned())
    }
}

pub type WorterbuchResult<T> = std::result::Result<T, WorterbuchError>;

#[derive(Debug)]
pub enum ConnectionError {
    IoError(io::Error),
    SendError(Box<dyn std::error::Error + Send + Sync>),
    WebsocketError(tungstenite::Error),
    TrySendError(Box<dyn std::error::Error + Send + Sync>),
    RecvError(oneshot::error::RecvError),
    BcRecvError(broadcast::error::RecvError),
    WorterbuchError(WorterbuchError),
    ConfigError(ConfigError),
    SerdeError(serde_json::Error),
    AckError(broadcast::error::SendError<u64>),
    Timeout,
    HttpError(tungstenite::http::Error),
    AuthenticationError(String),
}

impl std::error::Error for ConnectionError {}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => fmt::Display::fmt(&e, f),
            Self::SendError(e) => fmt::Display::fmt(&e, f),
            Self::WebsocketError(e) => fmt::Display::fmt(&e, f),
            Self::TrySendError(e) => fmt::Display::fmt(&e, f),
            Self::RecvError(e) => fmt::Display::fmt(&e, f),
            Self::BcRecvError(e) => fmt::Display::fmt(&e, f),
            Self::WorterbuchError(e) => fmt::Display::fmt(&e, f),
            Self::ConfigError(e) => fmt::Display::fmt(&e, f),
            Self::SerdeError(e) => fmt::Display::fmt(&e, f),
            Self::AckError(e) => fmt::Display::fmt(&e, f),
            Self::Timeout => fmt::Display::fmt("timeout", f),
            Self::HttpError(e) => fmt::Display::fmt(&e, f),
            Self::AuthenticationError(msg) => fmt::Display::fmt(&msg, f),
        }
    }
}

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

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

impl From<tungstenite::Error> for ConnectionError {
    fn from(e: tungstenite::Error) -> Self {
        ConnectionError::WebsocketError(e)
    }
}

impl From<oneshot::error::RecvError> for ConnectionError {
    fn from(e: oneshot::error::RecvError) -> Self {
        ConnectionError::RecvError(e)
    }
}

impl From<broadcast::error::RecvError> for ConnectionError {
    fn from(e: broadcast::error::RecvError) -> Self {
        ConnectionError::BcRecvError(e)
    }
}

impl From<ConfigError> for ConnectionError {
    fn from(e: ConfigError) -> Self {
        ConnectionError::ConfigError(e)
    }
}

impl From<serde_json::Error> for ConnectionError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeError(e)
    }
}

impl From<broadcast::error::SendError<u64>> for ConnectionError {
    fn from(e: broadcast::error::SendError<u64>) -> Self {
        Self::AckError(e)
    }
}

impl From<tungstenite::http::Error> for ConnectionError {
    fn from(e: tungstenite::http::Error) -> Self {
        Self::HttpError(e)
    }
}

impl From<&WorterbuchError> for ErrorCode {
    fn from(e: &WorterbuchError) -> Self {
        match e {
            WorterbuchError::IllegalWildcard(_) => ErrorCode::IllegalWildcard,
            WorterbuchError::IllegalMultiWildcard(_) => ErrorCode::IllegalMultiWildcard,
            WorterbuchError::MultiWildcardAtIllegalPosition(_) => {
                ErrorCode::MultiWildcardAtIllegalPosition
            }
            WorterbuchError::NoSuchValue(_) => ErrorCode::NoSuchValue,
            WorterbuchError::NotSubscribed => ErrorCode::NotSubscribed,
            WorterbuchError::IoError(_, _) => ErrorCode::IoError,
            WorterbuchError::SerDeError(_, _) => ErrorCode::SerdeError,
            WorterbuchError::ProtocolNegotiationFailed => ErrorCode::ProtocolNegotiationFailed,
            WorterbuchError::InvalidServerResponse(_) => ErrorCode::InvalidServerResponse,
            WorterbuchError::ReadOnlyKey(_) => ErrorCode::ReadOnlyKey,
            WorterbuchError::AuthenticationFailed => ErrorCode::AuthenticationFailed,
            WorterbuchError::AuthenticationRequired(_) => ErrorCode::AuthenticationRequired,
            WorterbuchError::AlreadyAuthenticated => ErrorCode::AlreadyAuthenticated,
            WorterbuchError::Other(_, _) | WorterbuchError::ServerResponse(_) => ErrorCode::Other,
        }
    }
}
