use std::{fmt, io, net::AddrParseError, num::ParseIntError};
use tokio::sync::{broadcast, mpsc::error::SendError, oneshot};
use worterbuch_codec::PROTOCOL_NEGOTIATION_FAILED;
pub use worterbuch_codec::{
    error::{DecodeError, EncodeError},
    Err, ErrorCode, Key, MetaData, RequestPattern, ILLEGAL_MULTI_WILDCARD, ILLEGAL_WILDCARD,
    IO_ERROR, MULTI_WILDCARD_AT_ILLEGAL_POSITION, NOT_SUBSCRIBED, NO_SUCH_VALUE, OTHER,
    SERDE_ERROR,
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
            ConfigError::InvalidInterval(e) => write!(f, "invalid port: {e}"),
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
    Other(Box<dyn std::error::Error + Send + Sync>, MetaData),
    ServerResponse(Err),
    ProtocolNegotiationFailed,
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
    WebsocketError(tungstenite::Error),
    TrySendError(Box<dyn std::error::Error + Send + Sync>),
    RecvError(oneshot::error::RecvError),
    BcRecvError(broadcast::error::RecvError),
    WorterbuchError(WorterbuchError),
    ConfigError(ConfigError),
    DecodeError(DecodeError),
    SerdeError(serde_json::Error),
}

impl std::error::Error for ConnectionError {}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => fmt::Display::fmt(&e, f),
            Self::EncodeError(e) => fmt::Display::fmt(&e, f),
            Self::SendError(e) => fmt::Display::fmt(&e, f),
            Self::WebsocketError(e) => fmt::Display::fmt(&e, f),
            Self::TrySendError(e) => fmt::Display::fmt(&e, f),
            Self::RecvError(e) => fmt::Display::fmt(&e, f),
            Self::BcRecvError(e) => fmt::Display::fmt(&e, f),
            Self::WorterbuchError(e) => fmt::Display::fmt(&e, f),
            Self::ConfigError(e) => fmt::Display::fmt(&e, f),
            Self::DecodeError(e) => fmt::Display::fmt(&e, f),
            Self::SerdeError(e) => fmt::Display::fmt(&e, f),
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

impl From<DecodeError> for ConnectionError {
    fn from(e: DecodeError) -> Self {
        ConnectionError::DecodeError(e)
    }
}

impl From<serde_json::Error> for ConnectionError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeError(e)
    }
}

impl From<&WorterbuchError> for ErrorCode {
    fn from(e: &WorterbuchError) -> Self {
        match e {
            WorterbuchError::IllegalWildcard(_) => ILLEGAL_WILDCARD,
            WorterbuchError::IllegalMultiWildcard(_) => ILLEGAL_MULTI_WILDCARD,
            WorterbuchError::MultiWildcardAtIllegalPosition(_) => {
                MULTI_WILDCARD_AT_ILLEGAL_POSITION
            }
            WorterbuchError::NoSuchValue(_) => NO_SUCH_VALUE,
            WorterbuchError::NotSubscribed => NOT_SUBSCRIBED,
            WorterbuchError::IoError(_, _) => IO_ERROR,
            WorterbuchError::SerDeError(_, _) => SERDE_ERROR,
            WorterbuchError::ProtocolNegotiationFailed => PROTOCOL_NEGOTIATION_FAILED,
            WorterbuchError::Other(_, _) | WorterbuchError::ServerResponse(_) => OTHER,
        }
    }
}

// impl TryFrom<&Err> for Option<WorterbuchError> {
//     type Error = DecodeError;

//     fn try_from(value: &Err) -> Result<Self, Self::Error> {
//         let Err {
//             error_code,
//             metadata,
//             ..
//         } = value;
//         match error_code {
//             &ILLEGAL_WILDCARD => Ok(Some(WorterbuchError::IllegalWildcard(
//                 serde_json::from_str(&metadata)?,
//             ))),
//             &ILLEGAL_MULTI_WILDCARD => Ok(Some(WorterbuchError::IllegalMultiWildcard(
//                 serde_json::from_str(&metadata)?,
//             ))),
//             &MULTI_WILDCARD_AT_ILLEGAL_POSITION => Ok(Some(
//                 WorterbuchError::MultiWildcardAtIllegalPosition(serde_json::from_str(&metadata)?),
//             )),
//             &IO_ERROR | &SERDE_ERROR | &OTHER => Ok(None),
//             _ => Err(DecodeError::UndefinedErrorCode(*error_code)),
//         }
//     }
// }
