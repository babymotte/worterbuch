/*
 *  Worterbuch error handling module
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::{
    AuthCheckOwned, ClientMessage, ErrorCode, Key, MetaData, Privilege, ProtocolVersionSegment,
    RequestPattern, TransactionId, server::Err,
};
use http::StatusCode;
use jsonwebtoken::Algorithm;
use miette::Diagnostic;
use opentelemetry_otlp::ExporterBuildError;
use std::{fmt, io, net::AddrParseError, num::ParseIntError};
use thiserror::Error;
use tokio::sync::{
    broadcast,
    mpsc::{self, error::SendError},
    oneshot,
};

#[derive(Debug, Error, Diagnostic)]
pub enum ConfigError {
    #[error("invalid separator: {0}; separator must be a single ASCII char")]
    InvalidSeparator(String),
    #[error("invalid wildcard: {0}; wildcard must be a single ASCII char")]
    InvalidWildcard(String),
    #[error("invalid multi-wildcard: {0}; multi-wildcard must be a single ASCII char")]
    InvalidMultiWildcard(String),
    #[error("invalid port: {0}")]
    InvalidPort(ParseIntError),
    #[error("invalid address: {0}")]
    InvalidAddr(AddrParseError),
    #[error("invalid interval: {0}")]
    InvalidInterval(ParseIntError),
    #[error("license file could not be loaded: {0}")]
    InvalidLicense(String),
    #[error("could not load config file: {0}")]
    IoError(#[from] io::Error),
    #[error("could not load config file: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("error setting up telemetry: {0}")]
    ExporterBuildError(#[from] ExporterBuildError),
}

pub trait ConfigIntContext<I> {
    fn to_port(self) -> Result<I, ConfigError>;
    fn to_interval(self) -> Result<I, ConfigError>;
}

impl<I> ConfigIntContext<I> for Result<I, ParseIntError> {
    fn to_port(self) -> Result<I, ConfigError> {
        self.map_err(ConfigError::InvalidPort)
    }
    fn to_interval(self) -> Result<I, ConfigError> {
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
#[derive(Debug, Clone, Error, Diagnostic)]
pub enum AuthorizationError {
    #[error("Client lacks privilege '{0} {1}'")]
    InsufficientPrivileges(Privilege, AuthCheckOwned),
    #[error("No JWT was included in the request")]
    MissingToken,
    #[error("No JWT was configured")]
    MissingSecret,
    // TODO should this not trigger a crash?
    #[error("Incorrect check provided. This is a bug.")]
    InvalidCheck,
    #[error("Unsupported encryption algorith: {0:?}")]
    UnsupportedEncryptionAlgorithm(Algorithm),
    #[error("JWT error: {0}")]
    JwtError(#[from] jsonwebtoken::errors::Error),
}

pub type AuthorizationResult<T> = Result<T, AuthorizationError>;

#[derive(Debug, Diagnostic, thiserror::Error)]
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
    ProtocolNegotiationFailed(ProtocolVersionSegment),
    ReadOnlyKey(Key),
    AuthorizationRequired(Privilege),
    AlreadyAuthorized,
    Unauthorized(#[from] AuthorizationError),
    NoPubStream(TransactionId),
    NotLeader,
    Cas,
    CasVersionMismatch,
    NotImplemented,
    KeyIsLocked(Key),
    KeyIsNotLocked(Key),
    FeatureDisabled(MetaData),
}

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
            WorterbuchError::ProtocolNegotiationFailed(v) => {
                write!(f, "The server does not implement protocol version {v}")
            }
            WorterbuchError::InvalidServerResponse(meta) => write!(
                f,
                "The server sent a response that is not valid for the issued request: {meta}"
            ),
            WorterbuchError::ReadOnlyKey(key) => {
                write!(f, "Tried to modify a read only key: {key}")
            }
            WorterbuchError::AuthorizationRequired(op) => {
                write!(f, "Operation {op} requires authorization")
            }
            WorterbuchError::AlreadyAuthorized => {
                write!(f, "Handshake already done")
            }
            WorterbuchError::Unauthorized(err) => err.fmt(f),
            WorterbuchError::NoPubStream(tid) => {
                write!(
                    f,
                    "There is no active publish stream for transaction ID {tid}"
                )
            }
            WorterbuchError::NotLeader => {
                write!(
                    f,
                    "Node cannot process the request since it is not the current cluster leader"
                )
            }
            WorterbuchError::Cas => {
                write!(
                    f,
                    "Tried to modify a compare-and-swap value without providing a version number"
                )
            }
            WorterbuchError::CasVersionMismatch => {
                write!(
                    f,
                    "Tried to modify a compare-and-swap value with an out-of-sync version number"
                )
            }
            WorterbuchError::NotImplemented => {
                write!(
                    f,
                    "This function is not implemented in the negotiated protocol version",
                )
            }
            WorterbuchError::KeyIsLocked(key) => {
                write!(f, "Key {key} is locked by another client",)
            }
            WorterbuchError::KeyIsNotLocked(key) => {
                write!(f, "Key {key} is not locked",)
            }
            WorterbuchError::FeatureDisabled(m) => m.fmt(f),
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

#[derive(Debug, Diagnostic)]
pub enum ConnectionError {
    IoError(Box<io::Error>),
    SendError(Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "ws")]
    WebsocketError(Box<tungstenite::Error>),
    #[cfg(feature = "wasm")]
    WebsocketError(Box<tokio_tungstenite_wasm::Error>),
    TrySendError(Box<dyn std::error::Error + Send + Sync>),
    RecvError(Box<oneshot::error::RecvError>),
    BcRecvError(Box<broadcast::error::RecvError>),
    WorterbuchError(Box<WorterbuchError>),
    ConfigError(Box<ConfigError>),
    SerdeError(Box<serde_json::Error>),
    AckError(Box<broadcast::error::SendError<u64>>),
    Timeout(Box<String>),
    #[cfg(feature = "ws")]
    HttpError(Box<tungstenite::http::Error>),
    AuthorizationError(Box<String>),
    NoServerAddressesConfigured,
    ServerResponse(Box<Err>),
}

impl std::error::Error for ConnectionError {}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => e.fmt(f),
            Self::SendError(e) => e.fmt(f),
            #[cfg(any(feature = "ws", feature = "wasm"))]
            Self::WebsocketError(e) => e.fmt(f),
            Self::TrySendError(e) => e.fmt(f),
            Self::RecvError(e) => e.fmt(f),
            Self::BcRecvError(e) => e.fmt(f),
            Self::WorterbuchError(e) => e.fmt(f),
            Self::ConfigError(e) => e.fmt(f),
            Self::SerdeError(e) => e.fmt(f),
            Self::AckError(e) => e.fmt(f),
            Self::Timeout(msg) => msg.fmt(f),
            #[cfg(feature = "ws")]
            Self::HttpError(e) => e.fmt(f),
            Self::AuthorizationError(msg) => msg.fmt(f),
            Self::NoServerAddressesConfigured => {
                fmt::Display::fmt("no server addresses configured", f)
            }
            Self::ServerResponse(e) => e.fmt(f),
        }
    }
}

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

impl From<Err> for ConnectionError {
    fn from(value: Err) -> Self {
        ConnectionError::ServerResponse(Box::new(value))
    }
}

impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::IoError(Box::new(e))
    }
}

impl<T: fmt::Debug + 'static + Send + Sync> From<SendError<T>> for ConnectionError {
    fn from(e: SendError<T>) -> Self {
        ConnectionError::SendError(Box::new(e))
    }
}

#[cfg(feature = "ws")]
impl From<tungstenite::Error> for ConnectionError {
    fn from(e: tungstenite::Error) -> Self {
        ConnectionError::WebsocketError(Box::new(e))
    }
}

#[cfg(feature = "wasm")]
impl From<tokio_tungstenite_wasm::Error> for ConnectionError {
    fn from(e: tokio_tungstenite_wasm::Error) -> Self {
        ConnectionError::WebsocketError(Box::new(e))
    }
}

impl From<oneshot::error::RecvError> for ConnectionError {
    fn from(e: oneshot::error::RecvError) -> Self {
        ConnectionError::RecvError(Box::new(e))
    }
}

impl From<broadcast::error::RecvError> for ConnectionError {
    fn from(e: broadcast::error::RecvError) -> Self {
        ConnectionError::BcRecvError(Box::new(e))
    }
}

impl From<ConfigError> for ConnectionError {
    fn from(e: ConfigError) -> Self {
        ConnectionError::ConfigError(Box::new(e))
    }
}

impl From<serde_json::Error> for ConnectionError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeError(Box::new(e))
    }
}

impl From<broadcast::error::SendError<u64>> for ConnectionError {
    fn from(e: broadcast::error::SendError<u64>) -> Self {
        Self::AckError(Box::new(e))
    }
}

impl From<mpsc::error::TrySendError<ClientMessage>> for ConnectionError {
    fn from(e: mpsc::error::TrySendError<ClientMessage>) -> Self {
        Self::TrySendError(Box::new(Box::new(e)))
    }
}

#[cfg(feature = "ws")]
impl From<tungstenite::http::Error> for ConnectionError {
    fn from(e: tungstenite::http::Error) -> Self {
        Self::HttpError(Box::new(e))
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
            WorterbuchError::ProtocolNegotiationFailed(_) => ErrorCode::ProtocolNegotiationFailed,
            WorterbuchError::InvalidServerResponse(_) => ErrorCode::InvalidServerResponse,
            WorterbuchError::ReadOnlyKey(_) => ErrorCode::ReadOnlyKey,
            WorterbuchError::AuthorizationRequired(_) => ErrorCode::AuthorizationRequired,
            WorterbuchError::AlreadyAuthorized => ErrorCode::AlreadyAuthorized,
            WorterbuchError::Unauthorized(_) => ErrorCode::Unauthorized,
            WorterbuchError::NoPubStream(_) => ErrorCode::NoPubStream,
            WorterbuchError::NotLeader => ErrorCode::NotLeader,
            WorterbuchError::Cas => ErrorCode::Cas,
            WorterbuchError::CasVersionMismatch => ErrorCode::CasVersionMismatch,
            WorterbuchError::NotImplemented => ErrorCode::NotImplemented,
            WorterbuchError::KeyIsLocked(_) => ErrorCode::KeyIsLocked,
            WorterbuchError::KeyIsNotLocked(_) => ErrorCode::KeyIsNotLocked,
            WorterbuchError::FeatureDisabled(_) => ErrorCode::KeyIsNotLocked,
            WorterbuchError::Other(_, _) | WorterbuchError::ServerResponse(_) => ErrorCode::Other,
        }
    }
}

impl From<WorterbuchError> for (StatusCode, String) {
    fn from(e: WorterbuchError) -> Self {
        match &e {
            WorterbuchError::IllegalMultiWildcard(_)
            | WorterbuchError::IllegalWildcard(_)
            | WorterbuchError::MultiWildcardAtIllegalPosition(_)
            | WorterbuchError::NotImplemented
            | WorterbuchError::KeyIsNotLocked(_) => (StatusCode::BAD_REQUEST, e.to_string()),

            WorterbuchError::AlreadyAuthorized
            | WorterbuchError::NotSubscribed
            | WorterbuchError::FeatureDisabled(_)
            | WorterbuchError::NoPubStream(_) => (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),

            WorterbuchError::KeyIsLocked(_)
            | WorterbuchError::Cas
            | WorterbuchError::CasVersionMismatch => (StatusCode::CONFLICT, e.to_string()),

            WorterbuchError::ReadOnlyKey(_) => (StatusCode::METHOD_NOT_ALLOWED, e.to_string()),

            WorterbuchError::AuthorizationRequired(_) => (StatusCode::UNAUTHORIZED, e.to_string()),

            WorterbuchError::NoSuchValue(_) => (StatusCode::NOT_FOUND, e.to_string()),

            WorterbuchError::Unauthorized(ae) => match &ae {
                AuthorizationError::MissingToken => (StatusCode::UNAUTHORIZED, e.to_string()),
                _ => (StatusCode::FORBIDDEN, e.to_string()),
            },

            WorterbuchError::IoError(_, _)
            | WorterbuchError::SerDeError(_, _)
            | WorterbuchError::InvalidServerResponse(_)
            | WorterbuchError::Other(_, _)
            | WorterbuchError::ServerResponse(_)
            | WorterbuchError::ProtocolNegotiationFailed(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }

            WorterbuchError::NotLeader => (StatusCode::NO_CONTENT, e.to_string()),
        }
    }
}

#[cfg(feature = "axum-errors")]
pub mod axum {
    use crate::error::{AuthorizationError, WorterbuchError};
    use axum::response::IntoResponse;
    use http::StatusCode;

    impl IntoResponse for WorterbuchError {
        fn into_response(self) -> axum::response::Response {
            let err: (StatusCode, String) = self.into();
            err.into_response()
        }
    }

    impl IntoResponse for AuthorizationError {
        fn into_response(self) -> axum::response::Response {
            let err: WorterbuchError = self.into();
            err.into_response()
        }
    }
}
