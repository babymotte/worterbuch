/*
 *  Worterbuch app error definitions
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

use crate::{cluster::protocol::ProxyMessage, persistence::error::PersistenceError};
use miette::Diagnostic;
use std::io;
use tokio::sync::{mpsc, oneshot};
use tosub::RootSystemError;
use worterbuch_common::error::{ConfigError, WorterbuchError};

#[derive(Debug, Diagnostic, thiserror::Error)]
pub enum WorterbuchAppError {
    #[error("Persistence error")]
    PersistenceError(
        #[source]
        #[from]
        PersistenceError,
    ),
    #[error("Worterbuch error")]
    WorterbuchError(
        #[source]
        #[from]
        WorterbuchError,
    ),
    #[error("Config error")]
    ConfigError(
        #[source]
        #[from]
        ConfigError,
    ),
    #[error("Cluster error: {0}")]
    ClusterError(String),
    #[error("I/O error")]
    IoError(
        #[source]
        #[from]
        io::Error,
    ),
    #[error("Channel error")]
    ChannelError(
        #[source]
        #[from]
        oneshot::error::RecvError,
    ),
    #[error("Proxy send error")]
    ProxySendError(
        #[source]
        #[from]
        mpsc::error::SendError<ProxyMessage>,
    ),
    #[error("No license for feature {0}")]
    NoLicense(String),
    #[error("A critical subsystem terminated with an error")]
    RootSystemError(
        #[source]
        #[from]
        RootSystemError,
    ),
    #[error("{0}")]
    Wrapped(String, #[source] Box<WorterbuchAppError>),
}

pub trait IntoWbAppResult<T> {
    fn into_wb_app_result(self) -> WorterbuchAppResult<T>;
}

impl<T, E: Into<WorterbuchAppError>> IntoWbAppResult<T> for Result<T, E> {
    fn into_wb_app_result(self) -> WorterbuchAppResult<T> {
        self.map_err(Into::into)
    }
}

pub trait WrappedResult {
    type Result;
    type Output;

    fn wrap(self, msg: impl Into<String>) -> Self::Result;

    fn wrap_with(self, f: impl FnOnce() -> String) -> Self::Result;
}

pub type WorterbuchAppResult<T> = Result<T, WorterbuchAppError>;

impl<T> WrappedResult for WorterbuchAppResult<T> {
    type Result = WorterbuchAppResult<T>;
    type Output = T;

    fn wrap(self, msg: impl Into<String>) -> Self::Result {
        self.map_err(|e| WorterbuchAppError::Wrapped(msg.into(), Box::new(e)))
    }

    fn wrap_with(self, f: impl FnOnce() -> String) -> Self::Result {
        self.map_err(|e| WorterbuchAppError::Wrapped(f(), Box::new(e)))
    }
}
