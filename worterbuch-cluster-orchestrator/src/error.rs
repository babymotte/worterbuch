/*
 *  Copyright (C) 2025 Michael Bachmann
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

use axum::{http::StatusCode, response::IntoResponse};
use miette::Diagnostic;
use opentelemetry_otlp::ExporterBuildError;
use std::io;
use thiserror::Error;
use tracing_subscriber::{filter::ParseError, util::TryInitError};

#[derive(Error, Debug, Diagnostic)]
pub enum WorterbuchClusterOrchestratorError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("Tracing init error: {0}")]
    TryInit(#[from] TryInitError),
    #[error("Tracing init error: {0}")]
    ExporterBuildError(#[from] ExporterBuildError),
    #[error("Tracing config parse error: {0}")]
    Parse(#[from] ParseError),
}

impl IntoResponse for WorterbuchClusterOrchestratorError {
    // TODO differentiate between error causes
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{self}")).into_response()
    }
}

pub type WorterbuchClusterOrchestratorResult<T> = Result<T, WorterbuchClusterOrchestratorError>;
