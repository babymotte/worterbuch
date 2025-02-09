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

use super::config::Config;
use miette::{IntoDiagnostic, Result};
use tokio::net::UdpSocket;

pub async fn init_socket(config: &Config) -> Result<UdpSocket> {
    log::info!("Creating node socket …");

    let sock = UdpSocket::bind(format!("0.0.0.0:{}", config.orchestration_port))
        .await
        .into_diagnostic()?;

    // TODO configure socket

    Ok(sock)
}
