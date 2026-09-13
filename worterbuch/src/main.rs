/*
 *  Entrypoint of the worterbuch server application
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

use clap::Parser;
use std::env;
use tosub::IntoSubsystemResult;
use tosub::SubsystemResult;
use worterbuch::{Args, Config, run_worterbuch};

fn main() -> SubsystemResult {
    if env::var("WORTERBUCH_SINGLE_THREADED")
        .map(|v| v.to_ascii_lowercase())
        .as_deref()
        == Ok("true")
    {
        run_single_threaded()
    } else {
        run_multi_threaded()
    }
}

fn run_single_threaded() -> SubsystemResult {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .into_subsystem_result("Failed to build single-threaded runtime")?
        .block_on(start())
}

fn run_multi_threaded() -> SubsystemResult {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .into_subsystem_result("Failed to build multi-threaded runtime")?
        .block_on(start())
}

async fn start() -> SubsystemResult {
    dotenvy::dotenv().ok();

    let args = Args::parse();

    let config = Config::new(Some(args.clone()))
        .await
        .into_subsystem_result("Failed to create config")?;

    #[cfg(feature = "telemetry")]
    let _telemetry_drop_guard = {
        use worterbuch::{Commands, telemetry};

        let hostname = hostname::get().into_subsystem_result("Failed to get hostname")?;
        let cluster_role = match args.command {
            Some(Commands::Leader { .. }) => Some("leader".to_owned()),
            Some(Commands::Follower { .. }) => Some("follower".to_owned()),
            Some(Commands::Proxy { .. }) => Some("proxy".to_owned()),
            None => None,
        };
        let drop_guard = telemetry::init(
            args.instance_name
                .unwrap_or_else(|| hostname.to_string_lossy().into_owned()),
            cluster_role,
        )
        .await
        .into_subsystem_result("telemetry initialization failed")?;
        drop_guard
    };

    #[cfg(not(feature = "telemetry"))]
    {
        use worterbuch::logging;
        logging::init()?;
    };

    let cfg = config.clone();

    let mut root_builder =
        tosub::build_default_root("worterbuch").with_timeout(cfg.shutdown_timeout);

    if cfg.role.is_orchestrated() || cfg.exit_on_stdin_close {
        root_builder = root_builder.shutdown_on_stdin_close();
    }

    root_builder
        .start(async |s| run_worterbuch(s, config).await)
        .await
}
