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

use miette::{IntoDiagnostic, Result};
use std::env;
use worterbuch::{Config, run_worterbuch};

fn main() -> Result<()> {
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

fn run_single_threaded() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .into_diagnostic()?
        .block_on(start())?;
    Ok(())
}

fn run_multi_threaded() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .into_diagnostic()?
        .block_on(start())?;
    Ok(())
}

async fn start() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = Config::new().await?;

    #[cfg(feature = "telemetry")]
    {
        use worterbuch::telemetry;

        let args = config.args.clone();

        let hostname = hostname::get().into_diagnostic()?;
        let cluster_role = if args.leader {
            Some("leader".to_owned())
        } else if args.follower {
            Some("follower".to_owned())
        } else {
            None
        };
        telemetry::init(
            args.instance_name
                .unwrap_or_else(|| hostname.to_string_lossy().into_owned()),
            cluster_role,
        )
        .await?;
    }

    #[cfg(not(feature = "telemetry"))]
    {
        use worterbuch::logging;

        logging::init()?;
    }

    let cfg = config.clone();

    tosub::build_root("worterbuch")
        .catch_signals()
        .with_timeout(cfg.shutdown_timeout)
        .start(async |s| run_worterbuch(s, config).await)
        .await?;

    Ok(())
}
