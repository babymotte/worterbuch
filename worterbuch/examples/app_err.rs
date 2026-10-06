/*
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

use miette::{Context, IntoDiagnostic};
use std::io;
use tosub::Subsystem;
use worterbuch::error::{IntoWbAppResult, WorterbuchAppResult, WrappedResult};

#[tokio::main]
async fn main() -> miette::Result<()> {
    tosub::build_default_root("worterbuch").start(run).await?;
    Ok(())
}

async fn run(s: Subsystem) -> miette::Result<()> {
    s.spawn("wb-subsys", |s| async move {
        mock_wb_app(s)
            .await
            .into_diagnostic()
            .wrap_err("failed to start worterbuch")
            .wrap_err("did some crazy shit")
    });

    s.shutdown_requested().await;

    Ok(())
}

async fn mock_wb_app(s: Subsystem) -> WorterbuchAppResult<()> {
    s.spawn::<()>("wb-subsys", |_| async move {
        Err(io::Error::new(io::ErrorKind::Other, "oopsies"))
            .into_wb_app_result()
            .wrap("something went wrong")
    });

    s.shutdown_requested().await;

    Ok(())
}
