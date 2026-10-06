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

use miette::Result;
use std::{io, time::Duration};
use tokio::{spawn, time::sleep};
use tracing_subscriber::EnvFilter;
use worterbuch_client::{Value, connect_with_default_config};
use worterbuch_common::topic;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let (wb1, _, _) = connect_with_default_config().await?;
    let (wb2, _, _) = connect_with_default_config().await?;

    let (mut sub, _) = wb1
        .psubscribe::<Value>(topic!("some", "thing", "?"), true, false, false, None)
        .await?;

    spawn(async move {
        while let Some(event) = sub.recv().await {
            eprintln!("{event:?}");
        }
    });

    for i in 0..999 {
        wb2.set_async(topic!("some", "thing", i), i).await?;
        sleep(Duration::from_secs(1)).await;
        wb2.delete_async(topic!("some", "thing", i)).await?;
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
