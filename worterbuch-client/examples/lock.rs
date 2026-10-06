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

#![allow(clippy::as_conversions)]
#![allow(clippy::unwrap_used)]

use miette::Result;
use std::{collections::BTreeSet, io, thread};
use tokio::runtime;
use tracing_subscriber::EnvFilter;
use worterbuch_client::connect_with_default_config;
use worterbuch_common::topic;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut threads = vec![];

    for i in 0..20 {
        let t = thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("creating tokio runtime failed");
            rt.block_on(async { add_number(i).await.expect("adding number failed") });
        });
        threads.push(t);
    }

    for t in threads {
        t.join().unwrap();
    }

    Ok(())
}

async fn add_number(i: usize) -> Result<()> {
    let (wb, _, _) = connect_with_default_config().await?;

    loop {
        if wb.lock(topic!("hello", "world")).await.is_err() {
            continue;
        }

        tracing::info!("{i} has acquired lock, updating");

        let mut set = wb
            .get(topic!("hello", "world"))
            .await?
            .unwrap_or_else(BTreeSet::new);

        tracing::info!("adding {i}");

        set.insert(i);

        tracing::info!("updating {i}");

        wb.set(topic!("hello", "world"), set).await?;

        tracing::info!("{i} done.");

        wb.disconnect();

        break;
    }

    Ok(())
}
