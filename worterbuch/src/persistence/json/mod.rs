/*
 *  Worterbuch persistence module
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

mod v1;
mod v2;
mod v3;

use crate::{
    config::Config,
    persistence::{
        PersistentStorage,
        error::{PersistenceError, PersistenceResult},
    },
    server::common::CloneableWbApi,
    worterbuch::Worterbuch,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, File, remove_file},
    io::{AsyncReadExt, AsyncWriteExt},
    select,
};
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{debug, info, instrument, warn};
use worterbuch_common::{GraveGoods, LastWill};

use lazy_static::lazy_static;
use std::sync::atomic::{AtomicBool, Ordering};

lazy_static! {
    pub static ref PERSISTENCE_LOCKED: AtomicBool = AtomicBool::new(true);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GraveGoodsLastWill {
    grave_goods: GraveGoods,
    last_will: LastWill,
}

pub(crate) async fn periodic(
    worterbuch: CloneableWbApi,
    config: Config,
    subsys: SubsystemHandle,
) -> PersistenceResult<()> {
    v3::periodic(worterbuch, config, subsys).await
}

pub(crate) async fn synchronous(
    worterbuch: &mut Worterbuch,
    config: &Config,
) -> PersistenceResult<()> {
    v3::synchronous(worterbuch, config).await
}

#[instrument(skip(config), err)]
pub(crate) async fn load(config: &Config) -> PersistenceResult<Worterbuch> {
    info!("Trying to load v3 persistence file …");
    let wb = match v3::load(config).await {
        Ok(wb) => Ok(wb),
        Err(e) => {
            warn!("Could not load persistence file: {e}");
            info!("Trying to load v2 persistence file …");
            match v2::load(config).await {
                Ok(wb) => Ok(wb),
                Err(e) => {
                    warn!("Could not load persistence file: {e}");
                    info!("Trying to load v1 persistence file …");
                    v1::load(config).await
                }
            }
        }
    };
    PERSISTENCE_LOCKED.store(false, Ordering::Release);
    Ok(wb?)
}

pub struct PersistentJsonStorage {
    config: Config,
}

impl PersistentJsonStorage {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl PersistentStorage for PersistentJsonStorage {
    fn update_value(
        &self,
        _: worterbuch_common::Key,
        _: worterbuch_common::Value,
    ) -> PersistenceResult<()> {
        // does nothing
        Ok(())
    }

    fn dalete_value(&self, _: worterbuch_common::Key) -> PersistenceResult<()> {
        // does nothing
        Ok(())
    }

    async fn flush(&self, worterbuch: &mut Worterbuch) -> PersistenceResult<()> {
        synchronous(worterbuch, &self.config).await
    }

    async fn load(&self) -> PersistenceResult<Worterbuch> {
        load(&self.config).await
    }
}
