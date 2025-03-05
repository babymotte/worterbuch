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

use crate::{config::Config, server::common::CloneableWbApi, worterbuch::Worterbuch};
use miette::{Context, IntoDiagnostic, Result, miette};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, File, remove_file},
    io::{AsyncReadExt, AsyncWriteExt},
    select,
    time::interval,
};
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{debug, info, warn};
use worterbuch_common::{GraveGoods, LastWill};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GraveGoodsLastWill {
    grave_goods: GraveGoods,
    last_will: LastWill,
}

pub(crate) async fn periodic(
    worterbuch: CloneableWbApi,
    config: Config,
    subsys: SubsystemHandle,
) -> Result<()> {
    let mut interval = interval(config.persistence_interval);

    loop {
        select! {
            _ = interval.tick() => once(&worterbuch, &config).await?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    debug!("persistence subsystem completed.");

    Ok(())
}

pub(crate) async fn once(worterbuch: &CloneableWbApi, config: &Config) -> Result<()> {
    let (store_path, grave_goods_last_will_path) = file_paths(config, true).await?;

    // TODO persist grave goods and last wills to disk

    debug!("Exporting database state …");
    match worterbuch.export().await? {
        Some((json, grave_goods, last_will)) => {
            debug!("Exporting database state done.");

            let json = json.to_string();
            write_and_check(json.as_bytes(), &store_path).await?;

            let json = serde_json::to_string(&GraveGoodsLastWill {
                grave_goods,
                last_will,
            })
            .into_diagnostic()?;
            write_and_check(json.as_bytes(), &grave_goods_last_will_path).await?;
        }
        None => {
            debug!("No unsaved changes, skipping export.");
        }
    }

    Ok(())
}

pub(crate) async fn synchronous(worterbuch: &mut Worterbuch, config: &Config) -> Result<()> {
    let (store_path, grave_goods_last_will_path) = file_paths(config, true).await?;

    // TODO persist grave goods and last wills to disk

    debug!("Exporting database state …");
    let (data, grave_goods, last_will) = worterbuch.export();
    debug!("Exporting database state done.");

    let json = json!({ "data": data }).to_string();
    write_and_check(json.as_bytes(), &store_path).await?;

    let json = serde_json::to_string(&GraveGoodsLastWill {
        grave_goods,
        last_will,
    })
    .into_diagnostic()?;
    write_and_check(json.as_bytes(), &grave_goods_last_will_path).await?;

    Ok(())
}

async fn file_paths(config: &Config, write: bool) -> Result<(PathBuf, PathBuf)> {
    let dir = PathBuf::from(&config.data_dir);

    let mut toggle_path = dir.clone();
    toggle_path.push(".toggle");

    let main = toggle_alternating_files(&toggle_path, write).await?;

    let mut store_path = dir.clone();
    let mut grave_goods_last_will_path = dir;

    if main {
        store_path.push(".store.a.json");
        grave_goods_last_will_path.push(".gglw.a.json");
    } else {
        store_path.push(".store.b.json");
        grave_goods_last_will_path.push(".gglw.b.json");
    }

    Ok((store_path, grave_goods_last_will_path))
}

async fn toggle_alternating_files(path: &Path, write: bool) -> Result<bool> {
    if write {
        if remove_file(path).await.is_ok() {
            debug!(
                "toggle file {} removed, writing to backup",
                path.to_string_lossy()
            );
            Ok(false)
        } else {
            File::create(path).await.into_diagnostic()?;
            debug!(
                "toggle file {} created, writing to main",
                path.to_string_lossy()
            );
            Ok(true)
        }
    } else if File::open(path).await.is_ok() {
        debug!(
            "toggle file {} exists, reading from main",
            path.to_string_lossy()
        );
        Ok(true)
    } else {
        debug!(
            "toggle file {} does not exists, reading from backup",
            path.to_string_lossy()
        );
        Ok(false)
    }
}

async fn write_and_check(data: &[u8], path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();

    write_to_disk(data, path).await?;
    validate_file_content(path, data).await?;

    Ok(())
}

async fn write_to_disk(data: &[u8], path: &Path) -> Result<()> {
    debug!("Writing file {} …", path.to_string_lossy());
    let mut file = File::create(path)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("creating file {} failed", path.to_string_lossy()))?;
    file.write_all(data)
        .await
        .into_diagnostic()
        .wrap_err("writing backup checksum file failed")?;
    file.flush()
        .await
        .into_diagnostic()
        .wrap_err("failed to flush file")?;
    debug!("Writing file {} done.", path.to_string_lossy());

    Ok(())
}

async fn validate_file_content(path: &Path, data: &[u8]) -> Result<()> {
    debug!("Validating content of file {} …", path.to_string_lossy());
    let mut written_data = vec![];
    let mut file = File::open(path).await.into_diagnostic()?;
    file.read_to_end(&mut written_data)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("failed to read written file {:?}", file))?;

    if written_data != data {
        Err(miette!(
            "writing file {} failed: written data and actual data don't match",
            path.to_string_lossy()
        ))?;
    }

    debug!("Content of file {} is OK.", path.to_string_lossy());

    Ok(())
}

pub(crate) async fn load(config: Config) -> Result<Worterbuch> {
    match load_v2(&config).await {
        Ok(wb) => Ok(wb),
        Err(e) => {
            warn!("Could not load persistence file: {e}");
            info!("Trying to load legacy persistence file …");
            legacy::load(config).await
        }
    }
}

async fn load_v2(config: &Config) -> Result<Worterbuch> {
    info!("Restoring Wörterbuch form persistence …");
    let (store_path, grave_goods_last_will_path) = file_paths(config, false).await?;

    let mut wb = match try_load(&store_path, config).await {
        Ok(worterbuch) => Ok(worterbuch),
        Err(e) => {
            warn!(
                "Could not load persistence file {}: {e}",
                store_path.to_string_lossy()
            );
            let (store_path, _) = file_paths(config, true).await?;
            info!(
                "Trying to load persistence file {} …",
                store_path.to_string_lossy()
            );
            try_load(&store_path, config).await
        }
    }?;

    if let Ok(grave_goods_last_will) =
        match try_load_grave_goods_last_will(&grave_goods_last_will_path).await {
            Ok(gglw) => Ok(gglw),
            Err(e) => {
                warn!(
                    "Could not load persistence file {}: {e}",
                    grave_goods_last_will_path.to_string_lossy()
                );
                let (_, grave_goods_last_will_path) = file_paths(config, true).await?;
                info!(
                    "Trying to load persistence file {} …",
                    grave_goods_last_will_path.to_string_lossy()
                );
                try_load_grave_goods_last_will(&grave_goods_last_will_path).await
            }
        }
    {
        wb.apply_grave_goods(grave_goods_last_will.grave_goods)
            .await;
        wb.apply_last_wills(grave_goods_last_will.last_will).await;
    }

    Ok(wb)
}

async fn try_load(path: &Path, config: &Config) -> Result<Worterbuch> {
    let json = fs::read_to_string(path).await.into_diagnostic()?;
    let worterbuch = Worterbuch::from_json(&json, config.to_owned())?;
    info!("Wörterbuch successfully restored form persistence.");
    Ok(worterbuch)
}

async fn try_load_grave_goods_last_will(path: &Path) -> Result<GraveGoodsLastWill> {
    let json = fs::read_to_string(path).await.into_diagnostic()?;
    let grave_goods_last_will = serde_json::from_str(&json).into_diagnostic()?;
    info!("Grave goods and last will successfully restored form persistence.");
    Ok(grave_goods_last_will)
}

mod legacy {

    use super::*;

    pub(crate) async fn load(config: Config) -> Result<Worterbuch> {
        let (json_temp_path, json_path, sha_temp_path, sha_path) = file_paths(&config);

        if !json_path.exists() && !json_temp_path.exists() {
            info!("No persistence file found, starting empty instance.");
            return Ok(Worterbuch::with_config(config));
        }

        match try_load(&json_path, &sha_path, &config).await {
            Ok(worterbuch) => {
                info!("Wörterbuch successfully restored form persistence.");
                Ok(worterbuch)
            }
            Err(e) => {
                warn!("Default persistence file could not be loaded: {e}");
                info!("Restoring Wörterbuch form backup file …");
                let worterbuch = try_load(&json_temp_path, &sha_temp_path, &config).await?;
                info!("Wörterbuch successfully restored form backup file.");
                Ok(worterbuch)
            }
        }
    }

    async fn try_load(
        json_path: &PathBuf,
        sha_path: &PathBuf,
        config: &Config,
    ) -> Result<Worterbuch> {
        let json = fs::read_to_string(json_path).await.into_diagnostic()?;
        let sha = fs::read_to_string(sha_path).await.into_diagnostic()?;

        let mut hasher = Sha256::new();
        hasher.update(&json);
        let result = hasher.finalize();
        let loaded_sha = hex::encode(result);

        if sha != loaded_sha {
            Err(miette!("checksums did not match"))
        } else {
            let worterbuch = Worterbuch::from_json(&json, config.to_owned())?;
            Ok(worterbuch)
        }
    }

    pub(crate) fn file_paths(config: &Config) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let dir = PathBuf::from(&config.data_dir);

        let mut json_temp_path = dir.clone();
        json_temp_path.push(".store.json~");
        let mut json_path = dir.clone();
        json_path.push(".store.json");
        let mut sha_temp_path = dir.clone();
        sha_temp_path.push(".store.sha~");
        let mut sha_path = dir.clone();
        sha_path.push(".store.sha");

        (json_temp_path, json_path, sha_temp_path, sha_path)
    }
}
