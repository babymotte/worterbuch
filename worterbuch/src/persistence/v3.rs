/*
 *  The worterbuch application library
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

use super::*;
use std::fmt::Debug;
use tracing::{Instrument, Level, debug_span, instrument};

pub(crate) async fn periodic(
    worterbuch: CloneableWbApi,
    config: Config,
    subsys: SubsystemHandle,
) -> Result<()> {
    let mut interval = config.persistence_interval();

    loop {
        select! {
            _ = interval.tick() => asynchronous(&worterbuch, &config).await?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    debug!("persistence subsystem completed.");

    Ok(())
}

#[instrument("persist_asynchronously", skip_all)]
async fn asynchronous(worterbuch: &CloneableWbApi, config: &Config) -> Result<()> {
    let span = debug_span!("export");
    debug!("Exporting database state …");
    let (json, grave_goods, last_will) = worterbuch.export(span).await?;
    debug!("Exporting database state done.");

    // checking AFTER export, since the interval may tick before the initial load is complete, but PERSISTENCE_LOCKED
    // is only set to false after the initial load. Export will however never complete before the initial load is done,
    // so this prevents random Errors
    if PERSISTENCE_LOCKED.load(Ordering::Acquire) {
        return Err(miette!("store is locked"));
    }

    let (
        store_path,
        store_path_checksum,
        grave_goods_last_will_path,
        grave_goods_last_will_path_checksum,
        last_persisted,
    ) = file_paths(config, true).await?;

    let json = json.to_string();
    write_and_check(json.as_bytes(), &store_path, &store_path_checksum).await?;

    let json = serde_json::to_string(&GraveGoodsLastWill {
        grave_goods,
        last_will,
    })
    .into_diagnostic()?;
    write_and_check(
        json.as_bytes(),
        &grave_goods_last_will_path,
        &grave_goods_last_will_path_checksum,
    )
    .await?;

    File::create(&last_persisted).await.into_diagnostic()?;

    Ok(())
}

#[instrument("persist_synchronously", level=Level::DEBUG, skip(worterbuch, config), err)]
pub(crate) async fn synchronous(worterbuch: &mut Worterbuch, config: &Config) -> Result<()> {
    if PERSISTENCE_LOCKED.load(Ordering::Acquire) {
        return Err(miette!("store is locked"));
    }

    let (
        store_path,
        store_path_checksum,
        grave_goods_last_will_path,
        grave_goods_last_will_path_checksum,
        last_persisted,
    ) = file_paths(config, true).await?;

    debug!("Exporting database state …");
    let (data, grave_goods, last_will) = worterbuch.export();
    debug!("Exporting database state done.");

    let json = json!({ "data": data }).to_string();
    write_and_check(json.as_bytes(), &store_path, &store_path_checksum).await?;

    let json = serde_json::to_string(&GraveGoodsLastWill {
        grave_goods,
        last_will,
    })
    .into_diagnostic()?;
    write_and_check(
        json.as_bytes(),
        &grave_goods_last_will_path,
        &grave_goods_last_will_path_checksum,
    )
    .await?;

    File::create(&last_persisted).await.into_diagnostic()?;

    Ok(())
}

#[instrument(level=Level::DEBUG, skip(data), err)]
async fn write_and_check(data: &[u8], file_path: &Path, checksum_file_path: &Path) -> Result<()> {
    let file_path = file_path;
    let checksum_file_path = checksum_file_path;

    let checksum = compute_checksum(data);

    write_to_disk(data, file_path).await?;
    write_to_disk(checksum.as_bytes(), checksum_file_path).await?;

    Ok(())
}

#[instrument(level=Level::DEBUG, skip(data), err)]
async fn write_to_disk(data: &[u8], path: &Path) -> Result<()> {
    debug!("Writing file {} …", path.to_string_lossy());
    let tmp_file = format!("{}.tmp", path.to_string_lossy());
    write_file(&tmp_file, data).await?;
    validate_file_content(&tmp_file, data).await?;
    fs::rename(tmp_file, path)
        .instrument(debug_span!("rename"))
        .await
        .into_diagnostic()
        .wrap_err("moving temp file to actual file failed")?;
    debug!("Writing file {} done.", path.to_string_lossy());

    Ok(())
}

#[instrument(level=Level::DEBUG, skip(data), err)]
async fn write_file<P: AsRef<Path> + Debug>(path: P, data: &[u8]) -> Result<()> {
    let mut file = File::create(&path)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("creating file {:?} failed", path))?;
    file.write_all(data)
        .await
        .into_diagnostic()
        .wrap_err("writing backup checksum file failed")?;
    file.flush()
        .await
        .into_diagnostic()
        .wrap_err("failed to flush file")?;
    Ok(())
}

#[instrument(level=Level::DEBUG, skip(data), err)]
async fn validate_file_content<P: AsRef<Path> + Debug>(path: P, data: &[u8]) -> Result<()> {
    let path = path.as_ref();
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

#[instrument(skip(config) fields(version=3), err)]
pub async fn load(config: &Config) -> Result<Worterbuch> {
    let (
        store_path,
        store_path_checksum,
        grave_goods_last_will_path,
        grave_goods_last_will_path_checksum,
        _,
    ) = file_paths(config, false).await?;

    let mut wb = match try_load(&store_path, &store_path_checksum, config).await {
        Ok(worterbuch) => Ok(worterbuch),
        Err(e) => {
            warn!(
                "Could not load persistence file {}: {e}",
                store_path.to_string_lossy()
            );
            let (store_path, store_path_checksum, _, _, _) = file_paths(config, true).await?;
            info!(
                "Trying to load persistence file {} …",
                store_path.to_string_lossy()
            );
            try_load(&store_path, &store_path_checksum, config).await
        }
    }?;

    if let Ok(grave_goods_last_will) = match try_load_grave_goods_last_will(
        &grave_goods_last_will_path,
        &grave_goods_last_will_path_checksum,
    )
    .await
    {
        Ok(gglw) => Ok(gglw),
        Err(e) => {
            warn!(
                "Could not load persistence file {}: {e}",
                grave_goods_last_will_path.to_string_lossy()
            );
            let (_, _, grave_goods_last_will_path, grave_goods_last_will_path_checksum, _) =
                file_paths(config, true).await?;
            info!(
                "Trying to load persistence file {} …",
                grave_goods_last_will_path.to_string_lossy()
            );
            try_load_grave_goods_last_will(
                &grave_goods_last_will_path,
                &grave_goods_last_will_path_checksum,
            )
            .await
        }
    } {
        wb.apply_grave_goods(grave_goods_last_will.grave_goods)
            .await;
        wb.apply_last_wills(grave_goods_last_will.last_will).await;
    }

    Ok(wb)
}

async fn try_load(path: &Path, checksum: &Path, config: &Config) -> Result<Worterbuch> {
    let json = read_json_from_file(path, checksum).await?;
    let worterbuch = Worterbuch::from_json(&json, config.to_owned())?;
    info!("Wörterbuch successfully restored form persistence.");
    Ok(worterbuch)
}

#[instrument(level=Level::DEBUG, err)]
async fn try_load_grave_goods_last_will(
    path: &Path,
    checksum: &Path,
) -> Result<GraveGoodsLastWill> {
    let json = read_json_from_file(path, checksum).await?;
    let grave_goods_last_will = serde_json::from_str(&json).into_diagnostic()?;
    info!("Grave goods and last will successfully restored form persistence.");
    Ok(grave_goods_last_will)
}

#[instrument(level=Level::DEBUG, err)]
async fn read_json_from_file(path: &Path, checksum: &Path) -> Result<String, miette::Error> {
    let json = fs::read_to_string(path)
        .instrument(debug_span!(
            "read_to_string",
            path = path.to_string_lossy().to_string()
        ))
        .await
        .into_diagnostic()?;
    let checksum = fs::read_to_string(checksum)
        .instrument(debug_span!(
            "read_to_string",
            path = checksum.to_string_lossy().to_string()
        ))
        .await
        .into_diagnostic()?;
    validate_checksum(json.as_bytes(), &checksum)?;
    Ok(json)
}

#[instrument(level=Level::DEBUG, skip(config), ret, err)]
pub(crate) async fn file_paths(
    config: &Config,
    write: bool,
) -> Result<(PathBuf, PathBuf, PathBuf, PathBuf, PathBuf)> {
    let dir = PathBuf::from(&config.data_dir);

    let mut toggle_path = dir.clone();
    toggle_path.push(".toggle");

    let main = toggle_alternating_files(&toggle_path, write).await?;

    let mut store_path = dir.clone();
    let mut store_path_checksum = dir.clone();
    let mut grave_goods_last_will_path = dir.clone();
    let mut grave_goods_last_will_path_checksum = dir.clone();
    let mut last_persisted = dir;

    if main {
        store_path.push("store.a.json");
        store_path_checksum.push("store.a.json.sha256");
        grave_goods_last_will_path.push("gglw.a.json");
        grave_goods_last_will_path_checksum.push("gglw.a.json.sha256");
    } else {
        store_path.push("store.b.json");
        store_path_checksum.push("store.b.json.sha256");
        grave_goods_last_will_path.push("gglw.b.json");
        grave_goods_last_will_path_checksum.push("gglw.b.json.sha256");
    }
    last_persisted.push("last-presisted");

    Ok((
        store_path,
        store_path_checksum,
        grave_goods_last_will_path,
        grave_goods_last_will_path_checksum,
        last_persisted,
    ))
}

#[instrument(level=Level::DEBUG, ret, err)]
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

#[instrument(level=Level::DEBUG, skip(data), ret)]
fn compute_checksum(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

#[instrument(level=Level::DEBUG, skip(data), ret, err)]
fn validate_checksum(data: &[u8], checksum: &str) -> Result<()> {
    let sum = compute_checksum(data);
    if sum == checksum {
        Ok(())
    } else {
        Err(miette!("checksum did not match"))
    }
}
