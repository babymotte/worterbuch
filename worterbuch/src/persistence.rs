use crate::{config::Config, server::common::CloneableWbApi, worterbuch::Worterbuch};
use anyhow::Result;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    select,
    time::interval,
};
use tokio_graceful_shutdown::SubsystemHandle;

pub(crate) async fn periodic(
    worterbuch: CloneableWbApi,
    config: Config,
    subsys: SubsystemHandle,
) -> Result<()> {
    let mut interval = interval(config.persistence_interval);

    loop {
        select! {
            _ = interval.tick() => once(&worterbuch, config.clone()).await?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

pub(crate) async fn once(worterbuch: &CloneableWbApi, config: Config) -> Result<()> {
    let (json_temp_path, json_path, sha_temp_path, sha_path) = file_paths(&config);

    let json = worterbuch.export().await?.to_string();

    let mut hasher = Sha256::new();
    hasher.update(&json);
    let result = hasher.finalize();
    let sha = hex::encode(result);

    let mut file = File::create(&json_temp_path).await?;
    file.write_all(json.as_bytes()).await?;

    let mut file = File::create(&sha_temp_path).await?;
    file.write_all(sha.as_bytes()).await?;

    fs::copy(&json_temp_path, &json_path).await?;
    fs::copy(&sha_temp_path, &sha_path).await?;

    Ok(())
}

pub(crate) async fn load(config: Config) -> Result<Worterbuch> {
    log::info!("Restoring Wörterbuch form persistence …");

    let (json_temp_path, json_path, sha_temp_path, sha_path) = file_paths(&config);

    if !json_path.exists() && !json_temp_path.exists() {
        log::info!("No persistence file found, starting empty instance.");
        return Ok(Worterbuch::with_config(config));
    }

    match try_load(&json_path, &sha_path, &config).await {
        Ok(worterbuch) => {
            log::info!("Wörterbuch successfully restored form persistence.");
            Ok(worterbuch)
        }
        Err(e) => {
            log::warn!("Default persistence file could not be loaded: {e}");
            log::info!("Restoring Wörterbuch form backup file …");
            let worterbuch = try_load(&json_temp_path, &sha_temp_path, &config).await?;
            log::info!("Wörterbuch successfully restored form backup file.");
            Ok(worterbuch)
        }
    }
}

async fn try_load(json_path: &PathBuf, sha_path: &PathBuf, config: &Config) -> Result<Worterbuch> {
    let json = fs::read_to_string(json_path).await?;
    let sha = fs::read_to_string(sha_path).await?;

    let mut hasher = Sha256::new();
    hasher.update(&json);
    let result = hasher.finalize();
    let loaded_sha = hex::encode(result);

    if sha != loaded_sha {
        Err(anyhow::Error::msg("checksums did not match"))
    } else {
        let worterbuch = Worterbuch::from_json(&json, config.to_owned())?;
        Ok(worterbuch)
    }
}

fn file_paths(config: &Config) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
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
