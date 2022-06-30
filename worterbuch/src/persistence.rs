use crate::{config::Config, worterbuch::Worterbuch};
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::RwLock,
    time::sleep,
};

pub(crate) async fn periodic(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) -> Result<()> {
    let interval = config.persistence_interval;

    loop {
        sleep(interval).await;
        once(worterbuch.clone(), config.clone()).await?;
    }
}

pub(crate) async fn once(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) -> Result<()> {
    let wb = worterbuch.read().await;

    let dir = PathBuf::from(&config.data_dir);

    let mut json_temp_path = dir.clone();
    json_temp_path.push(".store.json~");
    let mut json_path = dir.clone();
    json_path.push(".store.json");
    let mut sha_temp_path = dir.clone();
    sha_temp_path.push(".store.sha~");
    let mut sha_path = dir.clone();
    sha_path.push(".store.sha");

    let mut file = File::create(&json_temp_path).await?;
    let sha = wb.export_to_file(&mut file).await?;
    let sha = hex::encode(&sha);
    let mut file = File::create(&sha_temp_path).await?;
    file.write_all(sha.as_bytes()).await?;

    fs::copy(&json_temp_path, &json_path).await?;
    fs::copy(&sha_temp_path, &sha_path).await?;

    Ok(())
}
