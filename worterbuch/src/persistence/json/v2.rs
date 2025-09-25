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

#[instrument(skip(config) fields(version=2), err)]
pub async fn load(config: &Config) -> PersistenceResult<Worterbuch> {
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

async fn try_load(path: &Path, config: &Config) -> PersistenceResult<Worterbuch> {
    let json = fs::read_to_string(path).await?;
    let store = serde_json::from_str(&json)?;
    let worterbuch = Worterbuch::from_persistence(store, config.to_owned());
    info!("Wörterbuch successfully restored form persistence.");
    Ok(worterbuch)
}

async fn try_load_grave_goods_last_will(path: &Path) -> PersistenceResult<GraveGoodsLastWill> {
    let json = fs::read_to_string(path).await?;
    let grave_goods_last_will = serde_json::from_str(&json)?;
    info!("Grave goods and last will successfully restored form persistence.");
    Ok(grave_goods_last_will)
}

async fn file_paths(config: &Config, write: bool) -> PersistenceResult<(PathBuf, PathBuf)> {
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

pub(crate) async fn toggle_alternating_files(path: &Path, write: bool) -> PersistenceResult<bool> {
    if write {
        if remove_file(path).await.is_ok() {
            debug!(
                "toggle file {} removed, writing to backup",
                path.to_string_lossy()
            );
            Ok(false)
        } else {
            File::create(path).await?;
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
