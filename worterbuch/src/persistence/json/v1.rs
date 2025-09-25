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

#[instrument(skip(config) fields(version=1), err)]
pub async fn load(config: &Config) -> PersistenceResult<Worterbuch> {
    let (json_temp_path, json_path, sha_temp_path, sha_path) = file_paths(config);

    if !json_path.exists() && !json_temp_path.exists() {
        info!("No persistence file found, starting empty instance.");
        return Ok(Worterbuch::with_config(config.clone()));
    }

    match try_load(&json_path, &sha_path, config).await {
        Ok(worterbuch) => {
            info!("Wörterbuch successfully restored form persistence.");
            Ok(worterbuch)
        }
        Err(e) => {
            warn!("Default persistence file could not be loaded: {e}");
            info!("Restoring Wörterbuch form backup file …");
            let worterbuch = try_load(&json_temp_path, &sha_temp_path, config).await?;
            info!("Wörterbuch successfully restored form backup file.");
            Ok(worterbuch)
        }
    }
}

async fn try_load(
    json_path: &PathBuf,
    sha_path: &PathBuf,
    config: &Config,
) -> PersistenceResult<Worterbuch> {
    let json = fs::read_to_string(json_path).await?;
    let sha = fs::read_to_string(sha_path).await?;

    let mut hasher = Sha256::new();
    hasher.update(&json);
    let result = hasher.finalize();
    let loaded_sha = hex::encode(result);

    if sha != loaded_sha {
        Err(PersistenceError::ChecksumMismatch)
    } else {
        let store = serde_json::from_str(&json)?;
        let worterbuch = Worterbuch::from_persistence(store, config.to_owned());
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
