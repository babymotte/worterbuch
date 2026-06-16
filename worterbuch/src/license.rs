/*
 *  Worterbuch license module
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

use crate::PersistenceMode;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str;
use worterbuch_common::KeyValuePairs;
use worterbuch_common::error::ConfigResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Features {
    #[serde(default = "Default::default")]
    pub jwt_authorization: bool,
    #[serde(default = "Default::default")]
    pub clustering: bool,
    #[serde(default = "Default::default")]
    pub extended_monitoring: bool,
    #[serde(default = "Default::default")]
    pub persistence: Vec<PersistenceMode>,
}

impl Default for Features {
    fn default() -> Self {
        Self {
            jwt_authorization: true,
            clustering: true,
            extended_monitoring: true,
            persistence: vec![PersistenceMode::Json],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    Foss,
    Single,
    Business,
    Premium,
    Partner,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct License {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "Default::default")]
    pub sub: Option<String>,
    pub name: String,
    pub iat: u64,
    pub exp: u64,
    pub plan: Plan,
    pub versions: ((u32, u32), (u32, u32)),
    pub features: Features,
    pub license_validation: Option<KeyValuePairs>,
}

impl Default for License {
    fn default() -> Self {
        Self::foss()
    }
}

impl License {
    fn foss() -> Self {
        Self {
            sub: None,
            name: "Everyone".into(),
            iat: 1708291670,
            exp: 999999999999,
            plan: Plan::Foss,
            versions: ((0, 0), (99999, 99999)),
            features: Features {
                clustering: true,
                extended_monitoring: true,
                jwt_authorization: true,
                #[cfg(not(feature = "turso"))]
                persistence: vec![
                    PersistenceMode::Json,
                    #[cfg(feature = "redb")]
                    PersistenceMode::ReDB,
                    #[cfg(feature = "sqlite")]
                    PersistenceMode::SQLite,
                    #[cfg(feature = "turso")]
                    PersistenceMode::Turso,
                ],
            },
            license_validation: None,
        }
    }
}

#[cfg(not(feature = "commercial"))]
pub async fn load_license(_: Option<&Path>) -> ConfigResult<License> {
    Ok(License::foss())
}

#[cfg(feature = "commercial")]
pub async fn load_license(license_file_path: Option<&Path>) -> ConfigResult<License> {
    commercial::load_license(license_file_path).await
}

#[cfg(feature = "commercial")]
pub mod commercial {

    use super::License;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    use std::{env, path::Path, str};
    use tokio::fs;
    use worterbuch_common::{
        WorterbuchVersion,
        error::{ConfigError, ConfigResult},
    };

    pub const LICENSE_PUBLIC_KEY: &str = env!("WORTERBUCH_LICENSE_PUBLIC_KEY");
    pub const WORTERBUCH_VERSION: (&str, &str, &str) = (
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR"),
        env!("CARGO_PKG_VERSION_PATCH"),
    );

    pub async fn load_license(license_file_path: Option<&Path>) -> ConfigResult<License> {
        let license_file = license_file_path.unwrap_or_else(|| Path::new("./license"));

        let license_file = fs::read_to_string(license_file).await.map_err(|e| {
            ConfigError::InvalidLicense(format!("License file could not be read: {e}"))
        })?;

        let validation = Validation::new(Algorithm::EdDSA);
        let key = DecodingKey::from_ed_pem(LICENSE_PUBLIC_KEY.as_ref()).map_err(|e| {
            ConfigError::InvalidLicense(format!("License key could not be read: {e}"))
        })?;

        let token = decode::<License>(&license_file, &key, &validation).map_err(|e| {
            ConfigError::InvalidLicense(format!(
                "Validity of license token could not be confirmed: {e}"
            ))
        })?;

        let wb_version = WorterbuchVersion(
            WORTERBUCH_VERSION
                .0
                .parse::<u32>()
                .expect("invalid cargo version"),
            WORTERBUCH_VERSION
                .1
                .parse::<u32>()
                .expect("invalid cargo version"),
            WORTERBUCH_VERSION
                .2
                .parse::<u32>()
                .expect("invalid cargo version"),
        );

        wb_version.check_covered_by_license(token.claims.versions.0, token.claims.versions.1)?;

        Ok(token.claims)
    }
}

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn deserialize_incomplete_features() {
        let license = r#"{"clustering":true}"#;
        let features = serde_json::from_str::<Features>(license).unwrap();
        assert_eq!(
            features,
            Features {
                clustering: true,
                extended_monitoring: false,
                jwt_authorization: false,
                persistence: vec![],
            }
        )
    }

    #[test]
    fn deserialize_unknown_features() {
        let license = r#"{"clustering":true,"unknownFeature":false}"#;
        let features = serde_json::from_str::<Features>(license).unwrap();
        assert_eq!(
            features,
            Features {
                clustering: true,
                extended_monitoring: false,
                jwt_authorization: false,
                persistence: vec![],
            }
        )
    }
}
