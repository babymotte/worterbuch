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

use serde::{Deserialize, Serialize};
use std::str;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Features {
    #[serde(default = "Default::default")]
    pub jwt_authorization: bool,
    #[serde(default = "Default::default")]
    pub clustering: bool,
    #[serde(default = "Default::default")]
    pub extended_monitoring: bool,
}

impl Default for Features {
    fn default() -> Self {
        Self {
            jwt_authorization: true,
            clustering: true,
            extended_monitoring: true,
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
    pub versions: (usize, usize),
    pub features: Features,
}

#[cfg(not(feature = "commercial"))]
impl License {
    fn foss() -> Self {
        Self {
            sub: None,
            name: "Everyone".into(),
            iat: 1708291670,
            exp: 999999999999,
            plan: Plan::Foss,
            versions: (1, 99999),
            features: Features {
                clustering: true,
                extended_monitoring: true,
                jwt_authorization: true,
            },
        }
    }
}

#[cfg(not(feature = "commercial"))]
pub async fn load_license() -> miette::Result<License> {
    Ok(License::foss())
}

#[cfg(feature = "commercial")]
pub async fn load_license() -> miette::Result<License> {
    commercial::load_license().await
}

#[cfg(feature = "commercial")]
pub mod commercial {

    use super::License;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    use miette::{Context, IntoDiagnostic};
    use std::{env, str};
    use tokio::fs;

    pub const LICENSE_SECRET_KEY: &str = env!("WORTERBUCH_LICENSE_SECRET_KEY");

    pub async fn load_license() -> miette::Result<License> {
        // TODO get from config
        let license_file = env::var("WORTERBUCH_LICENSE_FILE")
            .into_diagnostic()
            .wrap_err("WORTERBUCH_LICENSE_FILE is not set")?;

        let license_file = fs::read_to_string(license_file)
            .await
            .into_diagnostic()
            .wrap_err("Could not read license file")?;

        let validation = Validation::new(Algorithm::EdDSA);
        let key = DecodingKey::from_ed_pem(LICENSE_SECRET_KEY.as_ref()).into_diagnostic()?;

        let token = decode::<License>(&license_file, &key, &validation)
            .into_diagnostic()
            .wrap_err("Validity of license token could not be confirmed")?;

        Ok(token.claims)
    }
}

#[cfg(test)]
mod test {
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
                jwt_authorization: false
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
                jwt_authorization: false
            }
        )
    }
}
