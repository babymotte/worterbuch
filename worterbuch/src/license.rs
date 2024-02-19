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
    pub licensee: String,
    pub issued: u64,
    pub expires: u64,
    pub plan: Plan,
    pub versions: (usize, usize),
    pub features: Features,
}

#[cfg(not(feature = "commercial"))]
impl License {
    fn foss() -> Self {
        Self {
            licensee: "Everyone".into(),
            issued: 1708291670,
            expires: 999999999999,
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
    use miette::{miette, Context, IntoDiagnostic};
    use pgp::{types::KeyTrait, Deserializable, Message, SignedPublicKey};
    use std::{env, str};
    use tokio::fs;

    pub const PUBLIC_KEY_FINGERPRINT: &str = env!("WORTERBUCH_PUBLIC_KEY_FINGERPRINT");

    pub async fn load_license() -> miette::Result<License> {
        let key = load_key_file().await?;

        let license = load_license_file().await?;

        let content = license
            .get_content()
            .into_diagnostic()
            .context("Could not read content of license file")?
            .ok_or_else(|| miette!("File contains no license data"))?;

        if let Message::Signed {
            message: _,
            one_pass_signature: _,
            signature,
        } = license
        {
            let data: &[u8] = &content;
            let key_id = signature
                .issuer()
                .ok_or_else(|| miette!("License signature has no issuer"))?;

            let mut success = false;
            for key in &key.public_subkeys {
                if key_id == &key.key_id() && signature.verify(key, data).is_ok() {
                    success = true;
                    log::info!("License signature successfully verified");
                    break;
                }
            }
            if !success {
                signature
                    .verify(&key, data)
                    .into_diagnostic()
                    .context("The validity of the license file could not be verified")?;
                log::info!("License signature successfully verified");
            }
        }

        let license = serde_json::from_slice(&content)
            .into_diagnostic()
            .context("Could not parse license file")?;

        Ok(license)
    }

    async fn load_key_file() -> miette::Result<SignedPublicKey> {
        let key_file = env::var("WORTERBUCH_PUBLIC_KEY_FILE")
            .into_diagnostic()
            .context("WORTERBUCH_PUBLIC_KEY_FILE is not set")?;

        let key_file = fs::read_to_string(key_file)
            .await
            .into_diagnostic()
            .context("Could not read public key file")?;

        let (key, _headers) = SignedPublicKey::from_string(&key_file)
            .into_diagnostic()
            .context("Could not parse public key")?;

        let fingerprint = hex::encode_upper(key.fingerprint());

        if fingerprint != PUBLIC_KEY_FINGERPRINT {
            return Err(miette!("Expected public key with fingerprint {PUBLIC_KEY_FINGERPRINT}, but got {fingerprint}. Cannot verify license file."));
        }

        key.verify()
            .into_diagnostic()
            .context("Could not verify public key")?;

        Ok(key)
    }

    async fn load_license_file() -> miette::Result<Message> {
        let license_file = env::var("WORTERBUCH_LICENSE_FILE")
            .into_diagnostic()
            .context("WORTERBUCH_LICENSE_FILE is not set")?;

        let license_file = fs::read_to_string(license_file)
            .await
            .into_diagnostic()
            .context("Could not read license file")?;

        let (license, _headers) = Message::from_string(&license_file)
            .into_diagnostic()
            .context("Could not parse license file")?;

        let license = license
            .decompress()
            .into_diagnostic()
            .context("Invalid license file")?;

        Ok(license)
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
