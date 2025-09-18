/*
 *  Worterbuch authorization module
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

use crate::Config;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use tracing::{Level, error, instrument};
use worterbuch_common::{
    AuthCheck, KeySegment, Privilege,
    error::{AuthorizationError, AuthorizationResult},
};

const EMPTY_PRIVILEGES: Vec<String> = vec![];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtClaims {
    pub sub: String,
    pub name: String,
    pub exp: u64,
    pub worterbuch_privileges: Privileges,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Privileges {
    pub read: Option<Vec<String>>,
    pub write: Option<Vec<String>>,
    pub delete: Option<Vec<String>>,
    pub profile: Option<bool>,
    pub web_login: Option<bool>,
}

impl JwtClaims {
    #[instrument(level=Level::DEBUG, err)]
    pub fn authorize(&self, privilege: &Privilege, check: AuthCheck) -> AuthorizationResult<()> {
        match privilege {
            Privilege::Read => {
                if let AuthCheck::Pattern(pattern) = check {
                    if self
                        .worterbuch_privileges
                        .read
                        .as_ref()
                        .unwrap_or(&EMPTY_PRIVILEGES)
                        .iter()
                        .any(|p| pattern_matches(p, pattern))
                    {
                        Ok(())
                    } else {
                        Err(AuthorizationError::InsufficientPrivileges(
                            privilege.to_owned(),
                            check.into(),
                        ))
                    }
                } else {
                    error!("Read privileges can only be checked against a pattern");
                    Err(AuthorizationError::InvalidCheck)
                }
            }
            Privilege::Write => {
                if let AuthCheck::Pattern(pattern) = check {
                    if self
                        .worterbuch_privileges
                        .write
                        .as_ref()
                        .unwrap_or(&EMPTY_PRIVILEGES)
                        .iter()
                        .any(|p| pattern_matches(p, pattern))
                    {
                        Ok(())
                    } else {
                        Err(AuthorizationError::InsufficientPrivileges(
                            privilege.to_owned(),
                            check.into(),
                        ))
                    }
                } else {
                    error!("Write privileges can only be checked against a pattern");
                    Err(AuthorizationError::InvalidCheck)
                }
            }
            Privilege::Delete => {
                if let AuthCheck::Pattern(pattern) = check {
                    if self
                        .worterbuch_privileges
                        .delete
                        .as_ref()
                        .unwrap_or(&EMPTY_PRIVILEGES)
                        .iter()
                        .any(|p| pattern_matches(p, pattern))
                    {
                        Ok(())
                    } else {
                        Err(AuthorizationError::InsufficientPrivileges(
                            privilege.to_owned(),
                            check.into(),
                        ))
                    }
                } else {
                    error!("Delete privileges can only be checked against a pattern");
                    Err(AuthorizationError::InvalidCheck)
                }
            }
            Privilege::Profile => {
                if let AuthCheck::Flag = check {
                    if *self
                        .worterbuch_privileges
                        .profile
                        .as_ref()
                        .unwrap_or(&false)
                    {
                        Ok(())
                    } else {
                        Err(AuthorizationError::InsufficientPrivileges(
                            privilege.to_owned(),
                            check.into(),
                        ))
                    }
                } else {
                    error!("Profile privileges can only be checked against a flag");
                    Err(AuthorizationError::InvalidCheck)
                }
            }
            Privilege::WebLogin => {
                if let AuthCheck::Flag = check {
                    if *self
                        .worterbuch_privileges
                        .web_login
                        .as_ref()
                        .unwrap_or(&false)
                    {
                        Ok(())
                    } else {
                        Err(AuthorizationError::InsufficientPrivileges(
                            privilege.to_owned(),
                            check.into(),
                        ))
                    }
                } else {
                    error!("WebLogin privileges can only be checked against a flag");
                    Err(AuthorizationError::InvalidCheck)
                }
            }
        }
    }
}

pub fn get_claims(jwt: Option<&str>, config: &Config) -> AuthorizationResult<JwtClaims> {
    if let Some(secret) = &config.auth_token_secret {
        if let Some(token) = jwt {
            let header = decode_header(token)?;

            let (alg, key) = match &header.alg {
                Algorithm::ES256 => (header.alg, DecodingKey::from_ec_pem(secret.as_ref())?),
                Algorithm::EdDSA => (header.alg, DecodingKey::from_ed_pem(secret.as_ref())?),
                Algorithm::HS256 => (header.alg, DecodingKey::from_secret(secret.as_ref())),
                _ => {
                    return Err(AuthorizationError::UnsupportedEncryptionAlgorithm(
                        header.alg,
                    ));
                }
            };

            let validation = Validation::new(alg);
            let token = decode::<JwtClaims>(token, &key, &validation)?;
            Ok(token.claims)
        } else {
            Err(AuthorizationError::MissingToken)
        }
    } else {
        Err(AuthorizationError::MissingSecret)
    }
}

pub fn pattern_matches(pattern: &str, key: &str) -> bool {
    let mut pattern = pattern.split('/');
    let mut key = key.split('/');

    loop {
        match (
            pattern.next().map(KeySegment::from),
            key.next().map(KeySegment::from),
        ) {
            (None, None) | (Some(KeySegment::MultiWildcard), Some(_)) => return true,
            (None, _) | (_, None) => return false,
            (Some(pattern_segment), Some(key_segment)) => {
                if (pattern_segment == KeySegment::Wildcard
                    && key_segment != KeySegment::MultiWildcard)
                    || pattern_segment == key_segment
                {
                    continue;
                } else {
                    return false;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_matches() {
        assert!(pattern_matches("hello", "hello"));
        assert!(!pattern_matches("hello", "hello/world"));
        assert!(!pattern_matches("hello", "hello/?"));
        assert!(!pattern_matches("hello", "hello/?/world"));
        assert!(!pattern_matches("hello", "hello/#"));
        assert!(!pattern_matches("hello", "?"));
        assert!(!pattern_matches("hello", "#"));

        assert!(pattern_matches("hello/world", "hello/world"));
        assert!(!pattern_matches("hello/world", "hello"));
        assert!(!pattern_matches("hello/world", "hello/there"));
        assert!(!pattern_matches("hello/world", "hello/there/world"));
        assert!(!pattern_matches("hello/world", "hello/?"));
        assert!(!pattern_matches("hello/world", "hello/?/world"));
        assert!(!pattern_matches("hello/world", "hello/#"));
        assert!(!pattern_matches("hello/world", "?"));
        assert!(!pattern_matches("hello/world", "#"));

        assert!(pattern_matches("hello/?", "hello/world"));
        assert!(pattern_matches("hello/?", "hello/there"));
        assert!(pattern_matches("hello/?", "hello/?"));
        assert!(!pattern_matches("hello/?", "hello"));
        assert!(!pattern_matches("hello/?", "hello/there/world"));
        assert!(!pattern_matches("hello/?", "hello/?/world"));
        assert!(!pattern_matches("hello/?", "hello/#"));
        assert!(!pattern_matches("hello/?", "?"));
        assert!(!pattern_matches("hello/?", "#"));

        assert!(pattern_matches("hello/#", "hello/#"));
        assert!(pattern_matches("hello/#", "hello/?"));
        assert!(pattern_matches("hello/#", "hello/there"));
        assert!(pattern_matches("hello/#", "hello/there/world"));
        assert!(pattern_matches("hello/#", "hello/?/world"));
        assert!(pattern_matches("hello/#", "hello/there/?"));
        assert!(pattern_matches("hello/#", "hello/there/#"));
        assert!(!pattern_matches("hello/#", "hello"));
        assert!(!pattern_matches("hello/#", "?"));
        assert!(!pattern_matches("hello/#", "#"));

        assert!(pattern_matches("?", "hello"));
        assert!(pattern_matches("?", "world"));
        assert!(pattern_matches("?", "?"));
        assert!(!pattern_matches("?", "hello/world"));
        assert!(!pattern_matches("?", "hello/?"));
        assert!(!pattern_matches("?", "hello/#"));
        assert!(!pattern_matches("?", "#"));

        assert!(pattern_matches("#", "hello"));
        assert!(pattern_matches("#", "world"));
        assert!(pattern_matches("#", "?"));
        assert!(pattern_matches("#", "#"));
    }
}
