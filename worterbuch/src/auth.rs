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
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use worterbuch_common::{
    error::{AuthorizationError, AuthorizationResult},
    KeySegment, Privilege, RequestPattern,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtClaims {
    pub sub: String,
    pub name: String,
    pub exp: usize,
    pub worterbuch_privileges: HashMap<Privilege, Vec<RequestPattern>>,
}

impl JwtClaims {
    pub fn authorize(&self, privilege: &Privilege, pattern: &str) -> AuthorizationResult<()> {
        self.worterbuch_privileges.get(&privilege).map_or_else(
            || {
                Err(AuthorizationError::InsufficientPrivileges(
                    privilege.to_owned(),
                    pattern.to_owned(),
                ))
            },
            |allowed_patters| {
                if allowed_patters
                    .iter()
                    .find(|p| pattern_matches(p, pattern))
                    .is_some()
                {
                    Ok(())
                } else {
                    Err(AuthorizationError::InsufficientPrivileges(
                        privilege.to_owned(),
                        pattern.to_owned(),
                    ))
                }
            },
        )
    }
}

pub fn get_claims(jwt: Option<&str>, config: &Config) -> AuthorizationResult<JwtClaims> {
    if let Some(secret) = &config.auth_token {
        if let Some(token) = jwt {
            let token = decode::<JwtClaims>(
                &token,
                &DecodingKey::from_secret(secret.as_ref()),
                &Validation::default(),
            )
            .map_err(|e| AuthorizationError::TokenDecodeError(e.to_string()))?;
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
