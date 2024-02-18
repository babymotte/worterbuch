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

use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};
use worterbuch_common::{
    error::{WorterbuchError, WorterbuchResult},
    KeySegment, RequestPattern,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Privilege {
    Read,
    Write,
    Delete,
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privilege::Read => "read".fmt(f),
            Privilege::Write => "write".fmt(f),
            Privilege::Delete => "delete".fmt(f),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtClaims {
    pub sub: String,
    pub name: String,
    pub exp: usize,
    pub worterbuch_privileges: HashMap<Privilege, Vec<RequestPattern>>,
}

impl JwtClaims {
    pub fn authorize(&self, privilege: &Privilege, pattern: &str) -> WorterbuchResult<()> {
        self.worterbuch_privileges.get(&privilege).map_or_else(
            || not_authorized(privilege, pattern),
            |allowed_patters| {
                if allowed_patters
                    .iter()
                    .find(|p| pattern_matches(p, pattern))
                    .is_some()
                {
                    Ok(())
                } else {
                    not_authorized(privilege, pattern)
                }
            },
        )
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

fn not_authorized(privilege: &Privilege, pattern: &str) -> WorterbuchResult<()> {
    Err(WorterbuchError::Unauthorized(format!(
        "client does not have {privilege} access to '{pattern}'"
    )))
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
