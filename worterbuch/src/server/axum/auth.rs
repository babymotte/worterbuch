/*
 *  Worterbuch server authorization module
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

use crate::{
    Config,
    auth::{JwtClaims, get_claims},
};
use axum::{
    extract::{OptionalFromRequestParts, Request, State},
    http::request::Parts,
    middleware::Next,
    response::IntoResponse,
};
use axum_extra::{TypedHeader, extract::CookieJar};
use headers::{Authorization, authorization::Bearer};
use std::{convert::Infallible, future};
use worterbuch_common::error::WorterbuchResult;

pub async fn bearer_auth(
    State(config): State<Config>,
    jar: CookieJar,
    header_jwt: Option<TypedHeader<Authorization<Bearer>>>,
    mut req: Request,
    next: Next,
) -> WorterbuchResult<impl IntoResponse> {
    let cookie_jwt = jar.get("worterbuch_auth_jwt").map(|c| c.value().to_owned());

    let jwt = header_jwt.map(|h| h.token().to_owned()).or(cookie_jwt);

    match get_claims(jwt.as_deref(), &config) {
        Ok(claims) => {
            // Attach claims to request extensions
            req.extensions_mut().insert(claims);
        }
        Err(e) => {
            if config.auth_token_key.is_some() {
                return Err(e.into());
            }
        }
    }

    Ok(next.run(req).await)
}

impl<S> OptionalFromRequestParts<S> for JwtClaims
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        future::ready(Ok(parts.extensions.get::<JwtClaims>().cloned()))
    }
}
