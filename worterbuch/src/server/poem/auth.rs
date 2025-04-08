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
use poem::{
    Endpoint, EndpointExt, Middleware, Request, Response, Result,
    middleware::{AddData, Cors},
    web::headers::{self, HeaderMapExt, authorization::Bearer},
};

pub struct BearerAuth {
    config: Config,
}

impl BearerAuth {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl<E: Endpoint> Middleware<E> for BearerAuth {
    type Output = BearerAuthEndpoint<E>;

    fn transform(&self, ep: E) -> Self::Output {
        BearerAuthEndpoint {
            ep,
            config: self.config.clone(),
        }
    }
}

pub struct BearerAuthEndpoint<E> {
    ep: E,
    config: Config,
}

impl<E> BearerAuthEndpoint<E> {
    fn auth_required(&self) -> bool {
        self.config.auth_token.is_some()
    }
}

impl<E: Endpoint> Endpoint for BearerAuthEndpoint<E> {
    type Output = Response;

    async fn call(&self, req: Request) -> Result<Self::Output> {
        let header_jwt = req
            .headers()
            .typed_get::<headers::Authorization<Bearer>>()
            .map(|it| it.0.token().to_owned());

        let cookie_jwt = req
            .cookie()
            .get("worterbuch_auth_jwt")
            .map(|c| c.value_str().to_owned());

        let jwt = header_jwt.or(cookie_jwt);

        let claims = get_claims(jwt.as_deref(), &self.config)?;

        let cors = Cors::new()
            .allow_credentials(true)
            .expose_header("Set-Cookie")
            .allow_origins(claims.cors.clone().unwrap_or_else(Vec::new));

        if self.auth_required() {
            (&self.ep)
                .with(AddData::<Option<JwtClaims>>::new(Some(claims)))
                .with(cors)
                .call(req)
                .await
        } else {
            (&self.ep)
                .with(AddData::<Option<JwtClaims>>::new(None))
                .with(cors)
                .call(req)
                .await
        }
    }
}
