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

use poem::{
    http::StatusCode,
    web::headers::{self, authorization::Bearer, HeaderMapExt},
    Endpoint, Error, Middleware, Request, Result,
};
use worterbuch_common::ErrorCode;

use crate::server::common::CloneableWbApi;

pub(crate) struct BearerAuth {
    wb: CloneableWbApi,
}

impl BearerAuth {
    pub fn new(wb: CloneableWbApi) -> Self {
        Self { wb }
    }
}

impl<E: Endpoint> Middleware<E> for BearerAuth {
    type Output = BearerAuthEndpoint<E>;

    fn transform(&self, ep: E) -> Self::Output {
        BearerAuthEndpoint {
            ep,
            wb: self.wb.clone(),
        }
    }
}

pub(crate) struct BearerAuthEndpoint<E> {
    ep: E,
    wb: CloneableWbApi,
}

#[poem::async_trait]
impl<E: Endpoint> Endpoint for BearerAuthEndpoint<E> {
    type Output = E::Output;

    async fn call(&self, req: Request) -> Result<Self::Output> {
        let auth_token = req
            .headers()
            .typed_get::<headers::Authorization<Bearer>>()
            .map(|it| it.0.token().to_owned());
        if self.wb.authenticate(auth_token, None).await.is_ok() {
            self.ep.call(req).await
        } else {
            let mut err = Error::from_status(StatusCode::UNAUTHORIZED);
            err.set_error_message("client failed to authenticate");
            err.set_data(ErrorCode::AuthenticationFailed);
            Err(err)
        }
    }
}
