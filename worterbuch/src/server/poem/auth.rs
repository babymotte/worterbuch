use poem::{
    http::StatusCode,
    web::headers::{self, authorization::Bearer, HeaderMapExt},
    Endpoint, Error, Middleware, Request, Result,
};

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
        if self.wb.authenticate(auth_token).await.is_ok() {
            self.ep.call(req).await
        } else {
            Err(Error::from_status(StatusCode::UNAUTHORIZED))
        }
    }
}
