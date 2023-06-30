use crate::{Config, Worterbuch};
use poem::http::StatusCode;
use poem::Result;
use poem::{listener::TcpListener, Route};
use poem_openapi::{
    param::Query,
    payload::{Form, Json},
    OpenApi, OpenApiService,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use worterbuch_common::error::WorterbuchError;
use worterbuch_common::{KeyValuePair, KeyValuePairs, RegularKeySegment};

struct Api {
    worterbuch: Arc<RwLock<Worterbuch>>,
}

#[OpenApi]
impl Api {
    #[oai(path = "/get", method = "get")]
    async fn get(&self, key: Query<String>) -> Result<Json<KeyValuePair>> {
        let wb = self.worterbuch.read().await;
        match wb.get(key.0) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pget", method = "get")]
    async fn pget(&self, pattern: Query<String>) -> Result<Json<KeyValuePairs>> {
        let wb = self.worterbuch.read().await;
        match wb.pget(&pattern.0) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/set", method = "post")]
    async fn set(&self, kvps: Form<HashMap<String, String>>) -> Result<Json<&'static str>> {
        let mut wb = self.worterbuch.write().await;
        println!("{kvps:?}");
        for (key, value) in kvps.0 {
            match wb.set(key, json!(value)) {
                Ok(()) => {}
                Err(e) => return to_error_response(e),
            }
        }
        Ok(Json("Ok"))
    }

    #[oai(path = "/delete", method = "post")]
    async fn delete(&self, key: Query<String>) -> Result<Json<KeyValuePair>> {
        let mut wb = self.worterbuch.write().await;
        match wb.delete(key.0) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pdelete", method = "post")]
    async fn pdelete(&self, pattern: Query<String>) -> Result<Json<KeyValuePairs>> {
        let mut wb = self.worterbuch.write().await;
        match wb.pdelete(pattern.0) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls", method = "get")]
    async fn ls(&self, pattern: Query<Option<String>>) -> Result<Json<Vec<RegularKeySegment>>> {
        let wb = self.worterbuch.read().await;
        match wb.ls(pattern.0) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }
}

fn to_error_response<T>(e: WorterbuchError) -> Result<Json<T>> {
    match e {
        WorterbuchError::IllegalMultiWildcard(_)
        | WorterbuchError::IllegalWildcard(_)
        | WorterbuchError::MultiWildcardAtIllegalPosition(_)
        | WorterbuchError::NoSuchValue(_)
        | WorterbuchError::ReadOnlyKey(_) => Err(poem::Error::new(e, StatusCode::BAD_REQUEST)),
        e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
    }
}

pub async fn start(
    worterbuch: Arc<RwLock<Worterbuch>>,
    config: Config,
) -> Result<(), std::io::Error> {
    let port = config.port;
    let bind_addr = config.bind_addr;

    let addr = format!("{bind_addr}:{port}");

    let api = Api { worterbuch };

    let host_addr = "http://localhost:8080/api";

    let api_service =
        OpenApiService::new(api, "Worterbuch", env!("CARGO_PKG_VERSION")).server(host_addr);

    log::info!("Starting openapi service at {}", host_addr);

    let openapi_explorer = api_service.openapi_explorer();
    let spec_json = api_service.spec_endpoint();
    let spec_yaml = api_service.spec_endpoint_yaml();

    let app = Route::new()
        .nest("/api", api_service)
        .nest("/doc", openapi_explorer)
        .nest("/json", spec_json)
        .nest("/yaml", spec_yaml);

    poem::Server::new(TcpListener::bind(addr)).run(app).await
}
