use crate::{Config, Worterbuch};
use poem::http::StatusCode;
use poem::Result;
use poem::{listener::TcpListener, Route};
use poem_openapi::{param::Query, payload::PlainText, OpenApi, OpenApiService};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;
use worterbuch_common::error::WorterbuchError;
use worterbuch_common::KeyValuePair;

struct Api {
    worterbuch: Arc<RwLock<Worterbuch>>,
}

#[OpenApi]
impl Api {
    #[oai(path = "/get", method = "get")]
    async fn get(&self, key: Query<String>) -> Result<PlainText<String>> {
        let wb = self.worterbuch.read().await;
        match wb.get(key.0) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                let json = serde_json::to_string(&kvp).expect("cannot fail");
                Ok(PlainText(json))
            }
            Err(e) => match e {
                WorterbuchError::IllegalMultiWildcard(_)
                | WorterbuchError::IllegalWildcard(_)
                | WorterbuchError::MultiWildcardAtIllegalPosition(_)
                | WorterbuchError::NoSuchValue(_)
                | WorterbuchError::ReadOnlyKey(_) => {
                    Err(poem::Error::new(e, StatusCode::BAD_REQUEST))
                }
                e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
            },
        }
    }

    #[oai(path = "/set", method = "post")]
    async fn set(&self, key: Query<String>, value: Query<String>) -> Result<PlainText<String>> {
        let mut wb = self.worterbuch.write().await;
        match wb.set(key.0, json!(value.0)) {
            Ok(()) => Ok(PlainText("Ok".to_owned())),
            Err(e) => match e {
                WorterbuchError::IllegalMultiWildcard(_)
                | WorterbuchError::IllegalWildcard(_)
                | WorterbuchError::MultiWildcardAtIllegalPosition(_)
                | WorterbuchError::NoSuchValue(_)
                | WorterbuchError::ReadOnlyKey(_) => {
                    Err(poem::Error::new(e, StatusCode::BAD_REQUEST))
                }
                e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
            },
        }
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

    let api_service = OpenApiService::new(api, "Worterbuch", env!("CARGO_PKG_VERSION"))
        .server("http://localhost:8080/api");

    let swagger = api_service.swagger_ui();
    let spec_json = api_service.spec_endpoint();
    let spec_yaml = api_service.spec_endpoint_yaml();

    let app = Route::new()
        .nest("/api", api_service)
        .nest("/doc", swagger)
        .nest("/json", spec_json)
        .nest("/yaml", spec_yaml);

    poem::Server::new(TcpListener::bind(addr)).run(app).await
}
