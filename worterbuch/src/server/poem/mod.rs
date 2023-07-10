use crate::{server::common::process_incoming_message, Config, Worterbuch};
use futures::{sink::SinkExt, stream::StreamExt};
use poem::{
    get, handler,
    http::StatusCode,
    listener::TcpListener,
    web::websocket::WebSocket,
    web::{
        websocket::{Message, WebSocketStream},
        Data, Yaml,
    },
    EndpointExt, IntoResponse, Request, Result, Route,
};
use poem_openapi::{param::Path, payload::Json, OpenApi, OpenApiService};
use serde_json::Value;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    spawn,
    sync::{mpsc, RwLock},
};
use uuid::Uuid;
use worterbuch_common::{
    error::WorterbuchError, quote, KeyValuePair, KeyValuePairs, RegularKeySegment,
};

const ASYNC_API_YAML: &'static str = include_str!("../../../../worterbuch-common/asyncapi.yaml");

struct Api {
    worterbuch: Arc<RwLock<Worterbuch>>,
}

#[OpenApi]
impl Api {
    #[oai(path = "/get/:key", method = "get")]
    async fn get(&self, key: Path<String>) -> Result<Json<KeyValuePair>> {
        let wb = self.worterbuch.read().await;
        match wb.get(key.0) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pget/:pattern", method = "get")]
    async fn pget(&self, pattern: Path<String>) -> Result<Json<KeyValuePairs>> {
        let wb = self.worterbuch.read().await;
        match wb.pget(&pattern.0) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/set/:key", method = "post")]
    async fn set(&self, key: Path<String>, value: Json<Value>) -> Result<Json<&'static str>> {
        let mut wb = self.worterbuch.write().await;
        match wb.set(key.0, value.0) {
            Ok(()) => {}
            Err(e) => return to_error_response(e),
        }
        Ok(Json("Ok"))
    }

    #[oai(path = "/delete/:key", method = "delete")]
    async fn delete(&self, key: Path<String>) -> Result<Json<KeyValuePair>> {
        let mut wb = self.worterbuch.write().await;
        match wb.delete(key.0) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pdelete/:pattern", method = "delete")]
    async fn pdelete(&self, pattern: Path<String>) -> Result<Json<KeyValuePairs>> {
        let mut wb = self.worterbuch.write().await;
        match wb.pdelete(pattern.0) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls/:key", method = "get")]
    async fn ls(&self, key: Path<String>) -> Result<Json<Vec<RegularKeySegment>>> {
        let wb = self.worterbuch.read().await;
        match wb.ls(Some(key.0)) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls", method = "get")]
    async fn ls_root(&self) -> Result<Json<Vec<RegularKeySegment>>> {
        let wb = self.worterbuch.read().await;
        match wb.ls(None) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }
}

fn to_error_response<T>(e: WorterbuchError) -> Result<T> {
    match e {
        WorterbuchError::IllegalMultiWildcard(_)
        | WorterbuchError::IllegalWildcard(_)
        | WorterbuchError::MultiWildcardAtIllegalPosition(_)
        | WorterbuchError::NoSuchValue(_)
        | WorterbuchError::ReadOnlyKey(_) => Err(poem::Error::new(e, StatusCode::BAD_REQUEST)),
        e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
    }
}

#[handler]
async fn ws(
    ws: WebSocket,
    worterbuch: Data<&Arc<RwLock<Worterbuch>>>,
    req: &Request,
) -> impl IntoResponse {
    let wb: Arc<RwLock<Worterbuch>> = worterbuch.clone();
    let remote = *req
        .remote_addr()
        .as_socket_addr()
        .expect("Client has no remote address.");
    ws.protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            if let Err(e) = serve(socket, wb, remote).await {
                log::error!("Error in WS connection: {e}");
            }
        })
}

#[handler]
fn asyncapi_spec_yaml(data: Data<&String>) -> Yaml<serde_yaml::Value> {
    let yaml_string = ASYNC_API_YAML.replace("${SERVER_URL}", &quote(&data.0));
    let value = serde_yaml::from_str(&yaml_string).expect("cannot fail");
    Yaml(value)
}

pub async fn start(
    worterbuch: Arc<RwLock<Worterbuch>>,
    config: Config,
) -> Result<(), std::io::Error> {
    let port = config.port;
    let bind_addr = config.bind_addr;
    let public_addr = config.public_address;
    let proto = config.proto;

    let addr = format!("{bind_addr}:{port}");

    let api = Api {
        worterbuch: worterbuch.clone(),
    };

    let public_url = &format!("http://{public_addr}:{port}/openapi");

    let api_service =
        OpenApiService::new(api, "Worterbuch", env!("CARGO_PKG_VERSION")).server(public_url);

    log::info!("Starting openapi service at {}", public_url);

    let openapi_explorer = api_service.openapi_explorer();
    let oapi_spec_json = api_service.spec_endpoint();
    let oapi_spec_yaml = api_service.spec_endpoint_yaml();

    let app = Route::new()
        .nest("/openapi", api_service)
        .nest("/doc", openapi_explorer)
        .nest("/openapi/json", oapi_spec_json)
        .nest("/openapi/yaml", oapi_spec_yaml)
        .nest(
            "/asyncapi/yaml",
            get(asyncapi_spec_yaml.data(format!("{proto}://{public_addr}:{port}"))),
        )
        .nest("/ws", get(ws.data(worterbuch)));

    poem::Server::new(TcpListener::bind(addr)).run(app).await
}

async fn serve(
    websocket: WebSocketStream,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: SocketAddr,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    let (mut client_write, mut client_read) = websocket.split();

    {
        let mut wb = worterbuch.write().await;
        wb.connected(client_id, remote_addr);
    }

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            let msg = Message::text(bytes);
            if let Err(e) = client_write.send(msg).await {
                log::error!("Error sending message to client {client_id} ({remote_addr}): {e}");
                break;
            }
        }
    });

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …");

    loop {
        if let Some(Ok(incoming_msg)) = client_read.next().await {
            if let Message::Text(data) = incoming_msg {
                if !process_incoming_message(client_id, &data, worterbuch.clone(), tx.clone())
                    .await?
                {
                    break;
                }
            }
        } else {
            break;
        }
    }

    log::info!("WS stream of client {client_id} ({remote_addr}) closed.");

    let mut wb = worterbuch.write().await;
    wb.disconnected(client_id, remote_addr);

    Ok(())
}
