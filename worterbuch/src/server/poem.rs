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
use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{mpsc, RwLock},
    time::sleep,
};
use uuid::Uuid;
use worterbuch_common::{
    error::WorterbuchError, quote, KeyValuePair, KeyValuePairs, ProtocolVersion, RegularKeySegment,
    ServerMessage,
};

const ASYNC_API_YAML: &'static str = include_str!("../../asyncapi.yaml");
const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Api {
    worterbuch: Arc<RwLock<Worterbuch>>,
}

#[OpenApi]
impl Api {
    #[oai(path = "/get/:key", method = "get")]
    async fn get(&self, Path(key): Path<String>) -> Result<Json<KeyValuePair>> {
        let wb = self.worterbuch.read().await;
        match wb.get(&key) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pget/:pattern", method = "get")]
    async fn pget(&self, Path(pattern): Path<String>) -> Result<Json<KeyValuePairs>> {
        let wb = self.worterbuch.read().await;
        match wb.pget(&pattern) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/set/:key", method = "post")]
    async fn set(
        &self,
        Path(key): Path<String>,
        Json(value): Json<Value>,
    ) -> Result<Json<&'static str>> {
        let mut wb = self.worterbuch.write().await;
        match wb.set(key, value) {
            Ok(()) => {}
            Err(e) => return to_error_response(e),
        }
        Ok(Json("Ok"))
    }

    #[oai(path = "/publish/:key", method = "post")]
    async fn publish(
        &self,
        Path(key): Path<String>,
        Json(value): Json<Value>,
    ) -> Result<Json<&'static str>> {
        let mut wb = self.worterbuch.write().await;
        match wb.publish(key, value) {
            Ok(()) => {}
            Err(e) => return to_error_response(e),
        }
        Ok(Json("Ok"))
    }

    #[oai(path = "/delete/:key", method = "delete")]
    async fn delete(&self, Path(key): Path<String>) -> Result<Json<KeyValuePair>> {
        let mut wb = self.worterbuch.write().await;
        match wb.delete(key) {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pdelete/:pattern", method = "delete")]
    async fn pdelete(&self, Path(pattern): Path<String>) -> Result<Json<KeyValuePairs>> {
        let mut wb = self.worterbuch.write().await;
        match wb.pdelete(pattern) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls/:key", method = "get")]
    async fn ls(&self, Path(key): Path<String>) -> Result<Json<Vec<RegularKeySegment>>> {
        let wb = self.worterbuch.read().await;
        match wb.ls(&Some(key)) {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls", method = "get")]
    async fn ls_root(&self) -> Result<Json<Vec<RegularKeySegment>>> {
        let wb = self.worterbuch.read().await;
        match wb.ls(&None) {
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
    Data(data): Data<&(Arc<RwLock<Worterbuch>>, ProtocolVersion)>,
    req: &Request,
) -> impl IntoResponse {
    let worterbuch = &data.0;
    let proto_version = data.1.to_owned();
    let wb: Arc<RwLock<Worterbuch>> = worterbuch.clone();
    let remote = *req
        .remote_addr()
        .as_socket_addr()
        .expect("Client has no remote address.");
    ws.protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            if let Err(e) = serve(socket, wb, remote, proto_version).await {
                log::error!("Error in WS connection: {e}");
            }
        })
}

#[handler]
fn asyncapi_spec_yaml(Data((server_url, api_version)): Data<&(String, String)>) -> Yaml<Value> {
    Yaml(async_api(server_url, api_version))
}

#[handler]
fn asyncapi_spec_json(Data((server_url, api_version)): Data<&(String, String)>) -> Json<Value> {
    Json(async_api(server_url, api_version))
}

fn async_api(server_url: &str, api_version: &str) -> Value {
    let (admin_name, admin_url, admin_email) = admin_data();

    let yaml_string = ASYNC_API_YAML
        .replace("${WS_SERVER_URL}", &quote(&server_url))
        .replace("${API_VERSION}", &quote(&api_version))
        .replace("${WORTERBUCH_VERSION}", VERSION)
        .replace("${WORTERBUCH_ADMIN_NAME}", &admin_name)
        .replace("${WORTERBUCH_ADMIN_URL}", &admin_url)
        .replace("${WORTERBUCH_ADMIN_EMAIL}", &admin_email);
    serde_yaml::from_str(&yaml_string).expect("cannot fail")
}

fn admin_data() -> (String, String, String) {
    let admin_name = env::var("WORTERBUCH_ADMIN_NAME").unwrap_or("<admin name>".to_owned());
    let admin_url = env::var("WORTERBUCH_ADMIN_URL").unwrap_or("<admin url>".to_owned());
    let admin_email = env::var("WORTERBUCH_ADMIN_EMAIL").unwrap_or("<admin email>".to_owned());
    (admin_name, admin_url, admin_email)
}

pub async fn start(
    worterbuch: Arc<RwLock<Worterbuch>>,
    config: Config,
) -> Result<(), std::io::Error> {
    let port = config.port;
    let bind_addr = config.bind_addr;
    let public_addr = config.public_address;
    let proto = config.proto;
    let proto_versions = {
        let wb = worterbuch.read().await;
        wb.supported_protocol_versions()
    };

    let addr = format!("{bind_addr}:{port}");

    let api = Api {
        worterbuch: worterbuch.clone(),
    };

    let api_path = "/api";
    let public_url = &format!("http://{public_addr}:{port}{api_path}");

    let api_service =
        OpenApiService::new(api, "Worterbuch", env!("CARGO_PKG_VERSION")).server(public_url);

    log::info!("Starting openapi service at {}", public_url);

    let openapi_explorer = api_service.openapi_explorer();
    let oapi_spec_json = api_service.spec_endpoint();
    let oapi_spec_yaml = api_service.spec_endpoint_yaml();

    let mut app = Route::new()
        .nest(api_path, api_service)
        .nest("/doc", openapi_explorer)
        .nest("/api/json", oapi_spec_json)
        .nest("/api/yaml", oapi_spec_yaml)
        .nest(
            format!("/ws"),
            get(ws.data((
                worterbuch.clone(),
                proto_versions
                    .iter()
                    .last()
                    .expect("cannot be none")
                    .to_owned(),
            ))),
        );

    for proto_ver in proto_versions {
        let spec_data = (
            format!("{proto}://{public_addr}:{port}/ws"),
            format!("{proto_ver}"),
        );
        app = app
            .nest(
                format!("/asyncapi/{proto_ver}/yaml"),
                get(asyncapi_spec_yaml.data(spec_data.clone())),
            )
            .nest(
                format!("/asyncapi/{proto_ver}/json"),
                get(asyncapi_spec_json.data(spec_data)),
            )
            .nest(
                format!("/ws/{proto_ver}"),
                get(ws.data((worterbuch.clone(), proto_ver.to_owned()))),
            );
        log::info!(
            "Serving asyncapi json at http://{public_addr}:{port}/asyncapi/{proto_ver}/json"
        );
        log::info!(
            "Serving asyncapi yaml at http://{public_addr}:{port}/asyncapi/{proto_ver}/yaml"
        );
        log::info!("Serving ws endpoint at {proto}://{public_addr}:{port}/ws/{proto_ver}");
    }

    poem::Server::new(TcpListener::bind(addr)).run(app).await
}

async fn serve(
    mut websocket: WebSocketStream,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: SocketAddr,
    proto_version: ProtocolVersion,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    {
        let mut wb = worterbuch.write().await;
        wb.connected(client_id, remote_addr);
    }

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …");

    let mut handshake_complete = false;
    let mut last_keepalive_rx = Instant::now();
    let mut last_keepalive_tx = Instant::now();

    loop {
        select! {
            recv = websocket.next() => if let Some(Ok(incoming_msg)) = recv {
                last_keepalive_rx = Instant::now();
                if last_keepalive_tx.elapsed().as_secs() >= 1 {
                    last_keepalive_tx = Instant::now();
                    if let Err(e) = send_keepalive(&mut websocket).await {
                        log::error!("Error sending keepalive signal: {e}");
                        break;
                    }
                }
                if let Message::Text(text) = incoming_msg {
                    let (msg_processed, handshake) = process_incoming_message(
                        client_id,
                        &text,
                        worterbuch.clone(),
                        tx.clone(),
                        &proto_version,
                    )
                    .await?;
                    handshake_complete |= msg_processed && handshake;
                    if !msg_processed
                    {
                        break;
                    }
                }
            } else {
                log::warn!("WS stream of client {client_id} ({remote_addr}) closed.");
                break;
            },
            recv = rx.recv() => if let Some(text) = recv {
                if handshake_complete && last_keepalive_rx.elapsed().as_secs() >= 2 {
                    log::warn!("Client has been inactive for too long. Disconnecting.");
                    break;
                }
                last_keepalive_tx = Instant::now();
                let msg = Message::text(text);
                if let Err(e) = websocket.send(msg).await {
                    log::error!("Error sending message to client {client_id} ({remote_addr}): {e}");
                    break;
                }
            } else {
                break;
            },
            _ = sleep(Duration::from_secs(1)) => {
                if handshake_complete && last_keepalive_rx.elapsed().as_secs() >= 2 {
                    log::warn!("Client has been inactive for too long. Disconnecting.");
                    break;
                }
                if last_keepalive_tx.elapsed().as_secs() >= 1 {
                    last_keepalive_tx = Instant::now();
                    if let Err(e) = send_keepalive(&mut websocket).await {
                        log::error!("Error sending keepalive signal: {e}");
                        break;
                    }
                }
            }
        }
    }

    let mut wb = worterbuch.write().await;
    wb.disconnected(client_id, remote_addr);

    Ok(())
}

async fn send_keepalive(websocket: &mut WebSocketStream) -> anyhow::Result<()> {
    log::trace!("Sending keepalive");
    let json = serde_json::to_string(&ServerMessage::Keepalive)?;
    let msg = Message::Text(json);
    websocket.send(msg).await?;
    Ok(())
}
