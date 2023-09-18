use crate::{
    server::common::{process_incoming_message, CloneableWbApi},
    Config,
};
use anyhow::anyhow;
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
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::mpsc,
    time::{sleep, MissedTickBehavior},
};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::{
    error::WorterbuchError, quote, KeyValuePair, KeyValuePairs, ProtocolVersion, RegularKeySegment,
    ServerMessage,
};

const ASYNC_API_YAML: &'static str = include_str!("../../asyncapi.yaml");
const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Api {
    worterbuch: CloneableWbApi,
    _subsys: SubsystemHandle,
}

#[OpenApi]
impl Api {
    #[oai(path = "/get/:key", method = "get")]
    async fn get(&self, Path(key): Path<String>) -> Result<Json<KeyValuePair>> {
        match self.worterbuch.get(key).await {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pget/:pattern", method = "get")]
    async fn pget(&self, Path(pattern): Path<String>) -> Result<Json<KeyValuePairs>> {
        match self.worterbuch.pget(pattern).await {
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
        match self.worterbuch.set(key, value).await {
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
        match self.worterbuch.publish(key, value).await {
            Ok(()) => {}
            Err(e) => return to_error_response(e),
        }
        Ok(Json("Ok"))
    }

    #[oai(path = "/delete/:key", method = "delete")]
    async fn delete(&self, Path(key): Path<String>) -> Result<Json<KeyValuePair>> {
        match self.worterbuch.delete(key).await {
            Ok(kvp) => {
                let kvp: KeyValuePair = kvp.into();
                Ok(Json(kvp))
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/pdelete/:pattern", method = "delete")]
    async fn pdelete(&self, Path(pattern): Path<String>) -> Result<Json<KeyValuePairs>> {
        match self.worterbuch.pdelete(pattern).await {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls/:key", method = "get")]
    async fn ls(&self, Path(key): Path<String>) -> Result<Json<Vec<RegularKeySegment>>> {
        match self.worterbuch.ls(Some(key)).await {
            Ok(kvps) => Ok(Json(kvps)),
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/ls", method = "get")]
    async fn ls_root(&self) -> Result<Json<Vec<RegularKeySegment>>> {
        match self.worterbuch.ls(None).await {
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
    Data(data): Data<&(CloneableWbApi, ProtocolVersion, SubsystemHandle)>,
    req: &Request,
) -> impl IntoResponse {
    log::info!("Client connected");
    let worterbuch = data.0.clone();
    let proto_version = data.1.to_owned();
    let remote = *req
        .remote_addr()
        .as_socket_addr()
        .expect("Client has no remote address.");
    ws.protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            let mut client_handler =
                ClientHandler::new(socket, worterbuch, remote, proto_version).await;
            if let Err(e) = client_handler.serve().await {
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
    worterbuch: CloneableWbApi,
    config: Config,
    subsys: SubsystemHandle,
) -> Result<(), std::io::Error> {
    let port = config.port;
    let bind_addr = config.bind_addr;
    let public_addr = config.public_address;
    let proto = config.proto;
    let proto_versions = worterbuch
        .supported_protocol_versions()
        .await
        .unwrap_or(Vec::new());

    let addr = format!("{bind_addr}:{port}");

    let api = Api {
        worterbuch: worterbuch.clone(),
        _subsys: subsys.clone(),
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
                subsys.clone(),
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

    poem::Server::new(TcpListener::bind(addr))
        .run_with_graceful_shutdown(
            app,
            subsys.on_shutdown_requested(),
            Some(Duration::from_secs(1)),
        )
        .await
}

struct ClientHandler {
    client_id: Uuid,
    handshake_complete: bool,
    last_keepalive_rx: Instant,
    last_keepalive_tx: Instant,
    keepalive_timeout: Duration,
    send_timeout: Duration,
    websocket: WebSocketStream,
    worterbuch: CloneableWbApi,
    remote_addr: SocketAddr,
    proto_version: ProtocolVersion,
}

impl ClientHandler {
    async fn new(
        websocket: WebSocketStream,
        worterbuch: CloneableWbApi,
        remote_addr: SocketAddr,
        proto_version: ProtocolVersion,
    ) -> Self {
        let config = worterbuch.config().await.ok();
        let (keepalive_timeout, send_timeout) = config
            .map(|c| (c.keepalive_timeout, c.send_timeout))
            .unwrap_or((Duration::from_secs(10), Duration::from_secs(10)));
        Self {
            client_id: Uuid::new_v4(),
            handshake_complete: false,
            last_keepalive_rx: Instant::now(),
            last_keepalive_tx: Instant::now(),
            keepalive_timeout,
            send_timeout,
            proto_version,
            remote_addr,
            websocket,
            worterbuch,
        }
    }

    async fn serve(&mut self) -> anyhow::Result<()> {
        let client_id = self.client_id;
        let remote_addr = self.remote_addr;

        log::info!("New client connected: {client_id} ({remote_addr})");

        self.worterbuch.connected(client_id, remote_addr).await?;

        log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

        if let Err(e) = self.serve_loop().await {
            log::error!("Error in serve loop: {e}");
        }

        self.worterbuch.disconnected(client_id, remote_addr).await?;

        Ok(())
    }

    async fn serve_loop(&mut self) -> anyhow::Result<()> {
        let client_id = self.client_id.clone();
        let remote_addr = self.remote_addr;
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
        keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            select! {
                recv = self.websocket.next() => if let Some(msg) = recv {
                    match msg {
                        Ok(incoming_msg) => {
                            self.last_keepalive_rx = Instant::now();
                            if let Message::Text(text) = incoming_msg {
                                let (msg_processed, handshake) = process_incoming_message(
                                    self.client_id,
                                    &text,
                                    &mut self.worterbuch,
                                    tx.clone(),
                                    &self.proto_version,
                                )
                                .await?;
                                self.handshake_complete |= msg_processed && handshake;
                                if !msg_processed {
                                    break;
                                }
                            }
                        },
                        Err(e) => {
                            log::error!("Error in WebSocket connection: {e}");
                            break;
                        }
                    }
                } else {
                    log::warn!("WS stream of client {client_id} ({remote_addr}) closed.");
                    break;
                },
                recv = rx.recv() => if let Some(text) = recv {
                    let msg = Message::text(text);
                    self.send_with_timeout(msg).await?;
                } else {
                    break;
                },
                _ = keepalive_timer.tick() => {
                    // check how long ago the last websocket message was received
                    self.check_client_keepalive()?;
                    // send out websocket message if the last has been more than a second ago
                    self.send_keepalive().await?;
                }
            }
        }

        Ok(())
    }

    async fn send_keepalive(&mut self) -> anyhow::Result<()> {
        if self.last_keepalive_tx.elapsed().as_secs() >= 1 {
            log::trace!("Sending keepalive");
            let json = serde_json::to_string(&ServerMessage::Keepalive)?;
            let msg = Message::Text(json);
            self.send_with_timeout(msg).await?;
        }

        Ok(())
    }

    fn check_client_keepalive(&mut self) -> anyhow::Result<()> {
        let lag = self.last_keepalive_rx - self.last_keepalive_tx;

        if self.handshake_complete && lag >= Duration::from_secs(2) {
            log::warn!(
                "Client {} has been inactive for {} seconds …",
                self.client_id,
                lag.as_secs()
            );
        }

        if self.handshake_complete && lag >= self.keepalive_timeout {
            log::warn!(
                "Client {} has been inactive for too long. Disconnecting.",
                self.client_id
            );
            Err(anyhow!("Client has been inactive for too long"))
        } else {
            Ok(())
        }
    }

    async fn send_with_timeout(&mut self, msg: Message) -> anyhow::Result<()> {
        select! {
            r = self.websocket.send(msg) => {
                self.last_keepalive_tx = Instant::now();
                Ok(r?)
            },
            _ = sleep(self.send_timeout) => {
                log::error!("Send timeout");
                Err(anyhow!("Send timeout"))
            },
        }
    }
}
