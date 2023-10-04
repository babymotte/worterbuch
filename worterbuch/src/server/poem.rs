use crate::{
    server::common::{process_incoming_message, CloneableWbApi},
    Config,
};
use anyhow::anyhow;
use futures::{
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use poem::{
    get, handler,
    http::StatusCode,
    listener::TcpListener,
    web::websocket::WebSocket,
    web::{
        websocket::{Message, WebSocketStream},
        Data, Query, Yaml,
    },
    EndpointExt, IntoResponse, Request, Result, Route,
};
use poem_openapi::{
    param::Path,
    payload::{Json, PlainText},
    OpenApi, OpenApiService,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    select, spawn,
    sync::mpsc,
    time::{sleep, MissedTickBehavior},
};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    quote, KeyValuePair, KeyValuePairs, ProtocolVersion, RegularKeySegment, ServerMessage,
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
    async fn get(
        &self,
        Path(key): Path<String>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Json<KeyValuePair>> {
        let pointer = params.get("extract");
        match self.worterbuch.get(key).await {
            Ok((key, value)) => {
                if let Some(pointer) = pointer {
                    let key = key + pointer;
                    let extracted = value.pointer(pointer);
                    if let Some(extracted) = extracted {
                        Ok(Json(KeyValuePair {
                            key,
                            value: extracted.to_owned(),
                        }))
                    } else {
                        to_error_response(WorterbuchError::NoSuchValue(key))
                    }
                } else {
                    Ok(Json(KeyValuePair { key, value }))
                }
            }
            Err(e) => to_error_response(e),
        }
    }

    #[oai(path = "/get/raw/:key", method = "get")]
    async fn get_raw(
        &self,
        Path(key): Path<String>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<PlainText<String>> {
        let pointer = params.get("extract");
        match self.worterbuch.get(key.clone()).await {
            Ok((_, value)) => {
                if let Some(pointer) = pointer {
                    let extracted = value.pointer(&pointer);
                    if let Some(value) = extracted {
                        match to_raw_string(value.to_owned()) {
                            Ok(text) => Ok(PlainText(text)),
                            Err(e) => to_error_response(e),
                        }
                    } else {
                        to_error_response(WorterbuchError::NoSuchValue(key + pointer))
                    }
                } else {
                    match to_raw_string(value) {
                        Ok(text) => Ok(PlainText(text)),
                        Err(e) => to_error_response(e),
                    }
                }
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
    let subsys = data.2.to_owned();
    let remote = *req
        .remote_addr()
        .as_socket_addr()
        .expect("Client has no remote address.");
    ws.protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            if let Err(e) = serve(remote, worterbuch, socket, proto_version, subsys).await {
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

    let swagger_ui = api_service.swagger_ui();
    let oapi_spec_json = api_service.spec_endpoint();
    let oapi_spec_yaml = api_service.spec_endpoint_yaml();

    let mut app = Route::new()
        .nest(api_path, api_service)
        .nest("/doc", swagger_ui)
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

async fn serve(
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
    proto_version: ProtocolVersion,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    worterbuch.connected(client_id, remote_addr).await?;

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

    if let Err(e) = serve_loop(
        client_id,
        remote_addr,
        worterbuch.clone(),
        websocket,
        proto_version,
        subsys,
    )
    .await
    {
        log::error!("Error in serve loop: {e}");
    }

    worterbuch.disconnected(client_id, remote_addr).await?;

    Ok(())
}

type WebSocketSender = SplitSink<WebSocketStream, poem::web::websocket::Message>;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
    proto_version: ProtocolVersion,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let config = worterbuch.config().await?;
    let send_timeout = config.send_timeout;
    let keepalive_timeout = config.keepalive_timeout;
    let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
    let mut last_keepalive_tx = Instant::now();
    let mut last_keepalive_rx = Instant::now();
    let mut handshake_complete = false;
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let (mut ws_tx, mut ws_rx) = websocket.split();
    let (ws_send_tx, mut ws_send_rx) = mpsc::channel(config.channel_buffer_size);
    let (keepalive_tx_tx, mut keepalive_tx_rx) = mpsc::unbounded_channel();

    // websocket send loop
    let subsys_send = subsys.clone();
    spawn(async move {
        while let Some(msg) = ws_send_rx.recv().await {
            send_with_timeout(
                msg,
                &mut ws_tx,
                send_timeout,
                &keepalive_tx_tx,
                &subsys_send,
            )
            .await;
        }
    });

    loop {
        select! {
            recv = ws_rx.next() => if let Some(msg) = recv {
                match msg {
                    Ok(incoming_msg) => {
                        last_keepalive_rx = Instant::now();
                        if let Message::Text(text) = incoming_msg {
                            let (msg_processed, handshake) = process_incoming_message(
                                client_id,
                                &text,
                                &worterbuch,
                                &ws_send_tx,
                                &proto_version,
                            )
                            .await?;
                            handshake_complete |= msg_processed && handshake;
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
                log::info!("WS stream of client {client_id} ({remote_addr}) closed.");
                break;
            },
            recv = keepalive_tx_rx.recv() => match recv {
                Some(keepalive) => last_keepalive_tx = keepalive?,
                None => break,
            },
            _ = keepalive_timer.tick() => {
                // check how long ago the last websocket message was received
                check_client_keepalive(last_keepalive_rx, last_keepalive_tx, handshake_complete, client_id, keepalive_timeout)?;
                // send out websocket message if the last has been more than a second ago
                send_keepalive(last_keepalive_tx, &ws_send_tx, ).await?;
            }
        }
    }

    Ok(())
}

async fn send_keepalive(
    last_keepalive_tx: Instant,
    ws_send_tx: &mpsc::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    if last_keepalive_tx.elapsed().as_secs() >= 1 {
        log::trace!("Sending keepalive");
        ws_send_tx.send(ServerMessage::Keepalive).await?;
    }
    Ok(())
}

fn check_client_keepalive(
    last_keepalive_rx: Instant,
    last_keepalive_tx: Instant,
    handshake_complete: bool,
    client_id: Uuid,
    keepalive_timeout: Duration,
) -> anyhow::Result<()> {
    let lag = last_keepalive_rx - last_keepalive_tx;

    if handshake_complete && lag >= Duration::from_secs(2) {
        log::warn!(
            "Client {} has been inactive for {} seconds …",
            client_id,
            lag.as_secs()
        );
    }

    if handshake_complete && lag >= keepalive_timeout {
        log::warn!(
            "Client {} has been inactive for too long. Disconnecting.",
            client_id
        );
        Err(anyhow!("Client has been inactive for too long"))
    } else {
        Ok(())
    }
}

async fn send_with_timeout(
    msg: ServerMessage,
    websocket: &mut WebSocketSender,
    send_timeout: Duration,
    result_handler: &mpsc::UnboundedSender<anyhow::Result<Instant>>,
    subsys: &SubsystemHandle,
) {
    let json = match serde_json::to_string(&msg) {
        Ok(it) => it,
        Err(e) => {
            handle_encode_error(e, subsys);
            return;
        }
    };

    let msg = Message::Text(json);

    select! {
        r = websocket.send(msg) => {
            if let Err(e) = r {
                result_handler.send(Err(e.into())).ok();
            } else {
                result_handler.send(Ok(Instant::now())).ok();
            }
        },
        _ = sleep(send_timeout) => {
            log::error!("Send timeout");
            result_handler.send(Err(anyhow!("Send timeout"))).ok();
        },
    }
}

fn handle_encode_error(e: serde_json::Error, subsys: &SubsystemHandle) {
    log::error!("Failed to encode a value to JSON: {e}");
    subsys.request_global_shutdown();
}

fn to_raw_string(value: Value) -> WorterbuchResult<String> {
    match value {
        Value::String(text) => Ok(text),
        other => {
            let text = serde_json::to_string(&other).context(|| "invalid json".to_owned())?;
            Ok(text)
        }
    }
}
