use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
#[cfg(feature = "jemalloc")]
use axum::{
    body::Body,
    http::{HeaderValue, Response},
};
use miette::{Context, IntoDiagnostic, Result};
use serde_json::json;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::{
    net::TcpListener,
    select,
    sync::{mpsc, oneshot},
};
use tosub::Subsystem;
#[cfg(feature = "jemalloc")]
use tower_http::cors::Any;
#[cfg(feature = "jemalloc")]
use tower_http::cors::CorsLayer;
use tracing::{Level, info, instrument};

pub struct StatsSender(mpsc::Sender<StatsEvent>);

impl StatsSender {
    pub async fn candidate(&self) {
        self.0
            .send(StatsEvent::Election(ElectionState::Candidate))
            .await
            .ok();
    }

    pub async fn follower(&self) {
        self.0
            .send(StatsEvent::Election(ElectionState::Follower))
            .await
            .ok();
    }

    pub async fn leader(&self) {
        self.0
            .send(StatsEvent::Election(ElectionState::Leader))
            .await
            .ok();
    }
}

#[derive(Debug, Clone)]
enum ElectionState {
    Candidate,
    Leader,
    Follower,
}

#[derive(Debug)]
enum StatsEvent {
    Election(ElectionState),
}

#[derive(Debug)]
enum StatsApiMessage {
    ElectionState(oneshot::Sender<ElectionState>),
}

#[derive(Clone)]
struct Server {
    api_tx: mpsc::Sender<StatsApiMessage>,
}

impl Server {
    #[instrument(level=Level::TRACE, skip(server), ret)]
    async fn ready(State(server): State<Server>) -> impl IntoResponse {
        let (tx, rx) = oneshot::channel();
        server
            .api_tx
            .send(StatsApiMessage::ElectionState(tx))
            .await
            .ok();
        let state = if let Ok(it) = rx.await {
            it
        } else {
            ElectionState::Candidate
        };

        match state {
            ElectionState::Candidate => (StatusCode::OK, Json(json!("candidate"))),
            ElectionState::Leader => (StatusCode::OK, Json(json!("leader"))),
            ElectionState::Follower => (StatusCode::SERVICE_UNAVAILABLE, Json(json!("follower"))),
        }
    }

    #[cfg(feature = "jemalloc")]
    #[instrument(level=Level::INFO)]
    async fn get_heap_files_list() -> Result<Response<Body>, (StatusCode, String)> {
        use worterbuch_common::profiling::list_heap_profile_files;

        let files = list_heap_profile_files().await.unwrap_or_else(Vec::new);

        Ok(Json(files).into_response())
    }

    #[cfg(feature = "jemalloc")]
    #[instrument(level=Level::INFO)]
    async fn get_live_heap() -> Result<Response<Body>, (StatusCode, String)> {
        use worterbuch_common::profiling::get_live_heap_profile;

        let pprof = get_live_heap_profile()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let mut response = pprof.into_response();

        response.headers_mut().insert(
            "Content-Disposition",
            HeaderValue::from_static("attachment; filename=heap.pb.gz"),
        );

        Ok(response)
    }

    #[cfg(feature = "jemalloc")]
    #[instrument(level=Level::INFO)]
    async fn get_live_flamegraph() -> Result<Response<Body>, (StatusCode, String)> {
        use worterbuch_common::profiling::get_live_flamegraph;

        let pprof = get_live_flamegraph()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let mut response = pprof.into_response();

        response
            .headers_mut()
            .insert("Content-Type", HeaderValue::from_static("image/svg+xml"));

        Ok(response)
    }

    #[cfg(feature = "jemalloc")]
    #[instrument(level=Level::INFO)]
    async fn get_heap_file(file: String) -> Result<Response<Body>, (StatusCode, String)> {
        use worterbuch_common::profiling::get_heap_profile_from_file;

        match get_heap_profile_from_file(&file)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        {
            Some(pprof) => {
                let mut response = pprof.into_response();

                if let Ok(val) = HeaderValue::from_str(&format!("attachment; filename={file}.gz")) {
                    response.headers_mut().insert("Content-Disposition", val);
                }

                Ok(response)
            }
            None => Ok((StatusCode::NOT_FOUND, "not found").into_response()),
        }
    }

    #[cfg(feature = "jemalloc")]
    #[instrument(level=Level::INFO)]
    async fn get_flamegraph_file(file: String) -> Result<Response<Body>, (StatusCode, String)> {
        use worterbuch_common::profiling::get_flamegraph_from_file;

        match get_flamegraph_from_file(&file)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        {
            Some(svg) => {
                let mut response = svg.into_response();

                response
                    .headers_mut()
                    .insert("Content-Type", HeaderValue::from_static("image/svg+xml"));

                Ok(response)
            }
            None => Ok((StatusCode::NOT_FOUND, "not found").into_response()),
        }
    }
}

pub async fn start_stats_endpoint(subsys: &Subsystem, port: u16) -> Result<StatsSender> {
    let (evt_tx, evt_rx) = mpsc::channel(1);
    let (api_tx, api_rx) = mpsc::channel(1);

    StatsActor::start_stats_actor(subsys, evt_rx, api_rx);

    subsys.spawn("stats-endpoint", async move |s| {
        run_server(s, api_tx, port).await
    });

    Ok(StatsSender(evt_tx))
}

async fn run_server(
    subsys: Subsystem,
    api_tx: mpsc::Sender<StatsApiMessage>,
    port: u16,
) -> Result<()> {
    let server = Server { api_tx };

    let mut app = Router::new();
    app = app.route("/ready", get(Server::ready));

    #[cfg(feature = "jemalloc")]
    if std::env::var("MALLOC_CONF")
        .unwrap_or("".into())
        .contains("prof_active:true")
    {
        app = app
            .route(
                "/debug/heap/list",
                get(Server::get_heap_files_list).layer(cors()),
            )
            .route("/debug/heap/live", get(Server::get_live_heap).layer(cors()))
            .route(
                "/debug/heap/file/{file}",
                get(Server::get_heap_file).layer(cors()),
            )
            .route(
                "/debug/flamegraph/live",
                get(Server::get_live_flamegraph).layer(cors()),
            )
            .route(
                "/debug/flamegraph/file/{file}",
                get(Server::get_flamegraph_file).layer(cors()),
            )
    }

    let ip = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let addr = SocketAddr::new(ip, port);

    info!("Starting stats endpoint at {addr} …");

    let listener = TcpListener::bind(addr)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("stats endpoint could not bind to socket address {addr}"))?;

    let shutdown_token = subsys.clone();
    let signal = async move {
        shutdown_token.shutdown_requested().await;
    };

    axum::serve(listener, app.with_state(server))
        .with_graceful_shutdown(signal)
        .await
        .into_diagnostic()
        .wrap_err("error starting stats endpoint web server")?;

    Ok(())
}

#[cfg(feature = "jemalloc")]
fn cors() -> CorsLayer {
    CorsLayer::new()
        // .allow_credentials(true)
        .allow_origin(Any)
    // .expose_headers([header::SET_COOKIE])
}

struct StatsActor {
    subsys: Subsystem,
    stats_rx: mpsc::Receiver<StatsEvent>,
    api_rx: mpsc::Receiver<StatsApiMessage>,
    election_state: ElectionState,
}

impl StatsActor {
    fn start_stats_actor(
        subsys: &Subsystem,
        stats_rx: mpsc::Receiver<StatsEvent>,
        api_rx: mpsc::Receiver<StatsApiMessage>,
    ) {
        subsys.spawn("stats-actor", async |s| {
            let actor = StatsActor {
                subsys: s,
                stats_rx,
                api_rx,
                election_state: ElectionState::Candidate,
            };
            actor.run_stats_actor().await
        });
    }

    async fn run_stats_actor(mut self) -> Result<()> {
        loop {
            select! {
                recv = self.stats_rx.recv() => match recv {
                    Some(it) => self.stats_event(it),
                    None => break,
                },
                recv = self.api_rx.recv() => match recv {
                    Some(it) => self.api_request(it),
                    None => break,
                },
                _ = self.subsys.shutdown_requested() => break,
            }
        }

        Ok(())
    }

    fn stats_event(&mut self, e: StatsEvent) {
        match e {
            StatsEvent::Election(state) => self.election_state = state,
        }
    }

    fn api_request(&mut self, e: StatsApiMessage) {
        match e {
            StatsApiMessage::ElectionState(sender) => {
                sender.send(self.election_state.clone()).ok();
            }
        }
    }
}
