/*
 *  Worterbuch speed test
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

use std::{
    convert::Infallible,
    env,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures::Stream;
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    spawn,
    sync::{broadcast, mpsc},
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tosub::SubsystemHandle;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing::{error, info};

use crate::{
    latency::{self, LatencySettings},
    throughput,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub agents: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub target_rate: Option<usize>,
}

#[derive(Clone)]
struct AppState {
    throughput_api: mpsc::UnboundedSender<throughput::Api>,
    throughput_stats_tx: broadcast::Sender<throughput::UiApi>,
    throughput_running: Arc<AtomicBool>,
    latency_api: mpsc::UnboundedSender<latency::Api>,
    latency_events_tx: broadcast::Sender<latency::UiApi>,
    latency_running: Arc<AtomicBool>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

async fn throughput_settings(
    State(state): State<AppState>,
    Json(s): Json<Settings>,
) -> ApiResult<Json<Value>> {
    if let Some(agents) = s.agents {
        state
            .throughput_api
            .send(throughput::Api::SetAgents(agents))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    if let Some(target_rate) = s.target_rate {
        state
            .throughput_api
            .send(throughput::Api::SetTargetRate(target_rate))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(Json(json!("Ok")))
}

async fn throughput_start(
    State(state): State<AppState>,
    Json(s): Json<Settings>,
) -> ApiResult<Json<Value>> {
    if let (Some(agents), Some(target_rate)) = (s.agents, s.target_rate) {
        state
            .throughput_api
            .send(throughput::Api::Start(agents, target_rate))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "need both agents and target rate".to_owned(),
        ));
    }

    Ok(Json(json!("Ok")))
}

async fn throughput_stop(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    state
        .throughput_api
        .send(throughput::Api::Stop)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!("Ok")))
}

async fn throughput_stats(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let running = state.throughput_running.load(Ordering::Acquire);
    let rx = state.throughput_stats_tx.subscribe();
    if running {
        state.throughput_stats_tx.send(throughput::UiApi::Running)
    } else {
        state.throughput_stats_tx.send(throughput::UiApi::Stopped)
    }
    .ok();
    let stream = BroadcastStream::new(rx).filter_map(|stat| {
        stat.ok()
            .and_then(|s| serde_json::to_string(&s).ok())
            .map(|data| Ok(Event::default().data(data)))
    });
    Sse::new(stream).keep_alive(KeepAlive::default().interval(Duration::from_secs(5)))
}

async fn latency_start(
    State(state): State<AppState>,
    Json(s): Json<LatencySettings>,
) -> ApiResult<Json<Value>> {
    state
        .latency_api
        .send(latency::Api::Start(s))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!("Ok")))
}

async fn latency_stop(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    state
        .latency_api
        .send(latency::Api::Stop)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!("Ok")))
}

async fn latency_events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let running = state.latency_running.load(Ordering::Acquire);
    let rx = state.latency_events_tx.subscribe();
    if running {
        state.latency_events_tx.send(latency::UiApi::Running)
    } else {
        state.latency_events_tx.send(latency::UiApi::Stopped)
    }
    .ok();
    let stream = BroadcastStream::new(rx).filter_map(|stat| {
        stat.ok()
            .and_then(|s| serde_json::to_string(&s).ok())
            .map(|data| Ok(Event::default().data(data)))
    });
    Sse::new(stream).keep_alive(KeepAlive::default().interval(Duration::from_secs(5)))
}

pub async fn run_web_ui(
    subsys: SubsystemHandle,
    mut throughput_backend_events: mpsc::UnboundedReceiver<throughput::UiApi>,
    mut latency_backend_events: mpsc::UnboundedReceiver<latency::UiApi>,
    throughput_api: mpsc::UnboundedSender<throughput::Api>,
    latency_api: mpsc::UnboundedSender<latency::Api>,
) -> miette::Result<()> {
    let web_root_path =
        env::var("WORTERBUCH_SPEEDTEST_WEBROOT_PATH").unwrap_or("./web-ui/dist".to_owned());
    let port = env::var("WORTERBUCH_SPEEDTEST_PORT")
        .ok()
        .and_then(|it| it.parse::<u16>().ok())
        .unwrap_or(4000);
    let (throughput_stats_tx, throughput_stats_rx) = broadcast::channel(1000);
    let (latency_events_tx, latency_events_rx) = broadcast::channel(1000);
    let throughput_running = Arc::new(AtomicBool::new(false));
    let latency_running = Arc::new(AtomicBool::new(false));

    let state = AppState {
        throughput_api,
        throughput_stats_tx: throughput_stats_tx.clone(),
        throughput_running: throughput_running.clone(),
        latency_api,
        latency_events_tx: latency_events_tx.clone(),
        latency_running: latency_running.clone(),
    };

    let web_root_path = PathBuf::from(web_root_path);
    let app = Router::new()
        .route("/throughput/settings", post(throughput_settings))
        .route("/throughput/start", post(throughput_start))
        .route("/throughput/stop", post(throughput_stop))
        .route("/throughput/stats", get(throughput_stats))
        .route("/latency/start", post(latency_start))
        .route("/latency/stop", post(latency_stop))
        .route("/latency/events", get(latency_events))
        .fallback_service(
            ServeDir::new(&web_root_path)
                .fallback(ServeFile::new(web_root_path.join("index.html"))),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    let throughput_tx = throughput_stats_tx.clone();
    spawn(async move {
        while let Some(s) = throughput_backend_events.recv().await {
            if throughput::UiApi::Running == s {
                throughput_running.store(true, Ordering::Release);
            }
            if throughput::UiApi::Stopped == s {
                throughput_running.store(false, Ordering::Release);
            }
            if let Err(e) = throughput_tx.send(s) {
                error!("Error forwarding stats: {e}");
                break;
            }
        }
    });

    let latency_tx = latency_events_tx.clone();
    spawn(async move {
        while let Some(s) = latency_backend_events.recv().await {
            if latency::UiApi::Running == s {
                latency_running.store(true, Ordering::Release);
            }
            if latency::UiApi::Stopped == s {
                latency_running.store(false, Ordering::Release);
            }
            if let Err(e) = latency_tx.send(s) {
                error!("Error forwarding stats: {e}");
                break;
            }
        }
    });

    let host = hostname::get().into_diagnostic()?;
    let host = host.to_str().unwrap_or("localhost");
    info!("Starting speedtest server at http://{host}:{port}");

    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .into_diagnostic()?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { subsys.shutdown_requested().await })
        .await
        .into_diagnostic()?;

    drop(throughput_stats_rx);
    drop(latency_events_rx);

    Ok(())
}
