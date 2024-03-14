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
    env, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use miette::IntoDiagnostic;
use poem::{
    endpoint::StaticFilesEndpoint,
    get, handler,
    http::StatusCode,
    listener::TcpListener,
    middleware::{AddData, Tracing},
    post,
    web::{
        sse::{Event, SSE},
        Data, Json,
    },
    EndpointExt, Error, IntoResponse, Result, Route, Server,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    spawn,
    sync::{broadcast, mpsc},
};
use tokio_graceful_shutdown::SubsystemHandle;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

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

#[handler]
async fn throughput_settings(
    Json(s): Json<Settings>,
    Data(api): Data<&mpsc::Sender<throughput::Api>>,
) -> Result<Json<Value>> {
    if let Some(agents) = s.agents {
        if let Err(e) = api.send(throughput::Api::SetAgents(agents)).await {
            return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
        }
    }

    if let Some(target_rate) = s.target_rate {
        if let Err(e) = api.send(throughput::Api::SetTargetRate(target_rate)).await {
            return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
        }
    }

    Ok(Json(json!("Ok")))
}

#[handler]
async fn throughput_start(
    Json(s): Json<Settings>,
    Data(api): Data<&mpsc::Sender<throughput::Api>>,
) -> Result<Json<Value>> {
    if let (Some(agents), Some(target_rate)) = (s.agents, s.target_rate) {
        if let Err(e) = api.send(throughput::Api::Start(agents, target_rate)).await {
            return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
        }
    } else {
        return Err(Error::new(
            io::Error::new(
                io::ErrorKind::InvalidData,
                "need both agents and target rate",
            ),
            StatusCode::BAD_REQUEST,
        ));
    }

    Ok(Json(json!("Ok")))
}

#[handler]
async fn throughput_stop(Data(api): Data<&mpsc::Sender<throughput::Api>>) -> Result<Json<Value>> {
    if let Err(e) = api.send(throughput::Api::Stop).await {
        return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
    }

    Ok(Json(json!("Ok")))
}

#[handler]
fn throughput_stats(
    Data(tx): Data<&broadcast::Sender<throughput::UiApi>>,
    Data(running): Data<&Arc<AtomicBool>>,
) -> Result<impl IntoResponse> {
    let running = running.load(Ordering::Acquire);
    let rx = tx.subscribe();
    if let Err(e) = if running {
        tx.send(throughput::UiApi::Running)
    } else {
        tx.send(throughput::UiApi::Stopped)
    } {
        return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
    }
    let stream = BroadcastStream::new(rx)
        .map(move |stat| {
            stat.ok()
                .and_then(|s| serde_json::to_string(&s).ok())
                .map(Event::message)
        })
        .filter_map(|it| it);
    Ok(SSE::new(stream)
        .keep_alive(Duration::from_secs(5))
        .with_header("Access-Control-Allow-Origin", "*"))
}

#[handler]
async fn latency_start(
    Json(s): Json<LatencySettings>,
    Data(api): Data<&mpsc::Sender<latency::Api>>,
) -> Result<Json<Value>> {
    if let Err(e) = api.send(latency::Api::Start(s)).await {
        return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
    }

    Ok(Json(json!("Ok")))
}

#[handler]
fn latency_events(
    Data(tx): Data<&broadcast::Sender<latency::UiApi>>,
    Data(running): Data<&Arc<AtomicBool>>,
) -> Result<impl IntoResponse> {
    let running = running.load(Ordering::Acquire);
    let rx = tx.subscribe();
    if let Err(e) = if running {
        tx.send(latency::UiApi::Running)
    } else {
        tx.send(latency::UiApi::Stopped)
    } {
        return Err(Error::new(e, StatusCode::INTERNAL_SERVER_ERROR));
    }
    let stream = BroadcastStream::new(rx)
        .map(move |stat| {
            stat.ok()
                .and_then(|s| serde_json::to_string(&s).ok())
                .map(Event::message)
        })
        .filter_map(|it| it);
    Ok(SSE::new(stream)
        .keep_alive(Duration::from_secs(5))
        .with_header("Access-Control-Allow-Origin", "*"))
}

pub async fn run_web_ui(
    subsys: SubsystemHandle,
    mut throughput_backend_events: mpsc::Receiver<throughput::UiApi>,
    mut latency_backend_events: mpsc::Receiver<latency::UiApi>,
    throughput_api: mpsc::Sender<throughput::Api>,
    latency_api: mpsc::Sender<latency::Api>,
) -> miette::Result<()> {
    let web_root_path = env::var("WORTERBUCH_SPEEDTEST_WEBROOT_PATH")
        .unwrap_or("../../worterbuch-speedtest-ui/build".to_owned());
    let port = env::var("WORTERBUCH_SPEEDTEST_PORT")
        .ok()
        .and_then(|it| it.parse::<u16>().ok())
        .unwrap_or(4000);
    let (throughput_stats_tx, throughput_stats_rx) = broadcast::channel(1000);
    let (latency_events_tx, latency_events_rx) = broadcast::channel(1000);
    let throughput_running = Arc::new(AtomicBool::new(false));
    let latency_running = Arc::new(AtomicBool::new(false));
    let app = Route::new()
        .at(
            "/throughput/settings",
            post(throughput_settings.with(AddData::new(throughput_api.clone()))),
        )
        .at(
            "/throughput/start",
            post(throughput_start.with(AddData::new(throughput_api.clone()))),
        )
        .at(
            "/throughput/stop",
            post(throughput_stop.with(AddData::new(throughput_api.clone()))),
        )
        .at(
            "/throughput/stats",
            get(throughput_stats
                .with(AddData::new(throughput_stats_tx.clone()))
                .with(AddData::new(throughput_running.clone()))),
        )
        .at(
            "/latency/start",
            post(latency_start.with(AddData::new(latency_api.clone()))),
        )
        .at(
            "/latency/events",
            get(latency_events
                .with(AddData::new(latency_events_tx.clone()))
                .with(AddData::new(latency_running.clone()))),
        )
        .nest(
            "/",
            StaticFilesEndpoint::new(web_root_path)
                .index_file("index.html")
                .fallback_to_index()
                .redirect_to_slash_directory(),
        )
        .with(Tracing);

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
                log::error!("Error forwarding stats: {e}");
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
                log::error!("Error forwarding stats: {e}");
                break;
            }
        }
    });

    let host = hostname::get().into_diagnostic()?;
    let host = host.to_str().unwrap_or("localhost");
    log::info!("Starting speedtest server at http://{host}:{port}");

    Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
        .name("worterbuch-speedtest-server")
        .run_with_graceful_shutdown(app, subsys.on_shutdown_requested(), None)
        .await
        .into_diagnostic()?;

    drop(throughput_stats_rx);
    drop(latency_events_rx);

    Ok(())
}
