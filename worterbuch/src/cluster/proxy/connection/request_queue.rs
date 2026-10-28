/*
 *
 *  Proxy request queue module
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

use crate::{
    Config,
    cluster::{
        self,
        protocol::{Connected, Disconnected, LeaderMessage, ProxyMessage, Request},
    },
    error::{IntoWbAppResult, WorterbuchAppResult},
    server::common::WbFunction,
    worterbuch::Worterbuch,
};
use miette::{Context, miette};
use std::ops::ControlFlow;
use tokio::{
    io::{BufReader, Lines},
    net::tcp::OwnedReadHalf,
    sync::{mpsc, oneshot},
};
use tosub::Subsystem;
use totils::{
    CancelOn,
    peekable_mpsc::{self, PeekableReceiver},
    while_select,
};
use tracing::{debug, trace, warn};
use worterbuch_common::{
    ClientId,
    protocol::v1::{ClientMessage, ServerMessage},
};

pub enum QueuedRequest {
    LeaderRequest(WbFunction),
    CoreRequest(WbFunction),
}

pub struct InFlightRequest {
    request: WbFunction,
    rx: Option<oneshot::Receiver<Response>>,
    response: Option<Response>,
    response_consumer: mpsc::Sender<Response>,
}

#[derive(Clone)]
pub struct Response {
    pub client_id: ClientId,
    pub response: ServerMessage,
}

impl InFlightRequest {
    pub fn new(
        request: WbFunction,
        response_receiver: oneshot::Receiver<Response>,
        response_consumer: mpsc::Sender<Response>,
    ) -> Self {
        Self {
            request,
            rx: Some(response_receiver),
            response: None,
            response_consumer,
        }
    }

    async fn resolve(&mut self, response: Response) -> WorterbuchAppResult<()> {
        if let Err(response) = self.response_consumer.send(response.clone()).await {
            self.response = Some(response.0);
            return Err(miette!("proxy channel closed")).into_wb_app_result();
        }

        Ok(())
    }
}

pub struct PendingRequest {
    request: WbFunction,
    response: Option<Response>,
}

impl From<PendingRequest> for WbFunction {
    fn from(value: PendingRequest) -> Self {
        value.request
    }
}

impl From<InFlightRequest> for PendingRequest {
    fn from(req: InFlightRequest) -> Self {
        Self {
            request: req.request,
            response: req.response,
        }
    }
}

pub struct RequestQueueAgent {
    pub subsys: Subsystem,
    pub config: Config,
    pub leader_responses_rx: mpsc::Receiver<Response>,
    pub core_responses_rx: mpsc::Receiver<Response>,
    pub in_flight_response_rx: mpsc::Receiver<oneshot::Sender<Response>>,
}

impl RequestQueueAgent {
    pub async fn run(
        mut self,
        in_flight_request_rx: PeekableReceiver<InFlightRequest>,
        remaining_requests_tx: oneshot::Sender<Vec<PendingRequest>>,
    ) -> miette::Result<()> {
        let cfg = self.config.clone();
        self.subsys
            .spawn("leader-core-response-forwarder", move |s| async move {
                forward_leader_core_responses(s, in_flight_request_rx, remaining_requests_tx, cfg)
                    .await
            });

        while_select! {
            _ = self.subsys.shutdown_requested() => break,
            recv = self.leader_responses_rx.recv() => self.resolve_response(recv).await,
            recv = self.core_responses_rx.recv() => self.resolve_response(recv).await,
            recv = self.in_flight_response_rx.recv() => self.resolve_in_flight_response(recv).await,
        }

        Ok(())
    }

    async fn resolve_response(&self, recv: Option<Response>) -> ControlFlow<()> {
        if let Some(msg) = recv {
            // TODO:
            // check sequence number, if this is next:
            //      look up in-flight request tx, if present
            //          send it
            //      else insert core message in queue
            // else insert core message in queue
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    async fn resolve_in_flight_response(
        &self,
        recv: Option<oneshot::Sender<Response>>,
    ) -> ControlFlow<()> {
        if let Some(tx) = recv {
            // look up if there is a response already waiting for this callback
            // if yes, send it immediately
            // else store callback and resolve it once response arrives
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
}

pub struct RequestQueue<'a> {
    subsys: Subsystem,
    in_flight_request_tx: mpsc::Sender<InFlightRequest>,
    in_flight_response_tx: mpsc::Sender<oneshot::Sender<Response>>,
    remaining_requests: oneshot::Receiver<Vec<PendingRequest>>,
    response_consumer: mpsc::Sender<Response>,
    core_responses_tx: mpsc::Sender<Response>,
    proxy_request_tx: mpsc::Sender<ProxyMessage>,
    worterbuch: &'a mut Worterbuch,
}

impl<'a> RequestQueue<'a> {
    pub fn new(
        subsys: &Subsystem,
        worterbuch: &'a mut Worterbuch,
        response_consumer: mpsc::Sender<Response>,
        leader_responses_rx: mpsc::Receiver<Response>,
        proxy_request_tx: mpsc::Sender<ProxyMessage>,
        config: Config,
    ) -> Option<Self> {
        let (in_flight_request_tx, in_flight_request_rx) =
            peekable_mpsc::channel(config.channel_buffer_size);
        let (remaining_requests_tx, remaining_requests_rx) = oneshot::channel();
        let (core_responses_tx, core_responses_rx) = mpsc::channel(config.channel_buffer_size);
        let (in_flight_response_tx, in_flight_response_rx) =
            mpsc::channel(config.channel_buffer_size);

        let subsys = subsys.spawn("request-queue", move |s| async move {
            RequestQueueAgent {
                subsys: s,
                config,
                leader_responses_rx,
                core_responses_rx,
                in_flight_response_rx,
            }
            .run(in_flight_request_rx, remaining_requests_tx)
            .await
        })?;

        Some(Self {
            subsys,
            in_flight_request_tx,
            in_flight_response_tx,
            remaining_requests: remaining_requests_rx,
            response_consumer,
            core_responses_tx,
            proxy_request_tx,
            worterbuch,
        })
    }

    pub async fn process_client_api_call(
        &mut self,
        fun: Option<WbFunction>,
    ) -> miette::Result<ControlFlow<()>> {
        trace!(enter = "process_client_api_call");
        debug!("Processing API call: {fun:?}");

        let Some(request) = fun else {
            trace!(exit = "process_client_api_call");
            return Ok(ControlFlow::Break(()));
        };

        let (response_tx, response_rx) = oneshot::channel();

        let in_flight_request =
            InFlightRequest::new(request, response_rx, self.response_consumer.clone());

        if self
            .in_flight_request_tx
            .send(in_flight_request)
            .await
            .is_err()
        {
            trace!(exit = "process_client_api_call");
            return Ok(ControlFlow::Break(()));
        }

        if self.in_flight_response_tx.send(response_tx).await.is_err() {
            trace!(exit = "process_client_api_call");
            return Ok(ControlFlow::Break(()));
        }

        // TODO split leader and core system requests
        // => leader: forward request to leader
        // => core system: process request directly and send result into self.core_responses_tx

        trace!(exit = "process_client_api_call");
        Ok(ControlFlow::Continue(()))
    }

    pub async fn close(self) -> WorterbuchAppResult<Vec<PendingRequest>> {
        self.subsys
            .request_local_shutdown_because("in flight request queue was closed");
        self.remaining_requests.await.into_wb_app_result()
    }

    async fn queue_leader_request(&self, request: ProxyMessage) -> miette::Result<()> {
        todo!()
    }
}

async fn receive_leader_responses(
    subsys: Subsystem,
    leader_lines: Lines<BufReader<OwnedReadHalf>>,
    leader_responses_tx: mpsc::Sender<LeaderMessage>,
) -> miette::Result<()> {
    todo!()
}

async fn forward_leader_core_responses(
    subsys: Subsystem,
    mut in_flight_request_rx: PeekableReceiver<InFlightRequest>,
    pending_requests_tx: oneshot::Sender<Vec<PendingRequest>>,
    config: Config,
) -> WorterbuchAppResult<()> {
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = in_flight_request_rx.pop() => forward_response(&subsys, recv, &mut in_flight_request_rx, &config).await,
    }

    let remaining_requests = in_flight_request_rx
        .drain()
        .await
        .into_iter()
        .map(|req| req.into())
        .collect();

    let _ = pending_requests_tx.send(remaining_requests);

    Ok(())
}

async fn forward_response(
    subsys: &Subsystem,
    recv: Option<InFlightRequest>,
    in_flight_request_rx: &mut PeekableReceiver<InFlightRequest>,
    config: &Config,
) -> ControlFlow<()> {
    // peek at next request if the channel is not closed
    let Some(mut request) = recv else {
        return ControlFlow::Break(());
    };

    // take the response receiver out of the request
    let Some(rx) = request.rx.take() else {
        panic!("must always be present, this is only optional so that we can consume the rx");
    };

    // await response coming back from core or leader
    let timeout = config.channel_buffer_timeout;
    let response_with_timeout = tokio::time::timeout(timeout, rx);
    let cancellable_response_with_timeout =
        response_with_timeout.or_cancel_on(subsys.shutdown_requested());

    let response = match cancellable_response_with_timeout.await {
        None => {
            trace!(
                subsystem = subsys.name(),
                "shutdown requested before response was received or timed out"
            );
            in_flight_request_rx.push_back(request);
            return ControlFlow::Break(());
        }
        Some(Err(_)) => {
            trace!(subsystem = subsys.name(), "response timed out");
            in_flight_request_rx.push_back(request);
            return ControlFlow::Break(());
        }
        Some(Ok(Err(_))) => {
            trace!(
                subsystem = subsys.name(),
                "request was dropped, core/leader subsystem stopped before sending a response"
            );
            in_flight_request_rx.push_back(request);
            return ControlFlow::Break(());
        }
        Some(Ok(Ok(it))) => it,
    };

    // resolve the request with the received response
    // this will attempt to send the response back to the proxy, which may fail if the proxy is shutting down. The request object will store the response so that the next leader connection can process it directly without having to re-send it to the new leader
    // it is important that the new leader connection forwards all pending resolved responses BEFORE syncing with the new leader
    if request.resolve(response).await.is_err() {
        in_flight_request_rx.push_back(request);
        return ControlFlow::Break(());
    }

    ControlFlow::Continue(())
}
