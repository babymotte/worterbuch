FROM lukemathwalker/cargo-chef:latest-rust-1 AS wbco-chef
WORKDIR /app

FROM wbco-chef AS wbco-planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM wbco-chef AS wbco-builder 
COPY --from=wbco-planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM babymotte/worterbuch:1.0.2
WORKDIR /app
COPY --from=wbco-builder /app/target/release/worterbuch-cluster-orchestrator /usr/local/bin
ENV WBCLUSTER_CONGIF_PATH=/cfg/config.yaml
ENV WBCLUSTER_HEARTBEAT_INTERVAL=100
ENV WBCLUSTER_HEARTBEAT_MIN_TIMEOUT=500
ENV WBCLUSTER_PORT=8181
ENV WBCLUSTER_SYNC_PORT=8282
ENV WBCLUSTER_WB_EXECUTABLE=/usr/local/bin/worterbuch
RUN apt update && apt install -y netcat-traditional
VOLUME [ "/cfg" ]
ENTRYPOINT ["/usr/local/bin/worterbuch-cluster-orchestrator"]