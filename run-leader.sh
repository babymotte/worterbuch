#!/bin/bash

# cargo build -p worterbuch --release &&
WORTERBUCH_LOG=info \
WORTERBUCH_DATA_DIR=./data/leader \
WORTERBUCH_TCP_SERVER_PORT=9090 \
WORTERBUCH_WS_SERVER_PORT=8080 \
cargo run -p worterbuch --features=tokio-console -- leader --sync-port 6060