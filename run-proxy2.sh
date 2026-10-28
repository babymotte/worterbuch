#!/bin/bash

WORTERBUCH_LOG=info \
WORTERBUCH_DATA_DIR=./data/proxy2 \
WORTERBUCH_WS_SERVER_PORT=8085 \
WORTERBUCH_TCP_SERVER_PORT=9095 \
WORTERBUCH_QUIC_SERVER_PORT=7075 \
WORTERBUCH_INITIAL_SYNC_TIMEOUT=5 \
cargo run -p worterbuch "$@" -- --instance-name proxy2 proxy -l localhost:6060 -l localhost:6061 -l localhost:6062
