#!/bin/bash

WORTERBUCH_TOKIO_CONSOLE_PORT=4444 \
WORTERBUCH_LOG=worterbuch::cluster::proxy=trace,worterbuch_common=trace,debug \
WORTERBUCH_DATA_DIR=./data/proxy \
WORTERBUCH_WS_SERVER_PORT=8084 \
WORTERBUCH_TCP_SERVER_PORT=9094 \
WORTERBUCH_QUIC_SERVER_PORT=7074 \
WORTERBUCH_INITIAL_SYNC_TIMEOUT=5 \
cargo watch -cx 'run -- --instance-name proxy proxy -l localhost:6060 -l localhost:6061 -l localhost:6062'
