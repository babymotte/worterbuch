#!/bin/bash

WORTERBUCH_TOKIO_CONSOLE_PORT=5555 \
WORTERBUCH_LOG=worterbuch::cluster::proxy=trace,debug \
WORTERBUCH_DATA_DIR=./data/proxy2 \
WORTERBUCH_WS_SERVER_PORT=8085 \
WORTERBUCH_TCP_SERVER_PORT=9095 \
WORTERBUCH_INITIAL_SYNC_TIMEOUT=5 \
cargo watch -cx 'run -- --instance-name proxy2 proxy -l 127.0.0.1:6060 -l 127.0.0.1:6061 -l 127.0.0.1:6062'