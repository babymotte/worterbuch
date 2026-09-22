#!/bin/bash

cargo build -p worterbuch &&
WORTERBUCH_TOKIO_CONSOLE_PORT=5555 \
WORTERBUCH_LOG=worterbuch::cluster::proxy=trace,debug \
WORTERBUCH_DATA_DIR=./data/proxy2 \
WORTERBUCH_WS_SERVER_PORT=8085 \
WORTERBUCH_TCP_SERVER_PORT=9095 \
WORTERBUCH_INITIAL_SYNC_TIMEOUT=5 \
./target/debug/worterbuch --instance-name proxy2 proxy -l localhost:6060 -l localhost:6061 -l localhost:6062