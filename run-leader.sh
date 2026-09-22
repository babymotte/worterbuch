#!/bin/bash

cargo build -p worterbuch &&
WORTERBUCH_LOG=info \
WORTERBUCH_DATA_DIR=./data/leader \
WORTERBUCH_TCP_SERVER_PORT=9090 \
WORTERBUCH_WS_SERVER_PORT=8080 \
./target/debug/worterbuch leader --sync-port 6060