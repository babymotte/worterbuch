#!/bin/bash

cargo build -p worterbuch &&
WORTERBUCH_LOG=debug \
WORTERBUCH_DATA_DIR=./data/leader2 \
WORTERBUCH_TCP_SERVER_PORT=9091 \
WORTERBUCH_WS_SERVER_PORT=8081 \
./target/debug/worterbuch leader --sync-port 6061