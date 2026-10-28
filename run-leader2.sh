#!/bin/bash

WORTERBUCH_LOG=debug \
WORTERBUCH_DATA_DIR=./data/leader2 \
WORTERBUCH_TCP_SERVER_PORT=9091 \
WORTERBUCH_WS_SERVER_PORT=8081 \
cargo run -p worterbuch "$@" -- leader --sync-port 6061