#!/bin/bash

WORTERBUCH_LOG=debug WORTERBUCH_DATA_DIR=./data/leader WORTERBUCH_TCP_SERVER_PORT=9090 WORTERBUCH_WS_SERVER_PORT=8080 cargo watch -cx 'run -- leader --sync-port 6060'