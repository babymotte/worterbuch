#!/bin/bash

WORTERBUCH_DATA_DIR=./data/follower WORTERBUCH_LOG=debug WORTERBUCH_WS_SERVER_PORT=0 WORTERBUCH_USE_PERSISTENCE=true cargo run -- --follower --leader-address 127.0.0.1:9999