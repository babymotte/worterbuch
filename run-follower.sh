#!/bin/bash

WORTERBUCH_LOG=worterbuch=trace,cluster::follower=trace,worterbuch::persistence=debug,debug WORTERBUCH_DATA_DIR=./data/follower WORTERBUCH_WS_SERVER_PORT=0 WORTERBUCH_TCP_SERVER_PORT=0 WORTERBUCH_INITIAL_SYNC_TIMEOUT=5 cargo watch -cx 'run -- follower --leader-address 127.0.0.1:6060'