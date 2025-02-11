#/bin/bash

RUST_LOG=info WORTERBUCH_DATA_DIR=./data/bob ./target/release/worterbuch-cluster-orchestrator -p 1338  -q 1 bob
# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/bob cargo run -- -p 1338 bob
# docker run --rm --name wbco-bob -v $(pwd)/data/bob:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 1338:1338 wbco -p 1338 bob
