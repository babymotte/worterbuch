#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/alice ./target/release/worterbuch-cluster-orchestrator -p 1337 -q 1 alice
RUST_LOG=info WORTERBUCH_DATA_DIR=./data/alice cargo run -- -p 1337 --stats-port 8383 alice
# docker run --rm --name wbco -v $(pwd)/wb-data:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 80:80 -p 9090:9090 -p 8282:8282 -p 9292:9292 docker.io/babymotte/worterbuch-cluster-orchestrator:latest -p 9292
