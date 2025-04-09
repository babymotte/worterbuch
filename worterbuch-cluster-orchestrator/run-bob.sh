#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/bob ./target/release/worterbuch-cluster-orchestrator -p 1338  -q 1 bob
WORTERBUCH_LOG=info WORTERBUCH_TRACING=debug WORTERBUCH_DATA_DIR=./data/bob WORTERBUCH_WS_SERVER_PORT=8180 cargo run -- -p 1338 --stats-port 8384 bob
# docker run --rm --name wbco-bob -v $(pwd)/data/bob:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 80:80 -p 9090:9090 -p 8282:8282 -p 1338:8181/udp docker.io/babymotte/worterbuch-cluster-orchestrator:1.1.1 bob
