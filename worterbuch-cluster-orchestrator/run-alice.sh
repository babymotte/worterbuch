#/bin/bash

export MALLOC_CONF=thp:always,metadata_thp:always,prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_gdump:false,prof_leak:true,prof_final:true,prof_prefix:/tmp/wb/jeprof
export RUST_BACKTRACE=1
# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/alice ./target/release/worterbuch-cluster-orchestrator -p 1337 -q 1 alice
WORTERBUCH_LOG=info WORTERBUCH_TRACING=debug WORTERBUCH_DATA_DIR=./data/alice WORTERBUCH_WS_SERVER_PORT=8080 cargo run --release -- -p 1337 --stats-port 8383 alice
# docker run --rm --name wbco-alice -v $(pwd)/data/alice:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 80:80 -p 9090:9090 -p 8281:8282 -p 1337:8181/udp docker.io/babymotte/worterbuch-cluster-orchestrator:1.1.1 alice
