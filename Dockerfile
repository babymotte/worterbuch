FROM messense/rust-musl-cross:x86_64-musl AS worterbuch-builder
WORKDIR /src/worterbuch
COPY . .
RUN cargo build -p worterbuch --release

FROM scratch
WORKDIR /app
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/worterbuch .
ENV RUST_LOG=info
ENV WORTERBUCH_WS_BIND_ADDRESS=0.0.0.0
ENV WORTERBUCH_TCP_BIND_ADDRESS=0.0.0.0
ENV WORTERBUCH_USE_PERSISTENCE=true
ENV WORTERBUCH_DATA_DIR=/data
ENV WORTERBUCH_PERSISTENCE_INTERVAL=5
ENV WORTERBUCH_WS_SERVER_PORT=80
ENV WORTERBUCH_TCP_SERVER_PORT=9090
ENV WORTERBUCH_SINGLE_THREADED=false
ENV WORTERBUCH_WEBAPP=false
VOLUME [ "/data" ]
CMD ["./worterbuch"]
