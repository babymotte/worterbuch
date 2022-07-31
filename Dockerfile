FROM messense/rust-musl-cross:x86_64-musl AS worterbuch-builder
COPY . /src/worterbuch
WORKDIR /src/worterbuch
RUN cargo test -p libworterbuch --release --all-features
RUN cargo test -p worterbuch --release --features docker,tcp,ws,graphql
RUN cargo test -p worterbuch-cli --release --features tcp
RUN cargo build -p worterbuch --release --features docker,tcp,ws,graphql,explorer
RUN cargo build -p worterbuch-cli --release --features tcp

FROM node:18.7 AS worterbuch-explorer-builder
WORKDIR /src/worterbuch-explorer
COPY worterbuch-explorer .
RUN npm i
RUN npm run build

FROM scratch
WORKDIR /app
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/worterbuch .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbget .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbpget .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbset .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbsub .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbpsub .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbimp .
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/wbexp .
COPY --from=worterbuch-explorer-builder /src/worterbuch-explorer/build ./html
ENV RUST_LOG=info
ENV WORTERBUCH_BIND_ADDRESS=0.0.0.0
ENV WORTERBUCH_USE_PERSISTENCE=true
ENV WORTERBUCH_DATA_DIR=/data
ENV WORTERBUCH_PERSISTENCE_INTERVAL=5
ENV WORTERBUCH_EXPLORER_WEBROOT_PATH=/app/html
ENV WORTERBUCH_WEB_PORT=80
VOLUME [ "/data" ]
CMD ["./worterbuch"]
