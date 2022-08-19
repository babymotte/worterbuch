FROM messense/rust-musl-cross:x86_64-musl AS worterbuch-builder
RUN curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | bash 
WORKDIR /src/worterbuch
COPY . .
RUN cargo build -p worterbuch --release
WORKDIR /src/worterbuch/worterbuch-js
RUN wasm-pack build --target web

FROM node AS worterbuch-explorer-builder
WORKDIR /src/worterbuch-explorer
COPY worterbuch-explorer .
COPY --from=worterbuch-builder /src/worterbuch/worterbuch-js/pkg /src/worterbuch-js/pkg
RUN npm i
RUN npm run build

FROM scratch
WORKDIR /app
COPY --from=worterbuch-builder /src/worterbuch/target/x86_64-unknown-linux-musl/release/worterbuch .
COPY --from=worterbuch-explorer-builder /src/worterbuch-explorer/build ./html
ENV RUST_LOG=info
ENV WORTERBUCH_BIND_ADDRESS=0.0.0.0
ENV WORTERBUCH_USE_PERSISTENCE=true
ENV WORTERBUCH_DATA_DIR=/data
ENV WORTERBUCH_PERSISTENCE_INTERVAL=5
ENV WORTERBUCH_EXPLORER_WEBROOT_PATH=/app/html
ENV WORTERBUCH_WEB_PORT=80
ENV WORTERBUCH_SINGLE_THREADED=false
ENV WORTERBUCH_EXPLORER=true
VOLUME [ "/data" ]
CMD ["./worterbuch"]
