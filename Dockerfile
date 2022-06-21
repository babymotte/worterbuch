FROM rust:latest AS worterbuch-builder
WORKDIR /src
RUN cargo new --lib worterbuch
WORKDIR /src/worterbuch
COPY . .
RUN cargo test -p libworterbuch --release
RUN cargo test -p worterbuch --release --features docker,tcp,ws,graphql
RUN cargo test -p worterbuch-cli --release --features tcp
RUN cargo build -p worterbuch --release --features docker,tcp,ws,graphql
RUN cargo build -p worterbuch-cli --release --features tcp

FROM debian
WORKDIR /app
COPY --from=worterbuch-builder /src/worterbuch/target/release/worterbuch .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbget .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbpget .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbset .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbsub .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbpsub .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbimp .
COPY --from=worterbuch-builder /src/worterbuch/target/release/wbexp .
VOLUME [ "/data" ]
CMD ["./worterbuch"]
