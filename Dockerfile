FROM rust:latest AS worterbuch-builder
WORKDIR /src
RUN cargo new --lib worterbuch
WORKDIR /src/worterbuch
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release
COPY . .
RUN cargo build --release --features tcp,ws,graphql

FROM debian
WORKDIR /app
COPY --from=worterbuch-builder /src/worterbuch/target/release/worterbuch .
COPY cert cert/
CMD ["./worterbuch"]
