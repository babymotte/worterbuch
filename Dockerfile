FROM messense/rust-musl-cross:x86_64-musl AS worterbuch-builder
WORKDIR /src
RUN cargo new --lib worterbuch
WORKDIR /src/worterbuch
COPY . .
RUN cargo test -p libworterbuch --release --all-features
RUN cargo test -p worterbuch --release --all-features
RUN cargo test -p worterbuch-cli --release --all-features
RUN cargo build -p worterbuch --release --features docker,tcp,ws,graphql
RUN cargo build -p worterbuch-cli --release --features tcp

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
VOLUME [ "/data" ]
CMD ["./worterbuch"]
