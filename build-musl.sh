#!/bin/bash

[[ -d target ]] || mkdir target || exit $?
[[ -d target/arm ]] || mkdir target/musl || exit $?
docker run -it --name worterbuch-musl-builder --rm -v $(pwd)/:/src -v $(pwd)/target/arm:/src/target --workdir /src messense/rust-musl-cross:x86_64-musl cargo build -p worterbuch --release
