#!/bin/bash

[[ -d target ]] || mkdir target || exit $?
[[ -d target/arm ]] || mkdir target/arm || exit $?
docker run -it --name worterbuch-arm-builder --rm -v $(pwd)/:/src -v $(pwd)/target/arm:/src/target --workdir /src messense/rust-musl-cross:armv7-musleabihf cargo build -p worterbuch --release
