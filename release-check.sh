#!/bin/bash
cargo make test-features || exit 1
cargo test -p libworterbuch --no-default-features || exit 1
cargo test -p libworterbuch --all-features || exit 1
cargo test -p worterbuch --no-default-features || exit 1
cargo test -p worterbuch --all-features || exit 1
cargo test -p worterbuch-cli --all-features || exit 1
cargo publish -p libworterbuch --dry-run || exit 1
