#!/bin/bash
cargo make test-features || exit 1
cargo test -p libworterbuch --no-default-features || exit 1
cargo test -p libworterbuch || exit 1
cargo test -p libworterbuch --all-features || exit 1
cargo test -p worterbuch --no-default-features || exit 1
cargo test -p worterbuch || exit 1
cargo test -p worterbuch --all-features || exit 1
cargo test -p worterbuch-cli --no-default-features --features tcp || exit 1
cargo test -p worterbuch-cli --no-default-features --features ws || exit 1
cargo test -p worterbuch-cli --no-default-features --features graphql || exit 1
cargo publish -p libworterbuch --dry-run || exit 1
