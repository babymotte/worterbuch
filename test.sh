#!/bin/bash
cargo test -p worterbuch-common || exit 1
cargo test -p worterbuch || exit 1
cargo test -p worterbuch-client || exit 1
cargo test -p worterbuch-cli || exit 1
