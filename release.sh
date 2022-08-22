#!/bin/bash
cargo make test || exit 1
cargo ws version || exit 1
cd worterbuch-codec && cargo make publish
cd ../worterbuch-common && cargo make publish
cd ../worterbuch-client && cargo make publish
cd ../worterbuch && cargo make publish
cd ../worterbuch-cli && cargo make publish
cd ../worterbuch-wasm && cargo make publish
