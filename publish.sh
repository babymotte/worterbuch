#!/bin/bash

cd worterbuch-common && cargo publish --token $CRATES_TOKEN || echo "already published"
cd ../worterbuch-client && cargo publish --token $CRATES_TOKEN || echo "already published"
cd ../worterbuch && cargo publish --token $CRATES_TOKEN || echo "already published"
cd ../worterbuch-cli && cargo publish --token $CRATES_TOKEN || echo "already published"
cd ../worterbuch-cluster-orchestrator && cargo publish --token $CRATES_TOKEN || echo "already published"
