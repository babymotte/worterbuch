#!/bin/bash

function workspace_clean() {
    git diff --exit-code &>/dev/null && git diff --cached --exit-code &>/dev/null
}

workspace_clean || {
    echo >&2 "Error: workspace is not clean, please commit or stash any changed files first"
    exit 1
}

VERSION=$(cargo metadata --format-version 1 | jq -r '.packages[]  | select(.name | test("worterbuch-cluster-orchestrator")) | .version')

echo "Preparing release of version $VERSION …"

[ $(git tag -l "v$VERSION") ] && {
    echo >&2 "Error: version $VERSION has already been released, update Cargo.toml first."
    exit 1
}

./check.sh || {
    echo >&2 "Error: Checks failed"
    exit 1
}

WB_TAG=$(curl -L \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    https://api.github.com/repos/babymotte/worterbuch/releases/latest 2>/dev/null | jq -r .tag_name)

[[ "$WB_TAG" =~ v([0-9]+\.[0-9]+\.[0-9]+) ]] && WB_VERSION=${BASH_REMATCH[1]} || {
    echo >&2 "Error: Could not get latest release tag of worterbuch"
    exit 1
}

echo "Using worterbuch version $WB_VERSION. If this is correct, press enter, otherwise press Ctrl + C"
read

DOCKERFILE="# Copyright (C) 2024 Michael Bachmann
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

FROM lukemathwalker/cargo-chef:latest-rust-1 AS wbco-chef
WORKDIR /app
RUN rustup component add rustfmt
RUN rustup component add clippy

FROM wbco-chef AS wbco-planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM wbco-chef AS wbco-builder 
COPY --from=wbco-planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo fmt --check
RUN cargo clippy -- --deny warnings
RUN cargo test
RUN cargo build --release

FROM babymotte/worterbuch:$WB_VERSION
WORKDIR /app
COPY --from=wbco-builder /app/target/release/worterbuch-cluster-orchestrator /usr/local/bin
ENV WBCLUSTER_CONFIG_PATH=/cfg/config.yaml
ENV WBCLUSTER_HEARTBEAT_INTERVAL=100
ENV WBCLUSTER_HEARTBEAT_MIN_TIMEOUT=500
ENV WBCLUSTER_RAFT_PORT=8181
ENV WBCLUSTER_SYNC_PORT=8282
ENV WBCLUSTER_WB_EXECUTABLE=/usr/local/bin/worterbuch
ENV MALLOC_CONF=thp:always,metadata_thp:always,prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_gdump:false,prof_leak:true,prof_final:true,prof_prefix:/profiling/jeprof
VOLUME [ \"/cfg\" ]
VOLUME [ "/profiling" ]
ENTRYPOINT [\"/usr/local/bin/worterbuch-cluster-orchestrator\"]"

echo "$DOCKERFILE" >Dockerfile

CHART=$(yq -y ".version=\"$VERSION\"|.appVersion=\"$VERSION\"" <kubernetes/chart/Chart.yaml)
echo "$CHART" >kubernetes/chart/Chart.yaml

git add Dockerfile && git add kubernetes/chart/Chart.yaml && git commit -m "updated Dockerfile and helm chart" || {
    echo >&2 "Error: Could not commit updated Dockerfile and helm chart"
    exit 1
}

git tag "v$VERSION" || {
    echo >&2 "Error: Could not tag commit"
    exit 1
}

git push --no-verify && git push --tags --no-verify || {
    echo >&2 "Error: Could not push"
    exit 1
}

echo SUCCESS
