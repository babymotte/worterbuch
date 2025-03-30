# Copyright (C) 2024 Michael Bachmann
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
ARG FEATURES=""
COPY --from=wbco-planner /app/recipe.json recipe.json
RUN cargo chef cook --release ${FEATURES} --recipe-path recipe.json
COPY . .
RUN cargo fmt --check
RUN cargo clippy -- --deny warnings
RUN cargo test
RUN cargo build --release ${FEATURES}

FROM babymotte/worterbuch:1.3.2
WORKDIR /app
COPY --from=wbco-builder /app/target/release/worterbuch-cluster-orchestrator /usr/local/bin
ENV WBCLUSTER_CONGIF_PATH=/cfg/config.yaml
ENV WBCLUSTER_HEARTBEAT_INTERVAL=100
ENV WBCLUSTER_HEARTBEAT_MIN_TIMEOUT=500
ENV WBCLUSTER_RAFT_PORT=8181
ENV WBCLUSTER_SYNC_PORT=8282
ENV WBCLUSTER_WB_EXECUTABLE=/usr/local/bin/worterbuch
ENV MALLOC_CONF=thp:always,metadata_thp:always,prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_gdump:false,prof_leak:true,prof_final:true,prof_prefix:/profiling/jeprof
VOLUME [ "/cfg" ]
VOLUME [ "/profiling" ]
ENTRYPOINT ["/usr/local/bin/worterbuch-cluster-orchestrator"]
