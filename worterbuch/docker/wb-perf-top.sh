#!/bin/sh
#
# Runs `perf top` against the worterbuch process in this container.
#
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
#
# Usage (perf needs extra privileges and an interactive terminal):
#
#   docker exec -it --privileged <container> wb-perf-top [additional perf top args]
#
# Any arguments are passed on to `perf top`, e.g. `wb-perf-top -g` for call graphs.
#
# Alternatively, `wb-perf-top --record <seconds>` records a profile for the given number of
# seconds (e.g. while triggering a client disconnect) and prints a report afterwards. This does
# not need an interactive terminal. The raw data is kept in /tmp/worterbuch-perf.data.

set -eu

record_secs=""
if [ "${1:-}" = "--record" ]; then
    record_secs="${2:?usage: wb-perf-top --record <seconds>}"
    shift 2
fi

pid=""
for dir in /proc/[0-9]*; do
    if [ "$(cat "$dir/comm" 2>/dev/null || true)" = "worterbuch" ]; then
        pid="${dir#/proc/}"
        break
    fi
done

if [ -z "$pid" ]; then
    echo "No worterbuch process found." >&2
    exit 1
fi

if [ -n "$record_secs" ]; then
    data=/tmp/worterbuch-perf.data
    echo "Recording worterbuch process $pid for $record_secs s …" >&2
    perf record -e cpu-clock -p "$pid" -o "$data" "$@" -- sleep "$record_secs"
    exec perf report -i "$data" --stdio --no-children --percent-limit 1
fi

echo "Running perf top on worterbuch process $pid …" >&2
exec perf top -p "$pid" "$@"
