#!/usr/bin/env bash
set -euo pipefail

shadow_data="${1:-shadow.data}"

grep -Rhasq "FAULT_INJECTION exiting before installing snapshot" "${shadow_data}/hosts/node1" \
    || { echo "FAIL: first snapshot attempt was not interrupted" >&2; exit 1; }
grep -Rhasq "Installing snapshot from leader" "${shadow_data}/hosts/node1" \
    || { echo "FAIL: restarted follower never retried snapshot installation" >&2; exit 1; }

raft_data=$(find "${shadow_data}/hosts/node1" -name "raft_data_1" -print -quit)
[ -n "$raft_data" ] || { echo "FAIL: missing node 1 WAL" >&2; exit 1; }
dump=$(cargo run --quiet --release --bin wal_dump -- "$raft_data")
snapshot_index=$(printf '%s\n' "$dump" | sed -n 's/^snapshot_index=\([0-9][0-9]*\).*/\1/p')
[ -n "$snapshot_index" ] && [ "$snapshot_index" -ge 3 ] \
    || { echo "FAIL: node 1 did not persist the retried snapshot" >&2; exit 1; }
printf '%s\n' "$dump" | grep -q 'command=\[0\]' \
    || { echo "FAIL: node 1 did not receive the post-snapshot write" >&2; exit 1; }

echo "PASS: interrupted snapshot installation retried and normal replication resumed"
