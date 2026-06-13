#!/usr/bin/env bash
set -euo pipefail

shadow_data="${1:-shadow.data}"

grep -Rhasq "FAULT_INJECTION exiting after commit_index advanced to 1" "${shadow_data}/hosts/node3" \
    || { echo "FAIL: initial leader did not exit after majority replication" >&2; exit 1; }
grep -Rhasq "Applied log entry 1 to state machine" "${shadow_data}/hosts/node1" "${shadow_data}/hosts/node2" \
    || { echo "FAIL: replacement term never applied the earlier entry" >&2; exit 1; }

reference_entries=""
for node in 1 2 3; do
    raft_data=$(find "${shadow_data}/hosts/node${node}" -name "raft_data_${node}" -print -quit)
    [ -n "$raft_data" ] || { echo "FAIL: missing WAL for node ${node}" >&2; exit 1; }
    entries=$(cargo run --quiet --release --bin wal_dump -- "$raft_data" \
        | sed -n '/^  \[[0-9][0-9]*\] term=/p')
    if [ -z "$reference_entries" ]; then
        reference_entries="$entries"
    elif [ "$entries" != "$reference_entries" ]; then
        echo "FAIL: logs did not converge after leader replacement" >&2
        exit 1
    fi
done

printf '%s\n' "$reference_entries" | grep -q '^[[:space:]]*\[1\].*command=\[1\]$' \
    || { echo "FAIL: majority-replicated entry was lost" >&2; exit 1; }
printf '%s\n' "$reference_entries" | grep -q '^[[:space:]]*\[2\].*command=\[0\]$' \
    || { echo "FAIL: replacement leader did not commit a current-term entry" >&2; exit 1; }

echo "PASS: majority-replicated entry survived and committed under the next leader"
