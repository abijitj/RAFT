#!/usr/bin/env bash
set -euo pipefail

shadow_data="${1:-shadow.data}"

grep -Rhasq "FAULT_INJECTION exiting after truncating conflicting suffix" "${shadow_data}/hosts/node3" \
    || { echo "FAIL: node 3 did not exit at the post-truncation crash point" >&2; exit 1; }

reference_entries=""
for node in 1 2 3; do
    raft_data=$(find "${shadow_data}/hosts/node${node}" -name "raft_data_${node}" -print -quit)
    [ -n "$raft_data" ] || { echo "FAIL: missing WAL for node ${node}" >&2; exit 1; }
    entries=$(cargo run --quiet --release --bin wal_dump -- "$raft_data" \
        | sed -n '/^  \[[0-9][0-9]*\] term=/p')
    [ -n "$entries" ] || { echo "FAIL: node ${node} has no recovered entry" >&2; exit 1; }
    if [ -z "$reference_entries" ]; then
        reference_entries="$entries"
    elif [ "$entries" != "$reference_entries" ]; then
        echo "FAIL: restarted node did not finish conflict repair" >&2
        exit 1
    fi
done

printf '%s\n' "$reference_entries" | grep -q 'command=\[0\]' \
    || { echo "FAIL: replacement entry is absent" >&2; exit 1; }
if printf '%s\n' "$reference_entries" | grep -q 'command=\[1\]'; then
    echo "FAIL: losing isolated entry reappeared after restart" >&2
    exit 1
fi
echo "PASS: conflict repair resumed after a crash following truncation"
