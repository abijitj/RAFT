#!/usr/bin/env bash
set -euo pipefail

shadow_data="${1:-shadow.data}"
reference_entries=""
max_term=0

for node in 1 2 3 4 5; do
    raft_data=$(find "${shadow_data}/hosts/node${node}" -name "raft_data_${node}" -print -quit)
    [ -n "$raft_data" ] || { echo "FAIL: missing WAL for node ${node}" >&2; exit 1; }

    dump=$(cargo run --quiet --release --bin wal_dump -- "$raft_data")
    term=$(printf '%s\n' "$dump" | sed -n 's/^term=\([0-9][0-9]*\).*/\1/p')
    entries=$(printf '%s\n' "$dump" | sed -n '/^  \[[0-9][0-9]*\] term=/p')
    [ -n "$entries" ] || { echo "FAIL: node ${node} has an empty log" >&2; exit 1; }
    [ "$term" -gt "$max_term" ] && max_term="$term"

    if [ -z "$reference_entries" ]; then
        reference_entries="$entries"
    elif [ "$entries" != "$reference_entries" ]; then
        echo "FAIL: node ${node} did not retain the leader-complete log" >&2
        exit 1
    fi
done

[ "$max_term" -ge 3 ] || { echo "FAIL: scenario did not cross at least three terms" >&2; exit 1; }
printf '%s\n' "$reference_entries" | grep -q '^[[:space:]]*\[1\].*command=\[1\]$' \
    || { echo "FAIL: the originally committed entry was not retained at index 1" >&2; exit 1; }
printf '%s\n' "$reference_entries" | grep -q 'command=\[0\]' \
    || { echo "FAIL: no later-term entry was committed after recovery" >&2; exit 1; }

echo "PASS: committed entry survived repeated elections and later leadership"
