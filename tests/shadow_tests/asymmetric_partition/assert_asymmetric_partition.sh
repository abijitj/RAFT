#!/usr/bin/env bash
set -euo pipefail

shadow_data="${1:-shadow.data}"

grep -Rhasq "Dropping AppendEntries from node 3" "${shadow_data}/hosts/node1" "${shadow_data}/hosts/node2" \
    || { echo "FAIL: asymmetric block never dropped node 3's AppendEntries" >&2; exit 1; }
grep -Rhasq "Saw higher term" "${shadow_data}/hosts/node3" \
    || { echo "FAIL: isolated sender never observed the replacement leader's higher term" >&2; exit 1; }

reference_entries=""
for node in 1 2 3; do
    raft_data=$(find "${shadow_data}/hosts/node${node}" -name "raft_data_${node}" -print -quit)
    [ -n "$raft_data" ] || { echo "FAIL: missing WAL for node ${node}" >&2; exit 1; }
    entries=$(cargo run --quiet --release --bin wal_dump -- "$raft_data" \
        | sed -n '/^  \[[0-9][0-9]*\] term=/p')
    if [ -z "$reference_entries" ]; then
        reference_entries="$entries"
    elif [ "$entries" != "$reference_entries" ]; then
        echo "FAIL: logs did not converge after asymmetric recovery" >&2
        exit 1
    fi
done

printf '%s\n' "$reference_entries" | grep -q 'command=\[0\]' \
    || { echo "FAIL: replacement leader's write was not retained" >&2; exit 1; }
echo "PASS: cluster recovered and converged after a one-way partition"
