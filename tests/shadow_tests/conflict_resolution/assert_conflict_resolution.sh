#!/usr/bin/env bash
set -euo pipefail

shadow_data="${1:-shadow.data}"

if ! grep -Rhasq "Received test client command (true). Appending to log" "${shadow_data}/hosts/node3"; then
    echo "FAIL: node 3 never appended the isolated leader entry" >&2
    exit 1
fi

if ! grep -Rhasq "Log conflict at index .* Truncating suffix" "${shadow_data}/hosts/node3"; then
    echo "FAIL: node 3 never detected and truncated a conflicting log suffix" >&2
    exit 1
fi

reference_entries=""
for node in 1 2 3; do
    host_dir="${shadow_data}/hosts/node${node}"
    raft_data=$(find "$host_dir" -name "raft_data_${node}" -print -quit)
    if [ -z "$raft_data" ]; then
        echo "FAIL: no WAL found for node ${node}" >&2
        exit 1
    fi

    entries=$(cargo run --quiet --release --bin wal_dump -- "$raft_data" \
        | sed -n '/^  \[[0-9][0-9]*\] term=/p')
    if [ -z "$entries" ]; then
        echo "FAIL: node ${node} has no replicated log entries" >&2
        exit 1
    fi

    if [ -z "$reference_entries" ]; then
        reference_entries="$entries"
    elif [ "$entries" != "$reference_entries" ]; then
        echo "FAIL: node ${node} did not converge to the same log as node 1" >&2
        exit 1
    fi
done

if ! printf '%s\n' "$reference_entries" | grep -q 'command=\[0\]'; then
    echo "FAIL: converged log does not contain the majority leader's false entry" >&2
    exit 1
fi

if printf '%s\n' "$reference_entries" | grep -q 'command=\[1\]'; then
    echo "FAIL: node 3's isolated true entry survived conflict resolution" >&2
    exit 1
fi

echo "PASS: divergent follower suffix was truncated and replaced"
