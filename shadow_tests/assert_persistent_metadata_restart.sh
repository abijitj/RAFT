#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

rm -rf shadow.data
shadow shadow_tests/persistent_metadata_restart.yaml > shadow.log

if ! grep -Rhasq "Loaded persistent Raft state" shadow.data; then
    echo "FAIL: no Raft startup logs found under shadow.data" >&2
    exit 1
fi

if ! grep -Rha "Loaded persistent Raft state" shadow.data |
    grep -q "current_term=1, voted_for=Some(1)"; then
    echo "FAIL: restarted node did not restore term 1 and vote for node 1" >&2
    exit 1
fi

if ! grep -Rha "Election timeout expired" shadow.data |
    grep -q "Initiating election for term 2"; then
    echo "FAIL: restarted node did not continue with term 2" >&2
    exit 1
fi

echo "PASS: persistent current_term and voted_for survived restart"
