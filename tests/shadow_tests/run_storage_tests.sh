#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo build --release

# tests/shadow_tests/assert_persistent_metadata_restart.sh

rm -rf shadow.data
shadow tests/shadow_tests/storage_failure_fail_stop/storage_failure_fail_stop.yaml > shadow.log

if ! grep -Rhasq "storage failure fail-stop checks passed" shadow.data; then
    echo "FAIL: storage failure probe did not complete" >&2
    exit 1
fi

echo "PASS: storage failures stop Raft participation"
