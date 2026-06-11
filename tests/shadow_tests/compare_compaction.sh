#!/usr/bin/env bash
set -euo pipefail

TEST_YAML="tests/shadow_tests/log_compaction/log_compaction.yaml"
RESULTS_DIR="runs/log_compaction_comparison"

mkdir -p "$RESULTS_DIR"
cargo build --release

run_variant () {
    local label="$1"
    local threshold="$2"
    local out_dir="${RESULTS_DIR}/${label}"

    echo "=== Running log_compaction with RAFT_COMPACTION_THRESHOLD=${threshold} (${label}) ==="

    rm -rf shadow.data
    [ -f shadow.log ] && rm -f shadow.log

    RAFT_COMPACTION_THRESHOLD="$threshold" shadow "$TEST_YAML" > shadow.log

    mkdir -p "$out_dir"
    cp shadow.log "$out_dir/"

    for wal_dir in shadow.data/hosts/*/; do
        host=$(basename "$wal_dir")
        raft_data=$(find "$wal_dir" -name "raft_data_*" | head -1)
        if [ -n "$raft_data" ]; then
            mkdir -p "${out_dir}/${host}"
            RAFT_COMPACTION_THRESHOLD="$threshold" cargo run --release --bin wal_dump -- "$raft_data" \
                > "${out_dir}/${host}/${host}_wal.txt" 2>/dev/null \
                || echo "  (dump failed for $host)"
        fi
    done

    cp -r shadow.data "${out_dir}/"
}

# "with" = default compaction threshold (50, as set in core.rs)
run_variant "with_compaction" 50

# "without" = threshold effectively unreachable for this test's 50-write workload
run_variant "without_compaction" 1000000000

echo ""
echo "=== WAL size comparison (on_disk_size_bytes) ==="
printf "%-10s %-20s %-20s\n" "host" "with_compaction" "without_compaction"

for wal_dir in shadow.data/hosts/*/; do
    host=$(basename "$wal_dir")
    with_size=$(grep -h "on_disk_size_bytes" "${RESULTS_DIR}/with_compaction/${host}/${host}_wal.txt" 2>/dev/null \
        | sed -n 's/on_disk_size_bytes=\([0-9]*\)/\1/p')
    without_size=$(grep -h "on_disk_size_bytes" "${RESULTS_DIR}/without_compaction/${host}/${host}_wal.txt" 2>/dev/null \
        | sed -n 's/on_disk_size_bytes=\([0-9]*\)/\1/p')
    printf "%-10s %-20s %-20s\n" "$host" "${with_size:-N/A}" "${without_size:-N/A}"
done

echo ""
echo "Full results (shadow.data, shadow.log, wal dumps) saved under ${RESULTS_DIR}/"