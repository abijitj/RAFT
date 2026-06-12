#!/usr/bin/env bash
# compare_compaction.sh
#
# Runs a shadow test twice — once with compaction enabled
# and once with compaction effectively disabled — and compares the
# resulting on-disk WAL sizes for each host by summing raft_data_* file sizes.
#
# Only a single summary file is preserved; per-node WAL dump files are not kept.
#
# Run from the repository root.

set -euo pipefail

DEFAULT_TEST_YAML="tests/shadow_tests/log_compaction/log_compaction.yaml"
TEST_YAML="${1:-$DEFAULT_TEST_YAML}"

if [ "$#" -gt 1 ]; then
    echo "Usage: $0 [test_yaml]" >&2
    exit 1
fi

if [ ! -f "$TEST_YAML" ]; then
    echo "Error: test YAML '$TEST_YAML' does not exist." >&2
    exit 1
fi

TEST_NAME=$(basename "$TEST_YAML" .yaml)
RESULTS_DIR="runs/shadow_tests/${TEST_NAME}/compaction_comparison"
SUMMARY_FILE="${RESULTS_DIR}/compaction_comparison_summary.txt"

declare -A with_counts=()
declare -A without_counts=()
declare -A hosts=()

mkdir -p "$RESULTS_DIR"
> "$SUMMARY_FILE"
cargo build --release

create_variant_yaml () {
    local src_yaml="$1"
    local dst_yaml="$2"
    local threshold="$3"

    python3 - "$src_yaml" "$dst_yaml" "$threshold" <<'PY'
import sys, yaml
src, dst, threshold = sys.argv[1], sys.argv[2], sys.argv[3]
with open(src) as f:
    data = yaml.safe_load(f)

def patch(obj):
    if isinstance(obj, dict):
        if 'processes' in obj and isinstance(obj['processes'], list):
            for proc in obj['processes']:
                if isinstance(proc, dict):
                    env = proc.get('environment', {}) or {}
                    env['RAFT_COMPACTION_THRESHOLD'] = str(threshold)
                    proc['environment'] = env
        for v in obj.values():
            patch(v)
    elif isinstance(obj, list):
        for item in obj:
            patch(item)

patch(data)
with open(dst, 'w') as f:
    yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
PY
}

run_variant () {
    local label="$1"
    local threshold="$2"
    local -n count_ref=$3

    echo "=== Running ${TEST_YAML} with RAFT_COMPACTION_THRESHOLD=${threshold} (${label}) ==="

    rm -rf shadow.data
    [ -f shadow.log ] && rm -f shadow.log

    temp_yaml="${RESULTS_DIR}/${label}_shadow.yaml"
    create_variant_yaml "$TEST_YAML" "$temp_yaml" "$threshold"
    shadow "$temp_yaml" > shadow.log

    for wal_dir in shadow.data/hosts/*/; do
        [ -d "$wal_dir" ] || continue
        host=$(basename "$wal_dir")

        entry_count=0
        while IFS= read -r -d '' raft_data; do
            # count only actual stored entries; wal_dump prints missing slots too when log_length is the max entry index
            count=$(cargo run --release --bin wal_dump -- "$raft_data" 2>/dev/null | grep -c '^  \[[0-9]\+\] term=' || true)
            entry_count=$((entry_count + count))
        done < <(find "$wal_dir" -maxdepth 1 -type f -name 'raft_data_*' -print0)

        if [ "$entry_count" -eq 0 ]; then
            echo "  (no raft_data_* file found for $host)"
            continue
        fi

        count_ref["$host"]="$entry_count"
        hosts["$host"]=1
        echo "  recorded $host entries=$entry_count"
    done

    rm -rf shadow.data shadow.log
}

# "with" = default compaction threshold (50, as set in core.rs)
run_variant "with_compaction" 50 with_counts

# "without" = threshold effectively unreachable for this test's 50-write workload
run_variant "without_compaction" 1000000000 without_counts

{
    echo "Test: $TEST_YAML"
    echo "Summary file: $SUMMARY_FILE"
    echo ""
    echo "=== WAL entry count comparison ==="
    printf "% -10s % -15s % -15s\n" "host" "with_entries" "without_entries"
    for host in $(printf '%s\n' "${!hosts[@]}" | sort); do
        with_entries=${with_counts[$host]:-N/A}
        without_entries=${without_counts[$host]:-N/A}
        printf "%-10s %-15s %-15s\n" "$host" "${with_entries}" "${without_entries}"
    done
} | tee "$SUMMARY_FILE"

echo ""
echo "Summary written to ${SUMMARY_FILE}"
