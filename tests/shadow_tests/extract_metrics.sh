#!/usr/bin/env bash
set -euo pipefail

SHADOW_DATA="${1:-shadow.data}"
OUT_CSV="${2:-metrics.csv}"

HOSTS_DIR="${SHADOW_DATA}/hosts"
if [ ! -d "$HOSTS_DIR" ]; then
    echo "Error: ${HOSTS_DIR} not found" >&2
    exit 1
fi

echo "sim_time_s,host,event,fields" > "$OUT_CSV"

for host_dir in "$HOSTS_DIR"/*/; do
    host=$(basename "$host_dir")

    # Shadow names process stdout files like raft.1000.stdout, etc.
    for log in "$host_dir"/*.stdout; do
        [ -e "$log" ] || continue

        grep -h "METRIC" "$log" | while IFS= read -r line; do
            # Extract the leading virtual-time field (seconds, possibly with ns).
            sim_time=$(echo "$line" | grep -oE '^[0-9]+\.[0-9]+' || echo "0")

            # Extract everything from "METRIC" onward.
            metric_part=$(echo "$line" | sed -n 's/.*METRIC \(.*\)/\1/p')
            event=$(echo "$metric_part" | awk '{print $1}')
            fields=$(echo "$metric_part" | cut -d' ' -f2- | tr ' ' ';')

            echo "${sim_time},${host},${event},${fields}" >> "$OUT_CSV"
        done
    done
done

echo "Wrote $(($(wc -l < "$OUT_CSV") - 1)) metric rows to ${OUT_CSV}"