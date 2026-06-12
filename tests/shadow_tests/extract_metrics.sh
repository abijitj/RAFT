#!/usr/bin/env bash

set -uo pipefail

SHADOW_DATA="${1:-shadow.data}"
OUT_CSV="${2:-metrics.csv}"

HOSTS_DIR="${SHADOW_DATA}/hosts"
if [ ! -d "$HOSTS_DIR" ]; then
    echo "Error: ${HOSTS_DIR} not found" >&2
    exit 1
fi

echo "sim_time_s,host,event,fields" > "$OUT_CSV"

shopt -s nullglob

for host_dir in "$HOSTS_DIR"/*/; do
    host=$(basename "$host_dir")

    for log in "$host_dir"*.stdout; do
        awk -v host="$host" '
            /METRIC/ {
                ts = $0
                if (match(ts, /T[0-9]+:[0-9]+:[0-9]+\.[0-9]+Z/)) {
                    tstr = substr(ts, RSTART+1, RLENGTH-2)
                    split(tstr, hms, ":")
                    sim_time = hms[1]*3600 + hms[2]*60 + hms[3]
                } else {
                    sim_time = 0
                }

                idx = index($0, "METRIC ")
                rest = substr($0, idx + length("METRIC "))

                n = split(rest, parts, " ")
                event = parts[1]
                fields = ""
                for (i = 2; i <= n; i++) {
                    fields = (fields == "" ? parts[i] : fields ";" parts[i])
                }

                printf "%s,%s,%s,%s\n", sim_time, host, event, fields
            }
        ' "$log" >> "$OUT_CSV"
    done
done

shopt -u nullglob

echo "Wrote $(($(wc -l < "$OUT_CSV") - 1)) metric rows to ${OUT_CSV}"