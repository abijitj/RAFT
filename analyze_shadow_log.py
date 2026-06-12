import glob
import os
import re
import statistics
import sys
from collections import defaultdict


def parse_lines(lines, wal_times, rpc_times, election_latencies, replication_latencies, elections):
    for line in lines:
        if m := re.search(r"WAL_TIMING append_entry index=(\d+) elapsed_us=(\d+)", line):
            wal_times.append(int(m.group(2)))
        if m := re.search(r"ELECTION_LATENCY term=(\d+) elapsed_us=(\d+)", line):
            election_latencies.append(int(m.group(2)))
        if m := re.search(r"REPLICATION_LATENCY index=(\d+) elapsed_us=(\d+)", line):
            replication_latencies.append(int(m.group(2)))
        if m := re.search(r"RPC_TIMING append_entries elapsed_us=(\d+)", line):
            rpc_times.append(int(m.group(1)))
        if "became leader" in line or re.search(r"Transitioning to Leader for term \d+!", line):
            elections.append(line)


def read_file_lines(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.readlines()


def collect_log_paths(path):
    paths = []
    if os.path.isdir(path):
        paths.extend(glob.glob(os.path.join(path, "**", "*.stdout"), recursive=True))
        paths.extend(glob.glob(os.path.join(path, "**", "*.stderr"), recursive=True))
    elif os.path.isfile(path):
        paths.append(path)
        shadow_data_dir = os.path.join(os.path.dirname(path), "shadow.data")
        if os.path.isdir(shadow_data_dir):
            paths.extend(glob.glob(os.path.join(shadow_data_dir, "**", "*.stdout"), recursive=True))
            paths.extend(glob.glob(os.path.join(shadow_data_dir, "**", "*.stderr"), recursive=True))
    return sorted(set(paths))


def log_node_name(path):
    normalized = os.path.normpath(path)
    parts = normalized.split(os.sep)
    if "hosts" in parts:
        idx = parts.index("hosts")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return "shadow.log"


def main(path):
    elections, wal_times, rpc_times = [], [], []
    election_latencies = []
    replication_latencies = []

    node_stats = defaultdict(lambda: {
        "wal_times": [],
        "rpc_times": [],
        "elections": [],
        "election_latencies": [],
        "replication_latencies": [],
    })

    for log_path in collect_log_paths(path):
        node = log_node_name(log_path)
        lines = read_file_lines(log_path)
        parse_lines(
            lines,
            node_stats[node]["wal_times"],
            node_stats[node]["rpc_times"],
            node_stats[node]["election_latencies"],
            node_stats[node]["replication_latencies"],
            node_stats[node]["elections"],
        )

    overall = {
        "wal_times": [],
        "rpc_times": [],
        "elections": [],
        "election_latencies": [],
        "replication_latencies": [],
    }

    def summarize(name, vals, indent=2):
        prefix = " " * indent
        if not vals:
            print(f"{prefix}{name}: no samples")
            return
        print(f"{prefix}{name}: n={len(vals)} mean={statistics.mean(vals):.1f}us "
              f"p50={statistics.median(vals):.1f}us max={max(vals)}us")

    for node in sorted(node_stats):
        stats = node_stats[node]
        print(f"Node {node}:")
        summarize("WAL append latency", stats["wal_times"])
        summarize("AppendEntries RPC latency", stats["rpc_times"])
        summarize("Replication latency", stats["replication_latencies"])
        summarize("Election latency", stats["election_latencies"])
        print(f"  Leader elections observed: {len(stats['elections'])}")
        print()

        overall["wal_times"].extend(stats["wal_times"])
        overall["rpc_times"].extend(stats["rpc_times"])
        overall["elections"].extend(stats["elections"])
        overall["election_latencies"].extend(stats["election_latencies"])
        overall["replication_latencies"].extend(stats["replication_latencies"])
        election_latencies.extend(stats["election_latencies"])
        replication_latencies.extend(stats["replication_latencies"])

    print("Overall:")
    summarize("WAL append latency", overall["wal_times"], indent=2)
    summarize("AppendEntries RPC latency", overall["rpc_times"], indent=2)
    summarize("Replication latency", replication_latencies, indent=2)
    summarize("Election latency", election_latencies, indent=2)
    print(f"  Leader elections observed: {len(overall['elections'])}")


if __name__ == "__main__":
    main(sys.argv[1])