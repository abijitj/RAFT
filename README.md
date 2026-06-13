# RAFT
An implementation of the RAFT Consensus protocol targeted towards embedded applications. 

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/abijitj/RAFT?machine=standardLinux32gb)

---

## Getting Started

### 1. Launching the Codespace
Click the badge above to launch a 16GB RAM cloud instance. Wait a few minutes for the `postCreateCommand` to install all necessary system dependencies, including `libigraph-dev` (required for TGen).

---

## Compiling & Installing Shadow & TGen

### 1. Shadow Simulator
Navigate to home, clone Shadow, and build:

    cd ~
    git clone https://github.com/shadow/shadow.git
    cd shadow
    ./setup build --jobs 4
    ./setup install
    echo 'export PATH="${PATH}:/home/vscode/.local/bin"' >> ~/.bashrc
    source ~/.bashrc

### 2. TGen (Traffic Generator)
Clone and install the TGen dependency to run the network verification:

    cd ~
    git clone https://github.com/shadow/tgen.git
    cd tgen
    mkdir build && cd build
    cmake ..
    make -j 4
    make install

---

## Running RAFT Simulations

With Shadow and TGen installed, transition to the workspace:

    cd /workspaces/RAFT

### Testing Loop
1. Run `bash run_shadow.sh` 
2. Select the desired test from the options
3. Inspect `shadow.data/` for node debug logs and `runs/<test_name>` for write-ahead-log (WAL) data 

---

## Profiling

The implementation collects WAL append latency, election latency, replication (commit) latency, and AppendEntries RPC latency. The results of these can be found under under `runs/shadow_tests/<test_name>/timing_report.txt`

The compaction threshold (default 50 entries) is configurable via the
`RAFT_COMPACTION_THRESHOLD` environment variable, which can be set to a very large
number to effectively disable compaction.

The `tests/shadow_tests/compare_compaction.sh` script compares the number
of WAL entries stored for each node with and without compaction.

To compare compaction behavior for the default `log_compaction` test, run:

```bash
./tests/shadow_tests/compare_compaction.sh
```

To compare compaction for any other shadow test, pass the test YAML path:

```bash
./tests/shadow_tests/compare_compaction.sh tests/shadow_tests/<test_dir>/<test>.yaml
```

You can also invoke this from `run_shadow.sh`:

```bash
./run_shadow.sh --compare-compaction tests/shadow_tests/<test_dir>/<test>.yaml
```

The script builds the project, runs the selected test twice with different
`RAFT_COMPACTION_THRESHOLD` values, sums the on-disk size of each host's
`raft_data_*` WAL files, dumps WAL contents for inspection, and prints a
side-by-side size comparison for each node.

---

## Advanced Shadow Configuration & Network Graph Setup

To rigorously test the RAFT protocol's fault tolerance and performance, it is necessary to customize the simulated network environment. Shadow uses a `shadow.yaml` configuration file to dictate the network topology and process behaviors.

For a complete breakdown of all `shadow.yaml` options, network graph attributes, and formatting rules, please see the **[Shadow Configuration Reference](shadow_config_ref.md)**.

### Quick Start: Defining the Network Graph (GML)
Shadow routes all inter-process communication through an internal routing module. Realistic internet paths can be modelled by defining a network graph using the GML format within the `network.graph` section of the config. 

**Example Custom Topology:**
```yaml
network:
  graph:
    type: gml
    inline: |
      graph [
        directed 0
        node [
          id 0
          host_bandwidth_down "1 Gbit"
          host_bandwidth_up "1 Gbit"
        ]
        node [
          id 1
          host_bandwidth_down "100 Mbit"
          host_bandwidth_up "100 Mbit"
        ]
        # Self-loops required for local host communication
        edge [ source 0 target 0 latency "1 ms" packet_loss 0.0 ]
        edge [ source 1 target 1 latency "1 ms" packet_loss 0.0 ]
        # Path between nodes
        edge [
          source 0
          target 1
          latency "50 ms"
          packet_loss 0.01  # 1% chance of dropped packets
        ]
      ]