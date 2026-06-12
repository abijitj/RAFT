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

### Workflow Loop
1. **Build:** `cargo build --release`
2. **Execute:** Run the simulation: `shadow shadow.yaml > shadow.log`
3. **Analyze Logs:** Inspect the `shadow.data/` directory.

---

## Profiling

The implementation collects WAL append latency, election latency, replication (commit) latency, and AppendEntries RPC latency. The results of these can be found under under `runs/shadow_tests/<test_name>/timing_report.txt`

The compaction threshold (default 50 entries) is configurable via the
`RAFT_COMPACTION_THRESHOLD` environment variable, which can be set to a very large
number to effectively disable compaction. To run the `log_compaction` test
both with and without compaction and compare resulting WAL sizes run the command: 

```bash
./tests/shadow_tests/compare_compaction.sh
```

It builds the project, runs `log_compaction.yaml` twice with different `RAFT_COMPACTION_THRESHOLD` values, dumps WALs for each variant under `runs/log_compaction_comparison/`, and prints a side-by-side `on_disk_size_bytes` table per node.

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