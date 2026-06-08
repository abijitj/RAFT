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
    sudo make install

---

## Running Your RAFT Simulations

With Shadow and TGen installed, transition to your workspace:

    cd /workspaces/RAFT

### Workflow Loop
1. **Build Your Nodes:** `cargo build --release`
2. **Execute:** Run the simulation: `shadow shadow.yaml > shadow.log`
3. **Analyze Logs:** Inspect the `shadow.data/` directory.
