# RAFT
An implementation of the RAFT Consensus protocol targeted towards embedded applications. 

---

## Getting Started

### 1. Launching the Codespace
You can launch the cloud environment instantly by clicking the **"Open in GitHub Codespaces"** button below. 

Alternatively, you can launch it manually:
1. Navigate to the main page of this repository on GitHub.
2. Click the green **"<> Code"** button.
3. Select the **"Codespaces"** tab.
4. Click **"Create codespace on main"** (or the **"+"** icon).

A new browser tab will open with a fully functional VS Code environment. Wait a few minutes for the container to build and initialize.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/YOUR-USERNAME/YOUR-REPO-NAME)

---

## Compiling & Installing Shadow

Once your Codespace is running and terminal access is available at the bottom of the screen, you need to compile Shadow.

### 1. Run the Build Toolchain
Navigate to the Shadow source directory, create a build directory, and run the compiler:

    cd ~/shadow
    ./setup build --only-generate
    cd build
    make -j 4

### 2. Install Globally
Once compilation reaches 100% successfully, complete the installation:

    cd ~/shadow
    ./setup install

### 3. Expose Shadow to your Environment Path
Add the local binary prefix to your container's bash profile and refresh your terminal environment:

    echo 'export PATH="${PATH}:/home/vscode/.local/bin"' >> ~/.bashrc
    source ~/.bashrc

Verify the installation is successful by checking the simulator version:

    shadow --version

---

## Running Your RAFT Simulations

With Shadow fully installed, transition back to your active RAFT workspace to begin testing.

    cd /workspaces/RAFT

### Workflow Loop
1. **Build Your Nodes:** Compile your Rust-based RAFT binaries natively within the Codespace:
    
    cargo build --release

2. **Define the Network:** Modify or inspect your `shadow.yaml` configuration file. This file manages your simulation topology, network latencies, node counts, and binary execution paths.
3. **Execute:** Execute the simulation runner:
    
    shadow shadow.yaml

4. **Analyze Logs:** Inspect the generated `shadow.data/` directory to evaluate consensus timings, RPC states, and network performance indicators.