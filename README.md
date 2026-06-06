# RAFT
An implementation of the RAFT Consensus protocol targeted towards embedded applications. 

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/YOUR-USERNAME/YOUR-REPO-NAME)

To avoid the complexities of cross-architecture compilation and local machine emulation (especially on Apple Silicon or ARM devices), this repository is configured to run entirely in the cloud using **GitHub Codespaces**. This provides a native `x86_64` Linux environment perfect for the **Shadow Discrete-Event Network Simulator**.

---

## Prerequisites

* A **GitHub Account**
* A modern web browser (or Visual Studio Code with the [GitHub Codespaces extension](https://marketplace.visualstudio.com/items?itemName=GitHub.codespaces) installed).

---

## Getting Started

### 1. Launching the Codespace
You can launch the cloud environment instantly by clicking the **"Open in GitHub Codespaces"** badge at the top of this README. 

Alternatively, you can launch it manually:
1. Navigate to the main page of this repository on GitHub.
2. Click the green **"<> Code"** button.
3. Select the **"Codespaces"** tab.
4. Click **"Create codespace on main"** (or the **"+"** icon).

A new browser tab will open with a fully functional VS Code environment. Wait a few minutes for the container to build and initialize.

---

## Compiling & Installing Shadow

Once your Codespace is running and terminal access is available at the bottom of the screen, you need to pull and compile Shadow. 

*Note: Free-tier Codespaces typically run on machines with limited RAM (e.g., 2 to 4 cores). We restrict compilation to a maximum of 4 parallel jobs to prevent the cloud container from crashing due to memory exhaustion.*

### 1. Clone and Build
Navigate to the home directory (outside of your RAFT workspace), clone the simulator, and run the compiler:

    cd ~
    git clone https://github.com/shadow/shadow.git
    cd shadow
    ./setup build --jobs 4

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