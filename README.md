# RAFT
An implementation of the RAFT Consensus protocol targeted towards embedded applications. 

---

## Prerequisites

Before getting started, ensure you have the following installed on your host machine:

* **Docker Desktop** (version 4.20+ recommended)
* **Visual Studio Code**
* **VS Code Extensions:**
    * [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

---

## Getting Started

### 1. Launching the Dev Container
1. Clone this repository to your local machine.
2. Open the repository folder in VS Code.
3. A popup should appear in the bottom-right corner asking to **"Reopen in Container"**. Click it.
    * *Alternative:* Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows), type `Dev Containers: Reopen in Container`, and hit Enter.
4. VS Code will pull the base Linux image, apply configurations, and launch your sandboxed workspace.

---

## Host-Specific Optimizations (Crucial)

Shadow is a low-level Linux application that intercepts system calls. Because compilation involves heavy x86_64 machine code translation and parallel processing, perform the relevant optimization for your host system below before building.

### Apple Silicon Macs (M1/M2/M3/M4)
Because you are emulating an `amd64` Linux kernel via Rosetta 2, parallel compilation requires significant memory allocations to prevent the Linux kernel from issuing a `SIGKILL (Signal 9)` error.

1. Open **Docker Desktop** on your Mac.
2. Navigate to **Settings** (Gear Icon) > **Resources**.
3. Set the **Memory** allocation to a minimum of **8 GB** (12 GB or higher is highly recommended).
4. Click **Apply & Restart**.

### Windows 11 / 10 (Intel, AMD, or Snapdragon)
Windows handles the container natively using the WSL2 backend.
* **Intel/AMD:** No emulation overhead is required.
* **ARM-based Windows (Snapdragon/Copilot+):** WSL2 automatically handles the x86 translation seamlessly. 
* Ensure your WSL2 instance is updated by running `wsl --update` in a Windows PowerShell prompt prior to starting Docker.

---

## Compiling & Installing Shadow

Once your Dev Container is running and terminal access is available, compile Shadow by controlling your thread count to prevent resource exhaustion.

### 1. Run the Build Toolchain
Navigate to the Shadow source directory, create a build directory, and run the compiler. Restrict compilation to a maximum of 4 parallel jobs to stay within container resource constraints:

```bash
cd ~/shadow
./setup build --only-generate
cd build
make -j 4
```

### 2. Install Globally
Once compilation reaches 100% successfully, complete the installation:

```bash
cd ~/shadow
./setup install
```

### 3. Expose Shadow to your Environment Path
Add the local binary prefix to your container's bash profile and refresh your terminal environment:

```bash
echo 'export PATH="${PATH}:/home/vscode/.local/bin"' >> ~/.bashrc
source ~/.bashrc
```

Verify the installation is successful by checking the simulator version:
```bash
shadow --version
```

---

## Running Your RAFT Simulations

With Shadow fully installed, you can transition over to your active RAFT workspace.

```bash
cd /workspaces/RAFT
```

### Workflow Loop
1. **Build Your Nodes:** Compile your Rust-based RAFT binaries natively within the container:
   ```bash
   cargo build --release
   ```
2. **Define the Network:** Modify or inspect your `shadow.yaml` configuration file. This file manages your simulation topology, network latencies, node counts, and binary execution paths.
3. **Execute:** Execute the simulation runner:
   ```bash
   shadow shadow.yaml
   ```
4. **Analyze Logs:** Inspect the generated `shadow.data/` directory to evaluate consensus timings, RPC states, and network performance indicators.