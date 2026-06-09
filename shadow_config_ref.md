# Shadow Configuration Reference

This document serves as a comprehensive reference for configuring the Shadow simulator, detailing the formatting rules, network graph attributes, and `shadow.yaml` options.

---

## 1. Formatting Rules & Units

Shadow requires strict, case-sensitive formatting for quantities. A space between the magnitude and unit is optional (e.g., `5Mbit` or `5 Mbits`). Pluralized units are accepted.

### Time
Time values are expressed as either sub-second units, seconds, minutes, or hours.
* **Nanosecond:** `nanosecond`, `ns`
* **Microsecond:** `microsecond`, `us`, `μs`
* **Millisecond:** `millisecond`, `ms`
* **Second:** `second`, `sec`, `s`
* **Minute:** `minute`, `min`, `m`
* **Hour:** `hour`, `hr`, `h`
* *Examples:* `30 s`, `2 hr`, `10 minutes`, `100 ms`

### Bandwidth
Expressed in bits-per-second. **Must be divisible by 8.**
* **Base Unit:** `bit`
* **Prefixes:** `kilo` / `K`, `kibi` / `Ki`, `mega` / `M`, `mebi` / `Mi`, `giga` / `G`, `gibi` / `Gi`, `tera` / `T`, `tebi` / `Ti`
* *Examples:* `100 Mbit`, `100 Mbits`, `10 kilobits`, `128 bits`

### Byte Sizes
* **Base Unit:** `byte`, `B`
* **Prefixes:** `kilo` / `K`, `kibi` / `Ki`, `mega` / `M`, `mebi` / `Mi`, `giga` / `G`, `gibi` / `Gi`, `tera` / `T`, `tebi` / `Ti`
* *Examples:* `20 B`, `100 MB`, `100 megabyte`, `10 kibibytes`, `30 MiB`, `1024 Mbytes`

### Unix Signals
Used for process shutdown configurations. 
* **Format:** String name (e.g., `SIGKILL`) or integer (e.g., `9`).
* **Note:** String names must be capitalized and include the `SIG` prefix. Realtime signals (32+) are not supported.

---

## 2. Network Graph Attributes (GML)

These attributes define the routing topology within the `network.graph` definition.

### Graph & Node Attributes
* `graph.directed`: `0` for undirected (symmetric paths), `1` for directed (asymmetric paths).
* `node.id`: Unique integer identifier for a specific point in the network.
* `node.label`: An optional, human-readable description of the node.
* `node.host_bandwidth_down`: The default receive bandwidth allowed for any host attached to this node.
* `node.host_bandwidth_up`: The default send bandwidth allowed for any host attached to this node.

### Edge Attributes
* `edge.source`: Starting node ID for this path.
* `edge.target`: Ending node ID for this path.
* `edge.label`: An optional, human-readable description of the edge.
* `edge.latency`: Simulated delay added to packets traversing this edge (e.g., `50 ms`). Cannot be `0`.
* `edge.jitter`: Delay variation (currently reserved for future use/experimental).
* `edge.packet_loss`: A float between `0` and `1` representing the probability a packet is dropped (e.g., `0.01` for 1% loss).

---

## 3. `shadow.yaml` Configuration Options

### General Settings (`general.*`)
Controls the overarching behavior of the simulation engine.

* `general.bootstrap_end_time`: The simulated time that ends a period of unrestricted network bandwidth and 0% packet drop. Useful for quickly booting large networks before applying constraints.
* `general.data_directory`: Path to store the simulation output logs (defaults to `shadow.data`).
* `general.heartbeat_interval`: The simulated time interval at which Shadow logs simulation performance statistics.
* `general.log_level`: Controls the verbosity of Shadow's internal engine logs (`error`, `warning`, `info`, `debug`, `trace`).
* `general.model_unblocked_syscall_latency`: Boolean. If `true`, Shadow advances simulated time slightly for non-blocking syscalls to prevent infinite 0-time busy loops.
* `general.parallelism`: Number of physical CPU cores/threads Shadow will use to run the simulation in parallel (`0` lets Shadow choose automatically).
* `general.progress`: Boolean. Whether to print simulation progress indicators to stdout.
* `general.seed`: An integer used to initialize the RNG. Hardcoding this guarantees 100% deterministic simulation runs.
* `general.stop_time`: **Required.** The simulated time when the simulation forcefully ends.
* `general.template_directory`: A path to a directory that will be recursively copied into the `data_directory` during startup (useful for injecting configuration files into virtual hosts).

### Network Settings (`network.*`)
Dictates how the virtual routing topology is defined.

* `network.graph`: **Required.** Container for the graph definition.
* `network.graph.type`: The type of graph. Usually `"gml"` (for custom setups) or a built-in like `"1_gbit_switch"`.
* `network.graph.<file|inline>`: Defines whether to provide the graph as a literal `inline` string in the YAML, or point to an external `file`.
* `network.graph.file.path`: Path to an external network graph file (e.g., a `.gml` file).
* `network.graph.file.compression`: Specifies if the external graph file is compressed (e.g., `"xz"`).
* `network.use_shortest_path`: Boolean. If `true` (default), Shadow computes routes using Dijkstra's algorithm. If `false`, Shadow assumes a fully-meshed graph where every node connects directly.

### Experimental Settings (`experimental.*`)
*Note: These are unstable performance/system tuning options that can alter simulation behavior.*

* `experimental.interface_qdisc`: The queueing discipline at the virtual network interface (e.g., `"fifo"` or `"round-robin"`).
* `experimental.max_unapplied_cpu_latency`: Max simulated time allowed to accumulate before the clock jumps forward.
* `experimental.native_preemption_enabled`: Boolean. Preempts managed code that runs too long without yielding (helps escape pure-CPU busy loops).
* `experimental.native_preemption_native_interval`: Wall-clock CPU time to wait before preempting a busy loop.
* `experimental.native_preemption_sim_interval`: Simulated time to jump forward after a preemption.
* `experimental.report_errors_to_stderr`: Boolean. Mirrors errors to `stderr`.
* `experimental.runahead`: Sets a minimum network latency limit to allow better multi-threaded parallelization.
* `experimental.scheduler`: The threading model used (e.g., `"thread_per_core"` vs `"thread_per_host"`).
* `experimental.socket_recv_autotune` / `socket_send_autotune`: Boolean to enable TCP buffer auto-tuning.
* `experimental.socket_recv_buffer` / `socket_send_buffer`: Hardcode specific socket buffer sizes.
* `experimental.strace_logging_mode`: Verbosity of intercepted syscall tracing.
* `experimental.unblocked_syscall_latency` / `unblocked_vdso_latency`: Specific simulated time penalties for non-blocking syscalls.
* `experimental.use_cpu_pinning`: Boolean. Pins worker threads to physical CPU cores for better cache performance.
* `experimental.use_dynamic_runahead`: Boolean. Allows Shadow to adjust the runahead window dynamically.
* `experimental.use_memory_manager`: Boolean. Uses shared memory maps to reduce overhead when Shadow reads managed processes.
* `experimental.use_new_tcp`: Boolean. Toggle for internal TCP stack implementations.
* `experimental.use_object_counters` / `use_syscall_counters`: Boolean flags to enable internal debugging metrics.
* `experimental.use_preload_libc` / `use_preload_openssl_crypto` / `use_preload_openssl_rng`: Boolean flags to control library preloading (critical for overriding RNG functions to maintain determinism).
* `experimental.use_sched_fifo`: Boolean. Tells Shadow to use a real-time FIFO scheduler.
* `experimental.use_worker_spinning`: Boolean. Uses spin-locks instead of yielding for thread synchronization.

### Host Defaults (`host_option_defaults.*`)
Applies baseline settings to all virtual machines.

* `host_option_defaults.log_level`: Default verbosity for application output.
* `host_option_defaults.pcap_capture_size`: Bytes to capture per packet if PCAP is enabled.
* `host_option_defaults.pcap_enabled`: Boolean. Generates `.pcap` files for network traffic analysis using Wireshark.

### Host Specific Settings (`hosts.<hostname>.*`)
Defines the individual virtual machines within your network.

* `hosts.<hostname>.network_node_id`: **Required.** The integer ID matching a node in your network graph. Maps the host to a physical topology location.
* `hosts.<hostname>.bandwidth_down` / `bandwidth_up`: Overrides the default node bandwidth for this specific virtual machine.
* `hosts.<hostname>.ip_addr`: Manually assigns an IPv4 address to the host instead of letting Shadow auto-assign it.
* `hosts.<hostname>.host_options`: Override `host_option_defaults` specifically for this machine (like turning PCAP on for only one node).

**Process Configuration (`hosts.<hostname>.processes[*].*`)**
Defines the applications executed by a specific virtual host.

* `...path`: **Required.** Path to the compiled binary (e.g., your RAFT node executable).
* `...args`: Command-line arguments passed to the binary upon execution.
* `...environment`: Environment variables set for the process (can be a dictionary or a string array).
* `...start_time`: The simulated time at which this process boots up.
* `...shutdown_time`: The simulated time to forcefully terminate the process (excellent for testing RAFT node crashes).
* `...shutdown_signal`: The specific signal sent at shutdown (e.g., `SIGKILL`, `SIGTERM`, or `9`).
* `...expected_final_state`: The state the process should be in at the end of `stop_time` (`running`, `exited`, `killed`). Shadow will flag an error if the process crashes unexpectedly before the end of the simulation.