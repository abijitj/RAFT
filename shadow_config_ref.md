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
* `graph.directed`: (0 for undirected, 1 for directed)
* `node.id`: Unique integer identifier.
* `node.label`: Human-readable description.
* `node.host_bandwidth_down`: Default receive bandwidth for attached hosts.
* `node.host_bandwidth_up`: Default send bandwidth for attached hosts.

### Edge Attributes
* `edge.source`: Starting node ID.
* `edge.target`: Ending node ID.
* `edge.label`: Human-readable description.
* `edge.latency`: Delay added to packets (e.g., `50 ms`). Cannot be 0.
* `edge.jitter`: Delay variation (experimental/reserved).
* `edge.packet_loss`: Float between 0 and 1 representing drop chance (e.g., `0.01` for 1%).

---

## 3. `shadow.yaml` Configuration Options

### General Settings (`general.*`)
* `general.bootstrap_end_time`
* `general.data_directory`
* `general.heartbeat_interval`
* `general.log_level`
* `general.model_unblocked_syscall_latency`
* `general.parallelism`
* `general.progress`
* `general.seed`
* `general.stop_time`
* `general.template_directory`

### Network Settings (`network.*`)
* `network.graph`
* `network.graph.type`
* `network.graph.<file|inline>`
* `network.graph.file.path`
* `network.graph.file.compression`
* `network.use_shortest_path`

### Experimental Settings (`experimental.*`)
* `experimental.interface_qdisc`
* `experimental.max_unapplied_cpu_latency`
* `experimental.native_preemption_enabled`
* `experimental.native_preemption_native_interval`
* `experimental.native_preemption_sim_interval`
* `experimental.report_errors_to_stderr`
* `experimental.runahead`
* `experimental.scheduler`
* `experimental.socket_recv_autotune`
* `experimental.socket_recv_buffer`
* `experimental.socket_send_autotune`
* `experimental.socket_send_buffer`
* `experimental.strace_logging_mode`
* `experimental.unblocked_syscall_latency`
* `experimental.unblocked_vdso_latency`
* `experimental.use_cpu_pinning`
* `experimental.use_dynamic_runahead`
* `experimental.use_memory_manager`
* `experimental.use_new_tcp`
* `experimental.use_object_counters`
* `experimental.use_preload_libc`
* `experimental.use_preload_openssl_crypto`
* `experimental.use_preload_openssl_rng`
* `experimental.use_sched_fifo`
* `experimental.use_syscall_counters`
* `experimental.use_worker_spinning`

### Host Defaults (`host_option_defaults.*`)
* `host_option_defaults.log_level`
* `host_option_defaults.pcap_capture_size`
* `host_option_defaults.pcap_enabled`

### Host Specific Settings (`hosts.<hostname>.*`)
* `hosts.<hostname>.bandwidth_down`
* `hosts.<hostname>.bandwidth_up`
* `hosts.<hostname>.ip_addr`
* `hosts.<hostname>.network_node_id`
* `hosts.<hostname>.host_options`

**Process Configuration (`hosts.<hostname>.processes[*].*`)**
* `...args`
* `...environment`
* `...expected_final_state`
* `...path`
* `...shutdown_signal`
* `...shutdown_time`
* `...start_time`