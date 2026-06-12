/**
 * initialize storage
 * instantiate core struct + start networking gRPC server/client
 * start the network server, network client and core in separate threads using tokio::spawn
 */

use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use tonic::transport::Channel;
use tokio::sync::mpsc;
use serde::Deserialize;

mod core;
mod network;
mod storage;

use core::events::RaftEvent;
use network::server::RaftServerImpl;
use network::pb::raft_server::RaftServer;
use network::pb::raft_client::RaftClient;
use network::client::RaftClientWorker;
use storage::log::UnixWal;

use std::sync::Arc;
use network::filter::{BlockRule, NetworkFilter};

use log::{error, info, warn};

/// Struct to map the JSON configuration file
#[derive(Deserialize)]
struct ClusterConfig {
    nodes: HashMap<u64, String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(
        env_logger::Env::default()
        .default_filter_or("info")
    )
    .target(env_logger::Target::Stdout)
    .format_timestamp_millis()
    .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        error!("Usage: {} <NODE_ID> <CONFIG_PATH>", args[0]);
        std::process::exit(1);
    }

    let my_id: u64 = args[1].parse().expect("Node ID must be an integer");
    let config_path = &args[2];

    // Vectors to hold multiple time-triggered injector states
    let mut write_ms: Vec<u128> = Vec::new();
    let mut write_values: Vec<bool> = Vec::new();
    let mut block_rules: Vec<BlockRule> = Vec::new();

    // Parse the optional arguments if present
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--block" => {
                if i + 5 >= args.len() {
                    error!(
                        "Usage: --block <NODE_ID> --from <START> --to <END>"
                    );
                    std::process::exit(1);
                }

                let sender_id: u64 =
                    args[i + 1].parse()
                        .expect("Node ID must be an integer");

                if args[i + 2] != "--from" {
                    error!("Expected --from");
                    std::process::exit(1);
                }

                let start_ms: u128 =
                    args[i + 3].parse()
                        .expect("Invalid start time");

                if args[i + 4] != "--to" {
                    error!("Expected --to");
                    std::process::exit(1);
                }

                let end_ms: u128 =
                    args[i + 5].parse()
                        .expect("Invalid end time");

                block_rules.push(BlockRule {
                    sender_id,
                    start_ms,
                    end_ms,
                });

                i += 6;
            }
            "--write-at-second" => {
                if i + 1 < args.len() {
                    let seconds: u128 = args[i + 1].parse().expect("Seconds must be an integer");
                    write_ms.push(seconds.saturating_mul(1000));
                    i += 2;
                } else {
                    error!("Error: --write-at-second requires a value");
                    std::process::exit(1);
                }
            }
            "--write-at-ms" => {
                if i + 1 < args.len() {
                    let ms: u128 = args[i + 1].parse().expect("Milliseconds must be an integer");
                    write_ms.push(ms);
                    i += 2;
                } else {
                    error!("Error: --write-at-ms requires a value");
                    std::process::exit(1);
                }
            }
            "--write-value" => {
                if i + 1 < args.len() {
                    let val = args[i + 1].parse().expect("Value must be true or false");
                    write_values.push(val);
                    i += 2;
                } else {
                    error!("Error: --write-value requires a boolean value");
                    std::process::exit(1);
                }
            }
            _ => {
                error!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
    }

    // Safety check: ensure every timestamp has a corresponding value matching it
    if write_ms.len() != write_values.len() {
        error!(
            "Error: Mismatch between scheduled writes count. Found {} timestamps and {} values.",
            write_ms.len(),
            write_values.len()
        );
        std::process::exit(1);
    }

    let config_data = fs::read_to_string(config_path)
        .expect("Failed to read configuration file");
    let config: ClusterConfig = serde_json::from_str(&config_data)
        .expect("Failed to parse JSON config");

    let my_listen_addr_str = config.nodes.get(&my_id)
        .unwrap_or_else(|| panic!("Node ID {} was not found in the config file!", my_id));
    let my_listen_addr: SocketAddr = my_listen_addr_str.parse()?;

    let mut peer_configs = HashMap::new();
    for (&node_id, address) in &config.nodes {
        if node_id != my_id {
            // Tonic requires the "http://" prefix for its URIs
            peer_configs.insert(node_id, format!("http://{}", address));
        }
    }

    // 2. Instantiate the MPSC Communication Channels
    let (inbound_tx, inbound_rx) = mpsc::channel(100);
    let (outbound_tx, outbound_rx) = mpsc::channel(5000);

    // 3. Connect to peers and build the RaftClients map
    let mut peers_map = HashMap::new();
    for (peer_id, peer_address) in peer_configs {
        // we need to manually specify a fairly small timeout for each request (i.e. 100ms) so that the 
        // micro-retries in the network layer don't take too long
        let endpoint = Channel::from_shared(peer_address)?
            .connect_timeout(std::time::Duration::from_millis(50)) // Timeout for the initial TCP handshake
            .timeout(std::time::Duration::from_millis(500));               // Max time allowed for ANY single RPC request
        
        // Connect lazily. Tonic won't actually hit the network until the first RPC is sent.
        // This is CRITICAL because if Node 2 is down on startup, Node 1 won't crash!
        let channel = endpoint.connect_lazy();
        
        // Create the actual gRPC client wrapper generated by Tonic
        let client = RaftClient::new(channel);
        peers_map.insert(peer_id, client);
    }

    let peer_ids = peers_map.keys().copied().collect();

    // 4. Initialize network client worker 
    let client_worker = RaftClientWorker::new(outbound_rx, peers_map);    

    // 5. Initialize network server 
    let network_filter = Arc::new(
        NetworkFilter::new(block_rules)
    );
    let server_impl = RaftServerImpl::new(inbound_tx.clone(), network_filter);


    // 6. Initialize storage
    let storage_path = format!("raft_data_{}", my_id);
    let storage = Box::new(UnixWal::new(&storage_path)
        .expect("Failed to initialize storage"));

    // 7. Initialize Core Logic Loop
    let cloned_tx = inbound_tx.clone();
    let mut core_loop =
        core::RaftCore::new_with_config(inbound_rx, inbound_tx, outbound_tx, my_id, peer_ids, storage);

    // 8. Spawn background execution tasks

    // Simulated Client Injectors (Spawns a task for every paired time and value)
    for (ms, val) in write_ms.into_iter().zip(write_values.into_iter()) {
        let injector_tx = cloned_tx.clone();
        info!("Node {} scheduling a simulated state write to `{}` at virtual ms {}", my_id, val, ms);
        
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(ms as u64)).await;
            info!("Node {} simulated client injector woke up. Injecting TestClientRequest (val: {})...", my_id, val);
            
            if injector_tx.send(RaftEvent::TestClientRequest { new_value: val }).await.is_err() {
                warn!("Node {} failed to inject TestClientRequest; core loop channel closed", my_id);
            }
        });
    }
 
    // Task A: Outbound Client worker
    tokio::spawn(async move {
        client_worker.run().await;
    });

    // Task B: Pure Core Logic State Machine
    tokio::spawn(async move {
        core_loop.run().await;
    });

    // spawn heartbeat tick worker
    let heartbeat_tx = cloned_tx;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            interval.tick().await; // Blocks non-blockingly for 50ms
            if heartbeat_tx.send(RaftEvent::HeartbeatTick).await.is_err() {
                break;
            }
        }
    });

    // Task C: Inbound gRPC Server (Blocks the main thread, keeping the app alive)
    info!("Raft Node {} listening on {}", my_id, my_listen_addr);
    tonic::transport::Server::builder()
        .add_service(RaftServer::new(server_impl))
        .serve(my_listen_addr)
        .await?;

    Ok(())
}
