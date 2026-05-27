/**
 * Listen for OutboundCommands from Core and send the RPC calls 
 */

use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::Channel;
use tokio::sync::mpsc;

use crate::core::events::OutboundCommand;
use super::pb as proto;
use proto::raft_client::RaftClient;

// Define micro-retry constants
const MAX_MICRO_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 10;

pub struct RaftClientWorker {
    rx_from_core: mpsc::Receiver<OutboundCommand>,
    peers: HashMap<u64, RaftClient<Channel>>,
}

impl RaftClientWorker {
    pub fn new(
        rx_from_core: mpsc::Receiver<OutboundCommand>, 
        peers: HashMap<u64, RaftClient<Channel>>
    ) -> Self {
        Self { rx_from_core, peers }
    }

    pub async fn run(mut self) {
        while let Some(command) = self.rx_from_core.recv().await {
            match command {
                OutboundCommand::SendRequestVote { target_node_id, args, result_channel } => {
                    let Some(mut client) = self.peers.get(&target_node_id).cloned() else {
                        let _ = result_channel.send(Err("Unknown target node ID".to_string()));
                        continue;
                    };

                    let proto_args = proto::RequestVoteArgs {
                        term: args.term,
                        candidate_id: args.candidate_id,
                        last_log_index: args.last_log_index,
                        last_log_term: args.last_log_term,
                    };

                    // The loop happens inside the spawned task so the worker loop stays totally unblocked
                    tokio::spawn(async move {
                        let mut attempts = 0;
                        loop {
                            // Cloning args because tonic consumes them on each call
                            match client.request_vote(proto_args.clone()).await {
                                Ok(response) => {
                                    let res = response.into_inner();
                                    let core_reply = crate::core::events::RequestVoteReply {
                                        term: res.term,
                                        vote_granted: res.vote_granted,
                                    };
                                    let _ = result_channel.send(Ok(core_reply));
                                    break; // Success! Exit the retry loop
                                }
                                Err(status) => {
                                    attempts += 1;
                                    
                                    // If we hit the cap or it's a non-transient error, give up and report to core
                                    if attempts >= MAX_MICRO_RETRIES || !is_transient_error(&status) {
                                        let _ = result_channel.send(Err(format!(
                                            "gRPC Error after {} attempts: {}", 
                                            attempts, status.message()
                                        )));
                                        break;
                                    }
                                    
                                    // Non-blocking wait before trying again
                                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                                }
                            }
                        }
                    });
                }

                OutboundCommand::SendAppendEntries { target_node_id, args, result_channel } => {
                    let Some(mut client) = self.peers.get(&target_node_id).cloned() else {
                        let _ = result_channel.send(Err("Unknown target node ID".to_string()));
                        continue;
                    };

                    let proto_entries = args.entries
                        .into_iter()
                        .map(|e| proto::LogEntry {
                            term: e.term,
                            index: e.index,
                            command: e.command,
                        })
                        .collect();

                    let proto_args = proto::AppendEntriesArgs {
                        term: args.term,
                        leader_id: args.leader_id,
                        prev_log_index: args.prev_log_index,
                        prev_log_term: args.prev_log_term,
                        entries: proto_entries,
                        leader_commit: args.leader_commit,
                    };

                    tokio::spawn(async move {
                        let mut attempts = 0;
                        loop {
                            match client.append_entries(proto_args.clone()).await {
                                Ok(response) => {
                                    let res = response.into_inner();
                                    let core_reply = crate::core::events::AppendEntriesReply {
                                        term: res.term,
                                        success: res.success,
                                    };
                                    let _ = result_channel.send(Ok(core_reply));
                                    break;
                                }
                                Err(status) => {
                                    attempts += 1;
                                    if attempts >= MAX_MICRO_RETRIES || !is_transient_error(&status) {
                                        let _ = result_channel.send(Err(format!(
                                            "gRPC Error after {} attempts: {}", 
                                            attempts, status.message()
                                        )));
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}

/// Helper function to filter out errors that won't benefit from a fast retry
/// (e.g., Unimplemented code vs. a broken network pipe)
fn is_transient_error(status: &tonic::Status) -> bool {
    match status.code() {
        tonic::Code::Unavailable  => true, // The server is down or unreachable right now
        tonic::Code::Internal     => true, // Can represent broken connection pipes under the hood
        tonic::Code::DeadlineExceeded => true, // Request timed out
        _ => false, // PermissionDenied, Unimplemented, InvalidArgument, etc. should fail immediately
    }
}