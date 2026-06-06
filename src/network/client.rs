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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, oneshot};
    use tonic::{Request, Response, Status};

    use crate::core::events::{
        RequestVoteArgs as CoreVoteArgs, 
        AppendEntriesArgs as CoreAppendArgs,
        LogEntry as CoreLogEntry,
    };

    use super::proto::raft_server::{Raft, RaftServer};
    use super::proto::{RequestVoteReply, AppendEntriesReply};

    // ========================================================================
    // Dummy Server Setup for Isolated Testing
    // ========================================================================
    
    #[derive(Default)]
    struct DummyRaftServer {
        /// Tracks how many times RequestVote was called so we can assert micro-retries
        vote_attempts: Arc<Mutex<u32>>,
    }

    #[tonic::async_trait]
    impl Raft for DummyRaftServer {
        async fn request_vote(
            &self,
            request: Request<proto::RequestVoteArgs>,
        ) -> Result<Response<RequestVoteReply>, Status> {
            let req = request.into_inner();
            
            // Increment the counter every time the client hits this endpoint
            let mut attempts = self.vote_attempts.lock().unwrap();
            *attempts += 1;

            // Trigger a transient error to test micro-retry loop
            if req.term == 999 {
                return Err(Status::unavailable("Server temporarily unavailable"));
            }
            
            // Trigger a non-transient error to test fail-fast logic
            if req.term == 888 {
                return Err(Status::invalid_argument("Malformed payload"));
            }

            Ok(Response::new(RequestVoteReply {
                term: req.term,
                vote_granted: true,
            }))
        }

        async fn append_entries(
            &self,
            request: Request<proto::AppendEntriesArgs>,
        ) -> Result<Response<AppendEntriesReply>, Status> {
            Ok(Response::new(AppendEntriesReply {
                term: request.into_inner().term,
                success: true,
            }))
        }
    }

    /// Helper function to spin up the dummy server and return a configured RaftClient
    async fn spawn_dummy_server() -> (RaftClient<Channel>, Arc<Mutex<u32>>) {
        let dummy_server = DummyRaftServer::default();
        let tracker_clone = Arc::clone(&dummy_server.vote_attempts);

        // Bind to port 0 so the OS assigns a random open port automatically
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn the gRPC server in the background
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(RaftServer::new(dummy_server))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give the background server a millisecond to initialize
        tokio::time::sleep(Duration::from_millis(10)).await;

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect_lazy();

        (RaftClient::new(channel), tracker_clone)
    }

    /// Helper to spin up the client worker task and return the core communication channels
    async fn setup_client_worker() -> (mpsc::Sender<OutboundCommand>, Arc<Mutex<u32>>) {
        let (client, tracker) = spawn_dummy_server().await;
        
        let mut peers = HashMap::new();
        peers.insert(2, client); // We map our dummy server to Node ID 2

        let (tx_outbound, rx_outbound) = mpsc::channel(10);
        let worker = RaftClientWorker::new(rx_outbound, peers);
        
        tokio::spawn(async move {
            worker.run().await;
        });

        (tx_outbound, tracker)
    }

    // ========================================================================
    // Test 1: Send RequestVote
    // ========================================================================
    #[tokio::test]
    async fn test_client_sends_request_vote_success() {
        let (tx_outbound, tracker) = setup_client_worker().await;
        let (reply_tx, reply_rx) = oneshot::channel();

        let args = CoreVoteArgs {
            term: 5,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        // Command the client worker to execute the network call
        tx_outbound.send(OutboundCommand::SendRequestVote {
            target_node_id: 2,
            args,
            result_channel: reply_tx,
        }).await.unwrap();

        // Await the response back to "The Brain"
        let result = reply_rx.await.unwrap();
        assert!(result.is_ok());
        
        let reply = result.unwrap();
        assert_eq!(reply.term, 5);
        assert_eq!(reply.vote_granted, true);

        // Verify the server was hit exactly once
        assert_eq!(*tracker.lock().unwrap(), 1);
    }

    // ========================================================================
    // Test 2: Send AppendEntries
    // ========================================================================
    #[tokio::test]
    async fn test_client_sends_append_entries_success() {
        let (tx_outbound, _) = setup_client_worker().await;
        let (reply_tx, reply_rx) = oneshot::channel();

        let args = CoreAppendArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![CoreLogEntry { term: 2, index: 1, command: true }],
            leader_commit: 0,
        };

        tx_outbound.send(OutboundCommand::SendAppendEntries {
            target_node_id: 2,
            args,
            result_channel: reply_tx,
        }).await.unwrap();

        let result = reply_rx.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().success, true);
    }

    // ========================================================================
    // Test 3: Micro-Retries Exhausted on Transient Error
    // ========================================================================
    #[tokio::test]
    async fn test_client_exhausts_micro_retries() {
        let (tx_outbound, tracker) = setup_client_worker().await;
        let (reply_tx, reply_rx) = oneshot::channel();

        // Term 999 is our magic number to force the DummyServer to return Code::Unavailable
        let args = CoreVoteArgs {
            term: 999, 
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        tx_outbound.send(OutboundCommand::SendRequestVote {
            target_node_id: 2,
            args,
            result_channel: reply_tx,
        }).await.unwrap();

        let result = reply_rx.await.unwrap();
        
        // It should eventually fail and return an Err to the Core
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("gRPC Error after 3 attempts"));

        // Crucial Check: Verify the server actually received exactly 3 requests
        assert_eq!(*tracker.lock().unwrap(), 3);
    }

    // ========================================================================
    // Test 4: Immediate Fail on Non-Transient Error
    // ========================================================================
    #[tokio::test]
    async fn test_client_fails_fast_on_bad_request() {
        let (tx_outbound, tracker) = setup_client_worker().await;
        let (reply_tx, reply_rx) = oneshot::channel();

        // Term 888 is our magic number to force the DummyServer to return Code::InvalidArgument
        let args = CoreVoteArgs {
            term: 888, 
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        tx_outbound.send(OutboundCommand::SendRequestVote {
            target_node_id: 2,
            args,
            result_channel: reply_tx,
        }).await.unwrap();

        let result = reply_rx.await.unwrap();
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("after 1 attempts")); // It should fail on the FIRST try

        // Crucial Check: Verify the client did NOT retry
        assert_eq!(*tracker.lock().unwrap(), 1);
    }
}
