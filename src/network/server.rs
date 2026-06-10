/**
 * Listen for RPC requests, convert them into RaftEvents and 
 * pass them to the Core and wait for the response 
 */

use tonic::{Request, Response, Status};
use tokio::sync::{mpsc, oneshot};
use log::{debug, error}; // Added logging macros
use super::pb; 

use crate::core::events::{
    RaftEvent, 
    RequestVoteArgs as CoreVoteArgs, 
    AppendEntriesArgs as CoreAppendArgs,
    InstallSnapshotArgs as CoreSnapshotArgs,
    LogEntry as CoreLogEntry,
};

pub struct RaftServerImpl {
    tx_to_core: mpsc::Sender<RaftEvent>,
}

impl RaftServerImpl {
    pub fn new(tx_to_core: mpsc::Sender<RaftEvent>) -> Self {
        Self { tx_to_core }
    }
}

#[tonic::async_trait]
impl pb::raft_server::Raft for RaftServerImpl {
    
    async fn request_vote(
        &self,
        request: Request<pb::RequestVoteArgs>,
    ) -> Result<Response<pb::RequestVoteReply>, Status> {
        let req = request.into_inner();
        debug!("Received RequestVote from candidate {} for term {}", req.candidate_id, req.term);

        let core_args = CoreVoteArgs {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        let event = RaftEvent::RequestVote {
            args: core_args,
            reply_channel: reply_tx,
        };
        
        if self.tx_to_core.send(event).await.is_err() {
            error!("Failed to route RequestVote to core: core loop is down");
            return Err(Status::internal("Raft core loop is down"));
        }

        match reply_rx.await {
            Ok(core_reply) => {
                debug!("Replying to RequestVote: vote_granted={}", core_reply.vote_granted);
                let proto_reply = pb::RequestVoteReply {
                    term: core_reply.term,
                    vote_granted: core_reply.vote_granted,
                };
                Ok(Response::new(proto_reply))
            }
            Err(_) => {
                error!("Failed to reply to RequestVote: core dropped the channel");
                Err(Status::internal("Core dropped the reply channel"))
            }
        }
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesArgs>,
    ) -> Result<Response<pb::AppendEntriesReply>, Status> {
        let req = request.into_inner();
        debug!("Received AppendEntries from leader {} for term {} (entries: {})", req.leader_id, req.term, req.entries.len());

        let core_entries: Vec<CoreLogEntry> = req.entries
            .into_iter()
            .map(|e| CoreLogEntry {
                term: e.term,
                index: e.index,
                command: e.command,
            })
            .collect();

        let core_args = CoreAppendArgs {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: core_entries,
            leader_commit: req.leader_commit,
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        let event = RaftEvent::AppendEntries {
            args: core_args,
            reply_channel: reply_tx,
        };

        if self.tx_to_core.send(event).await.is_err() {
            error!("Failed to route AppendEntries to core: core loop is down");
            return Err(Status::internal("Raft core loop is down"));
        }

        match reply_rx.await {
            Ok(core_reply) => {
                debug!("Replying to AppendEntries: success={}", core_reply.success);
                let proto_reply = pb::AppendEntriesReply {
                    term: core_reply.term,
                    success: core_reply.success,
                };
                Ok(Response::new(proto_reply))
            }
            Err(_) => {
                error!("Failed to reply to AppendEntries: core dropped the channel");
                Err(Status::internal("Core dropped the reply channel"))
            }
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotArgs>,
    ) -> Result<Response<pb::InstallSnapshotReply>, Status> {
        let req = request.into_inner();
        debug!("Received InstallSnapshot from leader {} for term {}", req.leader_id, req.term);

        let core_args = CoreSnapshotArgs {
            term: req.term,
            leader_id: req.leader_id,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            state_machine_value: req.state_machine_value,
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        let event = RaftEvent::InstallSnapshot {
            args: core_args,
            reply_channel: reply_tx,
        };

        if self.tx_to_core.send(event).await.is_err() {
            error!("Failed to route InstallSnapshot to core: core loop is down");
            return Err(Status::internal("Raft core loop is down"));
        }

        match reply_rx.await {
            Ok(core_reply) => {
                let proto_reply = pb::InstallSnapshotReply { term: core_reply.term };
                Ok(Response::new(proto_reply))
            }
            Err(_) => {
                error!("Failed to reply to InstallSnapshot: core dropped the channel");
                Err(Status::internal("Core dropped the reply channel"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Request, Code};
    use crate::core::events::{RequestVoteReply, AppendEntriesReply};

    // ========================================================================
    // Test 1: RequestVote Translation
    // ========================================================================
    #[tokio::test]
    async fn test_request_vote_success() {
        let (tx_to_core, mut rx_from_server) = mpsc::channel(1);
        let server = RaftServerImpl::new(tx_to_core);

        let proto_req = pb::RequestVoteArgs {
            term: 5,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 4,
        };

        // 1. Fire the request
        let server_handle = tokio::spawn(async move {
            use pb::raft_server::Raft;
            server.request_vote(Request::new(proto_req)).await
        });

        // 2. Intercept the event as "The Brain"
        let event = rx_from_server.recv().await.expect("Server should have emitted an event");
        match event {
            RaftEvent::RequestVote { args, reply_channel } => {
                assert_eq!(args.term, 5);
                assert_eq!(args.candidate_id, 2);
                
                // 3. Send back a successful reply
                let _ = reply_channel.send(RequestVoteReply {
                    term: 5,
                    vote_granted: true,
                });
            }
            _ => panic!("Expected RequestVote event"),
        }

        // 4. Verify the server translated the reply back to Tonic correctly
        let response = server_handle.await.unwrap().unwrap().into_inner();
        assert_eq!(response.term, 5);
        assert_eq!(response.vote_granted, true);
    }

    // ========================================================================
    // Test 2: AppendEntries (Testing the `repeated` entries)
    // ========================================================================
    #[tokio::test]
    async fn test_append_entries_success() {
        let (tx_to_core, mut rx_from_server) = mpsc::channel(1);
        let server = RaftServerImpl::new(tx_to_core);

        // Create a payload with 2 log entries
        let proto_req = pb::AppendEntriesArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![
                pb::LogEntry { term: 2, index: 6, command: true },
                pb::LogEntry { term: 2, index: 7, command: false },
            ],
            leader_commit: 5,
        };

        let server_handle = tokio::spawn(async move {
            use pb::raft_server::Raft;
            server.append_entries(Request::new(proto_req)).await
        });

        let event = rx_from_server.recv().await.unwrap();
        match event {
            RaftEvent::AppendEntries { args, reply_channel } => {
                assert_eq!(args.entries.len(), 2);
                assert_eq!(args.entries[0].index, 6);
                assert_eq!(args.entries[1].command, false);
                
                let _ = reply_channel.send(AppendEntriesReply {
                    term: 2,
                    success: true,
                });
            }
            _ => panic!("Expected AppendEntries event"),
        }

        let response = server_handle.await.unwrap().unwrap().into_inner();
        assert_eq!(response.success, true);
    }

    // ========================================================================
    // Test 3: Core Loop is Dead (mpsc closed)
    // ========================================================================
    #[tokio::test]
    async fn test_server_returns_error_if_core_is_down() {
        let (tx_to_core, rx_from_server) = mpsc::channel(1);
        let server = RaftServerImpl::new(tx_to_core);

        // Simulate "The Brain" crashing by dropping the receiver
        drop(rx_from_server);

        let proto_req = pb::RequestVoteArgs {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        use pb::raft_server::Raft;
        // Call directly (no spawn) because it should fail immediately
        let result = server.request_vote(Request::new(proto_req)).await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), Code::Internal);
        assert!(status.message().contains("down"));
    }

    // ========================================================================
    // Test 4: Core Panics During Processing (oneshot dropped)
    // ========================================================================
    #[tokio::test]
    async fn test_server_returns_error_if_core_drops_reply_channel() {
        let (tx_to_core, mut rx_from_server) = mpsc::channel(1);
        let server = RaftServerImpl::new(tx_to_core);

        let proto_req = pb::AppendEntriesArgs {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let server_handle = tokio::spawn(async move {
            use pb::raft_server::Raft;
            server.append_entries(Request::new(proto_req)).await
        });

        // The Brain receives the event...
        let event = rx_from_server.recv().await.unwrap();
        match event {
            RaftEvent::AppendEntries { reply_channel, .. } => {
                // ...but then a panic or bug causes the Brain to drop the channel 
                // without sending a reply back to the network layer
                drop(reply_channel); 
            }
            _ => panic!("Expected AppendEntries event"),
        }

        // The server should cleanly catch the dropped channel and return a gRPC error
        let result = server_handle.await.unwrap();
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), Code::Internal);
        assert!(status.message().contains("dropped"));
    }
}