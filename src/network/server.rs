/**
 * Listen for RPC requests, convert them into RaftEvents and 
 * pass them to the Core and wait for the response 
 */

use tonic::{Request, Response, Status};
use tokio::sync::{mpsc, oneshot};
use super::pb; 

use crate::core::events::{
    RaftEvent, 
    RequestVoteArgs as CoreVoteArgs, 
    AppendEntriesArgs as CoreAppendArgs,
    LogEntry as CoreLogEntry,
};

pub struct RaftServerImpl {
    /// Channel to send incoming events straight to the Core logic loop
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

        // 1. Translate Tonic protobuf struct to Core logic struct
        let core_args = CoreVoteArgs {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        // 2. Setup a oneshot channel to catch the answer from the Core
        let (reply_tx, reply_rx) = oneshot::channel();

        // 3. Dispatch the event to the Core loop
        let event = RaftEvent::RequestVote {
            args: core_args,
            reply_channel: reply_tx,
        };
        
        if self.tx_to_core.send(event).await.is_err() {
            return Err(Status::internal("Raft core loop is down"));
        }

        // 4. Wait for the core to make a decision
        match reply_rx.await {
            Ok(core_reply) => {
                // 5. Translate the Core reply back into a Tonic protobuf message
                let proto_reply = pb::RequestVoteReply {
                    term: core_reply.term,
                    vote_granted: core_reply.vote_granted,
                };
                Ok(Response::new(proto_reply))
            }
            Err(_) => Err(Status::internal("Core dropped the reply channel")),
        }
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesArgs>,
    ) -> Result<Response<pb::AppendEntriesReply>, Status> {
        let req = request.into_inner();

        // 1. Map repeated protobuf entries into a pure Rust Vec
        let core_entries: Vec<CoreLogEntry> = req.entries
            .into_iter()
            .map(|e| CoreLogEntry {
                term: e.term,
                index: e.index,
                command: e.command,
            })
            .collect();

        // 2. Translate to Core logic struct
        let core_args = CoreAppendArgs {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: core_entries,
            leader_commit: req.leader_commit,
        };

        let (reply_tx, reply_rx) = oneshot::channel();

        // 3. Dispatch to Core
        let event = RaftEvent::AppendEntries {
            args: core_args,
            reply_channel: reply_tx,
        };

        if self.tx_to_core.send(event).await.is_err() {
            return Err(Status::internal("Raft core loop is down"));
        }

        // 4. Wait for Core decision and translate back
        match reply_rx.await {
            Ok(core_reply) => {
                let proto_reply = pb::AppendEntriesReply {
                    term: core_reply.term,
                    success: core_reply.success,
                };
                Ok(Response::new(proto_reply))
            }
            Err(_) => Err(Status::internal("Core dropped the reply channel")),
        }
    }
}