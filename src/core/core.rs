use crate::core::events::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    LogEntry, OutboundCommand, RaftEvent, RequestVoteArgs, RequestVoteReply,
};
use crate::storage::WriteAheadLog;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::time::{Duration};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use rand::{RngExt, SeedableRng};
use rand::rngs::StdRng;

const MAX_NODES: usize = 100;
const MIN_ELECTION_TIMEOUT_MS: u64 = 150;
const MAX_ELECTION_TIMEOUT_MS: u64 = 300;
const COMPACTION_THRESHOLD: u64 = 50;

/// The operational states a Raft node can occupy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftCore {
    /// Channel to receive events from the outside world (Network, Timers, Clients).
    inbound_rx: mpsc::Receiver<RaftEvent>,
    /// Optional handle back into the inbound queue for async RPC response events.
    inbound_tx: Option<mpsc::Sender<RaftEvent>>,
    /// Channel to dispatch commands out to the Network layer.
    outbound_tx: mpsc::Sender<OutboundCommand>,

    node_id: u64,
    peer_ids: Vec<u64>,

    // --- Persistent Raft State ---
    /// Latest term server has seen (initialized to 0 on first boot).
    current_term: u64,
    /// Candidate ID that received vote in current term (None if none).
    voted_for: Option<u64>,
    /// Persistent storage for Raft metadata and log entries.
    storage: Box<dyn WriteAheadLog>,

    // --- Volatile Raft State ---
    state: NodeState,
    commit_index: u64,
    last_applied: u64,
    state_machine_value: bool,
    snapshot_index: u64,
    snapshot_term: u64,
    known_leader_id: Option<u64>,
    election_deadline: Instant,

    // --- Volatile Leader/Candidate State ---
    votes_received: HashSet<u64>,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
    pending_client_replies: HashMap<u64, oneshot::Sender<Result<(), String>>>,
    /// Peers for which an InstallSnapshot RPC is currently in-flight.
    /// Prevents redundant snapshots from being sent on every heartbeat tick
    /// while the first one is still awaiting a response.
    snapshot_in_flight: HashSet<u64>,
}

impl RaftCore {
    /// Use `new_with_config` when node identity and peers are available.
    pub fn new(
        inbound_rx: mpsc::Receiver<RaftEvent>,
        outbound_tx: mpsc::Sender<OutboundCommand>,
        storage: Box<dyn WriteAheadLog>,
    ) -> Self {
        Self::build(inbound_rx, None, outbound_tx, 1, Vec::new(), storage)
    }

    /// Creates a configured Raft core that can send RPC response events back
    /// into its own queue after network layer's outbound worker replies.
    pub fn new_with_config(
        inbound_rx: mpsc::Receiver<RaftEvent>,
        inbound_tx: mpsc::Sender<RaftEvent>,
        outbound_tx: mpsc::Sender<OutboundCommand>,
        node_id: u64,
        peer_ids: Vec<u64>,
        storage: Box<dyn WriteAheadLog>,
    ) -> Self {
        Self::build(inbound_rx, Some(inbound_tx), outbound_tx, node_id, peer_ids, storage)
    }

    fn build(
        inbound_rx: mpsc::Receiver<RaftEvent>,
        inbound_tx: Option<mpsc::Sender<RaftEvent>>,
        outbound_tx: mpsc::Sender<OutboundCommand>,
        node_id: u64,
        peer_ids: Vec<u64>,
        storage: Box<dyn WriteAheadLog>,
    ) -> Self {
        let mut deduped_peers: Vec<u64> = peer_ids
            .into_iter()
            .filter(|peer_id| *peer_id != node_id)
            .collect();
        deduped_peers.sort_unstable();
        deduped_peers.dedup();

        assert!(
            deduped_peers.len() + 1 <= MAX_NODES,
            "RaftCore supports at most {MAX_NODES} voting nodes"
        );

        let (current_term, voted_for) = storage
            .load_metadata()
            .unwrap_or_else(|e| {
                error!("Failed to load persistent Raft metadata: {}", e);
                panic!("failed to load persistent Raft metadata: {e}");
            });
        info!(
            "Loaded persistent Raft state on Node {}: current_term={}, voted_for={:?}",
            node_id, current_term, voted_for
        );

        let (snapshot_index, snapshot_term, state_machine_value) = storage
            .load_snapshot()
            .unwrap_or_else(|e| {
                error!("Failed to load snapshot: {}", e);
                panic!("failed to load snapshot: {e}");
            })
            .map(|s| (s.last_included_index, s.last_included_term, s.state_machine_value))
            .unwrap_or((0, 0, false));

        if snapshot_index > 0 {
            info!("Restored snapshot on Node {}: last_included_index={} last_included_term={} value={}", node_id, snapshot_index, snapshot_term, state_machine_value);
        }

        Self {
            inbound_rx,
            inbound_tx,
            outbound_tx,
            node_id,
            peer_ids: deduped_peers,
            current_term,
            voted_for: voted_for.map(u64::from),
            storage,
            state: NodeState::Follower,
            commit_index: snapshot_index,
            last_applied: snapshot_index,
            state_machine_value,
            snapshot_index,
            snapshot_term,
            known_leader_id: None,
            election_deadline: Self::new_election_deadline(node_id, current_term),
            votes_received: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            pending_client_replies: HashMap::new(),
            snapshot_in_flight: HashSet::new(),
        }
    }

    /// The main sequential execution loop for the Raft state machine.
    /// It processes inbound events and owns the randomized election timer.
    pub async fn run(&mut self) {
        info!("Raft Core Loop initialized on Node {}. Starting event processing...", self.node_id);

        loop {
            let election_timer_active = self.state != NodeState::Leader;
            let election_deadline = self.election_deadline;

            tokio::select! {
                event = self.inbound_rx.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    self.handle_event(event).await;
                }
                _ = tokio::time::sleep_until(election_deadline), if election_timer_active => {
                    self.handle_election_timeout().await;
                }
            }
        }

        info!("Raft Core Loop has shut down because the inbound channel closed.");
    }

    async fn handle_event(&mut self, event: RaftEvent) {
        match event {
            RaftEvent::ElectionTimeout => self.handle_election_timeout().await,
            RaftEvent::HeartbeatTick => self.handle_heartbeat_tick().await,
            RaftEvent::RequestVote {
                args,
                reply_channel,
            } => {
                let reply = self.handle_request_vote(args);
                let _ = reply_channel.send(reply);
            }
            RaftEvent::RequestVoteResponse {
                from_node_id,
                result,
            } => self.handle_request_vote_response(from_node_id, result).await,
            RaftEvent::AppendEntries {
                args,
                reply_channel,
            } => {
                let reply = self.handle_append_entries(args);
                let _ = reply_channel.send(reply);
            }
            RaftEvent::AppendEntriesResponse {
                from_node_id,
                sent_prev_log_index,
                sent_last_log_index,
                result,
            } => {
                self.handle_append_entries_response(
                    from_node_id,
                    sent_prev_log_index,
                    sent_last_log_index,
                    result,
                )
                .await
            }
            RaftEvent::InstallSnapshot {
                args,
                reply_channel,
            } => {
                let reply = self.handle_install_snapshot(args);
                let _ = reply_channel.send(reply);
            }
            RaftEvent::InstallSnapshotResponse {
                from_node_id,
                result,
            } => self.handle_install_snapshot_response(from_node_id, result).await,
            RaftEvent::ClientCommand {
                new_value,
                reply_channel,
            } => self.handle_client_command(new_value, reply_channel).await,
            RaftEvent::TestClientRequest {
                new_value,
            } => self.handle_test_client_command(new_value).await,
        }
    }

    async fn handle_election_timeout(&mut self) {
        if self.state == NodeState::Leader {
            return;
        }

        info!("Election timeout expired! Initiating election for term {}", self.current_term + 1);
        self.start_election().await;
    }

    async fn handle_heartbeat_tick(&mut self) {
        if self.state == NodeState::Leader {
            self.send_heartbeats().await;
        }
    }

    async fn start_election(&mut self) {
        self.become_candidate();
        if self.has_majority(self.votes_received.len()) {
            self.become_leader().await;
            return;
        }

        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        for peer_id in self.peer_ids.clone() {
            self.send_request_vote(peer_id, args.clone()).await;
        }
    }

    fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.node_id);
        self.known_leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.node_id);
        self.reset_election_timer();
        self.persist_term_and_vote();
        
        info!("Transitioned to Candidate for term {}", self.current_term);
    }

    fn become_follower(&mut self, term: u64) {
        if term > self.current_term {
            info!("Saw higher term ({} > {}). Updating term.", term, self.current_term);
            self.current_term = term;
            self.voted_for = None;
            self.persist_term_and_vote();
        }

        if self.state != NodeState::Follower {
            info!("Transitioned to Follower for term {}", self.current_term);
        }

        self.state = NodeState::Follower;
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.snapshot_in_flight.clear();
    }

    async fn become_leader(&mut self) {
        info!("Received majority votes. Transitioning to Leader for term {}!", self.current_term);
        self.state = NodeState::Leader;
        self.known_leader_id = Some(self.node_id);
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();

        let next_index = self.last_log_index() + 1;
        for peer_id in &self.peer_ids {
            self.next_index.insert(*peer_id, next_index);
            self.match_index.insert(*peer_id, 0);
        }
        self.match_index.insert(self.node_id, self.last_log_index());

        self.send_heartbeats().await;
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        debug!("Received RequestVote from Node {} for term {}", args.candidate_id, args.term);

        if args.term < self.current_term {
            warn!("Rejecting RequestVote from Node {} (stale term {})", args.candidate_id, args.term);
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if args.term > self.current_term {
            // Raft safety rule: any higher term means this node must step down.
            self.become_follower(args.term);
        }

        if !self.is_voting_node(args.candidate_id) {
            warn!("Rejecting RequestVote from unknown Node {}", args.candidate_id);
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        let vote_available = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);
        let log_is_current =
            self.is_log_up_to_date(args.last_log_term, args.last_log_index);
        let vote_granted = vote_available && log_is_current;

        if vote_granted {
            info!("Granting vote to Node {} for term {}", args.candidate_id, args.term);
            self.voted_for = Some(args.candidate_id);
            self.reset_election_timer();
            self.persist_term_and_vote();
        } else {
            debug!("Rejecting RequestVote from Node {} (already voted or log stale)", args.candidate_id);
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    async fn handle_request_vote_response(
        &mut self,
        from_node_id: u64,
        result: Result<RequestVoteReply, String>,
    ) {
        let Ok(reply) = result else {
            warn!("Failed to receive vote response from Node {}", from_node_id);
            return;
        };

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            return;
        }

        if !self.is_voting_node(from_node_id) {
            return;
        }

        if self.state != NodeState::Candidate || reply.term != self.current_term {
            return;
        }

        if reply.vote_granted {
            debug!("Received vote from Node {}", from_node_id);
            self.votes_received.insert(from_node_id);
        }

        if self.has_majority(self.votes_received.len()) {
            self.become_leader().await;
        }
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        if args.term > self.current_term || self.state != NodeState::Follower {
            // Valid leader contact with this term supersedes candidacy/leadership.
            self.become_follower(args.term);
        }

        self.known_leader_id = Some(args.leader_id);
        self.reset_election_timer();

        // Reject if prev_log_index is behind our snapshot: we've already compacted
        // those entries and cannot safely accept a batch that starts before them.
        // The leader will see the failure, find next_index[us] <= snapshot_index,
        // and re-send via InstallSnapshot instead.
        if args.prev_log_index < self.snapshot_index {
            warn!(
                "Rejecting AppendEntries from Node {}: prev_log_index={} is behind snapshot_index={}",
                args.leader_id, args.prev_log_index, self.snapshot_index
            );
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        if !self.log_contains(args.prev_log_index, args.prev_log_term) {
            warn!("Rejecting AppendEntries from Node {} (log mismatch at index {})", args.leader_id, args.prev_log_index);
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        self.append_entries_from_leader(args.prev_log_index, args.entries);

        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit.min(self.last_log_index());
            self.apply_committed_entries();
            self.maybe_compact();
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
        }
    }

    async fn handle_append_entries_response(
        &mut self,
        from_node_id: u64,
        sent_prev_log_index: u64,
        sent_last_log_index: u64,
        result: Result<AppendEntriesReply, String>,
    ) {
        let Ok(reply) = result else {
            debug!("AppendEntries to Node {} failed or timed out", from_node_id);
            return;
        };

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            return;
        }

        if !self.peer_ids.contains(&from_node_id) {
            return;
        }

        if self.state != NodeState::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            let current_match = self.match_index.get(&from_node_id).copied().unwrap_or(0);
            if sent_last_log_index > current_match {
                self.match_index.insert(from_node_id, sent_last_log_index);
                let current_next = self.next_index.get(&from_node_id).copied().unwrap_or(1);
                self.next_index
                    .insert(from_node_id, current_next.max(sent_last_log_index + 1));
                self.update_commit_index();
            }
        } else {
            let request_next_index = sent_prev_log_index + 1;
            let next_index = self.next_index.get(&from_node_id).copied().unwrap_or(1);
            if next_index == request_next_index {
                self.next_index.insert(from_node_id, next_index.saturating_sub(1).max(1));
                debug!("AppendEntries rejected by Node {}. Decrementing next_index to {}", from_node_id, self.next_index[&from_node_id]);
                self.send_append_entries_to_peer(from_node_id).await;
            } else {
                debug!(
                    "Ignoring stale AppendEntries rejection from Node {} (request next_index={}, current next_index={})",
                    from_node_id, request_next_index, next_index
                );
            }
        }
    }

    async fn handle_test_client_command(
        &mut self, 
        new_value: bool, 
    ) { 
        if self.state != NodeState::Leader {
            warn!("Rejected client command: Not the leader");
            return;
        }

        let index = self.last_log_index() + 1;
        info!("Received test client command ({}). Appending to log at index {}", new_value, index);
        
        let entry = LogEntry {
            term: self.current_term,
            index,
            command: new_value,
        };
        self.push_log_entry(entry);
        self.match_index.insert(self.node_id, index);

        self.update_commit_index();
        self.send_heartbeats().await;

        // Heartbeat batching alternative:
        // Comment out the send_heartbeats() call above to defer replication
        // until the next periodic heartbeat and batch commands from the interval.
    }

    async fn handle_client_command(
        &mut self,
        new_value: bool,
        reply_channel: oneshot::Sender<Result<(), String>>,
    ) {
        if self.state != NodeState::Leader {
            warn!("Rejected client command: Not the leader");
            let leader_hint = self
                .known_leader_id
                .map(|id| format!("; known leader is node {id}"))
                .unwrap_or_default();
            let _ = reply_channel.send(Err(format!("not leader{leader_hint}")));
            return;
        }

        let index = self.last_log_index() + 1;
        info!("Received client command ({}). Appending to log at index {}", new_value, index);
        
        let entry = LogEntry {
            term: self.current_term,
            index,
            command: new_value,
        };
        self.push_log_entry(entry);
        self.match_index.insert(self.node_id, index);
        self.pending_client_replies.insert(index, reply_channel);

        self.update_commit_index();
        self.send_heartbeats().await;

        // Heartbeat batching alternative:
        // Comment out the send_heartbeats() call above to defer replication
        // until the next periodic heartbeat and batch commands from the interval.
    }

    async fn send_request_vote(&self, target_node_id: u64, args: RequestVoteArgs) {
        let (result_tx, result_rx) = oneshot::channel();
        let command = OutboundCommand::SendRequestVote {
            target_node_id,
            args,
            result_channel: result_tx,
        };

        if self.outbound_tx.send(command).await.is_err() {
            error!("Failed to send RequestVote to outbound channel");
            return;
        }

        self.forward_request_vote_response(target_node_id, result_rx);
    }

    async fn send_heartbeats(&mut self) {
        for peer_id in self.peer_ids.clone() {
            self.send_append_entries_to_peer(peer_id).await;
        }
    }

    async fn send_append_entries_to_peer(&mut self, target_node_id: u64) {
        let next_index = self.next_index.get(&target_node_id).copied().unwrap_or(1);

        // If the follower needs entries we've already compacted, send snapshot instead
        if next_index <= self.snapshot_index {
            self.send_snapshot_to_peer(target_node_id).await;
            return;
        }
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = self.term_at(prev_log_index).unwrap_or(0);
        let entries = self.entries_from(next_index);
        let last_sent_index = entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(prev_log_index);

        let args = AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };

        let (result_tx, result_rx) = oneshot::channel();
        let command = OutboundCommand::SendAppendEntries {
            target_node_id,
            args,
            result_channel: result_tx,
        };

        if self.outbound_tx.send(command).await.is_err() {
            error!("Failed to send AppendEntries to outbound channel");
            return;
        }

        self.forward_append_entries_response(
            target_node_id,
            prev_log_index,
            last_sent_index,
            result_rx,
        );
    }

    fn forward_request_vote_response(
        &self,
        from_node_id: u64,
        result_rx: oneshot::Receiver<Result<RequestVoteReply, String>>,
    ) {
        let Some(inbound_tx) = self.inbound_tx.clone() else {
            return;
        };

        tokio::spawn(async move {
            let result = result_rx
                .await
                .unwrap_or_else(|_| Err("RequestVote response channel closed".to_string()));
            let _ = inbound_tx
                .send(RaftEvent::RequestVoteResponse {
                    from_node_id,
                    result,
                })
                .await;
        });
    }

    fn forward_append_entries_response(
        &self,
        from_node_id: u64,
        sent_prev_log_index: u64,
        sent_last_log_index: u64,
        result_rx: oneshot::Receiver<Result<AppendEntriesReply, String>>,
    ) {
        let Some(inbound_tx) = self.inbound_tx.clone() else {
            return;
        };

        tokio::spawn(async move {
            let result = result_rx
                .await
                .unwrap_or_else(|_| Err("AppendEntries response channel closed".to_string()));
            let _ = inbound_tx
                .send(RaftEvent::AppendEntriesResponse {
                    from_node_id,
                    sent_prev_log_index,
                    sent_last_log_index,
                    result,
                })
                .await;
        });
    }

    fn update_commit_index(&mut self) {
        if self.state != NodeState::Leader {
            return;
        }

        let old_commit_index = self.commit_index;
        let last_log_index = self.last_log_index();

        for candidate_index in (old_commit_index + 1)..=last_log_index {
            // Raft's current-term-only rule prevents old entries from being
            // considered committed solely because a later leader replicated them.
            if self.term_at(candidate_index) != Some(self.current_term) {
                continue;
            }

            let replicated_count = self
                .voting_node_ids()
                .into_iter()
                .filter(|node_id| {
                    self.match_index
                        .get(node_id)
                        .copied()
                        .unwrap_or_default()
                        >= candidate_index
                })
                .count();

            if self.has_majority(replicated_count) {
                self.commit_index = candidate_index;
            }
        }

        if self.commit_index != old_commit_index {
            info!("Leader updated commit_index to {}", self.commit_index);
            self.apply_committed_entries();
            self.maybe_compact();
        }
    }

    fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.entry_at(self.last_applied) {
                self.state_machine_value = entry.command;
                debug!("Applied log entry {} to state machine. Value is now: {}", self.last_applied, self.state_machine_value);
            }

            if let Some(reply_channel) = self.pending_client_replies.remove(&self.last_applied) {
                let _ = reply_channel.send(Ok(()));
            }
        }
    }

    fn append_entries_from_leader(&mut self, prev_log_index: u64, entries: Vec<LogEntry>) {
        let mut expected_index = prev_log_index + 1;

        for entry in entries {
            if entry.index != expected_index {
                expected_index += 1;
                continue;
            }

            match self.term_at(entry.index) {
                Some(existing_term) if existing_term == entry.term => {}
                Some(_) => {
                    info!("Log conflict at index {}. Truncating suffix.", entry.index);
                    self.truncate_suffix_from(entry.index);
                    self.push_log_entry(entry);
                }
                None => self.push_log_entry(entry),
            }

            expected_index += 1;
        }
    }

    fn truncate_suffix_from(&mut self, first_removed_index: u64) {
        // Truncate all entries at or after first_removed_index
        if let Err(e) = self.storage.truncate_log_suffix(first_removed_index) {
            error!(
                "Failed to persist log truncation from index {}: {}",
                first_removed_index, e
            );
            panic!("failed to persist log truncation from index {first_removed_index}: {e}");
        }
        self.pending_client_replies
            .retain(|index, _| *index < first_removed_index);
        self.commit_index = self.commit_index.min(first_removed_index.saturating_sub(1));
        self.last_applied = self.last_applied.min(self.commit_index);
    }

    fn log_contains(&self, index: u64, term: u64) -> bool {
        if index == 0 {
            return term == 0;
        }

        self.term_at(index) == Some(term)
    }

    fn is_log_up_to_date(&self, candidate_last_term: u64, candidate_last_index: u64) -> bool {
        let my_last_term = self.last_log_term();
        let my_last_index = self.last_log_index();

        // Leader completeness election restriction: compare last term first,
        // then last index only when terms tie.
        candidate_last_term > my_last_term
            || (candidate_last_term == my_last_term && candidate_last_index >= my_last_index)
    }

    fn has_majority(&self, votes_or_replicas: usize) -> bool {
        // TODO (configuration changes): replace this helper with joint-consensus
        // checks when configuration log-entry types exist.
        votes_or_replicas >= self.majority_threshold()
    }

    fn majority_threshold(&self) -> usize {
        (self.voting_node_count() / 2) + 1
    }

    fn voting_node_count(&self) -> usize {
        self.peer_ids.len() + 1
    }

    fn voting_node_ids(&self) -> Vec<u64> {
        let mut node_ids = self.peer_ids.clone();
        node_ids.push(self.node_id);
        node_ids
    }

    fn is_voting_node(&self, node_id: u64) -> bool {
        node_id == self.node_id || self.peer_ids.contains(&node_id)
    }

    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        match self.storage.get_entry(index) {
            Ok(Some((term, command_bytes))) => {
                if command_bytes.len() == 1 {
                    let command = command_bytes[0] != 0;
                    Some(LogEntry { term, index, command })
                } else {
                    error!(
                        "Corrupted persistent log entry {}: invalid command bytes",
                        index
                    );
                    panic!("corrupted persistent log entry {index}: invalid command bytes");
                }
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to read persistent log entry {}: {}", index, e);
                panic!("failed to read persistent log entry {index}: {e}");
            }
        }
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.entry_at(index).map(|entry| entry.term)
    }

    fn push_log_entry(&mut self, entry: LogEntry) {
        let command_byte = if entry.command { 1u8 } else { 0u8 };
        if let Err(e) = self.storage.append_entry(entry.index, entry.term, &[command_byte]) {
            error!("Failed to persist log entry {}: {}", entry.index, e);
            panic!("failed to persist log entry {}: {e}", entry.index);
        }
    }

    fn entries_from(&self, first_index: u64) -> Vec<LogEntry> {
        let mut entries = Vec::new();
        // We need to iterate through storage from first_index upward
        // Since storage doesn't provide range iteration, we'll iterate until we hit an error
        let mut index = first_index;
        loop {
            match self.storage.get_entry(index) {
                Ok(Some((term, command_bytes))) => {
                    if command_bytes.len() == 1 {
                        let command = command_bytes[0] != 0;
                        entries.push(LogEntry { term, index, command });
                        index += 1;
                    } else {
                        error!(
                            "Corrupted persistent log entry {}: invalid command bytes",
                            index
                        );
                        panic!("corrupted persistent log entry {index}: invalid command bytes");
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Failed to read persistent log entry {}: {}", index, e);
                    panic!("failed to read persistent log entry {index}: {e}");
                }
            }
        }
        entries
    }

    fn last_log_index(&self) -> u64 {
        let stored_last_index = self.storage
            .log_length()
            .unwrap_or_else(|e| {
                error!("Failed to read persistent log length: {}", e);
                panic!("failed to read persistent log length: {e}");
            });
        stored_last_index.max(self.snapshot_index)
    }

    fn last_log_term(&self) -> u64 {
        self.term_at(self.last_log_index()).unwrap_or(0)
    }

    fn reset_election_timer(&mut self) {
        self.election_deadline = Self::new_election_deadline(self.node_id, self.current_term);
    }

    fn new_election_deadline(node_id: u64, current_term: u64) -> Instant {
        // seed the rng with node id (as opposed to system time) so that each node will get a different timeout
        // in the shadow simulator
        let mut rng = StdRng::seed_from_u64(node_id + current_term);
        let final_timeout_ms = rng.random_range(MIN_ELECTION_TIMEOUT_MS..MAX_ELECTION_TIMEOUT_MS);
        Instant::now() + Duration::from_millis(final_timeout_ms)
    }

    fn maybe_compact(&mut self) {
        if self.last_applied < self.snapshot_index + COMPACTION_THRESHOLD {
            return;
        }
        let compact_up_to = self.last_applied;
        let compact_term = self.term_at(compact_up_to).unwrap_or(self.snapshot_term);
        info!("Compacting log up to index {} on Node {}", compact_up_to, self.node_id);
        if let Err(e) = self.storage.save_snapshot(
            compact_up_to,
            compact_term,
            self.state_machine_value,
        ) {
            error!("Failed to save snapshot at index {}: {}", compact_up_to, e);
            panic!("failed to save snapshot at index {compact_up_to}: {e}");
        }
        if let Err(e) = self.storage.truncate_log(compact_up_to) {
            error!("Failed to truncate log to index {}: {}", compact_up_to, e);
            panic!("failed to truncate log to index {compact_up_to}: {e}");
        }
        self.snapshot_index = compact_up_to;
        self.snapshot_term = compact_term;
        info!("Compaction complete. snapshot_index={} on Node {}", self.snapshot_index, self.node_id);
    }

    fn handle_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if args.term < self.current_term {
            return InstallSnapshotReply { term: self.current_term };
        }

        if args.term > self.current_term || self.state != NodeState::Follower {
            self.become_follower(args.term);
        }

        self.known_leader_id = Some(args.leader_id);
        self.reset_election_timer();

        // Only install if it's newer than our current snapshot
        if args.last_included_index <= self.snapshot_index {
            return InstallSnapshotReply { term: self.current_term };
        }

        info!(
            "Installing snapshot from leader {}: last_included_index={} value={}",
            args.leader_id, args.last_included_index, args.state_machine_value
        );

        if let Err(e) = self.storage.save_snapshot(
            args.last_included_index,
            args.last_included_term,
            args.state_machine_value,
        ) {
            error!("Failed to persist snapshot: {}", e);
            panic!("failed to persist snapshot: {e}");
        }

        if let Err(e) = self.storage.truncate_log(args.last_included_index) {
            error!("Failed to truncate log after snapshot install: {}", e);
            panic!("failed to truncate log after snapshot install: {e}");
        }

        self.snapshot_index = args.last_included_index;
        self.snapshot_term = args.last_included_term;
        self.state_machine_value = args.state_machine_value;
        self.commit_index = self.commit_index.max(args.last_included_index);
        self.last_applied = self.last_applied.max(args.last_included_index);

        InstallSnapshotReply { term: self.current_term }
    }

    async fn handle_install_snapshot_response(
        &mut self,
        from_node_id: u64,
        result: Result<InstallSnapshotReply, String>,
    ) {
        self.snapshot_in_flight.remove(&from_node_id);

        let Ok(reply) = result else {
            debug!("InstallSnapshot to Node {} failed or timed out", from_node_id);
            return;
        };

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            return;
        }

        if self.state != NodeState::Leader || reply.term != self.current_term {
            return;
        }

        // After a successful snapshot install the follower is caught up to snapshot_index
        self.match_index.insert(from_node_id, self.snapshot_index);
        self.next_index.insert(from_node_id, self.snapshot_index + 1);
        self.update_commit_index();
    }

    async fn send_snapshot_to_peer(&mut self, target_node_id: u64) {
        // Don't send a second snapshot while one is already in-flight for this peer.
        if self.snapshot_in_flight.contains(&target_node_id) {
            return;
        }

        let args = InstallSnapshotArgs {
            term: self.current_term,
            leader_id: self.node_id,
            last_included_index: self.snapshot_index,
            last_included_term: self.snapshot_term,
            state_machine_value: self.state_machine_value,
        };

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let command = OutboundCommand::SendInstallSnapshot {
            target_node_id,
            args,
            result_channel: result_tx,
        };

        if self.outbound_tx.send(command).await.is_err() {
            error!("Failed to send InstallSnapshot to outbound channel");
            return;
        }

        // Hold next_index at snapshot_index so subsequent heartbeat ticks don't
        // race ahead with AppendEntries while the snapshot RPC is in-flight.
        // handle_install_snapshot_response will advance it to snapshot_index + 1.
        self.next_index.insert(target_node_id, self.snapshot_index);
        self.snapshot_in_flight.insert(target_node_id);

        self.forward_install_snapshot_response(target_node_id, result_rx);
    }

    fn forward_install_snapshot_response(
        &self,
        from_node_id: u64,
        result_rx: tokio::sync::oneshot::Receiver<Result<InstallSnapshotReply, String>>,
    ) {
        let Some(inbound_tx) = self.inbound_tx.clone() else { return; };
        tokio::spawn(async move {
            let result = result_rx
                .await
                .unwrap_or_else(|_| Err("InstallSnapshot response channel closed".to_string()));
            let _ = inbound_tx
                .send(RaftEvent::InstallSnapshotResponse { from_node_id, result })
                .await;
        });
    }

    fn persist_term_and_vote(&mut self) {
        // Convert Option<u64> to Option<u32> for storage API
        let voted_for_u32 = self.voted_for.map(|id| id as u32);
        if let Err(e) = self.storage.save_metadata(self.current_term, voted_for_u32) {
            error!(
                "Failed to persist Raft term {} and vote {:?}: {}",
                self.current_term, self.voted_for, e
            );
            panic!(
                "failed to persist Raft term {} and vote {:?}: {e}",
                self.current_term, self.voted_for
            );
        }
    }

    pub fn state(&self) -> NodeState {
        self.state
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<u64> {
        self.voted_for
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_core(
        peer_ids: Vec<u64>,
    ) -> (
        RaftCore,
        mpsc::Sender<RaftEvent>,
        mpsc::Receiver<OutboundCommand>,
    ) {
        let (inbound_tx, inbound_rx) = mpsc::channel(100);
        let (outbound_tx, outbound_rx) = mpsc::channel(100);
        let storage = Box::new(crate::storage::tests::MockWal::new());
        (
            RaftCore::new_with_config(inbound_rx, inbound_tx.clone(), outbound_tx, 1, peer_ids, storage),
            inbound_tx,
            outbound_rx,
        )
    }

    fn entry(index: u64, term: u64, command: bool) -> LogEntry {
        LogEntry {
            index,
            term,
            command,
        }
    }

    #[test]
    fn snapshot_boundary_is_the_last_log_entry_when_suffix_is_empty() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.snapshot_index = 5;
        core.snapshot_term = 3;

        assert_eq!(core.last_log_index(), 5);
        assert_eq!(core.last_log_term(), 3);
    }

    #[test]
    fn stored_suffix_remains_the_last_log_entry_after_snapshot() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.snapshot_index = 5;
        core.snapshot_term = 3;
        core.push_log_entry(entry(6, 4, true));

        assert_eq!(core.last_log_index(), 6);
        assert_eq!(core.last_log_term(), 4);
    }

    async fn drain_request_votes(outbound_rx: &mut mpsc::Receiver<OutboundCommand>, count: usize) {
        for _ in 0..count {
            match outbound_rx.recv().await.unwrap() {
                OutboundCommand::SendRequestVote { .. } => {}
                OutboundCommand::SendAppendEntries { .. } => {
                    panic!("expected RequestVote command");
                }
                OutboundCommand::SendInstallSnapshot { .. } => {
                    panic!("expected RequestVote command");
                }
            }
        }
    }

    async fn drain_append_entries(outbound_rx: &mut mpsc::Receiver<OutboundCommand>, count: usize) {
        for _ in 0..count {
            match outbound_rx.recv().await.unwrap() {
                OutboundCommand::SendAppendEntries { .. } => {}
                OutboundCommand::SendRequestVote { .. } => {
                    panic!("expected AppendEntries command");
                }
                OutboundCommand::SendInstallSnapshot { .. } => {}
            }
        }
    }

    // Verifies that a new core starts in the initial Raft follower state.
    #[tokio::test]
    async fn server_starts_as_follower() {
        let (core, _, _) = test_core(vec![2, 3]);

        assert_eq!(core.state(), NodeState::Follower);
        assert_eq!(core.current_term(), 0);
        assert_eq!(core.voted_for(), None);
    }

    #[tokio::test]
    async fn server_restores_persisted_term_and_vote() {
        let (inbound_tx, inbound_rx) = mpsc::channel(100);
        let (outbound_tx, _) = mpsc::channel(100);
        let mut storage = crate::storage::tests::MockWal::new();
        storage.save_metadata(7, Some(2)).unwrap();

        let core = RaftCore::new_with_config(
            inbound_rx,
            inbound_tx,
            outbound_tx,
            1,
            vec![2, 3],
            Box::new(storage),
        );

        assert_eq!(core.current_term(), 7);
        assert_eq!(core.voted_for(), Some(2));
    }

    // Verifies that an election timeout starts a new term, self-votes, and sends RequestVote RPCs.
    #[tokio::test]
    async fn follower_becomes_candidate_votes_self_and_requests_votes() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);

        core.handle_election_timeout().await;

        assert_eq!(core.state(), NodeState::Candidate);
        assert_eq!(core.current_term(), 1);
        assert_eq!(core.voted_for(), Some(1));
        assert!(core.votes_received.contains(&1));
        drain_request_votes(&mut outbound_rx, 2).await;
    }

    // Verifies that the production core loop starts an election when its own deadline expires.
    #[tokio::test]
    async fn core_loop_starts_election_when_internal_deadline_expires() {
        let (mut core, inbound_tx, mut outbound_rx) = test_core(vec![2, 3]);
        core.election_deadline = Instant::now() + Duration::from_millis(10);

        let core_task = tokio::spawn(async move {
            core.run().await;
        });

        let command = tokio::time::timeout(Duration::from_millis(100), outbound_rx.recv())
            .await
            .expect("core did not start an election before the test timeout")
            .expect("outbound channel closed before RequestVote was sent");

        match command {
            OutboundCommand::SendRequestVote { args, .. } => {
                assert_eq!(args.term, 1);
                assert_eq!(args.candidate_id, 1);
            }
            OutboundCommand::SendAppendEntries { .. } => {
                panic!("expected RequestVote command");
            }
            OutboundCommand::SendInstallSnapshot { .. } => {
                panic!("expected RequestVote command");
            }
        }

        drop(inbound_tx);
        core_task.abort();
    }

    // Verifies that a candidate becomes leader after receiving a majority of votes.
    #[tokio::test]
    async fn candidate_becomes_leader_after_majority_votes() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;

        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;

        assert_eq!(core.state(), NodeState::Leader);
        assert_eq!(core.next_index.get(&2), Some(&1));
        assert_eq!(core.match_index.get(&2), Some(&0));
        drain_append_entries(&mut outbound_rx, 2).await;
    }

    // Verifies that a candidate accepts a valid leader heartbeat and steps down to follower.
    #[tokio::test]
    async fn candidate_steps_down_on_valid_append_entries() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.handle_election_timeout().await;

        let reply = core.handle_append_entries(AppendEntriesArgs {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        });

        assert!(reply.success);
        assert_eq!(core.state(), NodeState::Follower);
        assert_eq!(core.known_leader_id, Some(2));
    }

    // Verifies that a leader steps down immediately when it observes a higher term.
    #[tokio::test]
    async fn leader_steps_down_when_observing_higher_term() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;
        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;
        drain_append_entries(&mut outbound_rx, 2).await;

        core.handle_append_entries_response(
            2,
            0,
            0,
            Ok(AppendEntriesReply {
                term: 2,
                success: false,
            }),
        )
        .await;

        assert_eq!(core.state(), NodeState::Follower);
        assert_eq!(core.current_term(), 2);
        assert_eq!(core.voted_for(), None);
    }

    // Verifies that RequestVote requests from older terms are rejected.
    #[tokio::test]
    async fn request_vote_rejects_stale_term() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.current_term = 3;

        let reply = core.handle_request_vote(RequestVoteArgs {
            term: 2,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });

        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 3);
    }

    // Verifies the leader-completeness vote restriction rejects less up-to-date logs.
    #[tokio::test]
    async fn request_vote_rejects_stale_log() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.push_log_entry(entry(1, 2, true));
        core.current_term = 3;

        let reply = core.handle_request_vote(RequestVoteArgs {
            term: 3,
            candidate_id: 2,
            last_log_index: 2,
            last_log_term: 1,
        });

        assert!(!reply.vote_granted);
    }

    // Verifies that an available voter grants a vote to an up-to-date candidate.
    #[tokio::test]
    async fn request_vote_grants_up_to_date_candidate_when_vote_available() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.push_log_entry(entry(1, 2, true));
        core.current_term = 3;

        let reply = core.handle_request_vote(RequestVoteArgs {
            term: 3,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 2,
        });

        assert!(reply.vote_granted);
        assert_eq!(core.voted_for(), Some(2));
    }

    // Verifies that a leader sends AppendEntries heartbeats on heartbeat ticks.
    #[tokio::test]
    async fn leader_sends_heartbeats_on_tick() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;
        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;
        drain_append_entries(&mut outbound_rx, 2).await;

        core.handle_heartbeat_tick().await;

        drain_append_entries(&mut outbound_rx, 2).await;
    }

    #[tokio::test]
    async fn client_commands_send_append_entries_immediately() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;
        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;
        drain_append_entries(&mut outbound_rx, 2).await;

        core.handle_test_client_command(false).await;

        for _ in 0..2 {
            match outbound_rx.recv().await.unwrap() {
                OutboundCommand::SendAppendEntries { args, .. } => {
                    assert_eq!(args.prev_log_index, 0);
                    assert_eq!(args.entries, vec![entry(1, 1, false)]);
                }
                OutboundCommand::SendRequestVote { .. } => {
                    panic!("expected immediate AppendEntries command");
                }
                OutboundCommand::SendInstallSnapshot { .. } => {
                    panic!("expected immediate AppendEntries command");
                }
            }
        }
    }

    // Verifies that a failed AppendEntries response backs up nextIndex and retries.
    #[tokio::test]
    async fn append_entries_failure_decrements_next_index() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.push_log_entry(entry(1, 1, true));
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;
        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;
        drain_append_entries(&mut outbound_rx, 2).await;

        core.handle_append_entries_response(
            2,
            1,
            1,
            Ok(AppendEntriesReply {
                term: 1,
                success: false,
            }),
        )
        .await;

        assert_eq!(core.next_index.get(&2), Some(&1));
        drain_append_entries(&mut outbound_rx, 1).await;
    }

    // Verifies that successful AppendEntries updates matchIndex and nextIndex for the follower.
    #[tokio::test]
    async fn append_entries_success_updates_replication_indexes() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.push_log_entry(entry(1, 1, true));
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;
        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;
        drain_append_entries(&mut outbound_rx, 2).await;

        core.handle_append_entries_response(
            2,
            1,
            1,
            Ok(AppendEntriesReply {
                term: 1,
                success: true,
            }),
        )
        .await;

        assert_eq!(core.match_index.get(&2), Some(&1));
        assert_eq!(core.next_index.get(&2), Some(&2));
    }

    // Verifies that leaders only advance commitIndex by majority replication of current-term entries.
    #[tokio::test]
    async fn leader_commits_only_current_term_entries_by_majority() {
        let (mut core, _, mut outbound_rx) = test_core(vec![2, 3]);
        core.push_log_entry(entry(1, 1, false));
        core.current_term = 1;
        core.handle_election_timeout().await;
        drain_request_votes(&mut outbound_rx, 2).await;
        core.handle_request_vote_response(
            2,
            Ok(RequestVoteReply {
                term: 2,
                vote_granted: true,
            }),
        )
        .await;
        drain_append_entries(&mut outbound_rx, 2).await;

        core.handle_append_entries_response(
            2,
            1,
            1,
            Ok(AppendEntriesReply {
                term: 2,
                success: true,
            }),
        )
        .await;

        assert_eq!(core.commit_index, 0);

        core.push_log_entry(entry(2, 2, true));
        core.match_index.insert(1, 2);
        core.handle_append_entries_response(
            2,
            1,
            2,
            Ok(AppendEntriesReply {
                term: 2,
                success: true,
            }),
        )
        .await;

        assert_eq!(core.commit_index, 2);
        assert_eq!(core.last_applied, 2);
        assert!(core.state_machine_value);
    }

    #[tokio::test]
    async fn stale_heartbeat_success_does_not_acknowledge_newer_entries() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.state = NodeState::Leader;
        core.current_term = 1;
        core.push_log_entry(entry(1, 1, true));
        core.match_index.insert(1, 1);
        core.match_index.insert(2, 0);
        core.next_index.insert(2, 1);

        core.handle_append_entries_response(
            2,
            0,
            0,
            Ok(AppendEntriesReply {
                term: 1,
                success: true,
            }),
        )
        .await;

        assert_eq!(core.match_index.get(&2), Some(&0));
        assert_eq!(core.next_index.get(&2), Some(&1));
        assert_eq!(core.commit_index, 0);
    }

    #[tokio::test]
    async fn stale_append_entries_failure_does_not_back_up_newer_progress() {
        let (mut core, _, _) = test_core(vec![2, 3]);
        core.state = NodeState::Leader;
        core.current_term = 1;
        core.match_index.insert(2, 2);
        core.next_index.insert(2, 3);

        core.handle_append_entries_response(
            2,
            0,
            1,
            Ok(AppendEntriesReply {
                term: 1,
                success: false,
            }),
        )
        .await;

        assert_eq!(core.match_index.get(&2), Some(&2));
        assert_eq!(core.next_index.get(&2), Some(&3));
    }
}
