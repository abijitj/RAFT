use tokio::sync::oneshot;

// ============================================================================
// Core Data Types
// ============================================================================

/// A log entry containing your simple boolean application state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: bool,
}

// ============================================================================
// RPC Arguments & Replies (Pure Rust definitions)
// ============================================================================

#[derive(Debug, Clone)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

// ============================================================================
// INBOUND: Network & Timers -> The Brain
// ============================================================================

pub enum RaftEvent {
    // --- Timer Events ---
    
    /// Manual/test hook for triggering the same election path as the core-owned timer
    ElectionTimeout,
    
    /// Triggered periodically by the leader to prompt sending heartbeats
    HeartbeatTick,

    // --- Network Events ---
    
    /// Incoming RequestVote RPC from another node
    RequestVote {
        args: RequestVoteArgs,
        reply_channel: oneshot::Sender<RequestVoteReply>,
    },

    /// RequestVote RPC result returned by the outbound network worker
    RequestVoteResponse {
        from_node_id: u64,
        result: Result<RequestVoteReply, String>,
    },

    /// Incoming AppendEntries RPC (Heartbeat or Log replication)
    AppendEntries {
        args: AppendEntriesArgs,
        reply_channel: oneshot::Sender<AppendEntriesReply>,
    },

    /// AppendEntries RPC result returned by the outbound network worker
    AppendEntriesResponse {
        from_node_id: u64,
        result: Result<AppendEntriesReply, String>,
    },

    // --- Client Events ---
    
    /// A user wants to update the replicated boolean value via network/RPC
    ClientCommand {
        new_value: bool,
        // Using String as a placeholder for a proper Error enum
        reply_channel: oneshot::Sender<Result<(), String>>,
    },

    /// A time-triggered test hook used within Shadow simulations to propose 
    /// a local state change directly into the core event channel without gRPC
    TestClientRequest {
        new_value: bool,
    },
}

// ============================================================================
// OUTBOUND: The Brain -> Network Layer
// ============================================================================

pub enum OutboundCommand {
    /// Broadcast a RequestVote to a specific node and return the result
    SendRequestVote {
        target_node_id: u64,
        args: RequestVoteArgs,
        // Using String as a placeholder for a network Error enum (e.g., Timeout, Disconnected)
        result_channel: oneshot::Sender<Result<RequestVoteReply, String>>,
    },

    /// Send an AppendEntries to a specific node and return the result
    SendAppendEntries {
        target_node_id: u64,
        args: AppendEntriesArgs,
        result_channel: oneshot::Sender<Result<AppendEntriesReply, String>>,
    },
}
