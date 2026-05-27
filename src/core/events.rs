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
    
    /// Triggered by the background timer when no heartbeat is received
    ElectionTimeout,
    
    /// Triggered periodically by the leader to prompt sending heartbeats
    HeartbeatTick,

    // --- Network Events ---
    
    /// Incoming RequestVote RPC from another node
    RequestVote {
        args: RequestVoteArgs,
        reply_channel: oneshot::Sender<RequestVoteReply>,
    },

    /// Incoming AppendEntries RPC (Heartbeat or Log replication)
    AppendEntries {
        args: AppendEntriesArgs,
        reply_channel: oneshot::Sender<AppendEntriesReply>,
    },

    // --- Client Events ---
    
    /// A user wants to update the replicated boolean value
    ClientCommand {
        new_value: bool,
        // Using String as a placeholder for a proper Error enum
        reply_channel: oneshot::Sender<Result<(), String>>,
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