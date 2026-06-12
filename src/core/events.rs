use tokio::sync::oneshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: bool,
}

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

#[derive(Debug, Clone)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub state_machine_value: bool,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotReply {
    pub term: u64,
}

pub enum RaftEvent {
    ElectionTimeout,
    HeartbeatTick,
    RequestVote {
        args: RequestVoteArgs,
        reply_channel: oneshot::Sender<RequestVoteReply>,
    },
    RequestVoteResponse {
        from_node_id: u64,
        result: Result<RequestVoteReply, String>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        reply_channel: oneshot::Sender<AppendEntriesReply>,
    },
    AppendEntriesResponse {
        from_node_id: u64,
        sent_prev_log_index: u64,
        sent_last_log_index: u64,
        result: Result<AppendEntriesReply, String>,
    },
    InstallSnapshot {
        args: InstallSnapshotArgs,
        reply_channel: oneshot::Sender<InstallSnapshotReply>,
    },
    InstallSnapshotResponse {
        from_node_id: u64,
        result: Result<InstallSnapshotReply, String>,
    },
    ClientCommand {
        new_value: bool,
        reply_channel: oneshot::Sender<Result<(), String>>,
    },
    TestClientRequest {
        new_value: bool,
    },
}

pub enum OutboundCommand {
    SendRequestVote {
        target_node_id: u64,
        args: RequestVoteArgs,
        result_channel: oneshot::Sender<Result<RequestVoteReply, String>>,
    },
    SendAppendEntries {
        target_node_id: u64,
        args: AppendEntriesArgs,
        result_channel: oneshot::Sender<Result<AppendEntriesReply, String>>,
    },
    SendInstallSnapshot {
        target_node_id: u64,
        args: InstallSnapshotArgs,
        result_channel: oneshot::Sender<Result<InstallSnapshotReply, String>>,
    },
}
