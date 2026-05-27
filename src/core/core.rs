use tokio::sync::mpsc;
use crate::core::events::{RaftEvent, OutboundCommand};

/// The operational states a Raft node can occupy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftCore {
    /// Channel to receive events from the outside world (Network, Timers, Clients)
    inbound_rx: mpsc::Receiver<RaftEvent>,
    /// Channel to dispatch commands out to the Network layer
    outbound_tx: mpsc::Sender<OutboundCommand>,
    
    // --- Persistent Raft State (Required on all nodes) ---
    /// Latest term server has seen (initialized to 0 on first boot)
    current_term: u64,
    /// Candidate ID that received vote in current term (None if none)
    voted_for: Option<u64>,
    
    // --- Volatile Raft State (Required on all nodes) ---
    /// The current role of this node in the cluster
    state: NodeState,
    
    // TODO (Team Member 2): Add your storage engine / log structures here!
}

impl RaftCore {
    /// Creates a bare-bones instance of the Raft Consensus Core
    pub fn new(
        inbound_rx: mpsc::Receiver<RaftEvent>,
        outbound_tx: mpsc::Sender<OutboundCommand>,
    ) -> Self {
        Self {
            inbound_rx,
            outbound_tx,
            current_term: 0,
            voted_for: None,
            state: NodeState::Follower,
        }
    }

    /// The main sequential execution loop for the Raft state machine.
    /// This pulls events off the incoming queue and processes them one by one.
    pub async fn run(&mut self) {
        println!("Raft Core Loop initialized. Starting event processing...");
        while let Some(event) = self.inbound_rx.recv().await {
            todo!()
        }
        println!("Raft Core Loop has shut down because the inbound channel closed.");
    }
}