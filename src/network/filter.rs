use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct BlockRule {
    pub sender_id: u64,
    pub start_ms: u128,
    pub end_ms: u128,
}

pub struct NetworkFilter {
    start_time: Instant,
    rules: Vec<BlockRule>,
    dropped: AtomicU64,
}

impl NetworkFilter {
    pub fn new(rules: Vec<BlockRule>) -> Self {
        Self { start_time: Instant::now(), rules, dropped: AtomicU64::new(0) }
    }

    pub fn should_block_sender(&self, sender_id: u64) -> bool {
        let elapsed = self.start_time.elapsed().as_millis();
        let blocked = self.rules.iter().any(|rule| {
            rule.sender_id == sender_id && elapsed >= rule.start_ms && elapsed <= rule.end_ms
        });
        if blocked {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        blocked
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}