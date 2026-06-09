use raft::core::events::RaftEvent;
use raft::core::RaftCore;
use raft::storage::WriteAheadLog;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::sync::mpsc;

struct FailingWal {
    fail_reads: bool,
    fail_writes: bool,
}

impl WriteAheadLog for FailingWal {
    fn append_entry(
        &mut self,
        _index: u64,
        _term: u64,
        _command: &[u8],
    ) -> Result<(), String> {
        if self.fail_writes {
            Err("injected append failure".to_string())
        } else {
            Ok(())
        }
    }

    fn get_entry(&self, _index: u64) -> Result<Option<(u64, Vec<u8>)>, String> {
        if self.fail_reads {
            Err("injected read failure".to_string())
        } else {
            Ok(None)
        }
    }

    fn log_length(&self) -> Result<u64, String> {
        if self.fail_reads {
            Err("injected length failure".to_string())
        } else {
            Ok(0)
        }
    }

    fn save_metadata(
        &mut self,
        _current_term: u64,
        _voted_for: Option<u32>,
    ) -> Result<(), String> {
        if self.fail_writes {
            Err("injected metadata write failure".to_string())
        } else {
            Ok(())
        }
    }

    fn load_metadata(&self) -> Result<(u64, Option<u32>), String> {
        if self.fail_reads {
            Err("injected metadata read failure".to_string())
        } else {
            Ok((0, None))
        }
    }

    fn truncate_log(&mut self, _last_included_index: u64) -> Result<(), String> {
        if self.fail_writes {
            Err("injected truncate failure".to_string())
        } else {
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() {
    let startup_failed = catch_unwind(AssertUnwindSafe(|| {
        let (_inbound_tx, inbound_rx) = mpsc::channel(1);
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let storage = Box::new(FailingWal {
            fail_reads: true,
            fail_writes: false,
        });
        let _ = RaftCore::new(inbound_rx, outbound_tx, storage);
    }))
    .is_err();

    assert!(
        startup_failed,
        "RaftCore continued after persistent metadata could not be loaded"
    );

    let (inbound_tx, inbound_rx) = mpsc::channel(1);
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let storage = Box::new(FailingWal {
        fail_reads: false,
        fail_writes: true,
    });
    let mut core = RaftCore::new(inbound_rx, outbound_tx, storage);
    let core_task = tokio::spawn(async move {
        core.run().await;
    });

    inbound_tx
        .send(RaftEvent::ElectionTimeout)
        .await
        .expect("core stopped before the injected election");

    let result = core_task.await;
    assert!(
        result.is_err() && result.unwrap_err().is_panic(),
        "RaftCore continued participating after a metadata write failure"
    );

    println!("storage failure fail-stop checks passed");
}
