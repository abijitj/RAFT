pub trait WriteAheadLog: Send + Sync {
    fn append_entry(&mut self, index: u64, term: u64, command: &[u8]) -> Result<(), String>;
    fn get_entry(&self, index: u64) -> Result<Option<(u64, Vec<u8>)>, String>;
    fn log_length(&self) -> Result<u64, String>;
    fn save_metadata(&mut self, current_term: u64, voted_for: Option<u32>) -> Result<(), String>;
    fn load_metadata(&self) -> Result<(u64, Option<u32>), String>;
    fn truncate_log(&mut self, last_included_index: u64) -> Result<(), String>;
    fn truncate_log_suffix(&mut self, first_removed_index: u64) -> Result<(), String>;
    fn save_snapshot(&mut self, last_included_index: u64, last_included_term: u64, state_machine_value: bool) -> Result<(), String>;
    fn load_snapshot(&self) -> Result<Option<Snapshot>, String>;
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub state_machine_value: bool,
}

#[cfg(not(target_os = "espidf"))]
pub mod log;

#[cfg(target_os = "espidf")]
pub mod esp_nvs;

#[cfg(test)]
pub mod tests {
    use super::{WriteAheadLog, Snapshot};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    pub struct MockWal {
        log: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
        metadata: Arc<Mutex<HashMap<String, u64>>>,
        snapshot: Arc<Mutex<Option<Snapshot>>>,
    }

    impl MockWal {
        pub fn new() -> Self {
            Self {
                log: Arc::new(Mutex::new(HashMap::new())),
                metadata: Arc::new(Mutex::new(HashMap::new())),
                snapshot: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl WriteAheadLog for MockWal {
        fn append_entry(&mut self, index: u64, term: u64, command: &[u8]) -> Result<(), String> {
            let mut data = Vec::with_capacity(8 + command.len());
            data.extend_from_slice(&term.to_le_bytes());
            data.extend_from_slice(command);
            self.log.lock().unwrap().insert(index, data);
            let mut metadata = self.metadata.lock().unwrap();
            let length = metadata.get("length").copied().unwrap_or(0);
            metadata.insert("length".to_string(), length.max(index));
            Ok(())
        }

        fn get_entry(&self, index: u64) -> Result<Option<(u64, Vec<u8>)>, String> {
            if let Some(data) = self.log.lock().unwrap().get(&index) {
                if data.len() < 8 {
                    return Err("corrupted log entry: too short".into());
                }
                let term = u64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Some((term, data[8..].to_vec())))
            } else {
                Ok(None)
            }
        }

        fn log_length(&self) -> Result<u64, String> {
            Ok(self.metadata.lock().unwrap().get("length").copied().unwrap_or(0))
        }

        fn save_metadata(&mut self, current_term: u64, voted_for: Option<u32>) -> Result<(), String> {
            let mut metadata = self.metadata.lock().unwrap();
            metadata.insert("current_term".to_string(), current_term);
            if let Some(candidate_id) = voted_for {
                metadata.insert("voted_for".to_string(), candidate_id as u64);
            } else {
                metadata.remove("voted_for");
            }
            Ok(())
        }

        fn load_metadata(&self) -> Result<(u64, Option<u32>), String> {
            let metadata = self.metadata.lock().unwrap();
            let current_term = metadata.get("current_term").copied().unwrap_or(0);
            let voted_for = metadata.get("voted_for").copied().map(|id| id as u32);
            Ok((current_term, voted_for))
        }

        fn truncate_log(&mut self, last_included_index: u64) -> Result<(), String> {
            let mut log = self.log.lock().unwrap();
            log.retain(|&k, _| k > last_included_index);
            let length = log.keys().copied().max().unwrap_or(0);
            self.metadata.lock().unwrap().insert("length".to_string(), length);
            Ok(())
        }

        fn truncate_log_suffix(&mut self, first_removed_index: u64) -> Result<(), String> {
            let mut log = self.log.lock().unwrap();
            log.retain(|&k, _| k < first_removed_index);
            let length = log.keys().copied().max().unwrap_or(0);
            self.metadata.lock().unwrap().insert("length".to_string(), length);
            Ok(())
        }

        fn save_snapshot(&mut self, last_included_index: u64, last_included_term: u64, state_machine_value: bool) -> Result<(), String> {
            *self.snapshot.lock().unwrap() = Some(Snapshot {
                last_included_index,
                last_included_term,
                state_machine_value,
            });
            Ok(())
        }

        fn load_snapshot(&self) -> Result<Option<Snapshot>, String> {
            Ok(self.snapshot.lock().unwrap().clone())
        }
    }

    #[test]
    fn log_length_tracks_appended_entries() {
        let mut wal = MockWal::new();
        assert_eq!(wal.log_length().unwrap(), 0);
        wal.append_entry(1, 1, &[1]).unwrap();
        assert_eq!(wal.log_length().unwrap(), 1);
        wal.append_entry(2, 1, &[0]).unwrap();
        assert_eq!(wal.log_length().unwrap(), 2);
    }

    #[test]
    fn suffix_truncation_preserves_log_prefix() {
        let mut wal = MockWal::new();
        for index in 1..=5 {
            wal.append_entry(index, 1, &[1]).unwrap();
        }

        wal.truncate_log_suffix(4).unwrap();

        assert!(wal.get_entry(3).unwrap().is_some());
        assert!(wal.get_entry(4).unwrap().is_none());
        assert!(wal.get_entry(5).unwrap().is_none());
        assert_eq!(wal.log_length().unwrap(), 3);
    }

    #[test]
    fn metadata_round_trips() {
        let mut wal = MockWal::new();
        assert_eq!(wal.load_metadata().unwrap(), (0, None));
        wal.save_metadata(7, Some(3)).unwrap();
        assert_eq!(wal.load_metadata().unwrap(), (7, Some(3)));
    }

    #[test]
    fn snapshot_round_trips() {
        let mut wal = MockWal::new();
        assert!(wal.load_snapshot().unwrap().is_none());
        wal.save_snapshot(5, 2, true).unwrap();
        let snap = wal.load_snapshot().unwrap().unwrap();
        assert_eq!(snap.last_included_index, 5);
        assert_eq!(snap.last_included_term, 2);
        assert!(snap.state_machine_value);
    }
}
