pub trait WriteAheadLog: Send + Sync {
    /// Appends a new command to the log
    fn append_entry(&mut self, index: u64, term: u64, command: &[u8]) -> Result<(), String>;
    
    /// Retrieves a command from the log by its index
    fn get_entry(&self, index: u64) -> Result<Option<(u64, Vec<u8>)>, String>;
    
    /// Saves the Raft metadata that must survive a crash
    fn save_metadata(&mut self, current_term: u64, voted_for: Option<u32>) -> Result<(), String>;
    
    /// Discards all entries up to and including the given index
    fn truncate_log(&mut self, last_included_index: u64) -> Result<(), String>;
}

// Compile the Unix redb implementation if not targeting ESP32
#[cfg(not(target_os = "espidf"))]
pub mod log;

// ESP32 NVS implementation if targeting ESP32
#[cfg(target_os = "espidf")]
pub mod esp_nvs;

#[cfg(test)]
pub mod tests {
    use super::WriteAheadLog;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Simple in-memory mock storage for tests
    pub struct MockWal {
        log: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
        metadata: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl MockWal {
        pub fn new() -> Self {
            Self {
                log: Arc::new(Mutex::new(HashMap::new())),
                metadata: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl WriteAheadLog for MockWal {
        fn append_entry(&mut self, index: u64, term: u64, command: &[u8]) -> Result<(), String> {
            let mut data = Vec::with_capacity(8 + command.len());
            data.extend_from_slice(&term.to_le_bytes());
            data.extend_from_slice(command);
            self.log.lock().unwrap().insert(index, data);
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

        fn truncate_log(&mut self, last_included_index: u64) -> Result<(), String> {
            let mut log = self.log.lock().unwrap();
            log.retain(|&k, _| k > last_included_index);
            Ok(())
        }
    }
}