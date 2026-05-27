pub trait WriteAheadLog {
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