// Write ahead log implementation for RAFT

use super::WriteAheadLog;
use redb::{Database, TableDefinition, ReadableTable, Error as RedbError};
use std::path::Path;

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log"); // For the actual log entries
const METADATA_TABLE: TableDefinition<&str, u64> = TableDefinition::new("raft_metadata"); // For the persistent metadata (term and voted_for)

pub struct UnixWal {
    db: Database,
}

impl UnixWal {
    /// Initializes the Database and creates the tables if they don't exist.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let db = Database::create(path).map_err(|e| e.to_string())?;
        
        // Open a write transaction immediately to ensure tables are created
        let write_txn = db.begin_write().map_err(|e| e.to_string())?;
        {
            let _ = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let _ = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        
        Ok(Self { db })
    }
}

impl WriteAheadLog for UnixWal {
    fn append_entry(&mut self, index: u64, _term: u64, command: &[u8]) -> Result<(), String> {
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            table.insert(index, command).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn get_entry(&self, index: u64) -> Result<Option<Vec<u8>>, String> {
        let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
        
        let result = table.get(index).map_err(|e| e.to_string())?;
        if let Some(accessguard) = result {
            Ok(Some(accessguard.value().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn save_metadata(&mut self, current_term: u64, voted_for: Option<u32>) -> Result<(), String> {
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
            table.insert("current_term", current_term).map_err(|e| e.to_string())?;
            
            if let Some(candidate_id) = voted_for {
                table.insert("voted_for", candidate_id as u64).map_err(|e| e.to_string())?;
            } else {
                table.remove("voted_for").map_err(|e| e.to_string())?;
            }
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn truncate_log(&mut self, last_included_index: u64) -> Result<(), String> {
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let range = 0..=last_included_index;
            let mut keys_to_delete = Vec::new();
            
            for result in table.range(range).map_err(|e| e.to_string())? {
                if let Ok((key, _)) = result {
                    keys_to_delete.push(key.value());
                }
            }
            
            for key in keys_to_delete {
                table.remove(key).map_err(|e| e.to_string())?;
            }
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }
}