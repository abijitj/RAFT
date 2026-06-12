use super::{WriteAheadLog, Snapshot};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::convert::TryInto;
use std::path::Path;

const LOG_TABLE: TableDefinition<u64, Vec<u8>> = TableDefinition::new("raft_log");
const METADATA_TABLE: TableDefinition<&str, u64> = TableDefinition::new("raft_metadata");
// Snapshot stored as a single blob: 8 bytes last_included_index + 8 bytes last_included_term + 1 byte value
const SNAPSHOT_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("raft_snapshot");

pub struct UnixWal {
    db: Database,
}

impl UnixWal {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let db = Database::create(path).map_err(|e| e.to_string())?;

        let write_txn = db.begin_write().map_err(|e| e.to_string())?;
        {
            let _ = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let _ = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
            let _ = write_txn.open_table(SNAPSHOT_TABLE).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;

        // Populate the length key for databases created before it existed.
        let read_txn = db.begin_read().map_err(|e| e.to_string())?;
        let metadata = read_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
        let has_length = metadata.get("length").map_err(|e| e.to_string())?.is_some();
        drop(metadata);
        drop(read_txn);

        if !has_length {
            let read_txn = db.begin_read().map_err(|e| e.to_string())?;
            let table = read_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let mut length = 0;
            for result in table.range(0..).map_err(|e| e.to_string())? {
                let (key, _) = result.map_err(|e| e.to_string())?;
                length = length.max(key.value());
            }
            drop(table);
            drop(read_txn);

            let write_txn = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut metadata = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
                metadata.insert("length", length).map_err(|e| e.to_string())?;
            }
            write_txn.commit().map_err(|e| e.to_string())?;
        }

        Ok(Self { db })
    }
}

impl WriteAheadLog for UnixWal {
    fn append_entry(&mut self, index: u64, term: u64, command: &[u8]) -> Result<(), String> {
        let mut data = Vec::with_capacity(8 + command.len());
        data.extend_from_slice(&term.to_le_bytes());
        data.extend_from_slice(command);

        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            table.insert(index, data).map_err(|e| e.to_string())?;
        }
        {
            let mut metadata = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
            let length = metadata
                .get("length")
                .map_err(|e| e.to_string())?
                .map(|v| v.value())
                .unwrap_or(0);
            metadata.insert("length", length.max(index)).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn get_entry(&self, index: u64) -> Result<Option<(u64, Vec<u8>)>, String> {
        let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
        let result = table.get(index).map_err(|e| e.to_string())?;
        if let Some(accessguard) = result {
            let bytes = accessguard.value();
            if bytes.len() < 8 {
                return Err("corrupted log entry: too short".into());
            }
            let term = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            Ok(Some((term, bytes[8..].to_vec())))
        } else {
            Ok(None)
        }
    }

    fn log_length(&self) -> Result<u64, String> {
        let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
        let length = table
            .get("length")
            .map_err(|e| e.to_string())?
            .map(|v| v.value())
            .unwrap_or(0);
        Ok(length)
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

    fn load_metadata(&self) -> Result<(u64, Option<u32>), String> {
        let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
        let current_term = table
            .get("current_term")
            .map_err(|e| e.to_string())?
            .map(|v| v.value())
            .unwrap_or(0);
        let voted_for = table
            .get("voted_for")
            .map_err(|e| e.to_string())?
            .map(|v| v.value() as u32);
        Ok((current_term, voted_for))
    }

    fn truncate_log(&mut self, last_included_index: u64) -> Result<(), String> {
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let mut keys_to_delete = Vec::new();
            for result in table.range(0..=last_included_index).map_err(|e| e.to_string())? {
                if let Ok((key, _)) = result {
                    keys_to_delete.push(key.value());
                }
            }
            for key in keys_to_delete {
                table.remove(key).map_err(|e| e.to_string())?;
            }
        }
        {
            let table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let mut length = 0;
            for result in table.range(0..).map_err(|e| e.to_string())? {
                let (key, _) = result.map_err(|e| e.to_string())?;
                length = length.max(key.value());
            }
            drop(table);
            let mut metadata = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
            metadata.insert("length", length).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn truncate_log_suffix(&mut self, first_removed_index: u64) -> Result<(), String> {
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let mut keys_to_delete = Vec::new();
            for result in table.range(first_removed_index..).map_err(|e| e.to_string())? {
                let (key, _) = result.map_err(|e| e.to_string())?;
                keys_to_delete.push(key.value());
            }
            for key in keys_to_delete {
                table.remove(key).map_err(|e| e.to_string())?;
            }
        }
        {
            let table = write_txn.open_table(LOG_TABLE).map_err(|e| e.to_string())?;
            let mut length = 0;
            for result in table.range(0..).map_err(|e| e.to_string())? {
                let (key, _) = result.map_err(|e| e.to_string())?;
                length = length.max(key.value());
            }
            drop(table);
            let mut metadata = write_txn.open_table(METADATA_TABLE).map_err(|e| e.to_string())?;
            metadata.insert("length", length).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn save_snapshot(&mut self, last_included_index: u64, last_included_term: u64, state_machine_value: bool) -> Result<(), String> {
        let mut blob = Vec::with_capacity(17);
        blob.extend_from_slice(&last_included_index.to_le_bytes());
        blob.extend_from_slice(&last_included_term.to_le_bytes());
        blob.push(if state_machine_value { 1u8 } else { 0u8 });

        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut table = write_txn.open_table(SNAPSHOT_TABLE).map_err(|e| e.to_string())?;
            table.insert("snapshot", blob).map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<Snapshot>, String> {
        let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = read_txn.open_table(SNAPSHOT_TABLE).map_err(|e| e.to_string())?;
        let result = table.get("snapshot").map_err(|e| e.to_string())?;
        if let Some(guard) = result {
            let blob = guard.value();
            if blob.len() < 17 {
                return Err("corrupted snapshot: too short".into());
            }
            let last_included_index = u64::from_le_bytes(blob[0..8].try_into().unwrap());
            let last_included_term = u64::from_le_bytes(blob[8..16].try_into().unwrap());
            let state_machine_value = blob[16] != 0;
            Ok(Some(Snapshot { last_included_index, last_included_term, state_machine_value }))
        } else {
            Ok(None)
        }
    }
}
