use std::env;
use std::fs;
use raft::storage::log::UnixWal;
use raft::storage::WriteAheadLog;

fn dir_size(path: &str) -> u64 {
    let meta = match fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return 0,
    };
    if meta.is_file() {
        return meta.len();
    }
    if meta.is_dir() {
        let mut total = 0;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                total += dir_size(&entry.path().to_string_lossy());
            }
        }
        return total;
    }
    0
}

fn main() {
    let path = env::args().nth(1).expect("usage: wal_dump <path>");
    let size_bytes = dir_size(&path);
    let wal = UnixWal::new(&path).expect("failed to open WAL");
    let (term, voted_for) = wal.load_metadata().expect("failed to load metadata");
    let length = wal.log_length().expect("failed to read length");
    let snapshot = wal.load_snapshot().expect("failed to read snapshot");

    println!("term={} voted_for={:?} last_index={}", term, voted_for, length);
    println!("on_disk_size_bytes={}", size_bytes);
    match snapshot {
        Some(s) => println!(
            "snapshot_index={} snapshot_term={} snapshot_value={}",
            s.last_included_index, s.last_included_term, s.state_machine_value
        ),
        None => println!("snapshot_index=0 snapshot_term=0 snapshot_value=none"),
    }

    for i in 1..=length {
        match wal.get_entry(i).expect("failed to read entry") {
            Some((entry_term, command)) => println!("  [{}] term={} command={:?}", i, entry_term, command),
            None => println!("  [{}] missing", i),
        }
    }
}