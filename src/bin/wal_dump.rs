use std::env;
use raft::storage::log::UnixWal;
use raft::storage::WriteAheadLog;

fn main() {
    let path = env::args().nth(1).expect("usage: wal_dump <path>");
    let wal = UnixWal::new(&path).expect("failed to open WAL");
    let (term, voted_for) = wal.load_metadata().expect("failed to load metadata");
    let length = wal.log_length().expect("failed to read length");
    println!("term={} voted_for={:?} last_index={}", term, voted_for, length);
    for i in 1..=length {
        match wal.get_entry(i).expect("failed to read entry") {
            Some((entry_term, command)) => println!("  [{}] term={} command={:?}", i, entry_term, command),
            None => println!("  [{}] missing", i),
        }
    }
}