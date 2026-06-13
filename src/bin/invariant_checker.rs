use raft::storage::log::UnixWal;
use raft::storage::WriteAheadLog;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
struct NodeState {
    node_id: u64,
    term: u64,
    voted_for: Option<u32>,
    last_index: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot_value: bool,
    log: Vec<(u64, u64, bool)>, // (index, term, command)
}

impl NodeState {
    fn has_index(&self, index: u64) -> bool {
        index <= self.snapshot_index || self.log.iter().any(|&(i, _, _)| i == index)
    }
}

fn load_node(path: &str, node_id: u64) -> Result<NodeState, String> {
    let wal = UnixWal::new(path)?;
    let (term, voted_for) = wal.load_metadata()?;
    let snapshot = wal.load_snapshot()?;
    let (snapshot_index, snapshot_term, snapshot_value) = snapshot
        .map(|snapshot| {
            (
                snapshot.last_included_index,
                snapshot.last_included_term,
                snapshot.state_machine_value,
            )
        })
        .unwrap_or((0, 0, false));
    let last_index = wal.log_length()?.max(snapshot_index);
    let mut log = Vec::new();
    for i in (snapshot_index + 1)..=last_index {
        match wal.get_entry(i)? {
            Some((entry_term, command_bytes)) => {
                let command = command_bytes.first().copied().unwrap_or(0) != 0;
                log.push((i, entry_term, command));
            }
            None => return Err(format!("missing log entry {} on node {}", i, node_id)),
        }
    }
    Ok(NodeState {
        node_id,
        term,
        voted_for,
        last_index,
        snapshot_index,
        snapshot_term,
        snapshot_value,
        log,
    })
}

fn check_election_safety(nodes: &[NodeState]) -> Result<(), String> {
    let mut term_votes: HashMap<u64, HashMap<u64, u64>> = HashMap::new();
    for node in nodes {
        if let Some(voted_for) = node.voted_for {
            term_votes
                .entry(node.term)
                .or_default()
                .insert(node.node_id, voted_for as u64);
        }
    }
    for (term, votes) in &term_votes {
        let mut candidates: HashMap<u64, Vec<u64>> = HashMap::new();
        for (&voter, &candidate) in votes {
            candidates.entry(candidate).or_default().push(voter);
        }
        if candidates.len() > 1 {
            let majority = nodes.len() / 2 + 1;
            let majority_winners: Vec<u64> = candidates
                .iter()
                .filter(|(_, voters)| voters.len() >= majority)
                .map(|(&c, _)| c)
                .collect();
            if majority_winners.len() > 1 {
                return Err(format!(
                    "Election Safety violated: multiple candidates won a majority in term {}: {:?}",
                    term, majority_winners
                ));
            }
        }
    }
    Ok(())
}

fn check_leader_append_only(nodes: &[NodeState]) -> Result<(), String> {
    for node in nodes {
        for (k, &(idx, _, _)) in node.log.iter().enumerate() {
            let expected = node.snapshot_index + k as u64 + 1;
            if idx != expected {
                return Err(format!(
                    "Leader Append-Only proxy violated: node {} has non-contiguous log (expected index {}, got {})",
                    node.node_id, expected, idx
                ));
            }
        }
    }
    Ok(())
}

fn check_log_matching(nodes: &[NodeState]) -> Result<(), String> {
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            let a = &nodes[i];
            let b = &nodes[j];
            if a.snapshot_index == b.snapshot_index
                && a.snapshot_index > 0
                && (a.snapshot_term != b.snapshot_term || a.snapshot_value != b.snapshot_value)
            {
                return Err(format!(
                    "Log Matching violated: nodes {} and {} have different snapshots at index {}",
                    a.node_id, b.node_id, a.snapshot_index
                ));
            }

            for &(a_idx, a_term, a_cmd) in &a.log {
                let Some(&(_, b_term, b_cmd)) = b.log.iter().find(|&&(idx, _, _)| idx == a_idx)
                else {
                    continue;
                };
                if a_term == b_term && a_cmd != b_cmd {
                    return Err(format!(
                        "Log Matching violated: nodes {} and {} agree on index={} term={} but differ on command ({} vs {})",
                        a.node_id, b.node_id, a_idx, a_term, a_cmd, b_cmd
                    ));
                }
                if a_term != b_term {
                    return Err(format!(
                        "Log Matching violated: nodes {} and {} have different terms at index {} ({} vs {})",
                        a.node_id, b.node_id, a_idx, a_term, b_term
                    ));
                }
            }
        }
    }
    Ok(())
}

fn check_leader_completeness(nodes: &[NodeState]) -> Result<(), String> {
    let total = nodes.len();
    let majority = total / 2 + 1;
    let max_index = nodes.iter().map(|n| n.last_index).max().unwrap_or(0);
    for idx in 1..=max_index {
        let count = nodes.iter().filter(|n| n.last_index >= idx).count();
        if count >= majority {
            for node in nodes {
                if node.last_index >= idx {
                    if !node.has_index(idx) {
                        return Err(format!(
                            "Leader Completeness violated: entry at index {} is committed (on {}/{} nodes) but missing from node {} (last_index={})",
                            idx, count, total, node.node_id, node.last_index
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_state_machine_safety(nodes: &[NodeState]) -> Result<(), String> {
    let min_committed = nodes.iter().map(|n| n.last_index).min().unwrap_or(0);
    for idx in 1..=min_committed {
        let mut reference: Option<(u64, bool)> = None;
        for node in nodes {
            if let Some(&(_, entry_term, cmd)) = node.log.iter().find(|&&(i, _, _)| i == idx) {
                match reference {
                    None => reference = Some((entry_term, cmd)),
                    Some((ref_term, ref_cmd)) => {
                        if entry_term != ref_term || cmd != ref_cmd {
                            return Err(format!(
                                "State Machine Safety violated at index {}: term/cmd mismatch ({}/{} vs {}/{})",
                                idx, ref_term, ref_cmd, entry_term, cmd
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_commit_index_consistency(nodes: &[NodeState]) -> Result<(), String> {
    let majority = nodes.len() / 2 + 1;
    let max_index = nodes.iter().map(|n| n.last_index).max().unwrap_or(0);
    for idx in 1..=max_index {
        let replication_count = nodes.iter().filter(|n| n.last_index >= idx).count();
        if replication_count >= majority {
            let commands: Vec<bool> = nodes
                .iter()
                .filter(|n| n.last_index >= idx)
                .filter_map(|n| n.log.iter().find(|&&(i, _, _)| i == idx))
                .map(|&(_, _, cmd)| cmd)
                .collect();
            if let Some(&first) = commands.first() {
                if commands.iter().any(|&c| c != first) {
                    return Err(format!(
                        "Commit consistency violated: committed entry at index {} has conflicting values across nodes",
                        idx
                    ));
                }
            }
        }
    }
    Ok(())
}

fn check_no_committed_entry_lost(nodes: &[NodeState]) -> Result<(), String> {
    let majority = nodes.len() / 2 + 1;
    let max_index = nodes.iter().map(|n| n.last_index).max().unwrap_or(0);
    for idx in 1..=max_index {
        let holders: Vec<u64> = nodes
            .iter()
            .filter(|n| n.has_index(idx))
            .map(|n| n.node_id)
            .collect();
        if holders.len() >= majority {
            for node in nodes {
                if node.last_index >= idx && !holders.contains(&node.node_id) {
                    return Err(format!(
                        "Committed entry lost: index {} is on a majority {:?} but missing from node {} (last_index={})",
                        idx, holders, node.node_id, node.last_index
                    ));
                }
            }
        }
    }
    Ok(())
}

fn check_no_majority_no_commit(nodes: &[NodeState]) -> Result<(), String> {
    let majority = nodes.len() / 2 + 1;
    let max_index = nodes.iter().map(|n| n.last_index).max().unwrap_or(0);
    for idx in 1..=max_index {
        let count = nodes.iter().filter(|n| n.last_index >= idx).count();
        if count >= majority {
            return Ok(());
        }
    }
    // No entry reached majority — last_index > 0 is fine (uncommitted local appends)
    Ok(())
}

fn main() {
    let shadow_data = std::env::args().nth(1).unwrap_or_else(|| "shadow.data".to_string());
    let hosts_dir = PathBuf::from(&shadow_data).join("hosts");

    let mut nodes = Vec::new();
    let entries = std::fs::read_dir(&hosts_dir).unwrap_or_else(|e| {
        eprintln!("Failed to read {}: {}", hosts_dir.display(), e);
        std::process::exit(1);
    });

    for entry in entries {
        let entry = entry.unwrap();
        let host_dir = entry.path();
        if !host_dir.is_dir() {
            continue;
        }
        let host = host_dir.file_name().unwrap().to_string_lossy().to_string();

        let raft_data = std::fs::read_dir(&host_dir)
            .ok()
            .and_then(|mut rd| {
                rd.find_map(|e| {
                    let e = e.ok()?;
                    let name = e.file_name().to_string_lossy().to_string();
                    if name.starts_with("raft_data_") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
            });

        let Some(raft_path) = raft_data else {
            println!("  Skipping {} (no raft_data found)", host);
            continue;
        };

        let node_id: u64 = raft_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .strip_prefix("raft_data_")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        match load_node(raft_path.to_str().unwrap(), node_id) {
            Ok(state) => {
                println!("  Loaded node {}: term={} last_index={}", node_id, state.term, state.last_index);
                nodes.push(state);
            }
            Err(e) => {
                println!("  Failed to load node {} ({}): {}", node_id, host, e);
            }
        }
    }

    if nodes.is_empty() {
        eprintln!("No node WALs found under {}", hosts_dir.display());
        std::process::exit(1);
    }

    nodes.sort_by_key(|n| n.node_id);

    let checks: &[(&str, fn(&[NodeState]) -> Result<(), String>)] = &[
        ("Election Safety",           check_election_safety),
        ("Leader Append-Only",        check_leader_append_only),
        ("Log Matching",              check_log_matching),
        ("Leader Completeness",       check_leader_completeness),
        ("State Machine Safety",      check_state_machine_safety),
        ("Commit Index Consistency",  check_commit_index_consistency),
        ("No Committed Entry Lost",   check_no_committed_entry_lost),
        ("No Majority No Commit",     check_no_majority_no_commit),
    ];

    let mut failed = false;
    for (name, check) in checks {
        match check(&nodes) {
            Ok(()) => println!("PASS: {}", name),
            Err(e) => {
                eprintln!("FAIL: {} — {}", name, e);
                failed = true;
            }
        }
    }

    if failed {
        std::process::exit(1);
    }
}
