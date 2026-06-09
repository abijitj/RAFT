# Shadow Storage Tests

Run both storage-focused simulations from the repository root:

```bash
./shadow_tests/run_storage_tests.sh
```

The runner builds all release binaries and executes:

- `persistent_metadata_restart.yaml`: restarts a single-node cluster on the
  same Shadow host. It passes when the second process restores term 1 and its
  vote for node 1, then begins its next election in term 2.
- `storage_failure_fail_stop.yaml`: runs a probe with an injected failing WAL.
  It passes only when a metadata read failure prevents construction and a
  metadata write failure terminates the running Raft core.

The restart scenario can also be run independently:

```bash
cargo build --release
./shadow_tests/assert_persistent_metadata_restart.sh
```
