#!/bin/bash

set -e

TEST_DIR="tests/shadow_tests"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPARE_COMPACTION=0
FILE_NAME=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --compare-compaction)
      COMPARE_COMPACTION=1
      shift
      ;;
    *)
      if [ -n "$FILE_NAME" ]; then
        echo "Error: unexpected argument '$1'" >&2
        exit 1
      fi
      FILE_NAME="$1"
      shift
      ;;
  esac
done

if [ -z "$FILE_NAME" ]; then
  echo "No file name provided. Scanning for nested YAML tests in ${TEST_DIR}..."
  
  shopt -s nullglob
  yaml_files=("${TEST_DIR}"/*/*.yaml)
  shopt -u nullglob

  if [ ${#yaml_files[@]} -eq 0 ]; then
    echo "Error: No .yaml files found in subdirectories of ${TEST_DIR}/."
    exit 1
  fi

  friendly_names=()
  for f in "${yaml_files[@]}"; do
    raw_name=$(basename "$f" .yaml)
    friendly="${raw_name//_/ }"
    friendly="${friendly^}"
    friendly_names+=("$friendly")
  done

  echo ""
  echo "      Available Shadow Tests          "
  
  PS3="Enter the number of the test you want to run: "
  select opt in "${friendly_names[@]}"; do
    if [ -n "$opt" ]; then
      index=$((REPLY - 1))
      FILE_NAME="${yaml_files[$index]}"
      echo -e "\nSelected: $opt ($FILE_NAME)"
      break
    else
      echo "Invalid selection. Please try again."
    fi
  done
else
  if [ ! -f "$FILE_NAME" ]; then
    echo "Error: File '$FILE_NAME' does not exist."
    exit 1
  fi
fi

echo "Building project in release mode..."
cargo build --release

echo "Removing old shadow.data/ directory and shadow.log..."
[ -d shadow.data ] && rm -rf shadow.data/
[ -f shadow.log ] && rm shadow.log

if [ "$COMPARE_COMPACTION" -eq 1 ]; then
  echo "Running compaction comparison for ${FILE_NAME}..."
  "$SCRIPT_DIR/tests/shadow_tests/compare_compaction.sh" "$FILE_NAME"
  exit $?
fi

echo "Running shadow simulation for ${FILE_NAME}..."
shadow "${FILE_NAME}" > shadow.log

echo "Done! Output saved to shadow.log."

BASE_NAME=$(basename "$FILE_NAME")

if [[ "$BASE_NAME" == "storage_failure_fail_stop.yaml" ]]; then
  echo "Evaluating storage failure results..."
  if ! grep -Rhasq "storage failure fail-stop checks passed" shadow.data; then
    echo "FAIL: storage failure probe did not complete" >&2
    exit 1
  fi
  echo "PASS: storage failures stop Raft participation"
fi

if [[ "$BASE_NAME" == "conflict_resolution.yaml" ]]; then
  echo "Evaluating conflict resolution results..."
  tests/shadow_tests/conflict_resolution/assert_conflict_resolution.sh shadow.data
fi

case "$BASE_NAME" in
  leader_completeness_multi_term.yaml)
    echo "Evaluating leader completeness results..."
    tests/shadow_tests/leader_completeness_multi_term/assert_leader_completeness_multi_term.sh shadow.data
    ;;
  asymmetric_partition.yaml)
    echo "Evaluating asymmetric partition results..."
    tests/shadow_tests/asymmetric_partition/assert_asymmetric_partition.sh shadow.data
    ;;
  conflict_repair_crash.yaml)
    echo "Evaluating interrupted conflict repair results..."
    tests/shadow_tests/conflict_repair_crash/assert_conflict_repair_crash.sh shadow.data
    ;;
  interrupted_snapshot_install.yaml)
    echo "Evaluating interrupted snapshot results..."
    tests/shadow_tests/interrupted_snapshot_install/assert_interrupted_snapshot_install.sh shadow.data
    ;;
  leader_crash_after_majority_replication.yaml)
    echo "Evaluating majority replication crash results..."
    tests/shadow_tests/leader_crash_after_majority_replication/assert_leader_crash_after_majority_replication.sh shadow.data
    ;;
esac

TEST_NAME=$(basename "$FILE_NAME" .yaml)
ARCHIVE_DIR="runs/shadow_tests/${TEST_NAME}"

mkdir -p "$ARCHIVE_DIR"

echo "Dumping WAL contents for each node..."
for wal_dir in shadow.data/hosts/*/; do
  host=$(basename "$wal_dir")
  raft_data=$(find "$wal_dir" -name "raft_data_*" | head -1)
  if [ -n "$raft_data" ]; then
    node_dir="${ARCHIVE_DIR}/${host}"
    mkdir -p "$node_dir"
    echo "  Dumping WAL for $host..."
    cargo run --release --bin wal_dump -- "$raft_data" > "${node_dir}/${host}_wal.txt" 2>/dev/null \
      || echo "    (dump failed for $host)"
  fi
done

echo "Checking Raft invariants..."
if cargo run --release --bin invariant_checker -- shadow.data; then
  echo "PASS: all Raft invariants hold"
else
  echo "FAIL: invariant violation detected" >&2
  INVARIANT_FAILED=1
fi

echo "Copying shadow.data and shadow.log to the runs folder..."
cp -r shadow.data "${ARCHIVE_DIR}/"
cp shadow.log "${ARCHIVE_DIR}/"

echo "Archiving results to ${ARCHIVE_DIR}/shadow_data.tar.gz..."
# Create the tar file temporarily outside the archive dir to avoid including the tarball recursively in itself
tar -czf "${ARCHIVE_DIR}.tar.gz" -C "${ARCHIVE_DIR}" .
mv "${ARCHIVE_DIR}.tar.gz" "${ARCHIVE_DIR}/shadow_data.tar.gz"

echo "Archive successfully created!"

echo "Analyzing timing data..."
python3 analyze_shadow_log.py shadow.log > "${ARCHIVE_DIR}/timing_report.txt" || true

if [ -n "${INVARIANT_FAILED:-}" ]; then
  exit 1
fi
