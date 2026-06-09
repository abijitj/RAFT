#!/bin/bash

set -e

TEST_DIR="tests/shadow_tests"

if [ -z "$1" ]; then
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
  FILE_NAME=$1
  if [ ! -f "$FILE_NAME" ]; then
    echo "Error: File '$FILE_NAME' does not exist."
    exit 1
  fi
fi

echo "Building project in release mode..."
cargo build --release

echo "Removing old shadow.data/ directory and shadow.log..."
rm -rf shadow.data/
rm shadow.log

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

TEST_NAME=$(basename "$FILE_NAME" .yaml)
ARCHIVE_DIR="runs/shadow_tests/${TEST_NAME}"

echo "Archiving results to ${ARCHIVE_DIR}/shadow_data.tar.gz..."
mkdir -p "$ARCHIVE_DIR"
tar -czf "${ARCHIVE_DIR}/shadow_data.tar.gz" shadow.data shadow.log

echo "Archive successfully created!"