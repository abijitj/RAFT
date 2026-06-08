#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if a file name argument was provided
if [ -z "$1" ]; then
  echo "Error: No file name provided."
  echo "Usage: ./run_shadow.sh <file_name>"
  exit 1
fi

FILE_NAME=$1

echo "Building project in release mode..."
cargo build --release

echo "Removing old shadow.data/ directory..."
rm -rf shadow.data/

echo "Running shadow simulation for ${FILE_NAME}..."
shadow "${FILE_NAME}" > shadow.log

echo "Done! Output saved to shadow.log."