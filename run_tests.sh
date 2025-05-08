#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if pytest is installed
if ! command -v pytest &> /dev/null
then
    echo "Error: pytest is not installed. Install it with 'pip install pytest'"
    exit 1
fi

# Set default test directory
TEST_DIR="tests"

# Check if test directory exists
if [ ! -d "$TEST_DIR" ]; then
  echo "Error: Test directory '$TEST_DIR' does not exist."
  exit 1
fi

# Run pytest
echo "Running tests..."
pytest "$TEST_DIR"
