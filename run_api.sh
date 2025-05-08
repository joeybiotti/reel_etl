#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define app directory and entrypoint
APP_DIR="app"
APP_FILE="main.py"

# Check if uvicorn is installed
if ! command -v uvicorn &> /dev/null
then
    echo "Error: uvicorn is not installed. Install it with 'pip install uvicorn[standard]'"
    exit 1
fi

# Check if app directory exists
if [ ! -d "$APP_DIR" ]; then
  echo "Error: App directory '$APP_DIR' does not exist."
  exit 1
fi

# Check if app file exists
if [ ! -f "$APP_DIR/$APP_FILE" ]; then
  echo "Error: App file '$APP_DIR/$APP_FILE' not found."
  exit 1
fi

# Run the API with uvicorn
echo "Starting API..."
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
