#!/usr/bin/env bash

# Change to the script directory
cd "$(dirname "$0")"

# Create bin directory if it doesn't exist
mkdir -p bin

# Build the Docker image to compile the Go application
docker build --platform=linux/amd64 --no-cache --tag x-atlas-consortia/filescan .
sleep 2
docker run --rm --platform=linux/amd64 -v $(pwd)/bin:/dist x-atlas-consortia/filescan

echo "Build complete. The binary is located in the 'bin' directory."
