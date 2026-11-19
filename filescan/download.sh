#!/usr/bin/env bash

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <release-tag>" >&2
    exit 1
fi

if [[ -z "${TOKEN:-}" ]]; then
    read -rsp "GitHub token: " TOKEN
    echo
fi

RELEASE_TAG=$1

rm -f filescan

# Fetch release info from GitHub API
TAG_JSON=$(curl -L \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/x-atlas-consortia/file-management-service/releases/tags/$RELEASE_TAG")

# Extract asset info (first asset)
ASSET_URL=$(echo "$TAG_JSON" | jq -r '.assets[0].url')
ASSET_DIGEST=$(echo "$TAG_JSON" | jq -r '.assets[0].digest')

curl -L \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/octet-stream" \
  -o filescan \
  "$ASSET_URL"

# Calculate and compare checksum
DOWNLOADED_SHA256=$(sha256sum filescan | awk '{print $1}')
EXPECTED_SHA256=${ASSET_DIGEST#sha256:}

if [[ "$DOWNLOADED_SHA256" == "$EXPECTED_SHA256" ]]; then
    echo "Checksum matches."
else
    echo "Checksum does NOT match!"
    echo "Expected: $EXPECTED_SHA256"
    echo "Actual:   $DOWNLOADED_SHA256"
    exit 1
fi

chmod +x filescan
