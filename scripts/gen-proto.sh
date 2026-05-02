#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

if ! command -v protoc >/dev/null 2>&1; then
  echo "error: protoc not found in PATH" >&2
  exit 1
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  echo "error: protoc-gen-go not found in PATH" >&2
  echo "install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
  exit 1
fi

protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=module=github.com/tursom/turntf-go \
  client.proto \
  relay.proto
