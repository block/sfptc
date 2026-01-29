set positional-arguments := true
set shell := ["bash", "-c"]

# Import modules
mod docker

# Configuration

VERSION := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
GIT_COMMIT := `git rev-parse HEAD 2>/dev/null || echo "unknown"`
TAG := `git rev-parse --short HEAD 2>/dev/null || echo "dev"`
RELEASE := "dist"

_help:
    @just -l

# Run tests
test:
    @gotestsum --hide-summary output,skipped --format-hide-empty-pkg ${CI:+--format github-actions} ./... -- -race -timeout 30s

# Lint code
lint:
    golangci-lint run
    actionlint

# Format code
fmt:
    just --unstable --fmt
    golangci-lint fmt
    go mod tidy

# ============================================================================
# Build
# ============================================================================

# Build for current platform
build:
    @mkdir -p {{ RELEASE }}
    @go build -trimpath -o {{ RELEASE }}/cachewd \
        -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" \
        ./cmd/cachewd
    @echo "✓ Built {{ RELEASE }}/cachewd"

# Build for Linux (current arch)
build-linux:
    #!/usr/bin/env bash
    set -e
    mkdir -p {{ RELEASE }}
    ARCH=$(uname -m)
    [[ "$ARCH" == "x86_64" ]] && ARCH="amd64"
    [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]] && ARCH="arm64"
    echo "Building for linux/${ARCH}..."
    GOOS=linux GOARCH=${ARCH} go build -trimpath \
        -o {{ RELEASE }}/cachewd-linux-${ARCH} \
        -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" \
        ./cmd/cachewd
    echo "✓ Built {{ RELEASE }}/cachewd-linux-${ARCH}"

# Build all platforms
build-all:
    @mkdir -p {{ RELEASE }}
    @echo "Building all platforms..."
    @GOOS=darwin GOARCH=arm64 go build -trimpath -o {{ RELEASE }}/cachewd-darwin-arm64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @GOOS=darwin GOARCH=amd64 go build -trimpath -o {{ RELEASE }}/cachewd-darwin-amd64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @GOOS=linux GOARCH=arm64 go build -trimpath -o {{ RELEASE }}/cachewd-linux-arm64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @GOOS=linux GOARCH=amd64 go build -trimpath -o {{ RELEASE }}/cachewd-linux-amd64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @echo "✓ Built all platforms"

# ============================================================================
# Run
# ============================================================================

# Run natively
run: build
    @echo "→ Starting cachew at http://localhost:8080"
    @mkdir -p state
    @{{ RELEASE }}/cachewd --config cachew.hcl

# Clean up build artifacts
clean:
    @echo "Cleaning..."
    @rm -rf {{ RELEASE }}
    @echo "✓ Cleaned"
