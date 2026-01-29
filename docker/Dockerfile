# Build stage
FROM golang:1.25.5-alpine AS builder

ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG TARGETOS
ARG TARGETARCH

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -o cachewd \
    -ldflags "-s -w -X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT}" \
    ./cmd/cachewd

# Runtime stage
FROM alpine:3.21

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

# Install runtime dependencies for git operations and TLS
RUN apk add --no-cache ca-certificates git tzdata && \
    addgroup -g 1000 cachew && \
    adduser -D -u 1000 -G cachew cachew

# Set working directory (config uses relative paths like ./state/cache)
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/cachewd /usr/local/bin/cachewd

# Copy default configuration file
COPY cachew.hcl /app/cachew.hcl

# Create state directory with proper permissions
RUN mkdir -p /app/state/cache && \
    chown -R cachew:cachew /app

# Switch to non-root user
USER cachew

# Bind to all interfaces in Docker (can be overridden with CACHEW_BIND env var)
ENV CACHEW_BIND=0.0.0.0:8080

ENTRYPOINT ["/usr/local/bin/cachewd"]
CMD ["--config", "/app/cachew.hcl"]
