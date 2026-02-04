# syntax=docker/dockerfile:1

# ============================================================================
# Rivven Docker Image
# ============================================================================
#
# Packages pre-built static musl binaries into a minimal scratch image.
# Final image size: ~10-20MB (just the binary + SSL certs)
#
# Usage (CI release pipeline):
#   The `binaries` job builds static binaries, then the `docker` job copies them:
#     binaries/amd64/  - x86_64-unknown-linux-musl binaries
#     binaries/arm64/  - aarch64-unknown-linux-musl binaries
#
# Usage (local build):
#   1. Build binaries locally:
#      cargo build --release --target x86_64-unknown-linux-musl
#   2. Prepare binaries directory:
#      mkdir -p binaries/amd64 && cp target/x86_64-unknown-linux-musl/release/{rivvend,rivven,rivven-connect,rivven-operator,rivven-schema} binaries/amd64/
#   3. Build image:
#      docker build -t rivvend:local .
#
# ============================================================================

ARG BINARY=rivvend

# ============================================================================
# Runtime: scratch (absolute minimum - just the binary)
# ============================================================================
# For static musl binaries, we can use scratch (0 bytes base).
# We only need CA certificates for TLS connections.
FROM scratch

ARG BINARY=rivvend
ARG TARGETARCH

LABEL org.opencontainers.image.title="Rivven"
LABEL org.opencontainers.image.description="High-performance distributed event streaming platform"
LABEL org.opencontainers.image.source="https://github.com/hupe1980/rivven"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Copy CA certificates for TLS (from distroless or alpine)
# This enables HTTPS connections to external services
COPY --from=gcr.io/distroless/static-debian12:nonroot /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy pre-built static binary
# Docker buildx will select the correct architecture directory via TARGETARCH
COPY binaries/${TARGETARCH}/${BINARY} /app

# Data/config directory  
VOLUME ["/data"]

# Ports vary by binary:
#   rivvend:         9092 (broker), 9093 (cluster), 9090 (metrics), 9094 (HTTP API/dashboard)
#   rivven-connect:  8080 (health), 9091 (metrics)
#   rivven-operator: 8080 (metrics), 8081 (health)
#   rivven-schema:   8081 (API), 9090 (metrics)
EXPOSE 8080 8081 9090 9091 9092 9093 9094

# Run as non-root (UID 65532 = nonroot in distroless)
USER 65532:65532

ENTRYPOINT ["/app"]
