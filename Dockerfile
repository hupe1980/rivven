# syntax=docker/dockerfile:1

# ============================================================================
# Rivven Multi-Binary, Multi-Arch Dockerfile
# ============================================================================
#
# Build different images with:
#   docker build -t ghcr.io/hupe1980/rivvend:latest .
#   docker build --build-arg BINARY=rivven-connect -t ghcr.io/hupe1980/rivven-connect:latest .
#   docker build --build-arg BINARY=rivven-operator -t ghcr.io/hupe1980/rivven-operator:latest .
#
# Multi-arch build (uses native compilation on each platform via QEMU):
#   docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/hupe1980/rivvend:latest .
#
# ============================================================================

ARG BINARY=rivvend

# ============================================================================
# Stage 1: Build all binaries (static musl, native compilation per platform)
# ============================================================================
FROM rust:1.89-alpine AS builder

WORKDIR /build

# Install build dependencies for musl static linking
RUN apk add --no-cache \
    musl-dev \
    perl \
    make \
    cmake \
    clang \
    llvm \
    openssl-dev \
    openssl-libs-static

# Add wasm32 target for dashboard
RUN rustup target add wasm32-unknown-unknown

# Install trunk for dashboard build
RUN cargo install trunk --locked

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build dashboard first (targets wasm32)
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    cd crates/rivven-dashboard && trunk build --release

# Build all binaries as static musl binaries (native compilation, no cross-compile)
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-target,target=/build/target \
    RUSTFLAGS="-C target-feature=+crt-static" \
    cargo build --release \
       --package rivvend \
       --package rivven \
       --package rivven-connect \
       --package rivven-operator \
    && mkdir -p /out \
    && cp target/release/rivvend /out/ \
    && cp target/release/rivven /out/ \
    && cp target/release/rivven-connect /out/ \
    && cp target/release/rivven-operator /out/

# ============================================================================
# Stage 2: Runtime (distroless static - no libc needed for static binaries)
# ============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

ARG BINARY=rivvend

LABEL org.opencontainers.image.title="Rivven"
LABEL org.opencontainers.image.description="High-performance distributed event streaming platform"
LABEL org.opencontainers.image.source="https://github.com/hupe1980/rivven"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Copy selected binary and CLI tools
COPY --from=builder /out/${BINARY} /usr/local/bin/app
COPY --from=builder /out/rivven /usr/local/bin/rivven

# Data/config directory
VOLUME ["/data"]

# Ports (broker uses 9092/9094, connect uses 8080, operator uses 8443)
EXPOSE 9092 9094 8080 8443

# Run as non-root
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/app"]
