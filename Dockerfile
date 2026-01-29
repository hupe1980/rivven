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
# Stage 1: Build all binaries (static musl binaries via cross-compilation)
# ============================================================================
# Note: Using Debian as the build host because proc-macros (like apache-avro-derive)
# must run on the host and cannot be compiled for musl. We cross-compile to musl
# for the final static binaries. All crates use rustls (pure Rust TLS), no OpenSSL needed.
FROM rust:1.89-bookworm AS builder

WORKDIR /build

# Detect target architecture for musl cross-compilation
ARG TARGETARCH

# Install build dependencies (no OpenSSL needed - we use rustls)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    perl \
    make \
    cmake \
    clang \
    llvm \
    musl-tools \
    musl-dev \
    gcc-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

# Add musl targets for static linking
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

# Add wasm32 target for dashboard
RUN rustup target add wasm32-unknown-unknown

# Install trunk for dashboard build
RUN cargo install trunk --locked

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build all binaries as static musl binaries
# - Proc-macros run on host (glibc), final binaries target musl
# - Using rustls for TLS (pure Rust, no C dependencies)
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-target,target=/build/target \
    if [ "$TARGETARCH" = "arm64" ]; then \
        export TARGET=aarch64-unknown-linux-musl; \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-gnu-gcc; \
        export CC_aarch64_unknown_linux_musl=aarch64-linux-gnu-gcc; \
        export AR_aarch64_unknown_linux_musl=aarch64-linux-gnu-ar; \
    else \
        export TARGET=x86_64-unknown-linux-musl; \
        export CC_x86_64_unknown_linux_musl=musl-gcc; \
    fi && \
    RUSTFLAGS="-C target-feature=+crt-static" \
    cargo build --release --target $TARGET \
       --package rivvend \
       --package rivven \
       --package rivven-connect \
       --package rivven-operator \
    && mkdir -p /out \
    && cp target/$TARGET/release/rivvend /out/ \
    && cp target/$TARGET/release/rivven /out/ \
    && cp target/$TARGET/release/rivven-connect /out/ \
    && cp target/$TARGET/release/rivven-operator /out/ \
    && file /out/rivvend

# ============================================================================
# Stage 2: Runtime (distroless static - no libc needed for musl static binaries)
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

# Ports vary by binary:
#   rivvend:         9092 (broker), 9093 (cluster), 9090 (metrics), 9094 (HTTP API/dashboard)
#   rivven-connect:  8080 (health), 9091 (metrics)
#   rivven-operator: 8080 (metrics), 8081 (health)
# Note: EXPOSE is documentation only - actual port mapping at runtime via -p
EXPOSE 8080 8081 9090 9091 9092 9093 9094

# Run as non-root
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/app"]
