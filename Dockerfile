# syntax=docker/dockerfile:1

# ============================================================================
# Rivven Multi-Binary, Multi-Arch Dockerfile
# ============================================================================
#
# Build different images with:
#   docker build -t ghcr.io/hupe1980/rivven:latest .
#   docker build --build-arg BINARY=rivven-connect -t ghcr.io/hupe1980/rivven-connect:latest .
#   docker build --build-arg BINARY=rivven-operator -t ghcr.io/hupe1980/rivven-operator:latest .
#
# Multi-arch build:
#   docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/hupe1980/rivven:latest .
#
# ============================================================================

ARG BINARY=rivvend

# ============================================================================
# Stage 1: Build all binaries
# ============================================================================
FROM --platform=$BUILDPLATFORM rust:1.83-bookworm AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

WORKDIR /build

# Install build dependencies for cross-compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-tools \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*

# Add musl targets for static linking and wasm32 for dashboard
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl wasm32-unknown-unknown

# Install trunk for building the dashboard
RUN cargo install trunk --locked

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Determine target based on platform
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    case "$TARGETPLATFORM" in \
      "linux/arm64") \
        export TARGET="aarch64-unknown-linux-musl" \
        && export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="aarch64-linux-gnu-gcc" \
        && export CC_aarch64_unknown_linux_musl="aarch64-linux-gnu-gcc" \
        ;; \
      *) \
        export TARGET="x86_64-unknown-linux-musl" \
        ;; \
    esac \
    && cargo build --release --target $TARGET \
       --package rivven-server \
       --package rivven-cli \
       --package rivven-connect \
       --package rivven-operator \
    && mkdir -p /out \
    && cp target/$TARGET/release/rivvend /out/ \
    && cp target/$TARGET/release/rivvenctl /out/ \
    && cp target/$TARGET/release/rivven-connect /out/ \
    && cp target/$TARGET/release/rivven-operator /out/

# ============================================================================
# Stage 2: Runtime (distroless static - no libc, no shell)
# ============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

ARG BINARY=rivvend

LABEL org.opencontainers.image.title="Rivven"
LABEL org.opencontainers.image.description="High-performance distributed event streaming platform"
LABEL org.opencontainers.image.source="https://github.com/hupe1980/rivven"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Copy selected binary and CLI tools
COPY --from=builder /out/${BINARY} /usr/local/bin/app
COPY --from=builder /out/rivvenctl /usr/local/bin/rivvenctl

# Data/config directory
VOLUME ["/data"]

# Ports (broker uses 9092/9094, connect uses 8080, operator uses 8443)
EXPOSE 9092 9094 8080 8443

# Run as non-root
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/app"]
