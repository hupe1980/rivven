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
# Multi-arch build:
#   docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/hupe1980/rivvend:latest .
#
# ============================================================================

ARG BINARY=rivvend

# ============================================================================
# Stage 1: Build all binaries (static musl with cargo-zigbuild)
# ============================================================================
FROM --platform=$BUILDPLATFORM rust:1.89-bookworm AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

WORKDIR /build

# Install zig (for cargo-zigbuild) and other build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    xz-utils \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSfL https://ziglang.org/download/0.13.0/zig-linux-$(uname -m)-0.13.0.tar.xz | tar -xJ -C /usr/local \
    && ln -s /usr/local/zig-linux-$(uname -m)-0.13.0/zig /usr/local/bin/zig

# Add musl targets for static linking and wasm32 for dashboard
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl wasm32-unknown-unknown

# Install cargo-zigbuild and trunk
RUN cargo install cargo-zigbuild trunk --locked

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build dashboard first (runs on build platform, targets wasm32)
# Use target-specific cache ID to prevent corruption between parallel builds
RUN --mount=type=cache,id=cargo-registry-${TARGETPLATFORM},target=/usr/local/cargo/registry \
    cd crates/rivven-dashboard && trunk build --release

# Determine target based on platform and build static binaries with zigbuild
RUN --mount=type=cache,id=cargo-registry-${TARGETPLATFORM},target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-target-${TARGETPLATFORM},target=/build/target \
    case "$TARGETPLATFORM" in \
      "linux/arm64") \
        export TARGET="aarch64-unknown-linux-musl" \
        ;; \
      *) \
        export TARGET="x86_64-unknown-linux-musl" \
        ;; \
    esac \
    && cargo zigbuild --release --target $TARGET \
       --package rivvend \
       --package rivven \
       --package rivven-connect \
       --package rivven-operator \
    && mkdir -p /out \
    && cp target/$TARGET/release/rivvend /out/ \
    && cp target/$TARGET/release/rivven /out/ \
    && cp target/$TARGET/release/rivven-connect /out/ \
    && cp target/$TARGET/release/rivven-operator /out/

# ============================================================================
# Stage 2: Runtime (distroless static - no libc needed for musl binaries)
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
