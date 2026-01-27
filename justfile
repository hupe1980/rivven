# Rivven - Event Streaming Platform
# https://github.com/hupe1980/rivven

set shell := ["bash", "-uc"]

# Default recipe - show help
default:
    @just --list

# ============================================================================
# Development
# ============================================================================

# Build all crates in debug mode
build:
    cargo build --workspace

# Build all crates in release mode
build-release:
    cargo build --workspace --release

# Run all tests
test:
    cargo test --workspace --all-features

# Run tests with output
test-verbose:
    cargo test --workspace --all-features -- --nocapture

# Run a specific test
test-one NAME:
    cargo test --workspace --all-features {{NAME}} -- --nocapture

# Check code compiles without building
check:
    cargo check --workspace --all-features

# Format code
fmt:
    cargo fmt --all

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Run clippy linter
clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run all checks (fmt, clippy, test)
ci: fmt-check clippy test

# ============================================================================
# Documentation
# ============================================================================

# Build documentation
doc:
    cargo doc --workspace --no-deps --all-features

# Build and open documentation
doc-open:
    cargo doc --workspace --no-deps --all-features --open

# Serve Jekyll docs locally
docs-serve:
    cd docs && bundle install && bundle exec jekyll serve --baseurl ""

# ============================================================================
# Running
# ============================================================================

# Run the broker (rivvend)
server *ARGS:
    cargo run --bin rivvend -- {{ARGS}}

# Run the broker in release mode
server-release *ARGS:
    cargo run --release --bin rivvend -- {{ARGS}}

# Run the CLI (rivvenctl)
cli *ARGS:
    cargo run --bin rivvenctl -- {{ARGS}}

# Run rivven-connect
connect *ARGS:
    cargo run --bin rivven-connect -- {{ARGS}}

# Run rivven-operator
operator *ARGS:
    cargo run --bin rivven-operator -- {{ARGS}}

# Start broker with dashboard
run:
    cargo run --bin rivvend -- --dashboard --data-dir ./data

# ============================================================================
# Docker
# ============================================================================

# Build all Docker images
docker-build-all: docker-build docker-build-connect docker-build-operator

# Build broker image (default)
docker-build:
    docker build -t ghcr.io/hupe1980/rivven:latest .

# Build connect image
docker-build-connect:
    docker build --build-arg BINARY=rivven-connect -t ghcr.io/hupe1980/rivven-connect:latest .

# Build operator image
docker-build-operator:
    docker build --build-arg BINARY=rivven-operator -t ghcr.io/hupe1980/rivven-operator:latest .

# Run broker container
docker-run:
    docker run -d --name rivven -p 9092:9092 -p 9094:9094 -v rivven-data:/data \
        ghcr.io/hupe1980/rivven:latest --dashboard --data-dir /data

# Stop and remove container
docker-stop:
    docker stop rivven && docker rm rivven

# ============================================================================
# Security & Quality
# ============================================================================

# Run security audit
audit:
    cargo audit

# Check for outdated dependencies
outdated:
    cargo outdated -R

# Update dependencies
update:
    cargo update

# Run benchmarks
bench:
    cargo bench --workspace

# ============================================================================
# Release
# ============================================================================

# Create a new release tag
release VERSION:
    @echo "Creating release v{{VERSION}}..."
    git tag -a "v{{VERSION}}" -m "Release v{{VERSION}}"
    git push origin "v{{VERSION}}"

# Publish to crates.io (dry run)
publish-dry:
    cargo publish -p rivven-core --dry-run

# ============================================================================
# Cleanup
# ============================================================================

# Clean build artifacts
clean:
    cargo clean

# Clean and rebuild
rebuild: clean build

# Remove data directory
clean-data:
    rm -rf ./data

# ============================================================================
# Database (CDC testing)
# ============================================================================

# Start test PostgreSQL
postgres-start:
    ./scripts/setup-test-postgres.sh

# Stop test PostgreSQL
postgres-stop:
    ./scripts/stop-test-postgres.sh

# ============================================================================
# Demo
# ============================================================================

# Quick demo: broker + produce + consume
demo: build-release
    @echo "Starting Rivven broker..."
    @rm -rf ./demo-data && mkdir -p ./demo-data
    ./target/release/rivvend --data-dir ./demo-data &
    @sleep 2
    @echo "\nCreating topic 'test'..."
    ./target/release/rivvenctl topic create test --partitions 3
    @echo "\nProducing messages..."
    @for i in 1 2 3 4 5; do ./target/release/rivvenctl publish test "Message $$i"; done
    @echo "\nConsuming messages..."
    ./target/release/rivvenctl consume test --from-beginning --max 10
    @echo "\nDemo complete! Broker running on 127.0.0.1:9092"

# Demo with dashboard
demo-dashboard: build-release
    @rm -rf ./demo-data && mkdir -p ./demo-data
    ./target/release/rivvend --dashboard --data-dir ./demo-data

# ============================================================================
# Dashboard (Leptos/WASM)
# ============================================================================

# Build dashboard WASM bundle
dashboard-build:
    cd crates/rivven-dashboard && trunk build --release

# Start dashboard dev server
dashboard-dev:
    cd crates/rivven-dashboard && trunk serve --port 8081

# Build dashboard and embed in server
dashboard-install: dashboard-build
    rm -rf crates/rivven-server/static/*
    cp -r crates/rivven-dashboard/dist/* crates/rivven-server/static/

# Full release with embedded dashboard
release-with-dashboard: dashboard-install build-release

# ============================================================================
# Utilities
# ============================================================================

# Count lines of code
loc:
    @tokei --exclude target --exclude .venv

# Show dependency tree
deps:
    cargo tree --workspace

# Generate dependency graph
deps-graph:
    cargo depgraph --workspace | dot -Tpng > deps.png
    @echo "Generated deps.png"
