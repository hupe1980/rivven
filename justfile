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

# Serve Jekyll docs locally (without baseurl)
docs-serve:
    cd docs && bundle install && bundle exec jekyll serve --baseurl ""

# ============================================================================
# Running
# ============================================================================

# Run the server
server *ARGS:
    cargo run --bin rivven-server -- {{ARGS}}

# Run the server in release mode
server-release *ARGS:
    cargo run --release --bin rivven-server -- {{ARGS}}

# Run the CLI
cli *ARGS:
    cargo run --bin rivven -- {{ARGS}}

# Run the CLI in release mode
cli-release *ARGS:
    cargo run --release --bin rivven -- {{ARGS}}

# Start server with default data directory
run:
    cargo run --bin rivven-server -- --data-dir ./data

# ============================================================================
# Docker
# ============================================================================

# Build Docker image
docker-build:
    docker build -t rivven:latest .

# Run Docker container
docker-run:
    docker run -d --name rivven -p 9292:9292 -v rivven-data:/data rivven:latest

# Stop and remove Docker container
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
# Database (for CDC testing)
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

# Run the full demo (server + producer + consumer)
demo:
    ./scripts/demo.sh

# Quick demo: start server, create topic, produce, consume
demo-quick: build-release
    @echo "Starting Rivven server..."
    @rm -rf ./demo-data && mkdir -p ./demo-data
    ./target/release/rivven-server --data-dir ./demo-data &
    @sleep 2
    @echo "\nCreating topic 'test'..."
    ./target/release/rivven topic create test --partitions 3 --server 127.0.0.1:9092
    @echo "\nProducing messages..."
    @for i in 1 2 3 4 5; do ./target/release/rivven produce test "Message $$i" --server 127.0.0.1:9092; done
    @echo "\nConsuming messages..."
    ./target/release/rivven consume test --partition 0 --offset 0 --max 10 --server 127.0.0.1:9092
    @echo "\nDemo complete! Server running on 127.0.0.1:9092. Press Ctrl+C to stop."
    @wait

# Run server with dashboard enabled (requires --features dashboard)
demo-dashboard:
    ./scripts/demo-dashboard.sh

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
