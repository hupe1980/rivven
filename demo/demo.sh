#!/usr/bin/env bash
# Rivven Demo Script
#
# Usage:
#   ./demo.sh broker    # Start broker with dashboard
#   ./demo.sh connect   # Run datagen → stdout pipeline
#   ./demo.sh clean     # Clean up data directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
CONFIG_FILE="${SCRIPT_DIR}/connect.yaml"

case "${1:-help}" in
  broker)
    echo "🚀 Starting Rivven broker with dashboard..."
    echo "   Dashboard: http://localhost:9094/"
    echo "   Broker: localhost:9092"
    echo ""
    mkdir -p "${DATA_DIR}"
    
    # Use pre-built binary if available, otherwise build
    BINARY="${PROJECT_ROOT}/target/release/rivvend"
    if [[ ! -x "$BINARY" ]]; then
      echo "Building rivven-server..."
      cd "${PROJECT_ROOT}"
      cargo build -p rivven-server --release --features dashboard
    fi
    
    "$BINARY" \
      --data-dir "${DATA_DIR}" \
      --bind 0.0.0.0:9092
    ;;

  connect)
    echo "🔌 Starting Rivven Connect pipeline..."
    echo "   Config: ${CONFIG_FILE}"
    echo "   Source: datagen (2 events/sec, max 50)"
    echo "   Sink: stdout (pretty format)"
    echo ""
    echo "Press Ctrl+C to stop"
    echo ""
    
    # Use pre-built binary if available, otherwise build
    BINARY="${PROJECT_ROOT}/target/release/rivven-connect"
    if [[ ! -x "$BINARY" ]]; then
      echo "Building rivven-connect..."
      cd "${PROJECT_ROOT}"
      cargo build -p rivven-connect --release
    fi
    
    "$BINARY" --config "${CONFIG_FILE}" run
    ;;

  clean)
    echo "🧹 Cleaning demo data..."
    rm -rf "${DATA_DIR}"
    echo "✅ Done"
    ;;

  *)
    cat <<EOF
Rivven Demo

Usage:
  ./demo.sh broker    # Start broker with dashboard (Terminal 1)
  ./demo.sh connect   # Run datagen → stdout pipeline (Terminal 2)
  ./demo.sh clean     # Clean up data directory

Quick Start:
  # Terminal 1
  cd demo && ./demo.sh broker

  # Terminal 2 (wait for broker to start)
  cd demo && ./demo.sh connect

  # Browser
  open http://localhost:9094

What to expect:
  - Broker starts on port 9092 with dashboard on 9094
  - Connect generates 2 events/sec (max 50 total)
  - Events are printed to console with pretty formatting
  - Dashboard shows topics, consumer groups, and lag
EOF
    ;;
esac
