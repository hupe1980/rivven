#!/usr/bin/env bash
# Rivven pre-commit/pre-push checks
# Run this locally before pushing to catch CI failures early
#
# Usage:
#   ./scripts/check.sh              # Run all checks
#   ./scripts/check.sh --quick      # Run only fast checks (fmt, clippy, check)
#   ./scripts/check.sh --test       # Run checks + unit tests
#   ./scripts/check.sh --integration # Run checks + all tests including integration
#   ./scripts/check.sh --release    # Run full pre-release validation
#
# To install as pre-commit hook:
#   ln -sf ../../scripts/check.sh .git/hooks/pre-commit

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
QUICK=false
TEST=false
INTEGRATION=false
RELEASE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick|-q) QUICK=true; shift ;;
        --test|-t) TEST=true; shift ;;
        --integration|-i) INTEGRATION=true; shift ;;
        --release|-r) RELEASE=true; TEST=true; INTEGRATION=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_ok() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_err() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

run_check() {
    local name="$1"
    shift
    print_step "$name"
    if "$@"; then
        print_ok "$name passed"
    else
        print_err "$name failed"
        exit 1
    fi
}

run_check_optional() {
    local name="$1"
    shift
    print_step "$name"
    if "$@"; then
        print_ok "$name passed"
    else
        echo -e "${YELLOW}⚠ $name failed (non-blocking)${NC}"
    fi
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Rivven CI Checks"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Essential checks (always run)
run_check "Formatting" cargo fmt --all -- --check
run_check "Clippy" cargo clippy --all-targets --all-features -- -D warnings
run_check "Compilation" cargo check --all-features --workspace

if $QUICK; then
    echo ""
    print_ok "Quick checks passed!"
    exit 0
fi

# Additional checks
run_check "Documentation" cargo doc --no-deps --all-features 2>&1 | head -50

# MSRV check (requires the toolchain to be installed)
MSRV="1.89"
if rustup run "$MSRV" cargo --version &> /dev/null; then
    run_check "MSRV ($MSRV)" rustup run "$MSRV" cargo check --all-features --workspace
else
    echo -e "${YELLOW}⚠ Rust $MSRV not installed, skipping MSRV check${NC}"
    echo -e "${YELLOW}  Install with: rustup toolchain install $MSRV${NC}"
fi

if $TEST; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Running Unit Tests"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    run_check "Unit Tests" cargo test --all-features --workspace --lib --bins
fi

if $INTEGRATION; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Running Integration Tests"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Integration tests require Docker for testcontainers
    if command -v docker &> /dev/null && docker info &> /dev/null; then
        run_check "Integration Tests" cargo test --all-features --workspace -- --test-threads=1
    else
        print_err "Docker not available - integration tests require Docker"
        exit 1
    fi
fi

if $RELEASE; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Pre-Release Validation"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Security audit
    if command -v cargo-audit &> /dev/null; then
        run_check "Security Audit" cargo audit
    else
        echo -e "${YELLOW}⚠ cargo-audit not installed, skipping security audit${NC}"
        echo -e "${YELLOW}  Install with: cargo install cargo-audit${NC}"
    fi
    
    # Check for unused dependencies
    if command -v cargo-machete &> /dev/null; then
        run_check_optional "Unused Dependencies" cargo machete
    else
        print_info "cargo-machete not installed, skipping unused deps check"
    fi
    
    # Verify Cargo.toml metadata for publishing
    print_step "Crate Metadata Validation"
    MISSING_METADATA=false
    for crate_dir in crates/*/; do
        if [[ -f "$crate_dir/Cargo.toml" ]]; then
            crate_name=$(basename "$crate_dir")
            # Check for required fields
            if ! grep -q "description" "$crate_dir/Cargo.toml"; then
                echo -e "${YELLOW}  ⚠ $crate_name missing description${NC}"
                MISSING_METADATA=true
            fi
            if ! grep -q "license" "$crate_dir/Cargo.toml"; then
                echo -e "${YELLOW}  ⚠ $crate_name missing license${NC}"
                MISSING_METADATA=true
            fi
        fi
    done
    if ! $MISSING_METADATA; then
        print_ok "Crate Metadata Validation passed"
    fi
    
    # Version consistency check
    print_step "Version Consistency"
    WORKSPACE_VERSION=$(grep -E "^version = " Cargo.toml | head -1 | cut -d'"' -f2)
    print_info "Workspace version: $WORKSPACE_VERSION"
    VERSION_MISMATCH=false
    for crate_dir in crates/*/; do
        if [[ -f "$crate_dir/Cargo.toml" ]]; then
            crate_name=$(basename "$crate_dir")
            # Skip dashboard which has its own version
            if [[ "$crate_name" == "rivven-dashboard" ]]; then
                continue
            fi
            # Check if using workspace version
            if grep -q "version.workspace = true" "$crate_dir/Cargo.toml"; then
                continue
            fi
            CRATE_VERSION=$(grep -E "^version = " "$crate_dir/Cargo.toml" | head -1 | cut -d'"' -f2)
            if [[ -n "$CRATE_VERSION" && "$CRATE_VERSION" != "$WORKSPACE_VERSION" ]]; then
                echo -e "${YELLOW}  ⚠ $crate_name has version $CRATE_VERSION (expected $WORKSPACE_VERSION)${NC}"
                VERSION_MISMATCH=true
            fi
        fi
    done
    if ! $VERSION_MISMATCH; then
        print_ok "Version Consistency passed"
    fi
    
    # Build release binaries
    run_check "Release Build (rivvend)" cargo build --release --package rivvend
    run_check "Release Build (rivven)" cargo build --release --package rivven
    run_check "Release Build (rivven-connect)" cargo build --release --package rivven-connect
    run_check "Release Build (rivven-operator)" cargo build --release --package rivven-operator
    
    # Binary size report
    print_step "Binary Size Report"
    echo "  Binary sizes (release build):"
    for bin in rivvend rivven rivven-connect rivven-operator; do
        if [[ -f "target/release/$bin" ]]; then
            SIZE=$(du -h "target/release/$bin" | cut -f1)
            echo "    $bin: $SIZE"
        fi
    done
    print_ok "Binary Size Report complete"
    
    # Check for TODO/FIXME in code (informational)
    print_step "Code Quality Notes"
    TODO_COUNT=$(grep -r "TODO\|FIXME\|XXX\|HACK" --include="*.rs" crates/ 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$TODO_COUNT" -gt 0 ]]; then
        print_info "Found $TODO_COUNT TODO/FIXME/XXX/HACK comments in code"
    else
        print_ok "No TODO/FIXME comments found"
    fi
    
    # Check for unwrap() calls (informational)
    UNWRAP_COUNT=$(grep -r "\.unwrap()" --include="*.rs" crates/ 2>/dev/null | grep -v "_test\|tests\|#\[test\]" | wc -l | tr -d ' ')
    if [[ "$UNWRAP_COUNT" -gt 50 ]]; then
        print_info "Found $UNWRAP_COUNT .unwrap() calls in non-test code (consider reviewing)"
    fi
    
    # Feature matrix check (build with different feature combinations)
    print_step "Feature Matrix Build"
    run_check "Build without default features" cargo check --no-default-features --package rivven-core
    run_check "Build with TLS feature" cargo check --features tls --package rivven-client
    print_ok "Feature Matrix Build passed"
    
    # Python bindings check
    if [[ -f "crates/rivven-python/Cargo.toml" ]]; then
        print_step "Python Bindings"
        if command -v maturin &> /dev/null; then
            run_check "Python Build Check" maturin build --manifest-path crates/rivven-python/Cargo.toml --out /tmp/rivven-wheels 2>&1 | tail -5
            rm -rf /tmp/rivven-wheels
        else
            print_info "maturin not installed, skipping Python build check"
        fi
    fi
    
    # WASM dashboard check
    if [[ -f "crates/rivven-dashboard/Cargo.toml" ]]; then
        print_step "WASM Dashboard Build"
        if command -v trunk &> /dev/null; then
            run_check "Dashboard Build" bash -c "cd crates/rivven-dashboard && trunk build --release 2>&1 | tail -5"
        else
            print_info "trunk not installed, skipping dashboard build check"
        fi
    fi
    
    # Helm chart validation
    if [[ -d "charts/rivven-operator" ]]; then
        print_step "Helm Chart Validation"
        if command -v helm &> /dev/null; then
            run_check "Helm Lint" helm lint charts/rivven-operator
        else
            print_info "helm not installed, skipping chart validation"
        fi
    fi
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
print_ok "All checks passed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
