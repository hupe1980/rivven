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
#   ./scripts/check.sh --clean       # Clean build artifacts to free disk space
#   ./scripts/check.sh --help       # Show this help message
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
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Timing
START_TIME=$(date +%s)
CHECKS_PASSED=0
CHECKS_FAILED=0
CHECKS_SKIPPED=0
WARNINGS=()

show_help() {
    cat << EOF
${BOLD}Rivven CI Check Script${NC}

${CYAN}Usage:${NC}
  ./scripts/check.sh [OPTIONS]

${CYAN}Options:${NC}
  --quick, -q        Run only fast checks (fmt, clippy, check)
  --test, -t         Run checks + unit tests
  --integration, -i  Run checks + all tests including integration
  --release, -r      Run full pre-release validation
  --clean, -c        Clean build artifacts to free disk space
  --verbose, -v      Show verbose output
  --no-color         Disable colored output
  --help, -h         Show this help message

${CYAN}Examples:${NC}
  ./scripts/check.sh              # Standard checks
  ./scripts/check.sh -q           # Quick pre-commit check
  ./scripts/check.sh -t           # Run with unit tests
  ./scripts/check.sh -r           # Full release validation
  ./scripts/check.sh -c           # Clean target folder

${CYAN}Pre-commit Hook:${NC}
  ln -sf ../../scripts/check.sh .git/hooks/pre-commit

EOF
    exit 0
}

# Parse arguments
QUICK=false
TEST=false
INTEGRATION=false
RELEASE=false
CLEAN=false
VERBOSE=false
NO_COLOR=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick|-q) QUICK=true; shift ;;
        --test|-t) TEST=true; shift ;;
        --integration|-i) INTEGRATION=true; shift ;;
        --release|-r) RELEASE=true; TEST=true; INTEGRATION=true; shift ;;
        --clean|-c) CLEAN=true; shift ;;
        --verbose|-v) VERBOSE=true; shift ;;
        --no-color) NO_COLOR=true; shift ;;
        --help|-h) show_help ;;
        *) echo "Unknown option: $1. Use --help for usage."; exit 1 ;;
    esac
done

# Disable colors if requested or not a TTY
if $NO_COLOR || [[ ! -t 1 ]]; then
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' BOLD='' NC=''
fi

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_ok() {
    echo -e "${GREEN}✓ $1${NC}"
    ((CHECKS_PASSED++)) || true
}

print_err() {
    echo -e "${RED}✗ $1${NC}"
    ((CHECKS_FAILED++)) || true
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_warn() {
    echo -e "${YELLOW}⚠ $1${NC}"
    WARNINGS+=("$1")
}

print_skip() {
    echo -e "${CYAN}⊘ $1 (skipped)${NC}"
    ((CHECKS_SKIPPED++)) || true
}

run_check() {
    local name="$1"
    shift
    print_step "$name"
    local start=$(date +%s)
    if "$@"; then
        local elapsed=$(($(date +%s) - start))
        if $VERBOSE; then
            print_ok "$name passed (${elapsed}s)"
        else
            print_ok "$name passed"
        fi
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
        print_warn "$name failed (non-blocking)"
    fi
}

print_section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  ${BOLD}$1${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# Clean build artifacts
do_clean() {
    print_section "Cleaning Build Artifacts"
    
    local target_dir="$ROOT_DIR/target"
    
    if [[ -d "$target_dir" ]]; then
        local size_before=$(du -sh "$target_dir" 2>/dev/null | cut -f1)
        print_info "Current target folder size: $size_before"
        
        print_step "Running cargo clean"
        if cargo clean; then
            print_ok "Build artifacts cleaned successfully"
            print_info "Freed approximately $size_before of disk space"
        else
            print_err "Failed to clean build artifacts"
            exit 1
        fi
    else
        print_info "Target directory does not exist - nothing to clean"
    fi
    
    # Also clean any stale incremental compilation data
    if [[ -d "$ROOT_DIR/.cargo" ]]; then
        print_step "Cleaning .cargo cache (optional)"
        print_info "Run 'cargo cache --autoclean' for more thorough cleanup (requires cargo-cache)"
    fi
    
    echo ""
    print_ok "Cleanup complete!"
    exit 0
}

# Handle --clean flag early
if $CLEAN; then
    do_clean
fi

# Check for uncommitted changes
check_git_status() {
    print_step "Git Status"
    if [[ -d ".git" ]]; then
        local uncommitted=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
        if [[ "$uncommitted" -gt 0 ]]; then
            print_warn "You have $uncommitted uncommitted changes"
            if $VERBOSE; then
                git status --short
            fi
        else
            print_ok "Working directory clean"
        fi
    else
        print_skip "Git Status (not a git repository)"
    fi
}

# Check if Cargo.lock is up to date
check_cargo_lock() {
    print_step "Cargo.lock Freshness"
    if cargo check --locked --quiet 2>/dev/null; then
        print_ok "Cargo.lock is up to date"
    else
        print_warn "Cargo.lock may need updating (run 'cargo update')"
    fi
}

print_section "Rivven CI Checks"

# Show environment info
print_info "Rust: $(rustc --version 2>/dev/null || echo 'not found')"
print_info "Cargo: $(cargo --version 2>/dev/null || echo 'not found')"

# Git status check (informational)
check_git_status

# Essential checks (always run)
run_check "Formatting" cargo fmt --all -- --check
run_check "Clippy" cargo clippy --all-targets --all-features -- -D warnings
run_check "Compilation" cargo check --all-features --workspace

# Cargo.lock freshness
check_cargo_lock

if $QUICK; then
    echo ""
    ELAPSED=$(($(date +%s) - START_TIME))
    print_ok "Quick checks passed! (${ELAPSED}s)"
    exit 0
fi

# Additional checks
print_step "Documentation"
if cargo doc --no-deps --all-features 2>&1 | grep -E "^error|warning:" | head -20; then
    print_warn "Documentation has warnings"
else
    print_ok "Documentation passed"
fi

# MSRV check (requires the toolchain to be installed)
MSRV="1.89"
if rustup run "$MSRV" cargo --version &> /dev/null; then
    run_check "MSRV ($MSRV)" rustup run "$MSRV" cargo check --all-features --workspace
else
    print_skip "MSRV ($MSRV) - toolchain not installed"
    print_info "Install with: rustup toolchain install $MSRV"
fi

if $TEST; then
    print_section "Running Unit Tests"
    
    run_check "Unit Tests" cargo test --all-features --workspace --lib --bins
fi

if $INTEGRATION; then
    print_section "Running Integration Tests"
    
    # Integration tests require Docker for testcontainers
    if command -v docker &> /dev/null && docker info &> /dev/null; then
        run_check "Integration Tests" cargo test --all-features --workspace -- --test-threads=1
    else
        print_err "Docker not available - integration tests require Docker"
        exit 1
    fi
fi

if $RELEASE; then
    print_section "Pre-Release Validation"
    
    # Check for uncommitted changes (blocking for release)
    if [[ -d ".git" ]]; then
        local_changes=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
        if [[ "$local_changes" -gt 0 ]]; then
            print_warn "Uncommitted changes detected - consider committing before release"
        fi
    fi
    
    # Check current branch
    print_step "Branch Check"
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    if [[ "$CURRENT_BRANCH" == "main" || "$CURRENT_BRANCH" == "master" ]]; then
        print_ok "On release branch: $CURRENT_BRANCH"
    else
        print_warn "Not on main branch (currently on: $CURRENT_BRANCH)"
    fi
    
    # Security audit
    if command -v cargo-audit &> /dev/null; then
        run_check "Security Audit" cargo audit
    else
        print_skip "Security Audit - cargo-audit not installed"
        print_info "Install with: cargo install cargo-audit"
    fi
    
    # Check for outdated dependencies
    if command -v cargo-outdated &> /dev/null; then
        print_step "Dependency Freshness"
        OUTDATED=$(cargo outdated --depth 1 2>/dev/null | grep -c "^[a-z]" || echo "0")
        if [[ "$OUTDATED" -gt 10 ]]; then
            print_warn "$OUTDATED direct dependencies have updates available"
        else
            print_ok "Dependencies reasonably up to date ($OUTDATED with updates)"
        fi
    else
        print_skip "Dependency Freshness - cargo-outdated not installed"
    fi
    
    # Check for duplicate dependencies
    print_step "Duplicate Dependencies"
    DUPES=$(cargo tree --duplicates 2>/dev/null | grep -E "^[a-z]" | wc -l | tr -d ' ')
    if [[ "$DUPES" -gt 20 ]]; then
        print_warn "Found $DUPES duplicate dependency versions (consider deduplication)"
    else
        print_ok "Duplicate dependencies within limits ($DUPES)"
    fi
    
    # Check for unused dependencies
    if command -v cargo-machete &> /dev/null; then
        run_check_optional "Unused Dependencies" cargo machete
    else
        print_skip "Unused Dependencies - cargo-machete not installed"
    fi
    
    # Semver check
    if command -v cargo-semver-checks &> /dev/null; then
        print_step "Semver Compatibility"
        if cargo semver-checks check-release --package rivven-core 2>/dev/null; then
            print_ok "Semver compatibility check passed"
        else
            print_warn "Semver breaking changes detected (may be intentional)"
        fi
    else
        print_skip "Semver Compatibility - cargo-semver-checks not installed"
    fi
    
    # Verify Cargo.toml metadata for publishing
    print_step "Crate Metadata Validation"
    MISSING_METADATA=false
    METADATA_ISSUES=()
    for crate_dir in crates/*/; do
        if [[ -f "$crate_dir/Cargo.toml" ]]; then
            crate_name=$(basename "$crate_dir")
            # Check for required fields
            if ! grep -q "description" "$crate_dir/Cargo.toml"; then
                METADATA_ISSUES+=("$crate_name missing description")
                MISSING_METADATA=true
            fi
            if ! grep -q "license" "$crate_dir/Cargo.toml"; then
                METADATA_ISSUES+=("$crate_name missing license")
                MISSING_METADATA=true
            fi
            if ! grep -q "repository" "$crate_dir/Cargo.toml" && ! grep -q "repository.workspace = true" "$crate_dir/Cargo.toml"; then
                METADATA_ISSUES+=("$crate_name missing repository")
                MISSING_METADATA=true
            fi
            # Check for readme
            if ! grep -q "readme" "$crate_dir/Cargo.toml" && ! [[ -f "$crate_dir/README.md" ]]; then
                METADATA_ISSUES+=("$crate_name missing README")
                MISSING_METADATA=true
            fi
        fi
    done
    if $MISSING_METADATA; then
        for issue in "${METADATA_ISSUES[@]}"; do
            echo -e "${YELLOW}  ⚠ $issue${NC}"
        done
        print_warn "Some crates have incomplete metadata"
    else
        print_ok "Crate Metadata Validation passed"
    fi
    
    # Version consistency check
    print_step "Version Consistency"
    WORKSPACE_VERSION=$(grep -E "^version = " Cargo.toml | head -1 | cut -d'"' -f2)
    print_info "Workspace version: $WORKSPACE_VERSION"
    
    # Validate semver format
    if [[ ! "$WORKSPACE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+)?$ ]]; then
        print_warn "Version '$WORKSPACE_VERSION' may not be valid semver"
    fi
    
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
    else
        print_warn "Version mismatch detected"
    fi
    
    # Check CHANGELOG.md
    print_step "Changelog Validation"
    if [[ -f "CHANGELOG.md" ]]; then
        if grep -q "$WORKSPACE_VERSION" CHANGELOG.md; then
            print_ok "Changelog contains version $WORKSPACE_VERSION"
        else
            print_warn "CHANGELOG.md does not mention version $WORKSPACE_VERSION"
        fi
    else
        print_warn "No CHANGELOG.md found"
    fi
    
    # Check git tag doesn't already exist
    print_step "Git Tag Check"
    TAG_NAME="v$WORKSPACE_VERSION"
    if git rev-parse "$TAG_NAME" &> /dev/null; then
        print_warn "Git tag $TAG_NAME already exists"
    else
        print_ok "Git tag $TAG_NAME is available"
    fi
    
    # Build release binaries
    print_section "Release Builds"
    run_check "Release Build (rivvend)" cargo build --release --package rivvend
    run_check "Release Build (rivven)" cargo build --release --package rivven
    run_check "Release Build (rivven-connect)" cargo build --release --package rivven-connect
    run_check "Release Build (rivven-operator)" cargo build --release --package rivven-operator
    
    # Binary size report
    print_step "Binary Size Report"
    echo "  Binary sizes (release build):"
    TOTAL_SIZE=0
    for bin in rivvend rivven rivven-connect rivven-operator; do
        if [[ -f "target/release/$bin" ]]; then
            SIZE_BYTES=$(stat -f%z "target/release/$bin" 2>/dev/null || stat --printf="%s" "target/release/$bin" 2>/dev/null || echo "0")
            SIZE_HUMAN=$(du -h "target/release/$bin" | cut -f1)
            echo "    $bin: $SIZE_HUMAN"
            TOTAL_SIZE=$((TOTAL_SIZE + SIZE_BYTES))
        fi
    done
    TOTAL_MB=$((TOTAL_SIZE / 1024 / 1024))
    print_info "Total binary size: ~${TOTAL_MB}MB"
    if [[ "$TOTAL_MB" -gt 200 ]]; then
        print_warn "Total binary size exceeds 200MB - consider size optimization"
    fi
    print_ok "Binary Size Report complete"
    
    # Dry-run publish check
    print_step "Publish Dry Run"
    
    PUBLISH_FAILED=false
    for crate in rivven-core rivven-protocol rivven-cluster rivven-cdc rivven-client rivvend; do
        if [[ -f "crates/$crate/Cargo.toml" ]]; then
            if cargo publish --dry-run --package "$crate" --allow-dirty 2>/dev/null; then
                echo -e "    ${GREEN}✓${NC} $crate"
            else
                echo -e "    ${YELLOW}⚠${NC} $crate (may need fixes before publish)"
                PUBLISH_FAILED=true
            fi
        fi
    done
    if $PUBLISH_FAILED; then
        print_warn "Some crates may have publish issues"
    else
        print_ok "Publish dry run passed"
    fi
    
    # Check for TODO/FIXME in code (informational)
    print_section "Code Quality Analysis"
    print_step "Code Quality Notes"
    TODO_COUNT=$(grep -r "TODO\|FIXME\|XXX\|HACK" --include="*.rs" crates/ 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$TODO_COUNT" -gt 0 ]]; then
        print_info "Found $TODO_COUNT TODO/FIXME/XXX/HACK comments in code"
        if $VERBOSE; then
            echo "  Top files with TODOs:"
            grep -r "TODO\|FIXME\|XXX\|HACK" --include="*.rs" -l crates/ 2>/dev/null | head -5 | while read -r f; do
                count=$(grep -c "TODO\|FIXME\|XXX\|HACK" "$f" 2>/dev/null || echo "0")
                echo "    $f: $count"
            done
        fi
    else
        print_ok "No TODO/FIXME comments found"
    fi
    
    # Check for unwrap() calls (informational)
    UNWRAP_COUNT=$(grep -r "\.unwrap()" --include="*.rs" crates/ 2>/dev/null | grep -v "_test\|tests\|#\[test\]" | wc -l | tr -d ' ')
    EXPECT_COUNT=$(grep -r "\.expect(" --include="*.rs" crates/ 2>/dev/null | grep -v "_test\|tests\|#\[test\]" | wc -l | tr -d ' ')
    print_info "Found $UNWRAP_COUNT .unwrap() and $EXPECT_COUNT .expect() calls in non-test code"
    if [[ "$UNWRAP_COUNT" -gt 50 ]]; then
        print_warn "High unwrap() count ($UNWRAP_COUNT) - consider using proper error handling"
    fi
    
    # Check for unsafe code
    UNSAFE_COUNT=$(grep -r "unsafe " --include="*.rs" crates/ 2>/dev/null | grep -v "// *unsafe\|#\[test\]" | wc -l | tr -d ' ')
    if [[ "$UNSAFE_COUNT" -gt 0 ]]; then
        print_info "Found $UNSAFE_COUNT unsafe blocks in codebase"
    else
        print_ok "No unsafe code blocks found"
    fi
    
    # Check for panics
    PANIC_COUNT=$(grep -r "panic!\|unreachable!\|unimplemented!" --include="*.rs" crates/ 2>/dev/null | grep -v "_test\|tests\|#\[test\]" | wc -l | tr -d ' ')
    if [[ "$PANIC_COUNT" -gt 10 ]]; then
        print_warn "Found $PANIC_COUNT panic!/unreachable!/unimplemented! in non-test code"
    else
        print_info "Found $PANIC_COUNT panic macros in non-test code"
    fi
    
    # Feature matrix check (build with different feature combinations)
    print_section "Feature Matrix Validation"
    print_step "Feature Matrix Build"
    run_check "Build without default features" cargo check --no-default-features --package rivven-core
    run_check "Build with TLS feature" cargo check --features tls --package rivven-client
    
    # Test that examples compile
    print_step "Examples Compilation"
    if cargo build --examples --release 2>/dev/null; then
        print_ok "All examples compile successfully"
    else
        print_warn "Some examples failed to compile"
    fi
    print_ok "Feature Matrix Build passed"
    
    # Python bindings check
    if [[ -f "crates/rivven-python/Cargo.toml" ]]; then
        print_section "Language Bindings"
        print_step "Python Bindings"
        if command -v maturin &> /dev/null; then
            if maturin build --manifest-path crates/rivven-python/Cargo.toml --out /tmp/rivven-wheels 2>&1 | tail -5; then
                print_ok "Python bindings build successfully"
                rm -rf /tmp/rivven-wheels
            else
                print_warn "Python bindings build failed"
            fi
        else
            print_skip "Python Bindings - maturin not installed"
        fi
    fi
    
    # Helm chart validation
    if [[ -d "charts/rivven-operator" ]]; then
        print_section "Deployment Artifacts"
        print_step "Helm Chart Validation"
        if command -v helm &> /dev/null; then
            if helm lint charts/rivven-operator 2>/dev/null; then
                print_ok "Helm chart lint passed"
            else
                print_warn "Helm chart has lint warnings"
            fi
            
            # Template render test
            if helm template test charts/rivven-operator >/dev/null 2>&1; then
                print_ok "Helm template renders successfully"
            else
                print_warn "Helm template rendering has issues"
            fi
        else
            print_skip "Helm Chart Validation - helm not installed"
        fi
    fi
    
    # Dockerfile validation
    if [[ -f "Dockerfile" ]]; then
        print_step "Dockerfile Validation"
        if command -v hadolint &> /dev/null; then
            if hadolint Dockerfile 2>/dev/null; then
                print_ok "Dockerfile passes hadolint"
            else
                print_warn "Dockerfile has lint warnings"
            fi
        else
            # Basic syntax check
            if docker build --check . 2>/dev/null || grep -q "^FROM" Dockerfile; then
                print_ok "Dockerfile syntax appears valid"
            fi
        fi
    fi
fi

# Summary
print_section "Summary"

ELAPSED=$(($(date +%s) - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo -e "  ${GREEN}Passed:${NC}  $CHECKS_PASSED"
echo -e "  ${YELLOW}Skipped:${NC} $CHECKS_SKIPPED"
echo -e "  ${RED}Failed:${NC}  $CHECKS_FAILED"
echo -e "  ${BLUE}Time:${NC}    ${MINUTES}m ${SECONDS}s"
echo ""

if [[ ${#WARNINGS[@]} -gt 0 ]]; then
    echo -e "${YELLOW}Warnings:${NC}"
    for warn in "${WARNINGS[@]}"; do
        echo -e "  • $warn"
    done
    echo ""
fi

if [[ "$CHECKS_FAILED" -gt 0 ]]; then
    print_err "Some checks failed!"
    exit 1
fi

print_ok "All checks passed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
