//! Build script for rivvend
//!
//! Handles building the embedded WASM dashboard when the `dashboard` feature is enabled.
//! The dashboard source lives in `dashboard/` and is compiled to WASM using trunk.
//!
//! For crates.io packaging:
//! - The pre-built dist/ directory is included in the package
//! - During verification, we just use the pre-built assets
//! - We only run trunk for local development when dist/ doesn't exist
//!
//! NOTE: Cargo.toml is stored as .template to avoid cargo treating dashboard/ as separate package

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    // Only build dashboard when the feature is enabled
    if env::var("CARGO_FEATURE_DASHBOARD").is_ok() {
        build_dashboard();
    }
}

fn build_dashboard() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_path = Path::new(&manifest_dir);
    let dashboard_dir = manifest_path.join("dashboard");
    let dist_dir = dashboard_dir.join("dist");

    // Rerun if any dashboard source files change
    println!("cargo:rerun-if-changed=dashboard/src");
    println!("cargo:rerun-if-changed=dashboard/index.html");
    println!("cargo:rerun-if-changed=dashboard/styles.css");
    println!("cargo:rerun-if-changed=dashboard/Cargo.toml.template");
    println!("cargo:rerun-if-changed=dashboard/Trunk.toml");

    // Check if dist already exists with valid content (e.g., from crates.io package or previous build)
    if dist_dir.join("index.html").exists() {
        // Silent skip - dashboard is ready
        return;
    }

    // Need to build dashboard - check for source files
    let cargo_template = dashboard_dir.join("Cargo.toml.template");
    let cargo_toml = dashboard_dir.join("Cargo.toml");

    // Determine if we have source to build from
    let has_source = cargo_toml.exists() || cargo_template.exists();

    if !has_source {
        panic!(
            "\n\
            ========================================================================\n\
              Dashboard not found!\n\
            \n\
              Neither pre-built dist/ nor source files found.\n\
              Expected: dashboard/dist/index.html or dashboard/Cargo.toml.template\n\
            \n\
              This is an internal error - the dashboard should be\n\
              included in the rivvend package.\n\
            ========================================================================\n"
        );
    }

    // For local development: restore Cargo.toml from template if needed
    if cargo_template.exists() && !cargo_toml.exists() {
        fs::copy(&cargo_template, &cargo_toml).expect("Failed to restore Cargo.toml from template");
        println!("cargo:warning=Restored dashboard/Cargo.toml from template");

        let cargo_lock_template = dashboard_dir.join("Cargo.lock.template");
        let cargo_lock = dashboard_dir.join("Cargo.lock");
        if cargo_lock_template.exists() && !cargo_lock.exists() {
            let _ = fs::copy(&cargo_lock_template, &cargo_lock);
        }
    }

    println!("cargo:warning=Building dashboard with trunk...");

    // Verify trunk is installed
    let trunk_check = Command::new("trunk").arg("--version").output();
    if trunk_check.is_err() || !trunk_check.unwrap().status.success() {
        panic!(
            "\n\
            ========================================================================\n\
              Dashboard build requires 'trunk' but it's not installed.\n\
            \n\
              Install trunk with:\n\
                cargo install trunk\n\
            \n\
              Or disable the dashboard feature:\n\
                cargo build -p rivvend --no-default-features\n\
            ========================================================================\n"
        );
    }

    // Verify wasm32-unknown-unknown target is installed
    let target_check = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output();

    if let Ok(output) = target_check {
        let installed = String::from_utf8_lossy(&output.stdout);
        if !installed.contains("wasm32-unknown-unknown") {
            panic!(
                "\n\
                ========================================================================\n\
                  Dashboard build requires 'wasm32-unknown-unknown' target.\n\
                \n\
                  Install it with:\n\
                    rustup target add wasm32-unknown-unknown\n\
                \n\
                  Or disable the dashboard feature:\n\
                    cargo build -p rivvend --no-default-features\n\
                ========================================================================\n"
            );
        }
    }

    // Run trunk build
    let status = Command::new("trunk")
        .args(["build", "--release"])
        .current_dir(&dashboard_dir)
        .status()
        .expect("Failed to execute trunk build");

    if !status.success() {
        panic!(
            "\n\
            ========================================================================\n\
              Dashboard build failed!\n\
            \n\
              Try building manually:\n\
                cd crates/rivvend/dashboard && trunk build --release\n\
            \n\
              Or disable the dashboard feature:\n\
                cargo build -p rivvend --no-default-features\n\
            ========================================================================\n"
        );
    }

    // Verify build output
    if !dist_dir.join("index.html").exists() {
        panic!(
            "\n\
            ========================================================================\n\
              Dashboard build completed but dist/index.html not found!\n\
            \n\
              This is unexpected - trunk should have created the output.\n\
            ========================================================================\n"
        );
    }

    println!("cargo:warning=Dashboard built successfully!");
}
