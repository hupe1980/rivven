use std::env;
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
    let dashboard_dir = Path::new(&manifest_dir).join("../rivven-dashboard");
    let dist_dir = dashboard_dir.join("dist");

    // Check if we need to rebuild
    // Rerun if any dashboard source files change
    println!("cargo:rerun-if-changed=../rivven-dashboard/src");
    println!("cargo:rerun-if-changed=../rivven-dashboard/index.html");
    println!("cargo:rerun-if-changed=../rivven-dashboard/styles.css");
    println!("cargo:rerun-if-changed=../rivven-dashboard/Cargo.toml");
    println!("cargo:rerun-if-changed=../rivven-dashboard/Trunk.toml");

    // Check if dist directory exists and has content
    let needs_build = if dist_dir.exists() {
        // Check if index.html exists in dist
        !dist_dir.join("index.html").exists()
    } else {
        true
    };

    if needs_build {
        println!("cargo:warning=Building dashboard with trunk...");

        // Check if trunk is installed
        let trunk_check = Command::new("trunk").arg("--version").output();

        if trunk_check.is_err() || !trunk_check.unwrap().status.success() {
            panic!(
                "\n\
                ╔══════════════════════════════════════════════════════════════════╗\n\
                ║  Dashboard build requires 'trunk' but it's not installed.        ║\n\
                ║                                                                  ║\n\
                ║  Install trunk with:                                             ║\n\
                ║    cargo install trunk                                           ║\n\
                ║                                                                  ║\n\
                ║  Or disable the dashboard feature:                               ║\n\
                ║    cargo build -p rivvend --no-default-features                  ║\n\
                ╚══════════════════════════════════════════════════════════════════╝\n"
            );
        }

        // Check if wasm32-unknown-unknown target is installed
        let target_check = Command::new("rustup")
            .args(["target", "list", "--installed"])
            .output();

        if let Ok(output) = target_check {
            let installed = String::from_utf8_lossy(&output.stdout);
            if !installed.contains("wasm32-unknown-unknown") {
                panic!(
                    "\n\
                    ╔══════════════════════════════════════════════════════════════════╗\n\
                    ║  Dashboard build requires 'wasm32-unknown-unknown' target.       ║\n\
                    ║                                                                  ║\n\
                    ║  Install it with:                                                ║\n\
                    ║    rustup target add wasm32-unknown-unknown                      ║\n\
                    ║                                                                  ║\n\
                    ║  Or disable the dashboard feature:                               ║\n\
                    ║    cargo build -p rivvend --no-default-features                  ║\n\
                    ╚══════════════════════════════════════════════════════════════════╝\n"
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
                ╔══════════════════════════════════════════════════════════════════╗\n\
                ║  Dashboard build failed!                                         ║\n\
                ║                                                                  ║\n\
                ║  Try building manually:                                          ║\n\
                ║    cd crates/rivven-dashboard && trunk build --release           ║\n\
                ║                                                                  ║\n\
                ║  Or disable the dashboard feature:                               ║\n\
                ║    cargo build -p rivvend --no-default-features                  ║\n\
                ╚══════════════════════════════════════════════════════════════════╝\n"
            );
        }

        println!("cargo:warning=Dashboard built successfully!");
    }
}
