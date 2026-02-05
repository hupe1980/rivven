//! Build script for rivven-protocol
//!
//! Compiles protobuf files when the `protobuf` feature is enabled.

fn main() {
    #[cfg(feature = "protobuf")]
    {
        let proto_files = &["proto/rivven.proto"];
        let includes = &["proto"];

        // Rerun if proto files change
        for file in proto_files {
            println!("cargo:rerun-if-changed={}", file);
        }

        // Use OUT_DIR for generated files (standard cargo convention)
        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set");

        prost_build::Config::new()
            .out_dir(&out_dir)
            .compile_protos(proto_files, includes)
            .expect("Failed to compile protobuf files");
    }
}
