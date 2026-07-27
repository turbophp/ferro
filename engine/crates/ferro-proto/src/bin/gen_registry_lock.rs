//! Parse /proto/*.toml -> /proto/registry.lock.json. Run after editing any TOML.
use ferro_proto::registry::Registry;
use std::path::PathBuf;

fn proto_dir() -> PathBuf {
    // bin runs from the crate dir under `cargo run`; repo root is three up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto")
}

fn main() {
    let dir = proto_dir();
    let reg = Registry::from_toml_dir(&dir);
    std::fs::write(dir.join("registry.lock.json"), reg.to_lock_json()).unwrap();
    eprintln!("wrote {}", dir.join("registry.lock.json").display());
}
