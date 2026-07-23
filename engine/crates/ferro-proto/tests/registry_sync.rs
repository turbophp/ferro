//! Fails if /proto/*.toml was edited without regenerating registry.lock.json.
//! PURE and side-effect-free: parses the TOML in-process via `Registry::from_toml_dir` and compares
//! to the committed lock file. Does NOT run the gen binary and does NOT write to disk.
use ferro_proto::registry::Registry;
use std::path::PathBuf;

#[test]
fn lock_matches_toml() {
    let proto = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let committed = std::fs::read_to_string(proto.join("registry.lock.json")).unwrap();
    let regenerated = Registry::from_toml_dir(&proto).to_lock_json();
    assert_eq!(
        committed, regenerated,
        "registry.lock.json is stale — run `cargo run -p ferro-proto --bin gen-registry-lock` and commit"
    );
}
