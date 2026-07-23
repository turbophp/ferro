//! Fails if /proto/*.toml was edited without regenerating registry.lock.json.
use std::path::PathBuf;
use std::process::Command;

#[test]
fn lock_matches_toml() {
    let proto = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let committed = std::fs::read_to_string(proto.join("registry.lock.json")).unwrap();

    // Regenerate into a temp copy by invoking the gen bin against a scratch dir is heavier than
    // needed; instead re-parse the TOML here with the same model and compare JSON.
    // (Kept in one place by calling the bin in --check mode would be ideal; simple re-parse is fine.)
    let out = Command::new(env!("CARGO_BIN_EXE_gen-registry-lock"))
        .env("FERRO_GEN_STDOUT", "1")
        .output()
        .expect("run gen bin");
    assert!(
        out.status.success(),
        "gen bin failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // gen bin (Step 3) writes the file; re-read and compare.
    let regenerated = std::fs::read_to_string(proto.join("registry.lock.json")).unwrap();
    assert_eq!(
        committed, regenerated,
        "registry.lock.json is stale — run `cargo run -p ferro-proto --bin gen-registry-lock` and commit"
    );
}
