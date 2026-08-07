//! Fails if /proto/*.toml was edited without regenerating registry.lock.json.
//! PURE and side-effect-free: parses the TOML in-process via `Registry::from_toml_dir` and compares
//! to the committed lock file. Does NOT run the gen binary and does NOT write to disk.
use ferro_proto::registry::Registry;
use std::path::PathBuf;

fn proto_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto")
}

#[test]
fn lock_matches_toml() {
    let proto = proto_dir();
    let committed = std::fs::read_to_string(proto.join("registry.lock.json")).unwrap();
    let regenerated = Registry::from_toml_dir(&proto).to_lock_json();
    assert_eq!(
        committed, regenerated,
        "registry.lock.json is stale — run `cargo run -p ferro-proto --bin gen-registry-lock` and commit"
    );
}

/// The implemented-tag set is REAL (parsed + locked), not dead documentation like `m0_scalar`.
#[test]
fn implemented_tag_set_is_parsed_and_locked() {
    let reg = Registry::from_toml_dir(&proto_dir()); // infallible, takes &Path — no .expect()
    assert!(reg.implemented.iter().any(|t| t == "DECIMAL"));
    assert!(
        !reg.implemented.iter().any(|t| t == "ARRAY"),
        "ARRAY is deferred in S7"
    );
    // Every name must be a real tag, or the vector guard (Task 3) cannot resolve it.
    for name in &reg.implemented {
        assert!(
            reg.tags.contains_key(name),
            "`implemented` names unknown tag {name}"
        );
    }
    // SORTED: a cosmetic reorder of the TOML list must not mint a spurious handshake failure.
    let mut sorted = reg.implemented.clone();
    sorted.sort();
    assert_eq!(
        reg.implemented, sorted,
        "`implemented` must be emitted sorted"
    );
    // And it reaches the lock — which is what the hash is taken over.
    let lock = reg.to_lock_json();
    assert!(
        lock.contains("\"implemented\""),
        "`implemented` must be in registry.lock.json"
    );
    assert!(lock.contains("DECIMAL"));
}

/// The sort must be done by `from_toml_dir`, not merely observed on an already-sorted TOML: a
/// cosmetic reorder of the `implemented` list must produce a BYTE-IDENTICAL lock, or a no-op edit
/// mints a spurious handshake failure. Drives a real reversed-order TOML through the real parser.
#[test]
fn a_cosmetic_reorder_of_implemented_does_not_change_the_lock() {
    let proto = proto_dir();
    let canonical = Registry::from_toml_dir(&proto).to_lock_json();

    let tmp = std::env::temp_dir().join(format!("ferro_reorder_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    for f in ["methods.toml", "errors.toml"] {
        std::fs::copy(proto.join(f), tmp.join(f)).unwrap();
    }
    // Same set, reversed order.
    let types = std::fs::read_to_string(proto.join("types.toml")).unwrap();
    let mut reversed: Vec<String> = Registry::from_toml_dir(&proto).implemented;
    reversed.reverse();
    let list = reversed
        .iter()
        .map(|t| format!("\"{t}\""))
        .collect::<Vec<_>>()
        .join(", ");
    // Split on the real table header, not the comment that merely mentions `[tags]`.
    let at = types
        .find("\n[tags]\n")
        .expect("types.toml has a [tags] table");
    let rest = &types[at + 1..];
    std::fs::write(
        tmp.join("types.toml"),
        format!("implemented = [{list}]\n{rest}"),
    )
    .unwrap();

    let reordered = Registry::from_toml_dir(&tmp).to_lock_json();
    std::fs::remove_dir_all(&tmp).ok();
    assert_eq!(
        canonical, reordered,
        "`implemented` is not being sorted by from_toml_dir — a TOML reorder would move TYPE_REGISTRY_HASH"
    );
}

/// TYPE_REGISTRY_HASH is FNV-1a over the committed lock BYTES (build.rs:118-127), so ANY edit to
/// `implemented` necessarily moves it. That — not a perturbation API — is the skew mechanism.
#[test]
fn type_registry_hash_is_fnv1a_of_the_lock_bytes() {
    let bytes = std::fs::read(proto_dir().join("registry.lock.json")).unwrap();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in &bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    assert_eq!(ferro_proto::consts::TYPE_REGISTRY_HASH, format!("{h:016x}"));
}

/// CROSS-LANGUAGE GUARD (new): nothing offline asserts the PHP constant matches the Rust one today,
/// so a stale `Constants.php` would only surface as an unbootable live handshake.
#[test]
fn php_generated_constant_matches_the_rust_hash() {
    let php = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../php/client/src/Protocol/Generated/Constants.php"),
    )
    .unwrap();
    let needle = "public const TYPE_REGISTRY_HASH = '";
    let start = php
        .find(needle)
        .expect("Constants.php declares TYPE_REGISTRY_HASH")
        + needle.len();
    // Slice to the CLOSING QUOTE, not to a fixed 16 bytes: a fixed width silently truncates a
    // longer literal, so a generator bug that emitted
    // `TYPE_REGISTRY_HASH = '82a29fc665e4baf2deadbeef'` compared its first 16 chars, passed GREEN,
    // and shipped a 24-char hash the handshake rejects at runtime.
    let end = php[start..]
        .find('\'')
        .expect("TYPE_REGISTRY_HASH literal is unterminated in Constants.php");
    let hash = &php[start..start + end];
    assert_eq!(
        hash.len(),
        16,
        "TYPE_REGISTRY_HASH must be exactly 16 hex chars (FNV-1a u64), got {}: {hash:?}",
        hash.len()
    );
    assert_eq!(
        hash,
        ferro_proto::consts::TYPE_REGISTRY_HASH,
        "php/client Constants.php is stale — run `php proto/tools/gen-php.php` and commit"
    );
}
