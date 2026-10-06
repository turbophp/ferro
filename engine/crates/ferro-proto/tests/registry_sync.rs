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

/// The `[http.causes]` registry IS SPEC §23.5.6's table — parsed out of the spec file, not copied
/// into this test (the §13 pin-cause precedent: a vocabulary kept by hand in two places rots in one
/// of them). Every backticked token in the table's Tokens column, across every group, must be a
/// registry token, and every registry token must appear in the table. A token the spec names in two
/// groups (`deadline`, `timeout`, `cancelled`) is one registry entry.
#[test]
fn http_causes_are_exactly_the_spec_table() {
    use std::collections::BTreeSet;

    let spec = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../docs/spec/23-http.md"),
    )
    .unwrap();
    let start = spec
        .find("#### 23.5.6 The HTTP cause vocabulary")
        .expect("§23.5.6 heading");
    let section = &spec[start..];
    let table_at = section
        .find("| Group | Tokens |")
        .expect("§23.5.6 has its Group | Tokens table");
    let mut from_spec = BTreeSet::new();
    let mut rows = 0;
    for line in section[table_at..].lines().skip(2) {
        if !line.starts_with('|') {
            break; // the table ends at its first non-row line
        }
        rows += 1;
        let tokens_col = line.rsplit('|').nth(1).expect("a Tokens column");
        for (i, part) in tokens_col.split('`').enumerate() {
            if i % 2 == 1 {
                from_spec.insert(part.to_string());
            }
        }
    }
    assert!(rows >= 7, "parsed only {rows} rows of §23.5.6's table");

    let from_registry: BTreeSet<String> = ferro_proto::consts::http_cause::ALL
        .iter()
        .map(|t| (*t).to_string())
        .collect();
    assert_eq!(
        from_registry.len(),
        ferro_proto::consts::http_cause::ALL.len(),
        "a token appears twice in http_cause::ALL"
    );
    let missing: Vec<_> = from_spec.difference(&from_registry).collect();
    let extra: Vec<_> = from_registry.difference(&from_spec).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "[http.causes] drifted from SPEC §23.5.6 — in the spec, not the registry: {missing:?}; \
         in the registry, not the spec: {extra:?}"
    );
    // Spot-check the generated NAME → token mapping, and F3's seven policy groups (F4 maps
    // `ferro_http::validate::PolicyCause` onto exactly these constants).
    use ferro_proto::consts::http_cause as c;
    assert_eq!(c::UNSENT_WRITE, "unsent_write");
    assert_eq!(c::INFORMATIONAL_101, "informational_101");
    for (konst, token) in [
        (c::FORBIDDEN_UPSTREAM, "forbidden_upstream"),
        (c::FORBIDDEN_ORIGIN, "forbidden_origin"),
        (c::FORBIDDEN_TARGET, "forbidden_target"),
        (c::FORBIDDEN_METHOD, "forbidden_method"),
        (c::FORBIDDEN_HEADER, "forbidden_header"),
        (c::FORBIDDEN_BODY, "forbidden_body"),
        (c::FORBIDDEN_ADDRESS, "forbidden_address"),
    ] {
        assert_eq!(konst, token);
    }
}

/// The shape rule refuses each way a cause could acquire a second spelling, and accepts the shipped
/// table (which `from_toml_dir` already applied, or the lock test above could not have parsed it).
#[test]
fn the_http_cause_shape_rule_refuses_every_bad_entry() {
    use ferro_proto::registry::check_http_causes;
    use std::collections::BTreeMap;

    let one = |k: &str, v: &str| BTreeMap::from([(k.to_string(), v.to_string())]);
    assert!(check_http_causes(&one("EOF_EMPTY", "eof_empty")).is_ok());
    for (k, v, why) in [
        ("EOFEMPTY", "eof_empty", "key is not the token upper-cased"),
        ("EOF_EMPTY", "EOF_EMPTY", "token is not lowercase"),
        (
            "EOF-EMPTY",
            "eof-empty",
            "a `-` is not in the token alphabet",
        ),
        ("", "", "an empty token"),
        (
            "EOF EMPTY",
            "eof empty",
            "a space is not in the token alphabet",
        ),
    ] {
        assert!(check_http_causes(&one(k, v)).is_err(), "accepted: {why}");
    }
    let reg = Registry::from_toml_dir(&proto_dir());
    assert!(check_http_causes(&reg.http.causes).is_ok());
    assert_eq!(
        reg.http.causes.len(),
        45,
        "§23.5.6 names 45 distinct tokens"
    );
}

/// A misspelled table name must fail the PARSE rather than silently drop the vocabulary from the
/// lock (serde ignores unknown TOP-level keys, which is how `m0_scalar` once went dead).
#[test]
fn a_misspelled_http_causes_table_fails_the_parse() {
    let proto = proto_dir();
    let tmp = std::env::temp_dir().join(format!("ferro_http_causes_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    for f in ["methods.toml", "types.toml"] {
        std::fs::copy(proto.join(f), tmp.join(f)).unwrap();
    }
    let errors = std::fs::read_to_string(proto.join("errors.toml")).unwrap();
    assert!(errors.contains("\n[http.causes]\n"));
    std::fs::write(
        tmp.join("errors.toml"),
        errors.replace("\n[http.causes]\n", "\n[http.cause]\n"),
    )
    .unwrap();
    let res = std::panic::catch_unwind(|| Registry::from_toml_dir(&tmp));
    std::fs::remove_dir_all(&tmp).ok();
    assert!(res.is_err(), "a `[http.cause]` table must not parse");
}

/// The Rust half of the shape-rule AGREEMENT test: every case in the shared fixture
/// `proto/tools/http-causes-shape-cases.json` gets the verdict the fixture states. `build.rs` and
/// `Registry::from_toml_dir` share this one function (`include!`), and `php/client`'s
/// `HttpCausesShapeRuleTest` runs `gen-php.php` over the SAME cases — so the three readers of
/// `[http.causes]` cannot disagree about a token.
#[test]
fn the_shared_shape_rule_fixture_gets_its_stated_verdicts() {
    use ferro_proto::registry::check_http_causes;
    use std::collections::BTreeMap;

    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(proto_dir().join("tools/http-causes-shape-cases.json")).unwrap(),
    )
    .unwrap();
    let cases = fixture["cases"].as_array().expect("cases");
    assert!(cases.len() >= 10, "the fixture lost its cases");
    let (mut valid, mut invalid) = (0, 0);
    for case in cases {
        let why = case["why"].as_str().unwrap();
        let causes: BTreeMap<String, String> = case["causes"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
            .collect();
        let want = case["valid"].as_bool().unwrap();
        assert_eq!(
            check_http_causes(&causes).is_ok(),
            want,
            "{why}: {causes:?}"
        );
        if want { valid += 1 } else { invalid += 1 }
    }
    assert!(
        valid >= 1 && invalid >= 1,
        "the fixture must exercise both verdicts"
    );
}
