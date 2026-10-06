//! P3 and P16 (SPEC §23.19).
//!
//! P3: "The dependency set passes `deny.toml` unchanged and builds with no CMake and no toolchain
//! beyond the existing C compiler. `ring` is `Apache-2.0 AND ISC`; `rustls-native-certs` is
//! `Apache-2.0 OR ISC OR MIT`."
//!
//! P16: "`unicode-normalization`'s licence is on the allow-list. If it is not, `PATH_ENCODING=utf8`
//! ships with a fixed code-point refusal table instead."
//!
//! The licence half of both is gated by `cargo deny check` (CI's `deny` job). Its licence check
//! covers this crate's D20 set only because `deny.toml` sets `[licenses] include-dev = true`;
//! without it, dev-only crates are skipped (measured at F1a's review). The README records the run
//! and its negative control. What a test can pin from inside the build is the
//! resolved tree: this file reads the workspace `Cargo.lock` and asserts the D20 set resolved
//! WITHOUT any crate that would need CMake or carry a licence `deny.toml` refuses — the regression
//! a careless `default-features` (tokio-rustls's default is aws-lc-rs) would cause.

use std::collections::BTreeSet;

use unicode_normalization::UnicodeNormalization;

/// Crates whose presence would falsify P3: the aws-lc backend (CMake, `OpenSSL` licence), any CMake
/// driver, the `webpki-roots` snapshot (`CDLA-Permissive-2.0`, not allowed), OpenSSL bindings, and
/// `httpdate` — the one crate hyper's `server` feature adds, so its absence shows the build is
/// client-only (which is what makes P14's `is_parse_too_large` caveat real).
const FORBIDDEN: [&str; 7] = [
    "aws-lc-rs",
    "aws-lc-sys",
    "cmake",
    "webpki-roots",
    "openssl",
    "openssl-sys",
    "httpdate",
];

/// SPEC D20's adopted set, plus the TLS crates it necessarily brings (the ones whose licences P3
/// names).
const REQUIRED: [&str; 11] = [
    "hyper",
    "hyper-util",
    "http",
    "http-body",
    "tokio-rustls",
    "rustls",
    "rustls-native-certs",
    "rustls-pki-types",
    "unicode-normalization",
    "ring",
    "rustls-webpki",
];

fn lock_names(lock: &str) -> BTreeSet<String> {
    lock.lines()
        .filter_map(|l| l.strip_prefix("name = \""))
        .filter_map(|l| l.strip_suffix('"'))
        .map(str::to_owned)
        .collect()
}

fn forbidden_in(lock: &str) -> Vec<&'static str> {
    let names = lock_names(lock);
    // MUTATION SITE (M-P3): returning an empty list here must make the control below fail.
    FORBIDDEN
        .iter()
        .copied()
        .filter(|f| names.contains(*f))
        .collect()
}

fn workspace_lock() -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../Cargo.lock");
    std::fs::read_to_string(path).expect("workspace Cargo.lock")
}

/// **P3, the resolved tree.** The D20 set is in the lock, and nothing that needs CMake or carries a
/// refused licence came with it, nor `httpdate` (hyper's `server` feature).
#[test]
fn p3_the_d20_set_resolves_without_cmake_or_refused_licences() {
    let lock = workspace_lock();
    let names = lock_names(&lock);
    for r in REQUIRED {
        assert!(names.contains(r), "P3: {r} is not in the lock");
    }
    assert_eq!(forbidden_in(&lock), Vec::<&str>::new(), "P3 violated");
}

/// **P3, the negative control (table-driven, review F-5).** Every crate whose presence would
/// falsify P3, injected into the real lock ONE AT A TIME, is reported by the detector — so the
/// positive test cannot pass merely because the scan is blind, and dropping a name from
/// `FORBIDDEN` fails this test. The table is a separate literal on purpose: it is the spec's list
/// (§23.13 / D20), not a copy of the detector's.
#[test]
fn p3_control_the_scan_sees_every_forbidden_crate() {
    const MUST_DETECT: [&str; 7] = [
        "aws-lc-rs",
        "aws-lc-sys",
        "cmake",
        "webpki-roots",
        "openssl",
        "openssl-sys",
        "httpdate",
    ];
    let base = workspace_lock();
    assert_eq!(
        forbidden_in(&base),
        Vec::<&str>::new(),
        "the real lock is clean"
    );
    for name in MUST_DETECT {
        let lock = format!("{base}\n[[package]]\nname = \"{name}\"\nversion = \"0.0.1\"\n");
        assert_eq!(
            forbidden_in(&lock),
            vec![name],
            "the scan does not see an injected `{name}`"
        );
    }
}

/// **P16, beyond the licence: the property §23.4.2 step 8 needs.** NFKC folds the dot look-alikes
/// the validator must catch — fullwidth full stop U+FF0E, one-dot leader U+2024, two-dot leader
/// U+2025 — into ASCII dots, so the step-7 dot-segment check run on the NFKC form refuses them.
///
/// Control: NFC (canonical, not compatibility) leaves every one of them unchanged, so the property
/// is NFKC's specifically and the step-8 text is right to name it.
#[test]
fn p16_nfkc_folds_the_dot_lookalikes_and_nfc_does_not() {
    for (raw, want) in [("\u{FF0E}", "."), ("\u{2024}", "."), ("\u{2025}", "..")] {
        let nfkc: String = raw.nfkc().collect();
        assert_eq!(
            nfkc,
            want,
            "NFKC of U+{:04X}",
            raw.chars().next().unwrap() as u32
        );
        let nfc: String = raw.nfc().collect();
        assert_eq!(
            nfc,
            raw,
            "control: NFC must NOT fold U+{:04X}",
            raw.chars().next().unwrap() as u32
        );
    }
    // And a segment made of two fullwidth dots becomes `..` — a dot segment.
    let seg: String = "\u{FF0E}\u{FF0E}".nfkc().collect();
    assert_eq!(seg, "..");
}
