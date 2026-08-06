use serde::Deserialize;
use std::collections::BTreeMap;
use std::{env, fmt::Write as _, fs, path::PathBuf};

#[derive(Deserialize)]
struct ErrCode {
    code: u16,
    branch: u8,
}
// registry-shape change must update BOTH this struct and gen-php.php.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    protocol_version: u8,
    magic: u8,
    max_frame_payload: u32,
    default_credit_frames: u32,
    default_credit_bytes: u32,
    flags: BTreeMap<String, u16>,
    services: BTreeMap<String, u16>,
    methods: BTreeMap<String, BTreeMap<String, u16>>,
    features: BTreeMap<String, BTreeMap<String, u16>>,
    outcome: BTreeMap<String, u8>,
    // Not emitted as a constant; declared because `deny_unknown_fields` would otherwise reject the
    // lock and panic the build. It still feeds TYPE_REGISTRY_HASH via the raw lock bytes (M1-S7).
    #[allow(dead_code)]
    implemented: Vec<String>,
    tags: BTreeMap<String, u8>,
    branches: BTreeMap<String, u8>,
    codes: BTreeMap<String, ErrCode>,
}

fn lock_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/registry.lock.json")
}

fn main() {
    let lock = lock_path();
    println!("cargo:rerun-if-changed={}", lock.display());
    let lock_bytes = fs::read(&lock).unwrap();
    let reg: Registry = serde_json::from_slice(&lock_bytes).unwrap();

    let mut o = String::new();
    writeln!(
        o,
        "// @generated from /proto/registry.lock.json — do not edit."
    )
    .unwrap();
    writeln!(
        o,
        "pub const PROTOCOL_VERSION: u8 = {};",
        reg.protocol_version
    )
    .unwrap();
    writeln!(o, "pub const MAGIC: u8 = 0x{:02X};", reg.magic).unwrap();
    writeln!(
        o,
        "pub const MAX_FRAME_PAYLOAD: u32 = {};",
        reg.max_frame_payload
    )
    .unwrap();
    writeln!(
        o,
        "pub const DEFAULT_CREDIT_FRAMES: u32 = {};",
        reg.default_credit_frames
    )
    .unwrap();
    writeln!(
        o,
        "pub const DEFAULT_CREDIT_BYTES: u32 = {};",
        reg.default_credit_bytes
    )
    .unwrap();

    emit_mod_u16(&mut o, "flags", &reg.flags);
    emit_mod_u16(&mut o, "service", &reg.services);
    for (svc, m) in &reg.methods {
        emit_mod_u16(&mut o, &format!("method_{svc}"), m);
    }
    for (side, f) in &reg.features {
        emit_mod_u16(&mut o, &format!("feature_{side}"), f);
    }
    emit_mod_u8(&mut o, "outcome", &reg.outcome);
    emit_mod_u8(&mut o, "tag", &reg.tags);
    emit_mod_u8(&mut o, "branch", &reg.branches);

    writeln!(o, "pub mod errc {{").unwrap();
    for (name, ec) in &reg.codes {
        writeln!(
            o,
            "    pub const {}: u16 = 0x{:04X};",
            screaming(name),
            ec.code
        )
        .unwrap();
        writeln!(
            o,
            "    pub const {}_BRANCH: u8 = {};",
            screaming(name),
            ec.branch
        )
        .unwrap();
    }
    writeln!(o, "}}").unwrap();

    // A stable hex fingerprint of the committed registry.lock.json bytes. Sent in HELLO/HELLO_ACK
    // and hard-checked by ferrod's handshake (SPEC §5): a mismatch means the client and daemon
    // were built against different protocol registries, which is a session-fatal `Unsupported`
    // condition, not something to paper over. Deliberately hashes the lock file's bytes (not the
    // parsed `Registry`) so any byte-level drift — including formatting-only changes the parser
    // would ignore — is caught. PHP-side parity is a separate slice; this only defines the Rust
    // constant.
    let hash = fnv1a_hex(&lock_bytes);
    writeln!(o, "pub const TYPE_REGISTRY_HASH: &str = \"{hash}\";").unwrap();

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("consts.rs");
    fs::write(out, o).unwrap();
}

/// Inline FNV-1a (64-bit) over raw bytes, rendered as lowercase hex. No new dependency: this is
/// a fingerprint for drift-detection, not a cryptographic hash.
fn fnv1a_hex(bytes: &[u8]) -> String {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET_BASIS;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:016x}")
}

fn emit_mod_u16(o: &mut String, name: &str, m: &BTreeMap<String, u16>) {
    writeln!(o, "pub mod {name} {{").unwrap();
    for (k, v) in m {
        writeln!(o, "    pub const {}: u16 = {};", screaming(k), v).unwrap();
    }
    writeln!(o, "}}").unwrap();
}
fn emit_mod_u8(o: &mut String, name: &str, m: &BTreeMap<String, u8>) {
    writeln!(o, "pub mod {name} {{").unwrap();
    for (k, v) in m {
        writeln!(o, "    pub const {}: u8 = {};", screaming(k), v).unwrap();
    }
    writeln!(o, "}}").unwrap();
}
// NOTE: assumes each CamelCase boundary has a lowercase run between caps; consecutive-caps acronyms (e.g. "SQLError") would collapse to "SQLERROR". No current registry identifier triggers this.
fn screaming(s: &str) -> String {
    // ConnectionLost -> CONNECTION_LOST ; HELLO -> HELLO ; MEMFD_RX -> MEMFD_RX
    let mut out = String::new();
    let mut prev_lower = false;
    for c in s.chars() {
        if c.is_uppercase() && prev_lower {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
        prev_lower = c.is_lowercase() || c.is_ascii_digit();
    }
    out
}
