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
    let reg: Registry = serde_json::from_str(&fs::read_to_string(&lock).unwrap()).unwrap();

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

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("consts.rs");
    fs::write(out, o).unwrap();
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
