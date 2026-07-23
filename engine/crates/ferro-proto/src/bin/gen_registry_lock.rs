//! Parse /proto/*.toml -> /proto/registry.lock.json. Run after editing any TOML.
use ferro_proto::registry::{ErrCode, Registry};
use std::collections::BTreeMap;
use std::path::PathBuf;

// Local mirrors of the TOML shape (the src Registry is the *output* shape).
#[derive(serde::Deserialize)]
struct Methods {
    protocol_version: u8,
    magic: u8,
    max_frame_payload: u32,
    default_credit_frames: u32,
    default_credit_bytes: u32,
    flags: BTreeMap<String, u16>,
    services: BTreeMap<String, u16>,
    methods: BTreeMap<String, BTreeMap<String, u16>>,
    features: BTreeMap<String, BTreeMap<String, u16>>,
}
#[derive(serde::Deserialize)]
struct Types {
    tags: BTreeMap<String, u8>,
    #[allow(dead_code)]
    m0_scalar: Vec<String>,
}
#[derive(serde::Deserialize)]
struct Errors {
    branches: BTreeMap<String, u8>,
    codes: BTreeMap<String, ErrCode>,
}

fn proto_dir() -> PathBuf {
    // bin runs from crate dir under `cargo run`; repo root is three up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto")
}

fn main() {
    let dir = proto_dir();
    let m: Methods =
        toml::from_str(&std::fs::read_to_string(dir.join("methods.toml")).unwrap()).unwrap();
    let t: Types =
        toml::from_str(&std::fs::read_to_string(dir.join("types.toml")).unwrap()).unwrap();
    let e: Errors =
        toml::from_str(&std::fs::read_to_string(dir.join("errors.toml")).unwrap()).unwrap();
    let reg = Registry {
        protocol_version: m.protocol_version,
        magic: m.magic,
        max_frame_payload: m.max_frame_payload,
        default_credit_frames: m.default_credit_frames,
        default_credit_bytes: m.default_credit_bytes,
        flags: m.flags,
        services: m.services,
        methods: m.methods,
        features: m.features,
        tags: t.tags,
        branches: e.branches,
        codes: e.codes,
    };
    std::fs::write(dir.join("registry.lock.json"), reg.to_lock_json()).unwrap();
    eprintln!("wrote {}", dir.join("registry.lock.json").display());
}
