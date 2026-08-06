//! TOML registry -> lock model. Used by the gen bin and the sync test only (not the hot path).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

// registry-shape change must update BOTH this struct and gen-php.php.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Registry {
    pub protocol_version: u8,
    pub magic: u8,
    pub max_frame_payload: u32,
    pub default_credit_frames: u32,
    pub default_credit_bytes: u32,
    pub flags: BTreeMap<String, u16>,
    pub services: BTreeMap<String, u16>,
    pub methods: BTreeMap<String, BTreeMap<String, u16>>,
    pub features: BTreeMap<String, BTreeMap<String, u16>>,
    pub outcome: BTreeMap<String, u8>,
    /// The tags implemented end-to-end, SORTED. Part of the hashed lock: changing this set moves
    /// `TYPE_REGISTRY_HASH`, so an engine/client pair with different type coverage fails fast at
    /// the handshake instead of throwing mid-query on the first row of a new type (M1-S7).
    pub implemented: Vec<String>,
    pub tags: BTreeMap<String, u8>,
    pub branches: BTreeMap<String, u8>,
    pub codes: BTreeMap<String, ErrCode>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrCode {
    pub code: u16,
    pub branch: u8,
}

// Deserialize shapes for the three TOML files. serde ignores unknown keys, so a TOML key absent
// from these structs never reaches the lock (which is exactly how the old `m0_scalar` key became
// dead documentation — M1-S7 replaced it with the real, parsed `implemented` list below).
#[derive(Deserialize)]
struct MethodsToml {
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
}
#[derive(Deserialize)]
struct TypesToml {
    implemented: Vec<String>,
    tags: BTreeMap<String, u8>,
}
#[derive(Deserialize)]
struct ErrorsToml {
    branches: BTreeMap<String, u8>,
    codes: BTreeMap<String, ErrCode>,
}

impl Registry {
    /// Parse the three `/proto/*.toml` files in-process into a `Registry`. Shared by the gen bin
    /// (which serializes the result) and the sync test (which compares it) so both parse identically.
    pub fn from_toml_dir(dir: &Path) -> Registry {
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
        let m: MethodsToml = toml::from_str(&read("methods.toml")).unwrap();
        let t: TypesToml = toml::from_str(&read("types.toml")).unwrap();
        let e: ErrorsToml = toml::from_str(&read("errors.toml")).unwrap();
        // SORTED on the way in: the lock (and therefore the hash) must be stable against a
        // cosmetic reorder of the TOML list, or a no-op edit mints a spurious handshake failure.
        let mut implemented = t.implemented;
        implemented.sort();
        Registry {
            protocol_version: m.protocol_version,
            magic: m.magic,
            max_frame_payload: m.max_frame_payload,
            default_credit_frames: m.default_credit_frames,
            default_credit_bytes: m.default_credit_bytes,
            flags: m.flags,
            services: m.services,
            methods: m.methods,
            features: m.features,
            outcome: m.outcome,
            implemented,
            tags: t.tags,
            branches: e.branches,
            codes: e.codes,
        }
    }

    /// Produce the canonical lock JSON (stable key order via BTreeMap, 2-space indent).
    pub fn to_lock_json(&self) -> String {
        let mut s = serde_json::to_string_pretty(self).expect("serialize registry");
        s.push('\n');
        s
    }
}
