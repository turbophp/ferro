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
    /// What an `OOB_FD` frame's passed memfd holds (M3-D3, `/proto/PROTOCOL.md` §1.1).
    pub oob_encoding: BTreeMap<String, u8>,
    /// The tags the canonical WIRE CODEC carries, SORTED — a codec/wire scope, NOT a per-engine
    /// availability claim (a listed tag is one both codecs can move, not one every backend can
    /// produce; the per-engine matrix is SPEC §22.2). Part of the hashed lock: changing this set
    /// moves `TYPE_REGISTRY_HASH`, so an engine/client pair with different type coverage fails
    /// fast at the handshake instead of throwing mid-query on the first row of a new type (M1-S7).
    pub implemented: Vec<String>,
    pub tags: BTreeMap<String, u8>,
    pub branches: BTreeMap<String, u8>,
    pub codes: BTreeMap<String, ErrCode>,
    /// Ferro HTTP's closed vocabularies (M6-F2, SPEC §23.5.6), from `errors.toml`'s `[http.*]`.
    pub http: HttpVocab,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpVocab {
    /// `[http.causes]`: constant NAME → wire TOKEN. On service `HTTP`, `ErrorPayload.detail` is
    /// exactly one of these tokens. Every key is its token upper-cased ([`check_http_causes`]).
    pub causes: BTreeMap<String, String>,
}

/// The one shape rule for `[http.causes]`, applied wherever the table is read (here, `build.rs`,
/// and `gen-php.php`), so a cause cannot acquire two spellings: a token is non-empty lowercase ASCII
/// letters, digits and `_`, its KEY is exactly the token upper-cased, and no token appears twice
/// (the last is implied by the key rule, since keys are unique — it is checked anyway, so the rule
/// does not lean on that implication). Returns the first violation.
pub fn check_http_causes(causes: &BTreeMap<String, String>) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    for (key, token) in causes {
        if token.is_empty()
            || !token
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(format!(
                "[http.causes] {key}: token {token:?} is not lowercase [a-z0-9_]+"
            ));
        }
        if *key != token.to_ascii_uppercase() {
            return Err(format!(
                "[http.causes] {key}: the key must be the token upper-cased ({})",
                token.to_ascii_uppercase()
            ));
        }
        if !seen.insert(token.as_str()) {
            return Err(format!("[http.causes] token {token:?} appears twice"));
        }
    }
    Ok(())
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
    oob_encoding: BTreeMap<String, u8>,
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
    // Unlike the tables above, `deny_unknown_fields` (on `HttpVocab`): a misspelled `[http.cause]`
    // must fail the parse, not vanish from the lock the way an unknown top-level key would.
    http: HttpVocab,
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
        if let Err(why) = check_http_causes(&e.http.causes) {
            panic!("errors.toml: {why}");
        }
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
            oob_encoding: m.oob_encoding,
            implemented,
            tags: t.tags,
            branches: e.branches,
            codes: e.codes,
            http: e.http,
        }
    }

    /// Produce the canonical lock JSON (stable key order via BTreeMap, 2-space indent).
    pub fn to_lock_json(&self) -> String {
        let mut s = serde_json::to_string_pretty(self).expect("serialize registry");
        s.push('\n');
        s
    }
}
