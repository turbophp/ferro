//! TOML registry -> lock model. Used by the gen bin and the sync test only (not the hot path).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    pub tags: BTreeMap<String, u8>,
    pub branches: BTreeMap<String, u8>,
    pub codes: BTreeMap<String, ErrCode>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrCode {
    pub code: u16,
    pub branch: u8,
}

impl Registry {
    /// Produce the canonical lock JSON (stable key order via BTreeMap, 2-space indent).
    pub fn to_lock_json(&self) -> String {
        let mut s = serde_json::to_string_pretty(self).expect("serialize registry");
        s.push('\n');
        s
    }
}
