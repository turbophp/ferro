//! Ferro HTTP's startup configuration (SPEC §23.3.1, §23.3.3; M6-F4a): load once, log, never fail.
//!
//! `ferro-http` (slice F3) parses and refuses; it deliberately emits nothing. This is where the
//! daemon says what it found, at the levels §23.3.1 and §23.3.3 name:
//!
//! - every refused key, at `error`, naming the upstream and the key and never the value (each
//!   `ConfigError`'s `Display` is log-safe by construction);
//! - every `FERRO_UPSTREAM_*` variable no declared upstream owns, at `warn` (a typo in the NAME part
//!   leaves the intended upstream at its defaults — wider, §22.2 (cw)'s stated cost);
//! - §23.3.3's isolation warning, naming each upstream that carries attached headers (never their
//!   values) when PHP and `ferrod` can share a uid;
//! - one summary line naming the enabled and disabled upstreams.
//!
//! **HTTP is configured iff `FERRO_UPSTREAMS` is set and non-blank** (D14's blank-reads-as-unset
//! rule). Unset, the route answers `Unsupported` and `HELLO_ACK` does not advertise `HTTP`.

use std::sync::Arc;

use ferro_http::HttpConfig;
use ferro_http::config::{UPSTREAMS_VAR, UpstreamEntry};

use crate::config::Config;

/// Read `ferrod`'s environment and log what it holds (see the module docs).
pub fn load_and_log(config: &Config) -> Option<Arc<HttpConfig>> {
    let configured =
        std::env::var_os(UPSTREAMS_VAR).is_some_and(|v| !v.to_string_lossy().trim().is_empty());
    if !configured {
        return None;
    }
    let cfg = HttpConfig::from_env();
    log(
        &cfg,
        &config.peer_allow_uids,
        nix::unistd::geteuid().as_raw(),
    );
    Some(Arc::new(cfg))
}

/// The logging half, on an already-loaded configuration.
pub fn log(cfg: &HttpConfig, ferro_allow_uids: &[u32], own_uid: u32) {
    for e in cfg.errors() {
        tracing::error!(error = %e, "ferrod: HTTP configuration refused");
    }
    for k in cfg.orphan_keys() {
        tracing::warn!(
            variable = %k,
            "ferrod: a FERRO_UPSTREAM_* variable names no declared upstream and was ignored"
        );
    }
    let void = cfg.isolation_void(ferro_allow_uids, own_uid);
    if !void.is_empty() {
        tracing::warn!(
            upstreams = ?void,
            "ferrod: credential isolation is VOID for these upstreams: PHP can run as ferrod's uid \
             and read the attached-header files (SPEC §23.3.3)"
        );
    }
    let mut enabled = Vec::new();
    let mut disabled = Vec::new();
    for (name, entry) in cfg.entries() {
        match entry {
            UpstreamEntry::Enabled(_) => enabled.push(name),
            UpstreamEntry::Disabled(_) => disabled.push(name),
        }
    }
    if cfg.service_disabled() {
        tracing::error!(
            "ferrod: Ferro HTTP is DISABLED by a daemon-wide configuration error; every request \
             will be refused"
        );
    } else {
        tracing::info!(?enabled, ?disabled, "ferrod: Ferro HTTP configured");
    }
}
