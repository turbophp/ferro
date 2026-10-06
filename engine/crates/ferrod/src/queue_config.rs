//! Ferro Queue's startup configuration (SPEC §24.3; M7-G1a): load once, log, never fail.
//!
//! `ferro-queue` parses and refuses; it emits nothing. This is where the daemon says what it found:
//!
//! - every refused key, at `error`, naming the store and the key and never the value (each
//!   `ConfigError`'s `Display` is log-safe by construction, and a DSN never reaches the parser — a
//!   store names its pool, and the pool's FAMILY is all the parser is told about it);
//! - every `FERRO_QUEUE_*` variable no declared store owns, at `warn` (a typo in the STORE part);
//! - one summary line naming the enabled and disabled stores.
//!
//! **The queue is configured iff `FERRO_QUEUE_STORES` is set and non-blank** (D14's
//! blank-reads-as-unset rule). Unset, every QUEUE request answers `Unsupported`.

use std::sync::Arc;

use ferro_queue::PoolFamily;
use ferro_queue::config::{QueueConfig, STORES_VAR, StoreEntry};

use crate::config::{Config, PoolKind};

/// `ferrod`'s pool family, as the queue crate names it.
pub fn pool_family(kind: PoolKind) -> PoolFamily {
    match kind {
        PoolKind::Postgres => PoolFamily::Postgres,
        PoolKind::Mysql => PoolFamily::Mysql,
        PoolKind::Sqlite => PoolFamily::Sqlite,
    }
}

/// Read `ferrod`'s environment and log what it holds (see the module docs).
pub fn load_and_log(config: &Config) -> Option<Arc<QueueConfig>> {
    // `var_os`, not `var`: a NON-UTF-8 value is configured-and-refused (the parser reports it), never
    // silently read as "unset" — the `http_config` rule.
    let configured =
        std::env::var_os(STORES_VAR).is_some_and(|v| !v.to_string_lossy().trim().is_empty());
    if !configured {
        return None;
    }
    let cfg = load(std::env::vars_os(), config);
    log(&cfg);
    Some(Arc::new(cfg))
}

/// The parsing half, against `config`'s pools (the test seam).
pub fn load(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
    config: &Config,
) -> QueueConfig {
    let family = |name: &str| {
        config
            .pools
            .iter()
            .find(|p| p.name == name)
            .map(|p| pool_family(p.kind))
    };
    QueueConfig::load(vars, &family)
}

/// The logging half, on an already-loaded configuration.
pub fn log(cfg: &QueueConfig) {
    for e in cfg.errors() {
        tracing::error!(error = %e, "ferrod: Ferro Queue configuration refused");
    }
    for k in cfg.orphan_keys() {
        tracing::warn!(
            variable = %k,
            "ferrod: a FERRO_QUEUE_* variable names no declared store and was ignored"
        );
    }
    let mut enabled = Vec::new();
    let mut disabled = Vec::new();
    for (name, entry) in cfg.entries() {
        match entry {
            StoreEntry::Enabled(_) => enabled.push(name),
            StoreEntry::Disabled(_) => disabled.push(name),
        }
    }
    tracing::info!(?enabled, ?disabled, "ferrod: Ferro Queue configured");
}
