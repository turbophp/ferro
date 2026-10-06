//! Ferro Queue's store configuration (SPEC §24.3): `FERRO_QUEUE_STORES=<s1>,<s2>` plus
//! `FERRO_QUEUE_<STORE>_<KEY>`. Read once, at start — `ferrod` has no configuration reload in v1.
//!
//! The rules are §24.3's, applied in the shape `ferro-http`'s configuration established (§22.2 (cw)),
//! so an operator meets one discipline across both engines:
//!
//! - **A refusal is fatal for that store only, never the daemon.** A refused store is still listed,
//!   so a request naming it is refused exactly as an unknown store is.
//! - **Every refusal names the store and the key, and never quotes a value** — a value could be a
//!   pool name an operator considers private, and a DSN never reaches this module at all.
//! - **A blank value reads as unset** (D14's `ALLOW_DIR` rule).
//! - **An unknown key disables its store.** A typo is not cosmetic here: `…_LEASE=30` (no `_S`) would
//!   otherwise leave the lease at its default 90 s. Keys are matched exactly, so two stores whose env
//!   names share a prefix (`jobs`, `jobs_high`) still resolve; a variable both could own disables both.
//! - **Any `LIVENESS_RELEASE` key is refused**, not ignored: liveness release is not in v1 (§24.15),
//!   and a key naming it must not let an operator believe it is on.
//!
//! §24.3's refusals, each tested: a `KIND` other than `sql`; a missing or unknown pool; a pool whose
//! family is unsupported (SQLite); a `TABLE`/`DEDUP_TABLE` that is not `[schema.]identifier`
//! ([`crate::ident`]); `LEASE_S < 2`; `MAX_PAYLOAD_BYTES` above `max_frame_payload` minus the reply
//! envelope ([`crate::checks::sql_reserve_envelope`]). G1a adds, and records in §24.3: a zero
//! `POLL_MS`, `WAKER_STMT_TIMEOUT_MS`, `MAX_PAYLOAD_BYTES`, `DEDUP_TTL_S` or `DEDUP_PURGE_MS` (each is
//! a period or a bound for which 0 has no meaning, and `POLL_MS = 0` would spin), and a
//! `LABELLED_QUEUES` entry that is not a valid queue name. The version gate is NOT a load-time check:
//! pools are lazy, so a server version is known only at first use ([`crate::version`]).

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fmt;

use crate::PoolFamily;
use crate::ident::{IdentError, TableName};

pub const STORES_VAR: &str = "FERRO_QUEUE_STORES";
pub const STORE_PREFIX: &str = "FERRO_QUEUE_";

/// Every per-store key of §24.3's table.
pub const STORE_KEYS: &[&str] = &[
    "KIND",
    "POOL",
    "TABLE",
    "LEASE_S",
    "POLL_MS",
    "MAX_WAIT_MS",
    "WAKER_STMT_TIMEOUT_MS",
    "MAX_PAYLOAD_BYTES",
    "LABELLED_QUEUES",
    "DEDUP_TABLE",
    "DEDUP_TTL_S",
    "DEDUP_PURGE_MS",
    "DEPTH_SAMPLE_MS",
];

/// The key §24.15 refuses by name, and every key that starts with it.
pub const LIVENESS_RELEASE_KEY: &str = "LIVENESS_RELEASE";

/// The one v1 store kind (D22 amendment (b)).
pub const KIND_SQL: &str = "sql";
pub const DEFAULT_TABLE: &str = "ferro_jobs";
pub const DEFAULT_LEASE_S: u32 = 90;
pub const MIN_LEASE_S: u32 = 2;
pub const DEFAULT_POLL_MS: u32 = 1_000;
pub const DEFAULT_MAX_WAIT_MS: u32 = 30_000;
pub const DEFAULT_WAKER_STMT_TIMEOUT_MS: u32 = 5_000;
pub const DEFAULT_MAX_PAYLOAD_BYTES: u32 = 4_194_304;
pub const DEFAULT_DEDUP_TTL_S: u32 = 86_400;
pub const DEFAULT_DEDUP_PURGE_MS: u32 = 60_000;
pub const DEFAULT_DEPTH_SAMPLE_MS: u32 = 30_000;

/// The largest `MAX_PAYLOAD_BYTES` the `sql` kind admits: one reserved job of that size, with every
/// other field at its worst case, still fits one frame (§24.3).
pub fn max_payload_bytes_limit() -> u32 {
    ferro_proto::consts::MAX_FRAME_PAYLOAD - crate::checks::sql_reserve_envelope() as u32
}

/// A store's kind. v1 has one; the type exists so a later kind is a variant, not a shape change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    Sql,
}

/// One enabled store.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreConfig {
    pub name: String,
    pub kind: StoreKind,
    pub pool: String,
    /// The pool's family as configured (from its DSN scheme); never `Sqlite` here.
    pub family: PoolFamily,
    pub table: TableName,
    pub lease_s: u32,
    pub poll_ms: u32,
    pub max_wait_ms: u32,
    pub waker_stmt_timeout_ms: u32,
    pub max_payload_bytes: u32,
    pub labelled_queues: Vec<String>,
    pub dedup_table: Option<TableName>,
    pub dedup_ttl_s: u32,
    pub dedup_purge_ms: u32,
    /// `0` = depth sampling off.
    pub depth_sample_ms: u32,
}

/// Why a key was refused. Its `Display` never quotes a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    Unknown,
    Ambiguous,
    NotUtf8,
    NotANumber,
    BelowMinimum(u64),
    AboveMaximum(u64),
    /// `KIND` is not `sql`.
    KindUnsupported,
    /// `POOL` is unset (it is required for the `sql` kind).
    PoolRequired,
    /// `POOL` names no configured pool.
    PoolUnknown,
    /// `POOL` names a SQLite pool (§24.15).
    FamilyUnsupported,
    Identifier(IdentError),
    /// `LABELLED_QUEUES`' entry at this 1-based position is not a valid queue name.
    QueueName(usize),
    /// A `LIVENESS_RELEASE` key (§24.15).
    LivenessRelease,
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reason::Unknown => f.write_str("is not a Ferro Queue store key"),
            Reason::Ambiguous => f.write_str("could belong to more than one store"),
            Reason::NotUtf8 => f.write_str("is not UTF-8"),
            Reason::NotANumber => f.write_str("is not a decimal number"),
            Reason::BelowMinimum(m) => write!(f, "is below its minimum ({m})"),
            Reason::AboveMaximum(m) => write!(f, "is above its maximum ({m})"),
            Reason::KindUnsupported => f.write_str(
                "is not `sql`, the only store kind in v1 (any other kind needs its own SPEC §21 \
                 decision)",
            ),
            Reason::PoolRequired => f.write_str("is required for a `sql` store"),
            Reason::PoolUnknown => f.write_str("names no configured pool"),
            Reason::FamilyUnsupported => {
                f.write_str("names a SQLite pool, which cannot hold a store in v1 (SPEC §24.15)")
            }
            Reason::Identifier(e) => write!(f, "{e}"),
            Reason::QueueName(i) => write!(
                f,
                "entry {i} is not a queue name (1 to 255 characters without U+0000)"
            ),
            Reason::LivenessRelease => f.write_str(
                "names liveness release, which is not in v1 (SPEC §24.15) and is refused rather \
                 than ignored",
            ),
        }
    }
}

/// One configuration error. Its `Display` is safe to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A `FERRO_QUEUE_STORES` entry (1-based position) that is not a valid store name.
    BadName { position: usize },
    /// Store names that map to the same `FERRO_QUEUE_<STORE>_` prefix.
    NameCollision { names: Vec<String> },
    Store {
        store: String,
        key: String,
        reason: Reason,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::BadName { position } => write!(
                f,
                "{STORES_VAR}: entry {position} is not a valid store name (1-64 bytes of \
                 [A-Za-z0-9._-])"
            ),
            ConfigError::NameCollision { names } => write!(
                f,
                "{STORES_VAR}: stores {} share one {STORE_PREFIX}<STORE>_ prefix; all are disabled",
                names.join(", ")
            ),
            ConfigError::Store { store, key, reason } => write!(
                f,
                "queue store {store}: {STORE_PREFIX}{}_{key} {reason}; the store is disabled",
                env_name(store)
            ),
        }
    }
}

/// A declared store: enabled, or disabled with the errors that disabled it.
#[derive(Debug, Clone, PartialEq)]
pub enum StoreEntry {
    Enabled(Box<StoreConfig>),
    Disabled(Vec<ConfigError>),
}

/// The loaded configuration.
#[derive(Debug, Default)]
pub struct QueueConfig {
    stores: BTreeMap<String, StoreEntry>,
    list_errors: Vec<ConfigError>,
    orphan_keys: Vec<String>,
}

impl QueueConfig {
    /// Load from explicit variables (the test seam: `std::env::set_var` is `unsafe` under edition
    /// 2024, which `unsafe_code = "forbid"` refuses). `pool_family` answers for a configured pool
    /// name; `None` means no such pool.
    pub fn load(
        vars: impl IntoIterator<Item = (OsString, OsString)>,
        pool_family: &dyn Fn(&str) -> Option<PoolFamily>,
    ) -> QueueConfig {
        let mut env: HashMap<String, Result<String, ()>> = HashMap::new();
        for (k, v) in vars {
            let Some(k) = k.to_str() else { continue };
            if k.starts_with(STORE_PREFIX) {
                env.insert(k.to_string(), v.into_string().map_err(|_| ()));
            }
        }
        let mut cfg = QueueConfig::default();
        let names_raw = match env.get(STORES_VAR) {
            None => return cfg,
            Some(Err(())) => {
                cfg.list_errors.push(ConfigError::BadName { position: 0 });
                return cfg;
            }
            Some(Ok(v)) => v.clone(),
        };
        // Declared names, grouped by env name.
        let mut by_env: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (i, name) in list(&names_raw).enumerate() {
            if !is_valid_name(name) {
                cfg.list_errors
                    .push(ConfigError::BadName { position: i + 1 });
                continue;
            }
            let group = by_env.entry(env_name(name)).or_default();
            if !group.iter().any(|n| n == name) {
                group.push(name.to_string());
            }
        }
        // Assign every FERRO_QUEUE_<rest> variable to (store env name, key).
        let mut assigned: BTreeMap<String, BTreeMap<String, Result<String, ()>>> = BTreeMap::new();
        let mut key_errors: BTreeMap<String, Vec<(String, Reason)>> = BTreeMap::new();
        let mut vars: Vec<&String> = env.keys().filter(|k| *k != STORES_VAR).collect();
        vars.sort();
        for full in vars {
            let rest = &full[STORE_PREFIX.len()..];
            let readings: Vec<(&String, &str)> = by_env
                .keys()
                .filter_map(|e| {
                    let k = rest.strip_prefix(e.as_str())?.strip_prefix('_')?;
                    is_known_key(k).then_some((e, k))
                })
                .collect();
            match readings.as_slice() {
                [(e, k)] => {
                    assigned
                        .entry((*e).clone())
                        .or_default()
                        .insert((*k).to_string(), env[full].clone());
                }
                [] => {
                    let owner = by_env
                        .keys()
                        .filter(|e| {
                            rest.strip_prefix(e.as_str())
                                .is_some_and(|r| r.starts_with('_'))
                        })
                        .max_by_key(|e| e.len());
                    match owner {
                        Some(e) => key_errors
                            .entry(e.clone())
                            .or_default()
                            .push((safe_key(&rest[e.len() + 1..]), Reason::Unknown)),
                        None => cfg.orphan_keys.push(safe_key(full)),
                    }
                }
                many => {
                    for (e, k) in many {
                        key_errors
                            .entry((*e).clone())
                            .or_default()
                            .push((safe_key(k), Reason::Ambiguous));
                    }
                }
            }
        }
        for (e, names) in by_env {
            if names.len() > 1 {
                let err = ConfigError::NameCollision {
                    names: names.clone(),
                };
                for n in names {
                    cfg.stores
                        .insert(n, StoreEntry::Disabled(vec![err.clone()]));
                }
                continue;
            }
            let name = names.into_iter().next().unwrap_or_default();
            let mut errors: Vec<ConfigError> = key_errors
                .remove(&e)
                .unwrap_or_default()
                .into_iter()
                .map(|(key, reason)| ConfigError::Store {
                    store: name.clone(),
                    key,
                    reason,
                })
                .collect();
            let keys = assigned.remove(&e).unwrap_or_default();
            let entry = match parse_store(&name, &keys, pool_family) {
                Ok(s) if errors.is_empty() => StoreEntry::Enabled(Box::new(s)),
                Ok(_) => StoreEntry::Disabled(errors),
                Err(mut more) => {
                    errors.append(&mut more);
                    StoreEntry::Disabled(errors)
                }
            };
            cfg.stores.insert(name, entry);
        }
        cfg
    }

    /// The enabled store of that name, or `None` — one answer for unknown and disabled.
    pub fn store(&self, name: &str) -> Option<&StoreConfig> {
        match self.stores.get(name)? {
            StoreEntry::Enabled(s) => Some(s),
            StoreEntry::Disabled(_) => None,
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &StoreEntry)> {
        self.stores.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Every error: the list's, then each disabled store's.
    pub fn errors(&self) -> Vec<&ConfigError> {
        let mut out: Vec<&ConfigError> = self.list_errors.iter().collect();
        for entry in self.stores.values() {
            if let StoreEntry::Disabled(errs) = entry {
                out.extend(errs.iter());
            }
        }
        out
    }

    /// `FERRO_QUEUE_*` variables no declared store owns (a typo in the STORE part). Logged at `warn`.
    pub fn orphan_keys(&self) -> &[String] {
        &self.orphan_keys
    }
}

fn is_known_key(k: &str) -> bool {
    STORE_KEYS.contains(&k)
        || k == LIVENESS_RELEASE_KEY
        || k.starts_with(&format!("{LIVENESS_RELEASE_KEY}_"))
}

fn parse_store(
    name: &str,
    keys: &BTreeMap<String, Result<String, ()>>,
    pool_family: &dyn Fn(&str) -> Option<PoolFamily>,
) -> Result<StoreConfig, Vec<ConfigError>> {
    let mut errs: Vec<(String, Reason)> = Vec::new();
    // A blank value reads as unset; a non-UTF-8 one is refused.
    let mut get = |key: &str| -> Option<String> {
        match keys.get(key)? {
            Err(()) => {
                errs.push((key.to_string(), Reason::NotUtf8));
                None
            }
            Ok(v) if v.trim().is_empty() => None,
            Ok(v) => Some(v.trim().to_string()),
        }
    };
    let raw: BTreeMap<&str, Option<String>> = STORE_KEYS.iter().map(|k| (*k, get(k))).collect();
    let liveness: Vec<String> = keys
        .keys()
        .filter(|k| !STORE_KEYS.contains(&k.as_str()))
        .cloned()
        .collect();
    for k in liveness {
        errs.push((k, Reason::LivenessRelease));
    }
    let mut num = |key: &str, default: u32, min: u32, max: u32| -> u32 {
        match &raw[key] {
            None => default,
            Some(v) => match v.parse::<u32>() {
                Err(_) => {
                    errs.push((key.to_string(), Reason::NotANumber));
                    default
                }
                Ok(n) if n < min => {
                    errs.push((key.to_string(), Reason::BelowMinimum(u64::from(min))));
                    default
                }
                Ok(n) if n > max => {
                    errs.push((key.to_string(), Reason::AboveMaximum(u64::from(max))));
                    default
                }
                Ok(n) => n,
            },
        }
    };
    let lease_s = num("LEASE_S", DEFAULT_LEASE_S, MIN_LEASE_S, u32::MAX);
    let poll_ms = num("POLL_MS", DEFAULT_POLL_MS, 1, u32::MAX);
    let max_wait_ms = num("MAX_WAIT_MS", DEFAULT_MAX_WAIT_MS, 0, u32::MAX);
    let waker_stmt_timeout_ms = num(
        "WAKER_STMT_TIMEOUT_MS",
        DEFAULT_WAKER_STMT_TIMEOUT_MS,
        1,
        u32::MAX,
    );
    let max_payload_bytes = num(
        "MAX_PAYLOAD_BYTES",
        DEFAULT_MAX_PAYLOAD_BYTES,
        1,
        max_payload_bytes_limit(),
    );
    let dedup_ttl_s = num("DEDUP_TTL_S", DEFAULT_DEDUP_TTL_S, 1, u32::MAX);
    let dedup_purge_ms = num("DEDUP_PURGE_MS", DEFAULT_DEDUP_PURGE_MS, 1, u32::MAX);
    let depth_sample_ms = num("DEPTH_SAMPLE_MS", DEFAULT_DEPTH_SAMPLE_MS, 0, u32::MAX);

    let kind = match raw["KIND"].as_deref() {
        None | Some(KIND_SQL) => Some(StoreKind::Sql),
        Some(_) => {
            errs.push(("KIND".into(), Reason::KindUnsupported));
            None
        }
    };
    let mut family = None;
    let pool = match raw["POOL"].clone() {
        None => {
            errs.push(("POOL".into(), Reason::PoolRequired));
            String::new()
        }
        Some(p) => {
            match pool_family(&p) {
                None => errs.push(("POOL".into(), Reason::PoolUnknown)),
                Some(PoolFamily::Sqlite) => errs.push(("POOL".into(), Reason::FamilyUnsupported)),
                Some(f) => family = Some(f),
            }
            p
        }
    };
    let mut ident = |key: &str, raw: Option<&str>| -> Option<TableName> {
        let r = raw?;
        match TableName::parse(r) {
            Ok(t) => Some(t),
            Err(e) => {
                errs.push((key.to_string(), Reason::Identifier(e)));
                None
            }
        }
    };
    let table = ident(
        "TABLE",
        Some(raw["TABLE"].as_deref().unwrap_or(DEFAULT_TABLE)),
    );
    let dedup_table = ident("DEDUP_TABLE", raw["DEDUP_TABLE"].as_deref());
    let mut labelled_queues = Vec::new();
    if let Some(list_raw) = raw["LABELLED_QUEUES"].as_deref() {
        for (i, q) in list(list_raw).enumerate() {
            if crate::checks::queue_name(q).is_err() {
                errs.push(("LABELLED_QUEUES".into(), Reason::QueueName(i + 1)));
            } else {
                labelled_queues.push(q.to_string());
            }
        }
    }

    match (errs.is_empty(), kind, family, table) {
        (true, Some(kind), Some(family), Some(table)) => Ok(StoreConfig {
            name: name.to_string(),
            kind,
            pool,
            family,
            table,
            lease_s,
            poll_ms,
            max_wait_ms,
            waker_stmt_timeout_ms,
            max_payload_bytes,
            labelled_queues,
            dedup_table,
            dedup_ttl_s,
            dedup_purge_ms,
            depth_sample_ms,
        }),
        _ => Err(errs
            .into_iter()
            .map(|(key, reason)| ConfigError::Store {
                store: name.to_string(),
                key: safe_key(&key),
                reason,
            })
            .collect()),
    }
}

/// The env-var form of a name: ASCII-uppercased, every other byte `_` — `ferrod`'s pool rule.
pub fn env_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// 1–64 bytes of `[A-Za-z0-9._-]`. Store names reach logs and the `store` metrics label, so they are
/// held to a set that needs no escaping to be safe (the upstream-name rule, §23.3).
pub fn is_valid_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 64
        && n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// A key name as it may be logged: `[A-Z0-9_]` only, otherwise a placeholder.
fn safe_key(k: &str) -> String {
    if !k.is_empty()
        && k.len() <= 128
        && k.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        k.to_string()
    } else {
        "<key with unsafe characters>".to_string()
    }
}

fn list(raw: &str) -> impl Iterator<Item = &str> {
    raw.split(',').map(str::trim).filter(|s| !s.is_empty())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A valid store, for the other modules' tests.
    pub(crate) fn store() -> StoreConfig {
        let cfg = load(&[
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "main"),
        ]);
        cfg.store("jobs").cloned().expect("the minimal store loads")
    }

    fn pools(name: &str) -> Option<PoolFamily> {
        match name {
            "main" => Some(PoolFamily::Postgres),
            "my" => Some(PoolFamily::Mysql),
            "lite" => Some(PoolFamily::Sqlite),
            _ => None,
        }
    }

    fn load(vars: &[(&str, &str)]) -> QueueConfig {
        QueueConfig::load(
            vars.iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &pools,
        )
    }

    fn errors_of(cfg: &QueueConfig, store: &str) -> Vec<(String, Reason)> {
        match cfg.stores.get(store) {
            Some(StoreEntry::Disabled(errs)) => errs
                .iter()
                .map(|e| match e {
                    ConfigError::Store { key, reason, .. } => (key.clone(), reason.clone()),
                    other => (format!("{other}"), Reason::Unknown),
                })
                .collect(),
            other => panic!("{store} is not disabled: {other:?}"),
        }
    }

    #[test]
    fn the_minimal_store_takes_every_default() {
        let s = store();
        assert_eq!(s.kind, StoreKind::Sql);
        assert_eq!(s.pool, "main");
        assert_eq!(s.family, PoolFamily::Postgres);
        assert_eq!(s.table, TableName::parse(DEFAULT_TABLE).unwrap());
        assert_eq!(
            s.table.to_string(),
            "ferro_jobs",
            "the default is ferro_jobs (D22 (a))"
        );
        assert_eq!(
            (s.lease_s, s.poll_ms, s.max_wait_ms, s.waker_stmt_timeout_ms),
            (90, 1000, 30_000, 5000)
        );
        assert_eq!(s.max_payload_bytes, 4_194_304);
        assert_eq!((s.dedup_table.clone(), s.dedup_ttl_s), (None, 86_400));
        assert_eq!((s.dedup_purge_ms, s.depth_sample_ms), (60_000, 30_000));
        assert!(s.labelled_queues.is_empty());
    }

    #[test]
    fn every_key_is_read() {
        let cfg = load(&[
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_KIND", "sql"),
            ("FERRO_QUEUE_JOBS_POOL", "my"),
            ("FERRO_QUEUE_JOBS_TABLE", "app.jobs"),
            ("FERRO_QUEUE_JOBS_LEASE_S", "2"),
            ("FERRO_QUEUE_JOBS_POLL_MS", "250"),
            ("FERRO_QUEUE_JOBS_MAX_WAIT_MS", "0"),
            ("FERRO_QUEUE_JOBS_WAKER_STMT_TIMEOUT_MS", "1"),
            ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "1024"),
            ("FERRO_QUEUE_JOBS_LABELLED_QUEUES", " default, emails ,"),
            ("FERRO_QUEUE_JOBS_DEDUP_TABLE", "ferro_dedup"),
            ("FERRO_QUEUE_JOBS_DEDUP_TTL_S", "60"),
            ("FERRO_QUEUE_JOBS_DEDUP_PURGE_MS", "1000"),
            ("FERRO_QUEUE_JOBS_DEPTH_SAMPLE_MS", "0"),
        ]);
        let s = cfg.store("jobs").expect("enabled");
        assert_eq!(s.family, PoolFamily::Mysql);
        assert_eq!(s.table.to_string(), "app.jobs");
        assert_eq!(
            (s.lease_s, s.poll_ms, s.max_wait_ms, s.waker_stmt_timeout_ms),
            (2, 250, 0, 1)
        );
        assert_eq!(s.max_payload_bytes, 1024);
        assert_eq!(s.labelled_queues, vec!["default", "emails"]);
        assert_eq!(s.dedup_table.as_ref().unwrap().to_string(), "ferro_dedup");
        assert_eq!(
            (s.dedup_ttl_s, s.dedup_purge_ms, s.depth_sample_ms),
            (60, 1000, 0)
        );
        assert!(cfg.errors().is_empty());
    }

    #[test]
    fn each_refusal_disables_only_its_own_store_and_names_the_key() {
        let base = |extra: (&'static str, &'static str)| {
            load(&[
                ("FERRO_QUEUE_STORES", "jobs,other"),
                ("FERRO_QUEUE_JOBS_POOL", "main"),
                ("FERRO_QUEUE_OTHER_POOL", "main"),
                extra,
            ])
        };
        let max_plus_one: &'static str =
            Box::leak((max_payload_bytes_limit() + 1).to_string().into_boxed_str());
        let cases: Vec<((&str, &str), &str, Reason)> = vec![
            (
                ("FERRO_QUEUE_JOBS_KIND", "redis"),
                "KIND",
                Reason::KindUnsupported,
            ),
            (
                ("FERRO_QUEUE_JOBS_KIND", "SQL"),
                "KIND",
                Reason::KindUnsupported,
            ),
            (
                ("FERRO_QUEUE_JOBS_POOL", "nope"),
                "POOL",
                Reason::PoolUnknown,
            ),
            (
                ("FERRO_QUEUE_JOBS_POOL", "lite"),
                "POOL",
                Reason::FamilyUnsupported,
            ),
            (
                ("FERRO_QUEUE_JOBS_TABLE", "jobs;drop"),
                "TABLE",
                Reason::Identifier(IdentError::Characters),
            ),
            (
                ("FERRO_QUEUE_JOBS_DEDUP_TABLE", "a.b.c"),
                "DEDUP_TABLE",
                Reason::Identifier(IdentError::TooManyParts),
            ),
            (
                ("FERRO_QUEUE_JOBS_LEASE_S", "1"),
                "LEASE_S",
                Reason::BelowMinimum(2),
            ),
            (
                ("FERRO_QUEUE_JOBS_LEASE_S", "-5"),
                "LEASE_S",
                Reason::NotANumber,
            ),
            (
                ("FERRO_QUEUE_JOBS_LEASE_S", "90s"),
                "LEASE_S",
                Reason::NotANumber,
            ),
            (
                ("FERRO_QUEUE_JOBS_POLL_MS", "0"),
                "POLL_MS",
                Reason::BelowMinimum(1),
            ),
            (
                ("FERRO_QUEUE_JOBS_WAKER_STMT_TIMEOUT_MS", "0"),
                "WAKER_STMT_TIMEOUT_MS",
                Reason::BelowMinimum(1),
            ),
            (
                ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", max_plus_one),
                "MAX_PAYLOAD_BYTES",
                Reason::AboveMaximum(u64::from(max_payload_bytes_limit())),
            ),
            (
                ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "0"),
                "MAX_PAYLOAD_BYTES",
                Reason::BelowMinimum(1),
            ),
            (
                ("FERRO_QUEUE_JOBS_DEDUP_TTL_S", "0"),
                "DEDUP_TTL_S",
                Reason::BelowMinimum(1),
            ),
            (
                ("FERRO_QUEUE_JOBS_DEDUP_PURGE_MS", "0"),
                "DEDUP_PURGE_MS",
                Reason::BelowMinimum(1),
            ),
            (
                ("FERRO_QUEUE_JOBS_LABELLED_QUEUES", "a,\u{0}b"),
                "LABELLED_QUEUES",
                Reason::QueueName(2),
            ),
            (
                ("FERRO_QUEUE_JOBS_LIVENESS_RELEASE", "on"),
                "LIVENESS_RELEASE",
                Reason::LivenessRelease,
            ),
            (
                ("FERRO_QUEUE_JOBS_LIVENESS_RELEASE_MS", "500"),
                "LIVENESS_RELEASE_MS",
                Reason::LivenessRelease,
            ),
            (("FERRO_QUEUE_JOBS_LEASE", "30"), "LEASE", Reason::Unknown),
        ];
        for ((k, v), key, reason) in cases {
            let cfg = base((k, v));
            assert!(cfg.store("jobs").is_none(), "{k}={v:?} must disable jobs");
            assert_eq!(
                errors_of(&cfg, "jobs"),
                vec![(key.to_string(), reason)],
                "{k}={v:?}"
            );
            assert!(
                cfg.store("other").is_some(),
                "{k}: the OTHER store is unaffected"
            );
            for e in cfg.errors() {
                let line = e.to_string();
                assert!(
                    line.contains("queue store jobs") && line.contains(key),
                    "{line}"
                );
                if !v.is_empty() && v != "0" && v != "1" && v != "sql" {
                    assert!(
                        !line.contains(v),
                        "a refusal never quotes the value: {line}"
                    );
                }
            }
        }
        // The control at each bound: the limit itself is accepted.
        let at_limit: &'static str =
            Box::leak(max_payload_bytes_limit().to_string().into_boxed_str());
        assert!(
            base(("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", at_limit))
                .store("jobs")
                .is_some()
        );
        assert_eq!(
            base(("FERRO_QUEUE_JOBS_LEASE_S", "2"))
                .store("jobs")
                .unwrap()
                .lease_s,
            2
        );
    }

    #[test]
    fn pool_is_required_and_blank_reads_as_unset() {
        let cfg = load(&[("FERRO_QUEUE_STORES", "jobs")]);
        assert_eq!(
            errors_of(&cfg, "jobs"),
            vec![("POOL".into(), Reason::PoolRequired)]
        );
        let cfg = load(&[
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "  "),
        ]);
        assert_eq!(
            errors_of(&cfg, "jobs"),
            vec![("POOL".into(), Reason::PoolRequired)]
        );
        let cfg = load(&[
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "main"),
            ("FERRO_QUEUE_JOBS_TABLE", " "),
            ("FERRO_QUEUE_JOBS_LEASE_S", ""),
        ]);
        let s = cfg.store("jobs").unwrap();
        assert_eq!(
            (s.table.to_string().as_str(), s.lease_s),
            ("ferro_jobs", 90)
        );
    }

    #[test]
    fn stores_sharing_a_prefix_resolve_and_orphans_are_reported() {
        let cfg = load(&[
            ("FERRO_QUEUE_STORES", "jobs, jobs_high"),
            ("FERRO_QUEUE_JOBS_POOL", "main"),
            ("FERRO_QUEUE_JOBS_HIGH_POOL", "my"),
            ("FERRO_QUEUE_JOBS_HIGH_LEASE_S", "30"),
            ("FERRO_QUEUE_EMAILS_POOL", "main"),
        ]);
        assert_eq!(cfg.store("jobs").unwrap().family, PoolFamily::Postgres);
        let high = cfg.store("jobs_high").unwrap();
        assert_eq!((high.family, high.lease_s), (PoolFamily::Mysql, 30));
        assert_eq!(cfg.orphan_keys(), ["FERRO_QUEUE_EMAILS_POOL"]);
    }

    #[test]
    fn bad_and_colliding_names_are_refused() {
        let cfg = load(&[
            ("FERRO_QUEUE_STORES", "jobs,bad name,a-b,a_b"),
            ("FERRO_QUEUE_JOBS_POOL", "main"),
            ("FERRO_QUEUE_A_B_POOL", "main"),
        ]);
        assert!(cfg.store("jobs").is_some());
        assert!(cfg.store("a-b").is_none() && cfg.store("a_b").is_none());
        let errors: Vec<String> = cfg.errors().iter().map(|e| e.to_string()).collect();
        assert!(errors.iter().any(|e| e.contains("entry 2")), "{errors:?}");
        assert!(errors.iter().any(|e| e.contains("share one")), "{errors:?}");
    }

    #[test]
    fn unconfigured_means_no_stores() {
        let cfg = load(&[("FERRO_QUEUE_JOBS_POOL", "main")]);
        assert_eq!(cfg.entries().count(), 0);
        assert!(cfg.errors().is_empty());
        assert_eq!(
            cfg.orphan_keys(),
            [] as [String; 0],
            "no store list, so nothing to own a key"
        );
    }

    #[test]
    fn a_non_utf8_value_is_refused_by_key() {
        use std::os::unix::ffi::OsStringExt;
        let cfg = QueueConfig::load(
            vec![
                (OsString::from(STORES_VAR), OsString::from("jobs")),
                (
                    OsString::from("FERRO_QUEUE_JOBS_POOL"),
                    OsString::from("main"),
                ),
                (
                    OsString::from("FERRO_QUEUE_JOBS_TABLE"),
                    OsString::from_vec(vec![0xff]),
                ),
            ],
            &pools,
        );
        assert_eq!(
            errors_of(&cfg, "jobs"),
            vec![("TABLE".into(), Reason::NotUtf8)]
        );
    }
}
