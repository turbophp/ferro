//! Daemon configuration: env-loaded with defaults (charter rule: no hand-rolled protocol
//! constants — `credit_frames`/`credit_bytes` default from `ferro_proto::consts`;
//! `session_cap_bytes` is its OWN literal, a distinct concept from `MAX_FRAME_PAYLOAD`).

use std::path::PathBuf;
use std::time::Duration;

/// Default UDS bind path when `FERRO_SOCK` is unset.
const DEFAULT_SOCKET_PATH: &str = "/run/ferro/dev.sock";

/// Default per-session aggregate credit cap in bytes. A distinct concept from
/// `ferro_proto::consts::MAX_FRAME_PAYLOAD` (the codec's per-frame ceiling) — do NOT couple
/// the two.
const DEFAULT_SESSION_CAP_BYTES: usize = 16 * 1024 * 1024;

/// Default cap on concurrently in-flight requests per session.
const DEFAULT_MAX_INFLIGHT: usize = 1024;

/// Default deadline for a graceful (SIGTERM) drain before hard-closing remaining sessions.
const DEFAULT_DRAIN_DEADLINE: Duration = Duration::from_secs(5);

/// Default deadline for the mandatory first frame (`core/HELLO`) to arrive on a newly accepted
/// connection. Without this bound, a peer that passes the `SO_PEERCRED` gate and then simply
/// never sends anything pins an fd, a session task, and a writer task forever — a slowloris /
/// fd-exhaustion vector, not just a wasted connection.
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default `idle_in_transaction` deadline (S6): the max time a transaction may sit pinned between
/// statements before the engine cancels + rolls it back and reports `TxDeadline{Retryable}` (SPEC
/// §7). Reset on every processed command; modest, not tuned (charter rule 5).
const DEFAULT_IDLE_IN_TX: Duration = Duration::from_secs(10);

/// Default absolute transaction-lifetime deadline (S6): the max total time a transaction may stay
/// pinned, measured from BEGIN and never reset, before the engine cancels + rolls it back and
/// reports `TxDeadline{Retryable}` (SPEC §7). A running statement is bounded by this (not the idle
/// deadline, which only applies while the tx sits idle between statements).
const DEFAULT_MAX_TX: Duration = Duration::from_secs(60);

/// Default bound on the per-`tx_id` actor's teardown ROLLBACK (S6 hardening). On abort/deadline the
/// actor rolls the pinned connection back before releasing it; if that ROLLBACK hangs (a wedged
/// upstream), the actor must NOT hold the connection + its pool permit until an OS TCP timeout — so
/// the teardown ROLLBACK runs under this bound, and on timeout OR error the connection is TAINTED
/// and dropped, letting the pool's (also-bounded) recycle-at-next-checkout reset or evict it.
/// Symmetric with the pool's bounded recycle (`PoolConfig::checkout_timeout`).
const DEFAULT_TX_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// A configured connection pool: the logical `name` a client references in `ExecRequest.pool`,
/// plus the upstream `dsn`.
///
/// Per SPEC §12 the DSN is a SERVER-side secret: the client never sees it, and it must never be
/// logged. The manual `Debug` impl below REDACTS `dsn`, so a `{:?}` on a `Config` (or anywhere a
/// `PoolSpec` is formatted) can never leak a credential-bearing DSN into a log line — the field is
/// deliberately not exposed to the derived `Config` Debug.
#[derive(Clone)]
pub struct PoolSpec {
    pub name: String,
    pub dsn: String,
    /// The assist lexer's (`ferro-classify`, M1-S2) per-pool escape hatch: function names that
    /// always taint + pin-cause `PinFunction`, threaded verbatim into `PoolConfig::pin_functions`.
    /// From `FERRO_POOL_<NAME>_PIN_FUNCTIONS` (comma-separated), default empty.
    pub pin_functions: Vec<String>,
    /// Whether an unrecognized/unclassifiable statement taints the connection, threaded verbatim
    /// into `PoolConfig::pin_on_unknown`. From `FERRO_POOL_<NAME>_PIN_ON_UNKNOWN`, default `true`
    /// (SPEC §7.1 — prefer a false taint to a missed one, charter rule 5).
    pub pin_on_unknown: bool,
}

impl std::fmt::Debug for PoolSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolSpec")
            .field("name", &self.name)
            .field("dsn", &"<redacted>")
            .field("pin_functions", &self.pin_functions)
            .field("pin_on_unknown", &self.pin_on_unknown)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// UDS bind path. From `FERRO_SOCK`, default `/run/ferro/dev.sock`.
    pub socket_path: PathBuf,
    /// Peer-uid allow-list for `SO_PEERCRED` gating. Empty means "allow only the daemon's own
    /// uid" (see `uid_allowed`). From `FERRO_ALLOW_UIDS` (comma-separated), default empty.
    pub peer_allow_uids: Vec<u32>,
    /// Default per-request credit, in frames.
    pub credit_frames: u32,
    /// Default per-request credit, in bytes.
    pub credit_bytes: u32,
    /// Per-session aggregate credit cap in bytes (own literal, see `DEFAULT_SESSION_CAP_BYTES`).
    pub session_cap_bytes: usize,
    /// Max concurrently in-flight requests per session.
    pub max_inflight: usize,
    /// Deadline for a graceful drain (SIGTERM) before hard-closing remaining sessions.
    pub drain_deadline: Duration,
    /// Deadline for the mandatory first frame (`core/HELLO`) to arrive before the connection is
    /// dropped silently (no reply — there was never a valid session to fail).
    pub handshake_timeout: Duration,
    /// `idle_in_transaction` deadline (S6): the max a pinned transaction may sit idle between
    /// statements before it is cancelled + rolled back and reported `TxDeadline{Retryable}`. Reset
    /// on every processed command. Small values are injectable for deterministic actor tests.
    pub idle_in_tx: Duration,
    /// Absolute transaction-lifetime deadline (S6): the max total time a transaction may stay
    /// pinned, from BEGIN, never reset, before it is cancelled + rolled back and reported
    /// `TxDeadline{Retryable}`. Bounds a runaway statement. Injectable small for tests.
    pub max_tx: Duration,
    /// Bound on the actor's teardown ROLLBACK (S6 hardening, see [`DEFAULT_TX_TEARDOWN_TIMEOUT`]):
    /// on abort/deadline the pinned conn is rolled back before release; if that hangs, the conn is
    /// tainted + dropped rather than held (with its pool permit) until an OS TCP timeout.
    pub tx_teardown_timeout: Duration,
    /// Configured upstream connection pools (S5). Each `PoolSpec` names a pool and carries its DSN
    /// (§12 server-side secret — never sent to the client, never logged). Default: empty (the EXEC
    /// handler then answers every request with `Unsupported: unknown pool`). From `FERRO_POOLS`
    /// (comma-separated names) + per-pool `FERRO_POOL_<NAME>_DSN`.
    pub pools: Vec<PoolSpec>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            socket_path: PathBuf::from(DEFAULT_SOCKET_PATH),
            peer_allow_uids: Vec::new(),
            credit_frames: ferro_proto::consts::DEFAULT_CREDIT_FRAMES,
            credit_bytes: ferro_proto::consts::DEFAULT_CREDIT_BYTES,
            session_cap_bytes: DEFAULT_SESSION_CAP_BYTES,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            drain_deadline: DEFAULT_DRAIN_DEADLINE,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            idle_in_tx: DEFAULT_IDLE_IN_TX,
            max_tx: DEFAULT_MAX_TX,
            tx_teardown_timeout: DEFAULT_TX_TEARDOWN_TIMEOUT,
            pools: Vec::new(),
        }
    }
}

impl Config {
    /// Load configuration from the process environment, falling back to defaults for any
    /// variable that is unset or fails to parse.
    pub fn from_env() -> Self {
        let mut cfg = Config::default();

        if let Ok(path) = std::env::var("FERRO_SOCK") {
            cfg.socket_path = PathBuf::from(path);
        }

        if let Ok(list) = std::env::var("FERRO_ALLOW_UIDS") {
            cfg.peer_allow_uids = parse_allow_uids(&list);
        }

        if let Ok(names) = std::env::var("FERRO_POOLS") {
            cfg.pools = parse_pools(&names, &|k| std::env::var(k).ok());
        }

        cfg
    }

    /// The daemon's own uid, via the safe `nix` wrapper around `getuid(2)`.
    pub fn own_uid() -> u32 {
        nix::unistd::getuid().as_raw()
    }

    /// Whether `uid` is allowed to connect. An empty `peer_allow_uids` means "self only" — the
    /// daemon's own uid, per `own_uid()`. A non-empty list is an explicit allow-list membership
    /// check (the daemon's own uid is NOT implicitly included once the list is non-empty).
    pub fn uid_allowed(&self, uid: u32) -> bool {
        if self.peer_allow_uids.is_empty() {
            uid == Self::own_uid()
        } else {
            self.peer_allow_uids.contains(&uid)
        }
    }
}

/// Parse a comma-separated `FERRO_ALLOW_UIDS` value into the uids it names. An unparseable token
/// (empty after trimming aside) is `tracing::warn!`-ed and skipped, NOT silently discarded: a
/// wrong delimiter (e.g. `"33;44"`, a single token that fails to parse as `u32`) would otherwise
/// yield an empty allow-list, which falls back to self-only (`uid_allowed`) — a silent,
/// security-relevant surprise for an operator who intended to allow those other uids. Parsing
/// continues past a bad token (fail-fast is not required here, per the charter's "when uncertain"
/// guidance — a warn is the minimum).
fn parse_allow_uids(list: &str) -> Vec<u32> {
    list.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| match s.parse::<u32>() {
            Ok(uid) => Some(uid),
            Err(err) => {
                tracing::warn!(
                    token = s,
                    error = %err,
                    "FERRO_ALLOW_UIDS: skipping unparseable uid token"
                );
                None
            }
        })
        .collect()
}

/// Parse `FERRO_POOLS` (comma-separated pool names) into `PoolSpec`s, reading each pool's DSN from
/// `FERRO_POOL_<NAME>_DSN` (NAME per [`env_name`]) and its pin-engine escape hatch from
/// `FERRO_POOL_<NAME>_PIN_FUNCTIONS`/`FERRO_POOL_<NAME>_PIN_ON_UNKNOWN` (via
/// [`parse_pool_pin_config`]). A named pool whose DSN env var is unset or empty is
/// `tracing::warn!`-ed and SKIPPED — never defaulted to a bogus DSN (a silent self-connection
/// surprise). The DSN value itself is never logged (§12).
///
/// `lookup` abstracts the env read so this is unit-testable without process-env mutation
/// (`std::env::set_var`/`remove_var` are `unsafe fn` under this crate's edition-2024
/// `unsafe_code = "forbid"`): the real caller passes `&|k| std::env::var(k).ok()`; tests pass a
/// `HashMap`-backed closure.
fn parse_pools(names: &str, lookup: &impl Fn(&str) -> Option<String>) -> Vec<PoolSpec> {
    names
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|name| {
            let env_key = format!("FERRO_POOL_{}_DSN", env_name(name));
            match lookup(&env_key) {
                Some(dsn) if !dsn.is_empty() => {
                    let (pin_functions, pin_on_unknown) = parse_pool_pin_config(name, lookup);
                    Some(PoolSpec {
                        name: name.to_string(),
                        dsn,
                        pin_functions,
                        pin_on_unknown,
                    })
                }
                _ => {
                    tracing::warn!(
                        pool = name,
                        env = %env_key,
                        "FERRO_POOLS: no DSN set for pool; skipping (set the env var to enable it)"
                    );
                    None
                }
            }
        })
        .collect()
}

/// Parse a single pool's pin-engine escape hatch from `FERRO_POOL_<NAME>_PIN_FUNCTIONS`
/// (comma-separated function names, trimmed, empty entries dropped) and
/// `FERRO_POOL_<NAME>_PIN_ON_UNKNOWN` (falsy tokens `"0"`/`"false"`/`"no"`/`"off"`,
/// case-insensitive, trimmed; anything else — including unset — is truthy). NAME is normalized via
/// [`env_name`], the same convention as `FERRO_POOL_<NAME>_DSN`. Defaults (unset): `([], true)` —
/// SPEC §7.1's conservative default (charter rule 5: prefer a false taint to a missed one).
///
/// Pure function over an injected `lookup` — no `std::env` access here — so it is unit-testable
/// with a plain map, without the process-env mutation that `#[forbid(unsafe_code)]` blocks.
fn parse_pool_pin_config(
    name: &str,
    lookup: &impl Fn(&str) -> Option<String>,
) -> (Vec<String>, bool) {
    let fns = lookup(&format!("FERRO_POOL_{}_PIN_FUNCTIONS", env_name(name)))
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let pin_on_unknown = lookup(&format!("FERRO_POOL_{}_PIN_ON_UNKNOWN", env_name(name)))
        .map(|s| {
            !matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true); // default true (SPEC §7.1)
    (fns, pin_on_unknown)
}

/// The env-var-safe form of a pool name: ASCII-uppercased, every non-alphanumeric byte mapped to
/// `_` (so `read-replica` → `READ_REPLICA`, keying `FERRO_POOL_READ_REPLICA_DSN`).
fn env_name(name: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_spec_debug_redacts_dsn() {
        let s = PoolSpec {
            name: "default".to_string(),
            dsn: "postgres://user:hunter2@db.internal/app".to_string(),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        };
        let dbg = format!("{s:?}");
        assert!(dbg.contains("default"), "the pool name is shown");
        assert!(
            !dbg.contains("hunter2"),
            "the DSN (a §12 secret) must NOT appear in Debug output, got {dbg}"
        );
        assert!(dbg.contains("redacted"));
    }

    #[test]
    fn env_name_uppercases_and_sanitizes() {
        assert_eq!(env_name("default"), "DEFAULT");
        assert_eq!(env_name("read-replica"), "READ_REPLICA");
        assert_eq!(env_name("pool.1"), "POOL_1");
    }

    #[test]
    fn parse_allow_uids_skips_malformed_tokens_and_keeps_valid_ones() {
        // "33;44" is a single token with the wrong delimiter -- not parseable as a u32 -- and
        // must not silently swallow the whole list: 55 and 66 on either side of it still make it
        // into the result.
        let uids = parse_allow_uids("55, 33;44 ,66,not-a-uid,");
        assert_eq!(uids, vec![55, 66]);
    }

    #[test]
    fn parse_allow_uids_all_malformed_yields_empty_not_a_panic() {
        assert_eq!(parse_allow_uids("nope;nope"), Vec::<u32>::new());
    }

    #[test]
    fn parse_allow_uids_empty_string_yields_empty() {
        assert_eq!(parse_allow_uids(""), Vec::<u32>::new());
    }

    // -----------------------------------------------------------------------------------------
    // `parse_pool_pin_config` (M1-S2 Task 4): map-backed injected lookup, NO process-env
    // mutation anywhere — `std::env::set_var`/`remove_var` are `unsafe fn` under this crate's
    // edition-2024 `unsafe_code = "forbid"` and would not compile in a test.
    // -----------------------------------------------------------------------------------------

    /// Build a lookup closure backed by a `HashMap`, mirroring how the real `from_env` path
    /// passes `&|k| std::env::var(k).ok()` — here the "env" is just an in-memory map.
    fn map_lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn parse_pool_pin_config_reads_and_trims_pin_functions() {
        let lookup = map_lookup(&[("FERRO_POOL_MAIN_PIN_FUNCTIONS", "app_lock, other_fn")]);
        let (fns, pin_on_unknown) = parse_pool_pin_config("main", &lookup);
        assert_eq!(fns, vec!["app_lock".to_string(), "other_fn".to_string()]);
        assert!(pin_on_unknown, "PIN_ON_UNKNOWN unset must default to true");
    }

    #[test]
    fn parse_pool_pin_config_drops_whitespace_and_empty_entries() {
        let lookup = map_lookup(&[("FERRO_POOL_MAIN_PIN_FUNCTIONS", "a,,b, ")]);
        let (fns, _) = parse_pool_pin_config("main", &lookup);
        assert_eq!(fns, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn parse_pool_pin_config_pin_on_unknown_falsy_tokens() {
        for token in ["0", "false", "False", "FALSE", "no", "No", "off", "OFF"] {
            let lookup = map_lookup(&[("FERRO_POOL_MAIN_PIN_ON_UNKNOWN", token)]);
            let (_, pin_on_unknown) = parse_pool_pin_config("main", &lookup);
            assert!(!pin_on_unknown, "token {token:?} must parse as false");
        }
    }

    #[test]
    fn parse_pool_pin_config_pin_on_unknown_truthy_tokens_and_unset() {
        let lookup = map_lookup(&[]);
        let (_, pin_on_unknown) = parse_pool_pin_config("main", &lookup);
        assert!(pin_on_unknown, "unset must default to true");

        for token in ["1", "true", "TRUE", "yes", "anything-else"] {
            let lookup = map_lookup(&[("FERRO_POOL_MAIN_PIN_ON_UNKNOWN", token)]);
            let (_, pin_on_unknown) = parse_pool_pin_config("main", &lookup);
            assert!(pin_on_unknown, "token {token:?} must parse as true");
        }
    }

    #[test]
    fn parse_pool_pin_config_empty_map_yields_defaults() {
        let lookup = map_lookup(&[]);
        let (fns, pin_on_unknown) = parse_pool_pin_config("main", &lookup);
        assert!(fns.is_empty());
        assert!(pin_on_unknown);
    }

    #[test]
    fn parse_pool_pin_config_uses_env_name_normalization() {
        // Same normalization convention as the `FERRO_POOL_<NAME>_DSN` key: hyphens become `_`,
        // letters are uppercased (see `env_name_uppercases_and_sanitizes` above).
        let lookup = map_lookup(&[("FERRO_POOL_READ_REPLICA_PIN_FUNCTIONS", "app_lock")]);
        let (fns, _) = parse_pool_pin_config("read-replica", &lookup);
        assert_eq!(fns, vec!["app_lock".to_string()]);

        // A lookup keyed on the UN-normalized name must miss.
        let lookup_wrong_key = map_lookup(&[("FERRO_POOL_read-replica_PIN_FUNCTIONS", "app_lock")]);
        let (fns_wrong, _) = parse_pool_pin_config("read-replica", &lookup_wrong_key);
        assert!(fns_wrong.is_empty());
    }

    #[test]
    fn parse_pools_threads_pin_config_through_injected_lookup() {
        let lookup = map_lookup(&[
            ("FERRO_POOL_MAIN_DSN", "postgres://user@db/app"),
            ("FERRO_POOL_MAIN_PIN_FUNCTIONS", "app_lock"),
            ("FERRO_POOL_MAIN_PIN_ON_UNKNOWN", "false"),
            ("FERRO_POOL_OTHER_DSN", "postgres://user@db/other"),
        ]);
        let pools = parse_pools("main,other", &lookup);
        assert_eq!(pools.len(), 2);

        let main = pools.iter().find(|p| p.name == "main").unwrap();
        assert_eq!(main.pin_functions, vec!["app_lock".to_string()]);
        assert!(!main.pin_on_unknown);

        // "other" has a DSN but no pin overrides: must fall back to the conservative defaults.
        let other = pools.iter().find(|p| p.name == "other").unwrap();
        assert!(other.pin_functions.is_empty());
        assert!(other.pin_on_unknown);
    }

    #[test]
    fn parse_pools_skips_pool_with_no_dsn_regardless_of_pin_config() {
        let lookup = map_lookup(&[("FERRO_POOL_MAIN_PIN_FUNCTIONS", "app_lock")]);
        let pools = parse_pools("main", &lookup);
        assert!(
            pools.is_empty(),
            "a pool with no DSN must still be skipped, even if pin config is present"
        );
    }
}
