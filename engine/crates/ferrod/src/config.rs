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
}

impl std::fmt::Debug for PoolSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolSpec")
            .field("name", &self.name)
            .field("dsn", &"<redacted>")
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
            cfg.pools = parse_pools(&names);
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
/// `FERRO_POOL_<NAME>_DSN` (NAME per [`env_name`]). A named pool whose DSN env var is unset or
/// empty is `tracing::warn!`-ed and SKIPPED — never defaulted to a bogus DSN (a silent
/// self-connection surprise). The DSN value itself is never logged (§12).
fn parse_pools(names: &str) -> Vec<PoolSpec> {
    names
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|name| {
            let env_key = format!("FERRO_POOL_{}_DSN", env_name(name));
            match std::env::var(&env_key) {
                Ok(dsn) if !dsn.is_empty() => Some(PoolSpec {
                    name: name.to_string(),
                    dsn,
                }),
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
}
