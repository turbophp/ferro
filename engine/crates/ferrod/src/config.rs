//! Daemon configuration: env-loaded with defaults (charter rule: no hand-rolled protocol
//! constants — `credit_frames`/`credit_bytes` default from `ferro_proto::consts`).
//!
//! `credit_bytes` is DELIBERATELY COUPLED to `ferro_proto::consts::MAX_FRAME_PAYLOAD` (M1-S5,
//! user-confirmed Option B for the large-row hazard — SPEC §5.2/§22): a single indivisible row
//! can be as large as `MAX_FRAME_PAYLOAD`, and the client will not replenish a request's credit
//! window before it has SEEN that frame, so the initial per-request byte window must always be
//! able to fit one such frame — otherwise a large row can never be sent and the request hangs
//! forever. `session_cap_bytes` is a separate, own-literal concept (the *aggregate* per-session
//! cap vs. the *per-frame* codec ceiling) but is validated (`Config::validate`) to also be
//! `>= MAX_FRAME_PAYLOAD`, for the same reason: a per-request window cannot exceed the session
//! cap it draws from.

use std::path::PathBuf;
use std::time::Duration;

/// Default UDS bind path when `FERRO_SOCK` is unset.
const DEFAULT_SOCKET_PATH: &str = "/run/ferro/dev.sock";

/// Default per-session aggregate credit cap in bytes. A distinct concept from
/// `ferro_proto::consts::MAX_FRAME_PAYLOAD` (the codec's per-frame ceiling: this is the
/// session-wide running total, that is the per-frame limit) — its default is its OWN literal,
/// not derived from `MAX_FRAME_PAYLOAD` the way `credit_bytes` now is. It happens to equal 16 MiB
/// here, which already satisfies `Config::validate`'s `>= MAX_FRAME_PAYLOAD` floor (see the module
/// doc above for why that floor exists).
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

/// The upstream backend a pool speaks (M1-S6). Inferred from the DSN scheme by [`infer_pool_kind`]
/// (`postgres`/`postgresql` → [`PoolKind::Postgres`]; `mysql`/`mariadb` → [`PoolKind::Mysql`]) — the
/// daemon has no separate `kind =` knob, the scheme IS the selector. `PoolRegistry::build` matches
/// on this to construct the right concrete `Pool<B>` variant (`AnyPool`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolKind {
    Postgres,
    Mysql,
}

impl PoolKind {
    /// The backend-family token advertised in `HELLO_ACK`'s `PoolInfo.kind` (PROTOCOL.md §4) — the
    /// string a DBAL driver reads to pick a platform family before it has seen any server version.
    ///
    /// ONE source of truth on purpose, and now literally one CALLER:
    /// `pools::PoolRegistry::pool_info` is the only site that renders this token. Task 11 briefly
    /// had a second, config-derived derivation (`session::handshake::pool_info_from_config`);
    /// Task 12 DELETED it rather than keep it as a pool-less fallback that could never fire, on
    /// the grounds that two derivations of one wire field is how the two drift (SPEC §22.2 (v)).
    /// The match is exhaustive with no `_` arm, so a third backend family breaks the build here
    /// rather than silently inheriting `"postgres"`.
    pub fn wire_name(self) -> &'static str {
        match self {
            PoolKind::Postgres => "postgres",
            PoolKind::Mysql => "mysql",
        }
    }
}

/// The ONLY portion of a DSN that is safe to log (SPEC §12): the scheme token — the substring
/// strictly BEFORE a real `://` separator. In a URL-form DSN the credentials always follow `://`
/// (`scheme://user:pass@host`), so the scheme itself can never carry them. When the DSN has no
/// `://` at all there is no scheme to isolate and the WHOLE string is potentially credential-
/// bearing, so we return a fixed placeholder rather than any slice of it. This is what closes the
/// schemeless-DSN leak: `split("://").next()` returns the whole string when the delimiter is
/// absent, which would emit the credentials — `split_once` returns `None` and we log nothing of it.
fn loggable_scheme(dsn: &str) -> &str {
    dsn.split_once("://")
        .map_or("<no scheme>", |(scheme, _)| scheme)
}

/// Infer a pool's [`PoolKind`] from its DSN scheme (the substring before `://`, ASCII-lowercased):
/// `postgres`/`postgresql` → [`PoolKind::Postgres`]; `mysql`/`mariadb` → [`PoolKind::Mysql`]. An
/// unrecognized or missing scheme is `tracing::warn!`-ed and defaults to [`PoolKind::Postgres`]
/// (the M0 backend) — a conservative default that keeps a typo'd scheme from silently disabling a
/// pool. Pure over its `dsn` input (the warn is a side channel), so it is directly unit-testable.
/// The DSN VALUE is never logged here (§12) — only the scheme token via [`loggable_scheme`], which
/// yields `<no scheme>` (never any slice of the DSN) for a schemeless/typo'd credential-bearing DSN.
pub fn infer_pool_kind(dsn: &str) -> PoolKind {
    match dsn
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .as_deref()
    {
        Some("mysql") | Some("mariadb") => PoolKind::Mysql,
        Some("postgres") | Some("postgresql") => PoolKind::Postgres,
        _ => {
            tracing::warn!(
                scheme = loggable_scheme(dsn),
                "FERRO_POOLS: unrecognized DSN scheme; defaulting pool kind to Postgres"
            );
            PoolKind::Postgres
        }
    }
}

/// A configured connection pool: the logical `name` a client references in `ExecRequest.pool`,
/// the upstream `dsn`, and the `kind` (backend) inferred from that DSN's scheme.
///
/// Per SPEC §12 the DSN is a SERVER-side secret: the client never sees it, and it must never be
/// logged. The manual `Debug` impl below REDACTS `dsn`, so a `{:?}` on a `Config` (or anywhere a
/// `PoolSpec` is formatted) can never leak a credential-bearing DSN into a log line — the field is
/// deliberately not exposed to the derived `Config` Debug.
#[derive(Clone)]
pub struct PoolSpec {
    pub name: String,
    pub dsn: String,
    /// The upstream backend this pool speaks, inferred from the DSN scheme by [`infer_pool_kind`].
    pub kind: PoolKind,
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
            .field("kind", &self.kind)
            .field("pin_functions", &self.pin_functions)
            .field("pin_on_unknown", &self.pin_on_unknown)
            .finish()
    }
}

/// A `Config` invariant violated at load time. Caught by [`Config::validate`], which the daemon's
/// `main` calls right after `Config::from_env()` (fail fast at startup, not at first request).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// `credit_bytes` (the per-request DATA credit window, see the module doc's M1-S5 coupling
    /// note) is below `ferro_proto::consts::MAX_FRAME_PAYLOAD`: a single valid frame at the
    /// ceiling could never fit the initial window, and the client will not replenish credit before
    /// it has seen that frame — a permanent hang, reintroducing the large-row hazard this default
    /// was coupled to close.
    #[error(
        "credit_bytes ({credit_bytes}) must be >= MAX_FRAME_PAYLOAD ({max_frame_payload}): a \
         single valid DATA frame at the frame ceiling must always fit the initial per-request \
         credit window, or a large row can never be sent (permanent hang)"
    )]
    CreditBytesBelowMaxFramePayload {
        credit_bytes: u32,
        max_frame_payload: u32,
    },
    /// `session_cap_bytes` (the aggregate per-session credit cap) is below `MAX_FRAME_PAYLOAD`.
    /// Since a per-request window cannot exceed the session cap it draws from, this would make it
    /// impossible to grant a spec-conformant (`>= MAX_FRAME_PAYLOAD`) `credit_bytes` window at all.
    #[error(
        "session_cap_bytes ({session_cap_bytes}) must be >= MAX_FRAME_PAYLOAD \
         ({max_frame_payload})"
    )]
    SessionCapBelowMaxFramePayload {
        session_cap_bytes: usize,
        max_frame_payload: usize,
    },
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

    /// Fail-fast validation of the large-row invariant (M1-S5, see the module doc): both
    /// `credit_bytes` and `session_cap_bytes` must be `>= ferro_proto::consts::MAX_FRAME_PAYLOAD`,
    /// or a single maximally-sized DATA frame could never fit its credit window — a permanent
    /// hang, not merely a slow path. Called once at startup, right after `Config::from_env()`; not
    /// re-checked per-request (a `Config` is immutable for the life of the process).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let max_frame_payload = ferro_proto::consts::MAX_FRAME_PAYLOAD;
        if self.credit_bytes < max_frame_payload {
            return Err(ConfigError::CreditBytesBelowMaxFramePayload {
                credit_bytes: self.credit_bytes,
                max_frame_payload,
            });
        }
        if self.session_cap_bytes < max_frame_payload as usize {
            return Err(ConfigError::SessionCapBelowMaxFramePayload {
                session_cap_bytes: self.session_cap_bytes,
                max_frame_payload: max_frame_payload as usize,
            });
        }
        Ok(())
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
                    let kind = infer_pool_kind(&dsn);
                    Some(PoolSpec {
                        name: name.to_string(),
                        dsn,
                        kind,
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

    // ---------------------------------------------------------------------------------------
    // M1-S5 Task 1b: `credit_bytes` coupled to `MAX_FRAME_PAYLOAD` (large-row rule, user
    // Option B) + `Config::validate` enforcing the floor on both `credit_bytes` and
    // `session_cap_bytes`.
    // ---------------------------------------------------------------------------------------

    #[test]
    fn default_config_credit_bytes_equals_max_frame_payload() {
        let cfg = Config::default();
        assert_eq!(
            cfg.credit_bytes,
            ferro_proto::consts::MAX_FRAME_PAYLOAD,
            "credit_bytes must default to MAX_FRAME_PAYLOAD so a single maximally-sized DATA \
             frame always fits the initial per-request credit window"
        );
    }

    #[test]
    fn validate_accepts_the_default_config() {
        assert_eq!(Config::default().validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_credit_bytes_below_max_frame_payload() {
        let cfg = Config {
            credit_bytes: ferro_proto::consts::MAX_FRAME_PAYLOAD - 1,
            ..Config::default()
        };
        assert_eq!(
            cfg.validate(),
            Err(ConfigError::CreditBytesBelowMaxFramePayload {
                credit_bytes: ferro_proto::consts::MAX_FRAME_PAYLOAD - 1,
                max_frame_payload: ferro_proto::consts::MAX_FRAME_PAYLOAD,
            })
        );
    }

    #[test]
    fn validate_rejects_session_cap_bytes_below_max_frame_payload() {
        let cfg = Config {
            session_cap_bytes: ferro_proto::consts::MAX_FRAME_PAYLOAD as usize - 1,
            ..Config::default()
        };
        assert_eq!(
            cfg.validate(),
            Err(ConfigError::SessionCapBelowMaxFramePayload {
                session_cap_bytes: ferro_proto::consts::MAX_FRAME_PAYLOAD as usize - 1,
                max_frame_payload: ferro_proto::consts::MAX_FRAME_PAYLOAD as usize,
            })
        );
    }

    #[test]
    fn validate_checks_credit_bytes_before_session_cap_bytes() {
        // Both fields violated: the credit_bytes check must win (documented order), not silently
        // report only the session_cap_bytes violation.
        let cfg = Config {
            credit_bytes: 0,
            session_cap_bytes: 0,
            ..Config::default()
        };
        assert_eq!(
            cfg.validate(),
            Err(ConfigError::CreditBytesBelowMaxFramePayload {
                credit_bytes: 0,
                max_frame_payload: ferro_proto::consts::MAX_FRAME_PAYLOAD,
            })
        );
    }

    #[test]
    fn pool_spec_debug_redacts_dsn() {
        let s = PoolSpec {
            name: "default".to_string(),
            dsn: "postgres://user:hunter2@db.internal/app".to_string(),
            kind: PoolKind::Postgres,
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

    // -----------------------------------------------------------------------------------------
    // M1-S6 Task 5: `PoolKind` inferred from the DSN scheme (there is no separate `kind =` knob —
    // the scheme IS the selector). `PoolRegistry::build` matches on this to build the right
    // concrete `Pool<B>`.
    // -----------------------------------------------------------------------------------------

    #[test]
    fn infer_pool_kind_from_scheme() {
        assert_eq!(
            infer_pool_kind("mysql://ferro:ferro@127.0.0.1:33060/ferro"),
            PoolKind::Mysql
        );
        assert_eq!(
            infer_pool_kind("mariadb://ferro:ferro@127.0.0.1:33061/ferro"),
            PoolKind::Mysql
        );
        assert_eq!(
            infer_pool_kind("postgres://ferro:ferro@localhost:5432/ferro"),
            PoolKind::Postgres
        );
        assert_eq!(
            infer_pool_kind("postgresql://ferro@localhost/ferro"),
            PoolKind::Postgres
        );
        // Scheme is case-insensitive.
        assert_eq!(infer_pool_kind("MySQL://h/db"), PoolKind::Mysql);
        // Unknown / missing scheme defaults to Postgres (the M0 backend), never a panic.
        assert_eq!(infer_pool_kind("sqlite://x"), PoolKind::Postgres);
        assert_eq!(infer_pool_kind("not-a-dsn"), PoolKind::Postgres);
        assert_eq!(infer_pool_kind(""), PoolKind::Postgres);
    }

    /// §12 secret hygiene: the value handed to `tracing::warn!` for an unrecognized/missing scheme
    /// must NEVER be a slice of a credential-bearing DSN. `loggable_scheme` is that exact value.
    /// A schemeless/typo'd DSN (no `://`) — the case an operator misconfiguration lands in — must
    /// resolve to the fixed `<no scheme>` placeholder, not the whole password-bearing string.
    #[test]
    fn loggable_scheme_never_leaks_credentials() {
        // No `://` at all: the whole string is potentially credential-bearing → placeholder only.
        // These are the exact leak shapes the review flagged (one-slash typo + Go-form MySQL DSN).
        for dsn in [
            "mysql:/user:secret@db.internal/app",
            "admin:s3cret@tcp(10.0.0.5:3306)/prod",
            "not-a-dsn",
            "",
        ] {
            assert_eq!(loggable_scheme(dsn), "<no scheme>");
            // Belt-and-suspenders: whatever we log for these must not contain any credential text.
            let logged = loggable_scheme(dsn);
            assert!(!logged.contains("secret"), "leaked credential via {dsn:?}");
            assert!(!logged.contains("s3cret"), "leaked credential via {dsn:?}");
        }
        // A real scheme (before `://`) is credential-free by construction and IS safe to log —
        // even an unrecognized one, and even when the authority after `://` carries a password.
        assert_eq!(loggable_scheme("mysql://ferro:pw@h/db"), "mysql");
        assert_eq!(loggable_scheme("redis://user:secret@h:6379"), "redis");
    }

    #[test]
    fn parse_pools_infers_kind_from_each_dsn_scheme() {
        let lookup = map_lookup(&[
            ("FERRO_POOL_PGPOOL_DSN", "postgres://user@db/app"),
            ("FERRO_POOL_MYPOOL_DSN", "mysql://user@db/app"),
        ]);
        let pools = parse_pools("pgpool,mypool", &lookup);
        let pg = pools.iter().find(|p| p.name == "pgpool").unwrap();
        let my = pools.iter().find(|p| p.name == "mypool").unwrap();
        assert_eq!(pg.kind, PoolKind::Postgres);
        assert_eq!(my.kind, PoolKind::Mysql);
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
