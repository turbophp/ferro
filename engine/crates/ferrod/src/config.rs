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

/// Default cap on concurrent client connections (M1-S9a, M0-core-review finding 5b). Chosen for a
/// FD BUDGET, not for a round number: every session costs one client fd, and the daemon also holds
/// its upstream pool connections on the same table. systemd's `DefaultLimitNOFILE` soft limit is
/// still 1024 on mainstream distributions, and an fd ceiling that binds BEFORE this cap is strictly
/// worse than the cap — `accept(2)` then fails with `EMFILE` while the connection stays in the
/// backlog, which the accept loop retries immediately (see the note in `serve.rs`). 512 leaves that
/// headroom untouched while sitting comfortably above what a host actually opens: one connection per
/// PHP-FPM worker, and `pm.max_children` is memory-bound to the low hundreds per app.
///
/// An operator raising this MUST raise `LimitNOFILE` with it (SPEC §18's unit is the place for
/// that). Being at the cap is not data loss: the overflow connection gets one loud
/// `POOL_TIMEOUT{Retryable}` frame and the client's resilience loop retries.
const DEFAULT_MAX_CONNECTIONS: usize = 512;

/// Default deadline for a partially-received frame to make PROGRESS (M1-S9a, finding 5c). Read the
/// semantics off [`Config::frame_read_timeout`] — it is a stall detector, not a per-frame
/// completion deadline — which is what makes 30 s the right order of magnitude in BOTH directions:
///
/// - not shorter, because tripping it is session-fatal, and a session-fatal close also aborts every
///   SIBLING request multiplexed on that connection (§10.1). 30 s of a client delivering literally
///   zero bytes of a frame it began is not a slow client; it is a stopped one.
/// - not longer (and not off), because the measured hazard is exactly this: post-handshake, a
///   17-byte send held a session open indefinitely with no reply, no close, and (pre-Task-5) 16 MiB
///   of buffer.
///
/// A legitimate local UDS frame makes progress in microseconds, so this is ~7 orders of magnitude of
/// slack for a healthy client — and, being progress-based, it does not shrink as the frame grows.
const DEFAULT_FRAME_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How often a session re-checks its own liveness deadlines. Coarse on purpose: both knobs it
/// serves are seconds-scale, and this is a per-session timer — one wakeup per second per idle
/// connection is the cost. It is the ENFORCEMENT GRANULARITY, so an expiry lands somewhere in
/// `[timeout, timeout + 2 x LIVENESS_TICK]` (the first tick observing a stalled snapshot only
/// records it; a later one measures the elapsed time against it).
pub const LIVENESS_TICK: Duration = Duration::from_secs(1);

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

/// The DSN schemes the daemon recognizes, and the backend each selects — the ONE allow-list behind
/// both [`infer_pool_kind`] (which scheme means which [`PoolKind`]) and [`loggable_scheme`] (which
/// scheme tokens are safe to echo into a log line). Two derivations of one list is how the two
/// drift, so there is exactly one (the same reasoning that deleted the second `PoolKind::wire_name`
/// derivation in M1-S8a Task 12); the coupling is pinned by
/// `config::tests::every_allow_listed_scheme_is_both_recognized_and_echoed`.
///
/// Matching is ASCII-case-insensitive, and what is ECHOED is always the entry's own `&'static str`,
/// never the operator's bytes: `MariaDB://…` logs `mariadb`.
const KNOWN_SCHEMES: [(&str, PoolKind); 4] = [
    ("postgres", PoolKind::Postgres),
    ("postgresql", PoolKind::Postgres),
    ("mysql", PoolKind::Mysql),
    ("mariadb", PoolKind::Mysql),
];

/// Look a candidate scheme token up in [`KNOWN_SCHEMES`], ASCII-case-insensitively. Returns the
/// ALLOW-LIST's own `&'static str` (not the caller's slice) so a matched scheme can be logged
/// without any operator-supplied byte reaching the log line.
fn allow_listed_scheme(candidate: &str) -> Option<(&'static str, PoolKind)> {
    KNOWN_SCHEMES
        .iter()
        .copied()
        .find(|(name, _)| candidate.eq_ignore_ascii_case(name))
}

/// The ONLY part of a DSN that is safe to log (SPEC §12) — and, since M1-S9a, not a part of the DSN
/// at all: one of the daemon's own constants, chosen by looking the candidate scheme up in
/// [`KNOWN_SCHEMES`]. An unrecognized prefix, or no `://` at all, logs a fixed placeholder.
///
/// **Why an allow-list and not a better slice.** The M0 rule was "everything before the first
/// `://` is the scheme, and a scheme cannot carry credentials". That is false for a malformed DSN:
/// `user:pass://tcp/host` puts the credentials before the first `://` where no real scheme exists,
/// and the M0-core review measured `loggable_scheme("adminuser:s3cretPW://tcp/host")` returning
/// `"adminuser:s3cretPW"` straight into `infer_pool_kind`'s WARN. That was the SECOND member of the
/// class (M1-S6 fixed the no-`://` case by the same string surgery), so the fix is not a third
/// special case: nothing that is not already one of our constants is ever echoed.
///
/// The return type carries the guarantee: `&'static str` cannot borrow from `dsn`, so "log a slice
/// of the operator's string" is a compile error rather than a bug someone can reintroduce. The
/// `split_once` that remains only *locates* a candidate to match — its bytes never reach the output.
fn loggable_scheme(dsn: &str) -> &'static str {
    match dsn.split_once("://") {
        None => "<no scheme>",
        Some((candidate, _)) => {
            allow_listed_scheme(candidate).map_or("<unrecognized scheme>", |(name, _)| name)
        }
    }
}

/// Infer a pool's [`PoolKind`] from its DSN scheme (the token before `://`, matched
/// ASCII-case-insensitively against [`KNOWN_SCHEMES`]): `postgres`/`postgresql` →
/// [`PoolKind::Postgres`]; `mysql`/`mariadb` → [`PoolKind::Mysql`]. An unrecognized or missing
/// scheme is `tracing::warn!`-ed and defaults to [`PoolKind::Postgres`] (the M0 backend) — a
/// conservative default that keeps a typo'd scheme from silently disabling a pool. Pure over its
/// `dsn` input (the warn is a side channel), so it is directly unit-testable.
///
/// The DSN VALUE is never logged here (§12): the warn carries [`loggable_scheme`]'s output, which
/// is always one of the daemon's own constants — an allow-listed scheme name, `<no scheme>`, or
/// `<unrecognized scheme>` — and never a slice of the DSN, whatever shape the operator supplied.
pub fn infer_pool_kind(dsn: &str) -> PoolKind {
    match dsn
        .split_once("://")
        .and_then(|(candidate, _)| allow_listed_scheme(candidate))
    {
        Some((_, kind)) => kind,
        None => {
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
    /// How long a graceful drain (SIGTERM) lets EXISTING work continue before the daemon stops
    /// reading and winds down.
    ///
    /// M1-S9a (finding 6) made this reach the sessions themselves, so it is now the window in which
    /// a session refuses new checkout-acquiring work but keeps serving its already-PINNED
    /// transactions (§18 "let pins finish"). At the deadline each session exits through its own
    /// cleanup — in-flight requests get their one terminal, pinned transactions are rolled back and
    /// their pooled connections released, the writer flushes — and only then closes the socket.
    ///
    /// It is therefore NOT the daemon's total stop time any more: `serve` hard-aborts whatever is
    /// still outstanding one [`crate::serve::SESSION_DRAIN_GRACE`] LATER, so the worst case is
    /// `drain_deadline + SESSION_DRAIN_GRACE`. That grace is not slack — aborting at bare
    /// `drain_deadline` would cut a session off mid-cleanup and destroy terminals the client is
    /// owed (measured; charter rule 4). Size §18's `TimeoutStopSec` against the SUM.
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
    /// Max concurrent client connections the daemon will serve (M1-S9a, finding 5b). From
    /// `FERRO_MAX_CONNECTIONS`, default [`DEFAULT_MAX_CONNECTIONS`]. Checked at accept time, AFTER
    /// the peercred gate: the overflow connection is answered with ONE
    /// `POOL_TIMEOUT{Retryable}` frame and closed (SPEC G-4 — never a silent drop), so it never
    /// becomes a session and the ones already running are untouched.
    pub max_connections: usize,
    /// How long a PARTIALLY-received inbound frame may make NO PROGRESS before the session is
    /// closed with a fatal `PROTOCOL` frame (M1-S9a, finding 5c). From
    /// `FERRO_FRAME_READ_TIMEOUT_MS`, default [`DEFAULT_FRAME_READ_TIMEOUT`].
    ///
    /// **Progress, not completion — and the difference is the whole point.** The clock is reset by
    /// every byte that arrives (`session::codec::ReadProgress`), so it measures the gap between
    /// reads, never the frame's total transfer time. A client sending a large frame slowly is never
    /// killed however long it takes; a client that sent a header and stopped is killed. The
    /// alternative (deadline from when the frame began) was rejected deliberately: its budget is
    /// shared with the daemon's own scheduling latency, so it would start severing healthy sessions
    /// precisely when the host is busy — a new outage class in exchange for no additional
    /// protection, since what a drip client holds is a session SLOT and that is what
    /// `max_connections` bounds.
    ///
    /// Enforced at [`LIVENESS_TICK`] granularity, so the close lands in
    /// `[frame_read_timeout, frame_read_timeout + 2 x LIVENESS_TICK]`.
    pub frame_read_timeout: Duration,
    /// Reap sessions that have been completely quiet for this long. `None` — **the default** — is
    /// off. From `FERRO_IDLE_TIMEOUT_MS`, where unset or `0` means disabled.
    ///
    /// Off by default because the sync PHP client cannot ping while blocked between requests (there
    /// is no background thread — SPEC §10), and a PHP-FPM worker legitimately sits idle for minutes
    /// between web requests: ANY nonzero default would sever every quiet worker on the host, which
    /// is a new outage class, not a fix. The teeth against the measured hazard are
    /// [`Config::frame_read_timeout`] + the codec's bounded reserve + [`Config::max_connections`].
    /// The knob exists for operators who know their fleet (e.g. a rolling deploy that wants old
    /// workers' connections reclaimed promptly).
    ///
    /// A session counts as idle only when NOTHING is happening on it: no complete frame for the
    /// duration, no request in flight, and no partially-received frame outstanding. The close is
    /// silent (no frame) — there is no request to fail, and the client's resilience loop reconnects
    /// on next use.
    pub idle_timeout: Option<Duration>,
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
            max_connections: DEFAULT_MAX_CONNECTIONS,
            frame_read_timeout: DEFAULT_FRAME_READ_TIMEOUT,
            idle_timeout: None,
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

        apply_liveness_knobs(&mut cfg, &|k| std::env::var(k).ok());

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

/// Env var names for the three M1-S9a availability knobs — one place, so the docs, the parser and
/// the tests cannot drift.
const ENV_MAX_CONNECTIONS: &str = "FERRO_MAX_CONNECTIONS";
const ENV_FRAME_READ_TIMEOUT_MS: &str = "FERRO_FRAME_READ_TIMEOUT_MS";
const ENV_IDLE_TIMEOUT_MS: &str = "FERRO_IDLE_TIMEOUT_MS";

/// Apply the three M1-S9a availability knobs from the environment (via an injected `lookup`, the
/// same testability seam `parse_pools` uses — `std::env::set_var` is an `unsafe fn` under this
/// workspace's edition-2024 `unsafe_code = "forbid"`, so a test cannot mutate the real env).
///
/// An unparseable or out-of-range value keeps the default and is `tracing::warn!`-ed rather than
/// silently ignored: the same reasoning as [`parse_allow_uids`] — an operator who typed
/// `FERRO_MAX_CONNECTIONS=1_024` must not silently get 512 with no trace of why. Zero is rejected
/// for the two knobs where it is meaningless (a cap of 0 serves nobody; a 0 ms progress deadline
/// closes every session at the first tick) and ACCEPTED for `FERRO_IDLE_TIMEOUT_MS`, where it is
/// the documented spelling of "disabled".
fn apply_liveness_knobs(cfg: &mut Config, lookup: &impl Fn(&str) -> Option<String>) {
    if let Some(raw) = lookup(ENV_MAX_CONNECTIONS) {
        match raw.trim().parse::<usize>() {
            Ok(n) if n > 0 => cfg.max_connections = n,
            _ => tracing::warn!(
                env = ENV_MAX_CONNECTIONS,
                token = raw.trim(),
                default = cfg.max_connections,
                "unparseable or zero connection cap; keeping the default"
            ),
        }
    }

    if let Some(raw) = lookup(ENV_FRAME_READ_TIMEOUT_MS) {
        match raw.trim().parse::<u64>() {
            Ok(ms) if ms > 0 => cfg.frame_read_timeout = Duration::from_millis(ms),
            _ => tracing::warn!(
                env = ENV_FRAME_READ_TIMEOUT_MS,
                token = raw.trim(),
                default_ms = cfg.frame_read_timeout.as_millis(),
                "unparseable or zero frame-progress deadline; keeping the default"
            ),
        }
    }

    if let Some(raw) = lookup(ENV_IDLE_TIMEOUT_MS) {
        match raw.trim().parse::<u64>() {
            // 0 is the documented spelling of "disabled", not a parse failure.
            Ok(ms) => cfg.idle_timeout = (ms > 0).then(|| Duration::from_millis(ms)),
            Err(err) => tracing::warn!(
                env = ENV_IDLE_TIMEOUT_MS,
                token = raw.trim(),
                error = %err,
                "unparseable idle timeout; keeping it DISABLED"
            ),
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

    // -----------------------------------------------------------------------------------------
    // M1-S9a Task 11: the three availability knobs. The DEFAULTS are the design decision here, so
    // they are asserted directly — each one is a deliberate trade recorded in its own docblock.
    // -----------------------------------------------------------------------------------------

    #[test]
    fn idle_timeout_defaults_to_disabled() {
        assert!(
            Config::default().idle_timeout.is_none(),
            "the sync PHP client cannot ping while blocked between requests, so ANY nonzero \
             default severs every quiet PHP-FPM worker on the host — a new outage class, not a fix"
        );
    }

    #[test]
    fn the_connection_cap_default_leaves_fd_headroom() {
        let cfg = Config::default();
        assert!(
            cfg.max_connections > 0,
            "a cap of 0 would serve nobody at all"
        );
        assert!(
            cfg.max_connections < 1024,
            "the default must stay under systemd's 1024 DefaultLimitNOFILE soft limit: an fd \
             ceiling that binds before the cap turns a clean POOL_TIMEOUT rejection into EMFILE \
             on accept, which is strictly worse — got {}",
            cfg.max_connections
        );
    }

    #[test]
    fn the_frame_progress_deadline_defaults_on_and_generous() {
        let cfg = Config::default();
        assert!(
            cfg.frame_read_timeout >= Duration::from_secs(10),
            "tripping this is session-fatal and also aborts sibling multiplexed requests, so the \
             default must be generous — got {:?}",
            cfg.frame_read_timeout
        );
        assert!(
            cfg.frame_read_timeout <= Duration::from_secs(120),
            "and it must still bound the measured hostage shape (header + 1 byte + silence) — \
             got {:?}",
            cfg.frame_read_timeout
        );
    }

    #[test]
    fn liveness_knobs_read_their_env_vars() {
        let mut cfg = Config::default();
        apply_liveness_knobs(
            &mut cfg,
            &map_lookup(&[
                (ENV_MAX_CONNECTIONS, " 64 "),
                (ENV_FRAME_READ_TIMEOUT_MS, "1500"),
                (ENV_IDLE_TIMEOUT_MS, "900"),
            ]),
        );
        assert_eq!(cfg.max_connections, 64);
        assert_eq!(cfg.frame_read_timeout, Duration::from_millis(1500));
        assert_eq!(cfg.idle_timeout, Some(Duration::from_millis(900)));
    }

    #[test]
    fn idle_timeout_zero_means_disabled_not_instant() {
        let mut cfg = Config {
            idle_timeout: Some(Duration::from_secs(5)),
            ..Config::default()
        };
        apply_liveness_knobs(&mut cfg, &map_lookup(&[(ENV_IDLE_TIMEOUT_MS, "0")]));
        assert_eq!(
            cfg.idle_timeout, None,
            "`FERRO_IDLE_TIMEOUT_MS=0` is the documented spelling of DISABLED — reading it as a \
             0 ms deadline would close every session at the first tick"
        );
    }

    #[test]
    fn unparseable_or_zero_liveness_knobs_keep_the_defaults() {
        let defaults = Config::default();
        for token in ["0", "", "-1", "1_024", "lots", "12ms"] {
            let mut cfg = Config::default();
            apply_liveness_knobs(
                &mut cfg,
                &map_lookup(&[
                    (ENV_MAX_CONNECTIONS, token),
                    (ENV_FRAME_READ_TIMEOUT_MS, token),
                ]),
            );
            assert_eq!(
                cfg.max_connections, defaults.max_connections,
                "max_connections for token {token:?}"
            );
            assert_eq!(
                cfg.frame_read_timeout, defaults.frame_read_timeout,
                "frame_read_timeout for token {token:?}"
            );
        }
        // Same for a garbage idle timeout — which must stay DISABLED, never become 0 ms.
        let mut cfg = Config::default();
        apply_liveness_knobs(&mut cfg, &map_lookup(&[(ENV_IDLE_TIMEOUT_MS, "soon")]));
        assert_eq!(cfg.idle_timeout, None);
    }

    #[test]
    fn absent_liveness_env_vars_change_nothing() {
        let mut cfg = Config {
            max_connections: 7,
            frame_read_timeout: Duration::from_millis(11),
            idle_timeout: Some(Duration::from_millis(13)),
            ..Config::default()
        };
        apply_liveness_knobs(&mut cfg, &map_lookup(&[]));
        assert_eq!(cfg.max_connections, 7);
        assert_eq!(cfg.frame_read_timeout, Duration::from_millis(11));
        assert_eq!(cfg.idle_timeout, Some(Duration::from_millis(13)));
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

    /// §12 secret hygiene, second round (M1-S9a Task 6, M0-core-review finding 7a): the value
    /// handed to `tracing::warn!` for an unrecognized/missing scheme must NEVER be a slice of a
    /// credential-bearing DSN.
    ///
    /// The S6 fix covered no-`://` strings only, and it did so by SLICING — "everything before the
    /// first `://` is the scheme". The review then found the second member of the same class:
    /// `user:pass://…` puts the credentials BEFORE the first `://`, where there is no real scheme
    /// at all, so the slice IS the secret (measured: `loggable_scheme("adminuser:s3cretPW://tcp/host")`
    /// returned `"adminuser:s3cretPW"`, WARN-logged by `infer_pool_kind`). Slicing was never going
    /// to be right; only an ALLOW-LIST is.
    ///
    /// So the property asserted here is not "the output does not contain the word `secret`" — that
    /// is a containment scan and it passes for every password not literally spelled `secret`. It is
    /// **the output is always one of our own fixed constants**, for every input shape, which is
    /// exactly what makes "no operator bytes ever reach a log line" checkable rather than argued.
    /// Every value [`loggable_scheme`] is permitted to return: the allow-listed scheme names plus
    /// the two placeholders. All of them are compile-time constants of OURS — none is derived from
    /// the operator's string.
    const PERMITTED_LOG_VALUES: [&str; 6] = [
        "postgres",
        "postgresql",
        "mysql",
        "mariadb",
        "<no scheme>",
        "<unrecognized scheme>",
    ];

    /// The adversarial DSN corpus: schemeless, malformed, credentials-BEFORE-`://`, `@` in the
    /// password, a NEWLINE/CRLF in the password (a log-injection vector as well as a leak),
    /// non-ASCII, an embedded NUL, and degenerate separators. Every one of these is a shape an
    /// operator's misconfiguration can actually take, and every one carries credential text.
    ///
    /// Shared by the two tests below on purpose: the leak guard proves nothing here is ever
    /// echoed, and the totality guard proves the production caller (`infer_pool_kind`) survives
    /// all of it — one corpus, so a shape added to it is checked from both vantage points.
    const ADVERSARIAL_DSNS: [&str; 15] = [
        // No `://` at all (the S6 shapes: one-slash typo + Go-form MySQL DSN).
        "mysql:/user:secret@db.internal/app",
        "admin:s3cret@tcp(10.0.0.5:3306)/prod",
        "not-a-dsn",
        "",
        // Credentials BEFORE the first `://` — the M0-core-review shape.
        "adminuser:s3cretPW://tcp/host",
        "user:secret@host://whatever",
        // A password containing `@`, and ones containing a newline / CRLF.
        "user:p@ssw0rd://tcp/host",
        "user:pa\nss://tcp/host",
        "user:pa\r\nFAKE-LOG-LINE://tcp/host",
        // Non-ASCII userinfo, and a scheme-lookalike that ASCII-lowercasing cannot fold
        // (fullwidth latin) — the allow-list must reject both without echoing either.
        "üser:sécret://host",
        "ＭＹＳＱＬ://user:secret@h/db",
        "mysql\u{0000}://user:secret@h/db",
        // Degenerate separators.
        "://",
        "://user:secret@h",
        "a://b://c",
    ];

    #[test]
    fn loggable_scheme_never_leaks_credentials() {
        for dsn in ADVERSARIAL_DSNS {
            let logged = loggable_scheme(dsn);
            assert!(
                PERMITTED_LOG_VALUES.contains(&logged),
                "loggable_scheme({dsn:?}) returned {logged:?} — not one of our own constants, \
                 so it is a slice of the operator's DSN (§12)"
            );
        }

        // ---- and the shape-by-shape expectations, so the placeholders stay distinguishable ----
        for dsn in [
            "mysql:/user:secret@db.internal/app",
            "admin:s3cret@tcp(10.0.0.5:3306)/prod",
            "not-a-dsn",
            "",
        ] {
            assert_eq!(loggable_scheme(dsn), "<no scheme>", "for {dsn:?}");
        }
        for dsn in [
            "adminuser:s3cretPW://tcp/host",
            "user:secret@host://whatever",
            "user:p@ssw0rd://tcp/host",
            "user:pa\nss://tcp/host",
            "üser:sécret://host",
            "ＭＹＳＱＬ://user:secret@h/db",
        ] {
            assert_eq!(loggable_scheme(dsn), "<unrecognized scheme>", "for {dsn:?}");
        }
        // An unrecognized-but-harmless-LOOKING scheme is STILL not echoed — we cannot tell it from
        // credential text without parsing, so the allow-list decides, not a character class. This
        // expectation is the S6 test's inverted: it used to assert `redis` passed through.
        assert_eq!(
            loggable_scheme("redis://user:secret@h:6379"),
            "<unrecognized scheme>"
        );

        // The four recognized schemes pass through — and what is echoed is OUR constant, not the
        // operator's bytes: mixed case in, canonical lowercase out.
        assert_eq!(loggable_scheme("postgres://ferro:pw@h/db"), "postgres");
        assert_eq!(loggable_scheme("postgresql://h/db"), "postgresql");
        assert_eq!(loggable_scheme("mysql://ferro:pw@h/db"), "mysql");
        assert_eq!(loggable_scheme("MariaDB://h/db"), "mariadb");
        // A `@` or a second `://` inside the authority of a RECOGNIZED scheme changes nothing.
        assert_eq!(loggable_scheme("mysql://user:p@ss@h/db"), "mysql");
    }

    /// The allow-list is the ONE list behind both consumers, so neither can drift from it — the
    /// failure this pins is "someone adds a scheme to `infer_pool_kind` and the log line then says
    /// `<unrecognized scheme>` for a pool the daemon does recognize", and its mirror. Derived from
    /// `KNOWN_SCHEMES` rather than restating it, so an entry added to the table is checked from
    /// both sides for free.
    #[test]
    fn every_allow_listed_scheme_is_both_recognized_and_echoed() {
        for (name, kind) in KNOWN_SCHEMES {
            let dsn = format!("{name}://ferro:hunter2@db.internal/app");
            assert_eq!(infer_pool_kind(&dsn), kind, "kind for {name:?}");
            assert_eq!(loggable_scheme(&dsn), name, "echo for {name:?}");
            // Matching is ASCII-case-insensitive, and what is echoed is the TABLE's constant —
            // never the operator's casing, which is still operator-supplied bytes.
            let shouty = format!("{}://ferro:hunter2@db.internal/app", name.to_uppercase());
            assert_eq!(infer_pool_kind(&shouty), kind, "kind for shouty {name:?}");
            assert_eq!(loggable_scheme(&shouty), name, "echo for shouty {name:?}");
        }
        // The negative direction: a scheme that is NOT on the list is neither recognized (it falls
        // to the conservative Postgres default) nor echoed.
        assert_eq!(infer_pool_kind("sqlite://x"), PoolKind::Postgres);
        assert_eq!(loggable_scheme("sqlite://x"), "<unrecognized scheme>");
    }

    /// `infer_pool_kind` is the ONLY production caller of `loggable_scheme` (it is what hands the
    /// value to `tracing::warn!`), so the leak guard is asserted from the vantage point the leak
    /// actually happens at too: over the same adversarial corpus it must be total — no panic on a
    /// NUL, a lone `://`, an empty string or non-ASCII — and it must fall to the conservative
    /// Postgres default rather than silently disabling a misconfigured pool.
    #[test]
    fn infer_pool_kind_is_total_over_the_adversarial_corpus() {
        for dsn in ADVERSARIAL_DSNS {
            assert_eq!(
                infer_pool_kind(dsn),
                PoolKind::Postgres,
                "unrecognized DSN {dsn:?} must default to Postgres, never panic"
            );
        }
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
