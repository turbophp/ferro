//! The daemon pool registry (S5, heterogeneous since M1-S6): one [`AnyPool`] per configured
//! `[[pool]]` — a `Pool<PgBackend>` or a `Pool<MysqlBackend>` depending on the DSN scheme — resolved
//! by the logical name a client puts in `ExecRequest.pool`.
//!
//! Built at startup by [`PoolRegistry::build`], **after the tokio runtime is up** — `Pool::new`
//! spawns the background liveness reaper (`tokio::spawn`; the S4 reaper `max_size` fix is in place),
//! so constructing a pool off-runtime would panic. `main` builds the registry inside its
//! `#[tokio::main]` body; the integration tests build it inside a `#[tokio::test]`.
//!
//! The registry is deliberately heterogeneous (an `AnyPool` enum, not a `Pool<dyn Backend>`): the
//! pool's pin state machine and `Checkout` are monomorphized over the concrete backend `B`, so the
//! SQL/TX handlers dispatch by matching `AnyPool` and calling the SAME generic body (`run_exec_on_pool`
//! / `begin_on_pool` in `services::sql`) for whichever variant — no `dyn` in the hot path.
//!
//! DSNs come from `config.pools` (env/secret refs per SPEC §12): they are the daemon's secret, never
//! sent to a client and never logged — only the pool NAME is ever emitted here.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ferro_backend_mysql::MysqlBackend;
use ferro_backend_pg::PgBackend;
use ferro_pool::backend::{Cancel, PoolBackend};
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;
use ferro_proto::messages::PoolInfo;
use ferro_proto::value::Value;

use crate::config::{Config, PoolKind, PoolSpec};

/// One resolved pool of either supported backend. The SQL/TX handlers `match` on this and call the
/// same generic handler body with the concrete `Pool<B>` — both arms monomorphize, and the request
/// terminal is declared via the `Responder` (no typed return), so the arms unify cleanly.
pub enum AnyPool {
    Pg(Pool<PgBackend>),
    Mysql(Pool<MysqlBackend>),
}

/// Daemon per-pool defaults (M0). Deliberately modest, not tuned: correctness over throughput
/// until the D12 bench (charter rule 5). The reaper IS enabled (`Some`) so a backend killed out
/// from under an idle connection is evicted rather than handed to the next tenant.
const DEFAULT_POOL_MAX_SIZE: usize = 16;
const DEFAULT_POOL_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_POOL_MAX_LIFETIME: Duration = Duration::from_secs(30 * 60);
const DEFAULT_POOL_REAP_INTERVAL: Duration = Duration::from_secs(30);

/// The budget for the WHOLE [`PoolRegistry::pool_info`] call — every pool's probe together, not
/// each one (M1-S8a Task 12).
///
/// **This bound is the one that matters.** The probe runs inside the CLIENT's own I/O deadline:
/// `Ferro::connect` defaults `ioTimeout` to 5 s and applies it to the `HELLO_ACK` read
/// (`php/client/src/Ferro.php`, `Transport.php`), so a PER-POOL bound of 2 s with SERIAL probing
/// means three unreachable pools take 6 s and `Ferro::connect` fails outright — the exact property
/// this whole design exists to preserve. Bounded here, N unreachable pools cost what one does.
const VERSION_PROBE_BUDGET: Duration = Duration::from_millis(1_500);

/// How long a learned version is trusted before it is re-probed.
///
/// Not `OnceCell`-forever: a rolling backend upgrade would otherwise leave `ferrod` advertising a
/// version from before the restart for the daemon's entire life, and a DBAL driver picks a PLATFORM
/// (i.e. which SQL grammar it emits) from that string. Ten minutes is far longer than a handshake
/// storm and far shorter than an upgrade window.
const VERSION_TTL: Duration = Duration::from_secs(600);

/// How long a FAILED probe is remembered before trying again.
///
/// Caching nothing on failure sounds safe and is not: while a backend is down, EVERY handshake
/// would pay a full probe budget. A short negative cache makes a down backend cost one probe per
/// window instead of one per connection, while still recovering within seconds of it coming back.
const VERSION_RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// How long ONE probe task may spend waiting for its statement before it fires the out-of-band
/// CANCEL and drains. Deliberately LONGER than [`VERSION_PROBE_BUDGET`]: the handshake stops
/// WAITING at the budget, but the probe itself keeps going so its result still lands in the cache
/// (and so a hung backend eventually records a FAILURE that the backoff can then suppress).
const VERSION_STATEMENT_BUDGET: Duration = Duration::from_secs(10);

/// The version statement. Works VERBATIM on PostgreSQL, MySQL and MariaDB (function names are
/// case-insensitive in the MySQL family), so no per-backend method is needed — and it is a plain
/// leading `SELECT`, which the S2 assist lexer's safe-list accepts, so the probe does not taint the
/// connection it borrows.
const VERSION_SQL: &str = "SELECT version()";

/// What is currently known about one pool's server version, plus whether a probe is running.
struct VersionCache {
    state: VersionState,
    /// A probe task is in flight for this pool. Checked (never waited on) so a handshake storm —
    /// e.g. every FPM worker reconnecting after a `boot_epoch` change, SPEC §19.1 — starts ONE
    /// probe, not one per connection, and every handshake after the first returns immediately
    /// instead of parking on the budget.
    in_flight: bool,
}

/// What is currently known about one pool's server version.
enum VersionState {
    /// Never probed, or the last attempt's backoff has expired.
    Unknown,
    /// Learned at `at`; trusted until `at + VERSION_TTL`.
    Known { version: String, at: Instant },
    /// Failed at `at`; not retried until `at + VERSION_RETRY_BACKOFF`.
    Failed { at: Instant },
}

/// One resolved pool plus the metadata `HELLO_ACK` advertises for it.
pub struct PoolEntry {
    pool: AnyPool,
    kind: PoolKind,
    /// A plain `Mutex<VersionCache>` rather than a `OnceCell`, for three reasons a `OnceCell` could
    /// not give us: it can EXPIRE (a rolling upgrade must not pin a stale version for the daemon's
    /// life), it can remember a FAILURE for a backoff window, and it never serialises callers
    /// behind an in-flight initialiser — `OnceCell::get_or_try_init` QUEUES concurrent initialisers,
    /// which under a reconnect storm turns N handshakes into N sequential probes. The lock is held
    /// only to read/write the small state, NEVER across the probe await.
    version: Mutex<VersionCache>,
}

/// The set of live pools the SQL EXEC handler resolves by name. Cheap to clone-share behind an
/// `Arc` (each `Pool` is itself an `Arc` handle — cloning shares connections, it does not fork a
/// second pool).
pub struct PoolRegistry {
    by_name: HashMap<String, Arc<PoolEntry>>,
    /// Monotonic count of version probes ISSUED. See [`PoolRegistry::probes_issued`] — it exists so
    /// the "learned once" claim can be ASSERTED rather than assumed: "the second handshake reports
    /// the same string" proves stability, not caching, and a design that re-probed every time would
    /// pass it identically.
    probes_issued: AtomicU64,
}

impl PoolRegistry {
    /// Build one [`AnyPool`] per configured pool — a `Pool<PgBackend>` or a `Pool<MysqlBackend>`
    /// per `spec.kind` (inferred from the DSN scheme, `config::infer_pool_kind`). MUST be called
    /// with a tokio runtime already running (see the module docs — the pool's reaper `tokio::spawn`s).
    /// Logs only the pool NAME (+ kind), never the DSN (§12).
    pub fn build(config: &Config) -> Arc<Self> {
        let mut by_name = HashMap::with_capacity(config.pools.len());
        for spec in &config.pools {
            let cfg = daemon_pool_config(spec);
            let pool = match spec.kind {
                PoolKind::Postgres => AnyPool::Pg(Pool::new(PgBackend::new(spec.dsn.clone()), cfg)),
                PoolKind::Mysql => {
                    AnyPool::Mysql(Pool::new(MysqlBackend::new(spec.dsn.clone()), cfg))
                }
            };
            tracing::info!(pool = %spec.name, kind = ?spec.kind, "ferrod: built connection pool");
            by_name.insert(
                spec.name.clone(),
                Arc::new(PoolEntry {
                    pool,
                    kind: spec.kind,
                    version: Mutex::new(VersionCache {
                        state: VersionState::Unknown,
                        in_flight: false,
                    }),
                }),
            );
        }
        Arc::new(Self {
            by_name,
            probes_issued: AtomicU64::new(0),
        })
    }

    /// Resolve a pool by the name a client referenced in `ExecRequest.pool`. `None` ⇒ the handler
    /// answers `Unsupported: unknown pool`.
    pub fn get(&self, name: &str) -> Option<&AnyPool> {
        self.by_name.get(name).map(|entry| &entry.pool)
    }

    /// The advertised `HELLO_ACK` metadata for every pool, learning any not-yet-known server
    /// version (M1-S8a Task 12).
    ///
    /// **Why here and not at build time:** `ferrod` boots with unreachable backends today (pools are
    /// LAZY — `Pool::new` dials nothing and there is no warmup), and that is a property worth
    /// keeping. **Why not at first checkout:** a session may handshake before any pool has ever been
    /// used, and a driver needs the value deterministically at connect time.
    ///
    /// **Bounded as a WHOLE and probed CONCURRENTLY**, because this call sits on the handshake
    /// critical path inside the client's `ioTimeout` (5 s by default). Any pool that has not
    /// answered when [`VERSION_PROBE_BUDGET`] expires simply reports `None` for THIS handshake;
    /// nothing fails, and the probe keeps running so the next handshake finds the answer cached.
    ///
    /// A probe failure (unreachable backend, timeout, an unexpected row shape) yields `None` for
    /// that pool and is remembered only for [`VERSION_RETRY_BACKOFF`]. The handshake itself never
    /// fails because of it, in any state.
    pub async fn pool_info(&self) -> Vec<PoolInfo> {
        // 0. Iterate in a DETERMINISTIC order. `by_name` is a `HashMap`, so without this both the
        //    advertised pool order and the order probes are ISSUED in would vary per call. The
        //    first matters because a handshake reporting pools differently on every connection is
        //    needlessly untestable; the second matters because it is what makes "the pools are
        //    probed concurrently" a FALSIFIABLE claim — under a serialising mutation, whether the
        //    reachable pool gets probed before an unreachable one eats the budget would otherwise
        //    be a coin flip. Do not replace this with a bare `self.by_name.iter()`.
        let mut entries: Vec<(&String, &Arc<PoolEntry>)> = self.by_name.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        // 1. Start a probe for every pool that needs one — CONCURRENTLY, and as DETACHED tasks.
        //
        // Detached, not inlined into the `join_all`, for the engine's standing "drain, don't drop"
        // rule (see `services::sql::run_autocommit_exec`): when the whole-call budget expires we
        // must stop WAITING, and dropping an inlined future would drop a live `Checkout` with a
        // statement still in flight — which `Checkout::drop` would then push back onto the idle
        // stack UNTAINTED, handing a possibly mid-protocol connection to the next tenant (charter
        // rule 6). A `JoinHandle` can be dropped freely; the task itself runs to completion,
        // releases its checkout the normal way, and records its result for the NEXT handshake.
        let mut handles = Vec::new();
        for (_, entry) in &entries {
            if !entry.begin_probe() {
                continue;
            }
            self.probes_issued.fetch_add(1, Ordering::Relaxed);
            let entry = Arc::clone(entry);
            handles.push(tokio::spawn(async move { entry.probe_and_record().await }));
        }

        // 2. Wait for them under ONE budget. `join_all` inside a single `timeout` is the whole
        //    point: N unreachable pools cost what one does.
        if !handles.is_empty()
            && tokio::time::timeout(VERSION_PROBE_BUDGET, futures::future::join_all(handles))
                .await
                .is_err()
        {
            tracing::debug!(
                "ferrod: server-version probe budget expired; advertising cached values only"
            );
        }

        // 3. Read the cache — free, no I/O, and the SAME read whether the budget expired or not.
        entries
            .into_iter()
            .map(|(name, entry)| PoolInfo {
                name: name.clone(),
                kind: entry.kind.wire_name().to_string(),
                server_version: entry.cached_version(),
            })
            .collect()
    }

    /// How many version probes this registry has ISSUED since boot.
    ///
    /// Exists purely so the caching claim is OBSERVABLE. "The second handshake reports the same
    /// string" proves stability, not caching — a design that re-probed every time would pass it.
    /// This counter is what the live tests assert on.
    pub fn probes_issued(&self) -> u64 {
        self.probes_issued.load(Ordering::Relaxed)
    }

    /// The configured pool names (order unspecified) — for HELLO_ACK's `pools` advertisement and
    /// diagnostics.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(String::as_str)
    }

    /// Number of configured pools.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether no pools are configured (the EXEC handler then answers every request `Unsupported`).
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

impl PoolEntry {
    /// The cached version, WITHOUT probing. `None` for anything but a `Known` state still inside
    /// its TTL — a version we no longer trust is advertised as UNKNOWN, never as a stale string a
    /// driver would turn into a platform choice.
    fn cached_version(&self) -> Option<String> {
        match &self.lock().state {
            VersionState::Known { version, at } if at.elapsed() < VERSION_TTL => {
                Some(version.clone())
            }
            _ => None,
        }
    }

    /// Claim the right to probe this pool, marking it in-flight. `false` ⇒ do not probe: either a
    /// usable answer is already cached, or a failure is still inside its backoff, or another task
    /// is already probing.
    ///
    /// Deliberately non-blocking in every arm: a caller that loses the claim reads the cache and
    /// moves on. Nothing ever waits behind another caller's probe, which is precisely what a
    /// `OnceCell::get_or_try_init` would do.
    fn begin_probe(&self) -> bool {
        let mut cache = self.lock();
        if cache.in_flight {
            return false;
        }
        match &cache.state {
            VersionState::Known { at, .. } if at.elapsed() < VERSION_TTL => false,
            VersionState::Failed { at } if at.elapsed() < VERSION_RETRY_BACKOFF => false,
            _ => {
                cache.in_flight = true;
                true
            }
        }
    }

    /// Run the probe and record its outcome. Always clears `in_flight`, including on the failure
    /// path — a probe that never cleared it would seal the pool as permanently un-probeable, which
    /// is the `OnceCell`-forever failure mode wearing a different hat.
    async fn probe_and_record(&self) {
        let probed = probe_version(&self.pool).await;
        let mut cache = self.lock();
        cache.in_flight = false;
        cache.state = match probed {
            Some(version) => VersionState::Known {
                version,
                at: Instant::now(),
            },
            None => {
                tracing::debug!(kind = ?self.kind, "ferrod: server-version probe failed");
                VersionState::Failed { at: Instant::now() }
            }
        };
    }

    /// The version-state lock. Poisoning is recovered from rather than propagated: the guarded
    /// state is three plain scalars with no invariant a panic could break, and a poisoned mutex
    /// here would otherwise turn one unlucky probe into a permanently un-handshakeable daemon.
    fn lock(&self) -> std::sync::MutexGuard<'_, VersionCache> {
        self.version.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Ask one pool for its server version. `None` on ANY failure — unreachable backend, a refused
/// checkout, a statement error, or an unexpected row shape. The caller turns that into
/// `server_version: nil`; it never fails a handshake.
async fn probe_version(pool: &AnyPool) -> Option<String> {
    match pool {
        AnyPool::Pg(p) => probe_version_on(p).await,
        AnyPool::Mysql(p) => probe_version_on(p).await,
    }
}

/// [`VERSION_SQL`] goes through the ordinary GUARDED [`ferro_pool::pool::Checkout::query`], which
/// means the assist lexer, the RFQ read and the taint bookkeeping all run exactly as they do for a
/// user `SELECT` — nothing special-cased, and no raw-connection back door (charter rule 6).
///
/// The string is returned RAW: normalising it (stripping PG's leading word, extracting a
/// major.minor.patch) would bake one ecosystem's platform-selection conventions into the engine. A
/// Doctrine driver needs `mariadb` to survive in the string; that only works if nothing rewrites it.
///
/// **Drain, don't drop** (the same rule `services::sql::run_autocommit_exec` is built on): the
/// statement is bounded by firing the out-of-band CANCEL and then AWAITING the query future to
/// completion, never by dropping it — a dropped mid-flight statement could leave the connection
/// mid-protocol, and `Checkout::drop` would hand it straight to the next tenant.
async fn probe_version_on<B: PoolBackend>(pool: &Pool<B>) -> Option<String> {
    // A dial failure surfaces immediately (`Pool::checkout` has no hidden retry loop) and a
    // black-holed host simply never returns — which is why the CALLER bounds the wait rather than
    // this function bounding the dial.
    let mut co = pool.checkout().await.ok()?;

    // Captured BEFORE the mutable query borrow: it returns an OWNED handle, so this borrow ends
    // immediately and does not conflict with the `&mut co` the query future then holds.
    let cancel_handle = co.cancel_handle();
    let fut = co.query(VERSION_SQL, &[]);
    tokio::pin!(fut);
    let res = tokio::select! {
        biased;
        // Polled FIRST every round, so a statement that completes is never reported as interrupted.
        r = &mut fut => r,
        () = tokio::time::sleep(VERSION_STATEMENT_BUDGET) => {
            cancel_handle.cancel().await;
            (&mut fut).await
        }
    };

    match res.ok()?.rows.first()?.first()? {
        Value::Text(s) => Some(s.clone()),
        other => {
            tracing::debug!(
                ?other,
                "ferrod: version() returned an unexpected cell shape"
            );
            None
        }
    }
}

/// Build a pool's `PoolConfig` from its `PoolSpec`: fixed daemon defaults (size/timeout/lifetime/
/// reap) plus the per-pool pin-engine escape hatch (`pin_functions`/`pin_on_unknown`, M1-S2 Task 4)
/// carried verbatim from the spec parsed out of `FERRO_POOL_<NAME>_PIN_FUNCTIONS`/
/// `FERRO_POOL_<NAME>_PIN_ON_UNKNOWN` (see `config::parse_pool_pin_config`).
fn daemon_pool_config(spec: &PoolSpec) -> PoolConfig {
    PoolConfig {
        max_size: DEFAULT_POOL_MAX_SIZE,
        checkout_timeout: DEFAULT_POOL_CHECKOUT_TIMEOUT,
        max_lifetime: DEFAULT_POOL_MAX_LIFETIME,
        reap_interval: Some(DEFAULT_POOL_REAP_INTERVAL),
        pin_functions: spec.pin_functions.clone(),
        pin_on_unknown: spec.pin_on_unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::PoolSpec;

    fn spec(name: &str, dsn: &str) -> PoolSpec {
        PoolSpec {
            name: name.to_string(),
            dsn: dsn.to_string(),
            kind: crate::config::infer_pool_kind(dsn),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }
    }

    /// An empty pool config yields an empty registry that resolves nothing — the EXEC handler's
    /// `unknown pool` path. No runtime needed (no pool is built), so this stays a plain unit test.
    #[test]
    fn empty_config_builds_empty_registry() {
        let config = Config::default();
        let registry = PoolRegistry::build(&config);
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.get("default").is_none());
    }

    /// `build` picks the concrete `AnyPool` variant per `spec.kind`. `Pool::new` is LAZY (it only
    /// spawns the reaper; no connection is dialed until checkout), so this builds real pools against
    /// never-dialed DSNs and just inspects the variant — needs a runtime (the reaper spawns), hence
    /// `#[tokio::test]`. Proves the mysql:// DSN yields `AnyPool::Mysql` and postgres:// yields
    /// `AnyPool::Pg` — the monomorphic-registry → heterogeneous-registry fix (M1-S6 Task 5).
    #[tokio::test]
    async fn build_selects_backend_variant_per_kind() {
        let config = Config {
            pools: vec![
                spec("pg", "postgres://user@127.0.0.1:5432/app"),
                spec("my", "mysql://user@127.0.0.1:3306/app"),
            ],
            ..Config::default()
        };
        let registry = PoolRegistry::build(&config);
        assert_eq!(registry.len(), 2);
        assert!(matches!(registry.get("pg"), Some(AnyPool::Pg(_))));
        assert!(matches!(registry.get("my"), Some(AnyPool::Mysql(_))));
        assert!(registry.get("nope").is_none());
    }

    /// Two pools whose kinds are INFERRED from their DSN schemes (never hard-set), pointed at
    /// port 1 — reserved, so a dial is REFUSED instantly on loopback rather than hanging. That is
    /// the point of this fixture: it exercises `pool_info` end to end, offline, with the probe
    /// genuinely attempted and genuinely failing.
    fn config_with_two_unreachable_pools() -> Config {
        Config {
            pools: vec![
                // Declared MySQL-first so the deterministic-order assertion below can fail.
                spec("reporting", "mysql://u:secret@127.0.0.1:1/rep"),
                spec("default", "postgres://u:secret@127.0.0.1:1/app"),
            ],
            ..Config::default()
        }
    }

    /// The `HELLO_ACK` metadata: each pool's backend FAMILY rides the wire beside its name, sorted
    /// by name — and an UNREACHABLE backend still gets its name and family advertised, with
    /// `server_version: None`. This runs the REAL `pool_info` path (probe attempted, probe failed),
    /// which is what makes "the handshake never depends on backend availability" a property of the
    /// production code rather than of a config-only shortcut.
    #[tokio::test]
    async fn pool_info_carries_the_backend_family_even_when_the_backend_is_unreachable() {
        let registry = PoolRegistry::build(&config_with_two_unreachable_pools());
        let info = registry.pool_info().await;

        let seen: Vec<(&str, &str, Option<&str>)> = info
            .iter()
            .map(|p| {
                (
                    p.name.as_str(),
                    p.kind.as_str(),
                    p.server_version.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![("default", "postgres", None), ("reporting", "mysql", None)],
            "HELLO_ACK advertises name + backend family per pool, sorted by name; an unreachable \
             backend advertises no version rather than failing the handshake"
        );
        assert_eq!(
            registry.probes_issued(),
            2,
            "both pools were genuinely probed — the None above is a FAILED probe, not a skipped one"
        );
    }

    /// §12: the DSN is a SERVER-side secret. `PoolInfo` has no DSN field, so the only way one could
    /// reach the wire is via `name`/`kind`/`server_version` — assert on the ENCODED ack bytes, which
    /// is what actually leaves the process, not on the struct.
    #[tokio::test]
    async fn the_encoded_ack_never_carries_a_dsn_or_a_credential() {
        let config = config_with_two_unreachable_pools();
        let registry = PoolRegistry::build(&config);
        let frame = crate::session::handshake::hello_ack_frame(
            1,
            crate::epoch::BootEpoch(7),
            registry.pool_info().await,
        );
        let bytes = frame.payload.to_vec();
        let as_text = String::from_utf8_lossy(&bytes).into_owned();
        for spec in &config.pools {
            assert!(
                !as_text.contains(&spec.dsn),
                "the HELLO_ACK payload must never carry a DSN"
            );
        }
        assert!(
            !as_text.contains("secret"),
            "the HELLO_ACK payload must never carry a DSN credential"
        );
        // ...and the metadata it IS supposed to carry is really there, so the negative above
        // cannot pass merely because the encoder emitted nothing.
        assert!(as_text.contains("default") && as_text.contains("postgres"));
    }

    // ---------------------------------------------------------------------------------------
    // The probe must not leave state on the connection it borrows (charter rule 6).
    //
    // Asserted on a `Checkout` the test still OWNS, before it is returned to the pool: the
    // checkout-time recycle CLEARS `tainted`, so the same assertion made after a re-checkout
    // would be green no matter what the statement did — a guard incapable of failing. Driven
    // through the same `VERSION_SQL` constant and the same guarded `Checkout::query` entry the
    // probe itself uses, so it cannot drift from what production runs.
    // ---------------------------------------------------------------------------------------

    fn probe_pool_config() -> ferro_pool::config::PoolConfig {
        ferro_pool::config::PoolConfig {
            max_size: 1,
            checkout_timeout: Duration::from_secs(5),
            max_lifetime: Duration::from_secs(60),
            reap_interval: None,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }
    }

    fn env_url(var: &str) -> Option<String> {
        match std::env::var(var) {
            Ok(u) if !u.is_empty() => Some(u),
            _ => {
                eprintln!("skip: {var} unset");
                None
            }
        }
    }

    /// PG: `SELECT version()` must leave the connection CLEAN — no taint (so the next checkout does
    /// NOT need a `DISCARD ALL`) and no open transaction. The assist lexer's rule-8 safe-list
    /// accepts a leading `SELECT` and the RFQ byte comes back `I`.
    #[tokio::test]
    async fn the_version_statement_does_not_taint_a_pg_connection() {
        let Some(url) = env_url("FERRO_TEST_PG_URL") else {
            return;
        };
        let pool = Pool::new(PgBackend::new(url), probe_pool_config());
        let mut co = pool.checkout().await.expect("checkout");
        assert!(!co.tainted(), "a fresh connection starts clean");
        let r = co.query(VERSION_SQL, &[]).await.expect("SELECT version()");
        assert!(matches!(r.rows[0][0], Value::Text(_)));
        assert!(
            !co.tainted(),
            "the version probe must not taint the connection it borrows"
        );
        assert!(
            !co.tx_open(),
            "the version probe must not open a transaction"
        );
    }

    /// MySQL/MariaDB: same property, read off the OTHER signal — the OK packet's session-state
    /// trackers (`PinCause::SessionTracker`), which is what taints on this engine family.
    #[tokio::test]
    async fn the_version_statement_does_not_taint_a_mysql_connection() {
        let Some(url) = env_url("FERRO_TEST_MYSQL_URL") else {
            return;
        };
        let pool = Pool::new(MysqlBackend::new(url), probe_pool_config());
        let mut co = pool.checkout().await.expect("checkout");
        assert!(!co.tainted(), "a fresh connection starts clean");
        let r = co.query(VERSION_SQL, &[]).await.expect("SELECT version()");
        assert!(matches!(r.rows[0][0], Value::Text(_)));
        assert!(
            !co.tainted(),
            "the version probe must not taint the connection it borrows"
        );
        assert!(
            !co.tx_open(),
            "the version probe must not open a transaction"
        );
    }
}
