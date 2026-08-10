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

/// How long the probe may spend obtaining its connection.
///
/// `Pool::checkout`'s own `checkout_timeout` wraps ONLY the semaphore acquire
/// (`ferro-pool/src/pool.rs`), and `PgBackend::connect` calls `tokio_postgres::connect` with no
/// connect timeout, so against a host that accepts nothing and resets nothing the DIAL runs to the
/// OS TCP timeout (~127 s measured). Unbounded, that leaves the probe's `in_flight` marker set for
/// two minutes and holds one of the pool's 16 permits for the same span.
///
/// Bounding it HERE (rather than around the whole probe) is deliberate: dropping a dial is safe —
/// no connection has been handed out yet and the permit releases with the future — whereas dropping
/// a live statement is not (see [`probe_version_on`]).
/// See `docs/followups/2026-08-10-unbounded-backend-dial.md` for the un-fixed root cause.
const VERSION_CHECKOUT_BUDGET: Duration = Duration::from_secs(5);

/// How long the out-of-band CANCEL may take. `PgCancel::cancel` is
/// `tokio_postgres::CancelToken::cancel_query`, which DIALS A FRESH connection — also with no
/// connect timeout. Dropping that dial is harmless: it is a side connection, never the pooled one.
const VERSION_CANCEL_BUDGET: Duration = Duration::from_secs(2);

/// How long the post-CANCEL drain may take before the probe gives up on draining.
///
/// "Drain, don't drop" is the rule, but it cannot be UNBOUNDED: against a wedged backend (still
/// accepting TCP, no longer answering) the drain never completes, and an un-returning probe is what
/// seals a pool un-probeable. So the drain is bounded and, if it does not finish, the checkout is
/// force-TAINTED before it is released — the same net S5's `RowStreamHandle` Drop uses — so the
/// next checkout runs `ROLLBACK` + `DISCARD ALL` (and evicts the connection if that in turn hangs)
/// rather than handing a mid-protocol session to the next tenant (charter rule 6).
const VERSION_DRAIN_BUDGET: Duration = Duration::from_secs(5);

/// The version statement. Works VERBATIM on PostgreSQL, MySQL and MariaDB (function names are
/// case-insensitive in the MySQL family), so no per-backend method is needed — and it is a plain
/// leading `SELECT`, which the S2 assist lexer's safe-list accepts, so the probe does not taint the
/// connection it borrows.
const VERSION_SQL: &str = "SELECT version()";

/// Every knob the version probe is bounded by, in ONE value.
///
/// It exists so the state machine can be exercised at MILLISECOND scale. Before it, the TTL was
/// 600 s and no test could afford to reach the expiry arm, so deleting the `at.elapsed() < TTL`
/// comparison from both sites — i.e. reintroducing exactly the `OnceCell`-seals-for-the-daemon's-
/// life behaviour this design exists to avoid — was undetectable by the whole suite.
///
/// Production uses [`ProbeTuning::default`], which is *literally* the constants above. There is no
/// `#[cfg(test)]` branch in any logic these values feed, so a test that shrinks a duration drives
/// the same code the daemon runs.
#[derive(Clone, Debug)]
struct ProbeTuning {
    /// See [`VERSION_SQL`].
    version_sql: String,
    /// See [`VERSION_TTL`].
    ttl: Duration,
    /// See [`VERSION_RETRY_BACKOFF`].
    backoff: Duration,
    /// See [`VERSION_PROBE_BUDGET`] — the budget for the WHOLE `pool_info` call.
    call_budget: Duration,
    /// See [`VERSION_CHECKOUT_BUDGET`].
    checkout_budget: Duration,
    /// See [`VERSION_STATEMENT_BUDGET`].
    statement_budget: Duration,
    /// See [`VERSION_CANCEL_BUDGET`].
    cancel_budget: Duration,
    /// See [`VERSION_DRAIN_BUDGET`].
    drain_budget: Duration,
    /// Test-only fault injection. Compiled out of every non-test build, so the daemon carries no
    /// branch for it. It exists because the one probe-failure shape that cannot be produced from a
    /// real backend is a PANIC inside the probe task — and that is precisely the shape that used to
    /// leak `in_flight` for the daemon's life.
    #[cfg(test)]
    fault: Option<ProbeFault>,
}

/// A fault a test can inject into the probe. See [`ProbeTuning::fault`].
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeFault {
    /// The probe panics before it touches the pool, unwinding the probe task.
    Panic,
}

impl Default for ProbeTuning {
    fn default() -> Self {
        Self {
            version_sql: VERSION_SQL.to_string(),
            ttl: VERSION_TTL,
            backoff: VERSION_RETRY_BACKOFF,
            call_budget: VERSION_PROBE_BUDGET,
            checkout_budget: VERSION_CHECKOUT_BUDGET,
            statement_budget: VERSION_STATEMENT_BUDGET,
            cancel_budget: VERSION_CANCEL_BUDGET,
            drain_budget: VERSION_DRAIN_BUDGET,
            #[cfg(test)]
            fault: None,
        }
    }
}

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
    /// Shared with the owning [`PoolRegistry`] — the same `Arc`, so there is exactly one set of
    /// knobs per registry.
    tuning: Arc<ProbeTuning>,
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
    /// See [`ProbeTuning`]. Always [`ProbeTuning::default`] in production.
    tuning: Arc<ProbeTuning>,
}

impl PoolRegistry {
    /// Build one [`AnyPool`] per configured pool — a `Pool<PgBackend>` or a `Pool<MysqlBackend>`
    /// per `spec.kind` (inferred from the DSN scheme, `config::infer_pool_kind`). MUST be called
    /// with a tokio runtime already running (see the module docs — the pool's reaper `tokio::spawn`s).
    /// Logs only the pool NAME (+ kind), never the DSN (§12).
    pub fn build(config: &Config) -> Arc<Self> {
        Self::build_with(config, ProbeTuning::default())
    }

    /// [`PoolRegistry::build`] with the probe's timing constants overridden, so a test can drive the
    /// TTL / backoff / budget state machine at millisecond scale instead of at 600 s. The only
    /// difference from `build` is the values in [`ProbeTuning`] — every code path below is shared.
    #[cfg(test)]
    fn build_tuned(config: &Config, tuning: ProbeTuning) -> Arc<Self> {
        Self::build_with(config, tuning)
    }

    fn build_with(config: &Config, tuning: ProbeTuning) -> Arc<Self> {
        let tuning = Arc::new(tuning);
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
                    tuning: Arc::clone(&tuning),
                }),
            );
        }
        Arc::new(Self {
            by_name,
            probes_issued: AtomicU64::new(0),
            tuning,
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
        //
        // This is NOT an untestable discipline choice — inlining these futures into the `join_all`
        // (the natural simplification: it removes an `Arc::clone` and a `JoinHandle`) is caught by
        // `the_probe_is_detached_so_a_budget_expiry_never_recycles_a_mid_statement_connection`
        // below, which reads the pool's idle stack the instant the budget expires.
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
            && tokio::time::timeout(self.tuning.call_budget, futures::future::join_all(handles))
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
            VersionState::Known { version, at } if at.elapsed() < self.tuning.ttl => {
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
            VersionState::Known { at, .. } if at.elapsed() < self.tuning.ttl => false,
            VersionState::Failed { at } if at.elapsed() < self.tuning.backoff => false,
            _ => {
                cache.in_flight = true;
                true
            }
        }
    }

    /// Run the probe and record its outcome.
    ///
    /// `in_flight` is cleared by [`ProbeGuard`]'s `Drop`, not by a trailing statement, so it is
    /// cleared on EVERY exit: a normal return, a probe error, a PANIC unwinding the probe task, and
    /// the whole future being dropped. A trailing write covers only the first two, and the other two
    /// are reachable — a panicking or never-returning probe left `in_flight` set for the daemon's
    /// LIFE, after which `begin_probe` refused every future probe of that pool, its `server_version`
    /// stayed nil until a restart, and one of the pool's 16 permits was held forever. That is the
    /// `OnceCell`-forever failure mode this whole design exists to avoid, wearing a different hat.
    ///
    /// The "never returns" half is closed at the other end too: [`probe_version_on`] bounds every
    /// await it performs (dial, statement, cancel, drain), so the probe is TOTAL.
    async fn probe_and_record(&self) {
        let mut guard = ProbeGuard {
            entry: self,
            version: None,
        };
        guard.version = probe_version(&self.pool, &self.tuning).await;
    }

    /// The version-state lock. Poisoning is recovered from rather than propagated: the guarded
    /// state is three plain scalars with no invariant a panic could break, and a poisoned mutex
    /// here would otherwise turn one unlucky probe into a permanently un-handshakeable daemon.
    fn lock(&self) -> std::sync::MutexGuard<'_, VersionCache> {
        self.version.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The RAII half of [`PoolEntry::probe_and_record`]: whatever happens to the probe, this `Drop`
/// clears `in_flight` and writes an outcome exactly once.
///
/// A guard dropped WITHOUT a version — a probe that failed, panicked, or was dropped mid-flight —
/// records a FAILURE rather than merely clearing the flag. Clearing alone would be correct but
/// unkind: a panicking probe would then re-fire on every single handshake. `Failed` also starts the
/// backoff, so a broken probe costs one attempt per window and still recovers on its own.
struct ProbeGuard<'a> {
    entry: &'a PoolEntry,
    version: Option<String>,
}

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        let mut cache = self.entry.lock();
        cache.in_flight = false;
        cache.state = match self.version.take() {
            Some(version) => VersionState::Known {
                version,
                at: Instant::now(),
            },
            None => {
                tracing::debug!(kind = ?self.entry.kind, "ferrod: server-version probe failed");
                VersionState::Failed { at: Instant::now() }
            }
        };
    }
}

/// Ask one pool for its server version. `None` on ANY failure — unreachable backend, a refused
/// checkout, a statement error, or an unexpected row shape. The caller turns that into
/// `server_version: nil`; it never fails a handshake.
async fn probe_version(pool: &AnyPool, tuning: &ProbeTuning) -> Option<String> {
    #[cfg(test)]
    if tuning.fault == Some(ProbeFault::Panic) {
        panic!("ferrod test fault: the server-version probe panicked");
    }
    match pool {
        AnyPool::Pg(p) => probe_version_on(p, tuning).await,
        AnyPool::Mysql(p) => probe_version_on(p, tuning).await,
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
/// statement is bounded by firing the out-of-band CANCEL and then AWAITING the query future, not by
/// dropping it — a dropped mid-flight statement could leave the connection mid-protocol, and
/// `Checkout::drop` would hand it straight to the next tenant. The one case where the drain itself
/// is abandoned is covered by force-tainting the checkout first; see below.
///
/// **TOTAL by construction.** Every await here is bounded — the dial
/// ([`VERSION_CHECKOUT_BUDGET`]), the statement ([`VERSION_STATEMENT_BUDGET`]), the out-of-band
/// cancel ([`VERSION_CANCEL_BUDGET`]) and the post-cancel drain ([`VERSION_DRAIN_BUDGET`]) — because
/// a probe that never returns is what seals a pool permanently un-probeable. Two of those awaits
/// (the dial and the cancel) can be DROPPED safely: neither has a pooled connection in hand. The
/// drain cannot, so when it is the one that expires the checkout is force-TAINTED before release.
async fn probe_version_on<B: PoolBackend>(pool: &Pool<B>, tuning: &ProbeTuning) -> Option<String> {
    // A dial failure surfaces immediately (`Pool::checkout` has no hidden retry loop); a black-holed
    // host would otherwise never return, hence the bound. Dropping THIS future is safe: no
    // connection has been handed out and the semaphore permit releases with it.
    let mut co = match tokio::time::timeout(tuning.checkout_budget, pool.checkout()).await {
        Ok(Ok(co)) => co,
        Ok(Err(_)) => return None,
        Err(_) => {
            tracing::debug!("ferrod: server-version probe timed out dialling the backend");
            return None;
        }
    };

    // Scoped so the query future — and with it the `&mut co` borrow — is gone before the wedged
    // arm below can force-taint the checkout.
    let res = {
        // Captured BEFORE the mutable query borrow: it returns an OWNED handle, so this borrow ends
        // immediately and does not conflict with the `&mut co` the query future then holds.
        let cancel_handle = co.cancel_handle();
        let fut = co.query(&tuning.version_sql, &[]);
        tokio::pin!(fut);
        tokio::select! {
            biased;
            // Polled FIRST every round, so a statement that completes is never reported as interrupted.
            r = &mut fut => Some(r),
            () = tokio::time::sleep(tuning.statement_budget) => {
                let _ = tokio::time::timeout(tuning.cancel_budget, cancel_handle.cancel()).await;
                tokio::time::timeout(tuning.drain_budget, &mut fut).await.ok()
            }
        }
    };

    let Some(res) = res else {
        // The drain did not finish: the connection is somewhere mid-protocol and MUST NOT be
        // recycled clean. Tainting selects the full `ROLLBACK` + `DISCARD ALL` recycle, which the
        // pool itself bounds and — if that hangs too — evicts (charter rule 6).
        co.set_tainted(true);
        tracing::debug!("ferrod: server-version probe wedged; connection force-tainted");
        return None;
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

    /// Three pools whose kinds are INFERRED from their DSN schemes (never hard-set), pointed at
    /// port 1 — reserved, so a dial is REFUSED instantly on loopback rather than hanging. That is
    /// the point of this fixture: it exercises `pool_info` end to end, offline, with the probe
    /// genuinely attempted and genuinely failing.
    ///
    /// THREE, not two: the exact-order literal below compares against a `HashMap` whose iteration
    /// order is randomised per instance, so with two pools a deleted `sort_by` slips through half
    /// the time. Three takes that to 1-in-6 — and the ordering contract's REAL guard is
    /// `pool_info_is_advertised_in_name_order_whatever_the_maps_own_order`, which is not
    /// probabilistic at all.
    fn config_with_unreachable_pools() -> Config {
        Config {
            pools: vec![
                // Declared in reverse-sorted order so the deterministic-order assertion below can fail.
                spec("reporting", "mysql://u:secret@127.0.0.1:1/rep"),
                spec("default", "postgres://u:secret@127.0.0.1:1/app"),
                spec("archive", "postgres://u:secret@127.0.0.1:1/arc"),
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
        let registry = PoolRegistry::build(&config_with_unreachable_pools());
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
            vec![
                ("archive", "postgres", None),
                ("default", "postgres", None),
                ("reporting", "mysql", None)
            ],
            "HELLO_ACK advertises name + backend family per pool, sorted by name; an unreachable \
             backend advertises no version rather than failing the handshake"
        );
        assert_eq!(
            registry.probes_issued(),
            3,
            "every pool was genuinely probed — the None above is a FAILED probe, not a skipped one"
        );
    }

    /// The `pools` list is advertised in NAME order. `proto/PROTOCOL.md` §4 states that as a
    /// contract ("two connections to one engine see the identical list"), so it is not cosmetic.
    ///
    /// The exact-order literal in the test above CANNOT carry this claim: it compares against a
    /// `HashMap`, whose iteration order is randomised per instance, so deleting `sort_by` merely
    /// makes it fail *sometimes* (measured: 3 red in 5 runs at two pools). Neither would
    /// `is_sorted()` on one sample — a randomly-ordered 3-vector is already sorted 1 time in 6.
    ///
    /// So this test removes the coin flip instead of shrinking it: it builds registries until it
    /// finds one whose OWN map order is not already sorted, and only then asserts that `pool_info`
    /// comes back sorted. With that precondition established, a missing `sort_by` fails 100% of the
    /// time. Each `HashMap` gets its own `RandomState`, so rebuilding really does resample.
    #[tokio::test]
    async fn pool_info_is_advertised_in_name_order_whatever_the_maps_own_order() {
        let config = config_with_unreachable_pools();
        let sorted: Vec<String> = {
            let mut v: Vec<String> = config.pools.iter().map(|p| p.name.clone()).collect();
            v.sort();
            v
        };

        // P(a 3-element map iterates sorted) = 1/6, so 64 draws miss with probability ~1e-50.
        let registry = (0..64)
            .map(|_| PoolRegistry::build(&config))
            .find(|r| {
                let raw: Vec<&String> = r.by_name.keys().collect();
                raw != sorted.iter().collect::<Vec<&String>>()
            })
            .expect("a HashMap whose own iteration order is not already sorted");

        let raw_order: Vec<&str> = registry.by_name.keys().map(String::as_str).collect();
        assert_ne!(
            raw_order,
            sorted.iter().map(String::as_str).collect::<Vec<&str>>(),
            "precondition: the map's own order must differ from sorted, or this test proves nothing"
        );

        let advertised: Vec<String> = registry
            .pool_info()
            .await
            .into_iter()
            .map(|p| p.name)
            .collect();
        assert_eq!(
            advertised, sorted,
            "pool_info must sort by name, not hand back the map's own order (which here is \
             {raw_order:?})"
        );
    }

    /// §12: the DSN is a SERVER-side secret. `PoolInfo` has no DSN field, so the only way one could
    /// reach the wire is via `name`/`kind`/`server_version` — assert on the ENCODED ack bytes, which
    /// is what actually leaves the process, not on the struct.
    #[tokio::test]
    async fn the_encoded_ack_never_carries_a_dsn_or_a_credential() {
        let config = config_with_unreachable_pools();
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

    // ---------------------------------------------------------------------------------------
    // The probe's LIFECYCLE: detachment, TTL expiry, and the in-flight marker.
    //
    // Every test below drives the REAL `PoolRegistry::pool_info` / `PoolEntry::probe_and_record`
    // path; the only thing `build_tuned` changes is how long the production durations are.
    // ---------------------------------------------------------------------------------------

    /// A tuning whose durations are all long enough not to fire, so a test only has to shrink the
    /// one knob it is about.
    fn inert_tuning() -> ProbeTuning {
        ProbeTuning {
            ttl: Duration::from_secs(600),
            backoff: Duration::from_secs(600),
            call_budget: Duration::from_secs(30),
            checkout_budget: Duration::from_secs(30),
            statement_budget: Duration::from_secs(30),
            cancel_budget: Duration::from_secs(30),
            drain_budget: Duration::from_secs(30),
            ..ProbeTuning::default()
        }
    }

    fn one_pool(name: &str, dsn: &str) -> Config {
        Config {
            pools: vec![spec(name, dsn)],
            ..Config::default()
        }
    }

    /// Is there anything on this pool's idle stack RIGHT NOW?
    ///
    /// `poison_idle_for_test` runs its closure only when the stack is non-empty, which makes it a
    /// boolean observable of "has a connection been handed back" — no timing, no flake. The closure
    /// itself mutates nothing.
    fn idle_stack_non_empty(pool: &AnyPool) -> bool {
        let mut seen = false;
        match pool {
            AnyPool::Pg(p) => p.poison_idle_for_test(|_| seen = true),
            AnyPool::Mysql(p) => p.poison_idle_for_test(|_| seen = true),
        }
        seen
    }

    /// The probe tasks are DETACHED (`tokio::spawn`), and that is the only thing standing between
    /// the handshake's budget and a mid-statement connection being handed to the next tenant.
    ///
    /// Inlining them into the `join_all` is the natural simplification — it deletes an `Arc::clone`
    /// and a `JoinHandle` — and until this test nothing caught it. When the whole-call budget
    /// expires, an inlined future is DROPPED, which drops a live `Checkout`, which
    /// `Checkout::drop` pushes straight back onto the idle stack with `tainted == false` while the
    /// server session is still executing the previous tenant's statement. Measured at the
    /// production pool config: idle-stack-non-empty flips FALSE → TRUE, and the next checkout goes
    /// from 73 ms (a fresh dial) to 1.58 s (queued behind the abandoned statement).
    ///
    /// Both halves matter. The first assertion is the property; the second proves the vantage point
    /// is LIVE — without it, "the stack is empty" could just as well mean the closure never runs.
    #[tokio::test]
    async fn the_probe_is_detached_so_a_budget_expiry_never_recycles_a_mid_statement_connection() {
        let Some(url) = env_url("FERRO_TEST_PG_URL") else {
            return;
        };
        let registry = PoolRegistry::build_tuned(
            &one_pool("pg", &url),
            ProbeTuning {
                // Outlasts the whole-call budget, so the budget expires MID-STATEMENT. Well inside
                // `statement_budget`, so the probe is never cancelled — it simply keeps running.
                version_sql: "SELECT version() FROM pg_sleep(3)".to_string(),
                call_budget: Duration::from_millis(500),
                ..inert_tuning()
            },
        );

        let info = registry.pool_info().await;
        // Precondition: the budget really did expire before the statement finished. Without this,
        // a fast statement would make everything below vacuously true.
        assert_eq!(
            info[0].server_version, None,
            "precondition: the call budget must expire while the probe statement is still running"
        );

        let pool = registry.get("pg").expect("pool");
        assert!(
            !idle_stack_non_empty(pool),
            "the probe still owns its checkout: a dropped probe future would have pushed a \
             mid-statement connection back onto the idle stack, UNTAINTED, for the next tenant"
        );

        // The vantage point is live: once the detached probe finishes, that very same closure DOES
        // fire. This is what stops the assertion above from passing for the wrong reason.
        let deadline = Instant::now() + Duration::from_secs(15);
        while !idle_stack_non_empty(pool) {
            assert!(
                Instant::now() < deadline,
                "the detached probe never returned its connection — `idle_stack_non_empty` may be \
                 unable to observe anything at all, which would make the assertion above vacuous"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            registry.probes_issued(),
            1,
            "exactly one probe was issued for the one pool"
        );
    }

    /// TTL expiry, arm 1 of 2: `begin_probe` must RE-PROBE once the TTL has lapsed.
    ///
    /// Deleting the `at.elapsed() < ttl` comparison here reintroduces `OnceCell`-forever — a rolling
    /// backend upgrade would leave `ferrod` advertising the pre-restart version for the daemon's
    /// whole life, and S8b turns that string into a DBAL PLATFORM (i.e. which SQL grammar it emits).
    /// At the production 600 s TTL nothing could reach this arm, which is why the whole suite stayed
    /// green under exactly that deletion.
    #[tokio::test]
    async fn an_expired_version_is_re_probed_rather_than_trusted_for_the_daemons_life() {
        let Some(url) = env_url("FERRO_TEST_PG_URL") else {
            return;
        };
        let registry = PoolRegistry::build_tuned(
            &one_pool("pg", &url),
            ProbeTuning {
                ttl: Duration::from_millis(200),
                ..inert_tuning()
            },
        );

        assert!(
            registry.pool_info().await[0].server_version.is_some(),
            "the first handshake learns the version"
        );
        assert_eq!(registry.probes_issued(), 1);

        // Inside the TTL: no new probe. (The cache is real — this half already had a guard.)
        assert!(registry.pool_info().await[0].server_version.is_some());
        assert_eq!(
            registry.probes_issued(),
            1,
            "a handshake inside the TTL must not re-probe"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;

        let relearned = registry.pool_info().await[0].server_version.clone();
        assert_eq!(
            registry.probes_issued(),
            2,
            "once the TTL has lapsed the version must be RE-PROBED, not trusted forever"
        );
        assert!(relearned.is_some(), "and the re-probe relearns the version");
    }

    /// TTL expiry, arm 2 of 2: `cached_version` must advertise an expired version as UNKNOWN, never
    /// as a stale string a driver would turn into a platform choice.
    ///
    /// Staged so the read is unambiguous: a slow probe statement plus a tiny whole-call budget means
    /// no in-flight probe can ever land inside a `pool_info` call, so what the third call returns is
    /// purely what `cached_version` decided about the state left by the first. The middle call is
    /// the precondition — it proves a `Known` state really was established, so the final `None`
    /// cannot be a probe that simply failed.
    #[tokio::test]
    async fn an_expired_version_is_advertised_as_unknown_not_as_a_stale_string() {
        let Some(url) = env_url("FERRO_TEST_PG_URL") else {
            return;
        };
        let registry = PoolRegistry::build_tuned(
            &one_pool("pg", &url),
            ProbeTuning {
                // ~500 ms per probe, and no `pool_info` call waits longer than 100 ms for one.
                version_sql: "SELECT version() FROM pg_sleep(0.5)".to_string(),
                call_budget: Duration::from_millis(100),
                ttl: Duration::from_millis(600),
                ..inert_tuning()
            },
        );

        // t≈0: the probe is issued and outlives the budget, so this call reports nothing yet.
        assert_eq!(registry.pool_info().await[0].server_version, None);
        assert_eq!(registry.probes_issued(), 1);

        // t≈800 ms: the probe landed at t≈520 ms, so the cache is `Known` and ~280 ms old — inside
        // the 600 ms TTL. This is the precondition: a `Known` state exists to expire.
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(
            registry.pool_info().await[0].server_version.is_some(),
            "precondition: a version was successfully learned and is still inside its TTL"
        );
        assert_eq!(
            registry.probes_issued(),
            1,
            "precondition: still inside the TTL, so nothing has been re-probed yet"
        );

        // t≈1500 ms: that same `Known` entry is now ~980 ms old — EXPIRED. The re-probe this call
        // starts cannot possibly finish inside the 100 ms budget, so the answer comes from
        // `cached_version` alone.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            registry.pool_info().await[0].server_version,
            None,
            "an expired version must be advertised as UNKNOWN — never as a stale string, which a \
             DBAL driver would convert into a platform (i.e. a SQL dialect) choice"
        );
        assert_eq!(
            registry.probes_issued(),
            2,
            "and the expiry did start a fresh probe"
        );
    }

    /// A probe that PANICS must not seal the pool.
    ///
    /// `in_flight` used to be cleared by the statement AFTER the probe await, so an unwinding probe
    /// task left it set for the daemon's life: `begin_probe` then refused every future probe of that
    /// pool, its `server_version` stayed nil until a restart, and one of the pool's 16 permits was
    /// held forever. Measured before the fix: probes 2 → 2 → 2 across three windows, versus 2 → 4 → 6
    /// for a healthy pool. It is now cleared by `ProbeGuard`'s `Drop`, which unwinding runs.
    #[tokio::test]
    async fn a_panicking_probe_does_not_seal_the_pool_permanently_un_probeable() {
        let registry = PoolRegistry::build_tuned(
            &one_pool("dead", "postgres://u:secret@127.0.0.1:1/app"),
            ProbeTuning {
                fault: Some(ProbeFault::Panic),
                // Zero, so the FAILURE the guard records does not itself suppress the retry — this
                // test is about `in_flight`, and the backoff has its own test.
                backoff: Duration::ZERO,
                ..inert_tuning()
            },
        );

        // `join_all` resolves each `JoinHandle` to `Err(JoinError::panic)`, so `pool_info` has
        // already observed the unwind by the time it returns.
        assert_eq!(registry.pool_info().await[0].server_version, None);
        assert_eq!(registry.probes_issued(), 1);

        assert_eq!(registry.pool_info().await[0].server_version, None);
        assert_eq!(
            registry.probes_issued(),
            2,
            "a panicking probe must leave the pool PROBEABLE — a leaked in_flight marker seals it \
             for the daemon's life, which is the OnceCell-forever failure mode wearing a hat"
        );
    }

    /// The same guard, from the other direction: the probe FUTURE being dropped mid-flight also
    /// clears `in_flight`. (This is the mechanism the panic test above rides on, isolated — and it
    /// is the shape `pool_info` would produce if its `tokio::spawn` were ever inlined.)
    #[tokio::test]
    async fn a_probe_future_dropped_mid_flight_still_clears_the_in_flight_marker() {
        // A black hole, so the probe is guaranteed to still be running when we drop it.
        let registry = PoolRegistry::build_tuned(
            &one_pool("blackhole", "postgres://u:secret@10.255.255.1:5432/app"),
            ProbeTuning {
                backoff: Duration::ZERO,
                ..inert_tuning()
            },
        );
        let entry = Arc::clone(registry.by_name.get("blackhole").expect("entry"));

        assert!(entry.begin_probe(), "the claim is available to start with");
        {
            let fut = entry.probe_and_record();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), fut)
                    .await
                    .is_err(),
                "the probe must still be in flight when we drop it"
            );
        } // <- the probe future is dropped here

        assert!(
            entry.begin_probe(),
            "a dropped probe future must still clear in_flight; otherwise the pool is sealed \
             un-probeable for the daemon's life"
        );
    }

    /// The probe's DIAL is bounded, so a wedged backend cannot seal the pool either.
    ///
    /// This is the hang shape that needs no injection at all: `Pool::checkout`'s `checkout_timeout`
    /// wraps only the semaphore acquire and `PgBackend::connect` passes no connect timeout, so
    /// against a black-holed host the dial ran to the OS TCP timeout (~127 s measured) with
    /// `in_flight` set and a pool permit held for the whole span.
    #[tokio::test]
    async fn a_black_holed_dial_is_bounded_so_the_pool_stays_probeable() {
        let registry = PoolRegistry::build_tuned(
            &one_pool("blackhole", "postgres://u:secret@10.255.255.1:5432/app"),
            ProbeTuning {
                checkout_budget: Duration::from_millis(300),
                backoff: Duration::ZERO,
                call_budget: Duration::from_millis(100),
                ..inert_tuning()
            },
        );

        assert_eq!(registry.pool_info().await[0].server_version, None);
        assert_eq!(registry.probes_issued(), 1);

        // Past the dial bound, so the first probe has recorded its failure and released the claim.
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(registry.pool_info().await[0].server_version, None);
        assert_eq!(
            registry.probes_issued(),
            2,
            "an unbounded dial holds in_flight (and a pool permit) for ~127 s; bounded, the pool is \
             probeable again within the dial budget"
        );
    }
}
