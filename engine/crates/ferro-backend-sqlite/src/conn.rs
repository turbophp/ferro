//! `SqliteConn` + `SqliteBackend`: connection setup and the blocking-call bridge (C3-3a).
//!
//! ## Why every call parks the connection
//!
//! `rusqlite` is SYNCHRONOUS and `PoolBackend`'s methods are `async`, so each one must cross
//! `spawn_blocking` or it would stall a tokio worker. `Connection` is `Send` but not `Sync`, so it
//! cannot be borrowed into a blocking task — it has to be MOVED in and MOVED back. That is exactly
//! the park/unpark shape the spike's `p2` proved (the connection comes back usable and
//! autocommit-clean), and the same shape `MysqlConn` already uses for streaming, so the pool's
//! existing `reclaim_stream` seam fits when C3-5 needs it.
//!
//! The consequence worth stating: while parked, the wrapper is a husk. Every accessor reachable in
//! that window answers safely rather than unwrapping, and a blocking task that PANICS loses the
//! connection for good — so that case marks the wrapper dead and the pool discards it instead of
//! handing the next tenant a husk (charter rule 6).

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use ferro_pool::backend::{
    Cancel, Dialect, PoolBackend, QueryResult, Reclaimed, ResetProfile, TxStatus,
};
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;
use rusqlite::Connection;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use crate::stream::SqliteRowStream;

/// The busy-timeout used when the caller does not name one.
///
/// It matches `PoolConfig::default().checkout_timeout`, because §7.6 says the SQLite busy timeout is
/// "bounded by `checkout_timeout`": waiting longer for the writer lock than the pool will wait for
/// the whole checkout just converts one error into a slower version of the same error.
///
/// **Wiring debt, stated rather than implied:** a backend does not see `PoolConfig`, so this is a
/// default and not yet the real bound. C3-3e — which constructs the backend from config — must pass
/// the pool's actual `checkout_timeout` to [`SqliteBackend::with_busy_timeout`]. Until then a
/// non-default `checkout_timeout` is not reflected here.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolve a pool DSN to the database file path.
///
/// Accepts `sqlite://<path>` (the scheme `ferrod`'s `infer_pool_kind` will key on at C3-3e) and a
/// bare filesystem path. Split out from [`SqliteBackend::connect`] so the refusals below are
/// unit-testable without touching a filesystem.
///
/// # Why an in-memory database is REFUSED
///
/// SQLite gives every `:memory:` connection its OWN private database. A pool holds many
/// connections, so a `:memory:` DSN would silently hand different tenants different databases —
/// one worker's writes invisible to the next, with no error anywhere. That is a data-correctness
/// failure that presents as a mystery, and it is squarely against §7.6's "the engine owns the
/// file". The hazard is proven rather than assumed: see `memory_dsn_hazard_is_real` in
/// `tests/connect_it.rs`, which demonstrates two `:memory:` connections failing to see each other's
/// table before asserting that this function refuses the DSN.
///
/// `mode=memory` (the URI spelling, including the shared-cache form) is refused by the same rule:
/// `file::memory:?cache=shared` would at least be one database, but it is process-local and vanishes
/// with the daemon, which is not a pool backing store either.
pub fn resolve_path(dsn: &str) -> Result<PathBuf, PoolError> {
    let raw = dsn.strip_prefix("sqlite://").unwrap_or(dsn);

    // Checked on the RAW spelling, before any path handling, so the refusal cannot be dodged by a
    // scheme prefix or by the URI form.
    let lowered = raw.to_ascii_lowercase();
    if lowered.is_empty() {
        return Err(PoolError::Unsupported(
            "sqlite DSN names no database file (expected sqlite://<path>)".to_string(),
        ));
    }
    if lowered.contains(":memory:") || lowered.contains("mode=memory") {
        return Err(PoolError::Unsupported(
            "sqlite in-memory databases are not poolable: SQLite gives every :memory: connection \
             its own private database, so a pool would silently hand different tenants different \
             databases. Name a file (SPEC §7.6: the engine owns the file)."
                .to_string(),
        ));
    }

    Ok(PathBuf::from(raw))
}

/// A pooled SQLite connection.
pub struct SqliteConn {
    /// The driver handle — **`Option` because it is PARKED for the duration of every blocking
    /// call** (see the module docs). `None` means a call is in flight or the connection was lost to
    /// a panicking blocking task.
    conn: Option<Connection>,
    /// Whether `PRAGMA query_only` is currently armed. Tracked so a recycle can disarm it without a
    /// round trip to ask, and so a double-arm is a no-op rather than a second statement.
    query_only: bool,
}

impl SqliteConn {
    /// Borrow the live handle. `None` while parked or dead.
    pub fn driver(&self) -> Option<&Connection> {
        self.conn.as_ref()
    }

    /// Is this connection armed read-only (`PRAGMA query_only=ON`)?
    pub fn is_query_only(&self) -> bool {
        self.query_only
    }

    /// Take the handle out for a blocking call. `None` if already parked or dead.
    fn park(&mut self) -> Option<Connection> {
        self.conn.take()
    }

    /// Put the handle back after a blocking call.
    fn unpark(&mut self, conn: Connection) {
        self.conn = Some(conn);
    }

    /// [`SqliteConn::park`] for a STREAM (C3-5), where the window is not one blocking call but the
    /// whole life of the row stream.
    ///
    /// Separate names rather than making the pair `pub(crate)` because the two windows differ in
    /// the one way that matters: a blocking call always restores the handle before the `await`
    /// returns, so no other code can observe the gap, whereas a stream leaves it parked across
    /// arbitrarily many awaits. C3-3a removed a `dead` flag as unobservable and said C3-5 should
    /// reintroduce it only if that window turns out to be POOL-VISIBLE — it is not: between
    /// `query_stream` and `reclaim_stream` the connection is checked out, so the pool's `is_closed`
    /// sweep never sees it, and `finalize_stream` reads `tx_status` only after the reclaim has put
    /// the handle back. The flag stays gone, and these names are where a future reader will look.
    pub(crate) fn park_for_stream(&mut self) -> Option<Connection> {
        self.park()
    }

    /// The other half of [`SqliteConn::park_for_stream`].
    pub(crate) fn unpark_from_stream(&mut self, conn: Connection) {
        self.unpark(conn);
    }
}

/// The SQLite arm of the `PoolBackend` seam. One backend, one database file.
pub struct SqliteBackend {
    dsn: String,
    busy_timeout: Duration,
    /// SPEC D14: the directory the engine will open files in on a client's behalf. `None` means
    /// the DEFAULT — the database file's own directory — which is resolved per-connect because
    /// that is where the DSN is parsed.
    allow_dir: Option<PathBuf>,
}

impl SqliteBackend {
    /// Infallible, matching `PgBackend::new`/`MysqlBackend::new`: the DSN is parsed per-connect, so
    /// a bad one surfaces as a connect failure exactly as it does on the other two backends.
    pub fn new(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            allow_dir: None,
        }
    }

    /// Override the busy timeout — see [`DEFAULT_BUSY_TIMEOUT`]. Under `ferrod` this is ALWAYS
    /// called, with the owning pool's `checkout_timeout` (C3-3e); the default applies only when the
    /// backend is driven directly, as the tests here do.
    pub fn with_busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.busy_timeout = busy_timeout;
        self
    }

    /// SPEC **D14**: widen the directory the engine may open files in on a client's behalf.
    ///
    /// The default — the database file's own directory — needs no configuration and is what every
    /// existing deployment already satisfies. An operator sets this only to point snapshots at a
    /// backup volume.
    pub fn with_allow_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.allow_dir = Some(dir.into());
        self
    }

    /// The busy timeout this backend opens its connections with. Exists so the C3-3e wiring can be
    /// asserted at the seam it crosses — the registry builds the backend, and without a reader the
    /// only evidence that the pool's `checkout_timeout` arrived would be that a timing test passed,
    /// which a wrong-but-similar value would also produce.
    pub fn busy_timeout(&self) -> Duration {
        self.busy_timeout
    }

    /// Run `f` on the blocking pool with the connection MOVED in and back out.
    ///
    /// This is the bridge every later slice's statement runner goes through, so its failure
    /// handling is the contract: a panicking closure loses the handle, and that marks the wrapper
    /// dead rather than leaving a husk the pool would happily reuse.
    async fn with_conn<T, F>(conn: &mut SqliteConn, f: F) -> Result<T, PoolError>
    where
        F: FnOnce(&mut Connection) -> Result<T, PoolError> + Send + 'static,
        T: Send + 'static,
    {
        let mut handle = conn.park().ok_or(PoolError::Closed)?;
        let joined = tokio::task::spawn_blocking(move || {
            let out = f(&mut handle);
            (handle, out)
        })
        .await;

        match joined {
            Ok((handle, out)) => {
                conn.unpark(handle);
                out
            }
            Err(e) => {
                // The blocking task panicked; the Connection was owned by it and is gone.
                tracing::warn!(
                    error = %e,
                    "ferro-backend-sqlite: blocking task panicked; connection lost and marked dead"
                );
                // NOTHING to undo: `park` already moved the handle into the task, and this arm
                // simply never unparks it. The wrapper is left with `conn: None`, which `is_closed`
                // reports as closed, so the pool discards it. An explicit "mark dead" call was
                // written here first and REMOVED — see the unit test's note: a mutation proved it
                // was unreachable dead code, because park had already done the work.
                Err(PoolError::ConnectionLost)
            }
        }
    }

    /// Establish a connection and apply the §7.6 / D13 setup.
    pub async fn connect(&self) -> Result<SqliteConn, PoolError> {
        let path = resolve_path(&self.dsn)?;
        let busy_timeout = self.busy_timeout;
        let allow_dir = self.allow_dir.clone();

        let conn =
            tokio::task::spawn_blocking(move || open_configured(&path, busy_timeout, allow_dir))
                .await
                .map_err(|e| {
                    tracing::warn!(error = %e, "ferro-backend-sqlite: connect task panicked");
                    PoolError::ConnectionLost
                })??;

        Ok(SqliteConn {
            conn: Some(conn),
            query_only: false,
        })
    }

    /// Cheap liveness check.
    pub async fn ping(&self, conn: &mut SqliteConn) -> Result<(), PoolError> {
        Self::with_conn(conn, |c| {
            c.execute_batch("SELECT 1").map_err(|e| {
                tracing::debug!(error = %e, "ferro-backend-sqlite: ping failed");
                PoolError::ConnectionLost
            })
        })
        .await
    }

    /// Synchronous "obviously dead" check — no round trip, as the trait requires.
    ///
    /// A parked connection reads dead too. That is deliberate and mirrors `MysqlConn`'s FB-2
    /// contract: the pool must never hand on a wrapper whose handle is elsewhere. In practice the
    /// pool cannot observe a parked connection anyway, since the caller holds `&mut` across the
    /// whole blocking call.
    pub fn is_closed(&self, conn: &SqliteConn) -> bool {
        conn.conn.is_none()
    }

    /// A per-backend constant, as the trait documents.
    pub fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }

    /// The pin AUTHORITY (SPEC §7.1). A LIVE read of `sqlite3_get_autocommit()` — never a cached
    /// flag the pool maintains.
    ///
    /// The spike's `p6` is why: SQLite ends transactions BY ITSELF. A constraint declared
    /// `ON CONFLICT ROLLBACK` rolls the whole transaction back when violated, with nothing in the
    /// statement text saying so, so an engine tracking its own `BEGIN`/`COMMIT` would hold a pin
    /// for a transaction that no longer exists and eventually hand the next tenant a connection it
    /// believes is mid-transaction.
    ///
    /// **`Failed` here is the ABSENCE of a signal, not a signal.** SQLite has no
    /// aborted-open-transaction state in which later statements are refused, so no SQLite
    /// transaction-state signal ever means `Failed` (§7.1). The one case that returns it is a
    /// connection with no live handle — lost to a panicking blocking task — where there is nothing
    /// to read. `Failed` is the safest answer the enum offers (it has no "unknown"), and it matches
    /// `MysqlConn`'s contract for its own handle-less window. In practice the pool reaches
    /// `is_closed` first, which already reports such a connection closed; this is defence in depth,
    /// and it can never fire for a merely PARKED connection, because a caller holds `&mut` across
    /// every blocking window and so no `&` borrow can exist to call this.
    pub fn tx_status(&self, conn: &SqliteConn) -> TxStatus {
        match conn.driver() {
            Some(c) => {
                if c.is_autocommit() {
                    TxStatus::Idle
                } else {
                    TxStatus::InTx
                }
            }
            None => TxStatus::Failed,
        }
    }

    /// What a recycled NON-tainted connection gets.
    ///
    /// **`Some(Targeted)`, and C3-3b's `Some(Full)` was wrong the moment `Full` grew teeth.** That
    /// slice reasoned the two profiles were interchangeable "because SQLite's reset is in-process
    /// and destroys no prepared statements", which was true of the reset it shipped and is no
    /// longer true of this one: `Full` now CLOSES AND REOPENS the connection, which was measured at
    /// ~250 µs (debug build). §16's boundary target is p50 < 60 µs, so paying that on every
    /// recycled checkout would blow the latency budget on hygiene alone.
    ///
    /// **The gate is sound, which is the reason this is safe to narrow.** The state `Full` exists
    /// to clear is connection-scoped PRAGMA state, and the only statements that can set it are
    /// `PRAGMA` and `ATTACH` — both of which `ferro-classify`'s SQLite dialect taints
    /// UNCONDITIONALLY, ahead of its safe-list and independent of `pin_on_unknown` (the same shape
    /// as MySQL's `CALL`/`DO` backstop). A connection that reaches this profile has issued neither,
    /// so it has no pragma state to clear. What it CAN still carry — an open transaction, an armed
    /// `query_only`, temp objects (`CREATE` is safe-listed, so temp DDL does not taint on this
    /// dialect) — is exactly what `Targeted` handles.
    ///
    /// `None` (skip hygiene entirely) is still deliberately NOT taken: that is an optimization, it
    /// is the same one still deferred for MySQL (R2), and `Targeted` is cheap.
    pub fn clean_reset_profile(&self) -> Option<ResetProfile> {
        Some(ResetProfile::Targeted)
    }

    /// Hygiene reset. **The two profiles now differ, and the difference is the point.**
    ///
    /// * `Full` (a TAINTED connection) closes the handle and opens a fresh one — see
    ///   [`SqliteBackend::reopen`].
    /// * `Targeted` (a clean recycle) runs the explicit list below.
    ///
    /// C3-3b shipped both arms as the same list and said so. That list was built from what the
    /// ENGINE leaves on a connection, and it is complete for that — but a TENANT can leave a great
    /// deal more, and none of it was cleared. MEASURED through a real pool, one tenant per column,
    /// the next tenant inherited every one of these: `foreign_keys=OFF` (which silently disarms the
    /// integrity guarantee `open_configured` declares, for everyone afterwards),
    /// `writable_schema=1` (which lets the next tenant `DELETE FROM sqlite_master` — schema
    /// destruction — without ever asking for the privilege), `trusted_schema=0`,
    /// `ignore_check_constraints=1`, `read_uncommitted=1`, `recursive_triggers=1`,
    /// `legacy_alter_table=1`, `cell_size_check`, `automatic_index=0`, `reverse_unordered_selects`,
    /// `secure_delete`, `synchronous=0`, `fullfsync`, `checkpoint_fullfsync`, `cache_size`,
    /// `hard_heap_limit`, `analysis_limit`, `threads` — and `busy_timeout`, which is the pool's own
    /// checkout bound (C3-3e). Seventeen of the twenty-three probed.
    ///
    /// **Enumerating them is the wrong fix and was rejected for a stated reason.** SQLite has
    /// around sixty pragmas and gains more with each release, so a hand-kept list rots silently —
    /// which is exactly the failure C3-6a found in `foreign_keys` resting on a build flag nobody
    /// had decided. The right analogue is the one MySQL already uses: `COM_RESET_CONNECTION`
    /// (M1-S6). SQLite's equivalent of that is closing and reopening, and it is complete by
    /// construction rather than by enumeration.
    ///
    /// # The `Targeted` list
    ///
    /// SQLite has **no `DISCARD ALL` analogue**, so this is an explicit list rather than a ported
    /// one. What a pooled SQLite connection that issued no `PRAGMA`/`ATTACH` can still carry into
    /// the next tenant:
    ///
    /// 1. **An open transaction** — the floor. Gated on the live signal, because `ROLLBACK` errors
    ///    with "cannot rollback - no transaction is active" when none is open.
    /// 2. **`PRAGMA query_only`**, if a declared-`readonly` checkout armed it (C3-3a). This is the
    ///    ENGINE's own arming, not a tenant's, which is why it belongs on this profile too.
    /// 3. **ATTACHed databases** — kept even though `ATTACH` taints and so takes the `Full` arm:
    ///    the cost is one catalogue read on a connection that has none, and a profile that depended
    ///    on the classifier being exhaustive would fail silently the day it is not.
    /// 4. **Temp objects** — `CREATE TEMP TABLE` lives in the per-connection `temp` schema and
    ///    survives until the connection closes. `CREATE` is safe-listed on this dialect, so temp
    ///    DDL does NOT taint and this is the only profile that will ever see it.
    pub async fn reset(
        &self,
        conn: &mut SqliteConn,
        profile: ResetProfile,
    ) -> Result<(), PoolError> {
        if profile == ResetProfile::Full {
            return self.reopen(conn).await;
        }
        Self::with_conn(conn, |c| {
            // 1. Any open transaction, including one the caller never closed.
            if !c.is_autocommit() {
                c.execute_batch("ROLLBACK").map_err(|e| {
                    tracing::warn!(error = %e, "ferro-backend-sqlite: reset rollback failed");
                    PoolError::Backend(format!("reset rollback failed: {e}"))
                })?;
            }

            // 2. Read-only arming from a declared-readonly checkout.
            c.execute_batch("PRAGMA query_only=OFF")
                .map_err(|e| PoolError::Backend(format!("reset query_only=OFF failed: {e}")))?;

            // 3. Attached databases. `main` and `temp` are built in and cannot be detached.
            let attached: Vec<String> = {
                let mut stmt = c
                    .prepare(
                        "SELECT name FROM pragma_database_list WHERE name NOT IN ('main','temp')",
                    )
                    .map_err(|e| PoolError::Backend(format!("reset database_list failed: {e}")))?;
                let rows = stmt
                    .query_map([], |r| r.get::<_, String>(0))
                    .map_err(|e| PoolError::Backend(format!("reset database_list failed: {e}")))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PoolError::Backend(format!("reset database_list failed: {e}")))?
            };
            for name in attached {
                // The name comes from SQLite's own catalogue, not from user text, but it is still
                // quoted rather than interpolated bare: a database attached under a name containing
                // a quote would otherwise compose broken SQL.
                let quoted = name.replace('"', "\"\"");
                c.execute_batch(&format!("DETACH DATABASE \"{quoted}\""))
                    .map_err(|e| PoolError::Backend(format!("reset detach failed: {e}")))?;
            }

            // 4. Temp objects. Views and triggers go before tables, since dropping a table a view
            //    depends on is fine but the reverse leaves a dangling object.
            let temp_objects: Vec<(String, String)> = {
                let mut stmt = c
                    .prepare(
                        "SELECT type, name FROM temp.sqlite_master \
                         WHERE type IN ('view','trigger','index','table') \
                         ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 \
                                            WHEN 'index' THEN 2 ELSE 3 END",
                    )
                    .map_err(|e| PoolError::Backend(format!("reset temp scan failed: {e}")))?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                    .map_err(|e| PoolError::Backend(format!("reset temp scan failed: {e}")))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PoolError::Backend(format!("reset temp scan failed: {e}")))?
            };
            for (kind, name) in temp_objects {
                // SQLite creates internal indexes (autoindex) that cannot be dropped directly;
                // they disappear with their table, so a failure to drop one is not a reset failure.
                let quoted = name.replace('"', "\"\"");
                let sql = format!("DROP {kind} IF EXISTS temp.\"{quoted}\"");
                if let Err(e) = c.execute_batch(&sql) {
                    if name.starts_with("sqlite_") {
                        continue;
                    }
                    return Err(PoolError::Backend(format!(
                        "reset drop temp {kind} failed: {e}"
                    )));
                }
            }
            Ok(())
        })
        .await?;
        conn.query_only = false;
        Ok(())
    }

    /// The `Full` profile: close this connection and open a fresh one.
    ///
    /// **This is SQLite's `COM_RESET_CONNECTION`.** MySQL recycles every reused connection through
    /// that command (M1-S6) precisely because enumerating session state is not something a pool can
    /// keep correct; SQLite offers no such command, but it also has no server to reconnect to — the
    /// "reconnect" is a local file open — so closing and reopening buys the same completeness at a
    /// cost a network backend could not pay. Everything a tenant could have set is gone by
    /// construction, and [`open_configured`] re-applies the DECLARED setup (WAL verified,
    /// `busy_timeout` from the pool's `checkout_timeout`, `foreign_keys` on and read back) rather
    /// than trusting whatever the previous tenant left.
    ///
    /// **The old handle is closed BEFORE the new one opens, and the order is deliberate.** Dropping
    /// a `rusqlite::Connection` closes it, releasing its locks and its reference to the WAL; doing
    /// that first means the pool never briefly holds two handles per slot.
    ///
    /// **Two hazards were checked rather than assumed.**
    ///
    /// * `PRAGMA journal_mode=WAL` **cannot switch** a database while another connection holds a
    ///   lock (found by a failing test at C3-3e), and `open_configured` treats a non-WAL answer as
    ///   a hard connect error — so a reopen under a sibling's write lock could have made hygiene
    ///   fail. Measured: on an ALREADY-WAL database the same pragma returns `"wal"` under another
    ///   connection's write lock. C3-3e's failure was the rollback→WAL *switch*, which a reopen of
    ///   a live pool's database never performs.
    /// * A failed reopen must leave the connection DEAD, not half-reset. It does, by construction:
    ///   the handle is parked (moved out) first and is only restored on the success arm, so every
    ///   error path leaves `SqliteConn` handle-less, which `is_closed` reports and the pool
    ///   discards. That is the same contract the panic arm of [`SqliteBackend::with_conn`] relies
    ///   on.
    async fn reopen(&self, conn: &mut SqliteConn) -> Result<(), PoolError> {
        // Resolved BEFORE the handle is parked. Parking is what makes a connection dead on every
        // error path below, which is right for a failure to OPEN — but a DSN that parsed at connect
        // will parse again, so letting that (impossible) case kill a live connection would be a
        // gratuitous eviction.
        let path = resolve_path(&self.dsn)?;
        let busy_timeout = self.busy_timeout;
        let allow_dir = self.allow_dir.clone();
        let old = conn.park().ok_or(PoolError::Closed)?;

        let fresh = tokio::task::spawn_blocking(move || {
            drop(old);
            open_configured(&path, busy_timeout, allow_dir)
        })
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-sqlite: reopen task panicked");
            PoolError::ConnectionLost
        })??;

        conn.unpark(fresh);
        // The tracked flag follows the handle: a brand-new connection has `query_only` off.
        conn.query_only = false;
        Ok(())
    }

    /// Raw simple query — UNGUARDED, as the trait documents (the pin hook and internal reset).
    ///
    /// # Why the affected count is not simply `changes()`
    ///
    /// Both of SQLite's counters are wrong on their own, in opposite directions, and both were
    /// MEASURED rather than reasoned about:
    ///
    /// * **`changes()` goes STALE.** It reports the last data-modifying statement's count, so after
    ///   a `BEGIN`, a `SELECT` or a `COMMIT` it still returns whatever the previous INSERT changed.
    ///   Measured: 3 rows inserted, then `BEGIN` → still 3. Since this method is exactly what the
    ///   pin hook runs `BEGIN`/`COMMIT`/`ROLLBACK` through, that is the common case, not a corner.
    /// * **`total_changes()` OVER-reports.** It is cumulative and includes rows written by triggers
    ///   and by foreign-key cascades. Measured: inserting 1 row into a table with an `AFTER INSERT`
    ///   trigger moves it by 2; deleting 1 parent with 2 `ON DELETE CASCADE` children moves it by 3.
    ///
    /// So the delta of `total_changes()` decides WHETHER the statement changed anything, and
    /// `changes()` reports HOW MANY. That keeps `BEGIN` at 0 and keeps a triggered INSERT at the
    /// statement's own 1 — which is what PostgreSQL and MySQL report for the same shapes.
    ///
    /// **The trade-off, stated:** a multi-statement batch reports its LAST statement's count rather
    /// than the sum. That is also what PG and MySQL do for a simple-query batch (the command tag of
    /// the final statement), so it is family behaviour rather than a SQLite quirk.
    pub async fn simple_query(&self, conn: &mut SqliteConn, sql: &str) -> Result<u64, PoolError> {
        let sql = sql.to_string();
        Self::with_conn(conn, move |c| {
            let before = c.total_changes();
            c.execute_batch(&sql).map_err(|e| {
                tracing::debug!(error = %e, "ferro-backend-sqlite: simple_query failed");
                crate::error_map::map(&e)
            })?;
            Ok(if c.total_changes() == before {
                0
            } else {
                c.changes()
            })
        })
        .await
    }

    /// Row-returning, parameterized statement — buffered, as the trait documents.
    ///
    /// **No `?`→`$n` normalization is needed**: the trait's contract says `sql` arrives already
    /// normalized "by the backend", and SQLite's native placeholder IS `?`, so this arm has nothing
    /// to rewrite. Named after the PG backend's need, not a universal one.
    ///
    /// Cell tagging is per-VALUE and `ColMeta` is advisory — see [`crate::rowmap`] for the
    /// measurement behind that and for the nine §9 tags SQLite cannot produce.
    pub async fn query(
        &self,
        conn: &mut SqliteConn,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, PoolError> {
        let sql = sql.to_string();
        let bound: Vec<rusqlite::types::Value> = params
            .iter()
            .map(crate::rowmap::param_from_value)
            .collect::<Result<_, _>>()?;

        Self::with_conn(conn, move |c| {
            let before_changes = c.total_changes();
            let before_rowid = c.last_insert_rowid();

            let mut stmt = c.prepare(&sql).map_err(|e| {
                tracing::debug!(error = %e, "ferro-backend-sqlite: prepare failed");
                crate::error_map::map(&e)
            })?;
            let ncol = stmt.column_count();
            let names: Vec<String> = (0..ncol)
                .map(|i| stmt.column_name(i).unwrap_or_default().to_string())
                .collect();

            let mut rows_out: Vec<Vec<Value>> = Vec::new();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(bound.iter()))
                .map_err(|e| crate::error_map::map(&e))?;
            while let Some(r) = rows.next().map_err(|e| crate::error_map::map(&e))? {
                let mut row = Vec::with_capacity(ncol);
                for i in 0..ncol {
                    let v = r.get_ref(i).map_err(|e| crate::error_map::map(&e))?;
                    row.push(crate::rowmap::value_from_ref(v));
                }
                rows_out.push(row);
            }
            drop(rows);
            drop(stmt);

            // Advisory only (see `rowmap`): describes the FIRST row's actual storage classes, and
            // NULL for an empty result. It describes the data returned rather than a declared type
            // SQLite does not enforce — and the client drops this field regardless.
            let cols: Vec<ColMeta> = names
                .into_iter()
                .enumerate()
                .map(|(i, name)| ColMeta {
                    name,
                    tag: rows_out
                        .first()
                        .and_then(|r| r.get(i))
                        .map_or(ferro_proto::consts::tag::NULL, Value::tag),
                })
                .collect();

            // Same rule as `simple_query`: the delta decides WHETHER, `changes()` reports HOW MANY.
            let affected = if c.total_changes() == before_changes {
                0
            } else {
                c.changes()
            };

            // `last_insert_rowid()` is STICKY exactly like `changes()` — after a SELECT it still
            // reports the previous INSERT's rowid. So it is reported only when this statement
            // actually MOVED it. A key that is merely absent is an honest `None`; a key carried
            // over from an earlier statement would be silently WRONG, and §22.2 already records
            // (from PG's `lastval()`) that a silently wrong key is strictly worse than none.
            //
            // Residual, stated: an INSERT that explicitly reuses the previous rowid reports `None`
            // rather than that rowid. Conservative in the safe direction.
            let rowid = c.last_insert_rowid();
            let last_insert_id = if rowid != before_rowid && rowid > 0 {
                Some(rowid as u64)
            } else {
                None
            };

            Ok(QueryResult {
                cols,
                rows: rows_out,
                affected,
                last_insert_id,
            })
        })
        .await
    }

    /// Arm or disarm `PRAGMA query_only` for a declared-`readonly` checkout (D13; proven by the
    /// spike's `p5`).
    ///
    /// **Why this exists before anything calls it:** C3-2 shipped `BEGIN DEFERRED` for a declared
    /// `readonly` transaction, which is the spike's `p4a` setup exactly — so a client that declares
    /// `readonly` and then WRITES reaches the one failure class charter rule 3 forbids the engine
    /// resolving. `query_only` converts that into an up-front `SQLITE_READONLY`: deterministic,
    /// provably not executed. The wiring that decides WHEN to arm it is C3-4's `readonly` seam;
    /// this is the mechanism it will call.
    pub async fn set_query_only(&self, conn: &mut SqliteConn, on: bool) -> Result<(), PoolError> {
        if conn.query_only == on {
            return Ok(());
        }
        let sql = if on {
            "PRAGMA query_only=ON"
        } else {
            "PRAGMA query_only=OFF"
        };
        Self::with_conn(conn, move |c| {
            c.execute_batch(sql).map_err(|e| {
                tracing::warn!(error = %e, "ferro-backend-sqlite: query_only pragma failed");
                PoolError::Backend(format!("query_only pragma failed: {e}"))
            })
        })
        .await?;
        conn.query_only = on;
        Ok(())
    }
}

/// Open the database and apply the setup D13 requires. Runs on the blocking pool.
fn open_configured(
    path: &Path,
    busy_timeout: Duration,
    allow_dir: Option<PathBuf>,
) -> Result<Connection, PoolError> {
    let conn = Connection::open(path).map_err(|e| {
        // The path is operator configuration and may sit beside a credential in the same config
        // string, so the error carries the driver message but never the DSN (SPEC §12).
        tracing::warn!(error = %e, "ferro-backend-sqlite: open failed");
        PoolError::ConnectionLost
    })?;

    // WAL is not a preference: D13's whole argument assumes many readers alongside one writer, and
    // the spike's premises were all proven in WAL mode. `journal_mode` RETURNS the mode actually in
    // force, and SQLite can decline the change (a database on a filesystem without shared-memory
    // support stays in rollback mode), so the return value is CHECKED. A silent fallback would mean
    // the engine believed it had reader concurrency it did not have.
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-sqlite: could not set journal_mode");
            PoolError::Backend(format!("journal_mode=WAL failed: {e}"))
        })?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(PoolError::Backend(format!(
            "journal_mode is {mode:?} after requesting WAL; SQLite declined the change, so the \
             reader concurrency D13 assumes is not available on this database"
        )));
    }

    // Kept regardless of D13's outcome — the decision's own note says so. It is what turns the
    // ordinary writer-lock contention that BEGIN IMMEDIATE creates into a bounded wait rather than
    // an immediate failure (the spike's `p4b` measured exactly that).
    conn.busy_timeout(busy_timeout).map_err(|e| {
        tracing::warn!(error = %e, "ferro-backend-sqlite: busy_timeout failed");
        PoolError::Backend(format!("busy_timeout failed: {e}"))
    })?;

    // FOREIGN KEY ENFORCEMENT IS DECLARED HERE, and the reason is that it was already ON without
    // anyone deciding it. C3-3a left the question open ("SQLite defaults `foreign_keys` OFF while
    // Laravel and Doctrine turn it ON") and C3-6a measured the answer through the Doctrine tier: a
    // Ferro SQLite connection reports `PRAGMA foreign_keys = 1` and refuses an orphan INSERT with
    // errno 787, with no pragma anywhere in this file. It comes from the BUILD — `rusqlite`'s
    // `bundled` feature compiles libsqlite3-sys with `SQLITE_DEFAULT_FOREIGN_KEYS=1` — where
    // PHP's `pdo_sqlite`, linked against the system library, reports 0.
    //
    // An engine-wide integrity guarantee that comes from a dependency's compile flag is one
    // `cargo update` away from silently inverting, and nothing in the tree would notice. So it is
    // stated. The VALUE is ON, which changes nothing today and is what both drop-in tiers' worlds
    // expect: Doctrine ships an opt-in `EnableForeignKeys` middleware, Laravel's SQLite connector
    // sets the pragma itself, and — measured — NEITHER can work here, because both apply it on one
    // pooled connection at driver-connect time while the next request is served by another
    // (SPEC §7.4). A per-connection setting that must hold for every tenant belongs at dial, which
    // is the same place and the same reasoning as the MySQL family's `time_zone = '+00:00'` (S7).
    //
    // CHECKED rather than requested, like WAL above: the pragma is a silent no-op if the build
    // lacks foreign-key support altogether, and an integrity guarantee nobody verified is the kind
    // that is discovered by a corrupt row.
    conn.execute_batch("PRAGMA foreign_keys = ON")
        .map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-sqlite: foreign_keys pragma failed");
            PoolError::Backend(format!("foreign_keys=ON failed: {e}"))
        })?;
    let fk: i64 = conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-sqlite: could not read back foreign_keys");
            PoolError::Backend(format!("foreign_keys read-back failed: {e}"))
        })?;
    if fk != 1 {
        return Err(PoolError::Backend(format!(
            "foreign_keys is {fk} after requesting ON; this SQLite build does not enforce foreign \
             keys, so every FK this pool serves would be advisory only"
        )));
    }

    // SPEC D14 — see `install_path_guard`. Installed LAST, so the setup statements above run
    // unguarded: they are engine-composed, and a guard that could refuse the engine's own dial
    // would be a new failure mode for no gain.
    install_path_guard(&conn, path, allow_dir)?;

    Ok(conn)
}

/// **SPEC D14: confine every file the engine opens on a client's behalf to one directory.**
///
/// §12/D8 keeps the database path in the engine so PHP never learns it, and two ordinary SQLite
/// statements hand that choice back: `VACUUM INTO '<path>'` writes a complete copy of the database
/// wherever the daemon can write, and `ATTACH DATABASE '<path>'` opens (and creates) a file the
/// client names. That is a CONFUSED DEPUTY rather than a leak of data the client could not already
/// read: the write happens as **ferrod's** user, which under §18's systemd deployment is not
/// PHP-FPM's, so the client directs an authority it does not itself hold.
///
/// **The mechanism is SQLite's own authorizer, not a statement-text denylist, and the difference is
/// the point.** Refusing `VACUUM INTO` by name was the obvious fix and would have been security
/// theatre — measured, `ATTACH` plus `CREATE TABLE side.copy AS SELECT …` inside one transaction
/// produces the same copy at the same path (§22.2 (bo)). Both verbs arrive here as the SAME event:
/// SQLite reports `SQLITE_ATTACH` with the RESOLVED filename before it opens anything, so this
/// guard covers both, and any future verb that attaches a file, without parsing SQL. Charter rule 6
/// is untouched: nothing is rewritten and nothing is inferred from statement text — SQLite says
/// which file it is about to open and the engine answers.
///
/// MEASURED on the bundled SQLite 3.53.2: `VACUUM INTO` and `ATTACH` both fire
/// `AuthAction::Attach { filename }` carrying the resolved path, and a `Deny` yields `SQLITE_AUTH`
/// (extended 23) with **no file created** — a cleaner refusal than the declared-`readonly` path,
/// which leaves a zero-byte file behind.
fn install_path_guard(
    conn: &Connection,
    db_path: &Path,
    allow_dir: Option<PathBuf>,
) -> Result<(), PoolError> {
    // The DEFAULT is the database's own directory, which is why D14 needs no configuration to be
    // safe: `VACUUM INTO 'snap.db'` beside the database works out of the box, and reaching outside
    // it is an operator decision.
    let root = match allow_dir {
        Some(d) => d,
        None => db_path.parent().map(Path::to_path_buf).unwrap_or_default(),
    };
    // Canonicalised ONCE, here, because the comparison below must not be defeated by `..` or by a
    // symlink — and because a root that does not exist would silently allow nothing, which is a
    // failure worth surfacing at dial rather than on a tenant's first ATTACH.
    let root = root.canonicalize().map_err(|e| {
        tracing::warn!(error = %e, "ferro-backend-sqlite: allow_dir does not resolve");
        PoolError::Backend(format!(
            "the pool's allowed directory for engine-opened files does not resolve: {e}"
        ))
    })?;

    conn.authorizer(Some(move |ctx: AuthContext<'_>| match ctx.action {
        AuthAction::Attach { filename } => {
            if attach_is_allowed(filename, &root) {
                Authorization::Allow
            } else {
                tracing::warn!(
                    filename,
                    root = %root.display(),
                    "ferro-backend-sqlite: refused to open a file outside the pool's allowed directory (SPEC D14)"
                );
                Authorization::Deny
            }
        }
        _ => Authorization::Allow,
    }))
    .map_err(|e| {
        tracing::warn!(error = %e, "ferro-backend-sqlite: could not install the D14 path guard");
        PoolError::Backend(format!("path guard install failed: {e}"))
    })
}

/// Whether SQLite may open `filename` — the D14 rule, factored out so it is unit-testable without
/// a live connection.
///
/// An EMPTY name is allowed: that is how SQLite reports a temporary or in-memory attachment, which
/// writes no file the operator could care about. Everything else must resolve to a path whose
/// PARENT lies within `root` — the parent rather than the file itself, because the target of a
/// snapshot does not exist yet and so cannot be canonicalised.
fn attach_is_allowed(filename: &str, root: &Path) -> bool {
    if filename.is_empty() || filename.eq_ignore_ascii_case(":memory:") {
        return true;
    }
    let candidate = Path::new(filename);
    let absolute = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(candidate),
            // No working directory means no way to resolve a relative name; refuse rather than
            // guess, which is the safe direction for a guard.
            Err(_) => return false,
        }
    };
    let Some(parent) = absolute.parent() else {
        return false;
    };
    match parent.canonicalize() {
        Ok(p) => p.starts_with(root),
        // A parent that does not exist cannot be inside the allowed root by any reading, and
        // SQLite would fail to create the file there anyway.
        Err(_) => false,
    }
}

/// The out-of-band cancel handle (C3-3d). An owned `InterruptHandle`, which `p3` proved is
/// `Send + Sync + 'static` (compile-time) and really ends a running statement with
/// `SQLITE_INTERRUPT` (run-time), so it can be grabbed BEFORE a statement starts and fired from a
/// separate `select!` arm without borrowing the `Checkout` the running statement holds.
///
/// **There is no dial to bound here, and that is the whole difference from PG/MySQL.** Both of
/// those open a SIDE CONNECTION to deliver their cancel, so both bound that dial at a fixed 2 s —
/// otherwise an unreachable backend would stall the teardown the deadline started. SQLite's
/// `sqlite3_interrupt()` is an in-process flag set on a handle this struct already owns: nothing is
/// opened, nothing is awaited, and it returns immediately whether or not a statement is running.
/// Copying that 2 s constant would bound nothing and imply a hazard that does not exist here.
pub struct SqliteCancel {
    handle: rusqlite::InterruptHandle,
}

#[async_trait]
impl Cancel for SqliteCancel {
    async fn cancel(self) {
        // Best-effort and fire-and-forget by contract (charter rule 3): if the statement already
        // finished this is a no-op, and the caller is never promised the statement was interrupted.
        self.handle.interrupt();
    }
}

/// The `PoolBackend` impl (C3-3d).
///
/// **The trait lands here by the line drawn in C3-3b**: it goes in when only the STREAMING pair
/// would be `Unsupported`, which is exactly this slice's end state and precisely the shape the
/// MySQL backend shipped at M1-S6. Every method delegates to the inherent one above, so the
/// inherent surface stays directly testable without a pool.
#[async_trait]
impl PoolBackend for SqliteBackend {
    type Conn = SqliteConn;
    type RowStream = SqliteRowStream;
    type CancelHandle = SqliteCancel;

    /// **True as of C3-5 — flipped in the SAME change as `query_stream`, which is the whole point
    /// of stating it rather than inheriting it.**
    ///
    /// `ferrod`'s EXEC handler reads exactly this one method as the single authority for the
    /// streaming capability (M1-S8a), so that a `fetch:stream` against a backend that cannot stream
    /// is refused EARLY — before any checkout — instead of surfacing part-way through a result set
    /// the client has already begun consuming. C3-3e set it to `false` because inheriting the
    /// trait's `true` default would have turned that clean refusal into a mid-stream error the
    /// moment a SQLite pool became constructible (§22.2 (bh)); the same reasoning is why the two
    /// move together now, and `streaming_capability_agrees_with_query_stream` is the test that says
    /// so out loud rather than leaving it to a reader's memory.
    fn supports_row_streaming(&self) -> bool {
        true
    }

    /// **SQLite is the first backend for which this is not a no-op** (SPEC D13, C3-4), and it is
    /// what finally gives [`SqliteBackend::set_query_only`] a caller.
    ///
    /// A declared-`readonly` checkout is armed `PRAGMA query_only=ON`, so a client that declares
    /// `readonly` and then WRITES is refused up front with `SQLITE_READONLY` (8) instead of being
    /// believed. That matters because the same declaration is trusted elsewhere: `fate.rs` uses it
    /// to suppress `Indeterminate` (§19.3), and under D13 it chooses `BEGIN DEFERRED`, which is
    /// `p4a`'s exact unretryable setup. The C3-1 spike's `p5` proved the conversion — deterministic,
    /// provably not executed, and with no contention at all, so it is a property of the connection
    /// rather than a race that happened to be won.
    ///
    /// Disarming is NOT done here. `reset` clears it for the next tenant (C3-3b's explicit 4-item
    /// list), which is the right owner: a pragma armed for one tenant and cleared by the hygiene
    /// that runs for the next is the same shape as every other piece of session state, and making
    /// this method responsible for both would put the cross-tenant guarantee in the hands of
    /// whichever caller happened to check out next.
    async fn apply_readonly(&self, conn: &mut Self::Conn, readonly: bool) -> Result<(), PoolError> {
        self.set_query_only(conn, readonly).await
    }

    fn cancel_handle(&self, conn: &Self::Conn) -> Self::CancelHandle {
        // A connection with no live handle still yields a handle-shaped value the pool can hold;
        // there is nothing to interrupt, and firing it is the documented no-op.
        SqliteCancel {
            handle: conn
                .driver()
                .map(rusqlite::Connection::get_interrupt_handle)
                .unwrap_or_else(|| {
                    // Unreachable in practice (a caller holds `&mut` across every blocking window,
                    // and a handle-less conn is `is_closed`), but the trait method is infallible,
                    // so a throwaway in-memory handle keeps it total rather than panicking.
                    rusqlite::Connection::open_in_memory()
                        .expect("in-memory open for a no-op cancel handle")
                        .get_interrupt_handle()
                }),
        }
    }

    async fn connect(&self) -> Result<Self::Conn, PoolError> {
        SqliteBackend::connect(self).await
    }

    async fn ping(&self, conn: &mut Self::Conn) -> Result<(), PoolError> {
        SqliteBackend::ping(self, conn).await
    }

    fn is_closed(&self, conn: &Self::Conn) -> bool {
        SqliteBackend::is_closed(self, conn)
    }

    fn dialect(&self) -> Dialect {
        SqliteBackend::dialect(self)
    }

    fn tx_status(&self, conn: &Self::Conn) -> TxStatus {
        SqliteBackend::tx_status(self, conn)
    }

    async fn reset(&self, conn: &mut Self::Conn, profile: ResetProfile) -> Result<(), PoolError> {
        SqliteBackend::reset(self, conn, profile).await
    }

    fn clean_reset_profile(&self) -> Option<ResetProfile> {
        SqliteBackend::clean_reset_profile(self)
    }

    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError> {
        SqliteBackend::simple_query(self, conn, sql).await
    }

    async fn query(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, PoolError> {
        SqliteBackend::query(self, conn, sql, params).await
    }

    /// C3-5. The connection is MOVED into a blocking task that owns it for the stream's life —
    /// forced, not chosen: `Statement` and `Rows` borrow the `Connection`, and `Connection` is
    /// `Send` but not `Sync`. That makes SQLite a conn-owning backend in exactly MySQL's sense, so
    /// `reclaim_stream` below is mandatory rather than optional. See `crate::stream`.
    async fn query_stream(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        params: &[Value],
    ) -> Result<(Vec<ColMeta>, Self::RowStream), PoolError> {
        crate::stream::start(conn, sql, params).await
    }

    /// Put the connection back and answer the counters from IT, not from the stream (SPEC §22.2
    /// (n)'s measured rule, which holds here for SQLite's own reason: `changes()` reports the last
    /// data-modifying statement and goes stale, so anything read before the statement finished
    /// would be the previous one's).
    ///
    /// The `Err` contract is satisfied by construction: the handle is restored only on the success
    /// arm, so a producer that panicked or vanished leaves `SqliteConn` handle-less and `is_closed`
    /// reports it dead, which makes the pool discard the husk rather than recycle a mid-result-set
    /// session (charter rule 6).
    async fn reclaim_stream(
        &self,
        conn: &mut Self::Conn,
        rows: Self::RowStream,
    ) -> Result<Reclaimed, PoolError> {
        crate::stream::reclaim(conn, rows).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_backend() -> (tempfile::TempDir, SqliteBackend) {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = SqliteBackend::new(format!("sqlite://{}", dir.path().join("t.db").display()));
        (dir, backend)
    }

    /// **The FB-2 contract: a lost connection must read closed, never be handed on.**
    ///
    /// A blocking closure that panics OWNS the `Connection` and takes it down with it. The wrapper
    /// left behind must report `is_closed` so the pool DISCARDS it — the alternative is handing the
    /// next tenant a husk, the cross-tenant hazard charter rule 6 exists to prevent. `MysqlConn`
    /// carries the same contract for its parked window.
    ///
    /// **Honest about what this test does and does not establish.** It confirms the observable
    /// behaviour, but the contract holds BY CONSTRUCTION rather than by any statement this test
    /// could delete: `park` moves the handle into the task, and the error arm never unparks it. An
    /// explicit `lose_handle()` call once sat in that arm; removing it left this test GREEN, which
    /// is how it was found to be dead code, and it is now gone. So read this as a regression guard
    /// on the behaviour, not as proof that some particular line is load-bearing — the same
    /// distinction C3-1's non-buffering proof had to learn.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_blocking_call_marks_the_connection_dead() {
        let (_dir, backend) = temp_backend();
        let mut conn = backend.connect().await.expect("connect");
        assert!(!backend.is_closed(&conn), "healthy before");

        let out: Result<(), PoolError> =
            SqliteBackend::with_conn(&mut conn, |_c| panic!("blocking work panicked")).await;

        assert!(
            matches!(out, Err(PoolError::ConnectionLost)),
            "a lost connection reports ConnectionLost, got {out:?}"
        );
        assert!(
            backend.is_closed(&conn),
            "THE CONTRACT: the wrapper must read closed so the pool discards it rather than \
             handing on a husk"
        );
    }

    /// `tx_status` on a connection whose handle was LOST reports `Failed` — the one place it can,
    /// and reachable only from inside the crate because losing the handle needs the private
    /// blocking bridge. `Failed` here is the absence of a signal, not a SQLite signal: SQLite has
    /// no aborted-open-transaction state (§7.1), and the enum offers no "unknown".
    #[tokio::test(flavor = "multi_thread")]
    async fn tx_status_on_a_lost_handle_is_failed() {
        let (_dir, backend) = temp_backend();
        let mut conn = backend.connect().await.expect("connect");
        assert_eq!(backend.tx_status(&conn), TxStatus::Idle, "healthy before");

        let _ = SqliteBackend::with_conn(&mut conn, |_c| -> Result<(), PoolError> {
            panic!("lose the handle")
        })
        .await;

        assert_eq!(
            backend.tx_status(&conn),
            TxStatus::Failed,
            "with no handle there is nothing to read, so the safest answer the enum offers"
        );
    }

    /// And a second call on the dead wrapper fails cleanly instead of panicking on an unwrap.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_call_on_a_dead_connection_is_a_clean_error() {
        let (_dir, backend) = temp_backend();
        let mut conn = backend.connect().await.expect("connect");
        let _ = SqliteBackend::with_conn(&mut conn, |_c| -> Result<(), PoolError> {
            panic!("lose the handle")
        })
        .await;

        let again = backend.ping(&mut conn).await;
        assert!(
            matches!(again, Err(PoolError::Closed)),
            "a call on a husk is a clean Closed, never an unwrap panic: {again:?}"
        );
    }
}
