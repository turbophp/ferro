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

use ferro_pool::backend::{Dialect, QueryResult, ResetProfile, TxStatus};
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;
use rusqlite::Connection;

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
}

/// The SQLite arm of the `PoolBackend` seam. One backend, one database file.
pub struct SqliteBackend {
    dsn: String,
    busy_timeout: Duration,
}

impl SqliteBackend {
    /// Infallible, matching `PgBackend::new`/`MysqlBackend::new`: the DSN is parsed per-connect, so
    /// a bad one surfaces as a connect failure exactly as it does on the other two backends.
    pub fn new(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
        }
    }

    /// Override the busy timeout — see [`DEFAULT_BUSY_TIMEOUT`] for the bound this should carry and
    /// the C3-3e wiring debt.
    pub fn with_busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.busy_timeout = busy_timeout;
        self
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

        let conn = tokio::task::spawn_blocking(move || open_configured(&path, busy_timeout))
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
    /// `Some(Full)` — and unlike PostgreSQL that costs nothing. PG uses `Targeted` to avoid
    /// `DISCARD ALL`'s `DEALLOCATE ALL`, which would destroy its prepared statements; SQLite's
    /// reset is in-process against a local file, issues no round trip, and destroys no prepared
    /// statements, so there is no cheaper profile to choose and nothing is bought by choosing one.
    ///
    /// `None` (skip hygiene entirely) is deliberately NOT taken even though SQLite has no stored
    /// procedures and therefore none of the §7.4 function-body blind spot that motivated PG's
    /// backstop. That is an optimization, it is the same one still deferred for MySQL (R2), and
    /// nothing here is expensive enough to justify the risk.
    pub fn clean_reset_profile(&self) -> Option<ResetProfile> {
        Some(ResetProfile::Full)
    }

    /// Hygiene reset. **Both profiles do the same thing, stated rather than hidden:** see
    /// [`SqliteBackend::clean_reset_profile`] — there is no cheaper-but-weaker reset for SQLite to
    /// offer, so a `Targeted` that differed from `Full` would be a distinction with no behaviour
    /// behind it.
    ///
    /// SQLite has **no `DISCARD ALL` analogue**, so this is an explicit list rather than a ported
    /// one. What a pooled SQLite connection can carry into the next tenant:
    ///
    /// 1. **An open transaction** — the floor. Gated on the live signal, because `ROLLBACK` errors
    ///    with "cannot rollback - no transaction is active" when none is open.
    /// 2. **`PRAGMA query_only`**, if a declared-`readonly` checkout armed it (C3-3a).
    /// 3. **ATTACHed databases** — §7.1's assist lexer already names `ATTACH` pin-worthy, which is
    ///    exactly the admission that it leaves connection state behind.
    /// 4. **Temp objects** — `CREATE TEMP TABLE` lives in the per-connection `temp` schema and
    ///    survives until the connection closes, so it outlives the checkout that made it.
    pub async fn reset(
        &self,
        conn: &mut SqliteConn,
        _profile: ResetProfile,
    ) -> Result<(), PoolError> {
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
                PoolError::Backend(format!("{e}"))
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
                PoolError::Backend(format!("{e}"))
            })?;
            let ncol = stmt.column_count();
            let names: Vec<String> = (0..ncol)
                .map(|i| stmt.column_name(i).unwrap_or_default().to_string())
                .collect();

            let mut rows_out: Vec<Vec<Value>> = Vec::new();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(bound.iter()))
                .map_err(|e| PoolError::Backend(format!("{e}")))?;
            while let Some(r) = rows
                .next()
                .map_err(|e| PoolError::Backend(format!("{e}")))?
            {
                let mut row = Vec::with_capacity(ncol);
                for i in 0..ncol {
                    let v = r
                        .get_ref(i)
                        .map_err(|e| PoolError::Backend(format!("{e}")))?;
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
fn open_configured(path: &Path, busy_timeout: Duration) -> Result<Connection, PoolError> {
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

    Ok(conn)
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
