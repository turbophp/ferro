//! `MysqlConn` + `MysqlBackend`: the `ferro_pool::backend::PoolBackend` impl over the vendored
//! (`CLIENT_SESSION_TRACK`) `mysql_async` fork. The MySQL/MariaDB counterpart of `ferro-backend-pg`'s
//! `conn.rs`, at `PoolBackend` parity (M1-S6 Task 3).
//!
//! No TLS in M1 (like the PG backend's `NoTls`): the daemon dials a local MySQL/MariaDB over a
//! plaintext transport. MySQL 8.4 defaults to `caching_sha2_password`; the vendored `mysql_async`
//! negotiates it fine over a plaintext local connection (the DSN must NOT force
//! `mysql_native_password`).
//!
//! ## The two pin signals (SPEC §7.1)
//!
//! * [`MysqlBackend::tx_status`] — the transaction AUTHORITY — reads `SERVER_STATUS_IN_TRANS` off the
//!   last OK packet (see [`crate::tracker`]). It NEVER returns [`TxStatus::Failed`].
//! * [`MysqlBackend::take_session_mutated`] — the ASSIST taint — drains a per-lease `session_mutated`
//!   flag that the leaf statement-runners ([`MysqlBackend::simple_query`], and `query` in Task 4) set
//!   from the OK-packet session trackers. It is BASELINED at connect (the handshake/setup SETs never
//!   count) and read-and-cleared, so it reports one mutation exactly once per lease.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use ferro_pool::backend::{
    BackendRows, Cancel, Dialect, PoolBackend, QueryResult, ResetProfile, TxStatus,
};
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder};

use crate::{error_map, tracker};

/// The curated `session_track_system_variables` list — deliberately NOT `'*'` (the Task-1 spike
/// found `'*'` fires benign trackers like `statement_id` that would taint every connection). Mirrors
/// the testkit's `docker-compose.yml`/`mysql-init.sql` and INCLUDES `sort_buffer_size` (so the
/// stored-program fixture emits a `SystemVariables` tracker) and `autocommit` (so a user's own
/// autocommit change is visible — Ferro's own toggle is filtered by the tracker allowlist, not by
/// hiding it from tracking).
const CURATED_SESSION_TRACK_VARS: &str =
    "autocommit,sql_mode,time_zone,sort_buffer_size,foreign_key_checks,unique_checks";

/// A pooled MySQL/MariaDB connection: the driver handle + the per-lease session-mutation flag + a
/// synchronous "obviously dead" flag + the connect `Opts` (so the out-of-band `KILL QUERY` cancel
/// can open a SIDE connection with the same creds, borrowing nothing from this conn).
pub struct MysqlConn {
    /// The vendored `mysql_async` connection. `pub` for the same reason PG's `client` is: the
    /// pool-internal surface only reports success/failure, but a `Checkout<MysqlBackend>` holder
    /// (integration tests, the SQL EXEC service) needs the raw handle for real queries. The same
    /// CONTRACT applies — running statements directly BYPASSES the pin authority; go through the
    /// instrumented `Checkout` methods for anything that must be tracked.
    pub mysql: Conn,
    /// The fully-built connect `Opts` (creds + the session-tracker setup commands). Cloned into a
    /// [`MysqlCancel`] so the side-connection cancel is borrow-free and `Send + 'static`.
    opts: Opts,
    /// The per-lease session-mutation taint (SPEC §7.1). Set `true` by the leaf statement-runners
    /// when the OK-packet trackers report a real mutation; read-and-cleared by
    /// [`MysqlBackend::take_session_mutated`]. BASELINED to `false` at connect so the handshake/setup
    /// SETs never count as a per-lease mutation.
    session_mutated: bool,
    /// Flipped `true` when a transport/driver failure is observed on this conn (the analog of PG's
    /// spawned-driver `AtomicBool`; MySQL has no separate driver task, so we set it ourselves on a
    /// session-fatal error). Read by [`MysqlBackend::is_closed`] alongside the driver's own
    /// `Conn::is_disconnected`.
    closed: AtomicBool,
}

impl MysqlConn {
    /// Record the last statement's session-tracker verdict into the per-lease flag (§7.1). Additive
    /// (`|=`): once tainted, a subsequent benign statement never un-taints within the lease. Called
    /// by the leaf statement-runners AFTER the result drains (`last_ok_packet` is post-drain).
    fn record_session_mutation(&mut self) {
        let mutated = tracker::ok_reports_session_mutation(self.mysql.last_ok_packet());
        self.session_mutated |= mutated;
    }
}

/// `PoolBackend` impl over a single MySQL/MariaDB DSN (M1-S6). No TLS in M1.
pub struct MysqlBackend {
    /// The `mysql://` DSN dialed by [`MysqlBackend::connect`]. Parsed per-connect (like PG holding a
    /// URL string) so `new` stays infallible and matches `PgBackend::new`'s surface.
    url: String,
}

impl MysqlBackend {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

/// The out-of-band cancel handle (S6): an OWNED `(connection id, connect opts)` pair — borrows
/// NOTHING from the live conn (whose in-flight query future holds `&mut Conn`), so it is
/// `Send + 'static` and can fire from a separate `select!` arm. The MySQL analog of `PgCancel`:
/// where Postgres cancels via a `CancelToken` over a side connection, MySQL opens a fresh side
/// connection (same creds) and runs `KILL QUERY <id>`.
pub struct MysqlCancel {
    conn_id: u32,
    opts: Opts,
}

#[async_trait]
impl Cancel for MysqlCancel {
    async fn cancel(self) {
        // Open a SIDE connection with the same creds and KILL QUERY the pinned conn's in-flight
        // statement. Best-effort, fire-and-forget (charter rule 3): any failure — the statement
        // already finished, the server is gone — is swallowed; the actor tears the tx down
        // regardless and NEVER re-runs the statement.
        match Conn::new(self.opts).await {
            Ok(mut side) => {
                if let Err(e) = side
                    .query_drop(format!("KILL QUERY {}", self.conn_id))
                    .await
                {
                    tracing::debug!(
                        error = %e, conn_id = self.conn_id,
                        "ferro-backend-mysql: KILL QUERY failed (best-effort)"
                    );
                }
                let _ = side.disconnect().await;
            }
            Err(e) => tracing::debug!(
                error = %e, conn_id = self.conn_id,
                "ferro-backend-mysql: cancel side-connection failed (best-effort)"
            ),
        }
    }
}

/// Placeholder incremental row stream — MySQL streaming is a later slice (M1-S7), so
/// [`MysqlBackend::query_stream`] returns `PoolError::Unsupported` and this type is NEVER
/// constructed. Exists only to satisfy the `PoolBackend::RowStream: BackendRows` bound.
pub struct MysqlRowStream;

#[async_trait]
impl BackendRows for MysqlRowStream {
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>> {
        unreachable!("MysqlRowStream is a compile-time placeholder; MySQL streaming lands in M1-S7")
    }

    fn rows_affected(&self) -> u64 {
        unreachable!("MysqlRowStream is a compile-time placeholder; MySQL streaming lands in M1-S7")
    }
}

#[async_trait]
impl PoolBackend for MysqlBackend {
    type Conn = MysqlConn;
    type RowStream = MysqlRowStream;
    type CancelHandle = MysqlCancel;

    /// Owned, borrow-free cancel handle (S6): captures the server-side connection id + the connect
    /// opts so [`MysqlCancel::cancel`] can `KILL QUERY` over a side connection while this conn's own
    /// query future is still live.
    fn cancel_handle(&self, conn: &Self::Conn) -> Self::CancelHandle {
        MysqlCancel {
            conn_id: conn.mysql.id(),
            opts: conn.opts.clone(),
        }
    }

    async fn connect(&self) -> Result<Self::Conn, PoolError> {
        let base = Opts::from_url(&self.url).map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-mysql: invalid DSN");
            PoolError::ConnectionLost
        })?;

        // Ensure the curated session trackers are in force as SETUP commands (the fork negotiates
        // CLIENT_SESSION_TRACK at handshake; these guarantee the tracked-var list + state/transaction
        // trackers even against a server without the globals). `mysql_async` re-runs setup commands
        // after COM_RESET_CONNECTION too, so a recycled conn keeps them. NOT `'*'` (see the const).
        let opts: Opts = OptsBuilder::from_opts(base)
            .setup(vec![
                "SET SESSION session_track_state_change = ON".to_string(),
                "SET SESSION session_track_transaction_info = 'STATE'".to_string(),
                format!(
                    "SET SESSION session_track_system_variables = '{CURATED_SESSION_TRACK_VARS}'"
                ),
            ])
            .into();

        let mysql = Conn::new(opts.clone()).await.map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-mysql: connect failed");
            PoolError::ConnectionLost
        })?;

        // BASELINE (SPEC §7.1): the connect/handshake/setup SETs (incl. anything the driver applies
        // at handshake — autocommit/sql_mode/time_zone) NEVER went through `record_session_mutation`,
        // so `session_mutated` is a clean `false` here — a fresh conn is truly clean for the FIRST
        // per-lease measurement. Set explicitly to document the invariant.
        Ok(MysqlConn {
            mysql,
            opts,
            session_mutated: false,
            closed: AtomicBool::new(false),
        })
    }

    async fn ping(&self, conn: &mut Self::Conn) -> Result<(), PoolError> {
        // A real COM_PING round trip (catches a backend killed out from under an idle conn).
        let res = conn.mysql.ping().await;
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                conn.closed.store(true, Ordering::SeqCst);
                tracing::debug!(error = %e, "ferro-backend-mysql: ping (round trip) failed");
                Err(PoolError::ConnectionLost)
            }
        }
    }

    fn is_closed(&self, conn: &Self::Conn) -> bool {
        // Our own transport-failure flag OR the driver's own disconnected state (there is no separate
        // spawned driver task to mirror, unlike PG — mysql_async tracks disconnection internally).
        conn.closed.load(Ordering::SeqCst) || conn.mysql.is_disconnected()
    }

    /// This backend always speaks MySQL — a per-backend constant (the assist lexer keys off it).
    fn dialect(&self) -> Dialect {
        crate::DIALECT
    }

    /// The transaction AUTHORITY (SPEC §7.1): reads `SERVER_STATUS_IN_TRANS` off the last OK packet.
    /// NEVER returns `Failed` — MySQL/MariaDB have no aborted-open-tx state (see [`crate::tracker`]).
    fn tx_status(&self, conn: &Self::Conn) -> TxStatus {
        tracker::tx_status_from_ok(conn.mysql.last_ok_packet())
    }

    /// The ASSIST taint (SPEC §7.1): read-and-clear the per-lease session-mutation flag. Reported
    /// exactly once per statement (the leaf runners set it post-drain; this drains it).
    fn take_session_mutated(&self, conn: &mut Self::Conn) -> bool {
        let m = conn.session_mutated;
        conn.session_mutated = false;
        m
    }

    /// Hygiene reset (SPEC §7.2). MySQL has no cheaper "targeted" reset than `COM_RESET_CONNECTION`
    /// (there is no `DISCARD ALL`-minus-prepares equivalent — MySQL prepared statements are
    /// connection-scoped and cheap to re-prepare), so BOTH profiles run `COM_RESET_CONNECTION`.
    /// Since [`clean_reset_profile`](MysqlBackend::clean_reset_profile) is `Some(Full)` for now, the
    /// pool only ever requests `Full` for a clean recycled conn; the `Targeted` arm is defined (not
    /// `unreachable!`) so it can never panic if a caller does request it.
    async fn reset(&self, conn: &mut Self::Conn, profile: ResetProfile) -> Result<(), PoolError> {
        // `Conn::reset` sends COM_RESET_CONNECTION, clears the stmt cache, and re-runs our setup
        // commands (the session-tracker SETs), returning the whole session to a clean baseline.
        let res = conn.mysql.reset().await;
        match res {
            Ok(_) => {
                // A clean baseline again — clear the per-lease taint (belt-and-braces; the pool has
                // already drained it via take_session_mutated before deciding to reset).
                conn.session_mutated = false;
                Ok(())
            }
            Err(e) => {
                conn.closed.store(true, Ordering::SeqCst);
                tracing::warn!(error = %e, profile = ?profile, "ferro-backend-mysql: reset failed");
                Err(PoolError::ConnectionLost)
            }
        }
    }

    /// A non-tainted recycled MySQL conn currently gets the conservative `Full` profile
    /// (`COM_RESET_CONNECTION`). The tracker-clean `None` skip — provably safe once the OK-packet
    /// tracker is shown to fire for EVERY §7.1 mutation class inside stored programs — is switched on
    /// in Task 7 (after the hard gate). Until then `Some(Full)` is the correct conservative backstop.
    fn clean_reset_profile(&self) -> Option<ResetProfile> {
        Some(ResetProfile::Full)
    }

    /// Raw single-round-trip `COM_QUERY` (UNGUARDED) — used by the pin hook (BEGIN/COMMIT/ROLLBACK,
    /// Task 4) and internal reset. Drains the result, records the §7.1 session-mutation taint from
    /// the resulting OK packet, and returns the affected-row count. Failures route through
    /// `error_map` session-fatal-FIRST (exactly like PG's `simple_query`), so a serialization
    /// failure/deadlock caught AT COMMIT survives as `Sql{Retryable}`, while a transport failure is
    /// the distinct `ConnectionLost` variant.
    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError> {
        match conn.mysql.query_drop(sql).await {
            Ok(()) => {
                // Post-drain: the OK packet now carries the status flag + session trackers.
                conn.record_session_mutation();
                Ok(conn.mysql.affected_rows())
            }
            Err(e) => {
                if error_map::is_session_fatal(&e) {
                    conn.closed.store(true, Ordering::SeqCst);
                    tracing::warn!(
                        error = %e,
                        "ferro-backend-mysql: simple_query hit a session-ending failure (connection considered lost)"
                    );
                }
                Err(error_map::map(&e))
            }
        }
    }

    async fn query(
        &self,
        _conn: &mut Self::Conn,
        _sql: &str,
        _params: &[Value],
    ) -> Result<QueryResult, PoolError> {
        // The buffered row-returning path (with param bind + rowmap + the full error_map) lands in
        // Task 4. It MUST, after draining, call `conn.record_session_mutation()` — the same §7.1
        // taint the leaf `simple_query` records — so a mutation on the row-returning path also taints.
        todo!("MySQL row-returning query lands in M1-S6 Task 4")
    }

    async fn query_stream(
        &self,
        _conn: &mut Self::Conn,
        _sql: &str,
        _params: &[Value],
    ) -> Result<(Vec<ColMeta>, Self::RowStream), PoolError> {
        // MySQL streaming is a later slice (M1-S7); the buffered `query` path is what M1-S6 delivers.
        Err(PoolError::Unsupported(
            "MySQL streaming lands in M1-S7".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `MysqlBackend::dialect()` is a pure, synchronous constant — no live MySQL needed (parity with
    /// `PgBackend`'s `dialect_is_postgres`).
    #[test]
    fn dialect_is_mysql() {
        let backend = MysqlBackend::new("mysql://unused/unused");
        assert_eq!(backend.dialect(), Dialect::MySql);
    }

    /// The conservative clean-reset profile until Task 7 proves the tracker-clean skip.
    #[test]
    fn clean_reset_profile_is_full() {
        let backend = MysqlBackend::new("mysql://unused/unused");
        assert_eq!(backend.clean_reset_profile(), Some(ResetProfile::Full));
    }
}
