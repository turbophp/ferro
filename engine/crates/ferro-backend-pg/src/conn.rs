//! `PgConn` + `PgBackend`: the `ferro_pool::backend::PoolBackend` impl over `tokio-postgres`.
//!
//! No TLS in M0 (decision — `NoTls` only; TLS lands post-M0). `tokio_postgres::connect` returns a
//! `(Client, Connection)` pair where `Connection` is the actual I/O driver: it MUST be polled
//! (via `tokio::spawn`) for the client to make progress at all. When that driver future resolves
//! (EOF, a killed backend, a network error, ...) the connection is dead — we flip an `AtomicBool`
//! so the cheap, synchronous `is_closed()` check can see it without a round trip.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use ferro_pool::backend::{Cancel, PoolBackend};
use ferro_pool::error::PoolError;

/// A pooled Postgres connection.
///
/// `client` is `pub`: the pool-internal surface (`ping`/`reset`/`simple_query`) only reports
/// success/failure, never row data, but callers that hold a `Checkout<PgBackend>` (integration
/// tests here, and the SQL EXEC service in S5) need to run real queries and read results (e.g.
/// `SELECT pg_backend_pid()`) — so the raw `tokio_postgres::Client` is reachable via
/// `Checkout::conn()`/`conn_mut()`.
///
/// Deliberately does NOT duplicate `created_at`/`tx_open` bookkeeping: `ferro-pool`'s
/// `Checkout`/`IdleConn` already track both at the pool layer (see `ferro_pool::pool`), driven by
/// `Instant::now()` at connect time and by `Checkout::set_tx_open`/the pin hook, independent of
/// anything on `B::Conn`. A duplicate flag here would be write-only/dead code under
/// `clippy -D warnings`.
pub struct PgConn {
    pub client: tokio_postgres::Client,
    /// Flipped to `true` by the spawned connection-driver task once the `Connection` future
    /// resolves — i.e. the connection ended, for any reason (clean shutdown, a killed backend,
    /// a network error). Read by `is_closed()` alongside `client.is_closed()` (belt-and-braces:
    /// the driver task setting this is itself async and can lag a query's own error by a tick).
    closed: Arc<AtomicBool>,
}

/// `PoolBackend` impl over a single Postgres DSN/URL. No TLS in M0.
pub struct PgBackend {
    url: String,
}

impl PgBackend {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

/// A local newtype around `tokio_postgres::CancelToken` so this crate can implement ferro-pool's
/// [`Cancel`] trait for it (the orphan rule forbids `impl Cancel for tokio_postgres::CancelToken`
/// directly — both are foreign to this crate).
pub struct PgCancel(pub tokio_postgres::CancelToken);

/// Fire the out-of-band statement cancel over a SIDE connection (S6). `CancelToken::cancel_query`
/// opens its own short-lived connection using the captured backend key data, so it can run while
/// the pinned connection's own query future still holds `&mut Client`. Best-effort: a failure
/// (server already gone, or the statement finished first) is logged and swallowed — the actor tears
/// the transaction down regardless and NEVER re-runs the statement (charter rule 3).
#[async_trait]
impl Cancel for PgCancel {
    async fn cancel(self) {
        if let Err(e) = self.0.cancel_query(tokio_postgres::NoTls).await {
            tracing::debug!(error = %e, "ferro-backend-pg: out-of-band cancel_query failed (best-effort)");
        }
    }
}

#[async_trait]
impl PoolBackend for PgBackend {
    type Conn = PgConn;
    type CancelHandle = PgCancel;

    /// The out-of-band cancel handle (S6): `tokio_postgres::Client::cancel_token` captures this
    /// connection's backend key data into a `Send + 'static` `CancelToken`. Cancelling it runs
    /// `CancelToken::cancel_query` over a SIDE connection, so it can fire while the connection's own
    /// query future is still live (which holds `&mut Client`) — the engine grabs it BEFORE starting
    /// an interruptible statement.
    fn cancel_handle(&self, conn: &Self::Conn) -> Self::CancelHandle {
        PgCancel(conn.client.cancel_token())
    }

    async fn connect(&self) -> Result<Self::Conn, PoolError> {
        let (client, connection) = tokio_postgres::connect(&self.url, tokio_postgres::NoTls)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "ferro-backend-pg: connect failed");
                PoolError::ConnectionLost
            })?;

        let closed = Arc::new(AtomicBool::new(false));
        let closed_for_driver = Arc::clone(&closed);
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "ferro-backend-pg: connection driver ended with error");
            }
            // The driver future only resolves once the connection is done for good (clean or
            // not) — either way, this connection is no longer usable.
            closed_for_driver.store(true, Ordering::SeqCst);
        });

        Ok(PgConn { client, closed })
    }

    async fn ping(&self, conn: &mut Self::Conn) -> Result<(), PoolError> {
        // A round trip, not just a local flag check — this is what catches a backend killed out
        // from under an otherwise idle-looking connection (v2/M4).
        conn.client
            .simple_query("SELECT 1")
            .await
            .map(|_| ())
            .map_err(|e| {
                tracing::debug!(error = %e, "ferro-backend-pg: ping (round trip) failed");
                PoolError::ConnectionLost
            })
    }

    fn is_closed(&self, conn: &Self::Conn) -> bool {
        conn.closed.load(Ordering::SeqCst) || conn.client.is_closed()
    }

    async fn reset(&self, conn: &mut Self::Conn) -> Result<(), PoolError> {
        conn.client.batch_execute("DISCARD ALL").await.map_err(|e| {
            tracing::warn!(error = %e, "ferro-backend-pg: reset (DISCARD ALL) failed");
            PoolError::ConnectionLost
        })
    }

    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError> {
        conn.client.batch_execute(sql).await.map(|_| 0u64).map_err(|e| {
            if is_session_fatal(&e) {
                tracing::warn!(
                    error = %e,
                    "ferro-backend-pg: simple_query hit a session-ending failure (connection considered lost)"
                );
                PoolError::ConnectionLost
            } else {
                PoolError::Backend(e.to_string())
            }
        })
    }

    async fn query(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        params: &[crate::Value],
    ) -> Result<ferro_pool::backend::QueryResult, PoolError> {
        crate::query::run(&conn.client, sql, params).await
    }
}

/// Whether a `tokio_postgres::Error` means "this session is over" (connection lost) rather than
/// "this statement was rejected". Shared by `simple_query` (S4) and `error_map` (S5), which is why
/// it lives here as a named helper (v2/M4 + MAJOR-9). Two cases are session-fatal, neither caught
/// by `Error::is_closed()` alone (that only trips once the connection-driver task has dropped its
/// receiver — a tick behind a round trip's own error):
///   1. No DB error at all (`as_db_error() == None`): a closed request channel, an I/O error on
///      the socket mid-flight, a protocol violation — the transport itself failed.
///   2. A well-formed DB error whose severity is FATAL/PANIC: Postgres itself ended the session
///      (`pg_terminate_backend`, an idle-session/admin disconnect, a crash) and says so over the
///      wire before closing the socket, so it still parses as a `DbError` — but FATAL/PANIC means
///      "this session is over", never "retry on the same connection".
///
/// Anything else (severity ERROR/WARNING/…) is a statement-level failure; the connection is fine.
pub(crate) fn is_session_fatal(e: &tokio_postgres::Error) -> bool {
    match e.as_db_error() {
        None => true,
        Some(db_err) => matches!(
            db_err.parsed_severity(),
            Some(tokio_postgres::error::Severity::Fatal | tokio_postgres::error::Severity::Panic)
        ),
    }
}
