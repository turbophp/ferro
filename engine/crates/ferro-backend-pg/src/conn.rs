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
use ferro_pool::backend::PoolBackend;
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

#[async_trait]
impl PoolBackend for PgBackend {
    type Conn = PgConn;

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
            // Tell "the connection itself is dead" apart from "the statement was rejected".
            // Two cases surface as ConnectionLost, neither of which `Error::is_closed()` alone
            // catches on its own (that only trips once the connection driver task has already
            // dropped its receiver, which lags a round trip's own error by a tick — the exact
            // race v2/M4 flags):
            //   1. No DB error at all (`as_db_error() == None`): a closed request channel, an
            //      I/O error reading/writing the socket mid-flight, a protocol violation, etc —
            //      the transport itself failed.
            //   2. A well-formed DB error whose severity is FATAL/PANIC: Postgres itself ended
            //      the session (e.g. `pg_terminate_backend`, an idle-session/admin disconnect,
            //      a crash) and says so over the wire *before* closing the socket — so this
            //      still parses as a `DbError`, but per Postgres's own semantics FATAL/PANIC
            //      means "this session is over", never "retry within the same connection".
            // Anything else (severity ERROR/WARNING/...) is a genuine statement-level failure;
            // the connection itself is still fine — `PoolError::Backend` (NonRetryable).
            let is_session_fatal = match e.as_db_error() {
                None => true,
                Some(db_err) => matches!(
                    db_err.parsed_severity(),
                    Some(tokio_postgres::error::Severity::Fatal | tokio_postgres::error::Severity::Panic)
                ),
            };
            if is_session_fatal {
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
}
