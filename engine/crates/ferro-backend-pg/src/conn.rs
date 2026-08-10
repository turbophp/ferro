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
use ferro_pool::backend::{Cancel, Dialect, PoolBackend, ResetProfile, TxStatus};
use ferro_pool::error::PoolError;

/// A pooled Postgres connection.
///
/// `client` is `pub`: the pool-internal surface (`ping`/`reset`/`simple_query`) only reports
/// success/failure, never row data, but callers that hold a `Checkout<PgBackend>` (integration
/// tests here, and the SQL EXEC service in S5) need to run real queries and read results (e.g.
/// `SELECT pg_backend_pid()`) — so the raw `tokio_postgres::Client` is reachable via
/// `Checkout::conn()`/`conn_mut()`.
///
/// CONTRACT (see `Checkout::conn_mut()`'s doc for the full rationale): calling any statement
/// method directly on `client` (e.g. `client.batch_execute(..)`/`client.query(..)`) BYPASSES the
/// RFQ pin authority — `PgBackend::tx_status()` is never re-read afterward and the Err-arm
/// fail-safe (`tx_open`/`tainted` forcing) never runs — so it can leak an open/aborted
/// transaction to the next tenant of this pooled connection. `client` is reachable here for
/// NON-STATEMENT inspection ONLY (e.g. a test reading `pg_backend_pid()`); any real statement
/// execution MUST go through the instrumented `Checkout` methods instead.
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

/// The exact `Targeted` reset batch (M1-S3, SPEC §7.2 + the plan's verification-fix addenda) — a
/// named constant (rather than an inline literal) so a unit test can assert the exact string
/// verbatim without depending on Rust's string-literal line-continuation whitespace-stripping
/// behavior being read correctly by eye. This is Postgres's own `DISCARD ALL` MINUS its two
/// prepare-affecting statements (`DEALLOCATE ALL`, `DISCARD PLANS`) — do NOT add them back; that
/// would defeat the entire point of the `Targeted` profile (preserving the engine's future
/// namespaced prepared statements across a checkout recycle).
///
/// Only `DEALLOCATE ALL` destroys the statements themselves; `DISCARD PLANS` drops CACHED PLANS and
/// deallocates nothing (measured on PG 17: `PREPARE zzp` → `pg_prepared_statements` has 1 row;
/// `DISCARD PLANS` → still 1; `DEALLOCATE ALL` → 0). Both stay omitted anyway — the second because
/// discarding the plans of the prepares we are deliberately preserving would throw away exactly the
/// work that makes preserving them worth doing.
const TARGETED_RESET_SQL: &str = "CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;";

/// The `Full` reset batch — Postgres's own `DISCARD ALL`, unmodified.
const FULL_RESET_SQL: &str = "DISCARD ALL";

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
    type RowStream = crate::query::PgRowStream;

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

    /// `PgBackend` only ever speaks Postgres (M1-S2 Task 2) — MySQL/SQLite backends land in a
    /// future slice as their own `PoolBackend` impls.
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    /// Reads the authoritative RFQ status the driver already tracked off the wire (Task 1's fork
    /// addition) — no round trip, and no bespoke bookkeeping to keep in sync with reality.
    fn tx_status(&self, conn: &Self::Conn) -> TxStatus {
        TxStatus::from_pg_byte(conn.client.transaction_status())
    }

    /// M1-S3 (SPEC §7.2): `Full` runs Postgres's own `DISCARD ALL` (used for a `tainted` conn);
    /// `Targeted` runs a narrower batch — exactly `DISCARD ALL` minus its two prepare-affecting
    /// statements (`DEALLOCATE ALL`, which destroys them, and `DISCARD PLANS`, which drops their
    /// cached plans and deallocates nothing) — for a non-tainted recycled conn, so the
    /// engine's future namespaced prepared statements survive while every other §7.4 blind-spot
    /// leak class (holdable cursors, role/session-authorization, GUCs, LISTEN channels, advisory
    /// locks, temp tables/sequences) still gets closed. One `batch_execute` per profile (simple
    /// protocol, one round trip, one trailing `ReadyForQuery`).
    ///
    /// **M1-S8a review F1 — the driver's typeinfo STATEMENT cache is invalidated here, on BOTH
    /// profiles** (SPEC §22.2 (m); `docs/followups/2026-08-06-discard-all-typeinfo-cache-poisoning.md`).
    /// `tokio-postgres` prepares and then caches, for the connection's whole life, the three
    /// statements it uses to resolve an OID it does not know natively, and it never learns that the
    /// SERVER dropped them. `DISCARD ALL` (the `Full` profile) includes `DEALLOCATE ALL` and does
    /// exactly that, so without this call the next typeinfo lookup on the recycled connection dies
    /// with a bare `26000 prepared statement "sN" does not exist` — permanently, for that
    /// connection. That was a corner nobody was told to use until S8a made a DOMAIN-typed PARAMETER
    /// bindable, because `stmt.params()` reports a domain's OWN oid where `RowDescription` resolves
    /// it to the base — so an ordinary supported WRITE now performs a typeinfo lookup. Measured on
    /// PG 17, `INSERT INTO t (domcol) VALUES ($1)` and `UPDATE t SET domcol = $1` both report the
    /// DOMAIN (as does an explicit `$1::domain` cast), while `WHERE domcol = $1` resolves to the
    /// base and does not. Two writes into two DIFFERENT domain columns across one recycle is
    /// therefore all it takes, and that is ordinary ORM traffic for any schema with domain
    /// columns.
    ///
    /// `Targeted` is cleared too even though it never itself deallocates, because the pool is not
    /// the only party that can: `ferro-classify` safe-lists a USER-issued `DISCARD ALL` by design
    /// (`RESET`/`DISCARD` move session state toward default), so a connection whose own SQL ran one
    /// is recycled NON-tainted, through this exact profile, carrying the same dead handles. (A user
    /// `DEALLOCATE ALL` DOES taint — `PinTrigger::Prepare` — so that one lands on `Full`; and
    /// `DISCARD PLANS` deallocates nothing at all, verified on PG 17.) The cost is one re-prepare
    /// on the first custom OID a
    /// checkout resolves that its oid→`Type` map has not already cached — the `types` map is
    /// deliberately NOT cleared, since `DEALLOCATE ALL` destroys statements, not type definitions.
    ///
    /// Cleared BEFORE the batch runs: each dropped handle sends its usual `Close`, which is a real
    /// deallocation while the statement still exists and an accepted no-op once it does not ("It is
    /// not an error to issue Close against a nonexistent statement or portal name"), so the driver
    /// can never be left holding a handle to a statement the server dropped — not even if the batch
    /// below fails partway.
    ///
    /// RESIDUAL (documented, not fixed here): a user statement that deallocates and a custom-OID
    /// lookup within the SAME checkout still poisons that checkout — there is no reset between
    /// them. That needs a per-statement hook rather than a reset-time one; the pool-caused and
    /// recycle-visible cases, which are the ones a drop-in tier actually meets, are closed.
    async fn reset(&self, conn: &mut Self::Conn, profile: ResetProfile) -> Result<(), PoolError> {
        let sql = match profile {
            ResetProfile::Full => FULL_RESET_SQL,
            ResetProfile::Targeted => TARGETED_RESET_SQL,
        };
        conn.client.clear_typeinfo_statement_cache();
        conn.client.batch_execute(sql).await.map_err(|e| {
            tracing::warn!(error = %e, profile = ?profile, "ferro-backend-pg: reset failed");
            PoolError::ConnectionLost
        })
    }

    /// A non-tainted recycled PG conn always gets the `Targeted` profile (the §7.4 blind-spot
    /// backstop) — PG has no cheaper "provably clean" signal the way a future MySQL session-state
    /// tracker might.
    fn clean_reset_profile(&self) -> Option<ResetProfile> {
        Some(ResetProfile::Targeted)
    }

    /// M1-S4 fix (M4b, whole-branch final review): routed through the SAME `is_session_fatal`-first
    /// `error_map::map` the row-returning `query` path already uses, rather than a hand-rolled
    /// `if is_session_fatal { ConnectionLost } else { Backend(str) }`. The old `Backend(str)` arm
    /// DISCARDED the SQLSTATE — and `Checkout::commit_tx`/`rollback_tx`/`tx_control` (`pool.rs`) all
    /// route through this method, so a `40001`(serialization)/`40P01`(deadlock) conflict surfacing
    /// AT COMMIT (the dominant SERIALIZABLE case — SSI defers the pivot check to COMMIT) used to
    /// mis-classify `Protocol{NonRetryable}` instead of `Retryable{SerializationFailure/Deadlock}`.
    /// `error_map::map` is `is_session_fatal`-first, so this changes NOTHING about the fatal branch
    /// (still `ConnectionLost` → §19.3 `WriteUnconfirmed{Indeterminate}` for a lost COMMIT, byte-for-
    /// byte identical to before — see `declare_ctl_maps_replies_including_commit_loss_indeterminate`
    /// in `ferrod`'s `sql.rs`), only the non-fatal arm: a genuine SQLSTATE now survives as
    /// `PoolError::Sql{code, branch, sqlstate, message}` and passes through `classify_fate` verbatim
    /// (see `serialization_40001... `/`commit_time_serialization_write_skew_is_retryable_live` in
    /// `ferrod`'s `chaos_fate_it.rs`), exactly like `Checkout::query` already does.
    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError> {
        conn.client.batch_execute(sql).await.map(|_| 0u64).map_err(|e| {
            if is_session_fatal(&e) {
                tracing::warn!(
                    error = %e,
                    "ferro-backend-pg: simple_query hit a session-ending failure (connection considered lost)"
                );
            }
            crate::error_map::map(&e)
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

    /// The incremental, constant-memory row-returning path (S5 Task 3): prepare + `query_raw`, then
    /// return the prepared `cols` and a box-pinned [`crate::query::PgRowStream`] the caller drains
    /// one row at a time. See `crate::query::stream` for the prepare/bind-pre-validation flow (§19.3
    /// safety, identical to the buffered `query`).
    async fn query_stream(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        params: &[crate::Value],
    ) -> Result<(Vec<ferro_proto::messages::sql::ColMeta>, Self::RowStream), PoolError> {
        crate::query::stream(&conn.client, sql, params).await
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `PgBackend::dialect()` is a pure, synchronous constant — no live Postgres needed to assert
    /// it (M1-S2 Task 2 TDD).
    #[test]
    fn dialect_is_postgres() {
        let backend = PgBackend::new("postgres://unused/unused");
        assert_eq!(backend.dialect(), Dialect::Postgres);
    }

    /// M1-S3 TDD: a non-tainted recycled PG conn's hygiene profile is `Targeted`.
    #[test]
    fn clean_reset_profile_is_targeted() {
        let backend = PgBackend::new("postgres://unused/unused");
        assert_eq!(backend.clean_reset_profile(), Some(ResetProfile::Targeted));
    }

    /// M1-S8a: PG inherits the trait default and DOES stream — so `supports_row_streaming` can
    /// never be "false everywhere" and silently disable the PG producer (the M1-S5 windowed
    /// DATA-channel path). Behavioural: it calls the real method through the real trait.
    #[test]
    fn pg_supports_row_streaming() {
        let backend = PgBackend::new("postgres://unused/unused");
        assert!(backend.supports_row_streaming());
    }

    /// The exact targeted batch string, verbatim (task brief + SPEC §7.2 + the plan's
    /// verification-fix addenda): `DISCARD ALL` minus its two prepare-affecting statements, plus
    /// `CLOSE ALL` (holdable-cursor leak fix) and `SET SESSION AUTHORIZATION DEFAULT`
    /// (role/session-authorization coverage).
    #[test]
    fn targeted_reset_sql_is_exact() {
        // Deliberately a single-line literal (no `\`-newline continuation) so this assertion can't
        // share — and thus can't be silently confirming the correctness of — the same Rust
        // line-continuation whitespace-stripping the production constant's definition relies on.
        let expected = "CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;";
        assert_eq!(TARGETED_RESET_SQL, expected);
        // Never regains the two prepare-affecting statements `Targeted` exists to omit (only
        // `DEALLOCATE ALL` destroys them; `DISCARD PLANS` drops their cached plans).
        assert!(!TARGETED_RESET_SQL.contains("DEALLOCATE ALL"));
        assert!(!TARGETED_RESET_SQL.contains("DISCARD PLANS"));
    }

    #[test]
    fn full_reset_sql_is_discard_all() {
        assert_eq!(FULL_RESET_SQL, "DISCARD ALL");
    }
}
