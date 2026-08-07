use crate::error::PoolError;
use async_trait::async_trait;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

/// The upstream SQL dialect a [`PoolBackend`] talks to (M1-S2, `ferro-classify`'s assist lexer
/// needs it to pick the right keyword rule set — SPEC §7.1). Re-exported here so downstream
/// backend crates (`ferro-backend-pg`, `FakeBackend`) can write `ferro_pool::backend::Dialect`
/// without taking their own direct dependency on the `ferro-classify` leaf crate.
pub use ferro_classify::Dialect;

/// The buffered result of a row-returning statement (S5, BLOCKER-2). `cols` is populated even for
/// a zero-row result (built from the prepared statement's columns), `rows` is the fully-buffered
/// result set (D-S5-1: M0 buffers rather than streams), and `affected` comes from the command tag
/// (`RowStream::rows_affected()`), NEVER a hardcoded 0 (the S4 `batch_execute` defect). `Send` so
/// it can cross the reaper's `tokio::spawn` boundary like every other backend future's output.
///
/// Not `Eq` (it carries `Value`, whose `F64` arm forbids `Eq`), so it derives only
/// `Debug/Clone/PartialEq` — enough for the `FakeBackend`-driven guard tests.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryResult {
    pub cols: Vec<ColMeta>,
    pub rows: Vec<Vec<Value>>,
    pub affected: u64,
    /// The auto-generated key this statement produced, when the BACKEND PROTOCOL reports one
    /// (M1-S8a). MySQL/MariaDB fill it from the OK packet's `LAST_INSERT_ID()`; Postgres always
    /// leaves it `None` (PG has no such protocol field — callers use `INSERT … RETURNING`).
    ///
    /// **It cannot be recovered by a follow-up query.** Measured live on a transaction-mode pool:
    /// `SELECT LAST_INSERT_ID()` after an INSERT returned **0**, and PG's `SELECT lastval()` threw
    /// `55000` — the follow-up statement lands on a DIFFERENT pooled connection. Worse, once that
    /// other PG session HAS touched a sequence, `lastval()` stops erroring and returns ITS OWN last
    /// value (measured: `1`, from an unrelated table) — a silently WRONG key, which is strictly
    /// worse than an error. So the value is carried here, off the statement's own OK packet, or it
    /// is lost.
    pub last_insert_id: Option<u64>,
}

/// The real transaction status of a pooled connection, surfaced from Postgres's `ReadyForQuery`
/// (`Z`) status byte (M1, SPEC §21 open item resolved) — `I`dle, `T`ransaction, `E`rror. This is
/// the authoritative pin signal the pin engine (Task 4) checks AFTER every statement, replacing
/// the S4 stub's engine-side-only bookkeeping (`Checkout::begin_tx`/`commit_tx`/`rollback_tx`
/// setting `pin`/`tx_open` by hand) with the protocol's own word on whether a transaction is
/// still open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    /// No transaction in progress (RFQ `I`, or nothing observed yet).
    Idle,
    /// A transaction block is open (RFQ `T`).
    InTx,
    /// A transaction block is open but has hit an error and is aborted, pending ROLLBACK (RFQ
    /// `E`).
    Failed,
}

impl TxStatus {
    /// Maps a raw `ReadyForQuery` status byte to a [`TxStatus`]: `b'T'` → [`TxStatus::InTx`],
    /// `b'E'` → [`TxStatus::Failed`], anything else (including the fresh/idle `b'I'`) →
    /// [`TxStatus::Idle`] — a deliberately permissive fallback since `I` is by far the common case
    /// and an unrecognized byte should never be treated as "still in a transaction".
    pub fn from_pg_byte(byte: u8) -> TxStatus {
        match byte {
            b'T' => TxStatus::InTx,
            b'E' => TxStatus::Failed,
            _ => TxStatus::Idle,
        }
    }
}

/// Checkout-time hygiene profile (M1-S3, SPEC §7.2). Which one applies to a recycled connection is
/// driven by the pin engine's `tainted` bit, NOT by "was this conn ever in a tx" (a cleanly
/// committed transaction never taints — see `Checkout::commit_tx`) — the predicate and rationale
/// live at the call site in `pool.rs`'s recycle block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetProfile {
    /// Full reset (e.g. Postgres `DISCARD ALL`) for a `tainted` conn — a session mutation was
    /// observed, or the conn's fate after an error/aborted tx can't be trusted. Destroys prepared
    /// statements along with everything else; that's acceptable here because the conn is already
    /// suspect.
    Full,
    /// A narrower reset applied to a non-tainted recycled conn (the §7.4 assist-lexer blind-spot
    /// backstop): releases session state a safe-listed statement's function/`DO` body could have
    /// mutated invisibly to the lexer (advisory locks, temp objects, LISTEN channels, GUCs, holdable
    /// cursors, role) while preserving the engine's future namespaced prepared statements.
    Targeted,
}

/// A fire-once, out-of-band cancel handle (S6). Grabbed via [`PoolBackend::cancel_handle`] /
/// `Checkout::cancel_handle` BEFORE an interruptible statement starts, moved into a `select!` arm,
/// and [`Cancel::cancel`]led on a deadline/abort — WITHOUT borrowing the `Checkout` the live query
/// future holds `&mut` (a `tokio-postgres` query future keeps `&mut Client`, so the cancel MUST
/// fire over a SIDE connection). Consuming `self` makes it fire-once by construction.
///
/// It is its own trait (not a bare method on `PoolBackend`) so the per-`tx_id` actor — which owns a
/// `Checkout<B>` but NOT the `B` backend value — can fire the cancel generically as
/// `handle.cancel().await` without reaching back through the backend.
#[async_trait]
pub trait Cancel: Send + 'static {
    /// Fire the out-of-band cancel of the connection's in-flight server statement. A best-effort,
    /// fire-and-forget signal: a failure to reach the server (already gone, race with completion)
    /// is swallowed — the caller has already decided to tear the transaction down regardless, and
    /// the engine NEVER re-runs the statement either way (charter rule 3).
    async fn cancel(self);
}

/// A pull-based, INCREMENTAL row source (S5 Task 3, the constant-memory streaming path). Produced
/// by [`PoolBackend::query_stream`] and driven ONE row at a time by `Checkout`'s `RowStreamHandle`
/// (and, above it, the `fetch:stream` producer's credit window in Task 4) — the opposite of
/// [`PoolBackend::query`], which buffers the WHOLE result into a `QueryResult` up front.
///
/// The contract mirrors an async iterator: `next()` yields `Some(Ok(row))` per row, `Some(Err(_))`
/// on a mid-stream failure (SQLSTATE-preserving, via the backend's `error_map`), then `None` once
/// exhausted. `rows_affected()` is only meaningful AFTER `next()` has returned `None` — the command
/// tag (`RowStream::rows_affected()`) arrives in the `CommandComplete` message that immediately
/// precedes the terminating `ReadyForQuery`, so reading it before exhaustion returns `0`.
///
/// `#[async_trait]` (mirroring [`PoolBackend`]) so `next()`'s future is boxed `+ Send`; the `Send`
/// supertrait makes every implementor `Send` so the whole stream can cross the `fetch:stream`
/// producer's `tokio::spawn` boundary. Kept trait-generic (no pg type here) so `ferro-pool` stays
/// backend-agnostic — the pg impl wraps a box-pinned `tokio_postgres::RowStream`, the fake replays
/// a scripted `Vec`.
#[async_trait]
pub trait BackendRows: Send {
    /// Pull the NEXT row, lazily: a backend MUST NOT have produced this row until `next()` is
    /// polled (constant memory — this is the whole point of the streaming path). `Some(Ok(row))`
    /// per row; `Some(Err(_))` on a mid-stream error (after which the stream is done — a subsequent
    /// `next()` returns `None`); `None` once the result set is exhausted.
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>>;

    /// The command-tag affected-row count, valid ONLY after `next()` has returned `None` (see the
    /// trait docs); `0` before the stream is exhausted, never a fabricated value.
    fn rows_affected(&self) -> u64;
}

/// A pooled backend: connection factory + per-connection operations. `#[async_trait]` (not bare
/// async-fn-in-trait) so the futures are `Send` for the background reaper's `tokio::spawn` (Task
/// 3) — this is mandated by plan v2/B2, not a per-toolchain style choice.
#[async_trait]
pub trait PoolBackend: Send + Sync + 'static {
    type Conn: Send + 'static;

    /// The [`BackendRows`] implementation this backend's [`PoolBackend::query_stream`] produces
    /// (S5 Task 3). An associated type (not a `dyn` object) so `ferro-pool` stays backend-agnostic
    /// with no boxing forced and no pg type in the trait; `+ Send` so the concrete stream (pg's
    /// box-pinned `tokio_postgres::RowStream`, the fake's scripted `Vec`) can cross the
    /// `fetch:stream` producer's `tokio::spawn` boundary.
    type RowStream: BackendRows + Send;

    /// An out-of-band handle that cancels the connection's in-flight server statement (S6). It is
    /// `Send + 'static` (via the [`Cancel`] supertrait bound) so it can be grabbed BEFORE a query
    /// future starts and moved into a separate `select!` arm that fires the cancel WITHOUT borrowing
    /// the `Checkout` while the query future is live — a `tokio-postgres` query future keeps
    /// `&mut Client`, so the cancel MUST come from a SIDE connection. Kept as an associated type so
    /// `ferro-pool` stays backend-agnostic (no pg type in the trait); for Postgres it is a
    /// `tokio_postgres::CancelToken`.
    type CancelHandle: Cancel;

    /// Establish a brand-new backend connection.
    async fn connect(&self) -> Result<Self::Conn, PoolError>;

    /// Produce an out-of-band [`Self::CancelHandle`] for `conn`. Synchronous and borrows nothing
    /// from `conn` beyond the handshake it needs to build the handle (for Postgres, the backend
    /// key data captured by `Client::cancel_token`). The handle cancels via a SIDE connection.
    fn cancel_handle(&self, conn: &Self::Conn) -> Self::CancelHandle;

    /// Cheap liveness check (e.g. `SELECT 1`). Used by the checkout-time health check and the
    /// background reaper.
    async fn ping(&self, conn: &mut Self::Conn) -> Result<(), PoolError>;

    /// Cheap, synchronous "is this obviously dead" check (no round trip) used at checkout time.
    fn is_closed(&self, conn: &Self::Conn) -> bool;

    /// The upstream SQL [`Dialect`] this backend speaks — a per-backend constant, not per-`conn`
    /// (a `PoolBackend` impl only ever talks one dialect), so no round trip and no `conn` argument.
    /// Read by `Checkout`'s assist lexer (`ferro_classify::classify`, M1-S2 Task 3) to pick the
    /// right keyword rule set for the statement it is about to run.
    fn dialect(&self) -> Dialect;

    /// Cheap, synchronous read of `conn`'s current [`TxStatus`] — no round trip. Mirrors the real
    /// Postgres protocol's `ReadyForQuery` status byte, which every backend response ends with, so
    /// no query is needed to learn it: the real backend reads a value the driver already tracked
    /// off the wire (`conn.client.transaction_status()`), and the fake models the same thing
    /// per-connection so it can't be bypassed by pool-internal bookkeeping. This is the pin
    /// engine's authority (Task 4), replacing the S4 stub's engine-side-only pin tracking.
    fn tx_status(&self, conn: &Self::Conn) -> TxStatus;

    /// Did the backend OBSERVE (and now CONSUME/clear) a session-state MUTATION reported by the last
    /// statement's own protocol signal? A SECOND, additive pin signal alongside [`tx_status`] (M1-S6,
    /// SPEC §7.1), read at the same post-statement point and consumed by
    /// `Checkout::apply_session_tracker`, which taints for reuse-safety and labels the cause
    /// [`crate::pin::PinCause::SessionTracker`] WITHOUT touching the transaction authority
    /// (`tx_open`/`pin`).
    ///
    /// The MySQL/MariaDB backend overrides this to drain its connection's OK-packet session-tracker
    /// flag (`OkPacket::session_state_info`) — the signal that sees session mutations INSIDE stored
    /// programs, which the assist lexer's §7.1 hard gate cannot. Reading it CLEARS it (one mutation
    /// is reported exactly once), so it is called precisely once per statement.
    ///
    /// [`tx_status`]: PoolBackend::tx_status
    ///
    /// **Default `false`** — every backend WITHOUT an OK-packet-style session tracker (Postgres, the
    /// `FakeBackend`, and every other current impl) inherits it and is UNCHANGED: for them
    /// `apply_session_tracker` is a pure no-op that never taints and never sets a pin cause. Only a
    /// backend that can actually PROVE a session mutation off the wire overrides it.
    fn take_session_mutated(&self, _conn: &mut Self::Conn) -> bool {
        false
    }

    /// Can this backend produce an INCREMENTAL row stream ([`PoolBackend::query_stream`]) at all?
    ///
    /// The ONE authority for the `fetch:stream` capability. It exists because the SQL service has
    /// TWO dispatch arms (autocommit and tx-scoped) and, before M1-S8a, only the autocommit one
    /// carried a hand-written `matches!(pool, AnyPool::Mysql(_))` check — so a tx-scoped stream
    /// refused LATE (after checkout + BEGIN), force-tainting the pinned connection on the way out
    /// (`Checkout::query_stream`'s Err arm). Both arms now read THIS method, so a backend that
    /// gains streaming flips one line and both arms follow.
    ///
    /// **Default `true`** — Postgres and the `FakeBackend` stream today and are unchanged.
    fn supports_row_streaming(&self) -> bool {
        true
    }

    /// Hygiene reset, run at checkout before a recycled conn is handed to a new caller (v2/B1;
    /// profile-parameterized in M1-S3, SPEC §7.2). `profile` selects HOW MUCH state to release:
    /// [`ResetProfile::Full`] (e.g. Postgres `DISCARD ALL`) for a tainted conn, or
    /// [`ResetProfile::Targeted`] (e.g. a batch that releases advisory locks/temp/listens/GUCs/
    /// cursors while preserving prepared statements) for a non-tainted recycled conn. The caller
    /// (the pool's recycle block) decides which profile applies; this method just executes it.
    async fn reset(&self, conn: &mut Self::Conn, profile: ResetProfile) -> Result<(), PoolError>;

    /// The [`ResetProfile`] to apply to a recycled conn that is NOT `tainted` (M1-S3, SPEC §7.2).
    /// `Some(profile)` runs that profile as the §7.4 blind-spot backstop (a safe-listed statement's
    /// function/`DO` body can mutate session state invisibly to the assist lexer, leaving the conn
    /// `!tainted` yet dirty); `None` means "skip hygiene for a clean conn" — reserved for a future
    /// backend (e.g. an S6 MySQL session-state tracker) that can prove a recycled conn needs
    /// nothing at all. `PgBackend` always returns `Some(ResetProfile::Targeted)`.
    fn clean_reset_profile(&self) -> Option<ResetProfile>;

    /// Raw simple query — UNGUARDED. Used only by the pin hook (BEGIN/COMMIT/ROLLBACK, Task 4)
    /// and internal reset. The user-facing guarded entry point (`Checkout::exec`) lands in Task
    /// 4/5; callers outside those internals must not reach for this method directly.
    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError>;

    /// Row-returning, parameterized statement (S5, BLOCKER-2). Runs `sql` (already `?`→`$n`
    /// normalized by the backend) with bound `params`, buffers the full result, and returns
    /// `{cols, rows, affected}`. This is UNGUARDED at the trait level — the guard against bare
    /// tx-control lives in `Checkout::query` (which calls `pin::is_bare_tx_control` FIRST, exactly
    /// like `Checkout::exec`), so a handler that goes through `Checkout::query` can never open an
    /// untracked transaction the next tenant would inherit (cross-tenant leak, charter rule 6).
    async fn query(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, PoolError>;

    /// Row-returning, parameterized statement — INCREMENTAL (S5 Task 3, the constant-memory path).
    /// Runs `sql` (already `?`→`$n` normalized by the backend) with bound `params`, returns the
    /// `cols` (from the prepared statement — correct even for a zero-row result) plus a pull-based
    /// [`Self::RowStream`] that produces rows ONE AT A TIME, NOT a buffered `Vec` like
    /// [`PoolBackend::query`]. This is the producer half of the `fetch:stream` reply path (Task 4).
    ///
    /// UNGUARDED at the trait level, exactly like [`PoolBackend::query`]: the guard against bare
    /// tx-control lives in `Checkout::query_stream` (which calls `pin::is_bare_tx_control` FIRST),
    /// so a partially-drained/abandoned stream can never leave the next tenant an untracked open
    /// transaction (cross-tenant leak, charter rule 6). The pin/taint bookkeeping — the S1 RFQ
    /// post-drain read + Err-arm force-taint and the S2 assist classify — runs in the handle's
    /// `finish()` (or, on abandonment, the handle's `Drop` safety net), NOT here.
    async fn query_stream(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        params: &[Value],
    ) -> Result<(Vec<ColMeta>, Self::RowStream), PoolError>;
}

#[cfg(test)]
mod tests {
    use super::TxStatus;

    #[test]
    fn from_pg_byte_maps_known_bytes() {
        assert_eq!(TxStatus::from_pg_byte(b'I'), TxStatus::Idle);
        assert_eq!(TxStatus::from_pg_byte(b'T'), TxStatus::InTx);
        assert_eq!(TxStatus::from_pg_byte(b'E'), TxStatus::Failed);
    }

    #[test]
    fn from_pg_byte_falls_back_to_idle_for_unknown_bytes() {
        assert_eq!(TxStatus::from_pg_byte(b'?'), TxStatus::Idle);
        assert_eq!(TxStatus::from_pg_byte(0), TxStatus::Idle);
        assert_eq!(TxStatus::from_pg_byte(b'z'), TxStatus::Idle);
    }
}
