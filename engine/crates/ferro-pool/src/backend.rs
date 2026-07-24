use crate::error::PoolError;
use async_trait::async_trait;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

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
}

/// A pooled backend: connection factory + per-connection operations. `#[async_trait]` (not bare
/// async-fn-in-trait) so the futures are `Send` for the background reaper's `tokio::spawn` (Task
/// 3) — this is mandated by plan v2/B2, not a per-toolchain style choice.
#[async_trait]
pub trait PoolBackend: Send + Sync + 'static {
    type Conn: Send + 'static;

    /// Establish a brand-new backend connection.
    async fn connect(&self) -> Result<Self::Conn, PoolError>;

    /// Cheap liveness check (e.g. `SELECT 1`). Used by the checkout-time health check and the
    /// background reaper.
    async fn ping(&self, conn: &mut Self::Conn) -> Result<(), PoolError>;

    /// Cheap, synchronous "is this obviously dead" check (no round trip) used at checkout time.
    fn is_closed(&self, conn: &Self::Conn) -> bool;

    /// Hygiene reset (e.g. `DISCARD ALL`) — run at checkout for a tainted/tx-served conn before
    /// it is handed to a new caller (v2/B1).
    async fn reset(&self, conn: &mut Self::Conn) -> Result<(), PoolError>;

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
}
