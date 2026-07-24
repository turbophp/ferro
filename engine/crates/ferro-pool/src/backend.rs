use crate::error::PoolError;
use async_trait::async_trait;

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
}
