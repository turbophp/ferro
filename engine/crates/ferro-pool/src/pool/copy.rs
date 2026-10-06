//! The guarded COPY entry points on [`Checkout`] (M3-D4, SPEC §6.1) and their handles.
//!
//! Same discipline as `query_stream`/`RowStreamHandle`: the handle borrows the `Checkout`, a
//! terminal method (`finish`/`abort`) is MANDATORY and runs the S1/S2 post-statement bookkeeping, and
//! the `Drop` safety net taints and DISCARDS a connection whose COPY was neither finished nor aborted
//! (a panic, an unwind, a dropped future) — a connection mid-COPY must never reach the next tenant
//! (charter rule 6).
//!
//! **The shape guard runs first, before any backend call.** A COPY entry point drives the backend's
//! COPY sub-protocol, and a statement that cannot speak it in that direction is refused here
//! (`ferro_classify::copy_direction`): measured by reading the driver, a non-COPY statement on the
//! copy-in path would EXECUTE and COMMIT before the driver noticed, and a `COPY … FROM STDIN` on the
//! copy-out path would leave the server waiting for data forever. Nothing is rewritten — the statement
//! reaches the server byte for byte.

use std::sync::Arc;

use bytes::Bytes;
use ferro_classify::{CopyDirection, copy_direction};

use super::{Checkout, CheckoutStats};
use crate::backend::{BackendCopyIn, BackendCopyOut, PoolBackend};
use crate::error::PoolError;

/// What a finished COPY reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyEnd {
    /// The `COPY n` row count: rows copied in, or rows exported.
    pub affected: u64,
    pub stats: CheckoutStats,
}

impl<B: PoolBackend> Checkout<B> {
    /// Refuse a COPY this backend cannot run, or a statement that is not exactly one COPY in
    /// `want`'s direction — BEFORE anything reaches the backend.
    fn guard_copy(&self, sql: &str, want: CopyDirection) -> Result<(), PoolError> {
        if !self.pool.backend.supports_copy() {
            return Err(PoolError::Unsupported(
                "COPY is not supported on this backend (PostgreSQL only)".into(),
            ));
        }
        match copy_direction(sql) {
            Some(d) if d == want => Ok(()),
            _ => Err(PoolError::Unsupported(match want {
                CopyDirection::In => {
                    "COPY_IN requires exactly one `COPY … FROM STDIN` statement".to_string()
                }
                CopyDirection::Out => {
                    "COPY_OUT requires exactly one `COPY … TO STDOUT` statement".to_string()
                }
            })),
        }
    }

    /// Start a `COPY … FROM STDIN` (M3-D4). On `Ok` the server is in copy-in mode and the returned
    /// handle owns the rest of the statement: [`CopyInHandle::finish`] or [`CopyInHandle::abort`]
    /// must end it.
    pub async fn copy_in(&mut self, sql: &str) -> Result<CopyInHandle<'_, B>, PoolError> {
        self.guard_copy(sql, CopyDirection::In)?;
        let pool = Arc::clone(&self.pool);
        match pool.backend.copy_in(self.conn_mut(), sql).await {
            Ok(sink) => Ok(CopyInHandle {
                checkout: self,
                sink: Some(sink),
                sql: sql.to_owned(),
                ended: false,
            }),
            Err(e) => {
                self.copy_open_failed(&e, sql);
                Err(e)
            }
        }
    }

    /// Start a `COPY … TO STDOUT` (M3-D4). [`CopyOutHandle::finish`] must end it.
    pub async fn copy_out(&mut self, sql: &str) -> Result<CopyOutHandle<'_, B>, PoolError> {
        self.guard_copy(sql, CopyDirection::Out)?;
        let pool = Arc::clone(&self.pool);
        match pool.backend.copy_out(self.conn_mut(), sql).await {
            Ok(rows) => Ok(CopyOutHandle {
                checkout: self,
                rows: Some(rows),
                sql: sql.to_owned(),
                errored: false,
                ended: false,
            }),
            Err(e) => {
                self.copy_open_failed(&e, sql);
                Err(e)
            }
        }
    }

    /// An open that failed. A server error (`Sql`) leaves the connection in step, and the Rule-A
    /// force-taint (via `finalize_stream`) recycles it with the full reset. Anything else — a lost
    /// connection, or a reply the driver did not expect — leaves the COPY sub-protocol in an unknown
    /// state, so the connection is DISCARDED rather than trusted to any reset.
    fn copy_open_failed(&mut self, e: &PoolError, sql: &str) {
        if !matches!(e, PoolError::Sql { .. }) {
            self.discard = true;
        }
        self.finalize_stream(true, sql);
    }
}

/// The open `COPY … FROM STDIN` on a checked-out connection.
pub struct CopyInHandle<'a, B: PoolBackend> {
    checkout: &'a mut Checkout<B>,
    sink: Option<B::CopyIn>,
    sql: String,
    ended: bool,
}

impl<B: PoolBackend> CopyInHandle<'_, B> {
    /// Forward one chunk. An `Err` means the COPY has failed; call [`CopyInHandle::abort`] next.
    pub async fn send(&mut self, chunk: Bytes) -> Result<(), PoolError> {
        match self.sink.as_mut() {
            Some(sink) => sink.send(chunk).await,
            None => Err(PoolError::Closed),
        }
    }

    /// Send the end-of-data and wait for the server's verdict, through the implicit COMMIT. After
    /// this call has STARTED the COPY may apply: a caller that stops waiting (a deadline, a cancel)
    /// must fire the connection's out-of-band cancel and keep awaiting this future, never drop it.
    pub async fn finish(mut self) -> Result<CopyEnd, PoolError> {
        let mut sink = self
            .sink
            .take()
            .expect("CopyInHandle is finished at most once");
        let r = sink.finish().await;
        self.ended = true;
        let stats = self.checkout.stats();
        self.checkout.finalize_stream(r.is_err(), &self.sql);
        r.map(|affected| CopyEnd { affected, stats })
    }

    /// Abandon the COPY before its end-of-data. Nothing of it can apply. Bounded by the pool's
    /// `checkout_timeout` (the knob every other post-statement cleanup uses): an acknowledgement that
    /// does not arrive in time — or a failed one — DISCARDS the connection, because its COPY state is
    /// then unknown. Either way the connection is force-tainted, so a recycled one gets the full reset.
    pub async fn abort(mut self) {
        let mut sink = self
            .sink
            .take()
            .expect("CopyInHandle is aborted at most once");
        let bound = self.checkout.pool.config.checkout_timeout;
        match tokio::time::timeout(bound, sink.abort()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "ferro-pool: COPY abort was not acknowledged — connection will be discarded");
                self.checkout.discard = true;
            }
            Err(_elapsed) => {
                tracing::warn!(
                    timeout_ms = bound.as_millis() as u64,
                    "ferro-pool: COPY abort timed out — connection will be discarded"
                );
                self.checkout.discard = true;
            }
        }
        drop(sink);
        self.ended = true;
        self.checkout.finalize_stream(true, &self.sql);
    }
}

impl<B: PoolBackend> Drop for CopyInHandle<'_, B> {
    fn drop(&mut self) {
        if !self.ended {
            // The safety net (charter rule 6), as `RowStreamHandle`'s: dropping the backend sink
            // abandons the COPY (PostgreSQL: `CopyFail`), but nothing confirms the server got there,
            // so the connection is never reused.
            self.checkout.tainted = true;
            self.checkout.discard = true;
        }
    }
}

/// The open `COPY … TO STDOUT` on a checked-out connection.
pub struct CopyOutHandle<'a, B: PoolBackend> {
    checkout: &'a mut Checkout<B>,
    rows: Option<B::CopyOut>,
    sql: String,
    errored: bool,
    ended: bool,
}

impl<B: PoolBackend> CopyOutHandle<'_, B> {
    /// The next chunk of raw COPY bytes; `None` at the end of the statement.
    pub async fn next(&mut self) -> Option<Result<Bytes, PoolError>> {
        match self.rows.as_mut()?.next().await {
            Some(Err(e)) => {
                self.errored = true;
                Some(Err(e))
            }
            other => other,
        }
    }

    /// End the COPY: drain what is left (a no-op after `next()` returned `None`), bounded by
    /// `checkout_timeout` — a drain that does not finish DISCARDS the connection rather than hand
    /// out one that is mid-COPY — then run the post-statement bookkeeping. A caller abandoning the
    /// COPY early fires the out-of-band cancel first, so the drain is short.
    pub async fn finish(mut self) -> Result<CopyEnd, PoolError> {
        let bound = self.checkout.pool.config.checkout_timeout;
        let drained = tokio::time::timeout(bound, async {
            let mut first_err = None;
            while let Some(item) = self.next().await {
                if let Err(e) = item
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            first_err
        })
        .await;
        let rows = self
            .rows
            .take()
            .expect("CopyOutHandle is finished at most once");
        self.ended = true;
        let stats = self.checkout.stats();
        let result = match drained {
            Ok(None) if !self.errored => Ok(CopyEnd {
                affected: rows.rows_affected(),
                stats,
            }),
            Ok(Some(e)) => Err(e),
            Ok(None) => Err(PoolError::Backend("COPY TO STDOUT failed".into())),
            Err(_elapsed) => {
                tracing::warn!(
                    timeout_ms = bound.as_millis() as u64,
                    "ferro-pool: COPY TO STDOUT drain timed out — connection will be discarded"
                );
                self.checkout.discard = true;
                self.errored = true;
                Err(PoolError::ConnectionLost)
            }
        };
        drop(rows);
        self.checkout
            .finalize_stream(self.errored || result.is_err(), &self.sql);
        result
    }
}

impl<B: PoolBackend> Drop for CopyOutHandle<'_, B> {
    fn drop(&mut self) {
        if !self.ended {
            self.checkout.tainted = true;
            self.checkout.discard = true;
        }
    }
}
