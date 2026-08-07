//! The SQL `EXEC` service handler (S5, THE MILESTONE): the real replacement for S3's
//! `service=SQL, method=EXEC` `Unsupported` stub.
//!
//! Control flow (D-S5-1 — buffered into the single terminal, no DATA channel in M0):
//!  1. decode `ExecRequest` (per-request payload); a decode failure → per-request `Protocol` error.
//!  2. reject the not-yet-supported shapes as `Unsupported`: `query_id` (manifest is M3),
//!     `fetch=stream` (D-S5-1), an unknown pool name, a missing inline `sql`.
//!  3. `checkout()` the named pool → capture `queue_us` from `CheckoutStats`.
//!  4. run the GUARDED, row-returning [`ferro_pool::pool::Checkout::query`] — measuring `exec_us`
//!     around ONLY the DB call. We MUST use `Checkout::query` and MUST NOT touch `conn_mut()`/the
//!     raw client for user SQL: the raw path bypasses the `is_bare_tx_control` guard, so an
//!     `EXEC BEGIN` would open an untracked transaction the next tenant on this pooled connection
//!     inherits (a cross-tenant leak — charter rule 6). The `Checkout` guard is DROPPED here (RAII
//!     → the conn returns to the pool) BEFORE any framing/sending, so a slow client can never hold
//!     a pooled connection and `exec_us` reflects only the query (never client send time).
//!
//!     **M1-S4:** the autocommit path enforces `ExecRequest.timeout_ms` and the per-request
//!     `CANCEL` flag ([`run_autocommit_exec`]) via a `biased` `tokio::select!` that polls the query
//!     FIRST (mirroring the S6 tx actor's proven pattern, `tx/actor.rs` ~247-273) — so `sent` is
//!     honest whenever a later arm wins — and DRAINS (never drops) the query future on a
//!     timeout/cancel, via the out-of-band [`ferro_pool::backend::Cancel`] handle fired over a side
//!     connection. The drained value is a genuine `Result`: an `Ok` (the cancel/timeout LOST the
//!     race) is reported as the real success terminal, never fabricated as an error; an `Err` is
//!     mapped through `fate::classify_fate`. `responder.end_cancelled()`/`Outcome::Cancelled` are
//!     NOT used here — a fated cancel/timeout always rides a branch-carrying `Outcome::Error`
//!     (`Cancelled{NonRetryable}` for a read, `WriteUnconfirmed{Indeterminate}` for a dispatched
//!     write) — see [`declare_autocommit_exec`].
//!  5. shape by `fetch` (`rows` → include rows; `none` → drop rows, keep `cols`+`affected`), encode
//!     the terminal body, and SIZE-CHECK the FULLY-ENCODED `Outcome::Ok` payload (not the raw body
//!     — see [`OUTCOME_OK_OVERHEAD`]).
//!  6. declare the terminal `Outcome::Ok(body)` (or a mapped `Outcome::Error`) via the `Responder`;
//!     the supervisor sends the ONE terminal `END` on the existing S3 control path — exactly-one-END
//!     is untouched (no second terminal, no DATA frames).
//!
//! Error mapping ([`fate::classify_fate`]) layers the §19.3 `Indeterminate` classification on TOP
//! of the pool's coarse taxonomy, WITHOUT any read/write inference (charter rule 6): it branches on
//! the client-declared `readonly` flag, the honest `sent` (was the statement transmitted?) flag,
//! and the per-call-site `in_tx` (is this an in-transaction user STATEMENT?) flag — see
//! [`fate::OpContext`]. A known-fate `PoolError::Sql` (a server rejection OR a client-side bind
//! pre-validation) passes through VERBATIM — the override never applies to it. The engine NEVER
//! retries a user statement (charter rule 3); the wire branch only informs the client's own policy.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use ferro_pool::backend::{Cancel, PoolBackend, QueryResult};
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool, RowStreamHandle};
use ferro_proto::consts::{MAX_FRAME_PAYLOAD, errc, method_sql, method_tx, service};
use ferro_proto::messages::ErrorPayload;
use ferro_proto::messages::sql::{ExecOk, ExecRequest, Stats};
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, SavepointRequest, TxControl};
use ferro_proto::value::Value;

use crate::pools::{AnyPool, PoolRegistry};
use crate::services::fate::{self, OpContext};
use crate::session::codec::InFrame;
use crate::session::responder::{Responder, StreamSendError};
use crate::session::{HandlerFactory, HandlerFn, SessionId};
use crate::tx::{
    CtlReply, ExecReply, TxCommand, TxHandle, TxLookupErr, TxRegistry, actor, next_tx_id,
};

/// Bound on a per-`tx_id` actor's command mpsc. Commands are processed one at a time; a modest
/// buffer absorbs a client that pipelines a few tx commands without waiting for each terminal, and
/// otherwise applies backpressure on the (spawned, per-request) forwarding handler tasks.
const TX_CMD_CHANNEL_CAP: usize = 16;

/// `ExecRequest.fetch` modes. 0 = rows, 1 = none (affected only), 2 = stream (M1-S5: the windowed
/// DATA-channel producer — autocommit path in Task 4b, tx-scoped path in Task 5). Kept as named
/// constants (not magic numbers) at the handler boundary.
///
/// `pub` since M1-S8a so an INTEGRATION test (a separate crate) can name the mode it is exercising
/// instead of writing a literal `2` — a hand-written protocol constant in a test is charter rule 2's
/// defect. Values and definition site are unchanged; this is a visibility change only.
pub const FETCH_ROWS: u8 = 0;
pub const FETCH_NONE: u8 = 1;
pub const FETCH_STREAM: u8 = 2;

/// Bytes `Outcome::Ok(body).encode()` prepends to `body`: the outer 2-element fixarray marker
/// (`0x92`) plus the 1-byte positive-fixint status (`outcome::OK`). The terminal size-check MUST
/// account for this envelope — checking the raw body instead lets a body in the `{MAX-1, MAX}` band
/// pass, then the wrapped frame overflows `MAX_FRAME_PAYLOAD`, `FrameCodec::encode` hard-errors, the
/// writer loop breaks, and the WHOLE session tears down (BLOCKER-v2 — a zero-END violation for this
/// AND every other in-flight terminal). Locked against the codec by `outcome_ok_overhead_is_two`.
const OUTCOME_OK_OVERHEAD: usize = 2;

/// Build the real SQL/TX `HandlerFactory`, capturing the pool registry, the shared
/// `Arc<TxRegistry>` (S6 seam), and the transaction deadlines. The factory mints one `HandlerFn`
/// per connection given its `SessionId` — which is load-bearing: it is the OWNER key every
/// tx-scoped request is checked against (a tx opened by one session is invisible to any other).
///
/// Routing (S6): `service=SQL, method=EXEC` with `tx_id == None` is the S5 autocommit path,
/// byte-for-byte unchanged; with `tx_id.is_some()` it is forwarded to that tx's actor. `service=TX`
/// BEGIN opens a tx (spawns the actor); COMMIT/ROLLBACK/SAVEPOINT/RELEASE/ROLLBACK_TO are forwarded
/// to the owning actor. Anything else declares `Unsupported`.
pub fn make_handler(
    registry: Arc<PoolRegistry>,
    tx_registry: Arc<TxRegistry>,
    idle_in_tx: Duration,
    max_tx: Duration,
    teardown_timeout: Duration,
) -> HandlerFactory {
    Arc::new(move |session_id| -> HandlerFn {
        let registry = registry.clone();
        let tx_registry = tx_registry.clone();
        Arc::new(move |frame, responder, cancel| {
            let registry = registry.clone();
            let tx_registry = tx_registry.clone();
            async move {
                handle(
                    frame,
                    responder,
                    &registry,
                    &tx_registry,
                    session_id,
                    idle_in_tx,
                    max_tx,
                    teardown_timeout,
                    cancel,
                )
                .await;
            }
            .boxed()
        })
    })
}

#[allow(clippy::too_many_arguments)]
async fn handle(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    idle_in_tx: Duration,
    max_tx: Duration,
    teardown_timeout: Duration,
    cancel: CancellationToken,
) {
    match (frame.header.service, frame.header.method) {
        (service::SQL, method_sql::EXEC) => {
            handle_exec(frame, responder, registry, tx_registry, session_id, cancel).await
        }
        (service::TX, method_tx::BEGIN) => {
            handle_begin(
                frame,
                responder,
                registry,
                tx_registry,
                session_id,
                idle_in_tx,
                max_tx,
                teardown_timeout,
            )
            .await
        }
        (service::TX, method_tx::COMMIT) => {
            handle_tx_control(frame, responder, tx_registry, session_id, CtlKind::Commit).await
        }
        (service::TX, method_tx::ROLLBACK) => {
            handle_tx_control(frame, responder, tx_registry, session_id, CtlKind::Rollback).await
        }
        (service::TX, method_tx::SAVEPOINT) => {
            handle_savepoint(frame, responder, tx_registry, session_id, SpKind::Savepoint).await
        }
        (service::TX, method_tx::RELEASE) => {
            handle_savepoint(frame, responder, tx_registry, session_id, SpKind::Release).await
        }
        (service::TX, method_tx::ROLLBACK_TO) => {
            handle_savepoint(
                frame,
                responder,
                tx_registry,
                session_id,
                SpKind::RollbackTo,
            )
            .await
        }
        // Any other routed frame (an unrecognized SQL/TX method, or STREAM) → one END, session lives.
        _ => responder.end_error(unsupported("service/method not yet implemented")),
    }
}

/// `service=SQL, method=EXEC`. Validates the request shape (shared by both paths), then branches on
/// `tx_id`: `None` → the S5 autocommit path (unchanged); `Some` → forward to the owning tx actor.
async fn handle_exec(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    cancel: CancellationToken,
) {
    // (1) decode the per-request payload.
    let req = match ExecRequest::decode(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(protocol(format!("malformed ExecRequest: {e}")));
            return;
        }
    };

    // (2) reject not-yet-supported request shapes (each: one END, session survives).
    if req.query_id.is_some() {
        responder.end_error(unsupported(
            "query_id (manifest execution) is post-M0 (M3); send inline sql",
        ));
        return;
    }
    match req.fetch {
        // FETCH_STREAM is honored on BOTH paths: the autocommit producer (below) and, since Task 5,
        // the tx-scoped producer forwarded to the owning actor (the `Some(tx_id)` arm).
        FETCH_ROWS | FETCH_NONE | FETCH_STREAM => {}
        other => {
            responder.end_error(unsupported(format!("unknown fetch mode {other}")));
            return;
        }
    }
    let sql = match &req.sql {
        Some(s) => s.as_str(),
        None => {
            responder.end_error(unsupported(
                "EXEC requires an inline sql statement in M0 (query_id manifests are M3)",
            ));
            return;
        }
    };

    match req.tx_id {
        // ---- tx-scoped EXEC: forward to the owning actor (the conn is already pinned) ----
        Some(tx_id) => {
            // `req.pool` is ignored here — the transaction is already pinned to its pool's conn.
            let handle = match resolve_active(tx_registry, tx_id, session_id) {
                Ok(h) => h,
                Err(ep) => {
                    responder.end_error(ep);
                    return;
                }
            };

            // M1-S8a: the SAME streaming authority the autocommit arm reads, captured at BEGIN.
            if req.fetch == FETCH_STREAM && !handle.streaming {
                // Refuse BEFORE handing the `Responder` to the actor: the actor would reach
                // `Checkout::query_stream`, whose Err arm force-taints the PINNED tx connection
                // (`ferro-pool/src/pool.rs`'s `finalize_stream(true, ..)`) for a request that never
                // should have been dispatched. Exactly one END either way (charter rule 4).
                responder.end_error(stream_unsupported());
                return;
            }

            // A tx-scoped streamed fetch (M1-S5 Task 5): the actor owns the pinned conn and
            // `query_stream` borrows its `&mut co`, so the FORWARDING HANDLER cannot stream — it hands
            // the `Responder` INTO the actor, which runs the shared producer, emits HEAD/DATA, and
            // declares the ONE terminal. The handler awaits a done-ack, then RETURNS so the supervisor
            // delivers that terminal strictly AFTER the last DATA (B4). The buffered path below is
            // untouched.
            if req.fetch == FETCH_STREAM {
                let (done_tx, done_rx) = oneshot::channel::<()>();
                let cmd = TxCommand::ExecStreamed {
                    sql: sql.to_string(),
                    params: req.params.clone(),
                    timeout_ms: req.timeout_ms,
                    readonly: req.readonly,
                    cancel,
                    responder,
                    done: done_tx,
                };
                match handle.cmd_tx.send(cmd).await {
                    // The actor now owns the `Responder` and will declare the terminal. Await its ack,
                    // then return. An `Err` (the actor dropped the ack, e.g. a panic) leaves the
                    // terminal to the supervisor's synthetic-terminal safety net — the `Responder` is
                    // gone, so there is nothing to declare here; just return (still exactly-one-END).
                    Ok(()) => {
                        let _ = done_rx.await;
                    }
                    // The actor is already gone: RECOVER the `Responder` from the un-sent command and
                    // declare the prompt terminal ourselves (we still hold it — exactly-one-END).
                    Err(mpsc::error::SendError(cmd)) => {
                        if let TxCommand::ExecStreamed { responder, .. } = cmd {
                            responder.end_error(actor_gone_terminal(
                                tx_registry,
                                tx_id,
                                session_id,
                            ));
                        }
                    }
                }
                return;
            }

            let (reply_tx, reply_rx) = oneshot::channel();
            // M1-S4: thread `req.timeout_ms` + this request's own per-request `cancel` token into
            // the actor — DISTINCT from the tx's session-level `abort` (see `TxCommand::Exec`'s
            // doc comment). `cancel` is moved: this `Some(tx_id)` arm and the autocommit `None` arm
            // below are mutually exclusive, so nothing else in this call needs it afterward.
            let cmd = TxCommand::Exec {
                sql: sql.to_string(),
                params: req.params.clone(),
                timeout_ms: req.timeout_ms,
                cancel,
                reply: reply_tx,
            };
            if handle.cmd_tx.send(cmd).await.is_err() {
                responder.end_error(actor_gone_terminal(tx_registry, tx_id, session_id));
                return;
            }
            match reply_rx.await {
                Ok(ExecReply::Completed { result, exec_us }) => match result {
                    // queue_us is 0: a pinned conn is never queued for.
                    Ok(qr) => match build_terminal_body(qr, req.fetch, 0, exec_us) {
                        Ok(body) => responder.end_ok(Bytes::from(body)),
                        Err(ep) => responder.end_error(ep),
                    },
                    // The statement WAS transmitted on the pinned conn (sent=true). This is an
                    // in-transaction user STATEMENT (in_tx=true), NOT a control boundary: a
                    // link-loss here means the WHOLE TRANSACTION is dead (known outcome — it will
                    // never commit) → Retryable, never Indeterminate for the statement itself. Do
                    // NOT pass in_tx=false here (that would wrongly report an in-tx write loss as
                    // Indeterminate, same as the autocommit path — but there is no separate write
                    // to be unconfirmed about: the tx as a whole cannot be salvaged either way).
                    Err(e) => responder.end_error(fate::classify_fate(
                        e,
                        OpContext {
                            readonly: req.readonly,
                            sent: true,
                            in_tx: true,
                        },
                    )),
                },
                // A deadline cancelled the statement mid-flight → the ONE TxDeadline terminal. The
                // statement is never re-run (charter rule 3).
                Ok(ExecReply::Deadline) => responder.end_error(tx_deadline(
                    "transaction deadline exceeded mid-statement; the statement was cancelled and \
                     the transaction rolled back (retryable — the engine never re-runs)",
                )),
                Err(_recv) => {
                    responder.end_error(actor_gone_terminal(tx_registry, tx_id, session_id))
                }
            }
        }

        // ---- autocommit EXEC: the S5 path (M1-S4 timeout_ms + CANCEL), dispatched over the
        //      heterogeneous pool registry (M1-S6) ----
        None => {
            let Some(pool) = registry.get(&req.pool) else {
                responder.end_error(unsupported(format!("unknown pool {:?}", req.pool)));
                return;
            };

            // ONE authority for the streaming capability (M1-S8a): the backend's own
            // `supports_row_streaming()`, not a `matches!(pool, AnyPool::Mysql(_))` restated here.
            // Still refused EARLY — before any checkout — so the client sees a precise Unsupported
            // rather than a mid-stream error surfaced from the backend's `query_stream`. The
            // buffered FETCH_ROWS/FETCH_NONE paths run normally; PG streaming is unchanged.
            let streams = match pool {
                AnyPool::Pg(p) => p.backend().supports_row_streaming(),
                AnyPool::Mysql(p) => p.backend().supports_row_streaming(),
            };
            if req.fetch == FETCH_STREAM && !streams {
                responder.end_error(stream_unsupported());
                return;
            }

            // Dispatch to the SAME generic body for whichever concrete backend the named pool is.
            // Both arms monomorphize `run_exec_on_pool`; the terminal is declared via the moved
            // `Responder` (no typed return), so the arms unify. PG's path is byte-for-byte the
            // pre-M1-S6 inline body.
            match pool {
                AnyPool::Pg(p) => run_exec_on_pool(p, responder, &req, sql, cancel).await,
                AnyPool::Mysql(p) => run_exec_on_pool(p, responder, &req, sql, cancel).await,
            }
        }
    }
}

/// The autocommit EXEC body, generic over the backend `B` — extracted verbatim from `handle_exec`'s
/// `None` (autocommit) arm so the heterogeneous pool registry (M1-S6) dispatches to ONE
/// implementation per `AnyPool` variant. PG's behavior is byte-for-byte the pre-M1-S6 inline body;
/// the MySQL monomorphization reuses it verbatim for the buffered `fetch:rows`/`none` paths (its
/// `fetch:stream` is rejected upstream in `handle_exec`, so the stream branch below is unreachable
/// for `B = MysqlBackend` — it stays only because the fn is generic and must compile for both).
async fn run_exec_on_pool<B: PoolBackend>(
    pool: &Pool<B>,
    responder: Responder,
    req: &ExecRequest,
    sql: &str,
    cancel: CancellationToken,
) {
    // M1-S5 Task 4b: a streamed fetch runs the incremental HEAD + DATA×N producer under the credit
    // window instead of buffering the whole result into the terminal (D-S5-1). The buffered
    // FETCH_ROWS/FETCH_NONE path below is untouched. (Reached only for PG — a MySQL stream is
    // rejected in `handle_exec` before dispatch.)
    if req.fetch == FETCH_STREAM {
        run_autocommit_streamed(
            responder,
            pool,
            sql,
            &req.params,
            req.timeout_ms,
            req.readonly,
            &cancel,
            StreamBatch::DEFAULT,
        )
        .await;
        return;
    }

    // (3) checkout → queue_us (the pool-wait, including any recycle cleanup on the popped conn).
    let mut co = match pool.checkout().await {
        Ok(co) => co,
        Err(e) => {
            // A checkout failure means NO connection was established, so the user statement was
            // NEVER transmitted → known "did-not-apply", never Indeterminate (sent=false). This is
            // the autocommit path: in_tx=false.
            responder.end_error(fate::classify_fate(
                e,
                OpContext {
                    readonly: req.readonly,
                    sent: false,
                    in_tx: false,
                },
            ));
            return;
        }
    };
    let queue_us = co.stats().queue_us;

    // (4) run the GUARDED, INTERRUPTIBLE row-returning entry: enforces `timeout_ms` + the
    // per-request CANCEL via a biased select that polls the query FIRST, so `sent` is honest for
    // whatever `Err` comes back (see `run_autocommit_exec`'s doc). exec_us times ONLY the DB call —
    // the conn is released (below) before framing, so a slow client cannot inflate it (D-S5-1).
    // NEVER conn_mut()/the raw client here (that bypasses the tx-control guard → cross-tenant leak).
    let (result, exec_us) =
        run_autocommit_exec(&mut co, sql, &req.params, req.timeout_ms, &cancel).await;

    // Release the pooled connection BEFORE framing/sending (RAII): held only for the query. A
    // cancelled/timed-out statement returns Err(57014) here, which `Checkout::query`'s S1
    // unconditional Err-arm fail-safe ALREADY taints regardless of the RFQ byte — so `co` is never
    // handed back dirty; the next checkout's S3 recycle DISCARD-ALLs it before the next tenant.
    drop(co);

    // (5)+(6) Ok → the real success terminal, even if a cancel/timeout raced it and lost
    // (§5.2/§19.3 — never fabricate an error for a statement that actually completed); Err →
    // `fate::classify_fate` with the HONEST `sent: true`. This is the autocommit path: in_tx=false.
    declare_autocommit_exec(
        responder,
        result,
        req.fetch,
        queue_us,
        exec_us,
        req.readonly,
    );
}

/// Run one autocommit statement against `co`, honoring `timeout_ms` and the per-request `cancel`
/// token via a `biased` `tokio::select!` that polls the query future FIRST — mirroring the S6 tx
/// actor's proven pattern (`tx/actor.rs`'s `TxCommand::Exec` inner select!, ~247-273) exactly,
/// just without the actor's rollback/tombstone teardown (there is no transaction here).
///
/// **Why `sent` is honest.** Because the query is polled first every round, a timeout/cancel arm
/// can only ever WIN a round in which the query was ALSO polled (and was not yet ready) — so by
/// the time either arm wins, the statement has already been dispatched to the backend at least
/// once. This is what licenses the caller to report `sent: true` for whatever `Err` this function
/// returns, without hardcoding it blind (a `sent: true` that were NOT honestly earned would let an
/// unsent write be misreported as `Indeterminate` instead of the correct `Retryable`).
///
/// **Drain, don't drop.** On a timeout/cancel win, the out-of-band [`Cancel`] handle — captured
/// BEFORE the query borrow, exactly like the actor, since a live `tokio-postgres` query future
/// holds `&mut Client` and the cancel MUST fire over a SEPARATE connection — fires, then the query
/// future is AWAITED to its completion (never dropped: a dropped mid-flight statement's fate would
/// be unknowable, and could leave the connection mid-protocol). The drained value is a genuine
/// `Result`, not a synthesized one:
/// - `Ok(qr)` — the cancel/timeout LOST the race: the statement actually completed. The caller
///   MUST treat this as a real success (§5.2/§19.3 — never fabricate a cancel/error for a
///   statement that finished).
/// - `Err(e)` — the statement's real outcome. A cancelled/timed-out statement surfaces PG's
///   `57014`, which `fate::classify_fate`'s override then routes by `readonly`/`in_tx`.
///
/// Returns `(drained result, exec_us)`; `exec_us` measures only this call, matching the
/// pre-existing `build_terminal_body` stats contract.
async fn run_autocommit_exec<B: PoolBackend>(
    co: &mut Checkout<B>,
    sql: &str,
    params: &[Value],
    timeout_ms: Option<u32>,
    cancel: &CancellationToken,
) -> (Result<QueryResult, PoolError>, u64) {
    // Capture the out-of-band cancel handle BEFORE the mutable query borrow: it returns an OWNED
    // handle, so this borrow of `co` ends immediately and does not conflict with the `&mut co` the
    // query future then holds (mirrors `tx/actor.rs`'s `co.cancel_handle()` at ~247).
    let cancel_handle = co.cancel_handle();
    let exec_start = Instant::now();
    let query_fut = co.query(sql, params);
    tokio::pin!(query_fut);

    let result = tokio::select! {
        biased;

        // Polled FIRST every round: a statement that completes is never spuriously reported as
        // interrupted, even against an already-fired timer/cancel — this is both what makes `sent`
        // honest (see the doc above) and what makes the Ok-lost-race case possible at all.
        r = &mut query_fut => r,

        () = sleep_opt(timeout_ms) => {
            cancel_handle.cancel().await;
            (&mut query_fut).await
        }
        () = cancel.cancelled() => {
            cancel_handle.cancel().await;
            (&mut query_fut).await
        }
    };
    (result, exec_start.elapsed().as_micros() as u64)
}

/// `Some(ms)` → a real `tokio::time::sleep` deadline; `None` → a future that NEVER resolves (NOT a
/// 0ms timer) so [`run_autocommit_exec`]'s timeout arm is effectively absent when the caller
/// passed no deadline — a `timeout_ms: None` request must behave exactly as before M1-S4.
async fn sleep_opt(ms: Option<u32>) {
    match ms {
        Some(ms) => tokio::time::sleep(Duration::from_millis(u64::from(ms))).await,
        None => std::future::pending().await,
    }
}

/// Declare the autocommit EXEC terminal from a [`run_autocommit_exec`] result: `Ok` frames the
/// normal success terminal via [`build_terminal_body`] — a statement that actually completed is
/// ALWAYS reported as success, even if a cancel/timeout raced it and lost; `Err` maps through
/// [`fate::classify_fate`] with `sent: true` (honest — see `run_autocommit_exec`'s doc) and
/// `in_tx: false` (this is the autocommit path, never an in-transaction statement).
fn declare_autocommit_exec(
    responder: Responder,
    result: Result<QueryResult, PoolError>,
    fetch: u8,
    queue_us: u64,
    exec_us: u64,
    readonly: bool,
) {
    match result {
        Ok(r) => match build_terminal_body(r, fetch, queue_us, exec_us) {
            Ok(body) => responder.end_ok(Bytes::from(body)),
            Err(ep) => responder.end_error(ep),
        },
        Err(e) => responder.end_error(fate::classify_fate(
            e,
            OpContext {
                readonly,
                sent: true,
                in_tx: false,
            },
        )),
    }
}

// ============================ M1-S5 Task 4b: the streaming producer ============================

/// Batch flush policy for [`run_streamed_exec`]: accumulate pulled rows and flush the batch as ONE
/// `STREAM/DATA` frame when EITHER `max_rows` rows OR `max_bytes` (estimated) payload bytes have
/// accumulated — whichever trips first — keeping each DATA frame comfortably under
/// `MAX_FRAME_PAYLOAD` while amortizing per-frame overhead. `max_bytes` is a SOFT target measured by
/// a cheap per-row estimate ([`estimate_row_bytes`]); the HARD per-frame ceiling is still enforced
/// by `Responder::send_data`'s `Oversized` check. Passing the budget in (rather than a hidden const)
/// is what makes the DATA-frame count observable — a test forces a known count with `max_rows: 1`.
#[derive(Debug, Clone, Copy)]
struct StreamBatch {
    max_rows: usize,
    max_bytes: usize,
}

impl StreamBatch {
    /// Production default: ~a thousand rows or ~256 KiB per DATA frame — well under the 16 MiB
    /// `MAX_FRAME_PAYLOAD`, so the credit window and session cap flow smoothly and no single batch
    /// of typical rows risks the codec ceiling.
    const DEFAULT: StreamBatch = StreamBatch {
        max_rows: 1024,
        max_bytes: 256 * 1024,
    };
}

/// How a `fetch:stream` producer ([`run_streamed_exec`]) ended, for a caller that must decide
/// follow-up teardown — the tx actor (via [`run_tx_streamed`]). The autocommit caller ignores it
/// (there is no transaction to keep or tear down). EITHER way, `run_streamed_exec` has ALREADY
/// declared the request's ONE terminal via the consumed `Responder` — this only tells a tx caller
/// whether the transaction survives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEnded {
    /// The statement reached its OWN conclusion: the stream drained cleanly (an `Ok` terminal) or hit
    /// a natural, NON-cancel mid-stream error the client may still handle itself (an `Error`
    /// terminal). A tx-scoped caller keeps the transaction OPEN — mirroring the buffered path, which
    /// does NOT auto-roll-back a 23505 (`non_cancel_statement_error_reported_without_auto_rollback`).
    Intact,
    /// The statement was INTERRUPTED (cancel / deadline / backpressure-unwind / lost link) OR
    /// surfaced a `57014` — so the §19.3 uniform in-tx action applies: a tx-scoped caller must ROLL
    /// BACK (and, per the deadline-vs-abort split, tombstone). The ONE fated terminal is declared.
    Broken,
}

/// One iteration of [`run_streamed_exec`]'s outer `biased` select over cancel / deadline / the pull.
enum StreamStep {
    /// The pull resolved: `Some(Ok(row))` per row, `Some(Err(_))` mid-stream error, `None` drained.
    Row(Option<Result<Vec<Value>, PoolError>>),
    /// The request's `cancel` token fired mid-stream (a routed CANCEL or session teardown).
    Cancelled,
    /// The request's `timeout_ms` deadline passed mid-stream.
    Deadline,
}

/// The autocommit `fetch:stream` producer entry: checkout, capture the out-of-band cancel handle
/// BEFORE the stream borrow (mirroring [`run_autocommit_exec`]/the tx actor — a live query future
/// holds `&mut Client`, so the cancel MUST fire over a side connection), open the incremental
/// [`Checkout::query_stream`], then hand off to the shared [`run_streamed_exec`] loop. `co` is held
/// for the WHOLE stream (the `RowStreamHandle` borrows it) and drops only AFTER `finish()` inside
/// the loop — the reverse of the buffered path, where the conn is released before framing.
#[allow(clippy::too_many_arguments)]
async fn run_autocommit_streamed<B: PoolBackend>(
    responder: Responder,
    pool: &Pool<B>,
    sql: &str,
    params: &[Value],
    timeout_ms: Option<u32>,
    readonly: bool,
    cancel: &CancellationToken,
    batch: StreamBatch,
) {
    // A checkout failure means NO connection was established → the statement was NEVER transmitted
    // (sent=false), a known did-not-apply; autocommit (in_tx=false). Same fate as the buffered path.
    let mut co = match pool.checkout().await {
        Ok(co) => co,
        Err(e) => {
            responder.end_error(fate::classify_fate(
                e,
                OpContext {
                    readonly,
                    sent: false,
                    in_tx: false,
                },
            ));
            return;
        }
    };

    // Two out-of-band cancel handles, captured BEFORE the stream borrows `co` (each returns an OWNED
    // handle firing the SAME connection's cancel, so two is harmless): one for the OPEN race
    // (`open_streamed_raced`), one for the pull→send loop (`run_streamed_exec`). A single owned handle
    // can't serve both — it's fire-once by value — and the open and the loop are distinct interruption
    // points. Identical capture-before-borrow discipline to `run_autocommit_exec`/`tx/actor.rs`.
    let cancel_handle_open = co.cancel_handle();
    let cancel_handle_loop = co.cancel_handle();
    // A single fixed deadline shared by the OPEN race, the outer loop select, AND every
    // send_head/send_data flow-control wait, so `timeout_ms` bounds the WHOLE request (open, mid-pull,
    // and mid-backpressure alike).
    let deadline =
        timeout_ms.map(|ms| tokio::time::Instant::now() + Duration::from_millis(u64::from(ms)));
    let ctx = OpContext {
        readonly,
        // `sent: true` from the OPEN onward: `query_stream` returning Ok means `query_raw` already
        // dispatched the statement (rows then pull lazily off a channel), so the statement is in
        // flight — identical to the buffered path's unconditional `sent: true` for any post-dispatch
        // error. See `run_streamed_exec`'s doc for the §19.3 safety argument.
        sent: true,
        in_tx: false,
    };

    // M7: exec_us times ONLY backend-pull awaits — the `query_stream` OPEN (prepare + query_raw
    // dispatch, a real round-trip) is the first such await. `timeout_ms` + CANCEL are ENFORCED across
    // it via `open_streamed_raced` (see its doc for why draining is not type-expressible here).
    //
    // The open-race + loop live in an INNER BLOCK returning a plain `bool` (taint?), so every
    // co-BORROWING value (the raced `Result<RowStreamHandle<'_>, _>` and the handle) is confined to
    // the block; `co` is free again after it, which lets the taint below take `&mut co`. (A top-level
    // `let outcome = ...` would keep `co` borrowed through the taint — E0499/E0505.)
    let open_start = tokio::time::Instant::now();
    let needs_taint = {
        let outcome =
            open_streamed_raced(&mut co, sql, params, cancel, deadline, cancel_handle_open).await;
        let open_us = open_start.elapsed().as_micros() as u64;
        match outcome {
            Ok(handle) => {
                run_streamed_exec(
                    handle,
                    responder,
                    cancel,
                    deadline,
                    cancel_handle_loop,
                    ctx,
                    open_us,
                    batch,
                )
                .await;
                false
            }
            Err(e) => {
                // Either the open failed on its own (`query_stream` `Err` — already finalized+tainted
                // internally by Task 3) OR it was aborted by cancel/deadline (the future was dropped,
                // so Task 3's finalize did NOT run). Declare the one fated terminal here; TAINT below.
                responder.end_error(fate::classify_fate(e, ctx));
                true
            }
        }
    };

    if needs_taint {
        // Taint UNCONDITIONALLY on any open Err: idempotent on the already-tainted natural-failure
        // conn, and the load-bearing recycle trigger on an aborted-open conn (the next checkout's
        // bounded DISCARD ALL resyncs it — the equivalent-safety substitute for draining, which
        // cannot type-check here; see `open_streamed_raced`'s doc).
        co.set_tainted(true);
    }

    // `co` drops HERE (RAII), only after the loop's `handle.finish()` released the `&mut co` borrow
    // (or the taint above) — the conn returns to the pool correctly pinned/tainted (Task 3).
    drop(co);
}

/// The tx-scoped `fetch:stream` producer (M1-S5 Task 5): the tx-actor analog of
/// [`run_autocommit_streamed`], run against the actor's ALREADY-pinned `co` (never a fresh checkout —
/// the conn is pinned to the tx for its whole life). Captures the out-of-band cancel handles BEFORE
/// the stream borrow, races the `query_stream` OPEN via [`open_streamed_raced`], then hands off to the
/// SHARED [`run_streamed_exec`] with `in_tx: true` — so a mid-stream cancel/timeout classifies
/// `TxDeadline{Retryable}` (§19.3) and the credit/cap/ordering/fate logic is NOT duplicated. The ONE
/// terminal is declared INSIDE (via the moved `responder`); the returned [`StreamEnded`] tells the
/// actor whether to keep the tx open (`Intact`) or roll it back + tombstone (`Broken`).
///
/// Unlike the autocommit path, `co` is NOT dropped here — it stays pinned to the tx; the actor's own
/// teardown releases it. `cancel`/`deadline` are the actor's ALREADY-COMBINED signals (request cancel
/// OR session abort; `min(timeout_ms, max_tx)`), so this function stays identical in shape to the
/// autocommit producer — it just cannot own/return the checkout.
///
/// On an aborted open the conn is TAINTED (the actor's teardown ROLLBACK then resets/evicts it). A
/// NATURAL non-`57014` open error (a syntax/bind fault) leaves the tx OPEN — `query_stream`'s Err arm
/// already finalized+tainted it, and the client may `ROLLBACK` itself, mirroring the buffered
/// non-cancel statement-error path — so it is NOT forced to `Broken`.
pub(crate) async fn run_tx_streamed<B: PoolBackend>(
    co: &mut Checkout<B>,
    sql: &str,
    params: &[Value],
    responder: Responder,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    readonly: bool,
) -> StreamEnded {
    // Two out-of-band cancel handles captured BEFORE the stream borrows `co` (each owned, firing the
    // SAME connection's cancel): one for the OPEN race, one for the pull→send loop. Identical
    // capture-before-borrow discipline to `run_autocommit_streamed`/`tx/actor.rs`.
    let cancel_handle_open = co.cancel_handle();
    let cancel_handle_loop = co.cancel_handle();
    // `sent: true` from the OPEN onward (see `run_streamed_exec`'s doc); `in_tx: true` so the fate of
    // any cancel/timeout/loss is the in-tx `TxDeadline{Retryable}`/known-tx-dead classification.
    let ctx = OpContext {
        readonly,
        sent: true,
        in_tx: true,
    };

    // The open-race + loop live in a block confining every co-BORROWING value (the raced handle), so
    // `co` is free again after it for the `set_tainted` below (mirrors `run_autocommit_streamed`).
    let open_start = tokio::time::Instant::now();
    let (ended, needs_taint) = {
        let outcome =
            open_streamed_raced(co, sql, params, cancel, deadline, cancel_handle_open).await;
        let open_us = open_start.elapsed().as_micros() as u64;
        match outcome {
            Ok(handle) => {
                let ended = run_streamed_exec(
                    handle,
                    responder,
                    cancel,
                    deadline,
                    cancel_handle_loop,
                    ctx,
                    open_us,
                    StreamBatch::DEFAULT,
                )
                .await;
                (ended, false)
            }
            Err(e) => {
                // Distinguish an ABORTED open / app-`57014` (the tx is dead → `Broken` + taint) from a
                // natural NON-cancel open error (syntax/bind — the tx block is app-aborted but the
                // client may `ROLLBACK` itself → `Intact`, exactly like the buffered non-57014
                // statement error). The ONE terminal is `classify_fate(e)` either way.
                let broken = fate::is_57014(&e);
                responder.end_error(fate::classify_fate(e, ctx));
                if broken {
                    // Taint UNCONDITIONALLY: idempotent if `query_stream`'s Err arm already tainted a
                    // natural 57014, load-bearing if a raced/aborted open DROPPED the future before
                    // its finalize ran (the aborted-open path — the taint is the recycle trigger, the
                    // equivalent-safety substitute for draining, exactly as the autocommit path does).
                    (StreamEnded::Broken, true)
                } else {
                    (StreamEnded::Intact, false)
                }
            }
        }
    };
    if needs_taint {
        co.set_tainted(true);
    }
    ended
}

/// Race the incremental `query_stream` OPEN (prepare + `query_raw` dispatch — real backend
/// round-trips) against `cancel`/`deadline`, so `timeout_ms` + a routed CANCEL are ENFORCED across
/// the open the same way `run_autocommit_exec` enforces them across the buffered query. Pull-first
/// (the open arm is polled FIRST, `biased`), so a just-completed open never loses to a
/// simultaneously-ready cancel/deadline (§5.2 — never fabricate an abort for a statement that
/// finished). On abort it fires the out-of-band `cancel_handle` (stop the dispatched server work)
/// and returns `Err(57014)`; the CALLER then taints `co` (the aborted open's future is dropped, not
/// drained — see below — so nothing else taints it) and declares the one fated terminal.
///
/// **Why drop-not-drain (deviation from the review's literal ask, with compiler evidence).** The
/// buffered `run_autocommit_exec` drains its query future because its output (`QueryResult`) is
/// OWNED. Here the open's output — `RowStreamHandle<'a>` — BORROWS `co`, so a pinned open future
/// (needed to `(&mut fut).await`-drain on abort) cannot coexist with using the extracted handle:
/// `rustc` rejects it with **E0505** ("cannot move out of `co` … borrowed … when `open_fut` is
/// dropped and runs the destructor"). Draining-then-extracting is simply not type-expressible when
/// the handle borrows the checkout. So the open is raced BY VALUE (dropped on abort); the equivalent
/// safety comes from firing the cancel + the caller's `co.set_tainted(true)` (a bounded DISCARD-ALL
/// recycle at the next checkout). tokio-postgres's connection task drains the dropped request's
/// server response, so the connection does not desync; the taint guarantees no cross-tenant leak.
///
/// Reused by [`run_tx_streamed`] (Task 5 — the tx-actor open needs the SAME guard): same shape, with
/// the actor supplying its own pinned `Checkout`, combined `cancel`/`deadline`, and an in-tx `OpContext`.
async fn open_streamed_raced<'a, B: PoolBackend>(
    co: &'a mut Checkout<B>,
    sql: &str,
    params: &[Value],
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    cancel_handle: B::CancelHandle,
) -> Result<RowStreamHandle<'a, B>, PoolError> {
    tokio::select! {
        biased;
        // Polled FIRST: a completed open wins a tie over a ready cancel/deadline (§5.2).
        r = co.query_stream(sql, params) => r,
        () = sleep_until_opt(deadline) => {
            cancel_handle.cancel().await;
            Err(stream_cancel_error())
        }
        () = cancel.cancelled() => {
            cancel_handle.cancel().await;
            Err(stream_cancel_error())
        }
    }
}

/// The SHARED pull→send loop (called by BOTH the autocommit producer and, via [`run_tx_streamed`],
/// the tx actor): emit HEAD once, then pull
/// rows incrementally and flush them as DATA frames under the credit window, then exactly ONE
/// terminal — declared by this function via the consumed `responder`. Exactly-one-END and the §19.3
/// fate rules hold on EVERY exit.
///
/// **M7 (exec_us excludes backpressure).** `exec_us` starts at `open_us` (the caller-timed
/// `query_stream` open) and is incremented ONLY around each `handle.next()` pull — NEVER around
/// `send_head`/`send_data`, which include the credit/cap/channel waits. The strictly-sequential
/// pull-then-send loop makes this exact: no pull overlaps a send.
///
/// **Cancel/abort sequence.** On any abort — the outer `biased` select observing `cancel`/`deadline`
/// (even mid-pull), a `handle.next()` `Err`, or a `send_head`/`send_data` `StreamSendError` — the
/// loop STOPS producing and routes to [`abort_stream`]: fire the out-of-band `cancel_handle` (drain
/// the running query at the server), `handle.finish()` (drain to RFQ + hygiene), then ONE terminal
/// from `classify_fate` of the ABORT reason under `ctx`. A streamed READ never becomes
/// `Indeterminate` (classify_fate routes it by `readonly`).
///
/// **`ctx.sent` is `true` for the whole loop** and NEVER re-derived from "how many DATA frames went
/// out". Reaching this function at all means `query_stream` returned Ok, i.e. `query_raw` already
/// dispatched the statement (see `run_autocommit_streamed`); rows then pull lazily off a channel,
/// but the statement is already executing server-side. So a cancel/timeout/loss at ANY point in the
/// loop — even before the first DATA frame — is a dispatched-statement event, and a streamed WRITE's
/// fate is UNKNOWN → `Indeterminate` (§19.3, the "defining safety property"). Gating `sent` on
/// DATA-delivered would mint an UNSAFE `Retryable` for a write cancelled after dispatch but before
/// its first row (a double-apply hazard). This mirrors the buffered path, which declares `sent: true`
/// for any post-dispatch error unconditionally.
#[allow(clippy::too_many_arguments)]
async fn run_streamed_exec<B: PoolBackend>(
    mut handle: RowStreamHandle<'_, B>,
    responder: Responder,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    cancel_handle: B::CancelHandle,
    ctx: OpContext,
    open_us: u64,
    batch: StreamBatch,
) -> StreamEnded {
    let mut exec_us = open_us;
    // Total HEAD + DATA payload bytes actually enqueued — reported as `stats.bytes` (item 4: the
    // buffered path reports its full result-body size, so the streamed path reports the full shipped
    // HEAD+DATA size, not the tiny rows-less terminal envelope). `send_head`/`send_data` return the
    // payload length they enqueued so this is exact without re-encoding.
    let mut sent_bytes: u64 = 0;

    // (1) HEAD: the column metadata, once, before any DATA. Grab `cols` before `finish()` consumes
    // the handle. A StreamSendError here (a cancelled/deadlined flow-control wait, an oversized cols
    // frame, or a lost link) → the abort path with `fire_cancel: true` (the statement is already
    // dispatched — the out-of-band cancel stops the server producing rows we will never read).
    let cols = handle.cols().to_vec();
    match responder.send_head(&cols, cancel, deadline).await {
        Ok(n) => sent_bytes += n as u64,
        Err(e) => {
            abort_stream(
                handle,
                responder,
                cancel_handle,
                ctx,
                true,
                send_err_to_pool_error(e),
            )
            .await;
            return StreamEnded::Broken;
        }
    }

    // (2) pull → batch → send_data loop.
    let mut batch_rows: Vec<Vec<Value>> = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut streamed_rows: u64 = 0;

    loop {
        // Pull FIRST in the `biased` select (item 3, §5.2 — like `run_autocommit_exec` polls the
        // query first): a slow pull is Pending so cancel/deadline still get polled (fully
        // interruptible), but on a TIE — the stream draining to `None` in the same poll a
        // cancel/deadline becomes ready — the completed stream wins and ends in `end_ok`, never a
        // fabricated error terminal for a query that finished. exec_us times ONLY the pull (M7).
        let pull_start = tokio::time::Instant::now();
        let step = tokio::select! {
            biased;
            row = handle.next() => StreamStep::Row(row),
            () = cancel.cancelled() => StreamStep::Cancelled,
            () = sleep_until_opt(deadline) => StreamStep::Deadline,
        };
        exec_us += pull_start.elapsed().as_micros() as u64;

        match step {
            StreamStep::Cancelled | StreamStep::Deadline => {
                // The server is still producing rows we will never read → FIRE the out-of-band cancel.
                abort_stream(
                    handle,
                    responder,
                    cancel_handle,
                    ctx,
                    true,
                    stream_cancel_error(),
                )
                .await;
                return StreamEnded::Broken;
            }
            StreamStep::Row(Some(Err(e))) => {
                // The statement SELF-terminated (that is why `next()` yielded `Err`) → do NOT fire
                // the out-of-band cancel (item 2): opening a side connection to `CancelRequest` an
                // already-errored/idle backend is pure waste. `run_autocommit_exec` likewise only
                // cancels on its interruption arms, never on a natural query error.
                //
                // A 57014 surfacing HERE (an app-set `statement_timeout` firing mid-stream) is the
                // in-tx `Broken` case — it must roll back + tombstone exactly like the buffered
                // `ExecStep::Completed` bare-57014 path; any OTHER natural error (23505, a computed
                // 22012, …) leaves the tx block app-aborted but recoverable by the client, so the tx
                // stays OPEN (`Intact`) — matching the buffered non-cancel statement-error path. The
                // ONE terminal is `classify_fate(e)` either way (correct for both: 57014→TxDeadline,
                // 23505→the Sql passthrough).
                let broken = fate::is_57014(&e);
                abort_stream(handle, responder, cancel_handle, ctx, false, e).await;
                return if broken {
                    StreamEnded::Broken
                } else {
                    StreamEnded::Intact
                };
            }
            StreamStep::Row(Some(Ok(row))) => {
                batch_bytes += estimate_row_bytes(&row);
                batch_rows.push(row);
                streamed_rows += 1;
                if batch_rows.len() >= batch.max_rows || batch_bytes >= batch.max_bytes {
                    let to_send = std::mem::take(&mut batch_rows);
                    batch_bytes = 0;
                    match responder.send_data(to_send, cancel, deadline).await {
                        Ok(n) => sent_bytes += n as u64,
                        Err(e) => {
                            abort_stream(
                                handle,
                                responder,
                                cancel_handle,
                                ctx,
                                true,
                                send_err_to_pool_error(e),
                            )
                            .await;
                            return StreamEnded::Broken;
                        }
                    }
                }
            }
            StreamStep::Row(None) => {
                // The stream drained: flush any final partial batch, then finish → the ONE Ok
                // terminal. The handler future resolves only after this returns, so the supervisor's
                // terminal is ordered strictly AFTER the last DATA (B4).
                if !batch_rows.is_empty() {
                    let to_send = std::mem::take(&mut batch_rows);
                    match responder.send_data(to_send, cancel, deadline).await {
                        Ok(n) => sent_bytes += n as u64,
                        Err(e) => {
                            abort_stream(
                                handle,
                                responder,
                                cancel_handle,
                                ctx,
                                true,
                                send_err_to_pool_error(e),
                            )
                            .await;
                            return StreamEnded::Broken;
                        }
                    }
                }
                return match handle.finish().await {
                    Ok(end) => {
                        let body = build_stream_terminal_body(
                            end.affected,
                            // `StreamEnd` carries no generated key: PG (the only streaming backend)
                            // has no LAST_INSERT_ID protocol field, and MySQL streaming is deferred
                            // (§22.2 (n)). A streaming backend that reports one wires it HERE.
                            None,
                            streamed_rows,
                            end.stats.queue_us,
                            exec_us,
                            sent_bytes,
                        );
                        responder.end_ok(Bytes::from(body));
                        StreamEnded::Intact
                    }
                    // `finish()` currently always returns Ok (a late drain error force-taints but
                    // returns Ok); handle a future Err defensively — a read never goes Indeterminate.
                    // A late drain error means the conn is in trouble → a tx caller must roll back.
                    Err(e) => {
                        responder.end_error(fate::classify_fate(e, ctx));
                        StreamEnded::Broken
                    }
                };
            }
        }
    }
}

/// The single abort exit for [`run_streamed_exec`]: STOP producing, OPTIONALLY fire the out-of-band
/// backend cancel, `finish()` the handle (drain to RFQ + run hygiene; `finish` force-taints a late
/// drain error but returns Ok — we classify from `err`, the ABORT reason, not from finish's return),
/// then declare the ONE terminal via `classify_fate` under `ctx` (`sent: true` — the statement is
/// dispatched; see [`run_streamed_exec`]'s doc). A streamed READ never becomes `Indeterminate`
/// (classify_fate routes it by `readonly`).
///
/// `fire_cancel` (item 2): `true` for an interruption abort (cancel/deadline/backpressure-unwind —
/// the server is still producing rows we will never read, so the out-of-band `CancelRequest` stops
/// it); `false` for a natural mid-stream `next()` `Err` — the statement already self-terminated (that
/// is why it erred), so opening a side connection to cancel an already-errored/idle backend is pure
/// waste. Mirrors `run_autocommit_exec`, which cancels only on its interruption arms.
async fn abort_stream<B: PoolBackend>(
    handle: RowStreamHandle<'_, B>,
    responder: Responder,
    cancel_handle: B::CancelHandle,
    ctx: OpContext,
    fire_cancel: bool,
    err: PoolError,
) {
    if fire_cancel {
        cancel_handle.cancel().await;
    }
    let _ = handle.finish().await;
    responder.end_error(fate::classify_fate(err, ctx));
}

/// The terminal `Outcome::Ok` body for a finished `fetch:stream`: the rows already went out as DATA
/// frames and the cols as the HEAD frame, so the terminal carries ONLY `affected` + `stats` (no
/// cols, no rows). `stats.rows` is the TOTAL streamed row count and `stats.bytes` is the TOTAL
/// HEAD+DATA payload bytes shipped (item 4 — consistent with the buffered path reporting its full
/// result-body size, NOT the tiny rows-less terminal envelope). Both are accumulated by the producer
/// loop, so — unlike the buffered `build_terminal_body` — no `stats.bytes` fixed-point is needed
/// (the value is not self-referential: it counts the DATA-channel frames, not this terminal frame).
/// No one-frame size-check is needed either — with no rows and no cols the terminal body is tiny.
///
/// `last_insert_id` rides the SAME [`last_insert_id_value`] conversion as the buffered terminal so
/// the two can never disagree on the tag. It is `None` on every streaming backend TODAY — the only
/// one is PG, which has no such protocol field, and MySQL row streaming is deferred (§22.2 (n)) —
/// but it is a parameter rather than a hardcoded `None` so wiring a future streaming backend that
/// DOES report one is an edit at its call site, not a silent drop here.
fn build_stream_terminal_body(
    affected: u64,
    last_insert_id: Option<u64>,
    streamed_rows: u64,
    queue_us: u64,
    exec_us: u64,
    stream_bytes: u64,
) -> Vec<u8> {
    ExecOk {
        cols: Vec::new(),
        rows: Vec::new(),
        affected,
        last_insert_id: last_insert_id_value(last_insert_id),
        stats: Stats {
            queue_us,
            exec_us,
            rows: streamed_rows,
            bytes: stream_bytes,
        },
    }
    .encode()
}

/// A cheap per-row byte estimate used ONLY to bound batch size (see [`StreamBatch`]); the exact
/// per-frame ceiling is enforced by `Responder::send_data`'s `Oversized` check, so this need only be
/// roughly proportional to the encoded size, never exact.
fn estimate_row_bytes(row: &[Value]) -> usize {
    row.iter()
        .map(|v| match v {
            Value::Null | Value::Bool(_) => 1,
            Value::I64(_) | Value::F64(_) => 9,
            Value::Text(s) => s.len() + 5,
            Value::Bytes(b) => b.len() + 5,
            // M1-S7: `U64` is the uint ladder (≤ 9 bytes, same as `I64`); every other canonical tag
            // rides the `str` family, so its cost is length-proportional — worst-case `str32`
            // header (5) + the tag. A flat catch-all here would UNDER-size the soft batch bound for
            // a large JSON document and let a valid request trip `send_data`'s hard `Oversized`
            // check mid-stream.
            Value::U64(_) => 9,
            Value::Decimal(s)
            | Value::Date(s)
            | Value::Time(s)
            | Value::Timestamp(s)
            | Value::TimestampTz(s)
            | Value::Uuid(s)
            | Value::Json(s) => s.len() + 5,
        })
        .sum::<usize>()
        + 2
}

/// A 57014-shaped cancel `PoolError` for a stream aborted by cancel / deadline / a backpressure
/// unwind. `fate::classify_fate`'s `is_57014` override then routes it by `readonly`/`sent`/`in_tx`
/// exactly as it routes the buffered path's drained cancel: a streamed autocommit READ →
/// `Cancelled{NonRetryable}`, a dispatched streamed autocommit WRITE → `WriteUnconfirmed{Indeterminate}`,
/// a tx-scoped stream (Task 5) → `TxDeadline{Retryable}`.
fn stream_cancel_error() -> PoolError {
    PoolError::Sql {
        code: errc::CANCELLED,
        branch: errc::CANCELLED_BRANCH,
        sqlstate: Some("57014".to_string()),
        // Engine-minted, not a server answer: there is no vendor errno to report.
        errno: None,
        message: "streamed statement cancelled or timed out".to_string(),
    }
}

/// Map a `StreamSendError` (a failed `send_head`/`send_data`) to the `PoolError` the abort path
/// classifies from: a flow-control OR final-channel-send abort is a cancel/timeout (57014); an
/// oversized single row is the §5.2 large-row ceiling; a closed control channel is a lost link.
fn send_err_to_pool_error(e: StreamSendError) -> PoolError {
    match e {
        StreamSendError::Aborted(_) => stream_cancel_error(),
        StreamSendError::Oversized => PoolError::Unsupported(
            "a streamed row exceeds one frame (MAX_FRAME_PAYLOAD); large-row streaming is post-M1"
                .to_string(),
        ),
        StreamSendError::LinkLost => PoolError::ConnectionLost,
    }
}

/// Await `deadline` if there is one, else never resolve — the `Instant`-based sibling of
/// [`sleep_opt`], for [`run_streamed_exec`]'s outer select (a single fixed `Instant` deadline is
/// shared with every `send_head`/`send_data` wait, so the whole stream honors one `timeout_ms`).
async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

/// `service=TX, method=BEGIN`: resolve the pool, compose the engine `BEGIN`, checkout + open the tx
/// on the pinned conn, allocate a `tx_id`, spawn the actor (MOVING the `Checkout` in), register, and
/// reply the terminal `BeginResponse{tx_id}`. Any failure before the spawn → a mapped error, one
/// END, nothing registered, the conn released.
#[allow(clippy::too_many_arguments)]
async fn handle_begin(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    idle_in_tx: Duration,
    max_tx: Duration,
    teardown_timeout: Duration,
) {
    let req = match BeginRequest::decode(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(protocol(format!("malformed BeginRequest: {e}")));
            return;
        }
    };
    let Some(pool) = registry.get(&req.pool) else {
        responder.end_error(unsupported(format!("unknown pool {:?}", req.pool)));
        return;
    };

    // Dispatch to the SAME generic body for whichever concrete backend the named pool is: the tx
    // actor (`actor::run<B>`) and `TxRegistry` (backend-AGNOSTIC command channels) are already
    // generic, so each arm spawns the right `Checkout<B>` actor and both monomorphize
    // `begin_on_pool`. PG's path is byte-for-byte the pre-M1-S6 inline body.
    match pool {
        AnyPool::Pg(p) => {
            begin_on_pool(
                p,
                responder,
                &req,
                tx_registry,
                session_id,
                idle_in_tx,
                max_tx,
                teardown_timeout,
            )
            .await
        }
        AnyPool::Mysql(p) => {
            begin_on_pool(
                p,
                responder,
                &req,
                tx_registry,
                session_id,
                idle_in_tx,
                max_tx,
                teardown_timeout,
            )
            .await
        }
    }
}

/// The `service=TX, method=BEGIN` body, generic over the backend `B` — extracted from `handle_begin`
/// so the heterogeneous pool registry (M1-S6) dispatches per `AnyPool` variant. Composes the engine
/// BEGIN, checks out + opens the tx on the pinned conn, allocates a `tx_id`, spawns the (already
/// backend-agnostic) actor MOVING the `Checkout<B>` in, registers its control surface, and replies
/// the terminal `BeginResponse{tx_id}`. Any failure before the spawn → a mapped error, one END,
/// nothing registered, the conn released. PG's behavior is byte-for-byte the pre-M1-S6 inline body.
#[allow(clippy::too_many_arguments)]
async fn begin_on_pool<B: PoolBackend>(
    pool: &Pool<B>,
    responder: Responder,
    req: &BeginRequest,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    idle_in_tx: Duration,
    max_tx: Duration,
    teardown_timeout: Duration,
) {
    // M1-S8a Task 8: the BEGIN is composed for the POOL's dialect (`PoolBackend::dialect()` is a
    // synchronous per-backend constant), so a MySQL pool gets `START TRANSACTION [READ ONLY]` with
    // the isolation as a `SET TRANSACTION …;` prefix in the SAME statement string, and PG keeps its
    // byte-identical `BEGIN …` strings. SPEC §22.2 (s).
    let begin_sql =
        match actor::compose_begin_sql(pool.backend().dialect(), req.isolation, req.readonly) {
            Ok(s) => s,
            Err(msg) => {
                responder.end_error(protocol(msg));
                return;
            }
        };

    let mut co = match pool.checkout().await {
        Ok(co) => co,
        Err(e) => {
            // No conn established: BEGIN never ran, no user statement sent → known-fate (sent=false).
            // BEGIN precedes any open transaction: in_tx=false.
            responder.end_error(fate::classify_fate(
                e,
                OpContext {
                    readonly: req.readonly,
                    sent: false,
                    in_tx: false,
                },
            ));
            return;
        }
    };
    let tx_id = next_tx_id();
    if let Err(e) = co
        .begin_tx_with(ferro_pool::pin::TxId(tx_id), &begin_sql)
        .await
    {
        // BEGIN (engine tx-control) failed: nothing registered, `co` drops → conn released, one END.
        // BEGIN is not a user write and no user statement was sent → known-fate (sent=false).
        // BEGIN itself is a control op, never an in-tx statement: in_tx=false.
        responder.end_error(fate::classify_fate(
            e,
            OpContext {
                readonly: req.readonly,
                sent: false,
                in_tx: false,
            },
        ));
        return;
    }

    // Spawn the actor MOVING the pinned `Checkout` in, then register its control surface.
    let (cmd_tx, cmd_rx) = mpsc::channel(TX_CMD_CHANNEL_CAP);
    let (done_tx, done_rx) = watch::channel(false);
    let abort = CancellationToken::new();
    tx_registry.register(
        tx_id,
        TxHandle {
            owner: session_id,
            cmd_tx,
            abort: abort.clone(),
            done: done_rx,
            // Captured at BEGIN from the ONE authority, so the (backend-agnostic) forwarding
            // handler can refuse a tx-scoped `fetch:stream` WITHOUT touching the pinned conn.
            streaming: pool.backend().supports_row_streaming(),
        },
    );
    tokio::spawn(actor::run(
        tx_id,
        co,
        cmd_rx,
        abort,
        done_tx,
        tx_registry.clone(),
        idle_in_tx,
        max_tx,
        teardown_timeout,
    ));

    responder.end_ok(Bytes::from(BeginResponse { tx_id }.encode()));
}

/// COMMIT / ROLLBACK.
enum CtlKind {
    Commit,
    Rollback,
}

/// SAVEPOINT / RELEASE / ROLLBACK_TO.
enum SpKind {
    Savepoint,
    Release,
    RollbackTo,
}

/// `service=TX` COMMIT/ROLLBACK: look up the owning actor, forward the control command, await the
/// reply, and declare the mapped terminal.
async fn handle_tx_control(
    frame: InFrame,
    responder: Responder,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    kind: CtlKind,
) {
    let req = match TxControl::decode(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(protocol(format!("malformed TxControl: {e}")));
            return;
        }
    };
    let handle = match resolve_active(tx_registry, req.tx_id, session_id) {
        Ok(h) => h,
        Err(ep) => {
            responder.end_error(ep);
            return;
        }
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    // A COMMIT loss is a potential lost write (§19.3 Indeterminate); a ROLLBACK loss is not (the tx
    // is gone either way) — so map COMMIT errors as a write (readonly=false) and ROLLBACK as not.
    let (cmd, readonly) = match kind {
        CtlKind::Commit => (TxCommand::Commit { reply: reply_tx }, false),
        CtlKind::Rollback => (TxCommand::Rollback { reply: reply_tx }, true),
    };
    if handle.cmd_tx.send(cmd).await.is_err() {
        responder.end_error(actor_gone_terminal(tx_registry, req.tx_id, session_id));
        return;
    }
    match reply_rx.await {
        Ok(reply) => declare_ctl(responder, reply, readonly),
        Err(_recv) => responder.end_error(actor_gone_terminal(tx_registry, req.tx_id, session_id)),
    }
}

/// `service=TX` SAVEPOINT/RELEASE/ROLLBACK_TO: look up the owning actor, forward the (engine-named)
/// savepoint command, await the reply, and declare the mapped terminal. A savepoint control op is
/// never a lost write, so its error mapping uses `readonly=true` (never Indeterminate).
async fn handle_savepoint(
    frame: InFrame,
    responder: Responder,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    kind: SpKind,
) {
    let req = match SavepointRequest::decode(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(protocol(format!("malformed SavepointRequest: {e}")));
            return;
        }
    };
    let handle = match resolve_active(tx_registry, req.tx_id, session_id) {
        Ok(h) => h,
        Err(ep) => {
            responder.end_error(ep);
            return;
        }
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = match kind {
        SpKind::Savepoint => TxCommand::Savepoint {
            name: req.name,
            reply: reply_tx,
        },
        SpKind::Release => TxCommand::Release {
            name: req.name,
            reply: reply_tx,
        },
        SpKind::RollbackTo => TxCommand::RollbackTo {
            name: req.name,
            reply: reply_tx,
        },
    };
    if handle.cmd_tx.send(cmd).await.is_err() {
        responder.end_error(actor_gone_terminal(tx_registry, req.tx_id, session_id));
        return;
    }
    match reply_rx.await {
        Ok(reply) => declare_ctl(responder, reply, true),
        Err(_recv) => responder.end_error(actor_gone_terminal(tx_registry, req.tx_id, session_id)),
    }
}

/// Resolve a tx-scoped request to its live `TxHandle`, or the terminal `ErrorPayload` to declare:
/// `NotFoundOrForbidden` (unknown OR cross-session) → `Protocol`; the owner's own `Tombstoned` tx →
/// `TxDeadline{Retryable}`. A client can never tell a cross-session id from an unknown one.
fn resolve_active(
    tx_registry: &TxRegistry,
    tx_id: u64,
    session_id: SessionId,
) -> Result<TxHandle, ErrorPayload> {
    match tx_registry.lookup(tx_id, session_id) {
        Ok(h) => Ok(h),
        Err(TxLookupErr::NotFoundOrForbidden) => Err(protocol(
            "unknown or forbidden tx_id (committed, rolled back, aborted, or another session's)",
        )),
        Err(TxLookupErr::Tombstoned) => Err(tx_deadline(
            "transaction deadline exceeded; the pinned connection was rolled back and released \
             (retryable — the engine never re-runs)",
        )),
    }
}

/// Declare a tx-control reply's terminal: `Ok` → an empty `Outcome::Ok` ack; a backend `Err` →
/// mapped via `fate::classify_fate` (the COMMIT-loss `WriteUnconfirmed` §19.3 case rides in on
/// `readonly=false`); an `UnknownSavepoint` → `Protocol` (client misuse, never touched the backend).
///
/// EVERY control op routed through here — COMMIT, ROLLBACK, SAVEPOINT, RELEASE, ROLLBACK_TO — is a
/// tx CONTROL boundary, never an in-transaction user STATEMENT, so `in_tx` is always `false`. This
/// is load-bearing for COMMIT specifically: a lost COMMIT (`readonly=false, sent=true`) MUST stay
/// `Indeterminate` (double-apply risk — did it commit or not?). Passing `in_tx=true` here would
/// wrongly downgrade that to `Retryable` (the "in-tx statement, whole tx is dead" rule is for a
/// *statement* whose transaction can no longer commit either way — it does not apply to the COMMIT
/// itself, whose entire fate IS whether it committed).
fn declare_ctl(responder: Responder, reply: CtlReply, readonly: bool) {
    match reply {
        CtlReply::Ok => responder.end_ok(Bytes::new()),
        CtlReply::Err(e) => responder.end_error(fate::classify_fate(
            e,
            OpContext {
                readonly,
                sent: true,
                in_tx: false,
            },
        )),
        CtlReply::UnknownSavepoint => {
            responder.end_error(protocol("no such savepoint in this transaction"))
        }
    }
}

/// The prompt terminal to declare when the actor is gone mid-teardown (an mpsc send-Err or a
/// oneshot recv-Err): a fresh lookup decides — a now-tombstoned id → `TxDeadline`, otherwise
/// `Protocol`. Never a hang; the supervisor stays the sole terminal-sender so exactly-one-END holds.
fn actor_gone_terminal(
    tx_registry: &TxRegistry,
    tx_id: u64,
    session_id: SessionId,
) -> ErrorPayload {
    match tx_registry.lookup(tx_id, session_id) {
        Err(TxLookupErr::Tombstoned) => tx_deadline(
            "transaction deadline exceeded; the pinned connection was rolled back and released \
             (retryable — the engine never re-runs)",
        ),
        _ => protocol("transaction is no longer active"),
    }
}

/// Encode an `ExecOk` with an EXACT `stats.bytes` by iterating to a FIXED POINT: `stats.bytes` IS
/// the encoded body length, but the body carries `stats.bytes`, so setting it can widen that field
/// and grow the body. Body length is monotonic non-decreasing in the `bytes` value (a larger value
/// is the same-or-wider msgpack uint), and each step sets `bytes` to the current length, so the
/// sequence increases and converges in a couple of steps (bounded defensively). Shared by the
/// buffered [`build_terminal_body`] and the streamed [`build_stream_terminal_body`].
fn encode_exec_ok_fixpoint(exec_ok: &mut ExecOk) -> Vec<u8> {
    let mut body = exec_ok.encode();
    for _ in 0..8 {
        let len = body.len() as u64;
        if exec_ok.stats.bytes == len {
            break;
        }
        exec_ok.stats.bytes = len;
        body = exec_ok.encode();
    }
    body
}

/// THE `Option<u64>` → wire `Option<Value>` conversion for `ExecOk.last_insert_id` (M1-S8a). One
/// site, so the tag choice cannot diverge between the buffered and the streamed terminal.
///
/// `I64` while the id fits (the overwhelmingly common case, and the shape the golden vector
/// `sql_exec_response_lastid.json` already locks — a plain PHP `int` on the client), `U64` only above
/// `i64::MAX`, which a `BIGINT UNSIGNED` AUTO_INCREMENT can legally reach. Saturating into `I64`
/// there would be a silent wrong key.
fn last_insert_id_value(id: Option<u64>) -> Option<Value> {
    id.map(|n| match i64::try_from(n) {
        Ok(v) => Value::I64(v),
        Err(_) => Value::U64(n),
    })
}

/// Shape `result` by `fetch`, encode the `ExecOk` terminal body, and size-check the fully-encoded
/// `Outcome::Ok` payload. Returns the encoded body on success, or an `Unsupported` `ErrorPayload`
/// if the result would exceed one frame. Split out (no pool/DB) so the size-cap and shaping are
/// unit-tested deterministically without Docker.
fn build_terminal_body(
    result: QueryResult,
    fetch: u8,
    queue_us: u64,
    exec_us: u64,
) -> Result<Vec<u8>, ErrorPayload> {
    // fetch=none: drop the buffered rows, keep cols + affected (the shaping is the SERVICE's job —
    // the pool `query` faithfully returns both regardless).
    let rows = if fetch == FETCH_NONE {
        Vec::new()
    } else {
        result.rows
    };
    let nrows = rows.len() as u64;

    let mut exec_ok = ExecOk {
        cols: result.cols,
        rows,
        affected: result.affected,
        last_insert_id: last_insert_id_value(result.last_insert_id),
        stats: Stats {
            queue_us,
            exec_us,
            rows: nrows,
            bytes: 0,
        },
    };

    let body = encode_exec_ok_fixpoint(&mut exec_ok);

    // Size-check the FULLY-ENCODED Outcome::Ok payload (body + the 2-byte envelope), NOT the raw
    // body (BLOCKER-v2). A per-request Unsupported is the clean alternative to a session teardown.
    if body.len() + OUTCOME_OK_OVERHEAD > MAX_FRAME_PAYLOAD as usize {
        return Err(unsupported(
            "buffered result exceeds one frame; re-issue with fetch=stream to receive it \
             incrementally over the DATA channel",
        ));
    }
    Ok(body)
}

/// THE `fetch:stream`-unsupported terminal. One constructor, read by BOTH the autocommit arm and the
/// tx-scoped forwarding arm, so the two can never drift (they did: the tx arm used to fall through to
/// the backend's own string). Cites §22.2 so an operator can find the deferral rationale.
///
/// `pub` so the live gate can assert the observed terminal against THIS constructor rather than
/// against a literal copied into the test. That is what makes the guard falsifiable: a backend's own
/// late `query_stream` refusal carries a DIFFERENT string, so an arm that stops refusing pre-dispatch
/// (or a backend whose capability is wrongly `true`) is caught by an equality that is derived from
/// production code instead of restated beside it.
pub fn stream_unsupported() -> ErrorPayload {
    unsupported(
        "fetch=stream is not supported on this pool's backend (MySQL/MariaDB row streaming is \
         deferred — SPEC §22.2 (n)); re-issue with fetch=rows",
    )
}

fn unsupported(message: impl Into<String>) -> ErrorPayload {
    ErrorPayload {
        code: errc::UNSUPPORTED,
        branch: errc::UNSUPPORTED_BRANCH,
        sqlstate: None,
        errno: None,
        message: message.into(),
        detail: None,
        retry_after_ms: None,
    }
}

pub(crate) fn protocol(message: impl Into<String>) -> ErrorPayload {
    ErrorPayload {
        code: errc::PROTOCOL,
        branch: errc::PROTOCOL_BRANCH,
        sqlstate: None,
        errno: None,
        message: message.into(),
        detail: None,
        retry_after_ms: None,
    }
}

/// A `TxDeadline{Retryable}` terminal (0x1003, §7): the tx was cancelled + rolled back by a deadline
/// (idle or max) or the owner is retrying a timed-out tx. Retryable is CLIENT policy — the engine
/// never re-runs the statement (charter rule 3).
pub(crate) fn tx_deadline(message: impl Into<String>) -> ErrorPayload {
    ErrorPayload {
        code: errc::TX_DEADLINE,
        branch: errc::TX_DEADLINE_BRANCH,
        sqlstate: None,
        errno: None,
        message: message.into(),
        detail: None,
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_pool::config::PoolConfig;
    use ferro_pool::fake::FakeBackend;
    use ferro_pool::pool::Pool;
    use ferro_proto::consts::{branch, tag};
    use ferro_proto::messages::Outcome;
    use ferro_proto::messages::sql::ColMeta;

    /// Locks `OUTCOME_OK_OVERHEAD` against `Outcome::Ok`'s ACTUAL envelope, so the size-cap can
    /// never silently drift out of sync with the codec (which would re-open the BLOCKER-v2 teardown).
    #[test]
    fn outcome_ok_overhead_is_two() {
        assert_eq!(Outcome::Ok(vec![]).encode().len(), OUTCOME_OK_OVERHEAD);
        let body = vec![0x01u8, 0x02, 0x03];
        assert_eq!(
            Outcome::Ok(body.clone()).encode().len(),
            OUTCOME_OK_OVERHEAD + body.len()
        );
    }

    /// The I64/U64 boundary is exact and a `None` stays `None`. `i64::MAX` is the last I64 value;
    /// `i64::MAX as u64 + 1` is the first U64 one.
    #[test]
    fn last_insert_id_value_picks_the_narrowest_truthful_tag() {
        assert_eq!(last_insert_id_value(None), None);
        assert_eq!(last_insert_id_value(Some(0)), Some(Value::I64(0)));
        assert_eq!(last_insert_id_value(Some(200)), Some(Value::I64(200)));
        assert_eq!(
            last_insert_id_value(Some(i64::MAX as u64)),
            Some(Value::I64(i64::MAX))
        );
        assert_eq!(
            last_insert_id_value(Some(i64::MAX as u64 + 1)),
            Some(Value::U64(9_223_372_036_854_775_808)),
            "above i64::MAX the tag must widen, never saturate"
        );
        assert_eq!(
            last_insert_id_value(Some(u64::MAX)),
            Some(Value::U64(u64::MAX))
        );
    }

    /// The buffered terminal must CARRY the pool's `last_insert_id`, not drop it: drive a real
    /// `QueryResult` through the real `build_terminal_body` and read the field back off the encoded
    /// `ExecOk`. Behavioural (rule 3) — it fails the moment the field is hardcoded to `None` again.
    #[test]
    fn buffered_terminal_carries_the_generated_key() {
        let result = QueryResult {
            cols: vec![],
            rows: vec![],
            affected: 1,
            last_insert_id: Some(7),
        };
        let body = build_terminal_body(result, FETCH_NONE, 0, 0).expect("fits");
        let ok = ExecOk::decode(&body).expect("decode ExecOk");
        assert_eq!(ok.last_insert_id, Some(Value::I64(7)));

        // …and a backend that reports none leaves the wire field absent.
        let none = QueryResult {
            affected: 1,
            ..Default::default()
        };
        let body = build_terminal_body(none, FETCH_NONE, 0, 0).expect("fits");
        let ok = ExecOk::decode(&body).expect("decode ExecOk");
        assert_eq!(ok.last_insert_id, None);
    }

    /// An over-one-frame result is a clean per-request `Unsupported` — NEVER a body the frame codec
    /// would reject (which would tear down the whole session; BLOCKER-v2).
    #[test]
    fn oversized_result_is_unsupported_not_a_teardown() {
        let big = vec![0u8; MAX_FRAME_PAYLOAD as usize];
        let result = QueryResult {
            cols: vec![],
            rows: vec![vec![Value::Bytes(big)]],
            affected: 0,
            ..Default::default()
        };
        let err = build_terminal_body(result, FETCH_ROWS, 1, 2)
            .expect_err("an over-one-frame result must error cleanly, not tear down");
        assert_eq!(err.code, errc::UNSUPPORTED);
    }

    /// A small result encodes within one frame, round-trips through `ExecOk::decode`, and carries
    /// the stats (rows count + a nonzero encoded `bytes`).
    #[test]
    fn small_result_encodes_within_one_frame() {
        let result = QueryResult {
            cols: vec![ColMeta {
                name: "?column?".to_string(),
                tag: tag::I64,
            }],
            rows: vec![vec![Value::I64(1)]],
            affected: 0,
            ..Default::default()
        };
        let body = build_terminal_body(result, FETCH_ROWS, 5, 9).expect("small result fits");
        let ok = ExecOk::decode(&body).expect("decode ExecOk");
        assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
        assert_eq!(ok.stats.queue_us, 5);
        assert_eq!(ok.stats.exec_us, 9);
        assert_eq!(ok.stats.rows, 1);
        assert_eq!(
            ok.stats.bytes as usize,
            body.len(),
            "stats.bytes is the EXACT encoded body length (fixpoint)"
        );
    }

    /// `stats.bytes` stays EXACT even when the body length crosses the msgpack uint-width boundary
    /// (>127 → the `bytes` field widens from fixint to uint8, growing the body). The old two-pass
    /// (`0` placeholder, set once) undercounted here; the fixpoint converges. A ~200-byte text cell
    /// pushes the body over 127 bytes.
    #[test]
    fn stats_bytes_exact_across_uint_width_boundary() {
        let result = QueryResult {
            cols: vec![],
            rows: vec![vec![Value::Text("x".repeat(200))]],
            affected: 0,
            ..Default::default()
        };
        let body = build_terminal_body(result, FETCH_ROWS, 1, 2).expect("fits one frame");
        assert!(body.len() > 127, "body must cross the uint8 width boundary");
        let ok = ExecOk::decode(&body).expect("decode ExecOk");
        assert_eq!(
            ok.stats.bytes as usize,
            body.len(),
            "fixpoint keeps stats.bytes exact across the width boundary"
        );
    }

    /// fetch=none drops the buffered rows but keeps cols + affected; `stats.rows` reflects the
    /// shipped (empty) row set.
    #[test]
    fn fetch_none_drops_rows_keeps_affected_and_cols() {
        let result = QueryResult {
            cols: vec![ColMeta {
                name: "id".to_string(),
                tag: tag::I64,
            }],
            rows: vec![vec![Value::I64(1)], vec![Value::I64(2)]],
            affected: 2,
            ..Default::default()
        };
        let body = build_terminal_body(result, FETCH_NONE, 0, 0).expect("fits");
        let ok = ExecOk::decode(&body).expect("decode ExecOk");
        assert!(ok.rows.is_empty(), "fetch=none drops the buffered rows");
        assert_eq!(ok.affected, 2);
        assert_eq!(ok.cols.len(), 1, "cols are kept even for fetch=none");
        assert_eq!(ok.stats.rows, 0);
    }

    // ---- S6 tx-routing helpers -----------------------------------------------------------------

    use crate::session::responder::Terminal;
    use crate::tx::TxRegistry;

    fn dummy_handle(owner: SessionId) -> TxHandle {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<TxCommand>(1);
        let (_done_tx, done_rx) = watch::channel(false);
        TxHandle {
            owner,
            cmd_tx,
            abort: CancellationToken::new(),
            done: done_rx,
            // These fixtures only exercise `resolve_active`'s lookup states, never a streamed
            // fetch; `true` is the trait default (PG's real value).
            streaming: true,
        }
    }

    /// A `TxDeadline` terminal is `0x1003 / Retryable` — the retry is client policy; the engine
    /// never re-runs (charter rule 3).
    #[test]
    fn tx_deadline_is_retryable_1003() {
        let ep = tx_deadline("x");
        assert_eq!(ep.code, errc::TX_DEADLINE);
        assert_eq!(ep.branch, branch::RETRYABLE);
    }

    /// `resolve_active`: unknown/cross-session → `Protocol` (indistinguishable); the owner's own
    /// tombstone → `TxDeadline{Retryable}`; a live entry → the handle.
    #[test]
    fn resolve_active_maps_lookup_states() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();
        let other = reg.next_session_id();

        // Unknown id → Protocol.
        assert_eq!(
            resolve_active(&reg, 1, owner).unwrap_err().code,
            errc::PROTOCOL
        );

        // Live entry → the handle for the owner; Protocol (NOT leaked) for anyone else.
        reg.register(1, dummy_handle(owner));
        assert!(resolve_active(&reg, 1, owner).is_ok());
        assert_eq!(
            resolve_active(&reg, 1, other).unwrap_err().code,
            errc::PROTOCOL,
            "a cross-session lookup is Protocol, indistinguishable from unknown"
        );

        // The owner's tombstone → TxDeadline{Retryable}.
        reg.tombstone(1);
        let ep = resolve_active(&reg, 1, owner).unwrap_err();
        assert_eq!(ep.code, errc::TX_DEADLINE);
        assert_eq!(ep.branch, branch::RETRYABLE);
    }

    /// `actor_gone_terminal` (send-Err / recv-Err mid-teardown): a now-tombstoned id → `TxDeadline`,
    /// otherwise `Protocol`. Never a hang — a PROMPT declared terminal so exactly-one-END holds.
    #[test]
    fn actor_gone_terminal_prompt_maps_tombstone_else_protocol() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();

        // Actor gone + id already deregistered → Protocol.
        assert_eq!(actor_gone_terminal(&reg, 7, owner).code, errc::PROTOCOL);

        // Actor gone + id tombstoned (deadline raced the send) → TxDeadline.
        reg.register(7, dummy_handle(owner));
        reg.tombstone(7);
        let ep = actor_gone_terminal(&reg, 7, owner);
        assert_eq!(ep.code, errc::TX_DEADLINE);
        assert_eq!(ep.branch, branch::RETRYABLE);
    }

    /// `declare_ctl` reply → terminal mapping, including the COMMIT-loss §19.3 case. Exercises the
    /// REAL `declare_ctl` call site (now routed through `fate::classify_fate` with `in_tx: false`,
    /// per the M1-S4 refactor) — a lost COMMIT MUST stay `Indeterminate` here even though the tx is
    /// technically open when COMMIT is sent; `declare_ctl` is a control boundary, not an in-tx
    /// statement, so passing `in_tx: false` is what keeps this from being wrongly downgraded to
    /// `Retryable`.
    #[test]
    fn declare_ctl_maps_replies_including_commit_loss_indeterminate() {
        // Ok → an empty Outcome::Ok ack.
        let (r, cell) = Responder::new_pair();
        declare_ctl(r, CtlReply::Ok, true);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Ok(b)) => assert!(b.is_empty(), "a control-op ack is an empty Ok body"),
            other => panic!("expected empty Ok, got {other:?}"),
        }

        // UnknownSavepoint → Protocol.
        let (r, cell) = Responder::new_pair();
        declare_ctl(r, CtlReply::UnknownSavepoint, true);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => assert_eq!(ep.code, errc::PROTOCOL),
            other => panic!("expected Protocol error, got {other:?}"),
        }

        // A COMMIT (readonly=false) that loses the connection → §19.3 WriteUnconfirmed{Indeterminate}.
        let (r, cell) = Responder::new_pair();
        declare_ctl(r, CtlReply::Err(PoolError::ConnectionLost), false);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::WRITE_UNCONFIRMED);
                assert_eq!(ep.branch, branch::INDETERMINATE);
            }
            other => panic!("expected WriteUnconfirmed, got {other:?}"),
        }

        // A ROLLBACK (readonly=true) that loses the connection → known-fate ConnectionLost{Retryable},
        // NOT Indeterminate (a failed rollback is not a lost write).
        let (r, cell) = Responder::new_pair();
        declare_ctl(r, CtlReply::Err(PoolError::ConnectionLost), true);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::CONNECTION_LOST);
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected ConnectionLost, got {other:?}"),
        }
    }

    // ---- M1-S4 Task 2: run_autocommit_exec / declare_autocommit_exec ---------------------------
    //
    // Deterministic `FakeBackend`-driven proofs of the biased-select/drain-to-Result mechanics
    // that back the autocommit EXEC path's `timeout_ms` + per-request CANCEL enforcement —
    // mirroring exactly how `tx/actor.rs`'s own deadline/abort tests drive the same primitives
    // (`FakeBackend::block_query` + `FakeCancelHandle`), just without a transaction. Each test also
    // routes its `run_autocommit_exec` result through the REAL `declare_autocommit_exec` (not a
    // copy), so the wire-shaped `Terminal`/`ErrorPayload` asserted here is what the handler would
    // actually declare, not a stand-in.

    fn autocommit_test_pool_config() -> PoolConfig {
        PoolConfig {
            max_size: 1, // one conn: a fresh checkout after a taint proves the recycle ran
            checkout_timeout: Duration::from_secs(5),
            max_lifetime: Duration::from_secs(3600),
            reap_interval: None, // deterministic: no background reaper
            ..PoolConfig::default()
        }
    }

    /// A `timeout_ms`-elapsed autocommit WRITE → the drained `Err` is a 57014-shaped cancel, and
    /// declaring it (readonly=false, i.e. a write) yields `WriteUnconfirmed{Indeterminate}` — never
    /// `Cancelled{NonRetryable}`, never a bare `Retryable` (§19.3: a dispatched write's
    /// non-execution is UNCONFIRMED, not known).
    #[tokio::test(start_paused = true)]
    async fn autocommit_write_timeout_drains_to_indeterminate() {
        let backend = FakeBackend::new();
        backend.block_query(); // freeze the statement mid-flight
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let mut co = pool.checkout().await.expect("checkout");

        let cancel = CancellationToken::new(); // never fired — the TIMER must fire instead
        let (result, _exec_us) =
            run_autocommit_exec(&mut co, "UPDATE t SET n = n + 1", &[], Some(20), &cancel).await;

        let e = result.expect_err("a timed-out statement must drain to an Err, never Ok");
        assert!(
            matches!(&e, PoolError::Sql { sqlstate, .. } if sqlstate.as_deref() == Some("57014")),
            "expected a 57014-shaped cancel, got {e:?}"
        );

        // `sent: true` is honest here (the biased select polled the query before the timer could
        // win) — declaring a WRITE (readonly=false) must yield Indeterminate.
        let (r, cell) = Responder::new_pair();
        declare_autocommit_exec(r, Err(e), FETCH_ROWS, 0, 0, /* readonly */ false);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::WRITE_UNCONFIRMED);
                assert_eq!(ep.branch, branch::INDETERMINATE);
            }
            other => panic!("expected WriteUnconfirmed/Indeterminate, got {other:?}"),
        }
    }

    /// The SAME `timeout_ms`-elapsed cancel, but declared as a READ (readonly=true) →
    /// `Cancelled{NonRetryable}` — never `Indeterminate` (§19.3: a read's non-execution is a KNOWN,
    /// safe-to-retry fate, but the wire has no `Cancelled/Retryable` pairing, so it rides
    /// NonRetryable and the client's own read-retry policy decides).
    #[tokio::test(start_paused = true)]
    async fn autocommit_read_timeout_drains_to_cancelled_nonretryable() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let mut co = pool.checkout().await.expect("checkout");

        let cancel = CancellationToken::new();
        let (result, _exec_us) =
            run_autocommit_exec(&mut co, "SELECT pg_sleep(9)", &[], Some(20), &cancel).await;
        let e = result.expect_err("a timed-out statement must drain to an Err");

        let (r, cell) = Responder::new_pair();
        declare_autocommit_exec(r, Err(e), FETCH_ROWS, 0, 0, /* readonly */ true);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::CANCELLED);
                assert_eq!(ep.branch, branch::NON_RETRYABLE);
            }
            other => panic!("expected Cancelled/NonRetryable, got {other:?}"),
        }
    }

    /// A per-request CANCEL (the token, NOT the timer) racing an in-flight autocommit WRITE →
    /// the same drain-to-Err path, declared Indeterminate. Fires the cancel only once the
    /// statement is OBSERVED in flight (`queries_waiting() > 0`), proving the CANCEL arm itself —
    /// not a lucky pre-poll race — is what unblocks it.
    #[tokio::test]
    async fn autocommit_write_cancel_token_races_in_flight_drains_to_indeterminate() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let mut co = pool.checkout().await.expect("checkout");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let waiter = async {
            while pool.backend().queries_waiting() == 0 {
                tokio::task::yield_now().await;
            }
            cancel_clone.cancel();
        };

        let (exec_result, ()) = tokio::join!(
            run_autocommit_exec(&mut co, "UPDATE t SET n = n + 1", &[], None, &cancel),
            waiter,
        );
        let e = exec_result
            .0
            .expect_err("a cancelled statement must drain to an Err");
        assert!(
            matches!(&e, PoolError::Sql { sqlstate, .. } if sqlstate.as_deref() == Some("57014")),
            "expected a 57014-shaped cancel, got {e:?}"
        );

        let (r, cell) = Responder::new_pair();
        declare_autocommit_exec(r, Err(e), FETCH_ROWS, 0, 0, /* readonly */ false);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::WRITE_UNCONFIRMED);
                assert_eq!(ep.branch, branch::INDETERMINATE);
            }
            other => panic!("expected WriteUnconfirmed/Indeterminate, got {other:?}"),
        }
    }

    /// Cancel-LOSES-the-race: no gate is armed (the statement completes on its very first poll),
    /// and BOTH the timer (`timeout_ms: Some(0)`) and the per-request cancel token are ALREADY
    /// primed to fire before the call even starts — yet the query still wins (it is polled FIRST,
    /// every round) and the REAL result comes back as `Ok`, never a fabricated cancel/error.
    #[tokio::test]
    async fn autocommit_exec_lost_race_returns_real_ok_never_fabricated_error() {
        let backend = FakeBackend::new();
        backend.set_query_result(QueryResult {
            cols: vec![ColMeta {
                name: "n".to_string(),
                tag: tag::I64,
            }],
            rows: vec![vec![Value::I64(42)]],
            affected: 1,
            ..Default::default()
        });
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let mut co = pool.checkout().await.expect("checkout");

        let cancel = CancellationToken::new();
        cancel.cancel(); // already cancelled BEFORE the statement even starts
        let (result, _exec_us) =
            run_autocommit_exec(&mut co, "UPDATE t SET n = n + 1", &[], Some(0), &cancel).await;

        let qr = result.expect("the query completed first (biased) — must be Ok, not Err");
        assert_eq!(qr.rows, vec![vec![Value::I64(42)]]);

        // Declaring it must be the REAL success terminal, never a fabricated Cancelled/error.
        let (r, cell) = Responder::new_pair();
        declare_autocommit_exec(r, Ok(qr), FETCH_ROWS, 0, 0, false);
        match cell.lock().unwrap().clone() {
            Some(Terminal::Ok(body)) => {
                let ok = ExecOk::decode(&body).expect("decode ExecOk");
                assert_eq!(ok.rows, vec![vec![Value::I64(42)]]);
            }
            other => panic!("expected the real Ok result, got {other:?}"),
        }
    }

    /// `timeout_ms == None` → no timer arm at all (a `pending()` future, NOT a 0ms sleep):
    /// regression guard against a bug that would make a `None` deadline resolve immediately. A
    /// frozen statement with no timer and no cancel simply STAYS pending.
    #[tokio::test]
    async fn autocommit_exec_timeout_none_never_fires_a_phantom_timer() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let mut co = pool.checkout().await.expect("checkout");

        let cancel = CancellationToken::new();
        let outcome = tokio::time::timeout(
            Duration::from_millis(50),
            run_autocommit_exec(&mut co, "SELECT 1", &[], None, &cancel),
        )
        .await;
        assert!(
            outcome.is_err(),
            "timeout_ms=None must never resolve on its own; a 0ms-timer bug would return here"
        );
    }

    /// The post-cancel connection is TAINTED (the S1 `Checkout::query` Err-arm fail-safe forces
    /// `tainted` unconditionally) and therefore RECYCLED (a full `DISCARD ALL`-shaped reset) at the
    /// next checkout — no conn is ever handed to the next tenant still holding a cancelled/aborted
    /// statement's state.
    #[tokio::test(start_paused = true)]
    async fn autocommit_post_cancel_conn_is_tainted_and_recycled() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let mut co = pool.checkout().await.expect("checkout");

        let cancel = CancellationToken::new();
        let (result, _exec_us) =
            run_autocommit_exec(&mut co, "UPDATE t SET n = n + 1", &[], Some(20), &cancel).await;
        assert!(result.is_err(), "the timeout fired a cancel");
        drop(co); // RAII release, same as `handle_exec`

        // A fresh checkout on the size-1 pool recycles the tainted conn with a FULL reset.
        let co2 = pool
            .checkout()
            .await
            .expect("permit released despite the cancel");
        assert!(
            co2.conn().recorded.contains(&"RESET:Full".to_string()),
            "a cancelled statement taints the conn -> Full reset at the next checkout: {:?}",
            co2.conn().recorded
        );
    }

    // ---- M1-S5 Task 4b: the streaming producer (run_autocommit_streamed / run_streamed_exec) -----
    //
    // Deterministic `FakeBackend`-driven proofs of the pull->send loop under credit: HEAD + DATA*N
    // then exactly ONE terminal END (B4), backpressure pause/resume + cancel/timeout unwind (B3),
    // the session cap release (M6), and exec_us-excludes-backpressure (M7). Each drives the REAL
    // `run_autocommit_streamed` (checkout + query_stream + the shared `run_streamed_exec` loop) with
    // a `max_rows: 1` batch so the DATA-frame count is exactly the scripted row count.

    use crate::session::codec::ControlMsg;
    use crate::session::flow::{Credit, CreditCell, SessionCap};
    use crate::session::registry::Registry;
    use crate::session::supervisor;
    use ferro_pool::fake::StreamScript;
    use ferro_proto::consts::{flags, method_stream};
    use ferro_proto::messages::{StreamData, StreamHead};
    use tokio::time::timeout;

    const STREAM_REQ_ID: u32 = 77;

    fn stream_col(name: &str) -> ColMeta {
        ColMeta {
            name: name.to_string(),
            tag: tag::I64,
        }
    }

    fn i64_row(n: i64) -> Vec<Value> {
        vec![Value::I64(n)]
    }

    /// One row per DATA frame — so a scripted N-row result emits exactly N DATA frames (the
    /// DATA-frame count is observable; see `StreamBatch`'s doc).
    fn one_row_batch() -> StreamBatch {
        StreamBatch {
            max_rows: 1,
            max_bytes: usize::MAX,
        }
    }

    fn script(rows: Vec<Vec<Value>>, affected: u64, error_at: Option<usize>) -> StreamScript {
        StreamScript {
            cols: vec![stream_col("n")],
            rows,
            affected,
            error_at,
        }
    }

    /// Run one autocommit stream INLINE (not spawned — so a `&Pool` borrow is fine), drain every
    /// emitted frame into `(service, method, flags)` descriptors (dropping each `ControlMsg`, which
    /// releases its cap guard — modelling the writer), and return those plus the declared terminal.
    async fn drive_stream(
        pool: &Pool<FakeBackend>,
        session_cap: &Arc<SessionCap>,
        sql: &str,
        readonly: bool,
        batch: StreamBatch,
    ) -> (Vec<(u16, u16, u16)>, Terminal) {
        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(64);
        let credit_cell = Arc::new(CreditCell::new(Credit::new(1000, u32::MAX)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            credit_cell,
            Arc::clone(session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();
        // Channel cap 64 >> the frame count, so the inline producer never blocks on the send.
        run_autocommit_streamed(responder, pool, sql, &[], None, readonly, &cancel, batch).await;
        drop(control_tx);

        let mut frames = Vec::new();
        while let Some(msg) = control_rx.recv().await {
            frames.push((
                msg.frame.header.service,
                msg.frame.header.method,
                msg.frame.header.flags,
            ));
            // `msg` drops here -> its cap guard (if any) releases.
        }
        let terminal = cell
            .lock()
            .unwrap()
            .take()
            .expect("the producer declared exactly one terminal");
        (frames, terminal)
    }

    fn count_data(frames: &[(u16, u16, u16)]) -> usize {
        frames
            .iter()
            .filter(|(s, m, _)| *s == service::STREAM && *m == method_stream::DATA)
            .count()
    }

    /// B4: a streamed EXEC producing 3 DATA frames → the wire sees HEAD, DATA×3, then exactly ONE
    /// terminal END, in that order (the terminal is LAST — it can never overtake a DATA frame).
    #[tokio::test]
    async fn streamed_exec_head_data_x3_then_exactly_one_end_b4() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1), i64_row(2), i64_row(3)], 3, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        let registry = Arc::new(Registry::new(4));
        let (cancel, credit_cell) = registry
            .insert(STREAM_REQ_ID, Credit::new(10, 10_000_000))
            .unwrap();
        let permit = control_tx.clone().reserve_owned().await.unwrap();
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            credit_cell,
            Arc::clone(&session_cap),
            control_tx.clone(),
        );

        // The supervisor sends the terminal ONLY after the handler task joins — on the SAME ordered
        // channel as the DATA frames, so FIFO puts it strictly last (B4).
        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool,
                "SELECT n",
                &[],
                None,
                true,
                &cancel,
                one_row_batch(),
            )
            .await;
        });
        supervisor::supervise(
            STREAM_REQ_ID,
            service::SQL,
            method_sql::EXEC,
            permit,
            cell,
            task,
            Arc::clone(&registry),
        )
        .await;

        // HEAD first.
        let head = control_rx.recv().await.expect("HEAD frame");
        assert_eq!(head.frame.header.service, service::STREAM);
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        assert!(head.cap.is_some(), "HEAD carries its cap guard");
        let sh = StreamHead::decode(&head.frame.payload).expect("decode HEAD");
        assert_eq!(sh.cols.len(), 1);
        // Item 4: accumulate the HEAD + DATA payload bytes to check `stats.bytes` on the terminal.
        let mut shipped_bytes = head.frame.payload.len() as u64;

        // Then exactly 3 DATA frames, one scripted row each.
        for i in 1..=3i64 {
            let d = control_rx.recv().await.expect("a DATA frame");
            assert_eq!(d.frame.header.service, service::STREAM);
            assert_eq!(d.frame.header.method, method_stream::DATA);
            assert_eq!(d.frame.header.flags, flags::STREAM);
            let sd = StreamData::decode(&d.frame.payload).expect("decode DATA");
            assert_eq!(
                sd.rows,
                vec![i64_row(i)],
                "DATA frames stream rows in order"
            );
            shipped_bytes += d.frame.payload.len() as u64;
        }

        // Then exactly ONE terminal END — last, after all DATA.
        let end = control_rx.recv().await.expect("the terminal END");
        assert_eq!(end.frame.header.flags, flags::END);
        assert_eq!(end.frame.header.service, service::SQL);
        assert!(end.cap.is_none(), "the terminal carries no cap guard");
        match Outcome::decode(&end.frame.payload).expect("decode Outcome") {
            Outcome::Ok(body) => {
                let ok = ExecOk::decode(&body).expect("decode ExecOk");
                assert_eq!(ok.affected, 3);
                assert!(ok.rows.is_empty(), "the stream terminal carries no rows");
                assert!(
                    ok.cols.is_empty(),
                    "the stream terminal carries no cols (they went out in HEAD)"
                );
                assert_eq!(ok.stats.rows, 3, "stats.rows is the streamed count");
                assert_eq!(
                    ok.stats.bytes, shipped_bytes,
                    "item 4: stats.bytes is the TOTAL HEAD+DATA payload shipped, not the terminal size"
                );
                assert!(ok.stats.bytes > 0);
            }
            other => panic!("expected Outcome::Ok, got {other:?}"),
        }
        assert!(
            control_rx.try_recv().is_err(),
            "exactly one END — nothing follows the terminal"
        );
        assert_eq!(
            registry.len(),
            0,
            "the supervisor removed the registry entry"
        );
    }

    /// B3 backpressure pause+resume: a 1-frame credit window → HEAD (MINOR-12: HEAD debits a frame)
    /// consumes it, the first DATA then BLOCKS; a `WINDOW_UPDATE` replenishes → it resumes and
    /// finishes. Bounded by `tokio::time::timeout` so a lost-wakeup hang fails instead of stalling.
    #[tokio::test]
    async fn streamed_exec_backpressure_pause_then_resume_b3() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1)], 1, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // 1-frame window: HEAD consumes it; the first DATA then parks on the empty window.
        let credit_cell = Arc::new(CreditCell::new(Credit::new(1, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();

        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool,
                "SELECT n",
                &[],
                None,
                true,
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        // HEAD goes out and consumes the 1 frame; the first DATA then parks.
        let head = timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("HEAD within the bound")
            .expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !task.is_finished(),
            "the producer must BLOCK on the exhausted window after HEAD"
        );
        assert!(
            control_rx.try_recv().is_err(),
            "no DATA yet — the credit window is empty"
        );

        // A WINDOW_UPDATE replenishes → the parked DATA send resumes and the stream finishes.
        credit_cell.replenish(5, 10_000_000);
        let data = timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("DATA after replenish — never a lost-wakeup hang")
            .expect("DATA");
        assert_eq!(data.frame.header.method, method_stream::DATA);
        timeout(Duration::from_secs(5), task)
            .await
            .expect("the producer finishes after replenish, never hangs")
            .expect("task join");
        match cell.lock().unwrap().clone() {
            Some(Terminal::Ok(body)) => {
                assert_eq!(ExecOk::decode(&body).expect("ExecOk").affected, 1);
            }
            other => panic!("expected an Ok terminal after resume, got {other:?}"),
        }
    }

    /// B3 cancel mid-backpressure (the v1 lost-wakeup blocker): a 1-frame window NEVER replenished +
    /// a client CANCEL → the parked producer unwinds, `finish()` runs, exactly ONE `Cancelled`
    /// terminal is declared, and the conn is released (a fresh checkout succeeds) — NO hang, NO
    /// leaked checkout.
    #[tokio::test]
    async fn streamed_exec_cancel_mid_backpressure_one_terminal_no_leak_b3() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1), i64_row(2)], 2, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // 1-frame window, never replenished: only the CANCEL can unwind the parked first DATA.
        let credit_cell = Arc::new(CreditCell::new(Credit::new(1, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();
        let cancel_fire = cancel.clone();

        // Clone the `Pool` (a cheap `Arc` handle — sharing conns, not forking a pool) so the test can
        // still checkout after the producer task takes its own handle.
        let pool_task = pool.clone();
        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool_task,
                "SELECT n",
                &[],
                None,
                true,
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        let head = timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("HEAD")
            .expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !task.is_finished(),
            "the producer is parked on the never-replenished window"
        );

        // The CANCEL unwinds the parked producer (a wait ONLY a WINDOW_UPDATE could otherwise wake).
        cancel_fire.cancel();
        timeout(Duration::from_secs(5), task)
            .await
            .expect("cancel must unwind the parked producer, never hang")
            .expect("task join");

        // Exactly ONE terminal: an autocommit READ cancel → Cancelled{NonRetryable} (never
        // Indeterminate for a read).
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::CANCELLED);
                assert_eq!(ep.branch, branch::NON_RETRYABLE);
            }
            other => panic!("expected exactly one Cancelled terminal, got {other:?}"),
        }
        // No leaked checkout: `co` dropped inside the producer → a fresh checkout on the size-1 pool
        // succeeds (the permit was released) — no hang.
        let _co2 = timeout(Duration::from_secs(5), pool.checkout())
            .await
            .expect("no leaked checkout, no hang")
            .expect("conn released");
    }

    /// Timeout mid-stream (the S4 timeout gate on the streamed path): a `timeout_ms` that elapses
    /// while the producer is parked mid-stream → stop + ONE fated terminal AFTER the DATA already
    /// sent. A dispatched streamed WRITE (readonly=false, sent=true) → `WriteUnconfirmed{Indeterminate}`.
    #[tokio::test(start_paused = true)]
    async fn streamed_exec_timeout_mid_stream_one_fated_terminal_after_data() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1), i64_row(2)], 2, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // 2-frame window: HEAD + the first DATA go out (sent=true); the second DATA parks on the
        // empty window and the `timeout_ms` deadline (never replenished) fires while it is parked.
        let credit_cell = Arc::new(CreditCell::new(Credit::new(2, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();

        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool,
                "INSERT INTO t SELECT n RETURNING n",
                &[],
                Some(50), // timeout_ms
                false,    // a WRITE
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        // HEAD + DATA1 go out before the deadline.
        let head = timeout(Duration::from_secs(30), control_rx.recv())
            .await
            .expect("HEAD")
            .expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        let data1 = timeout(Duration::from_secs(30), control_rx.recv())
            .await
            .expect("DATA1")
            .expect("DATA1");
        assert_eq!(data1.frame.header.method, method_stream::DATA);

        // The paused clock auto-advances to the 50ms deadline while the 2nd DATA is parked → abort.
        timeout(Duration::from_secs(30), task)
            .await
            .expect("the deadline aborts the parked stream, no hang")
            .expect("task join");
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(
                    ep.code,
                    errc::WRITE_UNCONFIRMED,
                    "a dispatched streamed write timeout is Indeterminate"
                );
                assert_eq!(ep.branch, branch::INDETERMINATE);
            }
            other => panic!("expected one WriteUnconfirmed terminal after the DATA, got {other:?}"),
        }
        assert!(
            control_rx.try_recv().is_err(),
            "the 2nd DATA was aborted while parked — it never went out"
        );
    }

    /// M7: exec_us excludes backpressure. A stream stalled on an empty credit window for a long
    /// (virtual) time must report `exec_us` ≈ the DB pull time (≈0 for the fake), NOT the wall-clock
    /// including the stall. Proven under the paused clock: the 60s stall is advanced while the
    /// producer is parked on a send (never inside a timed pull).
    #[tokio::test(start_paused = true)]
    async fn streamed_exec_exec_us_excludes_backpressure_stall_m7() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1), i64_row(2)], 2, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // 2-frame window: HEAD + DATA1 go out; DATA2 parks (no deadline this time).
        let credit_cell = Arc::new(CreditCell::new(Credit::new(2, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();

        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool,
                "SELECT n",
                &[],
                None,
                true,
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        // Drain HEAD + DATA1; the producer then parks on DATA2's empty window.
        timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("HEAD")
            .expect("HEAD");
        timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("DATA1")
            .expect("DATA1");
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!task.is_finished(), "the producer is parked on the window");

        // Advance the VIRTUAL clock 60s while parked on backpressure — time a whole-loop timer would
        // wrongly count into exec_us. Then replenish so the stream finishes.
        tokio::time::advance(Duration::from_secs(60)).await;
        credit_cell.replenish(5, 10_000_000);
        timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("DATA2 after replenish")
            .expect("DATA2");
        timeout(Duration::from_secs(5), task)
            .await
            .expect("finishes")
            .expect("join");

        match cell.lock().unwrap().clone() {
            Some(Terminal::Ok(body)) => {
                let ok = ExecOk::decode(&body).expect("ExecOk");
                assert_eq!(ok.affected, 2);
                assert!(
                    ok.stats.exec_us < 1_000_000,
                    "exec_us ({}) must exclude the 60s backpressure stall (M7): it times pulls only",
                    ok.stats.exec_us
                );
            }
            other => panic!("expected an Ok terminal, got {other:?}"),
        }
    }

    /// M6: the session cap returns to baseline after the terminal (not monotonic), and a 2nd stream
    /// on the SAME session works (no wedge). Also covers the HEAD-only (zero-row) stream → used == 0.
    #[tokio::test]
    async fn streamed_exec_session_cap_releases_and_second_stream_works_m6() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1), i64_row(2), i64_row(3)], 3, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let session_cap = Arc::new(SessionCap::new(1_000_000));

        // Stream #1: a multi-frame result. `drive_stream` drops every emitted ControlMsg (modelling
        // the writer), so all DATA cap guards release.
        let (frames1, term1) =
            drive_stream(&pool, &session_cap, "SELECT n", true, one_row_batch()).await;
        assert_eq!(count_data(&frames1), 3, "3 rows → 3 DATA frames");
        assert!(matches!(term1, Terminal::Ok(_)));
        assert_eq!(
            session_cap.used(),
            0,
            "M6: the session cap returns to baseline after all DATA guards drop"
        );

        // Stream #2 on the SAME session cap + the SAME size-1 pool (recycled conn): a zero-row stream
        // emits HEAD but no DATA, and must ALSO release the cap to 0 (no wedge).
        pool.backend().set_stream_script(script(vec![], 0, None));
        let (frames2, term2) =
            drive_stream(&pool, &session_cap, "SELECT n", true, one_row_batch()).await;
        assert_eq!(count_data(&frames2), 0, "a zero-row stream emits no DATA");
        assert_eq!(
            frames2
                .iter()
                .filter(|(s, m, _)| *s == service::STREAM && *m == method_stream::HEAD)
                .count(),
            1,
            "a zero-row stream still emits its HEAD"
        );
        assert!(
            matches!(term2, Terminal::Ok(_)),
            "a 2nd stream on the same session works (no wedge)"
        );
        assert_eq!(
            session_cap.used(),
            0,
            "a HEAD-only stream also releases the cap to baseline"
        );
    }

    /// A mid-stream `next()` error → exactly ONE `Outcome::Error` terminal with the classified fate,
    /// AFTER whatever DATA already went out. (The fake's mid-stream error is a `PoolError::Backend`
    /// → `classify_fate` → Protocol; a real server error would be a `Sql` passed through verbatim.)
    #[tokio::test]
    async fn streamed_exec_mid_stream_error_one_terminal_after_data() {
        let backend = FakeBackend::new();
        // error_at: Some(1) → emit exactly 1 row, then error on the next pull.
        backend.set_stream_script(script(vec![i64_row(1), i64_row(2)], 0, Some(1)));
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let session_cap = Arc::new(SessionCap::new(1_000_000));

        let (frames, terminal) =
            drive_stream(&pool, &session_cap, "SELECT n", true, one_row_batch()).await;

        assert_eq!(
            frames
                .iter()
                .filter(|(s, m, _)| *s == service::STREAM && *m == method_stream::HEAD)
                .count(),
            1,
            "HEAD went out before the error"
        );
        assert_eq!(
            count_data(&frames),
            1,
            "exactly one DATA (row 1) went out before the mid-stream error"
        );
        match terminal {
            Terminal::Error(ep) => assert_eq!(
                ep.code,
                errc::PROTOCOL,
                "the fake's mid-stream Backend error classifies to Protocol"
            ),
            other => panic!("expected exactly one Error terminal, got {other:?}"),
        }
        // Item 2: a NATURAL mid-stream error must NOT fire the out-of-band cancel — the statement
        // already self-terminated (that is why next() erred), so a side-connection CancelRequest
        // against an already-errored/idle backend would be pure waste.
        assert_eq!(
            pool.backend().cancel_calls(),
            0,
            "a natural mid-stream error must NOT fire the out-of-band cancel (item 2)"
        );
        assert_eq!(
            session_cap.used(),
            0,
            "the cap is released even on a mid-stream error"
        );
    }

    /// A checkout failure on the streamed path → the same never-sent fate as the buffered path
    /// (sent=false → a write is known-fate Retryable, never Indeterminate), and no HEAD/DATA/rows.
    #[tokio::test]
    async fn streamed_exec_checkout_failure_is_known_fate_no_frames() {
        let backend = FakeBackend::new();
        backend.arm_fail_connect(1); // the pool's only connect attempt fails
        let pool = Pool::new(backend, autocommit_test_pool_config());
        let session_cap = Arc::new(SessionCap::new(1_000_000));

        // A WRITE (readonly=false): a checkout failure is a provable did-not-apply → Retryable,
        // NOT Indeterminate (nothing was transmitted).
        let (frames, terminal) = drive_stream(
            &pool,
            &session_cap,
            "INSERT INTO t VALUES (1)",
            false,
            one_row_batch(),
        )
        .await;
        assert!(frames.is_empty(), "a checkout failure emits no HEAD/DATA");
        match terminal {
            Terminal::Error(ep) => {
                assert_eq!(ep.code, errc::CONNECTION_LOST);
                assert_eq!(ep.branch, branch::RETRYABLE);
                assert_ne!(ep.branch, branch::INDETERMINATE);
            }
            other => panic!("expected a known-fate ConnectionLost terminal, got {other:?}"),
        }
    }

    /// SAFETY (the §19.3 "defining safety property"): a streamed WRITE cancelled AFTER dispatch but
    /// BEFORE its first DATA frame is `WriteUnconfirmed{Indeterminate}` — NOT `Retryable`. `sent` is
    /// `true` for the whole loop because `query_stream` returning Ok means `query_raw` already
    /// dispatched the statement (rows pull lazily off a channel afterward); gating `sent` on
    /// DATA-delivered would mint an UNSAFE `Retryable` here and invite a double-apply. The 1-frame
    /// window is consumed by HEAD, so the CANCEL fires while the first DATA is parked — provably
    /// before ANY DATA went out.
    #[tokio::test]
    async fn streamed_write_cancel_before_first_data_is_indeterminate_not_retryable() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1)], 1, None));
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // 1-frame window: HEAD consumes it; the first DATA parks before it can go out.
        let credit_cell = Arc::new(CreditCell::new(Credit::new(1, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();
        let cancel_fire = cancel.clone();

        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool,
                "INSERT INTO t SELECT 1 RETURNING id",
                &[],
                None,
                false, // a WRITE
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        // HEAD goes out (consuming the 1 frame); the first DATA then parks — no DATA has gone out.
        let head = timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("HEAD")
            .expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            control_rx.try_recv().is_err(),
            "no DATA has gone out yet — the cancel below is provably before the first DATA"
        );

        cancel_fire.cancel();
        timeout(Duration::from_secs(5), task)
            .await
            .expect("cancel unwinds, no hang")
            .expect("task join");

        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(
                    ep.code,
                    errc::WRITE_UNCONFIRMED,
                    "a dispatched streamed write cancelled before its first row is Indeterminate"
                );
                assert_eq!(ep.branch, branch::INDETERMINATE);
                assert_ne!(
                    ep.branch,
                    branch::RETRYABLE,
                    "NEVER Retryable — that would invite a double-apply of a possibly-applied write"
                );
            }
            other => panic!("expected WriteUnconfirmed/Indeterminate, got {other:?}"),
        }
    }

    // ---- review round 2: items 1 (open race), 2 (cancel policy), 3 (pull-first tie) ------------

    /// Item 1 (MUST-FIX): `timeout_ms`/CANCEL are ENFORCED across the `query_stream` OPEN itself
    /// (prepare + query_raw — real backend round-trips, which a lock-blocked prepare could hang on
    /// past the deadline). A blocked open + a fired CANCEL → exactly ONE fated terminal, bounded (no
    /// hang), and the conn tainted (the aborted-open recycle path). No HEAD/DATA — the open never
    /// completed.
    #[tokio::test]
    async fn streamed_exec_open_blocked_cancel_one_terminal_no_hang() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![i64_row(1)], 1, None));
        backend.block_stream_open(); // freeze the OPEN (prepare+query_raw)
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        let credit_cell = Arc::new(CreditCell::new(Credit::new(10, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();
        let cancel_fire = cancel.clone();
        // A READ so the fated terminal is the clean Cancelled/NonRetryable (a WRITE would be
        // Indeterminate — also correct, but Cancelled is the sharper assertion for an open abort).
        let pool_task = pool.clone();
        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool_task,
                "SELECT n",
                &[],
                None,
                true,
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        // Wait until the OPEN is provably in flight (parked on the open gate) before firing CANCEL —
        // so this proves the OPEN-race arm, not a pre-dispatch shortcut.
        for _ in 0..1000 {
            if pool.backend().stream_opens_waiting() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            pool.backend().stream_opens_waiting(),
            1,
            "the open must be parked (timeout/cancel were unenforced across it before this fix)"
        );

        cancel_fire.cancel();
        timeout(Duration::from_secs(5), task)
            .await
            .expect("the CANCEL must abort the blocked open promptly, never hang")
            .expect("task join");

        // Exactly ONE fated terminal; no HEAD/DATA (the open never returned a handle).
        assert!(
            control_rx.try_recv().is_err(),
            "a cancelled open emits no HEAD/DATA"
        );
        match cell.lock().unwrap().clone() {
            Some(Terminal::Error(ep)) => {
                assert_eq!(ep.code, errc::CANCELLED);
                assert_eq!(ep.branch, branch::NON_RETRYABLE);
            }
            other => panic!("expected one Cancelled terminal, got {other:?}"),
        }
        // The out-of-band cancel WAS fired (an interruption abort), and the conn is released.
        assert!(
            pool.backend().cancel_calls() >= 1,
            "an interruption abort fires the out-of-band cancel"
        );
        pool.backend().release_stream_pulls(); // no-op here; keep the gate tidy
        let co2 = timeout(Duration::from_secs(5), pool.checkout())
            .await
            .expect("no leaked checkout after a cancelled open")
            .expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"RESET:Full".to_string()),
            "an aborted open taints the conn → Full reset at the next checkout: {:?}",
            co2.conn().recorded
        );
    }

    /// Item 3 (§5.2 consistency): the loop's biased select polls the PULL FIRST, so a stream draining
    /// to `None` in the same poll a CANCEL becomes ready ends in `end_ok` — never a fabricated error
    /// terminal for a query that FINISHED. The per-pull gate pauses the (empty) stream's first pull;
    /// releasing it and firing cancel back-to-back (no await between → the producer is not polled in
    /// the gap) makes both arms ready at the next poll, and pull-first must win.
    #[tokio::test]
    async fn streamed_exec_final_none_races_cancel_ends_ok_pull_first() {
        let backend = FakeBackend::new();
        backend.set_stream_script(script(vec![], 0, None)); // EMPTY: the first pull is None
        backend.block_stream_pulls(); // pause at the first (None) pull
        let pool = Pool::new(backend, autocommit_test_pool_config());

        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(16);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        let credit_cell = Arc::new(CreditCell::new(Credit::new(10, 10_000_000)));
        let (responder, cell) = Responder::new_streaming(
            STREAM_REQ_ID,
            Arc::clone(&credit_cell),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );
        let cancel = CancellationToken::new();
        let cancel_fire = cancel.clone();
        let pool_task = pool.clone();
        let task = tokio::spawn(async move {
            run_autocommit_streamed(
                responder,
                &pool_task,
                "SELECT n",
                &[],
                None,
                true,
                &cancel,
                one_row_batch(),
            )
            .await;
        });

        // HEAD goes out (cancel not yet fired), then the first pull parks on the gate.
        let head = timeout(Duration::from_secs(5), control_rx.recv())
            .await
            .expect("HEAD")
            .expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        for _ in 0..1000 {
            if pool.backend().stream_pulls_waiting() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            pool.backend().stream_pulls_waiting(),
            1,
            "the producer must be parked on the final (None) pull"
        );

        // Release the pull AND fire cancel back-to-back with NO await between: single-threaded, so
        // the producer is not polled in the gap, and at its next poll BOTH the pull (→ None) and the
        // cancel are ready. Pull-first must resolve to None → end_ok.
        pool.backend().release_stream_pulls();
        cancel_fire.cancel();

        timeout(Duration::from_secs(5), task)
            .await
            .expect("the drained stream must finish, never hang")
            .expect("task join");

        match cell.lock().unwrap().clone() {
            Some(Terminal::Ok(body)) => {
                let ok = ExecOk::decode(&body).expect("ExecOk");
                assert_eq!(ok.affected, 0);
                assert_eq!(ok.stats.rows, 0);
            }
            other => panic!(
                "a stream that DRAINED (final None) must end in end_ok even when a cancel is also \
                 ready — pull-first (§5.2), got {other:?}"
            ),
        }
    }
}
