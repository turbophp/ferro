//! SQL/`COPY_IN` and SQL/`COPY_OUT` (M3-D4; SPEC §6.1, `/proto/PROTOCOL.md` §13): PostgreSQL's COPY
//! sub-protocol, with the client's statement passed through unmodified.
//!
//! **`COPY_IN`** (`COPY … FROM STDIN`): the handler reserves the COPY's client-to-engine window
//! against the session's inbound cap, checks out, starts the COPY, and only THEN grants the client
//! its window with a `CORE/WINDOW_UPDATE` on the request's id — the client sends nothing before that,
//! so no byte is ever sent into a COPY that failed to start. The reader loop routes each
//! STREAM/`COPY_DATA` chunk here without waiting (`session::registry::Registry::deliver`); this
//! handler forwards it to the server and re-grants as it goes. STREAM/`COPY_DONE` ends the data.
//!
//! **`COPY_OUT`** (`COPY … TO STDOUT`): exactly a stream — STREAM/`COPY_DATA` frames under the
//! request's credit window and the session cap, then the ONE terminal.
//!
//! **Fate (§19.3), and the one fact it stands on: PostgreSQL completes a `COPY FROM STDIN` only on
//! `CopyDone`.** So until the engine has begun [`CopyInHandle::finish`], NOTHING of the COPY can
//! apply, whatever happens — a client CANCEL, a deadline, the session dying, a lost backend link, a
//! flow-control violation. All of those abort the COPY (`CopyFail`, acknowledged) and report the
//! KNOWN did-not-apply through `fate::classify_fate` with `sent: false` — `Retryable` in autocommit,
//! `TxDeadline{Retryable}` with the transaction rolled back inside one. Once `finish` has begun the
//! S4 rules apply verbatim with `sent: true`: a cancelled or timed-out autocommit COPY that could not
//! be confirmed is `Indeterminate`, a lost link is `Indeterminate`, and a server error is the known
//! fate it reports. The engine never re-runs a COPY (charter rule 3).
//!
//! `COPY_OUT` trusts the CLIENT's `readonly` declaration exactly as a streamed EXEC does — a
//! `COPY (DELETE … RETURNING *) TO STDOUT` writes, and the engine does not infer otherwise.
//!
//! [`CopyInHandle::finish`]: ferro_pool::pool::CopyInHandle::finish

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use ferro_classify::{CopyDirection, copy_direction};
use ferro_pool::backend::{Cancel, PoolBackend};
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool};
use ferro_proto::consts::errc;
use ferro_proto::messages::CopyRequest;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::pools::{AnyPool, PoolRegistry};
use crate::services::fate::{self, OpContext};
use crate::services::sql::{
    StreamEnded, actor_gone_terminal, build_stream_terminal_body, protocol, resolve_active,
    send_err_to_pool_error, sleep_until_opt, stream_cancel_error, unsupported,
};
use crate::session::SessionId;
use crate::session::codec::InFrame;
use crate::session::registry::Inbound;
use crate::session::responder::{InboundRx, OpenInboundError, Responder, StreamSendError};
use crate::tx::{TxCommand, TxRegistry};

/// The most COPY bytes one engine-to-client `COPY_DATA` frame carries. PostgreSQL sends one
/// `CopyData` message per ROW, so the producer coalesces the rows the backend has already produced
/// up to this size (cutting per-frame overhead without ever waiting to fill a frame) and splits a row
/// larger than it (COPY bytes are divisible, so — unlike a row stream —
/// no row is ever too large for the frame ceiling).
pub const COPY_OUT_FRAME_BYTES: usize = 256 * 1024;

/// COPY counters for SPEC §13's Prometheus endpoint.
#[derive(Debug)]
pub struct CopyCounters {
    in_bytes: AtomicU64,
    out_bytes: AtomicU64,
    violations: AtomicU64,
}

impl CopyCounters {
    const fn new() -> Self {
        CopyCounters {
            in_bytes: AtomicU64::new(0),
            out_bytes: AtomicU64::new(0),
            violations: AtomicU64::new(0),
        }
    }
    pub fn record_violation(&self) {
        self.violations.fetch_add(1, Ordering::Relaxed);
    }
    pub fn in_bytes(&self) -> u64 {
        self.in_bytes.load(Ordering::Relaxed)
    }
    pub fn out_bytes(&self) -> u64 {
        self.out_bytes.load(Ordering::Relaxed)
    }
    pub fn violations(&self) -> u64 {
        self.violations.load(Ordering::Relaxed)
    }
}

pub static COUNTERS: CopyCounters = CopyCounters::new();

/// The error a COPY_IN stopped before its end-of-data reports. It carries `errc::CANCELLED` so it
/// takes `fate::classify_fate`'s 57014 override, whose `sent: false` arm is exactly this case: a
/// known did-not-apply (`Retryable`), or in a transaction `TxDeadline` with the transaction rolled
/// back. No SQLSTATE is invented: the stop may have come from the client, a deadline or the engine.
fn copy_stopped_before_done(why: &str) -> PoolError {
    PoolError::Sql {
        code: errc::CANCELLED,
        branch: errc::CANCELLED_BRANCH,
        sqlstate: None,
        errno: None,
        message: format!(
            "COPY FROM STDIN stopped before its end-of-data ({why}); PostgreSQL completes a COPY \
             only on its end-of-data, so nothing of it was applied"
        ),
    }
}

/// `service=SQL, method=COPY_IN|COPY_OUT`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_copy(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    cancel: CancellationToken,
    direction: CopyDirection,
) {
    let req = match CopyRequest::decode(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(protocol(format!("malformed CopyRequest: {e}")));
            return;
        }
    };
    if direction == CopyDirection::In && req.readonly {
        // The METHOD is the write; a readonly declaration on it would make `fate.rs` suppress
        // `Indeterminate` for a write. Refused rather than ignored.
        responder.end_error(unsupported(
            "COPY_IN is a write (COPY … FROM STDIN) and cannot be declared readonly",
        ));
        return;
    }
    // The statement's SHAPE, checked before anything else is involved — no connection, no
    // transaction actor. A refusal here sent nothing, so it must leave a transaction exactly as
    // EXEC's refusal does: open (review F2 — run in the actor, it had torn the transaction down).
    // `Checkout::copy_in`/`copy_out` keep the same check as defence in depth for any other caller.
    if copy_direction(&req.sql) != Some(direction) {
        responder.end_error(unsupported(match direction {
            CopyDirection::In => {
                "COPY_IN requires exactly one `COPY … FROM STDIN` statement (refused before it \
                 reached a connection; nothing was sent)"
            }
            CopyDirection::Out => {
                "COPY_OUT requires exactly one `COPY … TO STDOUT` statement (refused before it \
                 reached a connection; nothing was sent)"
            }
        }));
        return;
    }
    let deadline = req
        .timeout_ms
        .map(|ms| tokio::time::Instant::now() + Duration::from_millis(u64::from(ms)));

    match req.tx_id {
        Some(tx_id) => {
            let handle = match resolve_active(tx_registry, tx_id, session_id) {
                Ok(h) => h,
                Err(ep) => {
                    responder.end_error(ep);
                    return;
                }
            };
            if !handle.copy {
                responder.end_error(copy_unsupported());
                return;
            }
            // The data channel (and the wait for the session's COPY capacity) is opened INSIDE the
            // actor ([`run_tx_copy`]), never here: a stop during that wait must end the transaction
            // for the `TxDeadline` it reports to be true, and only the actor can end it (review F1 —
            // opened here, a CANCEL reported "rolled back" while the transaction lived on and
            // committed). The connection is pinned already, so waiting there holds nothing new.
            let (done_tx, done_rx) = oneshot::channel::<()>();
            let cmd = TxCommand::Copy {
                direction,
                sql: req.sql.clone(),
                timeout_ms: req.timeout_ms,
                readonly: req.readonly,
                cancel,
                responder,
                done: done_tx,
            };
            match handle.cmd_tx.send(cmd).await {
                Ok(()) => {
                    let _ = done_rx.await;
                }
                Err(mpsc::error::SendError(cmd)) => {
                    if let TxCommand::Copy { responder, .. } = cmd {
                        responder.end_error(actor_gone_terminal(tx_registry, tx_id, session_id));
                    }
                }
            }
        }
        None => {
            let Some(pool) = registry.get(&req.pool) else {
                responder.end_error(unsupported(format!("unknown pool {:?}", req.pool)));
                return;
            };
            // ONE authority for the capability, refused before any checkout (the
            // `supports_row_streaming` shape).
            let supported = match pool {
                AnyPool::Pg(p) => p.backend().supports_copy(),
                AnyPool::Mysql(p) => p.backend().supports_copy(),
                AnyPool::Sqlite(p) => p.backend().supports_copy(),
            };
            if !supported {
                responder.end_error(copy_unsupported());
                return;
            }
            match pool {
                AnyPool::Pg(p) => {
                    run_autocommit_copy(p, responder, &req, direction, cancel, deadline).await
                }
                AnyPool::Mysql(p) => {
                    run_autocommit_copy(p, responder, &req, direction, cancel, deadline).await
                }
                AnyPool::Sqlite(p) => {
                    run_autocommit_copy(p, responder, &req, direction, cancel, deadline).await
                }
            }
        }
    }
}

fn copy_unsupported() -> ferro_proto::messages::ErrorPayload {
    unsupported(
        "COPY is not supported on this pool's backend (PostgreSQL only; MySQL's LOAD DATA LOCAL \
         INFILE and SQLite have no COPY sub-protocol here)",
    )
}

/// For a COPY_IN, reserve its window and open its data channel — in autocommit BEFORE any checkout,
/// so a session at its inbound cap never holds a pooled connection while it waits; in a transaction
/// inside the actor. A wait the client cancels or a deadline ends is a known did-not-apply. `None`
/// for a COPY_OUT. The error's `bool` says whether the request was STOPPED (which ends a
/// transaction, like any other stop of an in-tx statement) rather than refused.
async fn open_inbound_for(
    direction: CopyDirection,
    responder: &Responder,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    in_tx: bool,
) -> Result<Option<InboundRx>, (ferro_proto::messages::ErrorPayload, bool)> {
    if direction == CopyDirection::Out {
        return Ok(None);
    }
    match responder.open_inbound(cancel, deadline).await {
        Ok(rx) => Ok(Some(rx)),
        Err(OpenInboundError::Aborted(_)) => Err((
            fate::classify_fate(
                copy_stopped_before_done("waiting for the session's COPY capacity"),
                OpContext {
                    readonly: false,
                    sent: false,
                    in_tx,
                },
            ),
            true,
        )),
        Err(OpenInboundError::Unavailable) => {
            Err((protocol("COPY_IN could not open its data channel"), false))
        }
    }
}

/// A tx-scoped COPY, run by the transaction's actor on its pinned `co`: open the COPY_IN's data
/// channel here (see `handle_copy`), then [`run_copy`]. A stop while waiting for capacity returns
/// `Broken`, so the actor rolls back and tombstones — which is what the `TxDeadline` it reports says.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_tx_copy<B: PoolBackend>(
    co: &mut Checkout<B>,
    direction: CopyDirection,
    sql: &str,
    responder: Responder,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    readonly: bool,
) -> StreamEnded {
    let inbound = match open_inbound_for(direction, &responder, cancel, deadline, true).await {
        Ok(i) => i,
        Err((ep, stopped)) => {
            responder.end_error(ep);
            return if stopped {
                StreamEnded::Broken
            } else {
                StreamEnded::Intact
            };
        }
    };
    run_copy(
        co, direction, sql, responder, inbound, cancel, deadline, readonly, true, 0,
    )
    .await
}

async fn run_autocommit_copy<B: PoolBackend>(
    pool: &Pool<B>,
    responder: Responder,
    req: &CopyRequest,
    direction: CopyDirection,
    cancel: CancellationToken,
    deadline: Option<tokio::time::Instant>,
) {
    let inbound = match open_inbound_for(direction, &responder, &cancel, deadline, false).await {
        Ok(i) => i,
        Err((ep, _)) => {
            responder.end_error(ep);
            return;
        }
    };
    // The checkout is raced against the same deadline and CANCEL as the COPY (the M3-D1c rule): a
    // request that never got a connection was never sent.
    let checkout = pool.checkout_declared(req.readonly);
    tokio::pin!(checkout);
    let checked_out = tokio::select! {
        biased;
        r = &mut checkout => r,
        () = sleep_until_opt(deadline) => Err(PoolError::Timeout),
        () = cancel.cancelled() => Err(crate::services::sql::cancelled_before_dispatch()),
    };
    let mut co = match checked_out {
        Ok(co) => co,
        Err(e) => {
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
    run_copy(
        &mut co,
        direction,
        &req.sql,
        responder,
        inbound,
        &cancel,
        deadline,
        req.readonly,
        false,
        queue_us,
    )
    .await;
    drop(co);
}

/// Run one COPY on `co` and declare its ONE terminal. Shared by the autocommit path and the tx
/// actor (`in_tx: true`). The return value tells the actor whether its transaction survives.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_copy<B: PoolBackend>(
    co: &mut Checkout<B>,
    direction: CopyDirection,
    sql: &str,
    responder: Responder,
    inbound: Option<InboundRx>,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    readonly: bool,
    in_tx: bool,
    queue_us: u64,
) -> StreamEnded {
    match (direction, inbound) {
        (CopyDirection::In, Some(inbound)) => {
            run_copy_in(
                co, sql, responder, inbound, cancel, deadline, in_tx, queue_us,
            )
            .await
        }
        (CopyDirection::Out, _) => {
            run_copy_out(
                co, sql, responder, cancel, deadline, readonly, in_tx, queue_us,
            )
            .await
        }
        (CopyDirection::In, None) => {
            responder.end_error(protocol("COPY_IN without a data channel"));
            StreamEnded::Intact
        }
    }
}

/// Did this error end the transaction it ran in? A server's own rejection (a malformed row, a
/// constraint) leaves the transaction open — failed, for the client to roll back, exactly as a
/// failed EXEC does — and so does a refusal that sent nothing (`Unsupported`). A cancel/timeout
/// (57014) or anything else — a lost link, a timeout — ends it.
fn ends_tx(e: &PoolError) -> bool {
    fate::is_57014(e) || !matches!(e, PoolError::Sql { .. } | PoolError::Unsupported(_))
}

#[allow(clippy::too_many_arguments)]
async fn run_copy_in<B: PoolBackend>(
    co: &mut Checkout<B>,
    sql: &str,
    responder: Responder,
    mut inbound: InboundRx,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    in_tx: bool,
    queue_us: u64,
) -> StreamEnded {
    let not_applied = |why: &str| {
        fate::classify_fate(
            copy_stopped_before_done(why),
            OpContext {
                readonly: false,
                sent: false,
                in_tx,
            },
        )
    };
    let before_done = |e: PoolError| {
        fate::classify_fate(
            e,
            OpContext {
                readonly: false,
                sent: false,
                in_tx,
            },
        )
    };
    // Out-of-band cancel handles, captured BEFORE the COPY borrows `co`.
    let cancel_open = co.cancel_handle();
    let cancel_finish = co.cancel_handle();
    let start = Instant::now();

    // (1) Open. Drain, don't drop: an interrupted open fires the backend cancel and is AWAITED, so
    // the connection's COPY sub-protocol state is never abandoned half-way.
    let mut stopped: Option<&'static str> = None;
    let open = co.copy_in(sql);
    tokio::pin!(open);
    let opened = tokio::select! {
        biased;
        r = &mut open => r,
        () = sleep_until_opt(deadline) => {
            stopped = Some("deadline");
            cancel_open.cancel().await;
            (&mut open).await
        }
        () = cancel.cancelled() => {
            stopped = Some("cancelled");
            cancel_open.cancel().await;
            (&mut open).await
        }
    };
    let handle = match (opened, stopped) {
        (Ok(h), None) => h,
        (Ok(h), Some(why)) => {
            // The open raced the stop and won: abandon it at once.
            h.abort().await;
            responder.end_error(not_applied(why));
            return StreamEnded::Broken;
        }
        (Err(e), why) => {
            let broken = why.is_some() || ends_tx(&e);
            responder.end_error(match why {
                Some(w) => not_applied(w),
                None => before_done(e),
            });
            return if broken {
                StreamEnded::Broken
            } else {
                StreamEnded::Intact
            };
        }
    };
    let mut handle = handle;

    // (2) The COPY has started: grant the client its window. This frame IS "go".
    let w = inbound.window();
    if responder
        .grant(&inbound, w.frames(), w.bytes())
        .await
        .is_err()
    {
        handle.abort().await;
        responder.end_error(not_applied("the session ended"));
        return StreamEnded::Broken;
    }

    // (3) Data, until COPY_DONE. Every stop in here aborts with nothing applied.
    let mut bytes_in: u64 = 0;
    loop {
        let msg = tokio::select! {
            biased;
            () = cancel.cancelled() => Err("cancelled"),
            () = sleep_until_opt(deadline) => Err("deadline"),
            m = inbound.recv() => m.ok_or("the request's data channel closed"),
        };
        let chunk = match msg {
            Ok(Inbound::Done) => break,
            Ok(Inbound::Data(b)) => b,
            Err(why) => {
                handle.abort().await;
                responder.end_error(not_applied(why));
                return StreamEnded::Broken;
            }
        };
        let n = chunk.len();
        let sent = tokio::select! {
            biased;
            r = handle.send(chunk) => r.map_err(Some),
            () = cancel.cancelled() => Err(None),
            () = sleep_until_opt(deadline) => Err(None),
        };
        match sent {
            Ok(()) => {}
            Err(None) => {
                // Stopped while the backend applied backpressure: fire its cancel too, so a server
                // stuck on a lock wait lets go of the abort.
                cancel_finish.cancel().await;
                handle.abort().await;
                responder.end_error(not_applied("stopped while the backend was busy"));
                return StreamEnded::Broken;
            }
            Err(Some(e)) => {
                // The server rejected the COPY (a malformed row, a constraint) or the link is gone:
                // nothing applied either way, and a server error is reported as itself.
                let broken = ends_tx(&e);
                handle.abort().await;
                responder.end_error(before_done(e));
                return if broken {
                    StreamEnded::Broken
                } else {
                    StreamEnded::Intact
                };
            }
        }
        bytes_in += n as u64;
        COUNTERS.in_bytes.fetch_add(n as u64, Ordering::Relaxed);
        if let Some((frames, bytes)) = inbound.consumed(n)
            && responder.grant(&inbound, frames, bytes).await.is_err()
        {
            handle.abort().await;
            responder.end_error(not_applied("the session ended"));
            return StreamEnded::Broken;
        }
    }
    // No more data is accepted for this request: a late chunk is discarded, never queued.
    drop(inbound);

    // (4) End-of-data. From here the COPY MAY apply: drain, never drop, and the S4 fate rules.
    let finish = handle.finish();
    tokio::pin!(finish);
    let result = tokio::select! {
        biased;
        r = &mut finish => r,
        () = sleep_until_opt(deadline) => {
            cancel_finish.cancel().await;
            (&mut finish).await
        }
        () = cancel.cancelled() => {
            cancel_finish.cancel().await;
            (&mut finish).await
        }
    };
    let exec_us = start.elapsed().as_micros() as u64;
    match result {
        Ok(end) => {
            let body =
                build_stream_terminal_body(end.affected, None, 0, queue_us, exec_us, bytes_in);
            responder.end_ok(Bytes::from(body));
            StreamEnded::Intact
        }
        Err(e) => {
            let broken = ends_tx(&e);
            responder.end_error(fate::classify_fate(
                e,
                OpContext {
                    readonly: false,
                    sent: true,
                    in_tx,
                },
            ));
            if broken {
                StreamEnded::Broken
            } else {
                StreamEnded::Intact
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_copy_out<B: PoolBackend>(
    co: &mut Checkout<B>,
    sql: &str,
    responder: Responder,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
    readonly: bool,
    in_tx: bool,
    queue_us: u64,
) -> StreamEnded {
    let ctx = OpContext {
        readonly,
        sent: true,
        in_tx,
    };
    let cancel_open = co.cancel_handle();
    let cancel_loop = co.cancel_handle();
    let start = Instant::now();

    let mut stopped = false;
    let open = co.copy_out(sql);
    tokio::pin!(open);
    let opened = tokio::select! {
        biased;
        r = &mut open => r,
        () = sleep_until_opt(deadline) => {
            stopped = true;
            cancel_open.cancel().await;
            (&mut open).await
        }
        () = cancel.cancelled() => {
            stopped = true;
            cancel_open.cancel().await;
            (&mut open).await
        }
    };
    let mut handle = match (opened, stopped) {
        (Ok(h), false) => h,
        (Ok(h), true) => {
            let _ = h.finish().await;
            responder.end_error(fate::classify_fate(stream_cancel_error(), ctx));
            return StreamEnded::Broken;
        }
        (Err(e), stopped) => {
            let broken = stopped || ends_tx(&e);
            responder.end_error(fate::classify_fate(
                if stopped { stream_cancel_error() } else { e },
                ctx,
            ));
            return if broken {
                StreamEnded::Broken
            } else {
                StreamEnded::Intact
            };
        }
    };

    let mut buf = BytesMut::new();
    let mut sent_bytes: u64 = 0;
    loop {
        let step = tokio::select! {
            biased;
            c = handle.next() => Ok(c),
            () = cancel.cancelled() => Err(()),
            () = sleep_until_opt(deadline) => Err(()),
        };
        let item = match step {
            Ok(item) => item,
            Err(()) => {
                cancel_loop.cancel().await;
                let _ = handle.finish().await;
                responder.end_error(fate::classify_fate(stream_cancel_error(), ctx));
                return StreamEnded::Broken;
            }
        };
        // Coalesce what the backend has ALREADY produced — never wait for more: under load the
        // frames fill up, and a slow export (rows trickling out of a long query) still reaches the
        // client row by row instead of after `COPY_OUT_FRAME_BYTES` have accumulated. Polling
        // `next()` once and dropping it is safe: the driver's stream loses nothing it has not
        // yielded.
        let mut item = item;
        let mut last = false;
        loop {
            match item {
                Some(Ok(b)) => buf.extend_from_slice(&b),
                Some(Err(e)) => {
                    let broken = ends_tx(&e);
                    let _ = handle.finish().await;
                    responder.end_error(fate::classify_fate(e, ctx));
                    return if broken {
                        StreamEnded::Broken
                    } else {
                        StreamEnded::Intact
                    };
                }
                None => {
                    last = true;
                    break;
                }
            }
            if buf.len() >= COPY_OUT_FRAME_BYTES {
                break;
            }
            match handle.next().now_or_never() {
                Some(next) => item = next,
                None => break,
            }
        }
        while !buf.is_empty() {
            let take = buf.len().min(COPY_OUT_FRAME_BYTES);
            let chunk = buf.split_to(take);
            match responder.send_copy_data(&chunk, cancel, deadline).await {
                Ok(_) => {
                    sent_bytes += take as u64;
                    COUNTERS.out_bytes.fetch_add(take as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    let err = send_err_to_pool_error(e);
                    if !matches!(e, StreamSendError::LinkLost) {
                        cancel_loop.cancel().await;
                    }
                    let _ = handle.finish().await;
                    responder.end_error(fate::classify_fate(err, ctx));
                    return StreamEnded::Broken;
                }
            }
        }
        if last {
            break;
        }
    }
    let exec_us = start.elapsed().as_micros() as u64;
    match handle.finish().await {
        Ok(end) => {
            let body = build_stream_terminal_body(
                end.affected,
                None,
                end.affected,
                queue_us,
                exec_us,
                sent_bytes,
            );
            responder.end_ok(Bytes::from(body));
            StreamEnded::Intact
        }
        Err(e) => {
            let broken = ends_tx(&e);
            responder.end_error(fate::classify_fate(e, ctx));
            if broken {
                StreamEnded::Broken
            } else {
                StreamEnded::Intact
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_or_a_server_error_keeps_the_transaction_and_a_stop_ends_it() {
        assert!(!ends_tx(&PoolError::Unsupported("shape".into())));
        assert!(!ends_tx(&PoolError::Sql {
            code: errc::PROTOCOL,
            branch: errc::PROTOCOL_BRANCH,
            sqlstate: Some("22P02".into()),
            errno: None,
            message: "bad row".into(),
        }));
        assert!(ends_tx(&copy_stopped_before_done("cancelled")));
        assert!(ends_tx(&PoolError::ConnectionLost));
        assert!(ends_tx(&PoolError::Timeout));
    }
}
