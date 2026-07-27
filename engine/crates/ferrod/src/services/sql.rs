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
//!  5. shape by `fetch` (`rows` → include rows; `none` → drop rows, keep `cols`+`affected`), encode
//!     the terminal body, and SIZE-CHECK the FULLY-ENCODED `Outcome::Ok` payload (not the raw body
//!     — see [`OUTCOME_OK_OVERHEAD`]).
//!  6. declare the terminal `Outcome::Ok(body)` (or a mapped `Outcome::Error`) via the `Responder`;
//!     the supervisor sends the ONE terminal `END` on the existing S3 control path — exactly-one-END
//!     is untouched (no second terminal, no DATA frames).
//!
//! Error mapping ([`pool_error_to_payload`]) layers the §19.3 `Indeterminate` classification on TOP
//! of the pool's coarse taxonomy, WITHOUT any read/write inference (charter rule 6): it branches on
//! the client-declared `readonly` flag ALONE. A known-fate `PoolError::Sql` (a server rejection OR
//! a client-side bind pre-validation) passes through VERBATIM — the `readonly` override never
//! applies to it. The engine NEVER retries a user statement (charter rule 3); the wire branch only
//! informs the client's own policy.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::FutureExt;

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use ferro_proto::consts::{MAX_FRAME_PAYLOAD, errc, method_sql, service};
use ferro_proto::messages::ErrorPayload;
use ferro_proto::messages::sql::{ExecOk, ExecRequest, Stats};

use crate::pools::PoolRegistry;
use crate::session::codec::InFrame;
use crate::session::responder::Responder;
use crate::session::{HandlerFactory, HandlerFn};
use crate::tx::TxRegistry;

/// `ExecRequest.fetch` modes. 0 = rows, 1 = none (affected only), 2 = stream (reserved — `Unsupported`
/// in M0 per D-S5-1). Kept as named constants (not magic numbers) at the handler boundary.
const FETCH_ROWS: u8 = 0;
const FETCH_NONE: u8 = 1;
const FETCH_STREAM: u8 = 2;

/// Bytes `Outcome::Ok(body).encode()` prepends to `body`: the outer 2-element fixarray marker
/// (`0x92`) plus the 1-byte positive-fixint status (`outcome::OK`). The terminal size-check MUST
/// account for this envelope — checking the raw body instead lets a body in the `{MAX-1, MAX}` band
/// pass, then the wrapped frame overflows `MAX_FRAME_PAYLOAD`, `FrameCodec::encode` hard-errors, the
/// writer loop breaks, and the WHOLE session tears down (BLOCKER-v2 — a zero-END violation for this
/// AND every other in-flight terminal). Locked against the codec by `outcome_ok_overhead_is_two`.
const OUTCOME_OK_OVERHEAD: usize = 2;

/// Build the real EXEC `HandlerFactory`, capturing the pool registry and the shared
/// `Arc<TxRegistry>` (S6 seam). The factory mints one `HandlerFn` per connection given its
/// `SessionId`. Any request-bearing frame that is NOT `service=SQL, method=EXEC` (a TX/STREAM
/// method, or an unrecognized SQL method) still declares `Unsupported`.
///
/// S6-Task3: `session_id` + `tx_registry` become load-bearing here — a tx-scoped `EXEC`
/// (`tx_id.is_some()`) will look the actor up by `(tx_id, session_id)` and forward the statement,
/// while the autocommit path (`tx_id == None`) stays byte-for-byte identical to S5. For now the
/// factory ignores the session id and behaves exactly as S5 did.
pub fn make_handler(registry: Arc<PoolRegistry>, tx_registry: Arc<TxRegistry>) -> HandlerFactory {
    Arc::new(move |_session_id| -> HandlerFn {
        let registry = registry.clone();
        // Captured now so the S6-Task3 tx-scoped routing has it in hand; unused on the autocommit
        // path (the only path this task builds).
        let _tx_registry = tx_registry.clone();
        Arc::new(move |frame, responder, _cancel| {
            let registry = registry.clone();
            async move {
                handle(frame, responder, &registry).await;
            }
            .boxed()
        })
    })
}

async fn handle(frame: InFrame, responder: Responder, registry: &PoolRegistry) {
    // Only service=SQL, method=EXEC is implemented in M0. Anything else routed here (TX/STREAM, or
    // an unrecognized SQL method) is a clean per-request Unsupported — one END, session survives.
    if frame.header.service != service::SQL || frame.header.method != method_sql::EXEC {
        responder.end_error(unsupported("service/method not yet implemented"));
        return;
    }

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
        FETCH_ROWS | FETCH_NONE => {}
        FETCH_STREAM => {
            responder.end_error(unsupported(
                "fetch=stream is post-M0 (D-S5-1: M0 buffers the result into one terminal frame)",
            ));
            return;
        }
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
    let Some(pool) = registry.get(&req.pool) else {
        responder.end_error(unsupported(format!("unknown pool {:?}", req.pool)));
        return;
    };

    // (3) checkout → queue_us (the pool-wait, including any recycle cleanup on the popped conn).
    let mut co = match pool.checkout().await {
        Ok(co) => co,
        Err(e) => {
            // A checkout failure means NO connection was established, so the user statement was
            // NEVER transmitted → fate is a KNOWN "did-not-apply", never Indeterminate (`sent=false`).
            responder.end_error(pool_error_to_payload(e, req.readonly, false));
            return;
        }
    };
    let queue_us = co.stats().queue_us;

    // (4) run the GUARDED row-returning entry. exec_us times ONLY the DB query — the conn is
    // released (below) before framing, so a slow client cannot inflate it (D-S5-1). NEVER
    // conn_mut()/the raw client here (that bypasses the tx-control guard → cross-tenant leak).
    let exec_start = Instant::now();
    let result = co.query(sql, &req.params).await;
    let exec_us = exec_start.elapsed().as_micros() as u64;

    // Release the pooled connection BEFORE framing/sending (RAII): held only for the query.
    drop(co);

    let result = match result {
        Ok(r) => r,
        Err(e) => {
            // The statement WAS transmitted (checkout succeeded, `co.query` ran), so a connection
            // loss here has a genuinely UNKNOWN fate → eligible for §19.3 Indeterminate (`sent=true`).
            responder.end_error(pool_error_to_payload(e, req.readonly, true));
            return;
        }
    };

    // (5)+(6) shape, size-check the ENCODED terminal payload, declare the single terminal.
    match build_terminal_body(result, req.fetch, queue_us, exec_us) {
        Ok(body) => responder.end_ok(Bytes::from(body)),
        Err(ep) => responder.end_error(ep),
    }
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

    // PG has no LAST_INSERT_ID (callers use `RETURNING`), so `last_insert_id` is always `None` on
    // this backend in M0. The field/codec path is exercised by the golden vectors, not here.
    let mut exec_ok = ExecOk {
        cols: result.cols,
        rows,
        affected: result.affected,
        last_insert_id: None,
        stats: Stats {
            queue_us,
            exec_us,
            rows: nrows,
            bytes: 0,
        },
    };

    // `stats.bytes` IS the encoded terminal body length, but the body carries `stats.bytes`, so
    // setting it can widen that field and grow the body. Iterate to a FIXED POINT so the reported
    // count is EXACT: body length is monotonic non-decreasing in the `bytes` value (a larger value
    // is the same-or-wider msgpack uint), and each step sets `bytes` to the current length, so the
    // sequence increases and converges in a couple of steps. (Bounded defensively; the size-check
    // below always uses the FINAL body regardless, so the one-frame bound is exact either way.)
    let mut body = exec_ok.encode();
    for _ in 0..8 {
        let len = body.len() as u64;
        if exec_ok.stats.bytes == len {
            break;
        }
        exec_ok.stats.bytes = len;
        body = exec_ok.encode();
    }

    // Size-check the FULLY-ENCODED Outcome::Ok payload (body + the 2-byte envelope), NOT the raw
    // body (BLOCKER-v2). A per-request Unsupported is the clean alternative to a session teardown.
    if body.len() + OUTCOME_OK_OVERHEAD > MAX_FRAME_PAYLOAD as usize {
        return Err(unsupported(
            "result exceeds one frame; streaming is post-M0 (D-S5-1)",
        ));
    }
    Ok(body)
}

/// Map a `PoolError` to a wire `ErrorPayload`. NEVER retries (charter rule 3); the branch only
/// informs the client's own policy.
///
/// The §19.3 `Indeterminate` split is layered on the client-declared `readonly` flag ALONE (no SQL
/// read/write inference — charter rule 6). A known-fate `PoolError::Sql` (server rejection OR a
/// client-side bind pre-validation, both fate-known) passes through VERBATIM — the override never
/// touches it.
///
/// `sent` gates the Indeterminate branch: a `ConnectionLost` is only Indeterminate if the statement
/// was actually TRANSMITTED (`sent=true`, a mid-`co.query()` loss whose fate is unknown). A
/// checkout-time connect failure (`sent=false` — DB down/restarting, the §19 bounce) means the SQL
/// was never sent, so its fate is a KNOWN "did-not-apply" → `ConnectionLost{Retryable}`, never a
/// false `WriteUnconfirmed`. Reporting fate-unknown for a provably-not-applied write would corrupt
/// the §19.3 guarantee just as much as a spurious auto-retry would.
fn pool_error_to_payload(err: PoolError, readonly: bool, sent: bool) -> ErrorPayload {
    match err {
        // Known-fate: pass the proto classification through verbatim. Never Indeterminate.
        PoolError::Sql {
            code,
            branch,
            sqlstate,
            message,
        } => ErrorPayload {
            code,
            branch,
            sqlstate,
            errno: None,
            message,
            detail: None,
            retry_after_ms: None,
        },
        // A connection loss. Indeterminate ONLY if the statement was transmitted AND non-readonly:
        //  - !sent (checkout failed, never transmitted) → known did-not-apply → Retryable.
        //  - sent & readonly=false → a possibly-applied write, fate UNKNOWN → §19.3 Indeterminate.
        //  - sent & readonly=true  → a read that observed no result → Retryable (client policy).
        PoolError::ConnectionLost => {
            if sent && !readonly {
                payload(
                    errc::WRITE_UNCONFIRMED,
                    errc::WRITE_UNCONFIRMED_BRANCH,
                    "connection lost during an in-flight non-readonly statement; the write may or \
                     may not have applied (§19.3 indeterminate — the engine never retries; retry \
                     is client policy)",
                )
            } else {
                payload(
                    errc::CONNECTION_LOST,
                    errc::CONNECTION_LOST_BRANCH,
                    "connection lost with a known-fate outcome (statement not transmitted, or a \
                     readonly read) — retryable; the engine never retries",
                )
            }
        }
        PoolError::Timeout => payload(
            errc::POOL_TIMEOUT,
            errc::POOL_TIMEOUT_BRANCH,
            "timed out waiting for a pooled connection",
        ),
        PoolError::Unsupported(m) => ErrorPayload {
            code: errc::UNSUPPORTED,
            branch: errc::UNSUPPORTED_BRANCH,
            sqlstate: None,
            errno: None,
            message: m,
            detail: None,
            retry_after_ms: None,
        },
        // No dedicated wire code for Closed/Backend yet — a generic NonRetryable Protocol, matching
        // `PoolError::errc()`'s own fallback.
        PoolError::Closed => payload(errc::PROTOCOL, errc::PROTOCOL_BRANCH, "pool is closed"),
        PoolError::Backend(m) => ErrorPayload {
            code: errc::PROTOCOL,
            branch: errc::PROTOCOL_BRANCH,
            sqlstate: None,
            errno: None,
            message: m,
            detail: None,
            retry_after_ms: None,
        },
    }
}

fn payload(code: u16, branch: u8, message: &str) -> ErrorPayload {
    ErrorPayload {
        code,
        branch,
        sqlstate: None,
        errno: None,
        message: message.to_string(),
        detail: None,
        retry_after_ms: None,
    }
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

fn protocol(message: impl Into<String>) -> ErrorPayload {
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

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::consts::{branch, tag};
    use ferro_proto::messages::Outcome;
    use ferro_proto::messages::sql::ColMeta;
    use ferro_proto::value::Value;

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

    /// The DETERMINISTIC proof of §19.3, across BOTH the `sent` and `readonly` axes. Indeterminate
    /// requires `sent && !readonly`; nothing else (and no SQL read/write inference):
    ///
    /// - a TRANSMITTED (`sent=true`) non-readonly loss → `WriteUnconfirmed{Indeterminate}` (fate unknown);
    /// - a transmitted readonly loss → `ConnectionLost{Retryable}`;
    /// - a NOT-transmitted loss (`sent=false` — a checkout-time connect failure, the §19 DB-bounce)
    ///   → `ConnectionLost{Retryable}` EVEN on a write, because the statement provably never ran.
    #[test]
    fn connection_lost_indeterminate_only_when_sent_and_write() {
        // sent + write → the ONE Indeterminate case.
        let sent_write = pool_error_to_payload(PoolError::ConnectionLost, false, true);
        assert_eq!(sent_write.code, errc::WRITE_UNCONFIRMED);
        assert_eq!(sent_write.branch, branch::INDETERMINATE);

        // sent + readonly → Retryable.
        let sent_read = pool_error_to_payload(PoolError::ConnectionLost, true, true);
        assert_eq!(sent_read.code, errc::CONNECTION_LOST);
        assert_eq!(sent_read.branch, branch::RETRYABLE);

        // NOT sent (checkout failure) + write → Retryable, NOT Indeterminate (the T3-review fix:
        // a write that provably never left the client is known-fate, not fate-unknown).
        let unsent_write = pool_error_to_payload(PoolError::ConnectionLost, false, false);
        assert_eq!(
            unsent_write.code,
            errc::CONNECTION_LOST,
            "a checkout-time (never-transmitted) loss must be known-fate Retryable, not Indeterminate"
        );
        assert_eq!(unsent_write.branch, branch::RETRYABLE);
        assert_ne!(unsent_write.code, errc::WRITE_UNCONFIRMED);
        assert_ne!(unsent_write.branch, branch::INDETERMINATE);

        // NOT sent + readonly → Retryable.
        let unsent_read = pool_error_to_payload(PoolError::ConnectionLost, true, false);
        assert_eq!(unsent_read.code, errc::CONNECTION_LOST);
        assert_eq!(unsent_read.branch, branch::RETRYABLE);
    }

    /// A known-fate `PoolError::Sql` passes through VERBATIM regardless of `readonly` — the
    /// Indeterminate override must NEVER touch it. This is what keeps a bind pre-validation error
    /// (COMMIT 1: `Sql{Unsupported}`) from being reclassified Indeterminate on a write.
    #[test]
    fn sql_error_passes_through_verbatim_ignoring_readonly() {
        let sql = PoolError::Sql {
            code: errc::SYNTAX,
            branch: branch::NON_RETRYABLE,
            sqlstate: Some("42601".to_string()),
            message: "syntax error".to_string(),
        };
        // Verbatim regardless of readonly AND sent — a known-fate Sql is never Indeterminate.
        for readonly in [true, false] {
            for sent in [true, false] {
                let ep = pool_error_to_payload(sql.clone(), readonly, sent);
                assert_eq!(ep.code, errc::SYNTAX);
                assert_eq!(ep.branch, branch::NON_RETRYABLE);
                assert_eq!(ep.sqlstate.as_deref(), Some("42601"));
            }
        }

        // The exact COMMIT-1 bind-error shape (Sql{Unsupported}) on a WRITE (readonly=false), even
        // if it were reported at the sent=true site, stays Unsupported — NOT WriteUnconfirmed.
        let bind = PoolError::Sql {
            code: errc::UNSUPPORTED,
            branch: errc::UNSUPPORTED_BRANCH,
            sqlstate: None,
            message: "parameter 0 type mismatch".to_string(),
        };
        let ep = pool_error_to_payload(bind, false, true);
        assert_eq!(ep.code, errc::UNSUPPORTED);
        assert_ne!(ep.code, errc::WRITE_UNCONFIRMED);
        assert_ne!(ep.branch, branch::INDETERMINATE);
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
        };
        let body = build_terminal_body(result, FETCH_NONE, 0, 0).expect("fits");
        let ok = ExecOk::decode(&body).expect("decode ExecOk");
        assert!(ok.rows.is_empty(), "fetch=none drops the buffered rows");
        assert_eq!(ok.affected, 2);
        assert_eq!(ok.cols.len(), 1, "cols are kept even for fetch=none");
        assert_eq!(ok.stats.rows, 0);
    }
}
