//! Service `QUEUE` (= 7): Ferro Queue's request handler (SPEC §24.4; M7-G1a).
//!
//! The service rides the chassis exactly as HTTP does: a new service id routed to the request
//! lifecycle by `dispatch`, decoded here, ending in exactly ONE terminal declared on the request's
//! `Responder` (charter rule 4). Nothing queue-specific lives in the session layer.
//!
//! **What G1a serves, in order** — every step before the first checkout is a known non-execution
//! (SPEC §24.4: "Refusals … are declared before any checkout"):
//!
//! 1. decode the verb's message; a malformed one is `Protocol` (a wire fault: arity, type, or a shape
//!    bound such as a 1 025-byte token);
//! 2. interpret `common.traceparent` once (`ExecRequest` field 9's rule: dropped and counted, never
//!    refused);
//! 3. resolve the store — unknown, refused at configuration, or the queue unconfigured is
//!    `Unsupported`, as an unknown pool is;
//! 4. the per-request refusals of `ferro_queue::checks` (`Unsupported`), and the store KIND's decode
//!    of `job_id` and `token` (`InvalidHandle`, SPEC §24.3 prerequisite (c));
//! 5. a `tx_id` on a RESERVE: refused for good (§24.5 — a wait would hold the pin, and a rollback
//!    would void a delivered token);
//! 6. a MySQL/MariaDB store is refused until slice G6;
//! 7. **first use per `boot_epoch`:** the version gate against the pool's existing version probe
//!    (the wait bounded by the request's own deadline and CANCEL), then shape verification — ONE
//!    catalog read (`to_regclass` on the quoted identifier, so `search_path` is honoured; the
//!    relation's kind; its columns by oid) — the FIRST statement, a read, through the guarded
//!    `Checkout::query` with the request's own `timeout_ms` and CANCEL, classified by the shared fate
//!    matrix as a read (never `Indeterminate`). A definitive verdict is cached for the process — one
//!    process is one `boot_epoch` — except a gate refusal (reused for the version probe's TTL) and an
//!    absent table (reused for [`ABSENT_RECHECK`]) (SPEC §24.3 amendment);
//! 8. **the verb itself (M7-G1b), autocommit on PostgreSQL:** ONE checkout — declared read-only for
//!    SIZE alone — and the verb's statement(s), each built by `ferro_queue::pg` (the closed,
//!    mutation-tested builder set D22's mitigation names) and run through the guarded, interruptible
//!    [`run_autocommit_exec`] with what is left of the request's ONE deadline, then decoded by the same
//!    module into the verb's success terminal or its known-fate refusal (`LeaseLost`). A failure is
//!    classified by the shared fate matrix with SPEC §24.6's `OpContext`: `readonly` for SIZE only
//!    (RESERVE writes `attempts` and a lease), `sent` honest per statement, `in_tx: false`. The engine
//!    never re-sends anything (charter rule 3, §24.2 I4).
//!
//! **A `tx_id` on any verb but RESERVE (M7-G2, §24.5)** inserts two steps between 6 and 7: resolve
//! the transaction exactly as a tx-scoped EXEC does ([`resolve_active`]: a missing or foreign id is
//! `TxNotFound`, the owner's tombstoned one `TxDeadline`), then compare the store's pool with the
//! transaction's — a mismatch is `PoolMismatch`, before anything is sent, so the transaction is
//! untouched (D22 (b): a `sql` store composes with a transaction only in that transaction's own
//! pool). Step 7 follows, and step 8 is replaced: the verb runs on the transaction's PINNED connection
//! as ONE [`TxCommand::Queue`] (see [`serve_tx`]).
//!
//! RESERVE serves its queues in the given order and answers from the FIRST queue that yields any job
//! (one statement per queue tried, on the one checkout), so every job in a reply comes from one queue
//! and a failure on queue *i* means queues before it reserved nothing — the failure's fate is that one
//! statement's. Its `LIMIT` is `max_jobs` clamped so the reply fits one frame
//! ([`checks::reserve_limit`]). **Not in G1b:** the unreserve of §24.8 — a RESERVE whose terminal is
//! raced by session teardown leaves its lease to expire (a stock-equivalent phantom) until slice G3
//! builds unreserve.
//!
//! **Tx-scoped verbs (M7-G2, SPEC §24.5).** The verb's statements are built here, run by the TX
//! actor on the pinned connection, decoded by the actor (`TxVerb::decode`), and classified with
//! `in_tx: true`. Differences from autocommit, each §24's: an unmatched fence is ALWAYS `LeaseLost`
//! (R1 — no probe, no `gone`); a cancel, timeout or `57014` rolls the transaction back and tombstones
//! it (`TxDeadline{Retryable}`); a WRITE whose result is unreadable does the same rather than answer
//! `Indeterminate`, because inside a transaction the engine CAN make the effect's fate known — by
//! rolling back — and must not let it commit unseen; and a verb's wake hint fires only on COMMIT.
//!
//! Never logged or put in a terminal: a payload, a dedup key, a token or a DSN.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ferro_pool::backend::{PoolBackend, QueryResult};
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool};
use ferro_proto::consts::{ack_outcome, branch, errc, method_queue};
use ferro_proto::messages::{
    AckResponse, ClearResponse, EnqueueRequest, EnqueueResponse, ErrorPayload, ExtendResponse,
    FencedRequest, QueueCommon, QueueScopeRequest, QueueStats, ReleaseRequest, ReleaseResponse,
    ReserveRequest, ReserveResponse, ReservedJob, SizeResponse,
};
use ferro_queue::config::{QueueConfig, StoreConfig, StoreKind};
use ferro_queue::ident::TableName;
use ferro_queue::pg::{self as pgq, NoMatch};
use ferro_queue::shape::{self, PgRelation, ShapeError, Statement};
use ferro_queue::sql::{JobId, Token, Undecodable};
use ferro_queue::{PoolFamily, checks, version};
use tokio_util::sync::CancellationToken;

use crate::pools::{AnyPool, PoolRegistry};
use crate::services::queue_metrics::{HintSource, Outcome as MetricOutcome, QueueMetrics, UnreserveCause};
use crate::services::queue_waker::{Offer, Runner, WaitOutcome, Waker};
use crate::session::{Liveness, SessionInfo};
use futures::FutureExt;
use crate::services::fate::{self, OpContext};
use crate::services::sql::{
    actor_gone_terminal, cancelled_before_dispatch, protocol, resolve_active, run_autocommit_exec,
    sleep_until_opt, unsupported,
};
use crate::session::SessionId;
use crate::session::codec::InFrame;
use crate::session::responder::Responder;
use crate::tx::{QueueReply, TxCommand, TxRegistry};

/// One decoded QUEUE request.
#[derive(Debug)]
enum QueueRequest {
    Enqueue(EnqueueRequest),
    Reserve(ReserveRequest),
    Ack(FencedRequest),
    Release(ReleaseRequest),
    Extend(FencedRequest),
    Size(QueueScopeRequest),
    Clear(QueueScopeRequest),
}

impl QueueRequest {
    /// `None` for a method id `[methods.queue]` does not define (unreachable through `dispatch`, which
    /// routes only registered ids here).
    fn decode(method: u16, payload: &[u8]) -> Option<Result<QueueRequest, String>> {
        let r = match method {
            method_queue::ENQUEUE => EnqueueRequest::decode(payload).map(QueueRequest::Enqueue),
            method_queue::RESERVE => ReserveRequest::decode(payload).map(QueueRequest::Reserve),
            method_queue::ACK => FencedRequest::decode(payload).map(QueueRequest::Ack),
            method_queue::RELEASE => ReleaseRequest::decode(payload).map(QueueRequest::Release),
            method_queue::EXTEND => FencedRequest::decode(payload).map(QueueRequest::Extend),
            method_queue::SIZE => QueueScopeRequest::decode(payload).map(QueueRequest::Size),
            method_queue::CLEAR => QueueScopeRequest::decode(payload).map(QueueRequest::Clear),
            _ => return None,
        };
        Some(r.map_err(|e| e.to_string()))
    }

    fn verb(&self) -> &'static str {
        match self {
            QueueRequest::Enqueue(_) => "ENQUEUE",
            QueueRequest::Reserve(_) => "RESERVE",
            QueueRequest::Ack(_) => "ACK",
            QueueRequest::Release(_) => "RELEASE",
            QueueRequest::Extend(_) => "EXTEND",
            QueueRequest::Size(_) => "SIZE",
            QueueRequest::Clear(_) => "CLEAR",
        }
    }

    /// `ops_total`'s `op` label index (`queue_metrics::OPS`).
    fn op_index(&self) -> usize {
        match self {
            QueueRequest::Enqueue(_) => 0,
            QueueRequest::Reserve(_) => 1,
            QueueRequest::Ack(_) => 2,
            QueueRequest::Release(_) => 3,
            QueueRequest::Extend(_) => 4,
            QueueRequest::Size(_) => 5,
            QueueRequest::Clear(_) => 6,
        }
    }

    fn op_label(&self) -> &'static str {
        crate::services::queue_metrics::OPS[self.op_index()]
    }

    /// The queue a request names, for its metric label before any statement: an ENQUEUE's first job's,
    /// a RESERVE's first queue, SIZE's and CLEAR's. The fenced verbs name none — the queue is in the
    /// row — so ACK and EXTEND are labelled `_other`, and RELEASE by the queue its statement returns.
    fn queue_hint(&self) -> Option<&str> {
        match self {
            QueueRequest::Enqueue(r) => r.jobs.first().map(|j| j.queue.as_str()),
            QueueRequest::Reserve(r) => r.queues.first().map(String::as_str),
            QueueRequest::Size(r) | QueueRequest::Clear(r) => Some(&r.queue),
            QueueRequest::Ack(_) | QueueRequest::Release(_) | QueueRequest::Extend(_) => None,
        }
    }

    fn store(&self) -> &str {
        match self {
            QueueRequest::Enqueue(r) => &r.store,
            QueueRequest::Reserve(r) => &r.store,
            QueueRequest::Ack(r) | QueueRequest::Extend(r) => &r.store,
            QueueRequest::Release(r) => &r.store,
            QueueRequest::Size(r) | QueueRequest::Clear(r) => &r.store,
        }
    }

    fn common(&self) -> &QueueCommon {
        match self {
            QueueRequest::Enqueue(r) => &r.common,
            QueueRequest::Reserve(r) => &r.common,
            QueueRequest::Ack(r) | QueueRequest::Extend(r) => &r.common,
            QueueRequest::Release(r) => &r.common,
            QueueRequest::Size(r) | QueueRequest::Clear(r) => &r.common,
        }
    }
}

/// How long an ABSENT table's verdict is reused before the catalog is read again (M7-G1a review M2).
/// Short enough to honour "start `ferrod`, then migrate" — a table created now is served within two
/// seconds — and long enough that a missing table costs one catalog read per store per two seconds,
/// not one checkout per request while every other request on the store waits for the verdict lock.
pub const ABSENT_RECHECK: Duration = Duration::from_secs(2);

/// A store's first-use verdict (SPEC §24.3), cached for the process — one process is one
/// `boot_epoch`, and `ferrod` has no configuration reload in v1 — with two timed exceptions.
#[derive(Debug, Clone)]
enum Verdict {
    Unverified,
    /// Verified, and the relation it RESOLVED — schema-qualified — which every verb's statement
    /// names (review F5: never the configured bare name a session's `search_path` could steer).
    Verified(TableName),
    /// A table that exists in the wrong form: every verb on the store answers `Unsupported` with
    /// this message until the next restart (§24.3).
    Refused(String),
    /// The version gate's refusal, reused for as long as the server version it was decided on is
    /// trusted (the probe's TTL), so a backend upgraded under a running `ferrod` is noticed (review L4).
    GateRefused {
        message: String,
        at: Instant,
    },
    /// No such table: reused for [`QueueStores::absent_recheck`], then the catalog is read again.
    Absent {
        message: String,
        at: Instant,
    },
}

/// The configured stores plus each store's verification state.
pub struct QueueStores {
    config: Arc<QueueConfig>,
    /// One per ENABLED store. A `tokio` mutex, held across the verification's awaits, so concurrent
    /// first requests verify ONCE and the rest read the verdict.
    verdicts: HashMap<String, tokio::sync::Mutex<Verdict>>,
    /// [`ABSENT_RECHECK`] in production; a test shortens it.
    absent_recheck: Duration,
    /// `None` in production (the registry's version TTL); a test shortens it.
    gate_recheck: Option<Duration>,
    /// Catalog reads ISSUED by shape verification — what makes "an absent table is not re-read on
    /// every request" a counted claim rather than a timed one.
    verifications: AtomicU64,
    /// When each store last warned that its pool's version is unknown (review L3).
    unknown_version_warned: std::sync::Mutex<HashMap<String, Instant>>,
    /// One waker per ENABLED store (M7-G3, SPEC §24.8): its wait sets, its poll schedule, and the
    /// sink every wake hint is routed to. Built here, off-runtime; it spawns nothing until a waiting
    /// RESERVE first registers.
    wakers: HashMap<String, Arc<Waker>>,
}

impl QueueStores {
    pub fn new(config: Arc<QueueConfig>) -> QueueStores {
        let verdicts = config
            .entries()
            .filter_map(|(name, _)| config.store(name).map(|s| s.name.clone()))
            .map(|name| (name, tokio::sync::Mutex::new(Verdict::Unverified)))
            .collect();
        let wakers = config
            .entries()
            .filter_map(|(name, _)| config.store(name))
            .map(|s| {
                let metrics = Arc::new(QueueMetrics::new(&s.name, &s.labelled_queues));
                (s.name.clone(), Waker::new(s, metrics))
            })
            .collect();
        QueueStores {
            config,
            verdicts,
            absent_recheck: ABSENT_RECHECK,
            gate_recheck: None,
            verifications: AtomicU64::new(0),
            unknown_version_warned: std::sync::Mutex::new(HashMap::new()),
            wakers,
        }
    }

    /// How many AFTER-COMMIT wake hints have fired since boot, over every store (M7-G2's counter,
    /// kept: "fired only on a successful COMMIT" stays a counted claim). The hints of every source
    /// are `ferro_queue_wake_hints_total{source}` (§24.9).
    pub fn wake_hints(&self) -> u64 {
        self.wakers
            .values()
            .map(|w| w.metrics().hints(HintSource::AfterCommit))
            .sum()
    }

    /// The store's waker (an enabled store always has one).
    pub fn waker(&self, store: &str) -> Option<&Arc<Waker>> {
        self.wakers.get(store)
    }

    /// Every enabled store's metrics, sorted by store name (for the §13 exposition).
    pub fn metrics(&self) -> Vec<&QueueMetrics> {
        let mut names: Vec<&String> = self.wakers.keys().collect();
        names.sort();
        names
            .into_iter()
            .filter_map(|n| self.wakers.get(n).map(|w| &**w.metrics()))
            .collect()
    }

    /// Fire one after-commit wake hint for `(store, queue)` (SPEC §24.5 step 4, §24.8 trigger 1),
    /// routed to the store's waker since M7-G3. A hint is never correctness (§24.5).
    fn wake(&self, store: &str, queue: &str) {
        if let Some(w) = self.wakers.get(store) {
            w.hint(queue, HintSource::AfterCommit);
        }
    }

    pub fn config(&self) -> &QueueConfig {
        &self.config
    }

    /// How many shape-verification catalog reads have been issued since boot.
    pub fn verifications(&self) -> u64 {
        self.verifications.load(Ordering::Relaxed)
    }

    /// Whether to warn now that `store`'s pool version is unknown: once per `window` (the probe's
    /// back-off), so a dead backend is named in the log without one line per request.
    fn should_warn_unknown_version(&self, store: &str, window: Duration) -> bool {
        let mut warned = self
            .unknown_version_warned
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        warn_once_per(
            warned.entry(store.to_string()).or_insert_with(far_past),
            window,
        )
    }
}

/// An `Instant` old enough that the first warning always fires.
fn far_past() -> Instant {
    Instant::now()
        .checked_sub(Duration::from_secs(86_400 * 365))
        .unwrap_or_else(Instant::now)
}

/// The rate limiter, pure: `true` (and the clock reset) iff `window` has passed since `last`.
fn warn_once_per(last: &mut Instant, window: Duration) -> bool {
    if last.elapsed() >= window {
        *last = Instant::now();
        true
    } else {
        false
    }
}

fn invalid_handle(store: &str, e: Undecodable) -> ErrorPayload {
    ErrorPayload {
        code: errc::INVALID_HANDLE,
        branch: errc::INVALID_HANDLE_BRANCH,
        sqlstate: None,
        errno: None,
        message: format!("queue store {store}: {e}; nothing was sent"),
        detail: None,
        retry_after_ms: None,
    }
}

/// The `sql` kind's decode of a fenced verb's handles (SPEC §24.3), before any statement: once among
/// the pre-checkout refusals, and again where the verb's statement consumes the decoded values.
fn decode_handles(
    store: &StoreConfig,
    job_id: &[u8],
    token: &[u8],
) -> Result<(JobId, Token), ErrorPayload> {
    match store.kind {
        StoreKind::Sql => {
            let id = JobId::decode(job_id).map_err(|e| invalid_handle(&store.name, e))?;
            let token = Token::decode(token).map_err(|e| invalid_handle(&store.name, e))?;
            Ok((id, token))
        }
    }
}

/// The engine's wall clock, Unix seconds — ONLY for the pre-send `delay_s` overflow check
/// ([`checks::delay`]). Every rule a verb applies to rows reads the DATABASE's clock (SPEC §24.3).
fn engine_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Steps 4–6 of the module doc: every refusal made before a checkout. `now` is the engine's wall
/// clock ([`engine_now`]).
fn refuse_before_checkout(
    req: &QueueRequest,
    store: &StoreConfig,
    now: i64,
) -> Result<(), ErrorPayload> {
    let refusal = |r: checks::Refusal| unsupported(format!("queue store {}: {r}", store.name));
    // Permanent, so FIRST for RESERVE (review L7): a tx-scoped RESERVE that also has `wait_ms > 0`
    // must be told the call can never work, not to send `wait_ms = 0`.
    if let QueueRequest::Reserve(_) = req
        && req.common().tx_id.is_some()
    {
        // SPEC §24.5: a wait would hold the pin, and a rollback would void a delivered token.
        return Err(unsupported(
            "RESERVE inside a transaction is refused (SPEC §24.5): send it without a tx_id",
        ));
    }
    match req {
        QueueRequest::Enqueue(r) => checks::enqueue(r, store, now).map_err(refusal)?,
        QueueRequest::Reserve(r) => checks::reserve(r).map_err(refusal)?,
        QueueRequest::Ack(r) | QueueRequest::Extend(r) => {
            decode_handles(store, &r.job_id, &r.token)?;
        }
        QueueRequest::Release(r) => {
            decode_handles(store, &r.job_id, &r.token)?;
            checks::release(r, store, now).map_err(refusal)?;
        }
        QueueRequest::Size(r) | QueueRequest::Clear(r) => {
            checks::queue_name(&r.queue).map_err(refusal)?
        }
    }
    if store.family == PoolFamily::Mysql {
        return Err(unsupported(format!(
            "queue store {}: MySQL/MariaDB stores are not served before slice G6 (SPEC §24.14)",
            store.name
        )));
    }
    Ok(())
}

/// Serve one QUEUE frame and declare its ONE terminal.
#[allow(clippy::too_many_arguments)]
pub async fn handle(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
    tx_registry: &TxRegistry,
    session_id: SessionId,
    info: SessionInfo,
    cancel: CancellationToken,
) {
    let method = frame.header.method;
    let req = match QueueRequest::decode(method, &frame.payload) {
        None => {
            responder.end_error(unsupported(format!("QUEUE method {method} does not exist")));
            return;
        }
        Some(Err(e)) => {
            responder.end_error(protocol(format!("malformed QUEUE request: {e}")));
            return;
        }
        Some(Ok(r)) => r,
    };
    drop(frame);
    // `ExecRequest` field 9's rule: interpreted once, a malformed value dropped and counted. Its
    // consumer is the request's SPEC §13 span (M7-G3, §24.9), opened HERE so a refused request is a
    // span too: one QUEUE request, one END, one span.
    let trace = crate::trace::from_request(req.common().traceparent.as_deref());
    let responder = responder.with_span(registry.tracer().and_then(|t| {
        t.begin(trace, || {
            vec![(
                "ferro.queue.op",
                crate::otlp::AttrValue::Str(req.op_label().to_string()),
            )]
        })
    }));
    let mut reply = Reply::bare(responder);

    let Some(stores) = registry.queue() else {
        reply.error(unsupported(
            "Ferro Queue is not configured on this daemon (FERRO_QUEUE_STORES is unset)",
        ));
        return;
    };
    let Some(store) = stores.config.store(req.store()) else {
        // One answer for an unknown store and one refused at configuration (the store's name is the
        // client's own; the reason is in the daemon's log).
        reply.error(unsupported(
            "unknown queue store (or one refused at configuration; see the ferrod log)",
        ));
        return;
    };
    let Some(waker) = stores.waker(&store.name).cloned() else {
        reply.error(unsupported("unknown queue store"));
        return;
    };
    reply.observe(
        store,
        waker.metrics(),
        req.op_index(),
        req.queue_hint(),
        req.common().tx_id.is_some(),
        registry.slow_log(),
    );
    if let Err(ep) = refuse_before_checkout(&req, store, engine_now()) {
        reply.error(ep);
        return;
    }
    // ONE deadline for the whole request — the first-use verification, the checkout and every
    // statement — as on EXEC (M3-D1c review F1).
    let deadline = deadline_of(req.common().timeout_ms);
    if let Some(tx_id) = req.common().tx_id {
        let tx = TxScope {
            tx_registry,
            session_id,
            tx_id,
        };
        serve_tx(stores, store, registry, tx, req, deadline, cancel, reply).await;
        return;
    }
    let table = match ensure_verified(stores, store, registry, deadline, &cancel).await {
        Ok(t) => t,
        Err(ep) => {
            reply.error(ep);
            return;
        }
    };
    match registry.get(&store.pool) {
        Some(AnyPool::Pg(pool)) => {
            // The waker's statements run on the same verified relation (M7-G3), bound once.
            waker.bind(&table, || pg_runner(pool.clone()));
            let ctx = AutocommitCtx {
                store,
                table: &table,
                deadline,
                cancel: &cancel,
                info: &info,
                waker: &waker,
            };
            match req {
                // SPEC §24.8: a WAITING RESERVE parks on the waker. A daemon that is draining serves
                // it as a non-waiting one (one sweep, then its answer): a drain never parks anyone.
                QueueRequest::Reserve(r)
                    if checks::wait_clamp(r.wait_ms, store.max_wait_ms) > 0
                        && !info.drain.is_draining() =>
                {
                    serve_waiting(&ctx, &r, reply).await
                }
                req => serve_pg(pool, req, &ctx, reply).await,
            }
        }
        // Unreachable: verification passed, so the pool is PostgreSQL (`ensure_verified` answers
        // every other family). Answered rather than asserted.
        Some(_) | None => reply.error(unsupported(format!(
            "queue store {}: its pool cannot serve QUEUE verbs in this build",
            store.name
        ))),
    }
}

/// The autocommit path's per-request context (M7-G3).
struct AutocommitCtx<'a> {
    store: &'a StoreConfig,
    table: &'a TableName,
    deadline: Option<tokio::time::Instant>,
    cancel: &'a CancellationToken,
    info: &'a SessionInfo,
    waker: &'a Arc<Waker>,
}

/// The waker's statement runner on a PostgreSQL pool (M7-G3, SPEC §24.8): its own checkout per
/// statement, the guarded, interruptible [`run_autocommit_exec`], everything — the checkout wait
/// included — bounded by the store's `WAKER_STMT_TIMEOUT_MS`. No request's CANCEL reaches it: a waker
/// statement serves every waiter in its batch.
fn pg_runner<B: PoolBackend + 'static>(pool: Pool<B>) -> Runner {
    Arc::new(move |stmt: Statement, timeout: Duration| {
        let pool = pool.clone();
        async move {
            let deadline = tokio::time::Instant::now() + timeout;
            let co = tokio::select! {
                biased;
                r = pool.checkout() => r,
                () = tokio::time::sleep_until(deadline) => Err(PoolError::Timeout),
            };
            let mut co = co.map_err(|e| (e, false))?;
            let queue_us = co.stats().queue_us;
            let left = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis();
            if left == 0 {
                return Err((PoolError::Timeout, false));
            }
            let never = CancellationToken::new();
            let (r, exec_us) = run_autocommit_exec(
                &mut co,
                &stmt.sql,
                &stmt.params,
                Some(u32::try_from(left).unwrap_or(u32::MAX)),
                &never,
            )
            .await;
            r.map(|qr| (qr, queue_us, exec_us)).map_err(|e| (e, true))
        }
        .boxed()
    })
}

/// A request's statement CANCEL that a client CANCEL fires and session TEARDOWN does not (M7-G3).
/// Teardown fires the same per-request token (`cancel_all`), but a RESERVE whose session is going
/// away must not be interrupted: SPEC §24.4 has its jobs UNRESERVED, and only a statement that
/// finishes says which jobs those are (an interrupted one is `Indeterminate`, a phantom attempt).
/// After teardown the statement is still bounded, by `bound` (the store's waker statement timeout).
fn client_cancel(
    cancel: &CancellationToken,
    liveness: &Liveness,
    bound: Duration,
) -> (CancellationToken, AbortOnDrop) {
    let child = CancellationToken::new();
    let (c, l, ch) = (cancel.clone(), liveness.clone(), child.clone());
    let task = tokio::spawn(async move {
        c.cancelled().await;
        if !l.is_live() {
            tokio::time::sleep(bound).await;
        }
        ch.cancel();
    });
    (child, AbortOnDrop(task))
}

/// Aborts its task when dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A waiting RESERVE (SPEC §24.8, M7-G3): register on the store's waker, wait for its outcome within
/// the wait bound, and declare the terminal. Holds no connection while it waits.
async fn serve_waiting(ctx: &AutocommitCtx<'_>, r: &ReserveRequest, reply: Reply) {
    let received = tokio::time::Instant::now();
    let wait = Duration::from_millis(u64::from(checks::wait_clamp(
        r.wait_ms,
        ctx.store.max_wait_ms,
    )));
    let grace = Duration::from_millis(u64::from(ferro_proto::consts::QUEUE_WAIT_GRACE_MS));
    // §24.8's normative wait bound — and the request's own deadline, when it set one.
    let mut wait_end = received + wait;
    let mut hard_end = wait_end + grace;
    if let Some(d) = ctx.deadline {
        wait_end = wait_end.min(d);
        hard_end = hard_end.min(d);
    }
    let clamp = checks::reserve_limit(r.max_jobs, ctx.store.max_payload_bytes);
    let Some(guard) = ctx.waker.register(&r.queues, clamp, wait_end) else {
        reply.error(unsupported("queue store: its waker is gone"));
        return;
    };
    let outcome = guard
        .wait(hard_end, ctx.cancel, &ctx.info.drain, &ctx.info.liveness)
        .await;
    let stats = guard.stats();
    drop(guard);
    ctx.waker
        .metrics()
        .observe_wait_us(u64::try_from(received.elapsed().as_micros()).unwrap_or(u64::MAX));
    match outcome {
        WaitOutcome::Offer(Offer::Jobs(jobs)) => hand_off_jobs(ctx, reply, jobs, stats),
        WaitOutcome::Offer(Offer::Failed(ep)) => reply.error(ep),
        WaitOutcome::Offer(Offer::Empty) | WaitOutcome::Expired | WaitOutcome::Drained => {
            reply.ok(VerbOk::Reserve(Vec::new()), stats)
        }
        WaitOutcome::Cancelled | WaitOutcome::Teardown => reply.cancelled(),
    }
}

/// SPEC §24.4: "a job is DELIVERED iff the terminal carrying it is handed to a live session's
/// writer". The declaration happens under the session's liveness lock; a session whose teardown has
/// begun gets nothing, and the jobs are UNRESERVED (cause `teardown`) — deliver xor unreserve.
fn hand_off_jobs(ctx: &AutocommitCtx<'_>, reply: Reply, jobs: Vec<pgq::Reserved>, stats: QueueStats) {
    if let Err((reply, jobs)) = ctx
        .info
        .liveness
        .hand_off((reply, jobs), |(r, j)| r.ok(VerbOk::Reserve(j), stats))
    {
        ctx.waker.unreserve(jobs, UnreserveCause::Teardown);
        reply.cancelled();
    }
}

/// The request's absolute deadline from its `timeout_ms`.
fn deadline_of(timeout_ms: Option<u32>) -> Option<tokio::time::Instant> {
    timeout_ms.map(|ms| tokio::time::Instant::now() + Duration::from_millis(u64::from(ms)))
}

/// SPEC §24.6's `OpContext` for an autocommit verb: `readonly` for SIZE ONLY — every other verb
/// writes, RESERVE included (`attempts` and a lease) — and never `in_tx` here (a tx-scoped verb is [`tx_verb_context`]'s).
fn verb_context(req: &QueueRequest, sent: bool) -> OpContext {
    OpContext {
        readonly: matches!(req, QueueRequest::Size(_)),
        sent,
        in_tx: false,
    }
}

/// A statement that RAN but whose rows do not have the shape its builder produces (only a table
/// altered after verification, or a defect, does that). For a write the effect is real but cannot be
/// reported, so the answer is `Indeterminate` — never a known-fate error that would tell the client
/// the verb did nothing. For SIZE (a read) it is a plain refusal.
fn malformed(req: &QueueRequest, store: &StoreConfig) -> ErrorPayload {
    let message = format!(
        "queue store {}: the QUEUE {} statement ran but its result did not have the expected shape \
         (was the table altered after it was verified? ferrod re-verifies at restart)",
        store.name,
        req.verb()
    );
    if verb_context(req, true).readonly {
        unsupported(message)
    } else {
        ErrorPayload {
            code: errc::WRITE_UNCONFIRMED,
            branch: errc::WRITE_UNCONFIRMED_BRANCH,
            sqlstate: None,
            errno: None,
            message: format!("{message}; its effect is unconfirmed"),
            detail: None,
            retry_after_ms: None,
        }
    }
}

/// `LeaseLost` (SPEC §24.4): the verb did nothing. In a transaction (R1) it also says the
/// transaction is still open — no SQL error occurred — and must be rolled back (§24.5, §24.6).
fn lease_lost(store: &str, verb: &str, in_tx: bool) -> ErrorPayload {
    let tx = if in_tx {
        "; the transaction is still open: roll it back (SPEC §24.5)"
    } else {
        ""
    };
    ErrorPayload {
        code: errc::LEASE_LOST,
        branch: errc::LEASE_LOST_BRANCH,
        sqlstate: None,
        errno: None,
        message: format!(
            "queue store {store}: QUEUE {verb} did nothing: the token names no current reservation \
             (another holder has the job, or it was reserved again after this lease expired){tx}"
        ),
        detail: None,
        retry_after_ms: None,
    }
}

/// One verb statement on `co`: what is left of the deadline (none left → answered UNSENT, never
/// dispatched with no time), then the guarded, interruptible [`run_autocommit_exec`]. A CANCEL that
/// arrived before this statement is answered UNSENT too, so a RESERVE serving its second queue does
/// not send it after the client cancelled. `exec_us` accumulates.
async fn run_verb_statement<B: PoolBackend>(
    co: &mut Checkout<B>,
    stmt: &Statement,
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
    ctx: impl Fn(bool) -> OpContext,
    exec_us: &mut u64,
) -> Result<QueryResult, ErrorPayload> {
    if cancel.is_cancelled() {
        return Err(fate::classify_fate(cancelled_before_dispatch(), ctx(false)));
    }
    let timeout_ms = match deadline {
        None => None,
        Some(d) => {
            let left = d
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis();
            if left == 0 {
                return Err(fate::classify_fate(PoolError::Timeout, ctx(false)));
            }
            Some(u32::try_from(left).unwrap_or(u32::MAX))
        }
    };
    let (result, us) = run_autocommit_exec(co, &stmt.sql, &stmt.params, timeout_ms, cancel).await;
    *exec_us += us;
    result.map_err(|e| fate::classify_fate(e, ctx(true)))
}

/// The verb, autocommit, on a verified PostgreSQL store (step 8 of the module doc). A RESERVE here
/// does not wait (its `wait_ms` is 0, clamped to 0, or the daemon is draining).
async fn serve_pg<B: PoolBackend>(
    pool: &Pool<B>,
    req: QueueRequest,
    ctx: &AutocommitCtx<'_>,
    reply: Reply,
) {
    let (store, cancel, deadline) = (ctx.store, ctx.cancel, ctx.deadline);
    let readonly = verb_context(&req, false).readonly;
    let checkout = pool.checkout_declared(readonly);
    tokio::pin!(checkout);
    let checked_out = tokio::select! {
        biased;
        r = &mut checkout => r,
        () = sleep_until_opt(deadline) => Err(PoolError::Timeout),
        () = cancel.cancelled() => Err(cancelled_before_dispatch()),
    };
    let mut co = match checked_out {
        Ok(co) => co,
        Err(e) => {
            // No connection: nothing was sent.
            reply.error(fate::classify_fate(e, verb_context(&req, false)));
            return;
        }
    };
    let queue_us = co.stats().queue_us;
    let mut exec_us = 0u64;
    // A RESERVE's statement is interrupted by a client CANCEL (G1b's rule) but NOT by its session's
    // teardown, which instead lets it finish and unreserves what it took (M7-G3, §24.4).
    let reserve_cancel = matches!(req, QueueRequest::Reserve(_)).then(|| {
        client_cancel(
            cancel,
            &ctx.info.liveness,
            Duration::from_millis(u64::from(store.waker_stmt_timeout_ms)),
        )
    });
    let stmt_cancel = reserve_cancel.as_ref().map_or(cancel, |(c, _)| c);
    let outcome = run_pg_verb(
        &mut co,
        &req,
        store,
        ctx.table,
        deadline,
        stmt_cancel,
        &mut exec_us,
    )
    .await;
    drop(reserve_cancel);
    // Release the connection before framing the terminal (as EXEC does): held only for the verb.
    drop(co);
    let stats = QueueStats { queue_us, exec_us };
    match outcome {
        Ok(VerbOk::Reserve(jobs)) if !jobs.is_empty() => hand_off_jobs(ctx, reply, jobs, stats),
        Ok(ok) => {
            // SPEC §24.8 trigger 1, the AUTOCOMMIT hints (M7-G3): an ENQUEUE wakes each of its
            // queues, a RELEASE with `delay_s = 0` the queue its row is in.
            for q in autocommit_hints(&req, &ok) {
                ctx.waker.hint(&q, HintSource::Autocommit);
            }
            reply.ok(ok, stats)
        }
        Err(ep) => {
            if matches!(req, QueueRequest::Reserve(_)) && ep.branch == branch::INDETERMINATE {
                ctx.waker.metrics().reserve_unconfirmed();
            }
            reply.error(ep)
        }
    }
}

/// The queues an applied AUTOCOMMIT verb wakes (§24.8 trigger 1): an ENQUEUE's distinct queues — any
/// delay, as the in-transaction rule G2 pinned — and a `delay_s = 0` RELEASE's row's queue.
fn autocommit_hints(req: &QueueRequest, ok: &VerbOk) -> Vec<String> {
    match (req, ok) {
        (QueueRequest::Enqueue(r), VerbOk::Enqueue(_)) => {
            let mut q: Vec<String> = r.jobs.iter().map(|j| j.queue.clone()).collect();
            q.sort();
            q.dedup();
            q
        }
        (QueueRequest::Release(r), VerbOk::Release(Some((_, queue)))) if r.delay_s == 0 => {
            vec![queue.clone()]
        }
        _ => Vec::new(),
    }
}

/// The ONE funnel a QUEUE request's terminal is declared through (M7-G3): every success, error and
/// cancel passes here, so SPEC §24.9's `ops_total`, the request's span and the slow log each see every
/// terminal exactly once. Before the store is known it only declares (no store, no series).
pub(crate) struct Reply {
    responder: Responder,
    obs: Option<Obs>,
}

struct Obs {
    metrics: Arc<QueueMetrics>,
    store: String,
    op: usize,
    label: usize,
    in_tx: bool,
    slow: crate::pools::SlowLogConfig,
}

impl Reply {
    fn bare(responder: Responder) -> Reply {
        Reply {
            responder,
            obs: None,
        }
    }

    /// The store is known: label the span, and count from here on.
    fn observe(
        &mut self,
        store: &StoreConfig,
        metrics: &Arc<QueueMetrics>,
        op: usize,
        queue: Option<&str>,
        in_tx: bool,
        slow: crate::pools::SlowLogConfig,
    ) {
        use crate::otlp::AttrValue::{Bool, Str};
        let label = metrics.label_of(queue);
        self.responder
            .push_span_attr(("ferro.queue.store", Str(store.name.clone())));
        self.responder
            .push_span_attr(("ferro.queue.queue", Str(metrics.label(label).to_string())));
        self.responder.push_span_attr(("ferro.in_tx", Bool(in_tx)));
        self.obs = Some(Obs {
            metrics: Arc::clone(metrics),
            store: store.name.clone(),
            op,
            label,
            in_tx,
            slow,
        });
    }

    fn error(self, ep: ErrorPayload) {
        if let Some(o) = &self.obs {
            let outcome = if ep.code == errc::LEASE_LOST {
                MetricOutcome::LeaseLost
            } else {
                MetricOutcome::Error
            };
            o.metrics.op(o.label, o.op, outcome);
        }
        self.responder.end_error(ep);
    }

    fn cancelled(self) {
        if let Some(o) = &self.obs {
            o.metrics.op(o.label, o.op, MetricOutcome::Error);
        }
        self.responder.end_cancelled();
    }

    fn ok(mut self, ok: VerbOk, stats: QueueStats) {
        use crate::otlp::AttrValue::Int;
        let (outcome, queue, jobs, attempts, affected) = match &ok {
            VerbOk::Enqueue(e) => (MetricOutcome::Ok, None, 0, None, u64::from(e.inserted)),
            VerbOk::Reserve(jobs) => match jobs.first() {
                Some(j) => (
                    MetricOutcome::Ok,
                    Some(j.queue.clone()),
                    jobs.len() as u64,
                    Some(j.attempts),
                    jobs.len() as u64,
                ),
                None => (MetricOutcome::Empty, None, 0, None, 0),
            },
            VerbOk::Ack { gone: true } => (MetricOutcome::Gone, None, 0, None, 0),
            VerbOk::Ack { gone: false } => (MetricOutcome::Ok, None, 0, None, 1),
            VerbOk::Release(Some((_, q))) => (MetricOutcome::Ok, Some(q.clone()), 0, None, 1),
            VerbOk::Release(None) => (MetricOutcome::Gone, None, 0, None, 0),
            VerbOk::Extend(_) => (MetricOutcome::Ok, None, 0, None, 1),
            VerbOk::Size(_) => (MetricOutcome::Ok, None, 0, None, 0),
            VerbOk::Clear(n) => (MetricOutcome::Ok, None, 0, None, *n),
        };
        let (is_enqueue, is_ack) = (
            matches!(ok, VerbOk::Enqueue(_)),
            matches!(ok, VerbOk::Ack { .. }),
        );
        let body = ok.encode(stats);
        if let Some(o) = &self.obs {
            let label = queue
                .as_deref()
                .map_or(o.label, |q| o.metrics.label_of(Some(q)));
            o.metrics.op(label, o.op, outcome);
            let us = stats.queue_us.saturating_add(stats.exec_us);
            if is_enqueue {
                o.metrics.enqueued(label, o.in_tx, affected);
                o.metrics.observe_enqueue_us(us);
            }
            if is_ack {
                o.metrics.observe_ack_us(us);
            }
            self.responder.record_exec(crate::otlp::ExecResult {
                rows_returned: jobs,
                rows_affected: affected,
                queue_us: stats.queue_us,
                exec_us: stats.exec_us,
                response_bytes: body.len() as u64,
            });
            self.responder.push_span_attr(("ferro.queue.jobs", Int(jobs)));
            if let Some(a) = attempts {
                self.responder
                    .push_span_attr(("ferro.queue.attempts", Int(u64::from(a))));
            }
            crate::slow_log::record_queue(
                &crate::slow_log::SlowQueueVerb {
                    op: crate::services::queue_metrics::OPS[o.op.min(6)],
                    store: &o.store,
                    queue: o.metrics.label(label),
                    count: jobs.max(affected),
                    queue_us: stats.queue_us,
                    exec_us: stats.exec_us,
                },
                o.slow.threshold_ms,
            );
        }
        self.responder.end_ok(Bytes::from(body));
    }
}

/// A verb's success, before its `stats` are known. Its `Debug` names the verb only: a RESERVE's jobs
/// carry payloads, which are never logged (§24.9).
pub enum VerbOk {
    Enqueue(pgq::Enqueued),
    Reserve(Vec<pgq::Reserved>),
    Ack { gone: bool },
    /// The new row's id and its QUEUE (M7-G3: the label and the `delay_s = 0` wake hint), or `None`
    /// (`gone`).
    Release(Option<(JobId, String)>),
    Extend(i64),
    Size(pgq::Sizes),
    Clear(u64),
}

impl std::fmt::Debug for VerbOk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let verb = match self {
            VerbOk::Enqueue(_) => "Enqueue",
            VerbOk::Reserve(_) => "Reserve",
            VerbOk::Ack { .. } => "Ack",
            VerbOk::Release(_) => "Release",
            VerbOk::Extend(_) => "Extend",
            VerbOk::Size(_) => "Size",
            VerbOk::Clear(_) => "Clear",
        };
        write!(f, "VerbOk::{verb}")
    }
}

impl VerbOk {
    fn encode(self, stats: QueueStats) -> Vec<u8> {
        match self {
            VerbOk::Enqueue(e) => EnqueueResponse {
                job_id: e.job_id.map(JobId::encode),
                inserted: e.inserted,
                deduplicated: false,
                stats,
            }
            .encode(),
            VerbOk::Reserve(jobs) => ReserveResponse {
                jobs: jobs
                    .into_iter()
                    .map(|j| ReservedJob {
                        job_id: j.id.encode(),
                        token: j.token.encode().to_vec(),
                        attempts: j.attempts,
                        queue: j.queue,
                        payload: j.payload,
                        created_at: j.created_at,
                        lease_deadline: j.lease_deadline,
                    })
                    .collect(),
                stats,
            }
            .encode(),
            VerbOk::Ack { gone } => AckResponse {
                outcome: if gone {
                    ack_outcome::GONE
                } else {
                    ack_outcome::ACKED
                },
                stats,
            }
            .encode(),
            VerbOk::Release(new_id) => ReleaseResponse {
                new_job_id: new_id.map(|(id, _)| id.encode()),
                stats,
            }
            .encode(),
            VerbOk::Extend(lease_deadline) => ExtendResponse {
                lease_deadline,
                stats,
            }
            .encode(),
            VerbOk::Size(s) => SizeResponse {
                pending: s.pending,
                delayed: s.delayed,
                reserved: s.reserved,
                oldest_pending_at: s.oldest_pending_at,
                stats,
            }
            .encode(),
            VerbOk::Clear(deleted) => ClearResponse { deleted, stats }.encode(),
        }
    }
}

/// Build, run and decode the verb's statement(s) on `co`.
async fn run_pg_verb<B: PoolBackend>(
    co: &mut Checkout<B>,
    req: &QueueRequest,
    store: &StoreConfig,
    t: &TableName,
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
    exec_us: &mut u64,
) -> Result<VerbOk, ErrorPayload> {
    let ctx = |sent| verb_context(req, sent);
    let bad = |_: pgq::Malformed| malformed(req, store);
    match req {
        QueueRequest::Enqueue(r) => {
            let n = r.jobs.len();
            let stmt = pgq::enqueue(t, r.jobs.clone());
            let qr = run_verb_statement(co, &stmt, deadline, cancel, ctx, exec_us).await?;
            pgq::decode_enqueue(&qr.rows, qr.affected, n)
                .map(VerbOk::Enqueue)
                .map_err(bad)
        }
        QueueRequest::Reserve(r) => {
            let limit = checks::reserve_limit(r.max_jobs, store.max_payload_bytes);
            for queue in &r.queues {
                let stmt = pgq::reserve(t, queue, store.lease_s, store.max_payload_bytes, limit);
                // A failure here is THIS statement's fate: every earlier queue reserved nothing.
                let qr = run_verb_statement(co, &stmt, deadline, cancel, ctx, exec_us).await?;
                let jobs = pgq::decode_reserve(&qr.rows, limit).map_err(bad)?;
                if !jobs.is_empty() {
                    return Ok(VerbOk::Reserve(jobs));
                }
            }
            Ok(VerbOk::Reserve(Vec::new()))
        }
        QueueRequest::Ack(r) => {
            let (id, token) = decode_handles(store, &r.job_id, &r.token)?;
            let qr =
                run_verb_statement(co, &pgq::ack(t, id, token), deadline, cancel, ctx, exec_us)
                    .await?;
            match pgq::decode_ack(&qr.rows, token).map_err(bad)? {
                Ok(()) => Ok(VerbOk::Ack { gone: false }),
                Err(NoMatch::Gone) => Ok(VerbOk::Ack { gone: true }),
                Err(NoMatch::LeaseLost) => Err(lease_lost(&store.name, req.verb(), false)),
            }
        }
        QueueRequest::Release(r) => {
            let (id, token) = decode_handles(store, &r.job_id, &r.token)?;
            let stmt = pgq::release(t, id, token, r.delay_s);
            let qr = run_verb_statement(co, &stmt, deadline, cancel, ctx, exec_us).await?;
            match pgq::decode_release(&qr.rows, token).map_err(bad)? {
                Ok(released) => Ok(VerbOk::Release(Some(released))),
                Err(NoMatch::Gone) => Ok(VerbOk::Release(None)),
                Err(NoMatch::LeaseLost) => Err(lease_lost(&store.name, req.verb(), false)),
            }
        }
        QueueRequest::Extend(r) => {
            let (id, token) = decode_handles(store, &r.job_id, &r.token)?;
            let stmt = pgq::extend(t, id, token, store.lease_s);
            let qr = run_verb_statement(co, &stmt, deadline, cancel, ctx, exec_us).await?;
            match pgq::decode_extend(&qr.rows).map_err(bad)? {
                Some(lease_deadline) => Ok(VerbOk::Extend(lease_deadline)),
                None => Err(lease_lost(&store.name, req.verb(), false)),
            }
        }
        QueueRequest::Size(r) => {
            let qr =
                run_verb_statement(co, &pgq::size(t, &r.queue), deadline, cancel, ctx, exec_us)
                    .await?;
            pgq::decode_size(&qr.rows).map(VerbOk::Size).map_err(bad)
        }
        QueueRequest::Clear(r) => {
            let qr =
                run_verb_statement(co, &pgq::clear(t, &r.queue), deadline, cancel, ctx, exec_us)
                    .await?;
            Ok(VerbOk::Clear(qr.affected))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Tx-scoped verbs (M7-G2, SPEC §24.5)
// ---------------------------------------------------------------------------------------------

/// The transaction a tx-scoped verb names, and the session asking.
struct TxScope<'a> {
    tx_registry: &'a TxRegistry,
    session_id: SessionId,
    tx_id: u64,
}

/// `PoolMismatch` (SPEC §24.4, §24.5 step 2; NonRetryable): the store's pool is not the
/// transaction's. Decided before anything is sent, so the transaction is untouched. Both names are
/// pool names — operator configuration the client already addresses — never a DSN.
fn pool_mismatch(store: &StoreConfig, tx_pool: &str) -> ErrorPayload {
    ErrorPayload {
        code: errc::POOL_MISMATCH,
        branch: errc::POOL_MISMATCH_BRANCH,
        sqlstate: None,
        errno: None,
        message: format!(
            "queue store {} is on pool {}, but the transaction is on pool {tx_pool}: a tx-scoped \
             QUEUE verb composes only with a store in the transaction's own pool (SPEC §24.5); \
             nothing was sent and the transaction is unaffected",
            store.name, store.pool
        ),
        detail: None,
        retry_after_ms: None,
    }
}

/// SPEC §24.6's `OpContext` for a TX-SCOPED verb: as [`verb_context`] (`readonly` for SIZE only),
/// with `in_tx: true` — a lost in-transaction statement means the transaction is dead (it will never
/// commit), a known fate, never `Indeterminate` for the statement itself.
fn tx_verb_context(req: &QueueRequest, sent: bool) -> OpContext {
    OpContext {
        in_tx: true,
        ..verb_context(req, sent)
    }
}

/// A tx-scoped verb (§24.5; the module doc's tx-scoped steps): resolve the transaction, check its pool,
/// verify the store at first use, then send ONE [`TxCommand::Queue`] to the transaction's actor and
/// declare the terminal from its reply.
///
/// First-use verification runs on a SEPARATE checkout of the store's pool, never inside the
/// application's transaction: a failed catalog statement there would abort the transaction, and its
/// snapshot (at REPEATABLE READ) is the application's, not the catalog's latest. Verification happens
/// once per store per process, so this costs a second connection at most once; on a pool whose every
/// connection is pinned it waits within the request's deadline and answers `PoolTimeout` (nothing
/// sent, the transaction untouched).
#[allow(clippy::too_many_arguments)]
async fn serve_tx(
    stores: &Arc<QueueStores>,
    store: &StoreConfig,
    registry: &PoolRegistry,
    tx: TxScope<'_>,
    req: QueueRequest,
    deadline: Option<tokio::time::Instant>,
    cancel: CancellationToken,
    reply: Reply,
) {
    // 1. The transaction, exactly as a tx-scoped EXEC resolves it (R2).
    let handle = match resolve_active(tx.tx_registry, tx.tx_id, tx.session_id) {
        Ok(h) => h,
        Err(ep) => {
            reply.error(ep);
            return;
        }
    };
    // 2. Its pool against the store's — before anything is sent (D22 (b)).
    if *handle.pool != *store.pool {
        reply.error(pool_mismatch(store, &handle.pool));
        return;
    }
    let table = match ensure_verified(stores, store, registry, deadline, &cancel).await {
        Ok(t) => t,
        Err(ep) => {
            reply.error(ep);
            return;
        }
    };
    // What is left of the request's ONE deadline bounds the verb on the actor. None left: answered
    // unsent (the transaction untouched), never dispatched with no time.
    let timeout_ms = match deadline {
        None => None,
        Some(d) => {
            let left = d
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis();
            if left == 0 {
                reply.error(fate::classify_fate(
                    PoolError::Timeout,
                    tx_verb_context(&req, false),
                ));
                return;
            }
            Some(u32::try_from(left).unwrap_or(u32::MAX))
        }
    };
    let verb = match TxVerb::new(&req, store, &table, Arc::clone(stores)) {
        Ok(v) => v,
        Err(ep) => {
            reply.error(ep);
            return;
        }
    };
    // 3. One actor command per verb.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let cmd = TxCommand::Queue {
        verb: Box::new(verb),
        timeout_ms,
        cancel,
        reply: reply_tx,
    };
    if handle.cmd_tx.send(cmd).await.is_err() {
        reply.error(actor_gone_terminal(tx.tx_registry, tx.tx_id, tx.session_id));
        return;
    }
    match reply_rx.await {
        // A pinned connection is never queued for: `queue_us` is 0, as on a tx-scoped EXEC.
        Ok(QueueReply::Done {
            outcome: Ok(ok),
            exec_us,
        }) => reply.ok(
            ok,
            QueueStats {
                queue_us: 0,
                exec_us,
            },
        ),
        Ok(QueueReply::Done {
            outcome: Err(ep), ..
        })
        | Ok(QueueReply::RolledBack(ep)) => reply.error(ep),
        // The statement WAS sent on the pinned connection, inside the transaction.
        Ok(QueueReply::Failed { error, .. }) => {
            reply.error(fate::classify_fate(error, tx_verb_context(&req, true)))
        }
        // The actor dropped the reply: a session abort, or a teardown that raced the command.
        Err(_) => reply.error(actor_gone_terminal(tx.tx_registry, tx.tx_id, tx.session_id)),
    }
}

/// How the actor decodes a tx-scoped verb's result (one step per verb on PostgreSQL).
#[derive(Debug, Clone, PartialEq, Eq)]
enum TxDecode {
    /// `queues`: the request's distinct queues, one wake hint each on COMMIT.
    Enqueue {
        jobs: usize,
        queues: Vec<String>,
    },
    Ack,
    Release {
        delay_s: u32,
    },
    Extend,
    Size,
    Clear,
}

/// A tx-scoped verb as the TX actor runs it (SPEC §24.5 step 3): the verb's whole engine-authored
/// statement list, built by `ferro_queue::pg`'s pure builders, plus how to decode the results.
///
/// **Every PostgreSQL verb is ONE step**, so G2 builds no per-step stop condition: the actor stops at
/// the first failing step, and a stop condition such as "the previous step matched no row" lands
/// with the first verb that has more than one step (G4's dedup ENQUEUE, G6's MySQL RELEASE). The
/// in-transaction ACK and RELEASE are `ack_in_tx`/`release_in_tx`: the fence without the probe,
/// because in a transaction an unmatched fence is always `LeaseLost` (R1).
pub struct TxVerb {
    pub(crate) steps: Vec<Statement>,
    decode: TxDecode,
    verb: &'static str,
    store: String,
    stores: Arc<QueueStores>,
}

/// Names the verb and its step count only — never a statement's parameters, which carry payloads.
impl std::fmt::Debug for TxVerb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxVerb")
            .field("verb", &self.verb)
            .field("steps", &self.steps.len())
            .finish()
    }
}

/// What the actor does with a tx-scoped verb's decoded result.
pub enum TxVerdict {
    /// The verb took effect: its success, and its wake hints, kept for COMMIT (§24.5 step 4).
    Applied { ok: VerbOk, wake: Vec<WakeHint> },
    /// The verb did nothing, or only read: its terminal, and the transaction stays open. A fenced
    /// verb's `LeaseLost` lands here — "the transaction stays open, since no SQL error occurred"
    /// (§24.6), and the caller rolls back.
    NoEffect(Result<VerbOk, ErrorPayload>),
    /// A WRITE ran but its result is unreadable: the actor rolls the transaction back and
    /// tombstones it, answering this payload (`TxDeadline{Retryable}`).
    Unreadable(ErrorPayload),
}

/// One wake hint for `(store, queue)`, held by the TX actor until COMMIT (SPEC §24.5 step 4, §24.8).
pub struct WakeHint {
    stores: Arc<QueueStores>,
    store: String,
    queue: String,
}

impl WakeHint {
    /// Fire it: only ever called after a SUCCESSFUL COMMIT.
    pub fn fire(self) {
        self.stores.wake(&self.store, &self.queue);
    }
}

/// Names the store only: a queue name is a label only when the operator lists it (§24.9).
impl std::fmt::Debug for WakeHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakeHint")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl TxVerb {
    /// Build the verb's steps. Every refusal here was already made before any checkout (the
    /// handles decode; a RESERVE never gets here), so an `Err` is answered rather than asserted.
    fn new(
        req: &QueueRequest,
        store: &StoreConfig,
        t: &TableName,
        stores: Arc<QueueStores>,
    ) -> Result<TxVerb, ErrorPayload> {
        let (step, decode) = match req {
            QueueRequest::Enqueue(r) => {
                let mut queues: Vec<String> = r.jobs.iter().map(|j| j.queue.clone()).collect();
                queues.sort();
                queues.dedup();
                (
                    pgq::enqueue(t, r.jobs.clone()),
                    TxDecode::Enqueue {
                        jobs: r.jobs.len(),
                        queues,
                    },
                )
            }
            QueueRequest::Reserve(_) => {
                return Err(unsupported(
                    "RESERVE inside a transaction is refused (SPEC §24.5): send it without a tx_id",
                ));
            }
            QueueRequest::Ack(r) => {
                let (id, token) = decode_handles(store, &r.job_id, &r.token)?;
                (pgq::ack_in_tx(t, id, token), TxDecode::Ack)
            }
            QueueRequest::Release(r) => {
                let (id, token) = decode_handles(store, &r.job_id, &r.token)?;
                (
                    pgq::release_in_tx(t, id, token, r.delay_s),
                    TxDecode::Release { delay_s: r.delay_s },
                )
            }
            QueueRequest::Extend(r) => {
                let (id, token) = decode_handles(store, &r.job_id, &r.token)?;
                (pgq::extend(t, id, token, store.lease_s), TxDecode::Extend)
            }
            QueueRequest::Size(r) => (pgq::size(t, &r.queue), TxDecode::Size),
            QueueRequest::Clear(r) => (pgq::clear(t, &r.queue), TxDecode::Clear),
        };
        Ok(TxVerb {
            steps: vec![step],
            decode,
            verb: req.verb(),
            store: store.name.clone(),
            stores,
        })
    }

    fn hint(&self, queue: &str) -> WakeHint {
        WakeHint {
            stores: Arc::clone(&self.stores),
            store: self.store.clone(),
            queue: queue.to_string(),
        }
    }

    /// Decode the steps' results (one per step, in order). Called by the TX actor, which acts on the
    /// verdict before it serves the transaction's next command.
    pub fn decode(&self, results: &[QueryResult]) -> TxVerdict {
        let [qr] = results else {
            return self.unreadable();
        };
        let applied = |ok| TxVerdict::Applied {
            ok,
            wake: Vec::new(),
        };
        let lost = || TxVerdict::NoEffect(Err(lease_lost(&self.store, self.verb, true)));
        match &self.decode {
            TxDecode::Enqueue { jobs, queues } => {
                match pgq::decode_enqueue(&qr.rows, qr.affected, *jobs) {
                    Ok(e) => TxVerdict::Applied {
                        ok: VerbOk::Enqueue(e),
                        wake: queues.iter().map(|q| self.hint(q)).collect(),
                    },
                    Err(pgq::Malformed) => self.unreadable(),
                }
            }
            TxDecode::Ack => match pgq::decode_ack_in_tx(&qr.rows, qr.affected) {
                Ok(true) => applied(VerbOk::Ack { gone: false }),
                Ok(false) => lost(),
                Err(pgq::Malformed) => self.unreadable(),
            },
            TxDecode::Release { delay_s } => match pgq::decode_release_in_tx(&qr.rows) {
                // §24.8: only a RELEASE with `delay_s = 0` makes a job available now.
                Ok(Some((new_id, queue))) => TxVerdict::Applied {
                    ok: VerbOk::Release(Some((new_id, queue.clone()))),
                    wake: if *delay_s == 0 {
                        vec![self.hint(&queue)]
                    } else {
                        Vec::new()
                    },
                },
                Ok(None) => lost(),
                Err(pgq::Malformed) => self.unreadable(),
            },
            TxDecode::Extend => match pgq::decode_extend(&qr.rows) {
                Ok(Some(lease_deadline)) => applied(VerbOk::Extend(lease_deadline)),
                Ok(None) => lost(),
                Err(pgq::Malformed) => self.unreadable(),
            },
            TxDecode::Size => match pgq::decode_size(&qr.rows) {
                Ok(sizes) => TxVerdict::NoEffect(Ok(VerbOk::Size(sizes))),
                Err(pgq::Malformed) => self.unreadable(),
            },
            TxDecode::Clear => applied(VerbOk::Clear(qr.affected)),
        }
    }

    /// The statement ran but its rows do not have the builder's shape. SIZE (a read, no effect) is a
    /// plain refusal and the transaction stays open. A WRITE's effect is real and unreportable, so the
    /// actor rolls the transaction back and tombstones it — the one in-transaction way to make that
    /// effect's fate KNOWN (it will never commit) — and the terminal is SPEC D25's: NonRetryable
    /// (`Unsupported`), because the cause is a table changed after the once-per-process verification,
    /// so every retry would fail identically (redoing the business work each time) until the table is
    /// fixed and `ferrod` restarts. Never a known-fate code claiming the verb did nothing, never an
    /// `Indeterminate` the client could follow with a COMMIT of the unseen effect, and never
    /// `Retryable`.
    fn unreadable(&self) -> TxVerdict {
        if self.decode == TxDecode::Size {
            return TxVerdict::NoEffect(Err(unsupported(format!(
                "queue store {}: the tx-scoped QUEUE SIZE statement ran but its result did not have \
                 the expected shape (was the table altered after it was verified? ferrod re-verifies \
                 at restart); the transaction is unaffected",
                self.store
            ))));
        }
        TxVerdict::Unreadable(self.write_rolled_back("its result did not have the expected shape"))
    }

    /// D25's terminal for a WRITE whose effect cannot be reported: the transaction was rolled back
    /// and tombstoned, and retrying cannot help until the store is fixed and `ferrod` restarts.
    fn write_rolled_back(&self, what: &str) -> ErrorPayload {
        unsupported(format!(
            "queue store {}: the tx-scoped QUEUE {} statement ran but {what} (was the table altered \
             after it was verified?). The transaction was rolled back, so the unreported effect can \
             never commit, and this transaction id is finished. Retrying cannot help until the \
             store's table is fixed and ferrod is restarted (it re-verifies at restart); the engine \
             re-runs nothing (SPEC D25)",
            self.store, self.verb
        ))
    }

    /// SPEC D25, the second half: a statement that FAILED after it was sent, with an error that is
    /// neither the backend's SQL answer (`PoolError::Sql`, a known fate the transaction survives)
    /// nor a lost link (`ConnectionLost`: the backend rolls the transaction back with the link, the
    /// established `Retryable` of §24.6). For a WRITE, anything else — a result the driver could
    /// not decode AFTER the statement executed (`PoolError::Backend`), an `Unsupported` result type —
    /// may leave the write applied inside the open transaction. So it takes the unreadable arm:
    /// `Some(terminal)`, and the actor rolls back and tombstones. `None` for a READ, and for the two
    /// known-fate errors, which take the `Failed` arm.
    pub fn rolls_back_on(&self, e: &PoolError) -> Option<ErrorPayload> {
        if self.decode == TxDecode::Size
            || matches!(e, PoolError::Sql { .. } | PoolError::ConnectionLost)
        {
            return None;
        }
        Some(self.write_rolled_back("the result could not be read"))
    }
}

/// The version gate and shape verification, once per store per process (SPEC §24.3).
async fn ensure_verified(
    stores: &QueueStores,
    store: &StoreConfig,
    registry: &PoolRegistry,
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
) -> Result<TableName, ErrorPayload> {
    let Some(cell) = stores.verdicts.get(&store.name) else {
        return Err(unsupported("unknown queue store"));
    };
    // The REQUEST's one deadline bounds the whole first use — the wait for another request's
    // verification, the checkout and the statement — as on EXEC (M3-D1c review F1).
    let not_sent = |e: PoolError| fate::classify_fate(e, verification_context(false));
    let mut verdict = tokio::select! {
        biased;
        g = cell.lock() => g,
        () = sleep_until_opt(deadline) => return Err(not_sent(PoolError::Timeout)),
        () = cancel.cancelled() => return Err(not_sent(cancelled_before_dispatch())),
    };
    match &*verdict {
        Verdict::Verified(table) => return Ok(table.clone()),
        Verdict::Refused(m) => return Err(unsupported(m.clone())),
        Verdict::GateRefused { message, at }
            if at.elapsed()
                < stores
                    .gate_recheck
                    .unwrap_or_else(|| registry.version_ttl()) =>
        {
            return Err(unsupported(message.clone()));
        }
        Verdict::Absent { message, at } if at.elapsed() < stores.absent_recheck => {
            return Err(unsupported(message.clone()));
        }
        Verdict::Unverified | Verdict::GateRefused { .. } | Verdict::Absent { .. } => {}
    }

    // The version gate, against the pool's existing probe — bounded by THIS request's deadline and
    // CANCEL (review L2): the wait can last the probe's whole budget, and the store's verdict lock is
    // held across it.
    let version = tokio::select! {
        biased;
        v = registry.server_version(&store.pool) => v,
        () = sleep_until_opt(deadline) => return Err(not_sent(PoolError::Timeout)),
        () = cancel.cancelled() => return Err(not_sent(cancelled_before_dispatch())),
    };
    let Some(server_version) = version else {
        if stores.should_warn_unknown_version(&store.name, registry.version_backoff()) {
            tracing::warn!(
                store = %store.name, pool = %store.pool,
                "ferrod: Ferro Queue store refused: its pool's server version is unknown (the \
                 version probe has failed or not answered; the probe's own failure is logged at \
                 debug)"
            );
        }
        return Err(ErrorPayload {
            code: errc::CONNECTION_LOST,
            branch: errc::CONNECTION_LOST_BRANCH,
            sqlstate: None,
            errno: None,
            message: format!(
                "queue store {}: the server version of pool {} is not known — its version probe \
                 has not succeeded (see the ferrod log) — so the version gate cannot pass; nothing \
                 was sent",
                store.name, store.pool
            ),
            detail: None,
            retry_after_ms: None,
        });
    };
    if let Err(refusal) = version::gate(store.family, &server_version) {
        let message = format!("queue store {}: {refusal}", store.name);
        tracing::error!(store = %store.name, refusal = %refusal, "ferrod: queue store refused by the version gate");
        *verdict = Verdict::GateRefused {
            message: message.clone(),
            at: Instant::now(),
        };
        return Err(unsupported(message));
    }

    let outcome = match registry.get(&store.pool) {
        Some(AnyPool::Pg(pool)) => verify_pg(pool, stores, store, deadline, cancel).await,
        // Unreachable: `refuse_before_checkout` refused the MySQL family and configuration refused
        // SQLite. Answered rather than asserted, so a future family cannot panic a session.
        Some(_) | None => Err(Verification::Refused(unsupported(format!(
            "queue store {}: its pool cannot be verified in this build",
            store.name
        )))),
    };
    match outcome {
        Ok(resolved) => {
            tracing::info!(store = %store.name, table = %store.table, resolved = %resolved, "ferrod: queue store verified");
            *verdict = Verdict::Verified(resolved.clone());
            Ok(resolved)
        }
        Err(Verification::Shape(e)) => {
            let message = format!("queue store {}: {e}", store.name);
            if e.is_definitive() {
                tracing::error!(store = %store.name, refusal = %e, "ferrod: queue store refused by shape verification");
                *verdict = Verdict::Refused(message.clone());
            } else {
                *verdict = Verdict::Absent {
                    message: message.clone(),
                    at: Instant::now(),
                };
            }
            Err(unsupported(message))
        }
        Err(Verification::Refused(ep)) => Err(ep),
    }
}

/// The fate context of every statement first-use verification runs: a READ (the catalog, the identity
/// probe), autocommit. So a lost or cancelled verification is never `Indeterminate`, whatever
/// verb triggered it — the verb itself was never sent (SPEC §24.6: "refused before sending").
fn verification_context(sent: bool) -> OpContext {
    OpContext {
        readonly: true,
        sent,
        in_tx: false,
    }
}

enum Verification {
    /// The table does not have the served shape (an absent table included).
    Shape(ShapeError),
    /// Verification itself failed: a classified checkout/statement error, or an unexpected result.
    Refused(ErrorPayload),
}

/// PostgreSQL shape verification: one guarded, interruptible read on one checkout, plus the
/// diagnostics-only identity probe.
async fn verify_pg<B: PoolBackend>(
    pool: &Pool<B>,
    stores: &QueueStores,
    store: &StoreConfig,
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
) -> Result<TableName, Verification> {
    let ctx = verification_context;
    // A declared read: on a backend that can enforce the declaration it does (C3-4).
    let checkout = pool.checkout_declared(true);
    tokio::pin!(checkout);
    let checked_out = tokio::select! {
        biased;
        r = &mut checkout => r,
        () = sleep_until_opt(deadline) => Err(PoolError::Timeout),
        () = cancel.cancelled() => Err(cancelled_before_dispatch()),
    };
    let mut co =
        checked_out.map_err(|e| Verification::Refused(fate::classify_fate(e, ctx(false))))?;
    let remaining = |d: Option<tokio::time::Instant>| -> Result<Option<u32>, Verification> {
        match d {
            None => Ok(None),
            Some(d) => {
                let left = d
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_millis();
                if left == 0 {
                    Err(Verification::Refused(fate::classify_fate(
                        PoolError::Timeout,
                        ctx(false),
                    )))
                } else {
                    Ok(Some(u32::try_from(left).unwrap_or(u32::MAX)))
                }
            }
        }
    };

    let stmt = shape::pg_relation_statement(&store.table);
    stores.verifications.fetch_add(1, Ordering::Relaxed);
    let (result, _exec_us) = run_autocommit_exec(
        &mut co,
        &stmt.sql,
        &stmt.params,
        remaining(deadline)?,
        cancel,
    )
    .await;
    let qr = result.map_err(|e| Verification::Refused(fate::classify_fate(e, ctx(true))))?;
    let Ok(relation) = PgRelation::from_rows(&qr.rows) else {
        return Err(Verification::Refused(unsupported(format!(
            "queue store {}: the catalog returned an unexpected row shape",
            store.name
        ))));
    };
    shape::verify_pg(&store.table, relation.as_ref()).map_err(Verification::Shape)?;
    // `verify_pg` passed, so the relation exists: it resolved in `schema`.
    let schema = relation.map(|r| r.schema).unwrap_or_default();
    let resolved = TableName::resolved(&schema, &store.table);

    // Diagnostics only (SPEC §24.3): a table whose `id` has no sequence default is logged, never
    // refused — ENQUEUE would then fail with `NotNull`, classified like any statement.
    let serial = shape::pg_serial_statement(&resolved);
    if let Ok(left) = remaining(deadline) {
        let (r, _) = run_autocommit_exec(&mut co, &serial.sql, &serial.params, left, cancel).await;
        match r
            .as_ref()
            .map(|qr| qr.rows.first().and_then(|row| row.first()))
        {
            Ok(Some(ferro_proto::value::Value::Text(_))) => {}
            Ok(_) => tracing::warn!(
                store = %store.name, table = %store.table,
                "ferrod: queue table's id column has no sequence default; ENQUEUE will fail"
            ),
            Err(_) => tracing::debug!(store = %store.name, "ferrod: queue identity probe failed"),
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::messages::EnqueueJob;
    use std::ffi::OsString;

    /// A fixed engine clock for the pre-send checks.
    const NOW: i64 = 1_790_000_000;

    fn store() -> StoreConfig {
        let cfg = QueueConfig::load(
            [
                ("FERRO_QUEUE_STORES", "jobs"),
                ("FERRO_QUEUE_JOBS_POOL", "main"),
            ]
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &|_| Some(PoolFamily::Postgres),
        );
        cfg.store("jobs").cloned().unwrap()
    }

    fn my_store() -> StoreConfig {
        let mut s = store();
        s.family = PoolFamily::Mysql;
        s
    }

    fn fenced(job_id: &[u8], token: &[u8]) -> QueueRequest {
        QueueRequest::Ack(FencedRequest {
            store: "jobs".into(),
            job_id: job_id.to_vec(),
            token: token.to_vec(),
            common: QueueCommon::default(),
        })
    }

    #[test]
    fn an_undecodable_handle_is_invalid_handle_never_protocol_or_lease_lost() {
        let good_token = Token::from_pg(1, 1).encode();
        assert!(refuse_before_checkout(&fenced(b"42", &good_token), &store(), NOW).is_ok());
        for (job_id, token) in [
            (&b"042"[..], &good_token[..]),
            (b"+42", &good_token),
            (b"abc", &good_token),
            (b"42", &[0u8; 7][..]),
            (b"42", &[1u8; 8][..]),
        ] {
            let ep = refuse_before_checkout(&fenced(job_id, token), &store(), NOW).unwrap_err();
            assert_eq!(ep.code, errc::INVALID_HANDLE);
            assert_eq!(ep.branch, errc::INVALID_HANDLE_BRANCH);
            assert_ne!(ep.code, errc::PROTOCOL);
            assert_ne!(ep.code, errc::LEASE_LOST);
        }
        // RELEASE decodes both too.
        let rel = QueueRequest::Release(ReleaseRequest {
            store: "jobs".into(),
            job_id: b"x".to_vec(),
            token: good_token.to_vec(),
            delay_s: 0,
            common: QueueCommon::default(),
        });
        assert_eq!(
            refuse_before_checkout(&rel, &store(), NOW)
                .unwrap_err()
                .code,
            errc::INVALID_HANDLE
        );
    }

    #[test]
    fn tx_scoped_and_mysql_requests_are_refused_before_any_checkout() {
        let tx = QueueCommon {
            tx_id: Some(7),
            ..QueueCommon::default()
        };
        let reserve = QueueRequest::Reserve(ReserveRequest {
            store: "jobs".into(),
            queues: vec!["default".into()],
            max_jobs: 1,
            wait_ms: 0,
            liveness: false,
            common: tx.clone(),
        });
        let ep = refuse_before_checkout(&reserve, &store(), NOW).unwrap_err();
        assert_eq!(ep.code, errc::UNSUPPORTED);
        assert!(ep.message.contains("§24.5"), "{}", ep.message);
        let enq = QueueRequest::Enqueue(EnqueueRequest {
            store: "jobs".into(),
            jobs: vec![EnqueueJob {
                queue: "default".into(),
                payload: "{}".into(),
                delay_s: 0,
            }],
            dedup_key: None,
            common: tx,
        });
        // M7-G2: a tx-scoped ENQUEUE is no longer refused here — it is resolved against the TX
        // registry, its pool checked and served on the pinned connection (`serve_tx`).
        assert!(refuse_before_checkout(&enq, &store(), NOW).is_ok());
        let mut my = store();
        // A MySQL-family store is still refused (G6), tx-scoped or not.
        assert!(
            refuse_before_checkout(&enq, &my_store(), NOW)
                .unwrap_err()
                .message
                .contains("G6")
        );
        my.family = PoolFamily::Mysql;
        let size = QueueRequest::Size(QueueScopeRequest {
            store: "jobs".into(),
            queue: "default".into(),
            common: QueueCommon::default(),
        });
        assert!(refuse_before_checkout(&size, &store(), NOW).is_ok());
        let ep = refuse_before_checkout(&size, &my, NOW).unwrap_err();
        assert!(ep.message.contains("G6"), "{}", ep.message);
    }

    /// A registry with one PostgreSQL pool nobody listens on, and the `jobs` store on it.
    fn registry_with_dead_pool() -> Arc<PoolRegistry> {
        registry_at("postgres://ferro:ferro@127.0.0.1:1/ferro", None)
    }

    /// One PostgreSQL pool `main` at `dsn`, and the `jobs` store on it (with `table`, if given).
    fn registry_at(dsn: &str, table: Option<&str>) -> Arc<PoolRegistry> {
        let mut config = crate::config::Config {
            pools: vec![crate::config::PoolSpec {
                name: "main".into(),
                dsn: dsn.into(),
                kind: crate::config::PoolKind::Postgres,
                pin_functions: Vec::new(),
                pin_on_unknown: true,
                allow_dir: None,
            }],
            ..crate::config::Config::default()
        };
        let mut vars = vec![
            ("FERRO_QUEUE_STORES".to_string(), "jobs".to_string()),
            ("FERRO_QUEUE_JOBS_POOL".to_string(), "main".to_string()),
        ];
        if let Some(t) = table {
            vars.push(("FERRO_QUEUE_JOBS_TABLE".to_string(), t.to_string()));
        }
        let vars = vars
            .into_iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)));
        config.queue = Some(Arc::new(crate::queue_config::load(vars, &config)));
        PoolRegistry::build(&config)
    }

    /// A fresh `QueueStores` over the registry's configuration, with test-sized timers.
    fn stores_with(
        registry: &PoolRegistry,
        absent_recheck: Duration,
        gate_recheck: Option<Duration>,
    ) -> QueueStores {
        let mut stores = QueueStores::new(Arc::clone(&registry.queue().unwrap().config));
        stores.absent_recheck = absent_recheck;
        stores.gate_recheck = gate_recheck;
        stores
    }

    fn pg_url() -> Option<String> {
        match std::env::var("FERRO_TEST_PG_URL") {
            Ok(u) => Some(u),
            Err(_) => {
                eprintln!("skip: FERRO_TEST_PG_URL unset");
                None
            }
        }
    }

    /// Review L7: a tx-scoped RESERVE is told the PERMANENT reason first, even when another of its
    /// fields would also be refused (here `wait_ms > 0`, whose refusal advises `wait_ms = 0`).
    #[test]
    fn a_tx_scoped_reserve_is_refused_for_the_transaction_before_anything_else() {
        let reserve = QueueRequest::Reserve(ReserveRequest {
            store: "jobs".into(),
            queues: vec!["default".into()],
            max_jobs: 0,
            wait_ms: 3_000,
            liveness: true,
            common: QueueCommon {
                tx_id: Some(7),
                ..QueueCommon::default()
            },
        });
        let ep = refuse_before_checkout(&reserve, &store(), NOW).unwrap_err();
        assert!(ep.message.contains("§24.5"), "{}", ep.message);
    }

    /// Review L3: an unknown version is warned about once per window, not per request.
    #[tokio::test]
    async fn the_unknown_version_warning_fires_once_per_window() {
        let mut last = far_past();
        let window = Duration::from_millis(40);
        assert!(warn_once_per(&mut last, window), "the first one fires");
        assert!(
            !warn_once_per(&mut last, window),
            "inside the window: silent"
        );
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            warn_once_per(&mut last, window),
            "after the window: fires again"
        );
        let registry = registry_with_dead_pool();
        let stores = registry.queue().unwrap();
        assert!(stores.should_warn_unknown_version("jobs", Duration::from_secs(60)));
        assert!(!stores.should_warn_unknown_version("jobs", Duration::from_secs(60)));
        assert!(
            stores.should_warn_unknown_version("other", Duration::from_secs(60)),
            "per store"
        );
    }

    /// Review R7 (a mutation that survived): an UNKNOWN version must refuse — on a LIVE pool, where a
    /// gate that let it through would reach the database and succeed — before any checkout.
    #[tokio::test]
    async fn an_unknown_version_refuses_before_any_checkout_on_a_live_pool() {
        let Some(url) = pg_url() else { return };
        let registry = registry_at(&url, None);
        let stores = stores_with(&registry, ABSENT_RECHECK, None);
        let store = stores.config().store("jobs").unwrap().clone();
        registry.seed_failed_probe_for_test("main");
        let ep = ensure_verified(&stores, &store, &registry, None, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(
            (ep.code, ep.branch),
            (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH)
        );
        assert!(ep.message.contains("version probe"), "{}", ep.message);
        assert_eq!(stores.verifications(), 0, "no catalog read");
        assert_eq!(
            registry.get("main").unwrap().checkout_histogram().count,
            0,
            "no checkout at all"
        );
    }

    /// Review L4: the gate's refusal lives as long as the version it was decided on — after the
    /// probe's TTL (shortened here) an upgraded backend passes.
    #[tokio::test]
    async fn a_gate_refusal_expires_with_the_version_it_was_decided_on() {
        let registry = registry_with_dead_pool();
        let stores = stores_with(&registry, ABSENT_RECHECK, Some(Duration::from_millis(50)));
        let store = stores.config().store("jobs").unwrap().clone();
        let cancel = CancellationToken::new();
        registry.seed_version_for_test("main", "PostgreSQL 11.22");
        let ep = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert!(ep.message.contains("12"), "{}", ep.message);
        registry.seed_version_for_test("main", "PostgreSQL 16.4");
        let still = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert_eq!(
            still.message, ep.message,
            "inside the TTL: the cached refusal"
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        let after = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert_eq!(
            after.code,
            errc::CONNECTION_LOST,
            "after the TTL the upgraded version passes the gate and verification needs the \
             (unreachable) database: {after:?}"
        );
    }

    /// Review L2: the version wait honours the REQUEST's deadline. The backend accepts the TCP
    /// connection and never answers, so the probe hangs for its whole budget (1.5 s); the request's
    /// 150 ms deadline answers first, as a known non-execution.
    #[tokio::test]
    async fn the_version_wait_is_bounded_by_the_requests_deadline() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let _hold = std::thread::spawn(move || {
            let mut held = Vec::new();
            for c in listener.incoming().flatten() {
                held.push(c); // accept, never speak
            }
        });
        let registry = registry_at(
            &format!("postgres://ferro:ferro@127.0.0.1:{port}/ferro"),
            None,
        );
        let stores = stores_with(&registry, ABSENT_RECHECK, None);
        let store = stores.config().store("jobs").unwrap().clone();
        let started = std::time::Instant::now();
        let ep = ensure_verified(
            &stores,
            &store,
            &registry,
            deadline_of(Some(150)),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_millis(1_000),
            "answered after {:?}",
            started.elapsed()
        );
        assert_eq!(ep.code, errc::POOL_TIMEOUT, "{ep:?}");
        // And CANCEL ends it the same way.
        let cancel = CancellationToken::new();
        let c2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c2.cancel();
        });
        let started = std::time::Instant::now();
        let ep = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(1_000));
        assert_ne!(
            ep.branch,
            ferro_proto::consts::branch::INDETERMINATE,
            "{ep:?}"
        );
    }

    /// Review M2: an absent table is re-read at most once per `absent_recheck`, COUNTED — two uses
    /// inside the window issue one catalog read — and a table created afterwards is then verified.
    #[tokio::test]
    async fn an_absent_table_is_re_read_once_per_window_not_per_request() {
        let Some(url) = pg_url() else { return };
        let name = format!("g1a_absent_ttl_{}", std::process::id());
        let registry = registry_at(&url, Some(&name));
        let mut stores = stores_with(&registry, Duration::from_secs(3_600), None);
        let store = stores.config().store("jobs").unwrap().clone();
        let cancel = CancellationToken::new();
        registry.seed_version_for_test("main", "PostgreSQL 16.4");
        for _ in 0..3 {
            let ep = ensure_verified(&stores, &store, &registry, None, &cancel)
                .await
                .unwrap_err();
            assert!(ep.message.contains("does not exist"), "{}", ep.message);
        }
        assert_eq!(stores.verifications(), 1, "one catalog read for three uses");

        // Create the table through the pool, then let the window lapse.
        let AnyPool::Pg(pool) = registry.get("main").unwrap() else {
            unreachable!()
        };
        let mut co = pool.checkout().await.unwrap();
        co.exec(&format!(
            "CREATE TABLE \"{name}\" (id bigserial PRIMARY KEY, queue varchar(255) NOT NULL, \
             payload text NOT NULL, attempts smallint NOT NULL, reserved_at integer NULL, \
             available_at integer NOT NULL, created_at integer NOT NULL)"
        ))
        .await
        .unwrap();
        drop(co);
        stores.absent_recheck = Duration::ZERO;
        let verified = ensure_verified(&stores, &store, &registry, None, &cancel).await;
        let mut co = pool.checkout().await.unwrap();
        co.exec(&format!("DROP TABLE \"{name}\"")).await.unwrap();
        drop(co);
        assert!(verified.is_ok(), "{verified:?}");
        assert_eq!(stores.verifications(), 2);
    }

    /// The version gate is WIRED, not merely written: a pool whose probed version is below the gate
    /// refuses every verb with the gate's message, before any checkout (the pool is unreachable, so a
    /// checkout would have answered a connection failure), and the refusal is cached for the process.
    /// The control: a version the gate passes proceeds to shape verification — which, against the
    /// unreachable pool, is a known non-execution.
    #[tokio::test]
    async fn the_version_gate_is_consulted_at_first_use_and_its_refusal_is_cached() {
        let registry = registry_with_dead_pool();
        let stores = registry.queue().unwrap().clone();
        let store = stores.config().store("jobs").unwrap().clone();
        let cancel = CancellationToken::new();

        registry.seed_version_for_test("main", "PostgreSQL 11.22 on x86_64-pc-linux-gnu");
        let ep = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
        assert!(ep.message.contains("12"), "{}", ep.message);
        // Cached for the probe's TTL (600 s in production): a version the gate would pass is not
        // consulted again within it.
        registry.seed_version_for_test("main", "PostgreSQL 16.4");
        let again = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert_eq!(again.message, ep.message);

        let registry = registry_with_dead_pool();
        let stores = registry.queue().unwrap().clone();
        registry.seed_version_for_test("main", "PostgreSQL 16.4");
        let ep = ensure_verified(&stores, &store, &registry, None, &cancel)
            .await
            .unwrap_err();
        assert_eq!(
            (ep.code, ep.branch),
            (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
            "past the gate, shape verification needs the (unreachable) database: {ep:?}"
        );
    }

    /// A verification statement cancelled or timed out on the server (`57014`) AFTER it was sent is
    /// classified as the read it is — never `Indeterminate`, even when the verb that triggered it is
    /// a write (an ENQUEUE's first use): the verb was not sent.
    #[test]
    fn a_cancelled_verification_statement_is_never_indeterminate() {
        let cancelled = || PoolError::Sql {
            code: errc::QUERY_TIMEOUT,
            branch: errc::QUERY_TIMEOUT_BRANCH,
            sqlstate: Some("57014".into()),
            errno: None,
            message: "canceling statement due to statement timeout".into(),
        };
        for sent in [true, false] {
            let ep = fate::classify_fate(cancelled(), verification_context(sent));
            assert_ne!(
                ep.branch,
                ferro_proto::consts::branch::INDETERMINATE,
                "sent={sent}: {ep:?}"
            );
            assert_ne!(ep.code, errc::WRITE_UNCONFIRMED);
        }
        // The control: the same error under a WRITE's context is Indeterminate — so the assertion
        // above is about the context, not about the error.
        let write = OpContext {
            readonly: false,
            sent: true,
            in_tx: false,
        };
        assert_eq!(
            fate::classify_fate(cancelled(), write).branch,
            ferro_proto::consts::branch::INDETERMINATE
        );
    }

    /// A two-queue RESERVE on a one-connection `FakeBackend` pool whose every statement answers no
    /// rows — so, unguarded, the verb would run BOTH queues' statements and answer `Ok(empty)`.
    async fn two_queue_reserve(
        deadline: Option<tokio::time::Instant>,
        cancel: &CancellationToken,
    ) -> Result<VerbOk, ErrorPayload> {
        use ferro_pool::config::PoolConfig;
        use ferro_pool::fake::FakeBackend;
        let backend = FakeBackend::new();
        backend.set_query_result(QueryResult::default());
        let pool = Pool::new(
            backend,
            PoolConfig {
                max_size: 1,
                reap_interval: None,
                ..PoolConfig::default()
            },
        );
        let mut co = pool.checkout().await.expect("checkout");
        let req = QueueRequest::Reserve(ReserveRequest {
            store: "jobs".into(),
            queues: vec!["high".into(), "default".into()],
            max_jobs: 1,
            wait_ms: 0,
            liveness: false,
            common: QueueCommon::default(),
        });
        let mut exec_us = 0;
        let s = store();
        run_pg_verb(&mut co, &req, &s, &s.table, deadline, cancel, &mut exec_us).await
    }

    /// Review F2 (mutation T7): a CANCEL that arrived before a statement is SENT answers the verb
    /// unsent — a known non-execution (`Retryable{ConnectionLost}`), never `Indeterminate` and never
    /// a statement sent after the client gave up.
    #[tokio::test]
    async fn a_cancel_before_the_statement_is_sent_answers_unsent() {
        let cancel = CancellationToken::new();
        let control = two_queue_reserve(None, &cancel).await;
        assert!(
            matches!(control, Ok(VerbOk::Reserve(ref j)) if j.is_empty()),
            "control: an uncancelled RESERVE runs both queues and answers empty"
        );
        cancel.cancel();
        let ep = two_queue_reserve(None, &cancel)
            .await
            .err()
            .unwrap_or_else(|| panic!("a cancelled RESERVE must not answer Ok"));
        assert_ne!(ep.branch, ferro_proto::consts::branch::INDETERMINATE);
        assert_eq!(
            (ep.code, ep.branch),
            (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
            "{ep:?}"
        );
    }

    /// Review F2 (mutation T8): a request whose deadline has already passed sends no statement and
    /// answers `PoolTimeout` — a write is never dispatched with no time left (which could only end
    /// `Indeterminate`).
    #[tokio::test]
    async fn no_time_left_answers_pool_timeout_unsent() {
        let past = tokio::time::Instant::now() - Duration::from_millis(5);
        let ep = two_queue_reserve(Some(past), &CancellationToken::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("an expired deadline must not answer Ok"));
        assert_eq!(ep.code, errc::POOL_TIMEOUT, "{ep:?}");
        assert_ne!(ep.branch, ferro_proto::consts::branch::INDETERMINATE);
    }

    /// One request of every verb, autocommit.
    fn every_verb() -> Vec<QueueRequest> {
        let token = Token::from_pg(1, 1).encode().to_vec();
        let fenced = FencedRequest {
            store: "jobs".into(),
            job_id: b"1".to_vec(),
            token: token.clone(),
            common: QueueCommon::default(),
        };
        let scope = QueueScopeRequest {
            store: "jobs".into(),
            queue: "default".into(),
            common: QueueCommon::default(),
        };
        vec![
            QueueRequest::Enqueue(EnqueueRequest {
                store: "jobs".into(),
                jobs: vec![EnqueueJob {
                    queue: "default".into(),
                    payload: "{}".into(),
                    delay_s: 0,
                }],
                dedup_key: None,
                common: QueueCommon::default(),
            }),
            QueueRequest::Reserve(ReserveRequest {
                store: "jobs".into(),
                queues: vec!["default".into()],
                max_jobs: 1,
                wait_ms: 0,
                liveness: false,
                common: QueueCommon::default(),
            }),
            QueueRequest::Ack(fenced.clone()),
            QueueRequest::Release(ReleaseRequest {
                store: "jobs".into(),
                job_id: b"1".to_vec(),
                token,
                delay_s: 0,
                common: QueueCommon::default(),
            }),
            QueueRequest::Extend(fenced),
            QueueRequest::Size(scope.clone()),
            QueueRequest::Clear(scope),
        ]
    }

    /// SPEC §24.6: `readonly` is true for SIZE ONLY — RESERVE writes `attempts` and a lease — and
    /// never `in_tx` on the autocommit path. A statement that ran but whose rows cannot be decoded is
    /// `Indeterminate` for every write (its effect is real but unreportable), and a plain refusal for
    /// SIZE.
    #[test]
    fn only_size_is_a_read_and_an_unreadable_write_result_is_indeterminate() {
        let s = store();
        for req in every_verb() {
            let is_size = matches!(req, QueueRequest::Size(_));
            for sent in [false, true] {
                let ctx = verb_context(&req, sent);
                assert_eq!(ctx.readonly, is_size, "{}", req.verb());
                assert_eq!(ctx.sent, sent);
                assert!(!ctx.in_tx);
            }
            let ep = malformed(&req, &s);
            if is_size {
                assert_eq!(ep.code, errc::UNSUPPORTED);
            } else {
                assert_eq!(
                    (ep.code, ep.branch),
                    (
                        errc::WRITE_UNCONFIRMED,
                        ferro_proto::consts::branch::INDETERMINATE
                    ),
                    "{}",
                    req.verb()
                );
            }
            assert!(ep.message.contains(req.verb()), "{}", ep.message);
        }
    }

    /// `LeaseLost` is the registry's known-fate code, and its message names the verb, never a token.
    #[test]
    fn lease_lost_is_the_known_fate_code() {
        let ep = lease_lost("jobs", "ACK", false);
        assert_eq!(
            (ep.code, ep.branch),
            (errc::LEASE_LOST, errc::LEASE_LOST_BRANCH)
        );
        assert_eq!(ep.branch, ferro_proto::consts::branch::NON_RETRYABLE);
        assert!(ep.message.contains("ACK did nothing"), "{}", ep.message);
    }

    #[test]
    fn every_registered_queue_method_decodes_to_its_own_verb() {
        let names: Vec<(&str, u16)> = method_queue::ALL.to_vec();
        assert_eq!(
            names.len(),
            7,
            "a new QUEUE method needs a decode arm and a verb"
        );
        for (name, id) in names {
            // An empty payload is malformed for every shape — but it must reach THAT shape's
            // decoder, never the "does not exist" arm.
            assert!(
                matches!(QueueRequest::decode(id, &[]), Some(Err(_))),
                "{name}"
            );
        }
        assert!(QueueRequest::decode(0, &[]).is_none());
        assert!(QueueRequest::decode(8, &[]).is_none());
    }

    // ---- M7-G2: tx-scoped verbs ---------------------------------------------------------------

    fn stores_arc() -> Arc<QueueStores> {
        let cfg = QueueConfig::load(
            [
                ("FERRO_QUEUE_STORES", "jobs"),
                ("FERRO_QUEUE_JOBS_POOL", "main"),
            ]
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &|_| Some(PoolFamily::Postgres),
        );
        Arc::new(QueueStores::new(Arc::new(cfg)))
    }

    fn in_tx(common: QueueCommon) -> QueueCommon {
        QueueCommon {
            tx_id: Some(7),
            ..common
        }
    }

    fn enqueue_of(queues: &[&str]) -> QueueRequest {
        QueueRequest::Enqueue(EnqueueRequest {
            store: "jobs".into(),
            jobs: queues
                .iter()
                .map(|q| EnqueueJob {
                    queue: (*q).into(),
                    payload: "{}".into(),
                    delay_s: 0,
                })
                .collect(),
            dedup_key: None,
            common: in_tx(QueueCommon::default()),
        })
    }

    fn release_of(delay_s: u32) -> QueueRequest {
        QueueRequest::Release(ReleaseRequest {
            store: "jobs".into(),
            job_id: b"9".to_vec(),
            token: Token::from_pg(100, 2).encode().to_vec(),
            delay_s,
            common: in_tx(QueueCommon::default()),
        })
    }

    fn qr(rows: Vec<Vec<ferro_proto::value::Value>>, affected: u64) -> QueryResult {
        QueryResult {
            rows,
            affected,
            ..QueryResult::default()
        }
    }

    fn int(n: i64) -> ferro_proto::value::Value {
        ferro_proto::value::Value::I64(n)
    }

    fn text(s: &str) -> ferro_proto::value::Value {
        ferro_proto::value::Value::Text(s.into())
    }

    /// The step each tx-scoped verb runs is the closed builder set's — the in-transaction ACK and
    /// RELEASE are the probe-less forms (R1), the rest are the autocommit statements — and RESERVE
    /// cannot be built at all.
    #[test]
    fn a_tx_verb_runs_the_in_tx_statement_of_its_verb() {
        let s = store();
        let t = &s.table;
        let id = JobId(9);
        let tok = Token::from_pg(100, 2);
        let fenced_tx = |r: QueueRequest| r;
        let ack = fenced_tx(QueueRequest::Ack(FencedRequest {
            store: "jobs".into(),
            job_id: b"9".to_vec(),
            token: tok.encode().to_vec(),
            common: in_tx(QueueCommon::default()),
        }));
        let extend = QueueRequest::Extend(FencedRequest {
            store: "jobs".into(),
            job_id: b"9".to_vec(),
            token: tok.encode().to_vec(),
            common: in_tx(QueueCommon::default()),
        });
        let scope = QueueScopeRequest {
            store: "jobs".into(),
            queue: "q".into(),
            common: in_tx(QueueCommon::default()),
        };
        let cases = [
            (
                enqueue_of(&["a"]),
                pgq::enqueue(
                    t,
                    vec![EnqueueJob {
                        queue: "a".into(),
                        payload: "{}".into(),
                        delay_s: 0,
                    }],
                ),
            ),
            (ack, pgq::ack_in_tx(t, id, tok)),
            (release_of(5), pgq::release_in_tx(t, id, tok, 5)),
            (extend, pgq::extend(t, id, tok, s.lease_s)),
            (QueueRequest::Size(scope.clone()), pgq::size(t, "q")),
            (QueueRequest::Clear(scope), pgq::clear(t, "q")),
        ];
        for (req, want) in cases {
            let verb = TxVerb::new(&req, &s, &s.table, stores_arc()).unwrap();
            assert_eq!(verb.steps, vec![want], "{}", req.verb());
            // The Debug never prints a statement (a payload is a parameter).
            assert!(!format!("{verb:?}").contains("{}"), "{verb:?}");
        }
        let reserve = QueueRequest::Reserve(ReserveRequest {
            store: "jobs".into(),
            queues: vec!["q".into()],
            max_jobs: 1,
            wait_ms: 0,
            liveness: false,
            common: in_tx(QueueCommon::default()),
        });
        assert_eq!(
            TxVerb::new(&reserve, &s, &s.table, stores_arc())
                .unwrap_err()
                .code,
            errc::UNSUPPORTED
        );
    }

    /// SPEC D25: an in-transaction write whose effect cannot be reported is rolled back and its
    /// terminal is NonRetryable, saying plainly that retrying cannot help.
    fn assert_d25(ep: &ErrorPayload) {
        assert_eq!(
            (ep.code, ep.branch),
            (errc::UNSUPPORTED, errc::UNSUPPORTED_BRANCH),
            "{ep:?}"
        );
        assert_eq!(ep.branch, ferro_proto::consts::branch::NON_RETRYABLE);
        assert!(
            ep.message.contains("Retrying cannot help") && ep.message.contains("rolled back"),
            "{}",
            ep.message
        );
    }

    fn verdict_kind(v: &TxVerdict) -> (&'static str, usize) {
        match v {
            TxVerdict::Applied { wake, .. } => ("applied", wake.len()),
            TxVerdict::NoEffect(Ok(_)) => ("read", 0),
            TxVerdict::NoEffect(Err(ep)) if ep.code == errc::LEASE_LOST => ("lease_lost", 0),
            TxVerdict::NoEffect(Err(ep)) if ep.code == errc::UNSUPPORTED => ("refused", 0),
            TxVerdict::NoEffect(Err(_)) => ("other", 0),
            TxVerdict::Unreadable(ep) => {
                assert_d25(ep);
                ("rolled_back", 0)
            }
        }
    }

    /// SPEC §24.4/§24.5/§24.6 for every tx-scoped verb: an applied verb keeps its wake hints (one per
    /// distinct ENQUEUE queue; a RELEASE's only at `delay_s = 0`); an unmatched fence is ALWAYS
    /// `LeaseLost` in a transaction (R1), with the "roll it back" message; an unreadable WRITE result
    /// rolls the transaction back (`TxDeadline{Retryable}`) while an unreadable SIZE is only refused.
    #[test]
    fn a_tx_verbs_decode_keeps_hints_only_when_applied_and_never_answers_gone() {
        let s = store();
        let verb = |req: QueueRequest| TxVerb::new(&req, &s, &s.table, stores_arc()).unwrap();
        let fenced = |ack: bool| {
            let r = FencedRequest {
                store: "jobs".into(),
                job_id: b"9".to_vec(),
                token: Token::from_pg(100, 2).encode().to_vec(),
                common: in_tx(QueueCommon::default()),
            };
            if ack {
                QueueRequest::Ack(r)
            } else {
                QueueRequest::Extend(r)
            }
        };
        let scope = |clear: bool| {
            let r = QueueScopeRequest {
                store: "jobs".into(),
                queue: "q".into(),
                common: in_tx(QueueCommon::default()),
            };
            if clear {
                QueueRequest::Clear(r)
            } else {
                QueueRequest::Size(r)
            }
        };
        let e = verb(enqueue_of(&["b", "a", "b"]));
        assert_eq!(
            verdict_kind(&e.decode(&[qr(vec![], 3)])),
            ("applied", 2),
            "one hint per DISTINCT queue"
        );
        assert_eq!(
            verdict_kind(&verb(enqueue_of(&["a"])).decode(&[qr(vec![vec![int(41)]], 1)])),
            ("applied", 1)
        );
        assert_eq!(
            verdict_kind(&e.decode(&[qr(vec![], 2)])),
            ("rolled_back", 0)
        );
        assert_eq!(
            verdict_kind(&e.decode(&[qr(vec![], 3), qr(vec![], 3)])),
            ("rolled_back", 0),
            "a result per step, and one step"
        );
        assert_eq!(verdict_kind(&e.decode(&[])), ("rolled_back", 0));

        let a = verb(fenced(true));
        assert_eq!(verdict_kind(&a.decode(&[qr(vec![], 1)])), ("applied", 0));
        let lost = a.decode(&[qr(vec![], 0)]);
        assert_eq!(verdict_kind(&lost), ("lease_lost", 0), "absent → LeaseLost");
        let TxVerdict::NoEffect(Err(ep)) = lost else {
            unreachable!()
        };
        assert!(ep.message.contains("roll it back"), "{}", ep.message);
        assert_eq!(
            verdict_kind(&a.decode(&[qr(vec![], 2)])),
            ("rolled_back", 0)
        );

        let now = verb(release_of(0));
        let later = verb(release_of(30));
        let moved = [qr(vec![vec![int(55), text("emails")]], 1)];
        assert_eq!(verdict_kind(&now.decode(&moved)), ("applied", 1));
        assert_eq!(
            verdict_kind(&later.decode(&moved)),
            ("applied", 0),
            "a delayed job wakes nobody"
        );
        assert_eq!(
            verdict_kind(&now.decode(&[qr(vec![], 0)])),
            ("lease_lost", 0)
        );
        assert_eq!(
            verdict_kind(&now.decode(&[qr(vec![vec![int(55)]], 1)])),
            ("rolled_back", 0)
        );

        let x = verb(fenced(false));
        assert_eq!(
            verdict_kind(&x.decode(&[qr(vec![vec![int(1091)]], 1)])),
            ("applied", 0)
        );
        assert_eq!(verdict_kind(&x.decode(&[qr(vec![], 0)])), ("lease_lost", 0));
        assert_eq!(
            verdict_kind(&x.decode(&[qr(vec![vec![text("x")]], 1)])),
            ("rolled_back", 0)
        );

        let size = verb(scope(false));
        assert_eq!(
            verdict_kind(&size.decode(&[qr(
                vec![vec![
                    int(1),
                    int(0),
                    int(0),
                    ferro_proto::value::Value::Null
                ]],
                1
            )])),
            ("read", 0)
        );
        assert_eq!(
            verdict_kind(&size.decode(&[qr(vec![], 0)])),
            ("refused", 0),
            "an unreadable READ is refused, the transaction stays"
        );
        assert_eq!(
            verdict_kind(&verb(scope(true)).decode(&[qr(vec![], 4)])),
            ("applied", 0)
        );
    }

    /// The tx-scoped `OpContext`: SIZE alone is a read, and every verb is `in_tx`.
    #[test]
    fn a_tx_scoped_verb_is_classified_in_tx() {
        for req in every_verb() {
            for sent in [false, true] {
                let ctx = tx_verb_context(&req, sent);
                assert!(ctx.in_tx, "{}", req.verb());
                assert_eq!(ctx.sent, sent);
                assert_eq!(ctx.readonly, matches!(req, QueueRequest::Size(_)));
            }
        }
        // A link lost under an in-tx write is Retryable (the transaction is dead), never Indeterminate.
        let enq = enqueue_of(&["a"]);
        let ep = fate::classify_fate(PoolError::ConnectionLost, tx_verb_context(&enq, true));
        assert_eq!(
            (ep.code, ep.branch),
            (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH)
        );
    }

    #[test]
    fn pool_mismatch_is_the_known_fate_code_and_names_both_pools() {
        let ep = pool_mismatch(&store(), "other");
        assert_eq!(
            (ep.code, ep.branch),
            (errc::POOL_MISMATCH, errc::POOL_MISMATCH_BRANCH)
        );
        assert_eq!(ep.branch, ferro_proto::consts::branch::NON_RETRYABLE);
        assert!(ep.message.contains("pool main"), "{}", ep.message);
        assert!(ep.message.contains("pool other"), "{}", ep.message);
    }

    /// One transaction actor on a one-connection `FakeBackend` pool, exactly as `begin_on_pool`
    /// spawns it.
    async fn spawn_fake_tx(
        pool: &Pool<ferro_pool::fake::FakeBackend>,
        registry: &TxRegistry,
        owner: SessionId,
    ) -> (u64, tokio::sync::mpsc::Sender<TxCommand>) {
        let mut co = pool.checkout().await.expect("checkout");
        let tx_id = crate::tx::next_tx_id();
        co.begin_tx_with(ferro_pool::pin::TxId(tx_id), "BEGIN")
            .await
            .expect("begin");
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let abort = CancellationToken::new();
        registry.register(
            tx_id,
            crate::tx::TxHandle {
                owner,
                cmd_tx: cmd_tx.clone(),
                abort: abort.clone(),
                done: done_rx,
                streaming: true,
                copy: true,
                pool: "main".into(),
                dialect: ferro_classify::Dialect::Postgres,
            },
        );
        tokio::spawn(crate::tx::actor::run(
            tx_id,
            co,
            cmd_rx,
            abort,
            done_tx,
            registry.clone(),
            Duration::from_secs(600),
            Duration::from_secs(600),
            Duration::from_secs(5),
        ));
        (tx_id, cmd_tx)
    }

    async fn queue_cmd(
        cmd_tx: &tokio::sync::mpsc::Sender<TxCommand>,
        req: &QueueRequest,
        stores: &Arc<QueueStores>,
    ) -> QueueReply {
        let (reply, rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(TxCommand::Queue {
                verb: Box::new({
                    let s = store();
                    TxVerb::new(req, &s, &s.table, Arc::clone(stores)).unwrap()
                }),
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply,
            })
            .await
            .unwrap();
        rx.await.expect("the actor replies")
    }

    async fn ctl(cmd_tx: &tokio::sync::mpsc::Sender<TxCommand>, commit: bool) -> bool {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let cmd = if commit {
            TxCommand::Commit { reply }
        } else {
            TxCommand::Rollback { reply }
        };
        cmd_tx.send(cmd).await.unwrap();
        matches!(rx.await.unwrap(), crate::tx::CtlReply::Ok)
    }

    /// SPEC §24.5 step 4 through the REAL actor: a verb's wake hint fires only on a successful COMMIT
    /// (never on ROLLBACK, never for a verb that failed or did nothing), and a WRITE whose result is
    /// unreadable rolls the transaction back and tombstones it.
    #[tokio::test]
    async fn the_actor_fires_hints_only_on_commit_and_rolls_back_an_unreadable_write() {
        use ferro_pool::config::PoolConfig;
        use ferro_pool::fake::FakeBackend;
        let backend = FakeBackend::new();
        let pool = Pool::new(
            backend,
            PoolConfig {
                max_size: 1,
                reap_interval: None,
                ..PoolConfig::default()
            },
        );
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let stores = stores_arc();
        let one_id = qr(vec![vec![int(41)]], 1);

        // Applied, then COMMIT: the hint fires — once, and only at COMMIT.
        pool.backend().set_query_result(one_id.clone());
        let (_, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        let r = queue_cmd(&tx, &enqueue_of(&["a"]), &stores).await;
        assert!(
            matches!(
                r,
                QueueReply::Done {
                    outcome: Ok(VerbOk::Enqueue(_)),
                    ..
                }
            ),
            "{r:?}"
        );
        assert_eq!(stores.wake_hints(), 0, "nothing fires before COMMIT");
        assert!(ctl(&tx, true).await);
        assert_eq!(stores.wake_hints(), 1);

        // Applied, then ROLLBACK: dropped.
        let (_, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        queue_cmd(&tx, &enqueue_of(&["a"]), &stores).await;
        assert!(ctl(&tx, false).await);
        assert_eq!(stores.wake_hints(), 1, "a rollback fires nothing");

        // A failed step: no hint, the transaction stays registered, then COMMIT fires nothing.
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        pool.backend().arm_next_query_err(PoolError::Sql {
            code: errc::NOT_NULL,
            branch: errc::NOT_NULL_BRANCH,
            sqlstate: Some("23502".into()),
            errno: None,
            message: "null value".into(),
        });
        let r = queue_cmd(&tx, &enqueue_of(&["a"]), &stores).await;
        assert!(matches!(r, QueueReply::Failed { .. }), "{r:?}");
        assert!(registry.lookup(tx_id, owner).is_ok(), "still open");
        assert!(ctl(&tx, true).await);
        assert_eq!(stores.wake_hints(), 1, "a failed verb keeps no hint");

        // An in-tx LeaseLost: no hint, the transaction stays open.
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        pool.backend().set_query_result(qr(vec![], 0));
        let ack = QueueRequest::Ack(FencedRequest {
            store: "jobs".into(),
            job_id: b"9".to_vec(),
            token: Token::from_pg(100, 2).encode().to_vec(),
            common: in_tx(QueueCommon::default()),
        });
        let r = queue_cmd(&tx, &ack, &stores).await;
        assert!(
            matches!(r, QueueReply::Done { outcome: Err(ref ep), .. } if ep.code == errc::LEASE_LOST),
            "{r:?}"
        );
        assert!(
            registry.lookup(tx_id, owner).is_ok(),
            "LeaseLost leaves the transaction open"
        );
        assert!(ctl(&tx, true).await);
        assert_eq!(stores.wake_hints(), 1);

        // An unreadable WRITE result: rolled back + tombstoned, its hint never kept.
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        pool.backend().set_query_result(qr(vec![], 0)); // an ENQUEUE of one job returns its id
        let r = queue_cmd(&tx, &enqueue_of(&["a"]), &stores).await;
        let QueueReply::RolledBack(ep) = r else {
            panic!("expected RolledBack, got {r:?}")
        };
        assert_d25(&ep);
        wait_tombstoned(&registry, tx_id, owner).await;
        let co = pool.checkout().await.expect("the connection came back");
        assert!(
            co.conn().recorded.iter().any(|s| s == "ROLLBACK"),
            "{:?}",
            co.conn().recorded
        );
        drop(co);
        assert_eq!(stores.wake_hints(), 1);
    }

    async fn wait_tombstoned(registry: &TxRegistry, tx_id: u64, owner: SessionId) {
        let mut waited = 0;
        while registry.lookup(tx_id, owner).is_ok() && waited < 200 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            waited += 1;
        }
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            crate::tx::TxLookupErr::Tombstoned
        );
    }

    fn one_conn_pool() -> Pool<ferro_pool::fake::FakeBackend> {
        Pool::new(
            ferro_pool::fake::FakeBackend::new(),
            ferro_pool::config::PoolConfig {
                max_size: 1,
                reap_interval: None,
                ..ferro_pool::config::PoolConfig::default()
            },
        )
    }

    fn size_in_tx() -> QueueRequest {
        QueueRequest::Size(QueueScopeRequest {
            store: "jobs".into(),
            queue: "q".into(),
            common: in_tx(QueueCommon::default()),
        })
    }

    /// SPEC D25, second half (review F2/O-G2): a WRITE that failed AFTER it was sent with an error
    /// that is not the backend's SQL answer — here a result the driver could not decode
    /// (`PoolError::Backend`) — may be applied inside the open transaction, so it is rolled back and
    /// tombstoned with D25's NonRetryable terminal, never answered `Failed` with the transaction open.
    /// The controls: the same error on SIZE (a read) and a lost link on a write (the backend ends
    /// the transaction itself) take the `Failed` arm and leave the transaction registered.
    #[tokio::test]
    async fn a_non_sql_error_after_a_write_was_sent_rolls_back_never_leaves_the_tx_open() {
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let stores = stores_arc();
        let backend_err = || PoolError::Backend("row decode failed".into());

        let pool = one_conn_pool();
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        pool.backend().arm_next_query_err(backend_err());
        let r = queue_cmd(&tx, &enqueue_of(&["a"]), &stores).await;
        let QueueReply::RolledBack(ep) = r else {
            panic!("expected RolledBack, got {r:?}")
        };
        assert_d25(&ep);
        wait_tombstoned(&registry, tx_id, owner).await;
        let co = pool.checkout().await.unwrap();
        assert!(co.conn().recorded.iter().any(|s| s == "ROLLBACK"));
        drop(co);

        let pool = one_conn_pool();
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        pool.backend().arm_next_query_err(backend_err());
        let r = queue_cmd(&tx, &size_in_tx(), &stores).await;
        assert!(matches!(r, QueueReply::Failed { .. }), "a read: {r:?}");
        assert!(
            registry.lookup(tx_id, owner).is_ok(),
            "a read leaves the tx open"
        );
        assert!(ctl(&tx, false).await);

        let pool = one_conn_pool();
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        pool.backend().arm_next_query_err(PoolError::ConnectionLost);
        let r = queue_cmd(&tx, &enqueue_of(&["a"]), &stores).await;
        assert!(
            matches!(
                r,
                QueueReply::Failed {
                    error: PoolError::ConnectionLost,
                    ..
                }
            ),
            "a lost link keeps §24.6's Retryable: {r:?}"
        );
        assert!(registry.lookup(tx_id, owner).is_ok());
        assert_eq!(stores.wake_hints(), 0);
    }

    /// Review F4: a CANCEL observed before the verb is dispatched answers `Cancelled` with NOTHING
    /// sent and the transaction intact (it commits afterwards, the verb's hint never kept).
    #[tokio::test]
    async fn a_cancel_before_dispatch_answers_cancelled_and_leaves_the_tx_intact() {
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let stores = stores_arc();
        let pool = one_conn_pool();
        pool.backend().set_query_result(qr(vec![vec![int(41)]], 1));
        let (tx_id, tx) = spawn_fake_tx(&pool, &registry, owner).await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (reply, rx) = tokio::sync::oneshot::channel();
        tx.send(TxCommand::Queue {
            verb: Box::new({
                let s = store();
                TxVerb::new(&enqueue_of(&["a"]), &s, &s.table, Arc::clone(&stores)).unwrap()
            }),
            timeout_ms: None,
            cancel,
            reply,
        })
        .await
        .unwrap();
        let r = rx.await.unwrap();
        assert!(
            matches!(r, QueueReply::Done { outcome: Err(ref ep), exec_us: 0 }
                if ep.code == errc::CANCELLED && ep.branch == errc::CANCELLED_BRANCH),
            "{r:?}"
        );
        assert!(
            registry.lookup(tx_id, owner).is_ok(),
            "the transaction is intact"
        );
        assert!(ctl(&tx, true).await, "and commits");
        assert_eq!(stores.wake_hints(), 0);
        let co = pool.checkout().await.unwrap();
        assert!(
            !co.conn().recorded.iter().any(|s| s.contains("INSERT")),
            "nothing was sent: {:?}",
            co.conn().recorded
        );
    }
}
