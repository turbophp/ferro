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
//! 5. a `tx_id`: RESERVE in a transaction is refused for good (§24.5); every other tx-scoped verb is
//!    refused until slice G2 builds the TX-actor path;
//! 6. a MySQL/MariaDB store is refused until slice G6;
//! 7. **first use per `boot_epoch`:** the version gate against the pool's existing version probe
//!    (the wait bounded by the request's own deadline and CANCEL), then shape verification — ONE
//!    catalog read (`to_regclass` on the quoted identifier, so `search_path` is honoured; the
//!    relation's kind; its columns by oid) — the FIRST statement, a read, through the guarded
//!    `Checkout::query` with the request's own `timeout_ms` and CANCEL, classified by the shared fate
//!    matrix as a read (never `Indeterminate`). A definitive verdict is cached for the process — one
//!    process is one `boot_epoch` — except a gate refusal (reused for the version probe's TTL) and an
//!    absent table (reused for [`ABSENT_RECHECK`]) (SPEC §24.3 amendment);
//! 8. every verb then answers `Unsupported` ("not served before slice G1b"): the seven PostgreSQL
//!    verbs, the fence and the clock rules are G1b's (SPEC §24.14).
//!
//! Never logged or put in a terminal: a payload, a dedup key, a token or a DSN.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ferro_pool::backend::PoolBackend;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;
use ferro_proto::consts::{errc, method_queue};
use ferro_proto::messages::{
    EnqueueRequest, ErrorPayload, FencedRequest, QueueCommon, QueueScopeRequest, ReleaseRequest,
    ReserveRequest,
};
use ferro_queue::config::{QueueConfig, StoreConfig, StoreKind};
use ferro_queue::shape::{self, PgRelation, ShapeError};
use ferro_queue::sql::{JobId, Token, Undecodable};
use ferro_queue::{PoolFamily, checks, version};
use tokio_util::sync::CancellationToken;

use crate::pools::{AnyPool, PoolRegistry};
use crate::services::fate::{self, OpContext};
use crate::services::sql::{
    cancelled_before_dispatch, protocol, run_autocommit_exec, sleep_until_opt, unsupported,
};
use crate::session::codec::InFrame;
use crate::session::responder::Responder;

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
    Verified,
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
}

impl QueueStores {
    pub fn new(config: Arc<QueueConfig>) -> QueueStores {
        let verdicts = config
            .entries()
            .filter_map(|(name, _)| config.store(name).map(|s| s.name.clone()))
            .map(|name| (name, tokio::sync::Mutex::new(Verdict::Unverified)))
            .collect();
        QueueStores {
            config,
            verdicts,
            absent_recheck: ABSENT_RECHECK,
            gate_recheck: None,
            verifications: AtomicU64::new(0),
            unknown_version_warned: std::sync::Mutex::new(HashMap::new()),
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

/// The `sql` kind's decode of a fenced verb's handles (SPEC §24.3), before any statement. G1b's
/// statement builders consume the decoded values; G1a only proves they decode.
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

/// Steps 4–6 of the module doc: every refusal made before a checkout.
fn refuse_before_checkout(req: &QueueRequest, store: &StoreConfig) -> Result<(), ErrorPayload> {
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
        QueueRequest::Enqueue(r) => checks::enqueue(r, store).map_err(refusal)?,
        QueueRequest::Reserve(r) => checks::reserve(r).map_err(refusal)?,
        QueueRequest::Ack(r) | QueueRequest::Extend(r) => {
            decode_handles(store, &r.job_id, &r.token)?;
        }
        QueueRequest::Release(r) => {
            decode_handles(store, &r.job_id, &r.token)?;
        }
        QueueRequest::Size(r) | QueueRequest::Clear(r) => {
            checks::queue_name(&r.queue).map_err(refusal)?
        }
    }
    if req.common().tx_id.is_some() {
        return Err(unsupported(format!(
            "tx-scoped QUEUE {} is not served before slice G2 (SPEC §24.14)",
            req.verb()
        )));
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
pub async fn handle(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
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
    // consumers (the span and the slow log, §24.9) land with the verbs.
    let _trace = crate::trace::from_request(req.common().traceparent.as_deref());

    let Some(stores) = registry.queue() else {
        responder.end_error(unsupported(
            "Ferro Queue is not configured on this daemon (FERRO_QUEUE_STORES is unset)",
        ));
        return;
    };
    let Some(store) = stores.config.store(req.store()) else {
        // One answer for an unknown store and one refused at configuration (the store's name is the
        // client's own; the reason is in the daemon's log).
        responder.end_error(unsupported(
            "unknown queue store (or one refused at configuration; see the ferrod log)",
        ));
        return;
    };
    if let Err(ep) = refuse_before_checkout(&req, store) {
        responder.end_error(ep);
        return;
    }
    if let Err(ep) =
        ensure_verified(stores, store, registry, req.common().timeout_ms, &cancel).await
    {
        responder.end_error(ep);
        return;
    }
    responder.end_error(unsupported(format!(
        "QUEUE {} is not served before slice G1b (SPEC §24.14); store {} is verified",
        req.verb(),
        store.name
    )));
}

/// The version gate and shape verification, once per store per process (SPEC §24.3).
async fn ensure_verified(
    stores: &QueueStores,
    store: &StoreConfig,
    registry: &PoolRegistry,
    timeout_ms: Option<u32>,
    cancel: &CancellationToken,
) -> Result<(), ErrorPayload> {
    let Some(cell) = stores.verdicts.get(&store.name) else {
        return Err(unsupported("unknown queue store"));
    };
    // ONE deadline for the whole first use — the wait for another request's verification, the
    // checkout and the statement — as on EXEC (M3-D1c review F1).
    let deadline =
        timeout_ms.map(|ms| tokio::time::Instant::now() + Duration::from_millis(u64::from(ms)));
    let not_sent = |e: PoolError| fate::classify_fate(e, verification_context(false));
    let mut verdict = tokio::select! {
        biased;
        g = cell.lock() => g,
        () = sleep_until_opt(deadline) => return Err(not_sent(PoolError::Timeout)),
        () = cancel.cancelled() => return Err(not_sent(cancelled_before_dispatch())),
    };
    match &*verdict {
        Verdict::Verified => return Ok(()),
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
        Ok(schema) => {
            tracing::info!(store = %store.name, table = %store.table, schema = %schema, "ferrod: queue store verified");
            *verdict = Verdict::Verified;
            Ok(())
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
) -> Result<String, Verification> {
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
        stmt.sql,
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
    let schema = relation.map(|r| r.schema).unwrap_or_default();

    // Diagnostics only (SPEC §24.3): a table whose `id` has no sequence default is logged, never
    // refused — ENQUEUE would then fail with `NotNull`, classified like any statement.
    let serial = shape::pg_serial_statement(&store.table);
    if let Ok(left) = remaining(deadline) {
        let (r, _) = run_autocommit_exec(&mut co, serial.sql, &serial.params, left, cancel).await;
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
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::messages::EnqueueJob;
    use std::ffi::OsString;

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
        assert!(refuse_before_checkout(&fenced(b"42", &good_token), &store()).is_ok());
        for (job_id, token) in [
            (&b"042"[..], &good_token[..]),
            (b"+42", &good_token),
            (b"abc", &good_token),
            (b"42", &[0u8; 7][..]),
            (b"42", &[1u8; 8][..]),
        ] {
            let ep = refuse_before_checkout(&fenced(job_id, token), &store()).unwrap_err();
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
            refuse_before_checkout(&rel, &store()).unwrap_err().code,
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
        let ep = refuse_before_checkout(&reserve, &store()).unwrap_err();
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
        let ep = refuse_before_checkout(&enq, &store()).unwrap_err();
        assert!(ep.message.contains("G2"), "{}", ep.message);
        let mut my = store();
        my.family = PoolFamily::Mysql;
        let size = QueueRequest::Size(QueueScopeRequest {
            store: "jobs".into(),
            queue: "default".into(),
            common: QueueCommon::default(),
        });
        assert!(refuse_before_checkout(&size, &store()).is_ok());
        let ep = refuse_before_checkout(&size, &my).unwrap_err();
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
        let ep = refuse_before_checkout(&reserve, &store()).unwrap_err();
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
            Some(150),
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
}
