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
//! 7. **first use per `boot_epoch`:** the version gate against the pool's existing version probe,
//!    then shape verification over `information_schema` — the FIRST statement, a read, through the
//!    guarded `Checkout::query` with the request's own `timeout_ms` and CANCEL, classified by the
//!    shared fate matrix as a read (never `Indeterminate`). A definitive verdict is cached for the
//!    process — one process is one `boot_epoch` — and an absent table is not (SPEC §24.3 amendment);
//! 8. every verb then answers `Unsupported` ("not served before slice G1b"): the seven PostgreSQL
//!    verbs, the fence and the clock rules are G1b's (SPEC §24.14).
//!
//! Never logged or put in a terminal: a payload, a dedup key, a token or a DSN.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ferro_pool::backend::PoolBackend;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;
use ferro_proto::consts::{errc, method_queue};
use ferro_proto::messages::{
    EnqueueRequest, ErrorPayload, FencedRequest, QueueCommon, QueueScopeRequest, ReleaseRequest,
    ReserveRequest,
};
use ferro_queue::config::{QueueConfig, StoreConfig, StoreKind};
use ferro_queue::shape::{self, ColumnRow, ShapeError};
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

/// A store's first-use verdict (SPEC §24.3), cached for the process — one process is one
/// `boot_epoch`, and `ferrod` has no configuration reload in v1.
#[derive(Debug, Clone)]
enum Verdict {
    Unverified,
    Verified,
    /// A definitive refusal (the version gate, or a table of the wrong shape): every verb on the
    /// store answers `Unsupported` with this message until the next restart.
    Refused(String),
}

/// The configured stores plus each store's verification state.
pub struct QueueStores {
    config: Arc<QueueConfig>,
    /// One per ENABLED store. A `tokio` mutex, held across the verification's awaits, so concurrent
    /// first requests verify ONCE and the rest read the verdict.
    verdicts: HashMap<String, tokio::sync::Mutex<Verdict>>,
}

impl QueueStores {
    pub fn new(config: Arc<QueueConfig>) -> QueueStores {
        let verdicts = config
            .entries()
            .filter_map(|(name, _)| config.store(name).map(|s| s.name.clone()))
            .map(|name| (name, tokio::sync::Mutex::new(Verdict::Unverified)))
            .collect();
        QueueStores { config, verdicts }
    }

    pub fn config(&self) -> &QueueConfig {
        &self.config
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
        return Err(match req {
            // Permanent (SPEC §24.5): a wait would hold the pin, and a rollback would void a
            // delivered token.
            QueueRequest::Reserve(_) => unsupported(
                "RESERVE inside a transaction is refused (SPEC §24.5): send it without a tx_id",
            ),
            _ => unsupported(format!(
                "tx-scoped QUEUE {} is not served before slice G2 (SPEC §24.14)",
                req.verb()
            )),
        });
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
        Verdict::Unverified => {}
    }

    // The version gate, against the pool's existing probe.
    let Some(server_version) = registry.server_version(&store.pool).await else {
        return Err(ErrorPayload {
            code: errc::CONNECTION_LOST,
            branch: errc::CONNECTION_LOST_BRANCH,
            sqlstate: None,
            errno: None,
            message: format!(
                "queue store {}: the server version of its pool is not known (is the backend \
                 reachable?), so the version gate cannot pass; nothing was sent",
                store.name
            ),
            detail: None,
            retry_after_ms: None,
        });
    };
    if let Err(refusal) = version::gate(store.family, &server_version) {
        let message = format!("queue store {}: {refusal}", store.name);
        tracing::error!(store = %store.name, refusal = %refusal, "ferrod: queue store refused by the version gate");
        *verdict = Verdict::Refused(message.clone());
        return Err(unsupported(message));
    }

    let outcome = match registry.get(&store.pool) {
        Some(AnyPool::Pg(pool)) => verify_pg(pool, store, deadline, cancel).await,
        // Unreachable: `refuse_before_checkout` refused the MySQL family and configuration refused
        // SQLite. Answered rather than asserted, so a future family cannot panic a session.
        Some(_) | None => Err(Verification::Refused(unsupported(format!(
            "queue store {}: its pool cannot be verified in this build",
            store.name
        )))),
    };
    match outcome {
        Ok(()) => {
            tracing::info!(store = %store.name, table = %store.table, "ferrod: queue store verified");
            *verdict = Verdict::Verified;
            Ok(())
        }
        Err(Verification::Shape(e)) => {
            let message = format!("queue store {}: {e}", store.name);
            if e.is_definitive() {
                tracing::error!(store = %store.name, refusal = %e, "ferrod: queue store refused by shape verification");
                *verdict = Verdict::Refused(message.clone());
            }
            Err(unsupported(message))
        }
        Err(Verification::Refused(ep)) => Err(ep),
    }
}

/// The fate context of every statement first-use verification runs: a READ (`information_schema`, the
/// identity probe), autocommit. So a lost or cancelled verification is never `Indeterminate`, whatever
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
    store: &StoreConfig,
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
) -> Result<(), Verification> {
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

    let stmt = shape::pg_columns_statement(&store.table);
    let (result, _exec_us) = run_autocommit_exec(
        &mut co,
        stmt.sql,
        &stmt.params,
        remaining(deadline)?,
        cancel,
    )
    .await;
    let qr = result.map_err(|e| Verification::Refused(fate::classify_fate(e, ctx(true))))?;
    let mut rows = Vec::with_capacity(qr.rows.len());
    for row in &qr.rows {
        let Some(r) = ColumnRow::from_values(row) else {
            return Err(Verification::Refused(unsupported(format!(
                "queue store {}: information_schema returned an unexpected row shape",
                store.name
            ))));
        };
        rows.push(r);
    }
    shape::verify_pg(&store.table, &rows).map_err(Verification::Shape)?;

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
    Ok(())
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
        let mut config = crate::config::Config {
            pools: vec![crate::config::PoolSpec {
                name: "main".into(),
                dsn: "postgres://ferro:ferro@127.0.0.1:1/ferro".into(),
                kind: crate::config::PoolKind::Postgres,
                pin_functions: Vec::new(),
                pin_on_unknown: true,
                allow_dir: None,
            }],
            ..crate::config::Config::default()
        };
        let vars = [
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "main"),
        ]
        .map(|(k, v)| (OsString::from(k), OsString::from(v)));
        config.queue = Some(Arc::new(crate::queue_config::load(vars, &config)));
        PoolRegistry::build(&config)
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
        // Cached: even a version the gate would pass is not consulted again in this process.
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
