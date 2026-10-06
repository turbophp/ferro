//! The Ferro HTTP engine, HTTP/1.1 plaintext path (SPEC §23.6; slices M6-F4a and M6-F4b).
//!
//! [`HttpEngine::serve_request`] runs one `REQUEST` through §23.6's lifecycle and returns its ONE
//! terminal ([`Terminal`]); the caller (`ferrod`'s HTTP service) declares it, so exactly one `END`
//! follows every request (charter rule 4). Response frames go out through a [`ResponseSink`], the
//! seam that keeps this crate free of `ferrod`'s session types.
//!
//! 1. **Validate** (§23.4, F3) and compute effective idempotency (§23.7.2). A refusal is
//!    `Forbidden` with its `forbidden_*` cause, made before anything is dialled.
//! 2. **Bounds** (§23.8.4): the total deadline runs from DECODE (`started`); each bound is
//!    `min(request field ?? upstream default, upstream ceiling)` — `read_timeout_ms` has no ceiling,
//!    `READ_TIMEOUT_MS` being only its default.
//! 3. **Admit** (§23.6 step 4, F4b): (a) the daemon's drain — a request that arrives while it is
//!    draining is refused `draining`; (d) the body budget ([`budget`]) — over either account,
//!    `body_budget`. Breaker, rate limit and the queue (b, c, e) are slice F6's.
//! 4. **Acquire** (step 5): reuse an idle connection (§23.8.2) or dial: DNS, the address guard,
//!    TCP to the checked `SocketAddr` (§23.8.5). A deadline or `CANCEL` here is "before dispatch".
//! 5. **Dispatch** (step 6): arm the write tracker, hand the request to `hyper`.
//! 6. **Head** (step 7): a 101 is `informational_101`, a head over [`MAX_HEAD_BYTES`] by the engine's
//!    exact measure is `oversize_head`; both are "sent, no head". Otherwise one `HEAD` frame — with
//!    `content-encoding`/`content-length` moved into `decoded` when the engine decodes (§23.9.2).
//! 7. **Body** (step 8): `BODY` frames of at most 256 KiB, each a credit debit; while credit is
//!    exhausted the sink parks and the engine stops polling the body (and stops DECODING it), so
//!    `hyper` stops reading and the upstream sees TCP backpressure (P7).
//! 8. **Terminal** (step 9): `HttpDone`, an error from [`fate::classify`], or `Cancelled`.
//!
//! **The engine never re-sends a request** (charter rule 3, §23.7): a dispatched request is never
//! moved to another connection, and a failed dial is not a retry (no byte of the request exists
//! anywhere yet). After a cancel, a timeout or any failure the connection is discarded, never
//! drained (§23.8.2), and `sent` is read only after `hyper`'s connection task is gone (§23.7.1) —
//! `hyper` keeps flushing a buffered request even after it has delivered a head-read error
//! (`track.rs`'s `hyper_keeps_writing_the_request_after_delivering_a_head_error`), so every arm
//! discards BEFORE it reads `sent`.
//!
//! **The drain (§23.6.1, F4b).** `ferrod` hands every request a [`DrainView`] of the daemon's drain
//! signal. A request admitted before the drain keeps running until its own terminal or
//! `FERRO_HTTP_DRAIN_MS` after the drain began, whichever is first; at that cap the engine stops it
//! through the same cancellation path a `CANCEL` takes, and classifies it with the `Drain` rows
//! (§23.7.1 as amended: the link-level rows, cause `draining`) — so a sent non-idempotent request
//! with no head is Indeterminate, a delivery in progress is `ResponseIncomplete`, and a declared-
//! idempotent request is Retryable. [`HttpEngine::in_flight_watch`] lets `serve` outlast the cap
//! (chassis change 2).
//!
//! **Slice F6:** concurrency limits, the queue, breaker and rate limits. **F5:** TLS (`https`
//! upstreams are `Unsupported` until then). **F7:** metrics, spans and the slow log.

pub mod body;
pub mod budget;
pub mod decode;
pub mod dial;
pub mod head;
pub mod pool;
pub mod track;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ferro_proto::messages::{HttpBody, HttpDone, HttpHeaderField, HttpRequest, HttpStats};
use http_body::Body as _;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::{HttpConfig, Partition, Upstream};
use crate::fate::{
    self, BeforeDispatch, DispatchedNotSent, Fate, HeadReceived, SentNoHead, Situation,
};
use crate::origin::Scheme;
use crate::validate::{self, PolicyCause, Validated};

pub use dial::{BoxIo, Connect, Io, Resolve, StaticResolver, SystemResolver, TcpConnect};
pub use head::MAX_HEAD_BYTES;

use body::OneChunk;
use budget::Budgets;
use dial::{DialFailure, DnsCache};
use pool::{HttpConn, LiveConn, PoolKey, Pools, ReusePolicy};
use track::Tracker;

/// The largest `BODY` chunk (§23.5.3). Not a registry constant: no receiver enforces it (§22.2 (cy)).
pub const MAX_BODY_CHUNK: usize = 256 * 1024;

/// How long a returned connection may take to become ready for its next request before it is
/// discarded instead of pooled. Normally immediate once the body's end was read.
const READY_ON_RETURN: Duration = Duration::from_millis(250);

/// The `Accept-Encoding` the engine adds when a request asks for decoding and names none (§23.9.2).
const DECODE_ACCEPT_ENCODING: &str = "gzip, deflate";

/// One request's terminal, for the caller to declare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminal {
    Done(HttpDone),
    Error(ferro_proto::messages::ErrorPayload),
    Cancelled,
}

impl From<Fate> for Terminal {
    fn from(f: Fate) -> Self {
        match f {
            Fate::Cancelled => Terminal::Cancelled,
            Fate::Error(ep) => Terminal::Error(ep),
        }
    }
}

/// Which response frame a sink is asked to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkFrame {
    /// `HTTP/HEAD`: not terminal, not `STREAM`, debits credit.
    Head,
    /// `HTTP/BODY`: flag `STREAM`, debits credit.
    Body,
}

/// Why a sink did not send a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkError {
    /// The request was cancelled (a `CANCEL`, or the daemon's drain cap) while the frame waited.
    Cancelled,
    /// The request's deadline passed while the frame waited for credit.
    Deadline,
    /// The session is gone; nobody will read another frame.
    LinkLost,
    /// The frame exceeds the frame cap. Unreachable here (a head is < 256 KiB by the engine's own
    /// check, a chunk ≤ 256 KiB), kept so the sink's contract is total.
    Oversized,
}

/// Where response frames go: `ferrod` implements it over its `Responder` (credit debit, session
/// cap, the one ordered control channel). `send` parks while credit is exhausted, and gives up when
/// `cancel` fires or `deadline` passes. `cancel` is passed per call (M6-F4b) because it is the
/// ENGINE's token for the request — the client's `CANCEL` and the daemon's drain cap both fire it —
/// so a frame parked on credit wakes for either.
pub trait ResponseSink: Send + Sync {
    fn send<'a>(
        &'a self,
        frame: SinkFrame,
        payload: Vec<u8>,
        deadline: tokio::time::Instant,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;
}

/// The daemon's drain signal as the engine sees it (§23.6.1, M6-F4b): whether the drain has begun,
/// and WHEN — one instant for every exchange, so the cap is the same wall-clock moment for all of
/// them however often an exchange re-arms its wait. `ferrod` builds it from the session layer's
/// drain signal (chassis change 1).
#[derive(Clone, Debug, Default)]
pub struct DrainView {
    started: CancellationToken,
    at: Arc<OnceLock<tokio::time::Instant>>,
}

impl DrainView {
    /// `started` is cancelled when the drain begins; `at` holds that instant, set BEFORE `started`
    /// is cancelled (so an observer of the token always finds it).
    pub fn new(started: CancellationToken, at: Arc<OnceLock<tokio::time::Instant>>) -> Self {
        DrainView { started, at }
    }

    /// A drain that never begins (a caller with no daemon, such as an engine-level test).
    pub fn never() -> Self {
        DrainView::default()
    }

    pub fn is_draining(&self) -> bool {
        self.started.is_cancelled()
    }

    /// Resolves `after` the drain began (never, if it never does).
    async fn cap(&self, after: Duration) {
        self.started.cancelled().await;
        let at = self
            .at
            .get()
            .copied()
            .unwrap_or_else(tokio::time::Instant::now);
        tokio::time::sleep_until(at + after).await;
    }
}

/// The number of requests inside [`HttpEngine::serve_request`], observable as a `watch` so `serve`
/// can wait for it to reach zero (§23.6.1 chassis change 2).
struct InFlight(watch::Sender<usize>);

struct InFlightGuard<'a>(&'a watch::Sender<usize>);

impl InFlight {
    fn enter(&self) -> InFlightGuard<'_> {
        self.0.send_modify(|n| *n += 1);
        InFlightGuard(&self.0)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n -= 1);
    }
}

/// The engine's stop signal for ONE request: a child of the client's `CANCEL` token that the drain
/// cap also fires, plus the flag that says which of the two it was. The flag is set BEFORE the
/// token is cancelled, so every observer of the cancellation reads the right reason.
struct Stop {
    token: CancellationToken,
    drained: AtomicBool,
}

impl Stop {
    fn drained(&self) -> bool {
        self.drained.load(Ordering::SeqCst)
    }
}

/// The engine: configuration, resolver, DNS cache, connection pools and body budgets. One per
/// daemon.
pub struct HttpEngine {
    config: Arc<HttpConfig>,
    resolver: Arc<dyn Resolve>,
    connector: Arc<dyn Connect>,
    dns: DnsCache,
    pools: Pools,
    budgets: Budgets,
    in_flight: InFlight,
    live: Arc<AtomicUsize>,
}

impl std::fmt::Debug for HttpEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpEngine").finish_non_exhaustive()
    }
}

/// What the request asked for, bounded by its upstream (§23.8.4).
struct Bounds {
    deadline: tokio::time::Instant,
    connect: Duration,
    read_idle: Option<Duration>,
}

fn ms(v: u32) -> Duration {
    Duration::from_millis(u64::from(v))
}

impl HttpEngine {
    /// The production engine: `getaddrinfo` resolution.
    pub fn new(config: Arc<HttpConfig>) -> Self {
        Self::with_resolver(config, Arc::new(SystemResolver))
    }

    /// The engine over an injected resolver (tests; the e2e suite).
    pub fn with_resolver(config: Arc<HttpConfig>, resolver: Arc<dyn Resolve>) -> Self {
        Self::with_seams(config, resolver, Arc::new(TcpConnect))
    }

    /// The engine over an injected resolver AND connector (fault injection below `hyper`).
    pub fn with_seams(
        config: Arc<HttpConfig>,
        resolver: Arc<dyn Resolve>,
        connector: Arc<dyn Connect>,
    ) -> Self {
        let budgets = Budgets::new(&config);
        HttpEngine {
            config,
            resolver,
            connector,
            dns: DnsCache::default(),
            pools: Pools::default(),
            budgets,
            in_flight: InFlight(watch::Sender::new(0)),
            live: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn config(&self) -> &HttpConfig {
        &self.config
    }

    /// Whether this engine serves HTTP: the `HTTP` feature bit (§23.5). A daemon-wide configuration
    /// error disables the service (§23.3.1) — requests are then refused `forbidden_upstream`, and the
    /// bit is not advertised.
    pub fn serves(&self) -> bool {
        !self.config.service_disabled()
    }

    /// Idle connections the engine holds for `upstream` (no partition) — diagnostics and tests.
    pub fn idle_connections(&self, upstream: &str) -> usize {
        self.pools.idle_count(&(upstream.to_string(), None))
    }

    /// Upstream connections alive anywhere in the engine — pooled, in use, or being torn down —
    /// counted from dial to drop (chaos 12; the `ferro_http_connections` gauge is F7's).
    pub fn live_connections(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// Body bytes currently charged to `upstream`'s budget (§23.8.6; the gauge is F7's).
    pub fn body_budget_in_use(&self, upstream: &str) -> u64 {
        self.budgets.in_use(upstream)
    }

    /// Body bytes currently charged to the daemon-wide budget.
    pub fn body_budget_daemon_in_use(&self) -> u64 {
        self.budgets.daemon_in_use()
    }

    /// Requests currently inside [`HttpEngine::serve_request`].
    pub fn in_flight(&self) -> usize {
        *self.in_flight.0.borrow()
    }

    /// The in-flight count as a `watch`, for `serve`'s drain wait (§23.6.1 chassis change 2).
    pub fn in_flight_watch(&self) -> watch::Receiver<usize> {
        self.in_flight.0.subscribe()
    }

    /// `FERRO_HTTP_DRAIN_MS` (§23.6.1).
    pub fn drain_cap(&self) -> Duration {
        ms(self.config.daemon.drain_ms)
    }

    /// [`HttpEngine::serve_request`] for a borrowed request that can never be drained (engine-level
    /// tests and callers with no daemon). Clones the request, body included.
    pub async fn exchange(
        &self,
        req: &HttpRequest,
        peer_uid: Option<u32>,
        started: Instant,
        cancel: &CancellationToken,
        sink: &dyn ResponseSink,
    ) -> Terminal {
        self.serve_request(
            req.clone(),
            peer_uid,
            started,
            cancel,
            &DrainView::never(),
            sink,
        )
        .await
    }

    /// Run one request to its terminal. `started` is when its frame was decoded: the total deadline
    /// runs from there (§23.6 step 4). `cancel` is the request's `CANCEL` (and session death);
    /// `drain` is the daemon's drain signal (§23.6.1). The request is taken by value so its body can
    /// be handed to `hyper` without a copy, charged to the body budget (§23.8.6).
    pub async fn serve_request(
        &self,
        req: HttpRequest,
        peer_uid: Option<u32>,
        started: Instant,
        cancel: &CancellationToken,
        drain: &DrainView,
        sink: &dyn ResponseSink,
    ) -> Terminal {
        let _in_flight = self.in_flight.enter();
        let stop = Stop {
            token: cancel.child_token(),
            drained: AtomicBool::new(false),
        };
        let run = self.run(req, peer_uid, started, &stop, drain, sink);
        let cap = drain.cap(self.drain_cap());
        tokio::pin!(run, cap);
        let mut capped = false;
        loop {
            tokio::select! {
                biased;
                t = &mut run => return t,
                () = &mut cap, if !capped => {
                    // §23.6.1: the exchange outlived `FERRO_HTTP_DRAIN_MS`. Stop it through the
                    // cancellation path every phase already handles — which discards the connection
                    // before reading `sent` — and let it classify itself with the `Drain` rows.
                    capped = true;
                    stop.drained.store(true, Ordering::SeqCst);
                    stop.token.cancel();
                }
            }
        }
    }

    async fn run(
        &self,
        mut req: HttpRequest,
        peer_uid: Option<u32>,
        started: Instant,
        stop: &Stop,
        drain: &DrainView,
        sink: &dyn ResponseSink,
    ) -> Terminal {
        let headers: Vec<(String, Vec<u8>)> = req
            .headers
            .iter()
            .map(|h| (h.name.clone(), h.value.clone()))
            .collect();
        let vreq = validate::Request {
            upstream: &req.upstream,
            method: &req.method,
            target: &req.target,
            origin: req.origin.as_deref(),
            headers: &headers,
            body: req.body.as_deref(),
            idempotent: req.idempotent,
            decode: req.decode,
        };
        let v = match validate::validate(&self.config, peer_uid, &vreq) {
            Ok(v) => v,
            Err(refusal) => {
                // The refusal's Display names a rule and at most an offset or a header NAME (F3).
                return policy(refusal.cause(), Some(refusal.to_string()));
            }
        };
        let up = v.upstream;
        let idem = v.idempotent;
        if up.origin.scheme() == Scheme::Https {
            // TLS origination is slice F5. Until it lands an `https` upstream is NOT SERVED by this
            // build: `Unsupported`, which on service HTTP carries no cause token (§22.2 (cy)) — not a
            // Retryable `tls_handshake` a client policy would loop on.
            return Terminal::Error(ferro_proto::messages::ErrorPayload {
                code: ferro_proto::consts::errc::UNSUPPORTED,
                branch: ferro_proto::consts::errc::UNSUPPORTED_BRANCH,
                sqlstate: None,
                errno: None,
                message: "https upstreams are not served by this build yet (TLS origination is \
                          slice M6-F5)"
                    .into(),
                detail: None,
                retry_after_ms: None,
            });
        }
        let bounds = Bounds {
            deadline: tokio::time::Instant::from_std(started)
                + ms(req
                    .timeout_ms
                    .unwrap_or(up.limits.timeout_ms)
                    .min(up.limits.timeout_ms)),
            connect: ms(req
                .connect_timeout_ms
                .unwrap_or(up.limits.connect_timeout_ms)
                .min(up.limits.connect_timeout_ms)),
            read_idle: req.read_timeout_ms.or(up.limits.read_timeout_ms).map(ms),
        };

        // The request `hyper` will send, built before any dial: a target or header the `http`
        // crate cannot represent byte-exactly is refused before anything is dialled. Its body is
        // attached after admission (the budget charge travels with it).
        let parts = match build_request(&v, &req, &headers) {
            Ok(r) => r,
            Err(cause) => return policy(cause, None),
        };

        // §23.8.1: `PARTITION=uid` gives every peer uid its own connections. A peer the transport
        // could not attest has no uid to be partitioned by, and pooling all such peers together
        // would be exactly the cross-tenant sharing the operator turned the key on to prevent, so it
        // is REFUSED (fail closed, as F3 refuses an unattested peer on an `ALLOW_UIDS` upstream).
        let key: PoolKey = (
            up.name.clone(),
            match (up.partition, peer_uid) {
                (Partition::Uid, Some(uid)) => Some(uid),
                (Partition::Uid, None) => {
                    return policy(
                        PolicyCause::Upstream,
                        Some("upstream not available to this peer".into()),
                    );
                }
                (Partition::None, _) => None,
            },
        );

        // ---- admission (§23.6 step 4; F4b: (a) and (d)) -------------------------------------------
        // (a) The drain: a request that arrives while the daemon drains is refused, unsent.
        if drain.is_draining() {
            return classify(Situation::BeforeDispatch(BeforeDispatch::Draining), idem);
        }
        // (d) The body budget: charged now, released when the body is fully written or at this
        // request's terminal, whichever is first (`budget`'s module docs).
        let body = req.body.take();
        let body_len = body.as_ref().map_or(0, |b| b.len() as u64);
        let charge = match self.budgets.charge(&up.name, body_len) {
            Ok(c) => c,
            Err(()) => {
                return classify(Situation::BeforeDispatch(BeforeDispatch::BodyBudget), idem);
            }
        };
        let body = match (body, &charge) {
            (Some(b), Some(c)) => OneChunk::new(c.wrap_body(b)),
            (Some(b), None) => OneChunk::new(Bytes::from(b)),
            (None, _) => OneChunk::empty(),
        };
        let hreq = http::Request::from_parts(parts, body);
        // This holder drops when `run` returns: the "at its terminal" half of the release rule.
        let _charge = charge;
        let admitted = Instant::now();

        // ---- before dispatch: acquire a connection ------------------------------------------------
        let acquire = self.acquire(&key, up, idem, bounds.connect);
        let (mut conn, reused, connect_us) = tokio::select! {
            biased;
            () = stop.token.cancelled() => {
                return classify(Situation::BeforeDispatch(before_dispatch_stop(stop)), idem)
            }
            () = tokio::time::sleep_until(bounds.deadline) => {
                return classify(Situation::BeforeDispatch(BeforeDispatch::Deadline), idem)
            }
            r = acquire => match r {
                Ok(c) => c,
                Err(f) => return classify(Situation::BeforeDispatch(dial_situation(f)), idem),
            },
        };

        // ---- dispatch -----------------------------------------------------------------------------
        // A `CANCEL`, the drain cap or the deadline that arrived while the connection was being
        // handed over is honoured BEFORE dispatch: a request not yet given to `hyper` is not sent,
        // and giving it only to classify it a moment later could put bytes on the wire for nothing.
        // The idle connection is still good, so it goes back to the pool.
        if stop.token.is_cancelled() || tokio::time::Instant::now() >= bounds.deadline {
            let event = if stop.token.is_cancelled() {
                before_dispatch_stop(stop)
            } else {
                BeforeDispatch::Deadline
            };
            // Unused: it keeps its `idle_since`, so the reuse rules see its true idleness.
            self.pools.return_unused(
                key,
                conn,
                usize::try_from(up.limits.max_connections).unwrap_or(usize::MAX),
            );
            return classify(Situation::BeforeDispatch(event), idem);
        }
        let dispatched = Instant::now();
        let queue_us =
            micros(dispatched.saturating_duration_since(admitted)).saturating_sub(connect_us);
        conn.track.arm();
        let send = conn.sender.try_send_request(hreq);
        let outcome = tokio::select! {
            // The response first: a head that arrived is a head received, whatever raced it.
            biased;
            r = send => Ok(r),
            () = stop.token.cancelled() => Err(true),
            () = tokio::time::sleep_until(bounds.deadline) => Err(false),
        };
        let resp = match outcome {
            Ok(Ok(resp)) => resp,
            Ok(Err(mut e)) => {
                let returned = e.take_message().is_some();
                let err = e.into_error();
                let track = conn.track.clone();
                conn.discard().await; // only now is `sent` final (§23.7.1)
                if !track.sent() {
                    return classify(Situation::DispatchedNotSent(head::not_sent(&track)), idem);
                }
                if returned {
                    // P19: `message returned ⇒ not sent`. The tracker is the authority; a
                    // disagreement is an engine invariant violation, reported, never acted on.
                    tracing::error!(
                        upstream = %up.name,
                        "http: hyper returned an unserialised request the tracker counts as sent"
                    );
                }
                return classify(
                    Situation::SentNoHead(head::sent_no_head(&err, &track)),
                    idem,
                );
            }
            Err(stopped) => {
                // Read the reason BEFORE the teardown, at the moment the stop was observed.
                let drained = stopped && stop.drained();
                let track = conn.track.clone();
                conn.discard().await;
                return classify(
                    match (track.sent(), stopped, drained) {
                        (false, true, true) => {
                            Situation::DispatchedNotSent(DispatchedNotSent::Drain)
                        }
                        (false, true, false) => {
                            Situation::DispatchedNotSent(DispatchedNotSent::Cancel)
                        }
                        (false, false, _) => {
                            Situation::DispatchedNotSent(DispatchedNotSent::Deadline)
                        }
                        (true, true, true) => Situation::SentNoHead(SentNoHead::Drain),
                        (true, true, false) => Situation::SentNoHead(SentNoHead::Cancel),
                        (true, false, _) => Situation::SentNoHead(SentNoHead::Timeout),
                    },
                    idem,
                );
            }
        };
        let ttfb_us = micros(dispatched.elapsed());

        // ---- head ---------------------------------------------------------------------------------
        // A head that arrived while NO byte of the request had been sent cannot be the answer to
        // it: the upstream spoke first (an unsolicited 101 or 200 on accept, read by `hyper` once the
        // request was queued behind a slow write). Every head-phase arm below — the delivered head,
        // the 101, the oversize check, `MAX_RESPONSE_BYTES` — would otherwise classify a request
        // that never left as received (review of M6-F4a, finding 1: a POST reported Indeterminate
        // with zero bytes at the upstream). So: tear the connection down, THEN read `sent` (§23.7.1),
        // and an unsent request is "dispatched, not sent" (`unsent_closed`, Retryable). If bytes
        // slipped out while the task was being stopped, the head still preceded the request, which
        // is a malformed exchange: "sent, no head" (`malformed_head`).
        if !conn.track.sent() {
            let track = conn.track.clone();
            conn.discard().await;
            return classify(
                if track.sent() {
                    Situation::SentNoHead(SentNoHead::MalformedHead)
                } else {
                    Situation::DispatchedNotSent(DispatchedNotSent::UnsentClosed)
                },
                idem,
            );
        }
        let (parts, mut body) = resp.into_parts();
        if parts.status == http::StatusCode::SWITCHING_PROTOCOLS {
            // `hyper` returns a 101 as an ordinary head (P14); §23.5.2 calls it malformed.
            conn.discard().await;
            return classify(Situation::SentNoHead(SentNoHead::Informational101), idem);
        }
        let reason = head::reason_phrase(&parts.extensions, parts.status);
        if head::head_size(&reason, &parts.headers) > MAX_HEAD_BYTES {
            conn.discard().await;
            return classify(Situation::SentNoHead(SentNoHead::OversizeHead), idem);
        }
        let max_response = up.limits.max_response_bytes;
        let over_max = |read: u64| max_response.is_some_and(|m| read > m);
        if over_max(conn.track.read()) {
            conn.discard().await;
            return classify(
                Situation::HeadReceived(HeadReceived::MaxResponseBytes),
                idem,
            );
        }
        let keep_alive = parts
            .headers
            .get("keep-alive")
            .map(|v| v.as_bytes().to_vec());
        let closes = parts
            .headers
            .get_all(http::header::CONNECTION)
            .iter()
            .any(|v| {
                v.to_str()
                    .is_ok_and(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case("close")))
            });
        // §23.9.2: decode only when the request asked AND the response carries exactly one coding
        // the engine decodes; everything else passes through with `decoded = nil`.
        let decoding = if req.decode {
            decode::coding_of(&parts.headers)
        } else {
            None
        };
        let mut decoder = decoding
            .as_ref()
            .map(|(c, _)| decode::Decoder::new(*c, MAX_BODY_CHUNK));
        let head_frame = head::http_head(
            parts.status,
            parts.version,
            reason,
            &parts.headers,
            idem,
            decoding.map(|(_, as_received)| as_received),
        );
        if let Err(e) = sink
            .send(
                SinkFrame::Head,
                head_frame.encode(),
                bounds.deadline,
                &stop.token,
            )
            .await
        {
            let event = sink_failure(e, stop);
            conn.discard().await;
            return classify(Situation::HeadReceived(event), idem);
        }

        // ---- body ---------------------------------------------------------------------------------
        let mut trailers: Vec<HttpHeaderField> = Vec::new();
        loop {
            let next = async {
                let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx));
                match bounds.read_idle {
                    Some(idle) => tokio::time::timeout(idle, frame).await.map_err(|_| ()),
                    None => Ok(frame.await),
                }
            };
            let polled = tokio::select! {
                // Cancel and deadline FIRST: a body that is always ready must not starve them.
                biased;
                () = stop.token.cancelled() => Err(after_head_stop(stop)),
                () = tokio::time::sleep_until(bounds.deadline) => Err(HeadReceived::Timeout),
                f = next => match f {
                    Err(()) => Err(HeadReceived::ReadIdle),
                    Ok(f) => Ok(f),
                },
            };
            let frame = match polled {
                Err(e) => {
                    conn.discard().await;
                    return classify(Situation::HeadReceived(e), idem);
                }
                Ok(None) => {
                    // The body's end: a decoded stream must end cleanly too.
                    if decoder.as_ref().is_some_and(|d| d.finish().is_err()) {
                        conn.discard().await;
                        return classify(Situation::HeadReceived(HeadReceived::Decode), idem);
                    }
                    break;
                }
                Ok(Some(Err(e))) => {
                    let cause = head::after_head(&e);
                    conn.discard().await;
                    return classify(Situation::HeadReceived(cause), idem);
                }
                Ok(Some(Ok(frame))) => frame,
            };
            if over_max(conn.track.read()) {
                conn.discard().await;
                return classify(
                    Situation::HeadReceived(HeadReceived::MaxResponseBytes),
                    idem,
                );
            }
            match frame.into_data() {
                Ok(data) => {
                    let sent = match decoder.as_mut() {
                        None => send_chunks(sink, &data, &bounds, stop).await,
                        Some(d) => {
                            d.feed(data);
                            send_decoded(sink, d, &bounds, stop).await
                        }
                    };
                    if let Err(event) = sent {
                        conn.discard().await;
                        return classify(Situation::HeadReceived(event), idem);
                    }
                }
                Err(frame) => {
                    if let Ok(t) = frame.into_trailers() {
                        trailers.extend(head::fields(&t, &[]));
                    }
                }
            }
        }

        // ---- complete -----------------------------------------------------------------------------
        let stats = HttpStats {
            queue_us,
            connect_us,
            tls_us: 0,
            ttfb_us,
            total_us: micros(started.elapsed()),
            bytes_sent: conn.track.written_armed(),
            bytes_received: conn.track.read(),
            reused,
        };
        // Returned to the pool BEFORE the terminal is declared, so a request the client sends after
        // this `END` finds it.
        if closes {
            conn.discard().await;
        } else {
            conn.idle_limit =
                pool::idle_limit(ms(up.limits.idle_timeout_ms), keep_alive.as_deref());
            match tokio::time::timeout(READY_ON_RETURN, conn.sender.ready()).await {
                Ok(Ok(())) => self.pools.checkin(
                    key,
                    conn,
                    usize::try_from(up.limits.max_connections).unwrap_or(usize::MAX),
                ),
                _ => conn.discard().await,
            }
        }
        Terminal::Done(HttpDone { trailers, stats })
    }

    /// Reuse (§23.8.2) or dial (§23.8.5). Returns the connection, whether it was reused, and its
    /// `connect_us`.
    async fn acquire(
        &self,
        key: &PoolKey,
        up: &Upstream,
        idempotent: bool,
        connect: Duration,
    ) -> Result<(HttpConn, bool, u64), DialFailure> {
        let policy = ReusePolicy {
            max_lifetime: ms(up.limits.max_lifetime_ms),
            idempotent,
            unsafe_reuse_max_idle: ms(up.limits.h1_unsafe_reuse_max_idle_ms),
        };
        if let Some(c) = self.pools.checkout(key, policy) {
            return Ok((c, true, 0));
        }
        let d = dial::dial(
            up,
            self.resolver.as_ref(),
            self.connector.as_ref(),
            &self.dns,
            &self.config.daemon.nat64,
            connect,
        )
        .await?;
        let (tracked, track) = Tracker::new(d.stream);
        let mut builder = http1::Builder::new();
        builder
            .max_buf_size(head::H1_MAX_BUF_SIZE)
            .max_headers(head::H1_MAX_HEADERS)
            .title_case_headers(true)
            // M6-F4b: the `Queue` write strategy, PINNED rather than inherited from the transport's
            // `is_write_vectored`. Under it `hyper` keeps the request body's own `Bytes` until its
            // last byte is written, which is what the body budget's release-when-written rule
            // observes (`budget`'s module docs); `Flatten` would copy the body and drop ours at once.
            .writev(true);
        let (sender, conn) = match builder.handshake(TokioIo::new(tracked)).await {
            Ok(x) => x,
            // No I/O happens in an h1 handshake (P1); a failure here is not a dial outcome the
            // table names, so it is the closest one: the connection is unusable.
            Err(_) => return Err(DialFailure::ConnectUnreachable),
        };
        let task = tokio::spawn(async move {
            let _ = conn.await;
        });
        let now = Instant::now();
        Ok((
            HttpConn {
                sender,
                task,
                track,
                peer: d.peer,
                created: now,
                idle_since: now,
                idle_limit: ms(up.limits.idle_timeout_ms),
                live: LiveConn::new(&self.live),
            },
            false,
            micros(d.elapsed),
        ))
    }
}

/// Send a raw body chunk as `BODY` frames of at most [`MAX_BODY_CHUNK`].
async fn send_chunks(
    sink: &dyn ResponseSink,
    data: &[u8],
    bounds: &Bounds,
    stop: &Stop,
) -> Result<(), HeadReceived> {
    for chunk in data.chunks(MAX_BODY_CHUNK) {
        let payload = HttpBody {
            chunk: chunk.to_vec(),
        }
        .encode();
        sink.send(SinkFrame::Body, payload, bounds.deadline, &stop.token)
            .await
            .map_err(|e| sink_failure(e, stop))?;
    }
    Ok(())
}

/// Drain a decoder into `BODY` frames, one bounded step at a time (§23.9.2). Each step is sent —
/// through the credit gauntlet — before the next is decoded, so a frame parked on credit stops
/// decompression (memory bounded by the window), and the stop token and the total deadline are
/// checked between steps (CPU bounded by the deadline, even for a bomb that never lacks credit).
async fn send_decoded(
    sink: &dyn ResponseSink,
    d: &mut decode::Decoder,
    bounds: &Bounds,
    stop: &Stop,
) -> Result<(), HeadReceived> {
    loop {
        if stop.token.is_cancelled() {
            return Err(after_head_stop(stop));
        }
        if tokio::time::Instant::now() >= bounds.deadline {
            return Err(HeadReceived::Timeout);
        }
        let Some(chunk) = d.next_chunk().map_err(|_| HeadReceived::Decode)? else {
            return Ok(());
        };
        let payload = HttpBody { chunk }.encode();
        sink.send(SinkFrame::Body, payload, bounds.deadline, &stop.token)
            .await
            .map_err(|e| sink_failure(e, stop))?;
        // A step that never waits (credit to spare) must still let other tasks run.
        tokio::task::yield_now().await;
    }
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX >> 1)
}

fn classify(s: Situation, idempotent: bool) -> Terminal {
    fate::classify(s, idempotent).into()
}

/// The stop token fired before dispatch: the drain cap, or a `CANCEL`.
fn before_dispatch_stop(stop: &Stop) -> BeforeDispatch {
    if stop.drained() {
        BeforeDispatch::Draining
    } else {
        BeforeDispatch::Cancel
    }
}

/// The stop token fired after the head: the drain cap, or a `CANCEL`.
fn after_head_stop(stop: &Stop) -> HeadReceived {
    if stop.drained() {
        HeadReceived::Drain
    } else {
        HeadReceived::Cancel
    }
}

/// A policy refusal, optionally with the validator's own (log-safe) sentence as the message.
fn policy(cause: PolicyCause, message: Option<String>) -> Terminal {
    match fate::classify(
        Situation::BeforeDispatch(BeforeDispatch::Policy(cause)),
        false,
    ) {
        Fate::Error(mut ep) => {
            if let Some(m) = message {
                ep.message = m;
            }
            Terminal::Error(ep)
        }
        Fate::Cancelled => Terminal::Cancelled,
    }
}

fn dial_situation(f: DialFailure) -> BeforeDispatch {
    match f {
        DialFailure::Dns => BeforeDispatch::Dns,
        DialFailure::Address(_) => BeforeDispatch::Policy(PolicyCause::Address),
        DialFailure::ConnectRefused => BeforeDispatch::ConnectRefused,
        DialFailure::ConnectUnreachable => BeforeDispatch::ConnectUnreachable,
        DialFailure::ConnectTimeout => BeforeDispatch::ConnectTimeout,
    }
}

fn sink_failure(e: SinkError, stop: &Stop) -> HeadReceived {
    match e {
        SinkError::Deadline => HeadReceived::Timeout,
        SinkError::Cancelled => after_head_stop(stop),
        // A session that is gone is a cancel nobody will read (§23.6 "session death").
        SinkError::LinkLost => HeadReceived::Cancel,
        SinkError::Oversized => HeadReceived::BodyFraming,
    }
}

/// The `hyper` request for a validated one, without its body: the method and target bytes as
/// accepted, `Host` from `ORIGIN` alone, PHP's kept headers verbatim and in order, the attached
/// headers, the decode-only `Accept-Encoding` (§23.9.2), and a recomputed `Content-Length` when a
/// body is present. Nothing else is added (§23.4.3).
fn build_request(
    v: &Validated<'_>,
    req: &HttpRequest,
    headers: &[(String, Vec<u8>)],
) -> Result<http::request::Parts, PolicyCause> {
    let method =
        http::Method::from_bytes(req.method.as_bytes()).map_err(|_| PolicyCause::Method)?;
    let uri: http::Uri = req.target.parse().map_err(|_| PolicyCause::Target)?;
    // The `http` crate must carry the target byte for byte (§23.4.2: "the engine sends exactly the
    // bytes it accepted"); a normalisation would be a rewrite, so it is refused instead.
    if uri.path_and_query().map(http::uri::PathAndQuery::as_str) != Some(req.target.as_str()) {
        return Err(PolicyCause::Target);
    }
    let mut b = http::Request::builder().method(method).uri(uri);
    let map = b.headers_mut().ok_or(PolicyCause::Header)?;
    map.insert(
        http::header::HOST,
        http::HeaderValue::from_str(v.host).map_err(|_| PolicyCause::Origin)?,
    );
    for &i in &v.send_headers {
        let (n, val) = &headers[i];
        let name = http::HeaderName::from_bytes(n.as_bytes()).map_err(|_| PolicyCause::Header)?;
        let value = http::HeaderValue::from_bytes(val).map_err(|_| PolicyCause::Header)?;
        map.append(name, value);
    }
    for a in v.upstream.attached.iter() {
        let name =
            http::HeaderName::from_bytes(a.name().as_bytes()).map_err(|_| PolicyCause::Header)?;
        let value = http::HeaderValue::from_bytes(a.value().expose_for_wire())
            .map_err(|_| PolicyCause::Header)?;
        map.append(name, value);
    }
    if v.add_accept_encoding {
        // §23.9.2 "Asking": the request asked for decoding and neither PHP nor the attached set
        // named an `Accept-Encoding` (the validator decided that, F3).
        map.insert(
            http::header::ACCEPT_ENCODING,
            http::HeaderValue::from_static(DECODE_ACCEPT_ENCODING),
        );
    }
    if req.body.is_some() {
        map.insert(
            http::header::CONTENT_LENGTH,
            http::HeaderValue::from(v.content_length),
        );
    }
    let (parts, ()) = b.body(()).map_err(|_| PolicyCause::Target)?.into_parts();
    Ok(parts)
}
