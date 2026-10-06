//! The Ferro HTTP engine, HTTP/1.1 plaintext path (SPEC §23.6; slice M6-F4a).
//!
//! [`HttpEngine::exchange`] runs one `REQUEST` through §23.6's lifecycle and returns its ONE
//! terminal ([`Terminal`]); the caller (`ferrod`'s HTTP service) declares it, so exactly one `END`
//! follows every request (charter rule 4). Response frames go out through a [`ResponseSink`], the
//! seam that keeps this crate free of `ferrod`'s session types.
//!
//! 1. **Validate** (§23.4, F3) and compute effective idempotency (§23.7.2). A refusal is
//!    `Forbidden` with its `forbidden_*` cause, made before anything is dialled.
//! 2. **Bounds** (§23.8.4): the total deadline runs from DECODE (`started`); each bound is
//!    `min(request field ?? upstream default, upstream ceiling)` — `read_timeout_ms` has no ceiling,
//!    `READ_TIMEOUT_MS` being only its default.
//! 3. **Acquire** (§23.6 step 5): reuse an idle connection (§23.8.2) or dial: DNS, the address guard,
//!    TCP to the checked `SocketAddr` (§23.8.5). A deadline or `CANCEL` here is "before dispatch".
//! 4. **Dispatch** (step 6): arm the write tracker, hand the request to `hyper`.
//! 5. **Head** (step 7): a 101 is `informational_101`, a head over [`MAX_HEAD_BYTES`] by the engine's
//!    exact measure is `oversize_head`; both are "sent, no head". Otherwise one `HEAD` frame.
//! 6. **Body** (step 8): `BODY` frames of at most 256 KiB, each a credit debit; while credit is
//!    exhausted the sink parks and the engine stops polling the body, so `hyper` stops reading and
//!    the upstream sees TCP backpressure (P7).
//! 7. **Terminal** (step 9): `HttpDone`, an error from [`fate::classify`], or `Cancelled`.
//!
//! **The engine never re-sends a request** (charter rule 3, §23.7): a dispatched request is never
//! moved to another connection, and a failed dial is not a retry (no byte of the request exists
//! anywhere yet). After a cancel, a timeout or any failure the connection is discarded, never
//! drained (§23.8.2), and `sent` is read only after `hyper`'s connection task is gone (§23.7.1).
//!
//! **Not in F4a (slice F4b, §22.2 (cz)):** the body budget, the HTTP drain, content decoding
//! (§23.9.2: `decode` is accepted and has no effect — the engine neither asks for an encoding nor
//! removes one, and `HttpHead.decoded` is `nil`). **Slice F6:** concurrency limits, the queue,
//! breaker and rate limits. **F5:** TLS (`https` upstreams are refused `tls_handshake` until then).
//! **F7:** metrics, spans and the slow log.

pub mod body;
pub mod dial;
pub mod head;
pub mod pool;
pub mod track;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ferro_proto::messages::{HttpBody, HttpDone, HttpHeaderField, HttpRequest, HttpStats};
use http_body::Body as _;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
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
use dial::{DialFailure, DnsCache};
use pool::{HttpConn, PoolKey, Pools, ReusePolicy};
use track::Tracker;

/// The largest `BODY` chunk (§23.5.3). Not a registry constant: no receiver enforces it (§22.2 (cy)).
pub const MAX_BODY_CHUNK: usize = 256 * 1024;

/// How long a returned connection may take to become ready for its next request before it is
/// discarded instead of pooled. Normally immediate once the body's end was read.
const READY_ON_RETURN: Duration = Duration::from_millis(250);

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
    /// The request was cancelled while the frame waited for credit.
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
/// cap, the one ordered control channel). `send` parks while credit is exhausted.
pub trait ResponseSink: Send + Sync {
    fn send<'a>(
        &'a self,
        frame: SinkFrame,
        payload: Vec<u8>,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;
}

/// The engine: configuration, resolver, DNS cache and connection pools. One per daemon.
pub struct HttpEngine {
    config: Arc<HttpConfig>,
    resolver: Arc<dyn Resolve>,
    connector: Arc<dyn Connect>,
    dns: DnsCache,
    pools: Pools,
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
        HttpEngine {
            config,
            resolver,
            connector,
            dns: DnsCache::default(),
            pools: Pools::default(),
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

    /// Run one request to its terminal. `started` is when its frame was decoded: the total deadline
    /// runs from there (§23.6 step 4). `cancel` is the request's `CANCEL` (and session death).
    pub async fn exchange(
        &self,
        req: &HttpRequest,
        peer_uid: Option<u32>,
        started: Instant,
        cancel: &CancellationToken,
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
        // crate cannot represent byte-exactly is refused before anything is dialled.
        let hreq = match build_request(&v, req, &headers) {
            Ok(r) => r,
            Err(cause) => return policy(cause, None),
        };

        let key: PoolKey = (
            up.name.clone(),
            match up.partition {
                Partition::Uid => peer_uid,
                Partition::None => None,
            },
        );
        let admitted = Instant::now();

        // ---- before dispatch: acquire a connection ------------------------------------------------
        let acquire = self.acquire(&key, up, idem, bounds.connect);
        let (mut conn, reused, connect_us) = tokio::select! {
            biased;
            () = cancel.cancelled() => return classify(Situation::BeforeDispatch(BeforeDispatch::Cancel), idem),
            () = tokio::time::sleep_until(bounds.deadline) => {
                return classify(Situation::BeforeDispatch(BeforeDispatch::Deadline), idem)
            }
            r = acquire => match r {
                Ok(c) => c,
                Err(f) => return classify(Situation::BeforeDispatch(dial_situation(f)), idem),
            },
        };

        // ---- dispatch -----------------------------------------------------------------------------
        // A `CANCEL` or the deadline that arrived while the connection was being handed over is
        // honoured BEFORE dispatch: a request not yet given to `hyper` is not sent, and giving it
        // only to classify it a moment later could put bytes on the wire for nothing. The idle
        // connection is still good, so it goes back to the pool.
        if cancel.is_cancelled() || tokio::time::Instant::now() >= bounds.deadline {
            let event = if cancel.is_cancelled() {
                BeforeDispatch::Cancel
            } else {
                BeforeDispatch::Deadline
            };
            self.pools.checkin(
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
            () = cancel.cancelled() => Err(true),
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
            Err(cancelled) => {
                let track = conn.track.clone();
                conn.discard().await;
                return classify(
                    match (track.sent(), cancelled) {
                        (false, true) => Situation::DispatchedNotSent(DispatchedNotSent::Cancel),
                        (false, false) => Situation::DispatchedNotSent(DispatchedNotSent::Deadline),
                        (true, true) => Situation::SentNoHead(SentNoHead::Cancel),
                        (true, false) => Situation::SentNoHead(SentNoHead::Timeout),
                    },
                    idem,
                );
            }
        };
        let ttfb_us = micros(dispatched.elapsed());

        // ---- head ---------------------------------------------------------------------------------
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
        let head_frame = head::http_head(parts.status, parts.version, reason, &parts.headers, idem);
        if let Err(e) = sink
            .send(SinkFrame::Head, head_frame.encode(), bounds.deadline)
            .await
        {
            conn.discard().await;
            return classify(Situation::HeadReceived(sink_failure(e)), idem);
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
                () = cancel.cancelled() => Err(HeadReceived::Cancel),
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
                Ok(None) => break,
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
                    for chunk in data.chunks(MAX_BODY_CHUNK) {
                        let payload = HttpBody {
                            chunk: chunk.to_vec(),
                        }
                        .encode();
                        if let Err(e) = sink.send(SinkFrame::Body, payload, bounds.deadline).await {
                            conn.discard().await;
                            return classify(Situation::HeadReceived(sink_failure(e)), idem);
                        }
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
            .title_case_headers(true);
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
            },
            false,
            micros(d.elapsed),
        ))
    }
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX >> 1)
}

fn classify(s: Situation, idempotent: bool) -> Terminal {
    fate::classify(s, idempotent).into()
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

fn sink_failure(e: SinkError) -> HeadReceived {
    match e {
        SinkError::Deadline => HeadReceived::Timeout,
        // A session that is gone is a cancel nobody will read (§23.6 "session death").
        SinkError::Cancelled | SinkError::LinkLost => HeadReceived::Cancel,
        SinkError::Oversized => HeadReceived::BodyFraming,
    }
}

/// The `hyper` request for a validated one: the method and target bytes as accepted, `Host` from
/// `ORIGIN` alone, PHP's kept headers verbatim and in order, the attached headers, and a recomputed
/// `Content-Length` when a body is present. Nothing else is added (§23.4.3).
fn build_request(
    v: &Validated<'_>,
    req: &HttpRequest,
    headers: &[(String, Vec<u8>)],
) -> Result<http::Request<OneChunk>, PolicyCause> {
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
    let body = match &req.body {
        Some(bytes) => {
            map.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from(v.content_length),
            );
            OneChunk::new(Bytes::copy_from_slice(bytes))
        }
        None => OneChunk::empty(),
    };
    b.body(body).map_err(|_| PolicyCause::Target)
}
