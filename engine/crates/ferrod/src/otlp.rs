//! **SPEC §13's OTLP traces (M2-C4c-2): one span per EXEC, linked to the caller's `traceparent`.**
//!
//! C4c-1 put the caller's W3C trace context on the wire (`ExecRequest::traceparent`) and parsed it
//! (`crate::trace`). This module turns each EXEC into one OpenTelemetry span whose PARENT is the
//! caller's span, and exports it to an OpenTelemetry Collector.
//!
//! # The consumer is a real collector, and that shaped the format
//!
//! The export is **OTLP/HTTP with the JSON encoding** (`POST <endpoint>/v1/traces`), measured
//! against a real `otelcol` 0.115 before any of this was written. Two things that measurement fixed:
//!
//! * Ids are LOWERCASE HEX strings, not base64 — OTLP/JSON's one deliberate departure from the
//!   proto3 JSON mapping. A wrong-length id is refused with HTTP 400.
//! * **A misspelled field is dropped SILENTLY with HTTP 200.** A typo'd `parentSpanId` yields a root
//!   span and a typo'd attribute value an empty one. So a 200 proves nothing about the content, and
//!   this module's end-to-end test reads back what the collector EXPORTED.
//!
//! # No new dependency
//!
//! The HTTP client is hand-rolled — one request shape, one status line read — for the same reason
//! `crate::metrics` hand-rolls its responder: this daemon holds every tenant's credentials, and a
//! POST does not justify an HTTP stack's dependency tree. `http://` only: TLS would need exactly
//! that tree, and the deployment this targets is a collector agent on the same host or pod.
//!
//! # Opt-in, configured the way every OpenTelemetry SDK is
//!
//! The standard variables are honoured, so an operator's existing configuration works:
//! `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (used as given) or `OTEL_EXPORTER_OTLP_ENDPOINT` (with
//! `/v1/traces` appended), `OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`, `OTEL_BSP_SCHEDULE_DELAY`
//! and `OTEL_SDK_DISABLED`. One deliberate difference: an SDK exports to `localhost:4318` when no
//! endpoint is set, and `ferrod` exports NOWHERE — the same reading of product-vision §5 as the
//! metrics port: a daemon should not start sending data because a package was upgraded.
//!
//! # Sampling follows the caller, and roots nothing by default
//!
//! A caller that sent a SAMPLED context gets a span; an unsampled context gets none (the caller
//! decided). With no context at all, the default `parentbased_always_off` exports nothing, where an
//! SDK's default `parentbased_always_on` would root a new trace. The engine is shared by every
//! application on the host, and one that does not trace would otherwise turn every statement into a
//! root span — the application, not the database engine, is the sampling authority.
//! `OTEL_TRACES_SAMPLER=parentbased_always_on` opts into root spans.
//!
//! # Tracing never fails, blocks or slows a statement
//!
//! A finished span is handed to a bounded queue with `try_send`: when the queue is full the span is
//! DROPPED and counted (`ferro_otlp_spans_dropped_total`), never awaited. Exports run on their own
//! task, bounded by a timeout; a failed export is counted (`ferro_otlp_spans_failed_total`) and
//! logged at most once a minute. Nothing in this module is on the path that sends a terminal.
//!
//! # The redaction contract holds here too
//!
//! A span carries what the slow log carries and nothing more: the normalized FINGERPRINT (never raw
//! SQL), the pool name, measurements, and an error's registry CODE NAME (never its message, which
//! quotes values). Parameters do not appear at all.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use ferro_proto::consts::{branch, errc};
use ferro_proto::messages::ErrorPayload;

use crate::trace::TraceParent;

/// Finished spans waiting for export. A burst beyond this is dropped and counted, never awaited.
pub const QUEUE_CAPACITY: usize = 2048;
/// The most spans one export request carries.
pub const MAX_BATCH: usize = 512;
/// How often a partial batch is exported when none fills up. `OTEL_BSP_SCHEDULE_DELAY` overrides it.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// The bound on one whole export request: connect, write, read the status line.
pub const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
/// The most response bytes read while looking for the status line.
const MAX_RESPONSE_HEAD: usize = 8 * 1024;
/// A failed export is logged at most this often, so a down collector cannot flood the log.
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// The span name. Ferro's own method name, constant, so a span's NAME never carries statement text.
pub const SPAN_NAME: &str = "EXEC";
/// OTLP `SPAN_KIND_CLIENT`: the engine is a client of the database.
const SPAN_KIND_CLIENT: u8 = 3;
/// OTLP `STATUS_CODE_ERROR`. Success leaves the status UNSET, as the OpenTelemetry instrumentation
/// guidance asks (`OK` is reserved for an explicit override).
const STATUS_CODE_ERROR: u8 = 2;
/// OTLP span flags bits 8–9: "the parent's remoteness is known" and "the parent is remote" — true
/// for every parent this engine sees, since it always arrives from another process.
const FLAGS_PARENT_IS_REMOTE: u32 = 0x100 | 0x200;

/// Export counters, exported as Prometheus series by `crate::metrics`.
#[derive(Debug)]
pub struct ExportCounters {
    exported: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
}

impl ExportCounters {
    const fn new() -> Self {
        Self {
            exported: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }
    /// Spans a collector accepted (a 2xx response).
    pub fn exported(&self) -> u64 {
        self.exported.load(Ordering::Relaxed)
    }
    /// Spans dropped because the export queue was full (or the exporter had stopped).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    /// Spans in an export that failed: refused, unreachable or timed out.
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
}

/// The daemon's one set of export counters.
pub static COUNTERS: ExportCounters = ExportCounters::new();

// ---------------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------------

/// Where spans are sent: an `http://` URL's parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub path: String,
}

impl Endpoint {
    /// Parse an `http://host[:port][/path]` URL. `https://` is refused by name rather than sent in
    /// the clear; userinfo is refused because a credential in a URL is one log line from a leak.
    pub fn parse(url: &str) -> Result<Self, String> {
        let rest = match url.strip_prefix("http://") {
            Some(r) => r,
            None if url.starts_with("https://") => {
                return Err(format!(
                    "{url:?}: https is not supported — export to a collector agent on this host \
                     over http (OTLP/HTTP, normally port 4318)"
                ));
            }
            None => return Err(format!("{url:?} is not an http:// URL")),
        };
        let (authority, path) = match rest.find(['/', '?', '#']) {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        if path.starts_with(['?', '#']) {
            return Err(format!("{url:?}: a query or fragment is not supported"));
        }
        if authority.contains('@') {
            return Err(format!("{url:?}: credentials in the URL are not supported"));
        }
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let end = v6
                .find(']')
                .ok_or_else(|| format!("{url:?}: unterminated IPv6 literal"))?;
            let port = match &v6[end + 1..] {
                "" => 80,
                p => parse_port(p.strip_prefix(':').unwrap_or("x"), url)?,
            };
            (&v6[..end], port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, parse_port(p, url)?),
                None => (authority, 80),
            }
        };
        if host.is_empty() {
            return Err(format!("{url:?}: no host"));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            path: path.to_string(),
        })
    }

    /// The `Host:` header value.
    fn host_header(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

fn parse_port(p: &str, url: &str) -> Result<u16, String> {
    p.parse::<u16>()
        .ok()
        .filter(|&n| n != 0)
        .ok_or_else(|| format!("{url:?}: bad port {p:?}"))
}

/// What happens to a statement whose caller sent NO trace context. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sampler {
    /// Follow the caller; with no caller context, export nothing. The default.
    ParentBasedAlwaysOff,
    /// Follow the caller; with no caller context, root a new trace.
    ParentBasedAlwaysOn,
}

/// The resolved exporter configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpConfig {
    pub endpoint: Endpoint,
    pub service_name: String,
    pub sampler: Sampler,
    pub flush_interval: Duration,
}

/// Resolve the exporter configuration from OpenTelemetry's standard variables. `Ok(None)` means
/// tracing is off — the default. `Err` names a value this exporter cannot honour; the daemon logs
/// it and runs WITHOUT tracing rather than guessing, and never refuses to start over it.
pub fn config_from(get: &dyn Fn(&str) -> Option<String>) -> Result<Option<OtlpConfig>, String> {
    let var = |k: &str| {
        get(k)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };

    if var("OTEL_SDK_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return Ok(None);
    }
    let url = match (
        var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
        var("OTEL_EXPORTER_OTLP_ENDPOINT"),
    ) {
        // The signal-specific variable is used AS GIVEN, the generic one gets the signal path —
        // the OpenTelemetry exporter specification's rule, so a URL that works for an SDK works here.
        (Some(url), _) => url,
        (None, Some(base)) => format!("{}/v1/traces", base.trim_end_matches('/')),
        (None, None) => return Ok(None),
    };
    let endpoint = Endpoint::parse(&url)?;

    let protocol =
        var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL").or_else(|| var("OTEL_EXPORTER_OTLP_PROTOCOL"));
    match protocol.as_deref() {
        // `http/protobuf` is accepted too: a collector's OTLP/HTTP receiver takes either encoding
        // on the same port, keyed on Content-Type, so the operator's endpoint is still right.
        None | Some("http/json") | Some("http/protobuf") => {}
        Some(other) => {
            return Err(format!(
                "OTEL_EXPORTER_OTLP_PROTOCOL={other:?} is not supported — ferrod exports \
                 OTLP/HTTP (JSON) only; point it at the collector's HTTP port (normally 4318)"
            ));
        }
    }

    let sampler = match var("OTEL_TRACES_SAMPLER").as_deref() {
        None | Some("parentbased_always_off") => Sampler::ParentBasedAlwaysOff,
        Some("parentbased_always_on") => Sampler::ParentBasedAlwaysOn,
        Some(other) => {
            return Err(format!(
                "OTEL_TRACES_SAMPLER={other:?} is not supported — use parentbased_always_off \
                 (the default) or parentbased_always_on"
            ));
        }
    };

    let flush_interval = var("OTEL_BSP_SCHEDULE_DELAY")
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map_or(DEFAULT_FLUSH_INTERVAL, Duration::from_millis);

    Ok(Some(OtlpConfig {
        endpoint,
        service_name: var("OTEL_SERVICE_NAME").unwrap_or_else(|| "ferrod".to_string()),
        sampler,
        flush_interval,
    }))
}

// ---------------------------------------------------------------------------------------------
// Ids
// ---------------------------------------------------------------------------------------------

static ID_SEED: OnceLock<u64> = OnceLock::new();
static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A fresh non-zero id word. `splitmix64` of a seeded COUNTER rather than a syscall per span:
/// `splitmix64` is a bijection, so distinct counter values can never collide within a process,
/// and the seed (drawn once from the OS) keeps two daemons' sequences apart.
fn next_id_word() -> u64 {
    let seed = *ID_SEED.get_or_init(|| {
        let mut b = [0u8; 8];
        // A failed draw falls back to the clock: ids must never become the reason tracing fails.
        if getrandom::getrandom(&mut b).is_err() {
            b = now_unix_nanos().to_le_bytes();
        }
        u64::from_le_bytes(b)
    });
    loop {
        let n = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let v = splitmix64(seed.wrapping_add(n));
        if v != 0 {
            return v;
        }
    }
}

fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn new_span_id() -> [u8; 8] {
    next_id_word().to_be_bytes()
}

fn new_trace_id() -> [u8; 16] {
    let mut t = [0u8; 16];
    t[..8].copy_from_slice(&next_id_word().to_be_bytes());
    t[8..].copy_from_slice(&next_id_word().to_be_bytes());
    t
}

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

// ---------------------------------------------------------------------------------------------
// Spans
// ---------------------------------------------------------------------------------------------

/// One attribute value. Only the three types this module emits.
#[derive(Debug, Clone, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(u64),
    Bool(bool),
}

/// One `key: value` attribute. Keys are `'static`: every attribute name is fixed in this crate.
pub type Attr = (&'static str, AttrValue);

/// How the statement ended — the ONE terminal the request was answered with.
#[derive(Debug)]
pub enum SpanOutcome<'a> {
    Ok,
    Error(&'a ErrorPayload),
    Cancelled,
}

/// A statement's measurements, recorded where a success terminal is built.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecResult {
    pub rows_returned: u64,
    pub rows_affected: u64,
    pub queue_us: u64,
    pub exec_us: u64,
    /// Bytes the engine sent for the result: the terminal, plus HEAD/DATA frames for a stream.
    pub response_bytes: u64,
}

/// A completed span, ready to export.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    /// The W3C trace flags the span was sampled under.
    pub trace_flags: u8,
    pub start_unix_nanos: u64,
    pub end_unix_nanos: u64,
    pub attrs: Vec<Attr>,
    /// `None` = success (status left unset); `Some(code name)` = an error terminal.
    pub error: Option<String>,
}

/// An EXEC's span while the statement runs. It is carried by the request's `Responder` and finished
/// by whichever `end_*` declares the ONE terminal — so one `END` is one span, by construction.
///
/// **Dropped unfinished, it still exports**, as an error with `error.type = NoTerminal`: a handler
/// that panics or returns without declaring a terminal gets a synthesized error `END` from the
/// supervisor, and the span says what the client saw rather than disappearing.
#[derive(Debug)]
pub struct ExecSpan {
    inner: Option<Pending>,
}

#[derive(Debug)]
struct Pending {
    tx: mpsc::Sender<SpanRecord>,
    counters: &'static ExportCounters,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    trace_flags: u8,
    start_unix_nanos: u64,
    started: Instant,
    attrs: Vec<Attr>,
    result: Option<ExecResult>,
}

impl ExecSpan {
    /// Record the statement's measurements. Called where the success terminal is built — the one
    /// place the counts and the `queue_us`/`exec_us` split are all known.
    pub fn record(&mut self, r: ExecResult) {
        if let Some(p) = self.inner.as_mut() {
            p.result = Some(r);
        }
    }

    /// Finish with the terminal the request was answered with.
    pub fn finish(mut self, outcome: SpanOutcome<'_>) {
        if let Some(p) = self.inner.take() {
            let error = match outcome {
                SpanOutcome::Ok => None,
                SpanOutcome::Error(ep) => Some((code_name(ep.code), Some(branch_name(ep.branch)))),
                SpanOutcome::Cancelled => Some(("Cancelled", None)),
            };
            p.emit(error);
        }
    }

    /// The span's id — for tests that need to tell spans apart.
    pub fn span_id(&self) -> Option<[u8; 8]> {
        self.inner.as_ref().map(|p| p.span_id)
    }
}

impl Drop for ExecSpan {
    fn drop(&mut self) {
        if let Some(p) = self.inner.take() {
            p.emit(Some(("NoTerminal", None)));
        }
    }
}

impl Pending {
    fn emit(self, error: Option<(&'static str, Option<&'static str>)>) {
        let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let mut attrs = self.attrs;
        if let Some(r) = self.result {
            attrs.push(("db.response.returned_rows", AttrValue::Int(r.rows_returned)));
            attrs.push(("ferro.rows_affected", AttrValue::Int(r.rows_affected)));
            attrs.push(("ferro.queue_us", AttrValue::Int(r.queue_us)));
            attrs.push(("ferro.exec_us", AttrValue::Int(r.exec_us)));
            attrs.push(("ferro.response_bytes", AttrValue::Int(r.response_bytes)));
        }
        if let Some((code, br)) = error {
            attrs.push(("error.type", AttrValue::Str(code.to_string())));
            if let Some(b) = br {
                attrs.push(("ferro.error.branch", AttrValue::Str(b.to_string())));
            }
        }
        let record = SpanRecord {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            trace_flags: self.trace_flags,
            start_unix_nanos: self.start_unix_nanos,
            end_unix_nanos: self.start_unix_nanos.saturating_add(elapsed),
            attrs,
            error: error.map(|(c, _)| c.to_string()),
        };
        // NEVER awaited: a full queue costs a span, not a statement.
        if self.tx.try_send(record).is_err() {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// An error code's registry NAME — a closed vocabulary, never the error's message.
fn code_name(code: u16) -> &'static str {
    errc::ALL
        .iter()
        .find(|&&(_, c, _)| c == code)
        .map_or("unregistered", |&(n, _, _)| n)
}

fn branch_name(b: u8) -> &'static str {
    branch::ALL
        .iter()
        .find(|&&(_, v)| v == b)
        .map_or("unregistered", |&(n, _)| n)
}

// ---------------------------------------------------------------------------------------------
// The tracer
// ---------------------------------------------------------------------------------------------

/// The exporter: decides which statements get a span, and ships finished spans to the collector.
#[derive(Debug)]
pub struct Tracer {
    tx: mpsc::Sender<SpanRecord>,
    sampler: Sampler,
    counters: &'static ExportCounters,
    stop: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Tracer {
    /// Start the export task. Must be called inside a tokio runtime (it spawns).
    pub fn start(cfg: OtlpConfig) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let (stop, stop_rx) = watch::channel(false);
        let task = tokio::spawn(export_loop(
            rx,
            cfg.endpoint,
            cfg.service_name,
            cfg.flush_interval,
            stop_rx,
            &COUNTERS,
        ));
        tracing::info!(sampler = ?cfg.sampler, "ferrod: OTLP trace export enabled");
        Self {
            tx,
            sampler: cfg.sampler,
            counters: &COUNTERS,
            stop,
            task: Mutex::new(Some(task)),
        }
    }

    /// Open a span for one EXEC, or `None` when this statement is not sampled. `attrs` is only
    /// called for a sampled statement, so an unsampled one never pays for its fingerprint.
    pub fn begin(
        &self,
        parent: Option<TraceParent>,
        attrs: impl FnOnce() -> Vec<Attr>,
    ) -> Option<ExecSpan> {
        let (trace_id, parent_span_id, trace_flags) = match (parent, self.sampler) {
            (Some(p), _) if p.sampled() => (p.trace_id, Some(p.parent_id), p.flags),
            // The caller decided not to sample: honour it, whatever the sampler says.
            (Some(_), _) => return None,
            (None, Sampler::ParentBasedAlwaysOn) => (new_trace_id(), None, 0x01),
            (None, Sampler::ParentBasedAlwaysOff) => return None,
        };
        Some(ExecSpan {
            inner: Some(Pending {
                tx: self.tx.clone(),
                counters: self.counters,
                trace_id,
                span_id: new_span_id(),
                parent_span_id,
                trace_flags,
                start_unix_nanos: now_unix_nanos(),
                started: Instant::now(),
                attrs: attrs(),
                result: None,
            }),
        })
    }

    /// Export what is queued and stop, waiting at most `budget`. Called once, at daemon exit.
    pub async fn shutdown(&self, budget: Duration) {
        let _ = self.stop.send(true);
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            let _ = tokio::time::timeout(budget, task).await;
        }
    }

    /// A tracer whose spans land on a channel the test reads directly, with private counters.
    #[cfg(test)]
    fn for_test(
        sampler: Sampler,
        capacity: usize,
        counters: &'static ExportCounters,
    ) -> (Self, mpsc::Receiver<SpanRecord>) {
        let (tx, rx) = mpsc::channel(capacity);
        let (stop, _) = watch::channel(false);
        (
            Self {
                tx,
                sampler,
                counters,
                stop,
                task: Mutex::new(None),
            },
            rx,
        )
    }
}

/// Collect spans and export them in batches: when one fills, on every flush interval, and once more
/// at shutdown.
async fn export_loop(
    mut rx: mpsc::Receiver<SpanRecord>,
    endpoint: Endpoint,
    service_name: String,
    flush_interval: Duration,
    mut stop: watch::Receiver<bool>,
    counters: &'static ExportCounters,
) {
    let mut batch: Vec<SpanRecord> = Vec::with_capacity(MAX_BATCH);
    let mut tick = tokio::time::interval(flush_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_warned: Option<Instant> = None;
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(span) => {
                    batch.push(span);
                    if batch.len() >= MAX_BATCH {
                        flush(&endpoint, &service_name, &mut batch, counters, &mut last_warned).await;
                    }
                }
                None => break,
            },
            _ = tick.tick() => {
                if !batch.is_empty() {
                    flush(&endpoint, &service_name, &mut batch, counters, &mut last_warned).await;
                }
            }
            _ = stop.changed() => break,
        }
    }
    while let Ok(span) = rx.try_recv() {
        batch.push(span);
        if batch.len() >= MAX_BATCH {
            flush(
                &endpoint,
                &service_name,
                &mut batch,
                counters,
                &mut last_warned,
            )
            .await;
        }
    }
    if !batch.is_empty() {
        flush(
            &endpoint,
            &service_name,
            &mut batch,
            counters,
            &mut last_warned,
        )
        .await;
    }
}

async fn flush(
    endpoint: &Endpoint,
    service_name: &str,
    batch: &mut Vec<SpanRecord>,
    counters: &ExportCounters,
    last_warned: &mut Option<Instant>,
) {
    let n = batch.len() as u64;
    let body = render_request(service_name, batch);
    batch.clear();
    let failure = match post(endpoint, body.as_bytes(), EXPORT_TIMEOUT).await {
        Ok(status) if (200..300).contains(&status) => {
            counters.exported.fetch_add(n, Ordering::Relaxed);
            None
        }
        Ok(status) => Some(format!("the collector answered HTTP {status}")),
        Err(e) => Some(e),
    };
    if let Some(why) = failure {
        counters.failed.fetch_add(n, Ordering::Relaxed);
        if last_warned.is_none_or(|t| t.elapsed() >= FAILURE_LOG_INTERVAL) {
            *last_warned = Some(Instant::now());
            tracing::warn!(
                spans = n,
                error = %why,
                "ferrod: OTLP export failed; the spans are dropped (counted in \
                 ferro_otlp_spans_failed_total; logged at most once a minute)",
            );
        }
    }
}

/// One `POST` of an export body. Returns the response's status code.
pub async fn post(endpoint: &Endpoint, body: &[u8], budget: Duration) -> Result<u16, String> {
    let exchange = async {
        let mut stream = tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .map_err(|e| format!("connect to {}: {e}", endpoint.host_header()))?;
        let head = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\nUser-Agent: ferrod/{}\r\n\r\n",
            endpoint.path,
            endpoint.host_header(),
            body.len(),
            env!("CARGO_PKG_VERSION"),
        );
        stream
            .write_all(head.as_bytes())
            .await
            .map_err(|e| format!("write: {e}"))?;
        stream
            .write_all(body)
            .await
            .map_err(|e| format!("write: {e}"))?;
        let mut buf = Vec::with_capacity(256);
        let mut chunk = [0u8; 512];
        while !buf.windows(2).any(|w| w == b"\r\n") && buf.len() < MAX_RESPONSE_HEAD {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|e| format!("read: {e}"))?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        parse_status_line(&buf)
    };
    tokio::time::timeout(budget, exchange)
        .await
        .map_err(|_| format!("no response within {budget:?}"))?
}

fn parse_status_line(buf: &[u8]) -> Result<u16, String> {
    let line_end = buf
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or("no HTTP status line in the response")?;
    let line = std::str::from_utf8(&buf[..line_end]).map_err(|_| "a non-UTF-8 status line")?;
    let mut parts = line.split(' ');
    match (parts.next(), parts.next()) {
        (Some(v), Some(code)) if v.starts_with("HTTP/1.") => code
            .parse::<u16>()
            .map_err(|_| format!("a malformed status line: {line:?}")),
        _ => Err(format!("a malformed status line: {line:?}")),
    }
}

// ---------------------------------------------------------------------------------------------
// OTLP/JSON
// ---------------------------------------------------------------------------------------------

/// Render one `ExportTraceServiceRequest` in OTLP's JSON encoding.
pub fn render_request(service_name: &str, spans: &[SpanRecord]) -> String {
    let mut out = String::with_capacity(256 + spans.len() * 512);
    out.push_str(r#"{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"#);
    json_str(&mut out, service_name);
    out.push_str(r#"}}]},"scopeSpans":[{"scope":{"name":"ferrod","version":"#);
    json_str(&mut out, env!("CARGO_PKG_VERSION"));
    out.push_str(r#"},"spans":["#);
    for (i, s) in spans.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        render_span(&mut out, s);
    }
    out.push_str("]}]}]}");
    out
}

fn render_span(out: &mut String, s: &SpanRecord) {
    out.push_str(r#"{"traceId":""#);
    out.push_str(&hex(&s.trace_id));
    out.push_str(r#"","spanId":""#);
    out.push_str(&hex(&s.span_id));
    out.push('"');
    let mut flags = u32::from(s.trace_flags);
    if let Some(p) = s.parent_span_id {
        out.push_str(r#","parentSpanId":""#);
        out.push_str(&hex(&p));
        out.push('"');
        flags |= FLAGS_PARENT_IS_REMOTE;
    }
    out.push_str(&format!(
        r#","flags":{flags},"name":"{SPAN_NAME}","kind":{SPAN_KIND_CLIENT},"startTimeUnixNano":"{}","endTimeUnixNano":"{}","attributes":["#,
        s.start_unix_nanos, s.end_unix_nanos,
    ));
    for (i, (k, v)) in s.attrs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(r#"{"key":"#);
        json_str(out, k);
        out.push_str(r#","value":{"#);
        match v {
            AttrValue::Str(v) => {
                out.push_str(r#""stringValue":"#);
                json_str(out, v);
            }
            // OTLP's intValue is an int64 carried as a JSON STRING; clamp rather than wrap.
            AttrValue::Int(n) => {
                out.push_str(&format!(r#""intValue":"{}""#, (*n).min(i64::MAX as u64)));
            }
            AttrValue::Bool(b) => out.push_str(&format!(r#""boolValue":{b}"#)),
        }
        out.push_str("}}");
    }
    out.push(']');
    if let Some(code) = &s.error {
        out.push_str(&format!(
            r#","status":{{"code":{STATUS_CODE_ERROR},"message":"#
        ));
        json_str(out, code);
        out.push('}');
    }
    out.push('}');
}

/// Append `s` as a JSON string literal.
fn json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 0x0f)] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    fn leak() -> &'static ExportCounters {
        Box::leak(Box::new(ExportCounters::new()))
    }

    #[test]
    fn no_endpoint_means_tracing_is_off() {
        assert_eq!(config_from(&env(&[])), Ok(None));
        assert_eq!(
            config_from(&env(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "  ")])),
            Ok(None),
            "a blank value reads as unset"
        );
        assert_eq!(
            config_from(&env(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
                ("OTEL_SDK_DISABLED", "TRUE"),
            ])),
            Ok(None)
        );
    }

    /// The OpenTelemetry exporter specification's rule: the signal-specific URL is used as given,
    /// the generic one gets `/v1/traces` appended — and the signal-specific one wins.
    #[test]
    fn endpoints_resolve_the_way_an_sdk_resolves_them() {
        let c = config_from(&env(&[(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://collector:4318/",
        )]))
        .unwrap()
        .unwrap();
        assert_eq!(c.endpoint.path, "/v1/traces");
        assert_eq!(
            (c.endpoint.host.as_str(), c.endpoint.port),
            ("collector", 4318)
        );
        let c = config_from(&env(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://ignored:1"),
            (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "http://127.0.0.1:9999/custom/path",
            ),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(c.endpoint.path, "/custom/path", "used AS GIVEN");
        assert_eq!(c.endpoint.port, 9999);
        assert_eq!(c.service_name, "ferrod");
        assert_eq!(
            c.sampler,
            Sampler::ParentBasedAlwaysOff,
            "the default roots nothing"
        );
        assert_eq!(c.flush_interval, DEFAULT_FLUSH_INTERVAL);
    }

    #[test]
    fn values_this_exporter_cannot_honour_are_refused_by_name() {
        let base = ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318");
        for (k, v) in [
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
            ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc"),
            ("OTEL_TRACES_SAMPLER", "always_on"),
            ("OTEL_TRACES_SAMPLER", "traceidratio"),
        ] {
            let e = config_from(&env(&[base, (k, v)])).expect_err(v);
            assert!(e.contains(v), "{e}");
        }
        for url in [
            "https://c:4318",
            "ftp://c",
            "http://",
            "http://:4318",
            "http://u:p@c:4318",
            "http://c:0",
            "http://c:99999",
            "http://c:4318?x=1",
            "http://[::1",
        ] {
            assert!(Endpoint::parse(url).is_err(), "{url} must be refused");
        }
        let ok = config_from(&env(&[
            base,
            ("OTEL_TRACES_SAMPLER", "parentbased_always_on"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
            ("OTEL_SERVICE_NAME", "billing-db"),
            ("OTEL_BSP_SCHEDULE_DELAY", "250"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(ok.sampler, Sampler::ParentBasedAlwaysOn);
        assert_eq!(ok.service_name, "billing-db");
        assert_eq!(ok.flush_interval, Duration::from_millis(250));
    }

    #[test]
    fn endpoint_parsing() {
        let e = Endpoint::parse("http://[::1]:4318/v1/traces").unwrap();
        assert_eq!(
            (e.host.as_str(), e.port, e.path.as_str()),
            ("::1", 4318, "/v1/traces")
        );
        assert_eq!(e.host_header(), "[::1]:4318");
        let e = Endpoint::parse("http://otel").unwrap();
        assert_eq!((e.port, e.path.as_str()), (80, "/"));
    }

    /// The sampling matrix: the caller's decision is honoured both ways, and with no caller the
    /// default exports nothing.
    #[test]
    fn sampling_follows_the_caller() {
        let sampled = TraceParent::parse(TP);
        let unsampled = TraceParent::parse(&TP.replace("-01", "-00"));
        for sampler in [Sampler::ParentBasedAlwaysOff, Sampler::ParentBasedAlwaysOn] {
            let (t, _rx) = Tracer::for_test(sampler, 8, leak());
            let mut s = t
                .begin(sampled, Vec::new)
                .expect("a sampled caller gets a span");
            let p = s.inner.as_ref().unwrap();
            assert_eq!(p.parent_span_id, Some(sampled.unwrap().parent_id));
            assert_eq!(p.trace_id, sampled.unwrap().trace_id);
            s.inner.take();
            assert!(
                t.begin(unsampled, Vec::new).is_none(),
                "{sampler:?}: an unsampled caller decided"
            );
        }
        let (off, _rx) = Tracer::for_test(Sampler::ParentBasedAlwaysOff, 8, leak());
        assert!(
            off.begin(None, || panic!("attrs must not be built"))
                .is_none()
        );
        let (on, _rx) = Tracer::for_test(Sampler::ParentBasedAlwaysOn, 8, leak());
        let mut root = on
            .begin(None, Vec::new)
            .expect("parentbased_always_on roots a trace");
        let p = root.inner.as_ref().unwrap();
        assert_eq!(p.parent_span_id, None);
        assert_ne!(p.trace_id, [0; 16]);
        assert_eq!(p.trace_flags, 0x01);
        root.inner.take();
    }

    /// One span per finish, carrying the outcome — and a span dropped UNFINISHED still exports, as
    /// the error the supervisor's synthesized terminal will report.
    #[tokio::test]
    async fn finishing_and_dropping_each_export_exactly_one_span() {
        let (t, mut rx) = Tracer::for_test(Sampler::ParentBasedAlwaysOff, 8, leak());
        let parent = TraceParent::parse(TP);

        let mut ok = t
            .begin(parent, || {
                vec![("ferro.pool", AttrValue::Str("main".into()))]
            })
            .unwrap();
        ok.record(ExecResult {
            rows_returned: 3,
            rows_affected: 0,
            queue_us: 7,
            exec_us: 9,
            response_bytes: 42,
        });
        ok.finish(SpanOutcome::Ok);
        let s = rx.try_recv().unwrap();
        assert_eq!(s.error, None);
        assert!(
            s.attrs
                .contains(&("db.response.returned_rows", AttrValue::Int(3)))
        );
        assert!(s.attrs.contains(&("ferro.queue_us", AttrValue::Int(7))));
        assert!(s.end_unix_nanos >= s.start_unix_nanos);

        let ep = ErrorPayload {
            code: errc::SYNTAX,
            branch: errc::SYNTAX_BRANCH,
            sqlstate: Some("42601".into()),
            errno: None,
            message: "syntax error at or near \"hunter2\"".into(),
            detail: None,
            retry_after_ms: None,
        };
        t.begin(parent, Vec::new)
            .unwrap()
            .finish(SpanOutcome::Error(&ep));
        let s = rx.try_recv().unwrap();
        assert_eq!(s.error.as_deref(), Some("Syntax"));
        assert!(
            s.attrs
                .contains(&("error.type", AttrValue::Str("Syntax".into())))
        );
        assert!(
            s.attrs
                .contains(&("ferro.error.branch", AttrValue::Str("NonRetryable".into())))
        );
        assert!(
            !format!("{s:?}").contains("hunter2"),
            "an error's MESSAGE quotes values and must never reach a span"
        );

        t.begin(parent, Vec::new)
            .unwrap()
            .finish(SpanOutcome::Cancelled);
        assert_eq!(rx.try_recv().unwrap().error.as_deref(), Some("Cancelled"));

        drop(t.begin(parent, Vec::new).unwrap());
        assert_eq!(rx.try_recv().unwrap().error.as_deref(), Some("NoTerminal"));

        assert!(rx.try_recv().is_err(), "exactly one span per EXEC");
    }

    /// A full queue drops and COUNTS — it never blocks. Exact, because these counters are this
    /// test's own.
    #[test]
    fn a_full_queue_drops_and_counts_and_never_blocks() {
        let counters = leak();
        let (t, _rx) = Tracer::for_test(Sampler::ParentBasedAlwaysOff, 1, counters);
        let parent = TraceParent::parse(TP);
        for _ in 0..3 {
            t.begin(parent, Vec::new).unwrap().finish(SpanOutcome::Ok);
        }
        assert_eq!(counters.dropped(), 2);
        assert_eq!(counters.exported(), 0);
    }

    #[test]
    fn ids_are_unique_and_never_zero() {
        let mut seen = HashSet::new();
        for _ in 0..200_000 {
            let id = new_span_id();
            assert_ne!(id, [0; 8]);
            assert!(seen.insert(id), "a span id repeated");
        }
        assert_ne!(new_trace_id(), new_trace_id());
    }

    /// The exact bytes of one request — the shape the collector probe accepted, with every value
    /// in the type OTLP/JSON expects (hex ids, int64 as a string, int enums).
    #[test]
    fn the_otlp_json_shape_is_exact() {
        let s = SpanRecord {
            trace_id: TraceParent::parse(TP).unwrap().trace_id,
            span_id: [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
            parent_span_id: Some([0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]),
            trace_flags: 0x01,
            start_unix_nanos: 1_759_416_000_000_000_000,
            end_unix_nanos: 1_759_416_000_001_000_000,
            attrs: vec![
                (
                    "db.query.text",
                    AttrValue::Str("select \"q\" from t where a = ?\\".into()),
                ),
                ("db.response.returned_rows", AttrValue::Int(3)),
                ("ferro.in_tx", AttrValue::Bool(false)),
            ],
            error: Some("Syntax".into()),
        };
        assert_eq!(
            render_request("ferrod", &[s]),
            format!(
                concat!(
                    r#"{{"resourceSpans":[{{"resource":{{"attributes":[{{"key":"service.name","value":{{"stringValue":"ferrod"}}}}]}},"#,
                    r#""scopeSpans":[{{"scope":{{"name":"ferrod","version":"{v}"}},"spans":[{{"#,
                    r#""traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"1122334455667788","#,
                    r#""parentSpanId":"00f067aa0ba902b7","flags":769,"name":"EXEC","kind":3,"#,
                    r#""startTimeUnixNano":"1759416000000000000","endTimeUnixNano":"1759416000001000000","#,
                    r#""attributes":[{{"key":"db.query.text","value":{{"stringValue":"select \"q\" from t where a = ?\\"}}}},"#,
                    r#"{{"key":"db.response.returned_rows","value":{{"intValue":"3"}}}},"#,
                    r#"{{"key":"ferro.in_tx","value":{{"boolValue":false}}}}],"#,
                    r#""status":{{"code":2,"message":"Syntax"}}}}]}}]}}]}}"#,
                ),
                v = env!("CARGO_PKG_VERSION"),
            )
        );
    }

    #[test]
    fn json_strings_escape_what_json_requires() {
        let mut out = String::new();
        json_str(&mut out, "a\"b\\c\nd\u{1}é");
        assert_eq!(out, r#""a\"b\\c\nd\u0001é""#);
    }

    #[test]
    fn status_lines() {
        assert_eq!(parse_status_line(b"HTTP/1.1 200 OK\r\n"), Ok(200));
        assert_eq!(parse_status_line(b"HTTP/1.0 400 Bad Request\r\nx"), Ok(400));
        assert!(parse_status_line(b"HTTP/1.1 200 OK").is_err(), "incomplete");
        assert!(parse_status_line(b"SSH-2.0-OpenSSH\r\n").is_err());
    }

    /// The request a collector receives: method, path, Host, and a Content-Length that matches the
    /// body — the one header a server needs to find the end of it.
    #[tokio::test]
    async fn post_sends_one_well_formed_request() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut got = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = s.read(&mut chunk).await.unwrap();
                got.extend_from_slice(&chunk[..n]);
                if got.ends_with(b"{\"x\":1}") || n == 0 {
                    break;
                }
            }
            s.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(got).unwrap()
        });
        let ep = Endpoint::parse(&format!("http://127.0.0.1:{}/v1/traces", addr.port())).unwrap();
        assert_eq!(post(&ep, b"{\"x\":1}", EXPORT_TIMEOUT).await, Ok(202));
        let req = server.await.unwrap();
        assert!(req.starts_with("POST /v1/traces HTTP/1.1\r\n"), "{req}");
        assert!(
            req.contains(&format!("\r\nHost: 127.0.0.1:{}\r\n", addr.port())),
            "{req}"
        );
        assert!(
            req.contains("\r\nContent-Type: application/json\r\n"),
            "{req}"
        );
        assert!(req.contains("\r\nContent-Length: 7\r\n"), "{req}");
        assert!(req.ends_with("\r\n\r\n{\"x\":1}"), "{req}");
    }

    /// A collector that accepts and never answers is bounded by the budget, not hung on.
    #[tokio::test]
    async fn a_silent_collector_is_bounded_by_the_budget() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let _hold = tokio::spawn(async move {
            let (_s, _) = l.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let ep = Endpoint::parse(&format!("http://127.0.0.1:{}/", addr.port())).unwrap();
        let t0 = Instant::now();
        let r = post(&ep, b"{}", Duration::from_millis(200)).await;
        assert!(r.is_err(), "{r:?}");
        assert!(t0.elapsed() < Duration::from_secs(2));
    }
}
