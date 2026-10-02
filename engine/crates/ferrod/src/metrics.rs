//! **SPEC §13's Prometheus endpoint (M2-C4b).**
//!
//! §13 lists four observability surfaces and C4a shipped the slow log first because its CONSUMER —
//! the `tracing` subscriber — already existed. This is the second, and the reason it comes before
//! `ferro top` is the same one: `ferro top` is "a live TUI over the admin service" in §13's own
//! words, and `ADMIN = 5` still has no method table (§22.2 (bo)); a Prometheus scrape is an HTTP
//! GET, which needs no `/proto` surface at all.
//!
//! **The exposition text is built separately from the socket that serves it**, so the format — the
//! part an operator's dashboards actually depend on — is tested without binding a port, and the
//! listener is tested for the handful of things a listener can get wrong.

use std::fmt::Write as _;

use std::sync::atomic::{AtomicU64, Ordering};

use ferro_proto::consts::{branch, errc};

use crate::pools::PoolRegistry;

/// The `Content-Type` Prometheus expects for the text exposition format.
const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The largest request head we will read before giving up. A scrape's request is a few hundred
/// bytes; this bounds a peer that opens a connection and streams headers forever.
const MAX_REQUEST_HEAD: usize = 8 * 1024;

/// SPEC §13's error-taxonomy counters (M2-C4b-2a): every error TERMINAL the daemon produces, by
/// registered code, plus the `indeterminate_total` §13 names explicitly.
///
/// **Process-wide, not per pool, and counted at the one place every `END` is built**
/// (`session::supervisor::build_terminal_frame`) — not at `classify_fate`, whose call sites are
/// spread across `services/sql.rs` and which never sees session-level errors (a codec fault, an
/// unknown flag bit, an unrouted service, a peercred denial). Counting at the chokepoint makes the
/// counter total by construction; counting at the classifier would make it total only for as long
/// as nobody adds a caller.
///
/// **Counted when the terminal is BUILT, not when it is written.** A terminal for a client that
/// has already disconnected is built, counted, and then dropped by the closed channel — so these
/// are terminals PRODUCED, not delivered. That is the reading an operator wants: the counter
/// measures the engine's own classification.
///
/// **The label vocabulary is GENERATED, never hand-kept:** `errc::ALL` and `branch::ALL` are
/// emitted by `ferro-proto`'s build script from `/proto/registry.lock.json` — itself generated
/// from `/proto/errors.toml` — so a code added to the registry appears here on the next build with
/// no edit to this file (charter rule 2; the C4b-1 pin-cause vocabulary is the precedent for what
/// a hand-kept list does instead).
pub struct ErrorMetrics {
    by_code: [AtomicU64; errc::ALL.len()],
    /// A terminal whose code is NOT in the registry. Should read 0 forever; any other value is a
    /// hand-written protocol constant somewhere, which charter rule 2 calls a defect.
    unregistered: AtomicU64,
    /// Terminals whose WIRE branch is `Indeterminate` — counted from the branch the client was
    /// actually told, not inferred from the code, because the §19.3 fate is what was SENT.
    indeterminate: AtomicU64,
}

/// The daemon's one error-taxonomy counter set.
pub static ERRORS: ErrorMetrics = ErrorMetrics::new();

impl ErrorMetrics {
    const fn new() -> Self {
        Self {
            by_code: [const { AtomicU64::new(0) }; errc::ALL.len()],
            unregistered: AtomicU64::new(0),
            indeterminate: AtomicU64::new(0),
        }
    }

    /// Record one error terminal.
    pub fn record(&self, code: u16, wire_branch: u8) {
        match errc::ALL.iter().position(|&(_, c, _)| c == code) {
            Some(i) => self.by_code[i].fetch_add(1, Ordering::Relaxed),
            None => self.unregistered.fetch_add(1, Ordering::Relaxed),
        };
        if wire_branch == branch::INDETERMINATE {
            self.indeterminate.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `(code name, registry branch name, count)` for every registered code, zeroes included.
    pub fn by_code(&self) -> Vec<(&'static str, &'static str, u64)> {
        errc::ALL
            .iter()
            .enumerate()
            .map(|(i, &(name, _, b))| {
                (
                    name,
                    branch_name(b),
                    self.by_code[i].load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// Terminals whose code is not in the registry.
    pub fn unregistered(&self) -> u64 {
        self.unregistered.load(Ordering::Relaxed)
    }

    /// Terminals sent with the `Indeterminate` branch.
    pub fn indeterminate(&self) -> u64 {
        self.indeterminate.load(Ordering::Relaxed)
    }
}

/// A branch value's registry name. A value outside the registry renders as `"unregistered"` — it
/// cannot happen for a code in `errc::ALL`, whose branches come from the same table.
fn branch_name(b: u8) -> &'static str {
    branch::ALL
        .iter()
        .find(|&&(_, v)| v == b)
        .map_or("unregistered", |&(n, _)| n)
}

/// Render the whole exposition body.
///
/// Every pool contributes every cause, including the zeroes — see `PinCause::ALL`. A series that
/// springs into existence on its first event is one an operator cannot write an alert against,
/// because "no data" and "it has not happened" are then the same observation.
pub fn render(registry: &PoolRegistry, boot_epoch: u64) -> String {
    let mut out = String::new();

    out.push_str("# HELP ferro_boot_epoch The engine's current boot epoch (SPEC §5/§19.1).\n");
    out.push_str("# TYPE ferro_boot_epoch gauge\n");
    let _ = writeln!(out, "ferro_boot_epoch {boot_epoch}");

    out.push_str(
        "# HELP ferro_pin_cause_total Connections pinned or tainted, by cause (SPEC §13).\n",
    );
    out.push_str("# TYPE ferro_pin_cause_total counter\n");
    // Sorted so a scrape's output is stable between calls — a diffable `/metrics` is worth the
    // negligible cost, and Prometheus itself does not care about order.
    let mut names: Vec<&str> = registry.names().collect();
    names.sort_unstable();
    for name in &names {
        let Some(pool) = registry.get(name) else {
            continue;
        };
        for (cause, count) in pool.pin_metrics_snapshot() {
            let _ = writeln!(
                out,
                "ferro_pin_cause_total{{pool=\"{}\",cause=\"{}\"}} {}",
                escape_label(name),
                cause.label(),
                count,
            );
        }
    }
    out.push_str(
        "# HELP ferro_hygiene_total Recycled connections by the hygiene they received (SPEC §13).\n",
    );
    out.push_str("# TYPE ferro_hygiene_total counter\n");
    for name in &names {
        let Some(pool) = registry.get(name) else {
            continue;
        };
        for (outcome, count) in pool.hygiene_snapshot() {
            let _ = writeln!(
                out,
                "ferro_hygiene_total{{pool=\"{}\",profile=\"{}\"}} {}",
                escape_label(name),
                outcome.label(),
                count,
            );
        }
    }

    // Three gauges, one family each, so each can carry its own HELP and be summed or ratioed in
    // PromQL (`in_use / max`) without a label that means three different things.
    for (metric, help, pick) in [
        (
            "ferro_pool_max_connections",
            "The pool's configured connection ceiling.",
            (|g: ferro_pool::pool::PoolGauges| g.max_size as u64) as fn(_) -> u64,
        ),
        (
            "ferro_pool_in_use_connections",
            "Connections handed out, being dialled, or held by the liveness reaper.",
            |g: ferro_pool::pool::PoolGauges| g.in_use as u64,
        ),
        (
            "ferro_pool_idle_connections",
            "Connections parked and ready for reuse.",
            |g: ferro_pool::pool::PoolGauges| g.idle as u64,
        ),
        (
            "ferro_pool_pinned_connections",
            "Connections pinned to an explicit transaction (SPEC §13).",
            |g: ferro_pool::pool::PoolGauges| g.pinned,
        ),
        (
            "ferro_pool_waiting_checkouts",
            "Checkouts waiting for a connection permit: SPEC §13's queue depth.",
            |g: ferro_pool::pool::PoolGauges| g.waiting as u64,
        ),
    ] {
        let _ = writeln!(out, "# HELP {metric} {help}");
        let _ = writeln!(out, "# TYPE {metric} gauge");
        for name in &names {
            let Some(pool) = registry.get(name) else {
                continue;
            };
            let _ = writeln!(
                out,
                "{metric}{{pool=\"{}\"}} {}",
                escape_label(name),
                pick(pool.gauges()),
            );
        }
    }

    // The histograms are read AFTER the gauges, and the pin pair depends on it: `PinSlot::release`
    // observes the duration BEFORE it decrements `pinned` (Release/Acquire), so a scrape that has
    // already read a pin's end off the gauge then reads its duration too. Reversing these blocks
    // would let one scrape show `pinned` fallen with the histogram not yet moved.
    render_histogram(
        &mut out,
        "ferro_checkout_duration_seconds",
        "Time to obtain a connection: permit wait plus recycle hygiene or a fresh dial (SPEC §13 checkout p50/p99).",
        registry,
        &names,
        |p| p.checkout_histogram(),
    );
    render_histogram(
        &mut out,
        "ferro_pin_duration_seconds",
        "How long a connection stayed pinned to a transaction, from pin to release (SPEC §13).",
        registry,
        &names,
        |p| p.pin_histogram(),
    );

    out.push_str(
        "# HELP ferro_errors_total Error terminals produced, by registered code (SPEC §13).\n",
    );
    out.push_str("# TYPE ferro_errors_total counter\n");
    for (code, branch, count) in ERRORS.by_code() {
        let _ = writeln!(
            out,
            "ferro_errors_total{{code=\"{code}\",branch=\"{branch}\"}} {count}"
        );
    }
    out.push_str(
        "# HELP ferro_errors_unregistered_total Error terminals whose code is not in /proto (a defect if nonzero).\n",
    );
    out.push_str("# TYPE ferro_errors_unregistered_total counter\n");
    let _ = writeln!(
        out,
        "ferro_errors_unregistered_total {}",
        ERRORS.unregistered()
    );
    out.push_str(
        "# HELP ferro_indeterminate_total Error terminals produced with the Indeterminate branch (SPEC §13, §19.3).\n",
    );
    out.push_str("# TYPE ferro_indeterminate_total counter\n");
    let _ = writeln!(out, "ferro_indeterminate_total {}", ERRORS.indeterminate());
    out.push_str(
        "# HELP ferro_traceparent_invalid_total EXEC requests whose W3C traceparent did not parse; the context was dropped, and no request is refused for it (SPEC §13).\n",
    );
    out.push_str("# TYPE ferro_traceparent_invalid_total counter\n");
    let _ = writeln!(
        out,
        "ferro_traceparent_invalid_total {}",
        crate::trace::invalid_total()
    );
    // SPEC §13 OTLP export (M2-C4c-2). Present — as zeroes — when export is off, like every other
    // series here: "no data" and "nothing happened" must not be the same observation to an alert.
    let otlp = &crate::otlp::COUNTERS;
    for (name, help, value) in [
        (
            "ferro_otlp_spans_exported_total",
            "Spans an OTLP collector accepted (SPEC §13).",
            otlp.exported(),
        ),
        (
            "ferro_otlp_spans_dropped_total",
            "Spans dropped because the export queue was full; the statement was never delayed (SPEC §13).",
            otlp.dropped(),
        ),
        (
            "ferro_otlp_spans_failed_total",
            "Spans in an OTLP export that was refused, unreachable or timed out (SPEC §13).",
            otlp.failed(),
        ),
    ] {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {value}");
    }
    out
}

/// One histogram family in the exposition format: `_bucket{pool,le}` (cumulative, ending in
/// `+Inf`), `_sum` and `_count`, per pool, in seconds.
fn render_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    registry: &PoolRegistry,
    names: &[&str],
    read: impl Fn(&crate::pools::AnyPool) -> crate::pools::HistogramSnapshot,
) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} histogram");
    for pool_name in names {
        let Some(pool) = registry.get(pool_name) else {
            continue;
        };
        write_histogram_series(out, name, &escape_label(pool_name), &read(pool));
    }
}

/// One pool's `_bucket`/`_sum`/`_count` lines. Split from [`render_histogram`] so the UNITS can be
/// pinned by a test with a known snapshot: the histogram records microseconds and the exposition
/// wants seconds, and a render that printed the raw µs — a 10⁶× error on every bound and every sum
/// — passed every test that only checked the series existed (review finding F2).
fn write_histogram_series(
    out: &mut String,
    name: &str,
    pool_label: &str,
    snap: &crate::pools::HistogramSnapshot,
) {
    use ferro_pool::histogram::fmt_seconds;
    for &(le, cum) in &snap.buckets {
        let le = le.map_or_else(|| "+Inf".to_string(), fmt_seconds);
        let _ = writeln!(
            out,
            "{name}_bucket{{pool=\"{pool_label}\",le=\"{le}\"}} {cum}"
        );
    }
    let _ = writeln!(
        out,
        "{name}_sum{{pool=\"{pool_label}\"}} {}",
        fmt_seconds(snap.sum_us)
    );
    let _ = writeln!(out, "{name}_count{{pool=\"{pool_label}\"}} {}", snap.count);
}

/// Escape a label VALUE per the exposition format: backslash, double quote, newline.
///
/// Only the `pool` label needs this — `cause` comes from [`PinCause::label`], a closed vocabulary
/// of bare identifiers (product-vision §5). A pool NAME is operator-supplied, so it is the one
/// place a stray quote could otherwise produce a line Prometheus parses as something else.
fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

/// Build the HTTP response for one request head.
///
/// Split out from the socket so the routing rules are testable as a pure function. Only `GET
/// /metrics` is served; everything else is a 404, and a malformed head is a 400. There is no other
/// route, no query handling and no body read — the endpoint's whole job is one scrape.
pub fn respond(head: &str, body: impl FnOnce() -> String) -> String {
    let Some(request_line) = head.lines().next() else {
        return simple(400, "Bad Request");
    };
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return simple(400, "Bad Request");
    };
    // The target may carry a query string; Prometheus does not send one, but a proxy may.
    // `split` always yields at least one piece, so this is total on any target.
    let path = target.split('?').next().unwrap_or_default();
    if method != "GET" {
        // 405 must name what IS allowed, or a client cannot tell a typo from a missing feature.
        return "HTTP/1.1 405 Method Not Allowed\r\nAllow: GET\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            .to_string();
    }
    if path != "/metrics" {
        return simple(404, "Not Found");
    }
    let body = body();
    // `String::len` is BYTES, which is exactly what Content-Length means — a pool name may be
    // non-ASCII, and "fixing" this to a character count would truncate the scrape.
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {EXPOSITION_CONTENT_TYPE}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    )
}

fn simple(code: u16, reason: &str) -> String {
    format!("HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
}

/// The request head, read up to `\r\n\r\n` and bounded by [`MAX_REQUEST_HEAD`].
pub(crate) async fn read_head<R>(mut r: R) -> std::io::Result<String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt as _;
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while buf.len() < MAX_REQUEST_HEAD {
        let n = r.read(&mut byte).await?;
        if n == 0 {
            break;
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Bind the scrape endpoint and serve it until `shutdown` resolves.
///
/// One connection at a time is deliberate. A scrape endpoint serves one client every few seconds;
/// spawning per connection would let an unauthenticated peer (it is loopback, but loopback is not
/// nobody — every process on the host can reach it) open connections faster than they complete.
/// Serving sequentially with a read deadline bounds the whole surface to one in-flight request,
/// and a scrape that queues behind another scrape is the correct behaviour anyway.
pub async fn serve(
    listener: tokio::net::TcpListener,
    registry: std::sync::Arc<PoolRegistry>,
    boot_epoch: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    use tokio::io::AsyncWriteExt as _;
    loop {
        let (mut sock, _peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(v) => v,
                // An accept error must not kill the endpoint: the daemon's real work is on the
                // UDS, and a metrics listener that silently stops is worse than one that skips a
                // scrape, because the dashboard goes flat rather than erroring.
                Err(e) => {
                    tracing::warn!(error = %e, "metrics accept failed");
                    continue;
                }
            },
            _ = shutdown.changed() => return,
        };

        // A peer that connects and never speaks holds the (sequential) loop, so the read is
        // deadlined rather than trusted.
        let head =
            match tokio::time::timeout(std::time::Duration::from_secs(5), read_head(&mut sock))
                .await
            {
                Ok(Ok(h)) => h,
                _ => continue,
            };

        let response = respond(&head, || render(&registry, boot_epoch));
        let _ = sock.write_all(response.as_bytes()).await;
        let _ = sock.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bounds and sums render in SECONDS. A known snapshot pins the exact lines, so a render that
    /// printed microseconds (or any other unit) cannot pass by merely producing the series.
    #[test]
    fn histogram_series_render_in_seconds_with_count_from_the_inf_bucket() {
        let snap = crate::pools::HistogramSnapshot {
            buckets: vec![(Some(25), 1), (Some(1_500_000), 3), (None, 4)],
            sum_us: 2_000_075,
            count: 4,
        };
        let mut out = String::new();
        write_histogram_series(&mut out, "ferro_x_seconds", "p", &snap);
        assert_eq!(
            out,
            "ferro_x_seconds_bucket{pool=\"p\",le=\"0.000025\"} 1\n\
             ferro_x_seconds_bucket{pool=\"p\",le=\"1.5\"} 3\n\
             ferro_x_seconds_bucket{pool=\"p\",le=\"+Inf\"} 4\n\
             ferro_x_seconds_sum{pool=\"p\"} 2.000075\n\
             ferro_x_seconds_count{pool=\"p\"} 4\n",
        );
    }

    /// Only `GET /metrics` is the endpoint; everything else has a defined, boring answer.
    #[test]
    fn only_get_metrics_is_served() {
        let body = || "BODY".to_string();
        let ok = respond("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", body);
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(
            ok.contains("Content-Type: text/plain; version=0.0.4"),
            "{ok}"
        );
        assert!(ok.ends_with("\r\n\r\nBODY"), "{ok}");
        assert!(
            ok.contains("Content-Length: 4"),
            "the length is the BODY's, not the frame's"
        );

        // A proxy may append a query string; Prometheus does not, but the path is what routes.
        assert!(respond("GET /metrics?x=1 HTTP/1.1\r\n\r\n", body).starts_with("HTTP/1.1 200"));

        assert!(respond("GET / HTTP/1.1\r\n\r\n", body).starts_with("HTTP/1.1 404"));
        assert!(respond("GET /metricsX HTTP/1.1\r\n\r\n", body).starts_with("HTTP/1.1 404"));

        // A write verb must not reach the body closure at all.
        let post = respond("POST /metrics HTTP/1.1\r\n\r\n", || {
            panic!("the body must not be rendered for a non-GET")
        });
        assert!(post.starts_with("HTTP/1.1 405"), "{post}");
        assert!(
            post.contains("Allow: GET"),
            "a 405 must name what is allowed: {post}"
        );

        assert!(respond("", body).starts_with("HTTP/1.1 400"));
        assert!(respond("GARBAGE\r\n\r\n", body).starts_with("HTTP/1.1 400"));
    }

    /// The error counter on a LOCAL instance — the global `ERRORS` is shared by every test in
    /// this binary, so exact counts are asserted only where nothing else can move them.
    #[test]
    fn error_metrics_count_by_registered_code_and_wire_branch() {
        let m = ErrorMetrics::new();
        m.record(errc::SYNTAX, errc::SYNTAX_BRANCH);
        m.record(errc::SYNTAX, errc::SYNTAX_BRANCH);
        m.record(errc::WRITE_UNCONFIRMED, errc::WRITE_UNCONFIRMED_BRANCH);
        let by = m.by_code();
        let syntax = by
            .iter()
            .find(|&&(n, _, _)| n == "Syntax")
            .expect("Syntax is registered");
        assert_eq!(syntax.2, 2);
        assert_eq!(
            syntax.1, "NonRetryable",
            "the label is the REGISTRY branch name"
        );
        assert_eq!(
            m.indeterminate(),
            1,
            "WriteUnconfirmed is sent Indeterminate (§19.3)"
        );
        assert_eq!(m.unregistered(), 0);

        // A code nobody registered is not dropped and not mis-filed: it has its own counter,
        // because a nonzero value there is a hand-written protocol constant (charter rule 2).
        m.record(0x7FFF, branch::NON_RETRYABLE);
        assert_eq!(m.unregistered(), 1);

        // The indeterminate count follows the branch that was SENT, not the code's usual one.
        m.record(errc::SYNTAX, branch::INDETERMINATE);
        assert_eq!(m.indeterminate(), 2);
    }

    /// Every registered code exports exactly once, zeroes included, so `errc::ALL` and the
    /// exposition cannot disagree about which series exist.
    #[test]
    fn every_registered_code_is_exported_once() {
        let m = ErrorMetrics::new();
        let by = m.by_code();
        assert_eq!(by.len(), errc::ALL.len());
        let mut names: Vec<&str> = by.iter().map(|&(n, _, _)| n).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), errc::ALL.len(), "two codes share a name");
        assert!(
            by.iter().all(|&(_, b, _)| b != "unregistered"),
            "a registered code has an unregistered branch"
        );
    }

    /// A production line that USES the `END` flag: `flags::END` under any path, or an import that
    /// brings a bare `END` (or every flag) into scope. Comments and assertions are not uses.
    ///
    /// **History, because each version was proven too narrow by a mutation.** v1 matched one exact
    /// spelling (`flags: flags::END`), and a fully qualified path slipped past it. v2 matched any
    /// `flags:` FIELD INITIALISER carrying the token, and the adversarial review then built an END
    /// frame with `header.flags |= flags::END` — not a field initialiser — which v2 did not see either.
    /// This version does not care HOW the flag is applied, only that it is named; the one sanctioned
    /// naming is inside `build_terminal_frame`.
    fn uses_end_flag(line: &str) -> bool {
        let t = line.trim_start();
        if t.starts_with("//") || t.contains("assert") {
            return false;
        }
        // Pieces, so this function's own source never matches itself.
        let qualified = concat!("flags", "::", "END");
        let named = t.match_indices(qualified).any(|(at, _)| {
            let after = t.as_bytes().get(at + qualified.len()).copied();
            !after.is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        });
        let imports_bare = t.starts_with("use ")
            && t.contains(concat!("flags", "::"))
            && (t.contains(concat!("flags", "::*"))
                || t.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .any(|tok| tok == concat!("E", "ND")));
        named || imports_bare
    }

    #[test]
    fn the_end_flag_matcher_sees_every_use_and_nothing_else() {
        // `\x3a\x3a` is `::` — escaped so these literals are not themselves uses to the scan below.
        for yes in [
            "        flags: flags\x3a\x3aEND,",
            "        flags: ferro_proto\x3a\x3aconsts\x3a\x3aflags\x3a\x3aEND,",
            "        header.flags |= flags\x3a\x3aEND;",
            "        frame.header.flags = flags\x3a\x3aEND | flags\x3a\x3aSTREAM;",
            "const END_FLAGS: u16 = flags\x3a\x3aEND;",
            "use ferro_proto\x3a\x3aconsts\x3a\x3aflags\x3a\x3aEND;",
            "use ferro_proto\x3a\x3aconsts\x3a\x3aflags\x3a\x3a{END, STREAM};",
            "use ferro_proto\x3a\x3aconsts\x3a\x3aflags\x3a\x3a*;",
        ] {
            assert!(uses_end_flag(yes), "missed {yes:?}");
        }
        for no in [
            "        flags: 0,",
            "        flags: flags\x3a\x3aSTREAM,",
            "        assert_eq!(frame.header.flags, flags\x3a\x3aEND);",
            "    // flags\x3a\x3aEND in a comment",
            "        flags: flags\x3a\x3aEND_OF_SOMETHING,",
            "use ferro_proto\x3a\x3aconsts\x3a\x3a{errc, flags, service};",
        ] {
            assert!(!uses_end_flag(no), "false positive on {no:?}");
        }
    }

    /// **The claim that makes the error counter total: exactly ONE place builds an `END` frame.**
    /// A second construction site would send error terminals the counter never sees, and nothing
    /// else would notice. This scans `ferrod`'s own non-test source for the construction spelling;
    /// assertions in tests compare `flags::END` with `==`/`assert_eq!` and do not match it.
    #[test]
    fn only_one_site_builds_an_end_frame() {
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(dir).expect("readable src dir") {
                let p = e.expect("dir entry").path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
        let mut files = Vec::new();
        walk(
            std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")),
            &mut files,
        );
        let mut sites = Vec::new();
        for f in files {
            let text = std::fs::read_to_string(&f).expect("readable source");
            // The WHOLE file, tests included: splitting at the first `#[cfg(test)]` would hide any
            // production code after an early test-only item. A test that ever builds an END frame
            // this way trips the guard — a loud false positive, which is the safe direction.
            for (n, line) in text.lines().enumerate() {
                if uses_end_flag(line) {
                    sites.push(format!("{}:{}", f.display(), n + 1));
                }
            }
        }
        assert_eq!(
            sites.len(),
            1,
            "expected exactly one use of the END flag in production code (supervisor::build_terminal_frame), found {sites:?}",
        );
        assert!(sites[0].contains("supervisor.rs"), "{sites:?}");
    }

    /// A pool name is operator-supplied, so it is the one label value that could otherwise break
    /// out of its quotes and forge a series.
    #[test]
    fn a_pool_name_cannot_break_out_of_its_label() {
        assert_eq!(escape_label("plain"), "plain");
        assert_eq!(escape_label(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_label(r"a\b"), r"a\\b");
        assert_eq!(escape_label("a\nb"), r"a\nb");
        // The shape that matters: a name that would otherwise close the label and forge another
        // series. The property is that EVERY quote in the output is backslash-escaped — not that
        // some substring is absent, which a first version of this assertion got wrong, since the
        // correctly-escaped `\",` still contains `",`.
        let forged = escape_label(r#"x",cause="tx"} 99999 #"#);
        let bytes: Vec<char> = forged.chars().collect();
        for (i, c) in bytes.iter().enumerate() {
            if *c == '"' {
                assert!(
                    i > 0 && bytes[i - 1] == '\\',
                    "an unescaped quote at {i} in {forged:?}",
                );
            }
        }
    }
}
