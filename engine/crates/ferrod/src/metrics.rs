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

use crate::pools::PoolRegistry;

/// The `Content-Type` Prometheus expects for the text exposition format.
const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The largest request head we will read before giving up. A scrape's request is a few hundred
/// bytes; this bounds a peer that opens a connection and streams headers forever.
const MAX_REQUEST_HEAD: usize = 8 * 1024;

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
    for name in names {
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
    out
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
