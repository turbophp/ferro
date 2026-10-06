//! P14 (SPEC §23.19): "`hyper`'s h1 client errors let the engine distinguish: EOF with zero
//! response bytes after sending; a reset or write error; a partial or malformed head; and a head over
//! the configured limits. If two are indistinguishable, the Guzzle mapping uses the stricter class
//! (`RequestException`) for both, and §23.11.3 is amended."
//!
//! Each fault is injected by a loopback upstream AFTER the request was sent, and the resulting
//! `hyper::Error` plus the tracker's observations are classified into the §23.5.6 "sent, no head"
//! cause tokens by `classify` below — the spike's version of what `ferro_http::fate` will do.

mod common;

use std::io;
use std::sync::atomic::Ordering;

use common::*;
use tokio::io::AsyncWriteExt;

/// hyper 1.11.1's `Display` for `Kind::Parse(Parse::TooLarge)` (`error.rs`, `description()`), which
/// both the read-buffer limit (`max_buf_size`) and the field-count limit (`httparse`
/// `TooManyHeaders`) produce.
const HYPER_TOO_LARGE: &str = "message head is too large";

/// What one failed exchange looked like to the engine.
#[derive(Debug)]
struct Observed {
    is_incomplete_message: bool,
    is_parse: bool,
    /// `Display` of the error. With D20's feature set (`client`, no `server`) hyper does NOT compile
    /// `Error::is_parse_too_large()` — it is `#[cfg(all(feature = "http1", feature = "server"))]` in
    /// hyper 1.11.1 — so the only public signal separating an oversize head from a malformed one is
    /// the error's `Display` text, `"message head is too large"`.
    display_text: String,
    io_kind: Option<io::ErrorKind>,
    /// Response bytes the plaintext layer delivered before the failure (the tracker's read count).
    bytes_received: u64,
    first_error: Option<(Dir, io::ErrorKind)>,
    sent: bool,
    upstream_received: u64,
    /// The error's `Debug`, for the report.
    debug: String,
}

fn io_kind(err: &hyper::Error) -> Option<io::ErrorKind> {
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<io::Error>() {
            return Some(io.kind());
        }
        src = s.source();
    }
    None
}

/// The cause classifier for "sent, no head", using ONLY what the engine can see: the `hyper::Error`
/// predicates and the plaintext tracker. `use_tracker = false` is the negative control: the same
/// classifier with the tracker's facts withheld.
fn classify(o: &Observed, use_tracker: bool) -> &'static str {
    // MUTATION SITE (M-P14b): dropping this arm files every oversize head as malformed_head.
    if o.is_parse && o.display_text == HYPER_TOO_LARGE {
        "oversize_head"
    } else if o.is_parse {
        "malformed_head"
    } else if o.is_incomplete_message {
        // hyper reports BOTH "closed before any response byte" and "closed mid-head" as
        // IncompleteMessage. Only the plaintext tracker's read count separates them.
        // MUTATION SITE (M-P14a): ignoring bytes_received collapses eof_partial_head into eof_empty.
        if use_tracker && o.bytes_received > 0 {
            "eof_partial_head"
        } else {
            "eof_empty"
        }
    } else if o.io_kind.is_some() {
        match (use_tracker, o.first_error) {
            (true, Some((Dir::Write, _))) => "write",
            _ => "reset",
        }
    } else {
        "unclassified"
    }
}

async fn run(fault: &'static str) -> Observed {
    const BIG: usize = 128 * 1024 * 1024; // past every socket buffer, so the write is still running
    let body_len = if fault == "write" { BIG } else { 64 };
    let up = fake_upstream(move |mut s, received| async move {
        match fault {
            "eof_empty" => {
                read_request(&mut s, &received, body_len).await;
                drop(s); // FIN, with zero response bytes
            }
            "reset" => {
                read_request(&mut s, &received, body_len).await;
                reset(s); // RST after the full request, zero response bytes
            }
            "write" => {
                // Read the head and a sliver of the body, then RST while the client still writes.
                read_request(&mut s, &received, 1024).await;
                reset(s);
            }
            "eof_partial_head" => {
                read_request(&mut s, &received, body_len).await;
                s.write_all(b"HTTP/1.1 200 OK\r\nContent-Le").await.unwrap();
                drop(s);
            }
            "partial_head_then_reset" => {
                read_request(&mut s, &received, body_len).await;
                s.write_all(b"HTTP/1.1 200 OK\r\nContent-Le").await.unwrap();
                s.flush().await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                reset(s);
            }
            "malformed_head" => {
                read_request(&mut s, &received, body_len).await;
                s.write_all(b"XTTP/1.1 200 OK\r\n\r\n").await.unwrap();
                let mut sink = [0u8; 1];
                let _ = tokio::io::AsyncReadExt::read(&mut s, &mut sink).await;
            }
            "oversize_head_fields" => {
                read_request(&mut s, &received, body_len).await;
                let mut head = String::from("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n");
                for i in 0..H1_MAX_HEADERS {
                    head.push_str(&format!("x-h{i}: v\r\n")); // 1 + 256 = 257 fields
                }
                head.push_str("\r\n");
                s.write_all(head.as_bytes()).await.unwrap();
                let mut sink = [0u8; 1];
                let _ = tokio::io::AsyncReadExt::read(&mut s, &mut sink).await;
            }
            "oversize_head_bytes" => {
                read_request(&mut s, &received, body_len).await;
                // 2 × max_buf_size: refused under every delivery shape (P15 measured that a head
                // only modestly over `max_buf_size` can be delivered when it arrives in one read).
                let big = "a".repeat(2 * H1_MAX_BUF_SIZE);
                let head = format!("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nx-big: {big}\r\n\r\n");
                let _ = s.write_all(head.as_bytes()).await;
                let mut sink = [0u8; 1];
                let _ = tokio::io::AsyncReadExt::read(&mut s, &mut sink).await;
            }
            other => unreachable!("{other}"),
        }
    })
    .await;

    let (mut send, track, _conn) = h1_over(dial(up.addr).await, &h1_builder()).await;
    within("ready", send.ready()).await.unwrap();
    track.arm();
    let err = within(fault, send.send_request(post(vec![0x42u8; body_len])))
        .await
        .expect_err("every fault here fails before a final head");
    let _ = up.done.await;
    Observed {
        is_incomplete_message: err.is_incomplete_message(),
        is_parse: err.is_parse(),
        display_text: err.to_string(),
        io_kind: io_kind(&err),
        bytes_received: track.read(),
        first_error: track.first_error(),
        sent: track.sent(),
        upstream_received: up.received.load(Ordering::SeqCst),
        debug: format!("{err:?}"),
    }
}

const FAULTS: [(&str, &str); 8] = [
    ("eof_empty", "eof_empty"),
    ("reset", "reset"),
    // An RST while the client is still writing the body. `write` or `reset` — see
    // `p14_a_reset_mid_body_write_is_usually_seen_on_the_read_side` for why both are accepted.
    ("write", "write|reset"),
    ("eof_partial_head", "eof_partial_head"),
    // A partial head followed by RST is the `reset` cause: bytes arrived, then the link died.
    ("partial_head_then_reset", "reset"),
    ("malformed_head", "malformed_head"),
    ("oversize_head_fields", "oversize_head"),
    ("oversize_head_bytes", "oversize_head"),
];

/// **P14.** Every "sent, no head" fault the engine must name is classified to its own §23.5.6 token
/// using only the `hyper::Error` and the plaintext tracker, and every one of them is `sent`.
#[tokio::test]
async fn p14_sent_no_head_causes_are_distinguishable_with_the_tracker() {
    let mut report = Vec::new();
    for (fault, want) in FAULTS {
        let o = run(fault).await;
        let got = classify(&o, true);
        report.push(format!("{fault:24} -> {got:16} {}", o.debug));
        assert!(
            o.sent,
            "{fault}: the request reached the upstream, so it is sent: {o:?}"
        );
        assert!(o.upstream_received > 0, "{fault}: upstream saw the request");
        assert!(
            want.split('|').any(|w| w == got),
            "{fault} misclassified as {got}, want {want}: {o:?}"
        );
    }
    eprintln!("P14 observations:\n{}", report.join("\n"));
}

/// **P14, the negative control and the caveat.** `hyper`'s error ALONE does not separate the two
/// EOF causes: "closed with zero response bytes" (curl 52, a `ConnectException`) and "closed
/// mid-head" (curl 56/8, a `RequestException`) are both `IncompleteMessage`, with no parse flag and
/// no I/O source. The distinction exists only because the tracker counts response bytes — so the
/// classifier with the tracker withheld must collapse them (and that is what this asserts).
#[tokio::test]
async fn p14_control_hyper_alone_conflates_eof_empty_and_eof_partial_head() {
    let empty = run("eof_empty").await;
    let partial = run("eof_partial_head").await;
    for o in [&empty, &partial] {
        assert!(o.is_incomplete_message, "{o:?}");
        assert!(!o.is_parse && o.io_kind.is_none(), "{o:?}");
    }
    assert_eq!(empty.bytes_received, 0);
    assert!(partial.bytes_received > 0);
    assert_eq!(classify(&empty, false), classify(&partial, false));
    assert_ne!(classify(&empty, true), classify(&partial, true));
}

/// **P14, what hyper does with a 101.** It is not an error: `hyper` returns it as a response head
/// with status 101 (it does not parse a body after it). §23.5.2 calls 101 malformed and §23.7.1 puts
/// `informational_101` in "sent, no head", so the ENGINE must intercept a 101 head itself; hyper
/// will not. Other 1xx are consumed by hyper and never surface (asserted with a 103 here).
#[tokio::test]
async fn p14_a_101_reaches_the_engine_as_a_head_and_1xx_are_consumed() {
    for (lead, want) in [
        (
            &b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: x\r\n\r\n"[..],
            101u16,
        ),
        (
            &b"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"[..],
            200u16,
        ),
    ] {
        let up = fake_upstream(move |mut s, received| async move {
            read_request(&mut s, &received, 0).await;
            s.write_all(lead).await.unwrap();
            let mut sink = [0u8; 1];
            let _ = tokio::io::AsyncReadExt::read(&mut s, &mut sink).await;
        })
        .await;
        let (mut send, track, _conn) = h1_over(dial(up.addr).await, &h1_builder()).await;
        within("ready", send.ready()).await.unwrap();
        track.arm();
        let resp = within("exchange", send.send_request(get()))
            .await
            .expect("hyper returns a head");
        assert_eq!(resp.status().as_u16(), want);
    }
}

/// **P14, the second caveat, pinned.** Under D20's exact hyper features an oversize head and a
/// malformed head are BOTH `is_parse()`, and the oversize predicate does not exist in this build (a
/// call to `is_parse_too_large()` does not compile — the first draft of this spike tried). What
/// separates them is the `Display` text. This test fails loudly if a hyper upgrade changes either
/// half: the two must stay distinguishable, by that text, for every oversize shape.
#[tokio::test]
async fn p14_oversize_and_malformed_differ_only_by_display_text() {
    let malformed = run("malformed_head").await;
    let by_fields = run("oversize_head_fields").await;
    let by_bytes = run("oversize_head_bytes").await;
    for o in [&malformed, &by_fields, &by_bytes] {
        assert!(o.is_parse, "{o:?}");
    }
    assert_eq!(by_fields.display_text, HYPER_TOO_LARGE);
    assert_eq!(by_bytes.display_text, HYPER_TOO_LARGE);
    assert_ne!(malformed.display_text, HYPER_TOO_LARGE, "{malformed:?}");
}

/// **P14, the first caveat, measured.** An upstream that resets the connection while the client is
/// still writing a large body is the `write` cause of §23.5.6 — but `hyper`'s h1 client reads for
/// an early response head WHILE it writes the body, and its dispatcher polls the read side first,
/// so the reset is normally observed on the READ side as `ConnectionReset` with nothing on the write
/// side. `write` and `reset` therefore cannot be told apart reliably. Both are the same Guzzle class
/// (`RequestException`, §23.11.3) and the same fate (§23.7.1 "sent, no head"), so nothing changes
/// but the token: the engine may name `write` only when the tracker saw the write fail first.
#[tokio::test]
async fn p14_a_reset_mid_body_write_is_usually_seen_on_the_read_side() {
    let mut tally = std::collections::BTreeMap::new();
    for _ in 0..5 {
        let o = run("write").await;
        assert!(o.sent, "{o:?}");
        assert!(
            matches!(
                o.io_kind,
                Some(io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe)
            ),
            "{o:?}"
        );
        *tally.entry(classify(&o, true)).or_insert(0) += 1;
    }
    eprintln!("P14 reset-mid-body-write classifications over 5 runs: {tally:?}");
    assert!(tally.keys().all(|k| *k == "write" || *k == "reset"));
}
