//! **M6-F4b — Ferro HTTP's body budgets, the HTTP drain, content decoding, and the chaos cases F4a
//! left (SPEC §23.6.1, §23.8.6, §23.9.2, §23.14 cases 4, 5, 12, 15, 16).**
//!
//! Every test drives the REAL handler factory over a real Unix socket against loopback upstreams
//! that record what they received, so "received 0" and "never re-sent" are read-back assertions
//! (charter rule 3). The drain tests drive the REAL `serve` — the accept loop `main` runs — and
//! trigger the same `Drain` the `SIGTERM` watcher does. The RSS half of chaos 6 is its own binary
//! (`http_rss_it.rs`), so no other test shares the process it measures.
#![cfg(feature = "http")]

mod common;
mod http_support;

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ferro_http::engine::{
    BoxIo, Connect, DrainView, ResponseSink, SinkError, SinkFrame, TcpConnect, Terminal,
};
use ferro_proto::consts::{branch, errc, flags, http_cause, method_http, service};
use ferro_proto::messages::{HttpDecoded, HttpHeaderField, HttpRequest, Outcome};
use http_support::*;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;

const MIB: usize = 1024 * 1024;
const OK_EMPTY: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

fn body_of(len: usize, tag: u8) -> Vec<u8> {
    (0..len)
        .map(|i| tag.wrapping_add((i % 251) as u8))
        .collect()
}

// =================================================================================================
// Chaos 16: body budgets (§23.8.6)
// =================================================================================================

/// An upstream that reads each whole request and answers it; on a stalled connection the read
/// never completes and the script waits for the engine to close it.
async fn answering() -> Upstream {
    upstream(|mut s, rec| async move {
        loop {
            if rec.read_request(&mut s).await.is_none() {
                rec.closed_by_peer.fetch_add(1, Ordering::SeqCst);
                return;
            }
            if rec.write(&mut s, OK_EMPTY).await.is_err() {
                return;
            }
        }
    })
    .await
}

/// **Chaos 16, per upstream.** A 3 MiB body that can never be fully written (its connection stops
/// accepting bytes after the head) holds 3 MiB of a 4 MiB `MAX_BODY_BYTES`; a concurrent 2 MiB
/// body is refused Retryable `PoolTimeout` (`body_budget`) and never dialled (received 0). Once the
/// first request ends (a `CANCEL`), the charge is back at 0 and the same 2 MiB body is admitted.
#[tokio::test]
async fn chaos16_a_body_past_the_upstream_budget_is_refused_unsent() {
    let up = answering().await;
    let stall = StallConnect::new(1024, 1);
    let d = daemon_with(
        upstreams(&[("u", up.addr)], &[("u", "MAX_BODY_BYTES", "4194304")]),
        stall,
        |_| {},
    );
    let mut c = d.client().await;

    send(&mut c, 2, &post("u", &body_of(3 * MIB, 1))).await;
    wait_for("A's head at the upstream", || up.rec.heads() == 1).await;
    assert_eq!(d.engine.body_budget_in_use("u"), 3 * MIB as u64);
    assert_eq!(d.engine.body_budget_daemon_in_use(), 3 * MIB as u64);

    let b = exchange(&mut c, 3, &post("u", &body_of(2 * MIB, 2))).await;
    b.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::BODY_BUDGET,
    );
    assert!(b.head.is_none());
    assert_eq!(up.rec.conns(), 1, "the refused request dialled nothing");
    assert_eq!(up.rec.heads(), 1, "received 0");
    assert_eq!(
        d.engine.body_budget_in_use("u"),
        3 * MIB as u64,
        "a refused charge takes nothing"
    );

    c.cancel(2).await;
    let a = collect(&mut c, 2).await;
    // A sent, non-idempotent POST cancelled before a head (§23.7.1).
    a.assert_error(
        errc::WRITE_UNCONFIRMED,
        branch::INDETERMINATE,
        http_cause::CANCELLED,
    );
    wait_for("the budget back at 0", || {
        d.engine.body_budget_in_use("u") == 0 && d.engine.body_budget_daemon_in_use() == 0
    })
    .await;

    let b = exchange(&mut c, 4, &post("u", &body_of(2 * MIB, 2))).await;
    b.done();
    assert_eq!(up.rec.requests().len(), 1, "B, once, on a fresh connection");
    assert_eq!(up.rec.requests()[0].body, body_of(2 * MIB, 2));
    assert_eq!(d.engine.body_budget_in_use("u"), 0);
}

/// **Chaos 16, daemon-wide.** Two upstreams with room each, a daemon budget that holds only one
/// of the two bodies: the second is refused `body_budget` even though its own upstream is empty.
#[tokio::test]
async fn chaos16_the_daemon_wide_budget_spans_upstreams() {
    let u1 = answering().await;
    let u2 = answering().await;
    let d = daemon_with(
        upstreams(
            &[("u1", u1.addr), ("u2", u2.addr)],
            &[
                ("u1", "MAX_BODY_BYTES", "4194304"),
                ("u2", "MAX_BODY_BYTES", "4194304"),
                ("", "MAX_BODY_BYTES", "5242880"),
            ],
        ),
        StallConnect::new(1024, 1),
        |_| {},
    );
    let mut c = d.client().await;
    send(&mut c, 2, &post("u1", &body_of(3 * MIB, 1))).await;
    wait_for("A's head", || u1.rec.heads() == 1).await;
    let b = exchange(&mut c, 3, &post("u2", &body_of(3 * MIB, 2))).await;
    b.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::BODY_BUDGET,
    );
    assert_eq!(u2.rec.conns(), 0, "received 0");
    assert_eq!(d.engine.body_budget_in_use("u2"), 0);
    c.cancel(2).await;
    collect(&mut c, 2).await;
    wait_for("both budgets at 0", || {
        d.engine.body_budget_daemon_in_use() == 0 && d.engine.body_budget_in_use("u1") == 0
    })
    .await;
}

/// **Released when fully written, not at the terminal (§23.8.6).** The upstream reads the whole
/// 3 MiB body and then sits on its answer: the charge is already back at 0 while the request is
/// still in flight, so a second 3 MiB body fits a 4 MiB budget at once.
#[tokio::test]
async fn the_budget_is_released_when_the_body_is_written_not_at_the_terminal() {
    let up = upstream(|mut s, rec| async move {
        while rec.read_request(&mut s).await.is_some() {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            if rec.write(&mut s, OK_EMPTY).await.is_err() {
                return;
            }
        }
    })
    .await;
    let d = daemon(upstreams(
        &[("u", up.addr)],
        &[("u", "MAX_BODY_BYTES", "4194304")],
    ));
    let mut c = d.client().await;
    send(&mut c, 2, &post("u", &body_of(3 * MIB, 1))).await;
    wait_for("the whole body at the upstream", || {
        up.rec.requests().len() == 1
    })
    .await;
    wait_for("the charge released once written", || {
        d.engine.body_budget_in_use("u") == 0
    })
    .await;
    assert!(
        c.recv_or_none(Duration::from_millis(20)).await.is_none(),
        "A is still in flight (no head, no terminal)"
    );
    send(&mut c, 3, &post("u", &body_of(3 * MIB, 2))).await;
    let mut done = 0;
    while done < 2 {
        let f = c.recv().await;
        if f.header.flags & flags::END == 0 {
            continue; // each one's HEAD
        }
        assert!(matches!(
            Outcome::decode(&f.payload).unwrap(),
            Outcome::Ok(_)
        ));
        done += 1;
    }
    assert_eq!(up.rec.requests().len(), 2);
    assert_eq!(d.engine.body_budget_daemon_in_use(), 0);
}

// =================================================================================================
// Chaos 12: session death
// =================================================================================================

/// **Chaos 12.** A session dies with three exchanges in flight — a POST whose body can never be
/// fully written (charged), a GET waiting for its head, and a GET whose body is parked on credit.
/// Every exchange is aborted, every upstream connection is closed, no connection is left in the
/// engine, and the budget is back at 0.
#[tokio::test]
async fn chaos12_session_death_leaks_no_connection_and_returns_the_budget() {
    let stalled = answering().await;
    let silent = silent().await;
    let streaming = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 1073741824\r\n\r\n";
        if rec.write(&mut s, head).await.is_err() {
            return;
        }
        let piece = vec![b'z'; 64 * 1024];
        while rec.write(&mut s, &piece).await.is_ok() {}
        rec.closed_by_peer.fetch_add(1, Ordering::SeqCst);
    })
    .await;
    let d = daemon_with(
        upstreams(
            &[
                ("stalled", stalled.addr),
                ("silent", silent.addr),
                ("streaming", streaming.addr),
            ],
            &[],
        ),
        StallConnect::new(1024, 1),
        |cfg| cfg.credit_frames = 2,
    );
    let mut c = d.client().await;
    send(&mut c, 2, &post("stalled", &body_of(3 * MIB, 7))).await;
    wait_for("the stalled POST's head", || stalled.rec.heads() == 1).await;
    send(&mut c, 3, &idempotent_get("silent")).await;
    wait_for("the GET at the silent upstream", || {
        silent.rec.requests().len() == 1
    })
    .await;
    send(&mut c, 4, &idempotent_get("streaming")).await;
    // HEAD + one BODY: the credit window (2 frames), never replenished.
    for _ in 0..2 {
        let f = c.recv().await;
        assert_eq!(f.header.request_id, 4);
    }
    assert_eq!(d.engine.in_flight(), 3);
    assert_eq!(d.engine.live_connections(), 3);
    assert_eq!(d.engine.body_budget_in_use("stalled"), 3 * MIB as u64);

    drop(c); // the session dies

    wait_for("every exchange aborted", || d.engine.in_flight() == 0).await;
    wait_for("no connection left", || d.engine.live_connections() == 0).await;
    assert_eq!(
        d.engine.body_budget_daemon_in_use(),
        0,
        "the budget is back at 0"
    );
    for (name, rec) in [
        ("stalled", &stalled.rec),
        ("silent", &silent.rec),
        ("streaming", &streaming.rec),
    ] {
        wait_for(&format!("{name}'s connection closed"), || {
            rec.closed_by_peer() == 1
        })
        .await;
    }
    for name in ["stalled", "silent", "streaming"] {
        assert_eq!(d.engine.idle_connections(name), 0, "{name} pooled nothing");
    }
}

/// A sink that never delivers (and ignores cancellation): the exchange parks in it forever.
struct BlackHole;
impl ResponseSink for BlackHole {
    fn send<'a>(
        &'a self,
        _: SinkFrame,
        _: Vec<u8>,
        _: tokio::time::Instant,
        _: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

/// **No connection outlives its exchange, even when the exchange's future is DROPPED** (a panic
/// unwinding through it, a task aborted, a runtime shutting down): dropping a tokio `JoinHandle`
/// does not stop `hyper`'s connection task, so before F4b such an exchange left its socket open.
#[tokio::test]
async fn a_dropped_exchange_closes_its_connection() {
    let up = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let _ = rec
            .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
            .await;
        rec.hold_until_closed(&mut s, Duration::from_secs(60)).await;
    })
    .await;
    let engine = engine_with(upstreams(&[("u", up.addr)], &[]), Arc::new(TcpConnect));
    let e = engine.clone();
    let task = tokio::spawn(async move {
        e.exchange(
            &idempotent_get("u"),
            None,
            Instant::now(),
            &CancellationToken::new(),
            &BlackHole,
        )
        .await
    });
    wait_for("the head written", || up.rec.written() > 0).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(engine.live_connections(), 1);
    task.abort();
    let _ = task.await;
    wait_for("the connection dropped", || engine.live_connections() == 0).await;
    wait_for("the upstream saw the close", || {
        up.rec.closed_by_peer() == 1
    })
    .await;
    assert_eq!(engine.in_flight(), 0);
}

/// The same, mid-WRITE: the exchange is dropped while `hyper` is still trying to write a body the
/// connection no longer accepts. Here `hyper`'s own connection task would wait on the write for
/// ever — nothing in `hyper` closes it — so only the engine's abort-on-drop closes the socket. (The
/// head-phase case above `hyper` closes by itself once its last handle is dropped, which is why
/// both are pinned.)
#[tokio::test]
async fn an_exchange_dropped_mid_write_closes_its_connection() {
    let up = answering().await;
    let engine = engine_with(
        upstreams(&[("u", up.addr)], &[]),
        StallConnect::new(1024, 1),
    );
    let e = engine.clone();
    let task = tokio::spawn(async move {
        e.exchange(
            &post("u", &body_of(3 * MIB, 3)),
            None,
            Instant::now(),
            &CancellationToken::new(),
            &BlackHole,
        )
        .await
    });
    wait_for("the head at the upstream", || up.rec.heads() == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(engine.body_budget_in_use("u"), 3 * MIB as u64);
    task.abort();
    let _ = task.await;
    wait_for("the connection dropped", || engine.live_connections() == 0).await;
    wait_for("the upstream saw the close", || {
        up.rec.closed_by_peer() == 1
    })
    .await;
    assert_eq!(engine.body_budget_daemon_in_use(), 0);
    assert_eq!(engine.in_flight(), 0);
}

/// An always-ready sink that records when each frame was sent, and cancels `cancel_at.1` once it
/// has accepted `cancel_at.0` frames — a sink that, unlike `ferrod`'s, checks nothing itself.
struct Recorder {
    frames: std::sync::Mutex<Vec<(SinkFrame, Instant, usize)>>,
    cancel_at: Option<(usize, CancellationToken)>,
    ticks: Arc<std::sync::atomic::AtomicUsize>,
}

impl Recorder {
    fn new(
        cancel_at: Option<(usize, CancellationToken)>,
        ticks: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Recorder {
            frames: std::sync::Mutex::new(Vec::new()),
            cancel_at,
            ticks,
        }
    }
}

impl ResponseSink for Recorder {
    fn send<'a>(
        &'a self,
        frame: SinkFrame,
        _: Vec<u8>,
        _: tokio::time::Instant,
        _: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        let mut f = self.frames.lock().unwrap();
        f.push((frame, Instant::now(), self.ticks.load(Ordering::SeqCst)));
        if let Some((n, t)) = &self.cancel_at
            && f.len() == *n
        {
            t.cancel();
        }
        Box::pin(async { Ok(()) })
    }
}

/// An endless gzip bomb upstream (~1000× per byte), every write as big as the socket takes.
async fn bomb_upstream() -> Upstream {
    let member = Arc::new(zeros_member(64 * MIB));
    upstream(move |mut s, rec| {
        let member = Arc::clone(&member);
        async move {
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            let head =
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n";
            if rec.write(&mut s, head).await.is_err() {
                return;
            }
            loop {
                let mut out = format!("{:x}\r\n", member.len()).into_bytes();
                out.extend_from_slice(&member);
                out.extend_from_slice(b"\r\n");
                if rec.write(&mut s, &out).await.is_err() {
                    return;
                }
            }
        }
    })
    .await
}

/// **The decode loop checks the deadline and the stop token between steps itself**, not only
/// through the sink: with a sink that checks nothing (`ferrod`'s does — this is the engine's own
/// bound, §23.9.2 "CPU bounded by the deadline"), no frame is sent after the deadline passed or
/// the request was stopped, though one network read of the bomb inflates to many MiB. Counted, not
/// timed: a frame recorded after the deadline is a frame decoded after it, however fast or slow
/// the machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_decode_loop_checks_the_deadline_and_the_stop_between_steps() {
    let up = bomb_upstream().await;
    let engine = engine_with(upstreams(&[("u", up.addr)], &[]), Arc::new(TcpConnect));
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // The deadline.
    let rec = Recorder::new(None, ticks.clone());
    let started = Instant::now();
    let t = engine
        .exchange(
            &HttpRequest {
                timeout_ms: Some(400),
                ..decode_get("/bomb")
            },
            None,
            started,
            &CancellationToken::new(),
            &rec,
        )
        .await;
    let deadline = started + Duration::from_millis(400);
    let (sent, late) = {
        let frames = rec.frames.lock().unwrap();
        let late = frames.iter().filter(|(_, at, _)| *at > deadline).count();
        (frames.len(), late)
    };
    assert!(sent > 10, "the bomb was decoding: {sent} frames");
    assert!(
        matches!(&t, ferro_http::engine::Terminal::Error(ep) if ep.detail.as_deref() == Some(http_cause::TIMEOUT)),
        "{t:?}"
    );
    assert!(
        late <= 1,
        "{late} frames decoded and sent after the deadline"
    );

    // The stop token (a CANCEL — and the drain cap, which fires the same token).
    let cancel = CancellationToken::new();
    let rec = Recorder::new(Some((20, cancel.clone())), ticks.clone());
    let t = engine
        .exchange(&decode_get("/bomb"), None, Instant::now(), &cancel, &rec)
        .await;
    assert_eq!(t, ferro_http::engine::Terminal::Cancelled);
    assert_eq!(
        rec.frames.lock().unwrap().len(),
        20,
        "no frame decoded after the stop"
    );
}

/// **The decode loop yields between steps.** On a current-thread runtime with an always-ready sink,
/// a bomb would otherwise hold the thread for a whole network read's worth of inflation (hundreds of
/// 256 KiB steps); a cooperative task beside it must keep running between frames.
#[tokio::test(flavor = "current_thread")]
async fn the_decode_loop_yields_between_steps() {
    let up = bomb_upstream().await;
    let engine = engine_with(upstreams(&[("u", up.addr)], &[]), Arc::new(TcpConnect));
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ticker = {
        let ticks = ticks.clone();
        tokio::spawn(async move {
            loop {
                ticks.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        })
    };
    let cancel = CancellationToken::new();
    let rec = Recorder::new(Some((400, cancel.clone())), ticks.clone());
    let _ = engine
        .exchange(&decode_get("/bomb"), None, Instant::now(), &cancel, &rec)
        .await;
    ticker.abort();
    let frames = rec.frames.lock().unwrap();
    // The longest run of consecutive BODY frames during which the ticker never ran.
    let mut longest = 0;
    let mut run = 0;
    for w in frames.windows(2) {
        if w[1].2 == w[0].2 {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    assert!(
        longest <= 2,
        "{longest} consecutive decode steps with no other task running"
    );
}

// =================================================================================================
// Content decoding (§23.9.2)
// =================================================================================================

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}
fn zlib(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}
fn raw_deflate(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

fn plain() -> Vec<u8> {
    (0..300_000u32)
        .flat_map(|i| format!("line {i}\n").into_bytes())
        .collect()
}

/// The response for a target: `/<encoding>/<shape>` where shape is `cl` (Content-Length) or
/// `chunked`.
fn encoded_response(target: &str) -> Vec<u8> {
    let data = plain();
    let (ce, body): (&str, Vec<u8>) = match target.split('/').nth(1).unwrap_or("") {
        "gzip" => ("gzip", gzip(&data)),
        "xgzip" => ("x-gzip", gzip(&data)),
        "mixedgzip" => ("GZip", gzip(&data)),
        // 5000 EMPTY members, then the data: far more members than one bounded decode step
        // starts, so the decoder must be stepped until it asks for input, never left holding
        // undecoded input at the end of a read (review round).
        "manyempty" => {
            let mut body = gzip(b"").repeat(5000);
            body.extend(gzip(&data));
            ("gzip", body)
        }
        "zlib" => ("deflate", zlib(&data)),
        "raw" => ("Deflate", raw_deflate(&data)),
        "br" => ("br", b"not really brotli".to_vec()),
        "stacked" => ("gzip, br", b"stacked".to_vec()),
        "corrupt" => {
            let mut g = gzip(&data);
            let n = g.len();
            g[n - 6] ^= 0xff; // the CRC32
            ("gzip", g)
        }
        "truncated" => {
            let g = gzip(&data);
            ("gzip", g[..g.len() / 2].to_vec())
        }
        other => panic!("unknown target {other}"),
    };
    let mut out =
        format!("HTTP/1.1 200 OK\r\nContent-Encoding: {ce}\r\nX-Kept: yes\r\n").into_bytes();
    if target.ends_with("/chunked") {
        out.extend_from_slice(b"Transfer-Encoding: chunked\r\n\r\n");
        for piece in body.chunks(7_000) {
            out.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            out.extend_from_slice(piece);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"0\r\n\r\n");
    } else {
        out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        out.extend_from_slice(&body);
    }
    out
}

async fn encoding_upstream() -> Upstream {
    upstream(|mut s, rec| async move {
        while let Some(seen) = rec.read_request(&mut s).await {
            let target = seen.head.split(' ').nth(1).unwrap_or("/").to_string();
            if rec.write(&mut s, &encoded_response(&target)).await.is_err() {
                return;
            }
        }
    })
    .await
}

fn decode_get(target: &str) -> HttpRequest {
    HttpRequest {
        decode: true,
        idempotent: Some(true),
        ..request("u", "GET", target)
    }
}

fn head_value<'a>(h: &'a [HttpHeaderField], name: &str) -> Option<&'a [u8]> {
    h.iter()
        .find(|f| f.name == name)
        .map(|f| f.value.as_slice())
}

/// **§23.9.2 end to end.** With `decode = true` and no `Accept-Encoding` from PHP, the engine asks
/// for `gzip, deflate`; a `gzip`, `x-gzip`, zlib-`deflate` or raw-`deflate` body — Content-Length
/// or chunked — arrives decoded, its head without `content-encoding`/`content-length`, both
/// reported in `decoded`. `br` and a stacked list pass through untouched with `decoded = nil`.
#[tokio::test]
async fn gzip_and_deflate_are_decoded_and_everything_else_passes_through() {
    let up = encoding_upstream().await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    let data = plain();
    let mut rid = 2;
    for (enc, received) in [
        ("gzip", "gzip"),
        ("xgzip", "x-gzip"),
        ("mixedgzip", "GZip"),
        ("manyempty", "gzip"),
        ("zlib", "deflate"),
        ("raw", "Deflate"),
    ] {
        for shape in ["cl", "chunked"] {
            let target = format!("/{enc}/{shape}");
            let r = exchange(&mut c, rid, &decode_get(&target)).await;
            rid += 1;
            r.done();
            let head = r.head.as_ref().unwrap();
            assert!(r.body == data, "{target}: the decoded body");
            assert_eq!(
                head_value(&head.headers, "content-encoding"),
                None,
                "{target}"
            );
            assert_eq!(
                head_value(&head.headers, "content-length"),
                None,
                "{target}"
            );
            assert_eq!(head_value(&head.headers, "x-kept"), Some(&b"yes"[..]));
            let wire_len = encoded_response(&target).len() as u64;
            let want_len = (shape == "cl").then(|| {
                // The Content-Length the upstream sent: the encoded body's length.
                let resp = encoded_response(&target);
                let head_end = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                (resp.len() - head_end) as u64
            });
            assert_eq!(
                head.decoded,
                Some(HttpDecoded {
                    content_encoding: received.into(),
                    content_length: want_len,
                }),
                "{target}"
            );
            assert_eq!(
                r.done().stats.bytes_received,
                wire_len,
                "wire bytes, not decoded ones"
            );
        }
    }
    let seen = up.rec.requests();
    assert!(
        seen.iter()
            .all(|s| s.header("accept-encoding").as_deref() == Some("gzip, deflate")),
        "the engine asked for what it decodes"
    );

    for target in ["/br/cl", "/stacked/cl"] {
        let r = exchange(&mut c, rid, &decode_get(target)).await;
        rid += 1;
        let head = r.head.as_ref().unwrap();
        assert_eq!(head.decoded, None, "{target} passes through");
        assert!(head_value(&head.headers, "content-encoding").is_some());
        assert!(head_value(&head.headers, "content-length").is_some());
        let resp = encoded_response(target);
        let head_end = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        assert_eq!(r.body, resp[head_end..], "{target}: untouched");
    }
}

/// `decode = false` is F4a's behaviour, unchanged: no `Accept-Encoding` is added and an encoded
/// body arrives encoded. A PHP-supplied `Accept-Encoding` is never replaced, and a body it asked
/// for in a coding the engine decodes is still decoded.
#[tokio::test]
async fn decode_false_and_a_php_accept_encoding_are_respected() {
    let up = encoding_upstream().await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    let r = exchange(
        &mut c,
        2,
        &HttpRequest {
            decode: false,
            ..decode_get("/gzip/cl")
        },
    )
    .await;
    assert_eq!(r.head.as_ref().unwrap().decoded, None);
    assert_eq!(r.body, gzip(&plain()), "still encoded");
    assert_eq!(up.rec.requests()[0].header("accept-encoding"), None);

    let r = exchange(
        &mut c,
        3,
        &HttpRequest {
            headers: vec![HttpHeaderField {
                name: "Accept-Encoding".into(),
                value: b"gzip".to_vec(),
            }],
            ..decode_get("/gzip/cl")
        },
    )
    .await;
    assert!(r.body == plain());
    assert_eq!(
        up.rec.requests()[1].header("accept-encoding").as_deref(),
        Some("gzip"),
        "PHP's own value, not replaced"
    );
}

/// **A corrupt or truncated encoded body is `ResponseIncomplete` (`decode`)** after the `HEAD`,
/// NonRetryable in both idempotency columns (§23.7.1), and the connection is discarded.
#[tokio::test]
async fn a_corrupt_or_truncated_body_is_response_incomplete_decode() {
    let up = encoding_upstream().await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    let mut rid = 2;
    for target in ["/corrupt/cl", "/truncated/cl", "/corrupt/chunked"] {
        for idem in [Some(true), None] {
            let r = exchange(
                &mut c,
                rid,
                &HttpRequest {
                    idempotent: idem,
                    ..decode_get(target)
                },
            )
            .await;
            rid += 1;
            assert!(r.head.is_some(), "{target}: the head was delivered");
            r.assert_error(
                errc::RESPONSE_INCOMPLETE,
                branch::NON_RETRYABLE,
                http_cause::DECODE,
            );
        }
    }
    assert_eq!(
        d.engine.idle_connections("u"),
        0,
        "never pooled after a decode failure"
    );
    wait_for("discarded", || d.engine.live_connections() == 0).await;
}

/// An EMPTY body is never a decode error: a `HEAD` response and a 204 routinely carry
/// `Content-Encoding: gzip` with nothing after the head.
#[tokio::test]
async fn an_empty_encoded_body_is_not_an_error() {
    let up = upstream(|mut s, rec| async move {
        while let Some(seen) = rec.read_request(&mut s).await {
            let resp: &[u8] = if seen.head.starts_with("HEAD ") {
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 1234\r\n\r\n"
            } else {
                b"HTTP/1.1 204 No Content\r\nContent-Encoding: gzip\r\n\r\n"
            };
            if rec.write(&mut s, resp).await.is_err() {
                return;
            }
        }
    })
    .await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    let r = exchange(
        &mut c,
        2,
        &HttpRequest {
            decode: true,
            ..request("u", "HEAD", "/x")
        },
    )
    .await;
    r.done();
    assert!(r.body.is_empty());
    assert_eq!(
        r.head.unwrap().decoded,
        Some(HttpDecoded {
            content_encoding: "gzip".into(),
            content_length: Some(1234),
        })
    );
    let r = exchange(&mut c, 3, &decode_get("/x")).await;
    r.done();
    assert_eq!(r.head.unwrap().status, 204);
}

/// A gzip stream of zeros, endless: each piece is a complete member of `piece` zeros compressed.
fn zeros_member(piece: usize) -> Vec<u8> {
    gzip(&vec![0u8; piece])
}

/// **A decompression bomb, memory side: the window bounds it** (§23.9.2). 512 MiB of zeros arrive
/// as ~0.5 MiB of gzip; a client that reads but never replenishes receives exactly the credit's
/// frames (the `HEAD` and 3 `BODY` of ≤ 256 KiB each) and nothing more, however much the engine
/// could still inflate — decoding stops with the parked frame.
#[tokio::test]
async fn a_bomb_is_bounded_by_the_window() {
    // Compressed BEFORE the upstream exists: in a debug build, compressing 64 MiB takes seconds.
    let member = Arc::new(zeros_member(64 * MIB));
    let up = upstream(move |mut s, rec| {
        let member = Arc::clone(&member);
        async move {
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            let mut out =
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n"
                    .to_vec();
            for _ in 0..8 {
                out.extend_from_slice(format!("{:x}\r\n", member.len()).as_bytes());
                out.extend_from_slice(&member);
                out.extend_from_slice(b"\r\n");
            }
            out.extend_from_slice(b"0\r\n\r\n");
            if rec.write(&mut s, &out).await.is_err() {
                return;
            }
            rec.hold_until_closed(&mut s, Duration::from_secs(60)).await;
        }
    })
    .await;
    let d = daemon_with(
        upstreams(&[("u", up.addr)], &[]),
        Arc::new(TcpConnect),
        |cfg| cfg.credit_frames = 4,
    );
    let mut c = d.client().await;
    send(&mut c, 2, &decode_get("/bomb")).await;
    let mut frames = 0;
    let mut decoded = 0usize;
    while let Some(f) = c.recv_or_none(Duration::from_millis(1000)).await {
        assert_eq!(f.header.flags & flags::END, 0, "no terminal while parked");
        frames += 1;
        if f.header.method == method_http::BODY {
            decoded += ferro_proto::messages::HttpBody::decode(&f.payload)
                .unwrap()
                .chunk
                .len();
        }
    }
    assert_eq!(frames, 4, "exactly the window: HEAD + 3 BODY");
    assert!(decoded <= 3 * 256 * 1024);
    c.cancel(2).await;
    let r = collect(&mut c, 2).await;
    assert!(matches!(r.end, Outcome::Cancelled));
}

/// **A decompression bomb, CPU side: the deadline bounds it** (§23.9.2). An endless gzip stream
/// whose every chunk inflates ~1000×, read by a client that always has credit, ends at the
/// request's total deadline with `QueryTimeout` (`timeout`), promptly — the deadline is checked
/// between decode steps, not only between the network reads one of which may inflate to hundreds
/// of MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bomb_is_bounded_by_the_deadline() {
    let member = Arc::new(zeros_member(256 * MIB));
    let up = upstream(move |mut s, rec| {
        let member = Arc::clone(&member);
        async move {
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            if rec
            .write(
                &mut s,
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
            )
            .await
            .is_err()
        {
            return;
        }
            loop {
                let mut out = format!("{:x}\r\n", member.len()).into_bytes();
                out.extend_from_slice(&member);
                out.extend_from_slice(b"\r\n");
                if rec.write(&mut s, &out).await.is_err() {
                    return;
                }
            }
        }
    })
    .await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    let started = Instant::now();
    let r = exchange(
        &mut c,
        2,
        &HttpRequest {
            timeout_ms: Some(1_000),
            ..decode_get("/bomb")
        },
    )
    .await;
    let took = started.elapsed();
    assert!(r.head.is_some());
    r.assert_error(
        errc::QUERY_TIMEOUT,
        branch::NON_RETRYABLE,
        http_cause::TIMEOUT,
    );
    eprintln!(
        "F4b decode deadline: terminal after {took:?}, {} B decoded",
        r.body.len()
    );
    assert!(
        took < Duration::from_millis(1_000 + 700),
        "the deadline stopped the decoding promptly: {took:?}"
    );
}

// =================================================================================================
// Chaos 5: slow-loris heads
// =================================================================================================

/// An upstream that reads the request, then dribbles a head one byte every 40 ms, forever.
async fn slow_loris_head() -> Upstream {
    upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        if rec
            .write(&mut s, b"HTTP/1.1 200 OK\r\nx-slow: ")
            .await
            .is_err()
        {
            rec.closed_by_peer.fetch_add(1, Ordering::SeqCst);
            return;
        }
        loop {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if rec.write(&mut s, b"a").await.is_err() {
                rec.closed_by_peer.fetch_add(1, Ordering::SeqCst);
                return;
            }
        }
    })
    .await
}

/// **Chaos 5.** A head that keeps arriving, one byte at a time, never completes: the request's
/// total deadline still fires — bytes arriving do not extend it — so a POST is Indeterminate
/// (`timeout`) and a declared GET `QueryTimeout`, and the connection is evicted (closed, never
/// pooled). Each received exactly once.
#[tokio::test]
async fn chaos5_a_slow_loris_head_is_ended_by_the_deadline_and_evicted() {
    let up = slow_loris_head().await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    for (rid, req, code, br) in [
        (
            2,
            post("u", b"charge"),
            errc::WRITE_UNCONFIRMED,
            branch::INDETERMINATE,
        ),
        (
            3,
            idempotent_get("u"),
            errc::QUERY_TIMEOUT,
            branch::NON_RETRYABLE,
        ),
    ] {
        let started = Instant::now();
        let r = exchange(
            &mut c,
            rid,
            &HttpRequest {
                timeout_ms: Some(700),
                ..req
            },
        )
        .await;
        let took = started.elapsed();
        assert!(r.head.is_none(), "no head ever completed");
        r.assert_error(code, br, http_cause::TIMEOUT);
        assert!(
            took >= Duration::from_millis(650) && took < Duration::from_millis(1_700),
            "the deadline, not the trickle, ended it: {took:?}"
        );
    }
    assert_eq!(up.rec.requests().len(), 2, "each received exactly once");
    wait_for("both connections closed by the engine", || {
        up.rec.closed_by_peer() == 2
    })
    .await;
    assert_eq!(d.engine.idle_connections("u"), 0, "evicted, never pooled");
    wait_for("no live connection", || d.engine.live_connections() == 0).await;
}

/// The body-phase slow loris: a body dribbled one byte every 40 ms defeats the idle bound by
/// design (bytes keep arriving) and is ended by the total deadline, `QueryTimeout` (`timeout`)
/// after the head.
#[tokio::test]
async fn a_slow_loris_body_is_ended_by_the_total_deadline() {
    let up = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        if rec
            .write(
                &mut s,
                b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n",
            )
            .await
            .is_err()
        {
            return;
        }
        while rec.write(&mut s, b"b").await.is_ok() {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        rec.closed_by_peer.fetch_add(1, Ordering::SeqCst);
    })
    .await;
    let d = daemon(upstreams(&[("u", up.addr)], &[]));
    let mut c = d.client().await;
    let r = exchange(
        &mut c,
        2,
        &HttpRequest {
            timeout_ms: Some(700),
            read_timeout_ms: Some(300),
            ..post("u", b"x")
        },
    )
    .await;
    assert!(r.head.is_some());
    r.assert_error(
        errc::QUERY_TIMEOUT,
        branch::NON_RETRYABLE,
        http_cause::TIMEOUT,
    );
    wait_for("closed by the engine", || up.rec.closed_by_peer() == 1).await;
}

// =================================================================================================
// Chaos 15: the HTTP drain (§23.6.1)
// =================================================================================================

/// One request's reply, and when its terminal arrived (relative to `t0`).
struct Timed {
    reply: Reply,
    at: Duration,
}

/// Collect every request in `rids` to its terminal, interleaved, replenishing credit. Panics if a
/// read waits longer than `wait`.
async fn collect_many(
    c: &mut common::TestClient,
    rids: &[u32],
    t0: Instant,
    wait: Duration,
) -> HashMap<u32, Timed> {
    let mut partial: HashMap<u32, Reply> = rids
        .iter()
        .map(|&r| {
            (
                r,
                Reply {
                    head: None,
                    body: Vec::new(),
                    body_frames: 0,
                    end: Outcome::Cancelled,
                },
            )
        })
        .collect();
    let mut done = HashMap::new();
    while done.len() < rids.len() {
        let f = c
            .recv_or_none(wait)
            .await
            .unwrap_or_else(|| panic!("no frame within {wait:?}; done: {:?}", done.keys()));
        let rid = f.header.request_id;
        assert_eq!(f.header.service, service::HTTP);
        let p = partial
            .get_mut(&rid)
            .expect("a frame for an expected request");
        if f.header.flags & flags::END != 0 {
            let mut reply = partial.remove(&rid).unwrap();
            reply.end = Outcome::decode(&f.payload).unwrap();
            done.insert(
                rid,
                Timed {
                    reply,
                    at: t0.elapsed(),
                },
            );
            continue;
        }
        if f.header.method == method_http::HEAD {
            p.head = Some(ferro_proto::messages::HttpHead::decode(&f.payload).unwrap());
        } else {
            p.body.extend_from_slice(
                &ferro_proto::messages::HttpBody::decode(&f.payload)
                    .unwrap()
                    .chunk,
            );
            p.body_frames += 1;
        }
        c.window_update(rid, 1, f.payload.len() as u32).await;
    }
    done
}

/// An upstream whose body never ends: a head, then a byte every 20 ms.
async fn endless_body() -> Upstream {
    upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        if rec
            .write(
                &mut s,
                b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n",
            )
            .await
            .is_err()
        {
            return;
        }
        while rec.write(&mut s, b"s").await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        rec.closed_by_peer.fetch_add(1, Ordering::SeqCst);
    })
    .await
}

/// **Chaos 15, scaled** (`FERRO_HTTP_DRAIN_MS` = 1.5 s, `drain_deadline` = 300 ms, through the real
/// `serve`): at the drain, in flight are a POST its upstream answers after 800 ms (§23.14's "10 s
/// POST"), a POST its upstream never answers (the "60 s POST"), a declared-idempotent GET that is
/// never answered, and two GETs mid-body.
///
/// - a NEW request during the drain is refused at once, Retryable `UpstreamUnavailable`
///   (`draining`), received 0 — chassis change 1: the session was told;
/// - the 800 ms POST completes `Ok` — AFTER `drain_deadline` has passed, which is chassis change
///   2: `serve` outlasted it because an HTTP exchange was in flight;
/// - at the 1.5 s cap the unanswered POST is Indeterminate (`draining`), received exactly 1; the
///   declared GET is Retryable `ConnectionLost` — what the engine says and §23.7.3 could not; the
///   GETs mid-body are `ResponseIncomplete` (undeclared) and Retryable (declared) — and every one
///   of those terminals reaches the client BEFORE the hard abort;
/// - then `serve` ends: `drain_deadline` after the last HTTP terminal, not at the 1.8 s sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos15_the_drain_refuses_new_requests_finishes_short_ones_and_caps_the_rest() {
    let slow = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            tokio::time::sleep(Duration::from_millis(800)).await;
            let _ = rec.write(&mut s, OK_EMPTY).await;
        }
    })
    .await;
    let stuck = silent().await;
    let stream = endless_body().await;
    let fresh = responder(OK_EMPTY).await;
    let s = served(
        upstreams(
            &[
                ("slow", slow.addr),
                ("stuck", stuck.addr),
                ("stream", stream.addr),
                ("fresh", fresh.addr),
            ],
            &[("", "DRAIN_MS", "1500")],
        ),
        Duration::from_millis(300),
    );
    let mut c = s.client().await;
    send(&mut c, 2, &post("slow", b"ten-second")).await;
    send(&mut c, 3, &post("stuck", b"sixty-second")).await;
    send(&mut c, 4, &idempotent_get("stuck")).await;
    send(&mut c, 5, &request("stream", "GET", "/feed")).await;
    send(&mut c, 6, &idempotent_get("stream")).await;
    wait_for("all five at their upstreams", || {
        slow.rec.requests().len() == 1
            && stuck.rec.requests().len() == 2
            && stream.rec.requests().len() == 2
    })
    .await;
    wait_for("both streams' heads written", || stream.rec.written() > 0).await;

    let t0 = Instant::now();
    s.drain.trigger();
    send(&mut c, 7, &post("fresh", b"during-the-drain")).await;
    let r = collect_many(&mut c, &[2, 3, 4, 5, 6, 7], t0, Duration::from_secs(3)).await;

    let e = &r[&7];
    e.reply.assert_error(
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::DRAINING,
    );
    assert!(
        e.at < Duration::from_millis(300),
        "refused at once: {:?}",
        e.at
    );
    assert_eq!(fresh.rec.conns(), 0, "received 0");

    let a = &r[&2];
    a.reply.done();
    assert!(
        a.at > Duration::from_millis(300),
        "the 800 ms POST finished after drain_deadline: {:?}",
        a.at
    );

    let b = &r[&3];
    b.reply.assert_error(
        errc::WRITE_UNCONFIRMED,
        branch::INDETERMINATE,
        http_cause::DRAINING,
    );
    assert!(
        b.at >= Duration::from_millis(1_400),
        "stopped at the cap, not before: {:?}",
        b.at
    );
    r[&4].reply.assert_error(
        errc::CONNECTION_LOST,
        branch::RETRYABLE,
        http_cause::DRAINING,
    );
    assert!(r[&5].reply.head.is_some() && r[&6].reply.head.is_some());
    r[&5].reply.assert_error(
        errc::RESPONSE_INCOMPLETE,
        branch::NON_RETRYABLE,
        http_cause::DRAINING,
    );
    r[&6].reply.assert_error(
        errc::CONNECTION_LOST,
        branch::RETRYABLE,
        http_cause::DRAINING,
    );
    let bodies: Vec<Vec<u8>> = stuck.rec.requests().into_iter().map(|s| s.body).collect();
    assert_eq!(
        bodies
            .iter()
            .filter(|b| b.as_slice() == b"sixty-second")
            .count(),
        1,
        "the Indeterminate POST was received exactly once"
    );

    // The session is hard-closed `drain_deadline` after the last HTTP terminal: well before the
    // 1.8 s sum would have it, and never before the cap.
    tokio::time::timeout(Duration::from_secs(3), s.served)
        .await
        .expect("serve returned")
        .unwrap();
    let ended = t0.elapsed();
    assert!(
        ended >= Duration::from_millis(1_500) && ended < Duration::from_millis(2_600),
        "serve ended at {ended:?}"
    );
    assert_eq!(s.engine.in_flight(), 0);
    wait_for("no connection left", || s.engine.live_connections() == 0).await;
}

/// **The drain cap before a byte left is Retryable and unsent**, in both pre-send phases: a POST
/// dispatched to a connection that accepts no byte ("dispatched, not sent"), and a POST still
/// waiting for its connect ("before dispatch") are both `UpstreamUnavailable` (`draining`) at the
/// cap — the same answer a request refused at admission gets — and the upstream received nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_cap_before_any_byte_is_sent_is_retryable() {
    let up = answering().await;
    // Connection 0 accepts no request byte; connection 1's connect never completes.
    let s = served_with(
        upstreams(
            &[("u", up.addr)],
            &[
                ("", "DRAIN_MS", "500"),
                ("u", "CONNECT_TIMEOUT_MS", "30000"),
            ],
        ),
        Duration::from_millis(300),
        StallConnect::hanging(0, 1, 1),
    );
    let mut c = s.client().await;
    send(&mut c, 2, &post("u", b"never-leaves")).await;
    wait_for("the first connection open", || up.rec.conns() == 1).await;
    send(&mut c, 3, &post("u", b"never-dialled")).await;
    wait_for("both in flight", || s.engine.in_flight() == 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t0 = Instant::now();
    s.drain.trigger();
    let r = collect_many(&mut c, &[2, 3], t0, Duration::from_secs(3)).await;
    for rid in [2, 3] {
        r[&rid].reply.assert_error(
            errc::UPSTREAM_UNAVAILABLE,
            branch::RETRYABLE,
            http_cause::DRAINING,
        );
        assert!(
            r[&rid].at >= Duration::from_millis(450),
            "at the cap: {:?}",
            r[&rid].at
        );
    }
    assert_eq!(
        up.rec.bytes_in.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
    assert_eq!(up.rec.conns(), 1, "the second POST never connected");
}

/// **The extension ends early, and only when HTTP is in flight.** An idle session drains in
/// `drain_deadline` exactly as before F4b (1 s here: under 1.6 s, so an extension entered with
/// nothing in flight — another `drain_deadline` — fails it); one 400 ms exchange in flight at a 5 s
/// cap ends `serve` `drain_deadline` after its terminal, not at the 5.3 s sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_extension_is_bounded_by_the_last_http_terminal() {
    let slow = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let _ = rec.write(&mut s, OK_EMPTY).await;
        }
    })
    .await;
    let env = upstreams(&[("slow", slow.addr)], &[("", "DRAIN_MS", "5000")]);

    // An idle session: unchanged.
    let s = served(env.clone(), Duration::from_secs(1));
    let _c = s.client().await;
    let t0 = Instant::now();
    s.drain.trigger();
    s.served.await.unwrap();
    let idle = t0.elapsed();
    assert!(
        idle >= Duration::from_millis(980) && idle < Duration::from_millis(1_600),
        "an idle session drains in drain_deadline, with no extension: {idle:?}"
    );

    // One short exchange in flight.
    let s = served(env, Duration::from_millis(300));
    let mut c = s.client().await;
    send(&mut c, 2, &post("slow", b"short")).await;
    wait_for("at the upstream", || {
        slow.rec.requests().len() == 2 || slow.rec.requests().len() == 1
    })
    .await;
    let t0 = Instant::now();
    s.drain.trigger();
    let r = collect_within(&mut c, 2, Duration::from_secs(3)).await;
    r.done();
    s.served.await.unwrap();
    let ended = t0.elapsed();
    assert!(
        ended < Duration::from_millis(1_500),
        "ended drain_deadline after the last terminal, not at the 5.3 s sum: {ended:?}"
    );
}

// =================================================================================================
// Chaos 4: the stale keep-alive residual (§23.8.2)
// =================================================================================================

#[derive(Default, Debug)]
struct Tally {
    ok: u32,
    indeterminate: u32,
    retryable_unsent: u32,
}

/// Run `n` POSTs on one session with the given idle gaps; every body is unique, and the upstream
/// must have received each AT MOST once (charter rule 3). Outcomes are tallied.
async fn run_posts(
    c: &mut common::TestClient,
    rec: &Rec,
    gaps: &[Duration],
    first_rid: u32,
) -> Tally {
    let mut t = Tally::default();
    for (i, gap) in gaps.iter().enumerate() {
        tokio::time::sleep(*gap).await;
        let body = format!("post-{first_rid}-{i}").into_bytes();
        let r = exchange(c, first_rid + i as u32, &post("u", &body)).await;
        match &r.end {
            Outcome::Ok(_) => t.ok += 1,
            Outcome::Error(ep) if ep.branch == branch::INDETERMINATE => t.indeterminate += 1,
            Outcome::Error(ep)
                if ep.branch == branch::RETRYABLE
                    && matches!(
                        ep.detail.as_deref(),
                        Some(http_cause::UNSENT_CLOSED | http_cause::UNSENT_WRITE)
                    ) =>
            {
                t.retryable_unsent += 1
            }
            other => panic!("POST {i}: neither success, Indeterminate nor unsent: {other:?}"),
        }
        let received = rec.requests().iter().filter(|s| s.body == body).count();
        assert!(received <= 1, "POST {i} received {received} times: re-sent");
    }
    t
}

/// The "crossing" upstream: a keep-alive server whose idle close crossed the next request — it
/// reads a request that arrives on a connection idle for ≥ 100 ms, and closes without answering.
/// The race §23.8.2 narrows, made deterministic.
async fn crossing_upstream() -> Upstream {
    upstream(|mut s, rec| async move {
        let mut last: Option<Instant> = None;
        loop {
            let Some(_seen) = rec.read_request(&mut s).await else {
                return;
            };
            if last.is_some_and(|l| l.elapsed() >= Duration::from_millis(100)) {
                return; // closed without an answer: the request crossed the close
            }
            if rec.write(&mut s, OK_EMPTY).await.is_err() {
                return;
            }
            last = Some(Instant::now());
        }
    })
    .await
}

/// A keep-alive upstream with a real 100 ms idle timer: it closes a connection that has been idle
/// that long (a FIN), the way servers do.
async fn idle_timer_upstream() -> Upstream {
    upstream(|mut s, rec| async move {
        loop {
            let got = tokio::time::timeout(Duration::from_millis(100), async {
                let mut b = [0u8; 1];
                s.peek(&mut b).await
            })
            .await;
            match got {
                Err(_) | Ok(Ok(0)) | Ok(Err(_)) => return,
                Ok(Ok(_)) => {}
            }
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            if rec.write(&mut s, OK_EMPTY).await.is_err() {
                return;
            }
        }
    })
    .await
}

fn gaps(n: usize, lo_ms: u64, hi_ms: u64) -> Vec<Duration> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            Duration::from_millis(lo_ms + x % (hi_ms - lo_ms + 1))
        })
        .collect()
}

/// **Chaos 4.** The stale keep-alive race: a POST is a success or Indeterminate (or, when the
/// close is noticed before a byte leaves, Retryable `unsent_*`), and is NEVER re-sent. The
/// residual Indeterminate rate is measured at `H1_UNSAFE_REUSE_MAX_IDLE_MS` = its default (2 s)
/// and = 0, against a server whose close crosses the request (deterministic) and one with a real
/// 100 ms idle timer (the race as it happens). At 0 a POST never reuses an idle connection, so the
/// residual is 0 — at the cost of a dial per POST.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos4_the_stale_keep_alive_residual_at_the_default_and_at_zero() {
    for (model, unsafe_idle) in [
        ("crossing", None),
        ("crossing", Some("0")),
        ("idle-timer", None),
        ("idle-timer", Some("0")),
    ] {
        let up = if model == "crossing" {
            crossing_upstream().await
        } else {
            idle_timer_upstream().await
        };
        let extra: Vec<(&str, &str, &str)> = unsafe_idle
            .map(|v| vec![("u", "H1_UNSAFE_REUSE_MAX_IDLE_MS", v)])
            .unwrap_or_default();
        let d = daemon(upstreams(&[("u", up.addr)], &extra));
        let mut c = d.client().await;
        let g = if model == "crossing" {
            vec![Duration::from_millis(150); 20]
        } else {
            gaps(40, 80, 120)
        };
        let t = run_posts(&mut c, &up.rec, &g, 2).await;
        let n = g.len() as u32;
        assert_eq!(t.ok + t.indeterminate + t.retryable_unsent, n);
        let received = up.rec.requests().len() as u32;
        assert!(received <= n, "never more requests than POSTs");
        let label = format!(
            "{model} @ H1_UNSAFE_REUSE_MAX_IDLE_MS={}",
            unsafe_idle.unwrap_or("2000 (default)")
        );
        eprintln!(
            "F4b chaos 4: {label}: {n} POSTs → ok {} / indeterminate {} / unsent {}; {} connections",
            t.ok,
            t.indeterminate,
            t.retryable_unsent,
            up.rec.conns()
        );
        if unsafe_idle == Some("0") {
            assert_eq!(
                t.indeterminate, 0,
                "{model}: at 0 no POST reuses an idle connection"
            );
            assert_eq!(up.rec.conns(), u64::from(n), "{model}: one dial per POST");
        } else if model == "crossing" {
            assert!(
                t.indeterminate >= n / 3,
                "the crossing close reaches a reused POST at the default: {t:?}"
            );
        }
    }
}

// =================================================================================================
// Review round (§22.2 (db)): the adversarial review's probes, adopted
// =================================================================================================

/// A connector whose connection's FIRST write blocks its worker thread for `block` before it
/// reaches the socket — so the request's bytes leave while the engine is already tearing the
/// connection down — and whose `connect` itself can block its thread for `connect_block` (an
/// exchange the cap cannot stop on time). `entered` is set as either begins.
struct BlockConnect {
    entered: Arc<AtomicBool>,
    block: Duration,
    connect_block: Duration,
}
struct BlockIo {
    inner: tokio::net::TcpStream,
    first: bool,
    entered: Arc<AtomicBool>,
    block: Duration,
}
impl BlockIo {
    fn block_once(&mut self) {
        if self.first {
            self.first = false;
            self.entered.store(true, Ordering::SeqCst);
            std::thread::sleep(self.block);
        }
    }
}
impl AsyncRead for BlockIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}
impl AsyncWrite for BlockIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.block_once();
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.block_once();
        Pin::new(&mut self.inner).poll_write_vectored(cx, b)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl Connect for BlockConnect {
    fn connect<'a>(
        &'a self,
        peer: std::net::SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        Box::pin(async move {
            if !self.connect_block.is_zero() {
                self.entered.store(true, Ordering::SeqCst);
                std::thread::sleep(self.connect_block);
            }
            let inner = tokio::net::TcpStream::connect(peer).await?;
            Ok(Box::new(BlockIo {
                inner,
                first: true,
                entered: self.entered.clone(),
                block: self.block,
            }) as BoxIo)
        })
    }
}

/// An upstream that reads one request and then holds the connection open without answering.
async fn reads_and_holds() -> Upstream {
    upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
        }
    })
    .await
}

/// **R1: `sent` is read AFTER the teardown, for the drain cap and for a `CANCEL`.** The stop fires
/// while `hyper`'s first write of a POST is in progress on another worker (the connector blocks
/// that write for 400 ms, then lets it through). The upstream RECEIVES the request, so the answer
/// must be Indeterminate — never the "stopped before any byte was sent" (Retryable for the drain,
/// `Cancelled` for a `CANCEL`) that reading `sent` before `discard` completes would give.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_during_the_first_write_is_indeterminate_for_the_drain_and_a_cancel() {
    for drained in [true, false] {
        let up = reads_and_holds().await;
        let in_write = Arc::new(AtomicBool::new(false));
        let engine = engine_with(
            upstreams(&[("u", up.addr)], &[("", "DRAIN_MS", "0")]),
            Arc::new(BlockConnect {
                entered: in_write.clone(),
                block: Duration::from_millis(400),
                connect_block: Duration::ZERO,
            }),
        );
        let drain_token = CancellationToken::new();
        let at: Arc<OnceLock<tokio::time::Instant>> = Arc::new(OnceLock::new());
        let drain = DrainView::new(drain_token.clone(), at.clone());
        let cancel = CancellationToken::new();
        let task = {
            let (e, cancel) = (engine.clone(), cancel.clone());
            tokio::spawn(async move {
                e.serve_request(
                    post("u", b"probe-body"),
                    None,
                    Instant::now(),
                    &cancel,
                    &drain,
                    &BlackHole,
                )
                .await
            })
        };
        wait_for("hyper inside its first write", || {
            in_write.load(Ordering::SeqCst)
        })
        .await;
        if drained {
            at.get_or_init(tokio::time::Instant::now);
            drain_token.cancel();
        } else {
            cancel.cancel();
        }
        let t = task.await.unwrap();
        wait_for("the upstream received the request", || {
            up.rec.requests().len() == 1
        })
        .await;
        let want = if drained {
            http_cause::DRAINING
        } else {
            http_cause::CANCELLED
        };
        match &t {
            Terminal::Error(ep) => {
                assert_eq!(ep.detail.as_deref(), Some(want), "{t:?}");
                assert_eq!(
                    ep.branch,
                    branch::INDETERMINATE,
                    "drained={drained}: a RECEIVED POST reported {t:?}"
                );
            }
            other => panic!("drained={drained}: a RECEIVED POST reported {other:?}"),
        }
    }
}

/// **R3: the drain cap reaching a BODY frame parked on credit is the drain row.** The client stops
/// replenishing credit; the exchange parks; the drain cap fires. The client never cancelled, so the
/// terminal is `ResponseIncomplete` (`draining`) — not `Cancelled`, which mapping the sink's
/// cancellation to a `CANCEL` regardless of the drain would give.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_cap_while_parked_on_credit_is_the_drain_row() {
    let up = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 1073741824\r\n\r\n";
        if rec.write(&mut s, head).await.is_err() {
            return;
        }
        let piece = vec![b'z'; 16 * 1024];
        while rec.write(&mut s, &piece).await.is_ok() {}
    })
    .await;
    let s = served(
        upstreams(&[("u", up.addr)], &[("", "DRAIN_MS", "300")]),
        Duration::from_millis(300),
    );
    let mut c = s.client().await;
    send(&mut c, 2, &request("u", "GET", "/feed")).await;
    let mut frames = 0;
    while let Some(f) = c.recv_or_none(Duration::from_millis(500)).await {
        assert_eq!(
            f.header.flags & flags::END,
            0,
            "no terminal before the drain"
        );
        frames += 1;
    }
    assert!(
        frames >= 2,
        "head + a body parked on credit: {frames} frames"
    );
    s.drain.trigger();
    let end = loop {
        let f = c
            .recv_or_none(Duration::from_secs(3))
            .await
            .expect("a terminal");
        if f.header.flags & flags::END != 0 {
            break Outcome::decode(&f.payload).unwrap();
        }
    };
    match &end {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::RESPONSE_INCOMPLETE, "{end:?}");
            assert_eq!(ep.detail.as_deref(), Some(http_cause::DRAINING), "{end:?}");
        }
        other => panic!("a request the client never cancelled ended {other:?}"),
    }
}

/// **R4: validation comes before the drain check (§23.6's order).** A request the validator
/// refuses gets its `forbidden_*` answer during the drain too, not `draining`; the control, a
/// valid request in the same drain, is refused `draining`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forbidden_request_during_the_drain_is_refused_forbidden() {
    let up = responder(OK_EMPTY).await;
    let s = served(
        upstreams(&[("u", up.addr)], &[("", "DRAIN_MS", "1000")]),
        Duration::from_millis(300),
    );
    let mut c = s.client().await;
    s.drain.trigger();
    exchange(&mut c, 2, &request("nope", "GET", "/"))
        .await
        .assert_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            http_cause::FORBIDDEN_UPSTREAM,
        );
    exchange(&mut c, 3, &request("u", "TRACE", "/"))
        .await
        .assert_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            http_cause::FORBIDDEN_METHOD,
        );
    exchange(&mut c, 4, &request("u", "GET", "/"))
        .await
        .assert_error(
            errc::UPSTREAM_UNAVAILABLE,
            branch::RETRYABLE,
            http_cause::DRAINING,
        );
    assert_eq!(up.rec.conns(), 0);
}

/// **R10: the extension's hard end is measured from the DRAIN's start**, not from when `serve`
/// entered the extension. An exchange the cap cannot stop on time (its connector blocks its
/// thread for 4 s, past the cap) keeps the in-flight count up; `serve` must still end at
/// `FERRO_HTTP_DRAIN_MS + drain_deadline` = 1.2 s after the drain began — measured from the
/// extension's entry it would be 1.8 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_extension_hard_end_is_measured_from_the_drain_start() {
    let up = reads_and_holds().await;
    let in_connect = Arc::new(AtomicBool::new(false));
    let s = served_with(
        upstreams(&[("u", up.addr)], &[("", "DRAIN_MS", "600")]),
        Duration::from_millis(600),
        Arc::new(BlockConnect {
            entered: in_connect.clone(),
            block: Duration::ZERO,
            connect_block: Duration::from_secs(4),
        }),
    );
    let mut c = s.client().await;
    send(&mut c, 2, &post("u", b"stuck-in-connect")).await;
    wait_for("the connector blocking its thread", || {
        in_connect.load(Ordering::SeqCst)
    })
    .await;
    let t0 = Instant::now();
    s.drain.trigger();
    tokio::time::timeout(Duration::from_secs(3), s.served)
        .await
        .expect("serve returned")
        .unwrap();
    let ended = t0.elapsed();
    assert!(
        ended >= Duration::from_millis(1_150) && ended < Duration::from_millis(1_700),
        "the hard end is cap + drain_deadline from the drain's start (1.2 s), not from the \
         extension's entry (1.8 s): {ended:?}"
    );
}

/// An upstream that answers with `Content-Encoding: <ce>`, chunked, and streams `piece` for ever.
async fn endless_encoded(ce: &'static str, piece: Vec<u8>) -> Upstream {
    let piece = Arc::new(piece);
    upstream(move |mut s, rec| {
        let piece = Arc::clone(&piece);
        async move {
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Encoding: {ce}\r\nTransfer-Encoding: chunked\r\n\r\n"
            );
            if rec.write(&mut s, head.as_bytes()).await.is_err() {
                return;
            }
            loop {
                let mut out = format!("{:x}\r\n", piece.len()).into_bytes();
                out.extend_from_slice(&piece);
                out.extend_from_slice(b"\r\n");
                if rec.write(&mut s, &out).await.is_err() {
                    return;
                }
            }
        }
    })
    .await
}

/// Endless EMPTY gzip members: valid gzip that inflates to nothing.
async fn empty_members_upstream() -> Upstream {
    let member = gzip(b"");
    endless_encoded("gzip", member.repeat((256 * 1024) / member.len())).await
}

/// Endless EMPTY fixed-Huffman deflate blocks, raw (RFC 1951): each block is BFINAL=0, BTYPE=01
/// and the end-of-block code (seven 0 bits) — 10 bits, so four blocks are exactly five bytes,
/// `02 08 20 80 00`, and the stream repeats byte-aligned for ever. It inflates to nothing at
/// ~2.5 µs of CPU per input byte (release).
async fn empty_static_blocks_upstream() -> Upstream {
    endless_encoded(
        "deflate",
        [0x02u8, 0x08, 0x20, 0x80, 0x00].repeat(256 * 1024 / 5),
    )
    .await
}

/// One decoding exchange with a 300 ms deadline on a current-thread runtime, beside a cooperative
/// ticker that counts how often it ran: the terminal, how long it took, and the tick count. Every
/// time the exchange yields (or waits on the network) the ticker runs once.
async fn decode_with_ticker(up: &Upstream) -> (Terminal, Duration, usize) {
    let engine = engine_with(upstreams(&[("u", up.addr)], &[]), Arc::new(TcpConnect));
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ticker = {
        let ticks = ticks.clone();
        tokio::spawn(async move {
            loop {
                ticks.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        })
    };
    let rec = Recorder::new(None, Arc::new(std::sync::atomic::AtomicUsize::new(0)));
    let started = Instant::now();
    let t = engine
        .exchange(
            &HttpRequest {
                timeout_ms: Some(300),
                ..decode_get("/decode")
            },
            None,
            started,
            &CancellationToken::new(),
            &rec,
        )
        .await;
    let took = started.elapsed();
    ticker.abort();
    (t, took, ticks.load(Ordering::SeqCst))
}

fn assert_timed_out(what: &str, t: &Terminal, took: Duration) {
    assert!(
        matches!(t, Terminal::Error(ep) if ep.detail.as_deref() == Some(http_cause::TIMEOUT)),
        "{what}: {t:?}"
    );
    assert!(
        took < Duration::from_millis(450),
        "{what}: the terminal is the 300 ms deadline's, not a step's: {took:?}"
    );
}

/// **The review's MEDIUM defect (round 1): a decode step is bounded by its INPUT, not only its
/// output.** An endless stream of EMPTY gzip members decodes to nothing, so no frame is ever sent
/// and no credit can park it. Before the fix one step consumed a whole network read (thousands of
/// members) with no yield and no check: 93–97 ms executor stalls, a terminal 62–79 ms late, a core
/// burned until the deadline. Counted, not a gap: a cooperative task must run at least 30 times in
/// the 300 ms exchange (one per 10 ms; the fixed decoder yields after every bounded step, ~1 ms).
/// The count is per unit of deadline, not per byte the upstream wrote: on loopback the upstream
/// fills the socket buffers (~3.9 MB) whatever the decoder consumes, so its written count says
/// nothing about the decoder.
#[tokio::test(flavor = "current_thread")]
async fn empty_gzip_members_are_bounded_by_the_deadline_and_yield() {
    let up = empty_members_upstream().await;
    let (t, took, ticks) = decode_with_ticker(&up).await;
    eprintln!(
        "F4b review: empty gzip members → {took:?}, {ticks} ticks, upstream wrote {} B",
        up.rec.written()
    );
    assert_timed_out("empty gzip members", &t, took);
    assert!(
        ticks >= 30,
        "{ticks} ticks in a 300 ms exchange: the decoder ran long steps without yielding"
    );
}

/// **The review's MEDIUM defect (round 2): a step is bounded by TIME as well.** Empty fixed-Huffman
/// deflate blocks cost ~2.5 µs of inflate per input byte and decode to nothing, so even the 64 KiB
/// input bound allowed a ~160 ms step (release; through the engine in debug, 294–303 ms stalls and
/// a terminal 253–270 ms late). The time bound ends a step after ~1 ms. Counted, not a gap: this
/// input is consumed so slowly that the bytes the upstream wrote (mostly sitting in socket buffers)
/// say nothing about the decoder, so the count is per unit of deadline — at least 15 runs of a
/// cooperative task in the 300 ms (one per 20 ms). Measured: 60–69 in a debug build, where one
/// 256-byte inflate slice of this input already takes ~4 ms; a decoder bounded only by bytes gives
/// one or two, its first step outlasting the deadline.
#[tokio::test(flavor = "current_thread")]
async fn empty_fixed_huffman_blocks_are_bounded_by_time_and_yield() {
    let up = empty_static_blocks_upstream().await;
    let (t, took, ticks) = decode_with_ticker(&up).await;
    eprintln!(
        "F4b review: empty fixed-Huffman blocks → {took:?}, {ticks} ticks, upstream wrote {} B",
        up.rec.written()
    );
    assert_timed_out("empty fixed-Huffman blocks", &t, took);
    assert!(
        ticks >= 15,
        "{ticks} ticks in a 300 ms exchange: the decoder ran long steps without yielding"
    );
}
