//! P7 (h1) (SPEC §23.19): "Credit-gated reading of a `hyper` body yields TCP backpressure, with no
//! unbounded internal buffering, under a calibrated stall probe (§22.2 (bj))."
//!
//! The probe measures the one thing a buffering client cannot fake: how many body bytes the
//! UPSTREAM managed to hand its kernel. The engine model is §23.6 step 8 — deliver body frames while
//! credit lasts, then stop polling the body. If `hyper` keeps reading into its own buffers once the
//! engine stops, the upstream keeps writing; if it does not, the upstream stalls at roughly the
//! credit plus the kernel socket buffers plus hyper's one read buffer.
//!
//! **Calibration (the §22.2 (bj) rule: a stall probe is a proof only when what it measures cannot
//! also be produced by the mutated code being merely slow).** Kernel buffers are pinned small on
//! both sockets (autotuning is off once `SO_RCVBUF`/`SO_SNDBUF` are set), so the honest excess is a
//! few hundred KiB; the bound is 4 MiB. The negative control runs the SAME probe against a client
//! that reads ahead regardless of credit, and must see an excess of at least 16 × the bound — so a
//! read-ahead implementation is caught even if it were 16 times slower than this one, and the probe
//! waits up to 5 s for it, during which a loopback reader of any speed above ~13 MB/s overshoots.
//!
//! **The "merely slow" half, after review F-2.** A stall is first declared after a 300 ms quiet
//! window, so a credit-IGNORING reader that consumes one frame every 350 ms looked exactly like
//! backpressure to the first draft (its excess stayed under the bound and the upstream "stalled").
//! The probe now HOLDS for 3 s after the stall and requires the upstream's written count not to move
//! at all, so a reader that consumes past credit is caught when its reads reach the SOCKET inside the
//! hold. A very slow reader (one 64 KiB frame per 2 s) may only drain hyper's own buffer during the
//! hold and leave the upstream still: caught locally, MISSED on a GitHub runner (held=true, excess
//! 585 KB), so the guaranteed bound is the review's mutation, one frame per 350 ms, which
//! `p7_control_a_slow_read_ahead_client_fails_the_probe` keeps as a permanent control. A slower
//! reader's harm is bounded by its rate.

mod common;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::*;
use http_body::Body;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;

const MIB: u64 = 1024 * 1024;
/// Credit the "engine" grants before it stops polling the body.
const CREDIT: u64 = MIB;
/// The honest bound on (upstream-written − delivered) once stalled.
const BOUND: u64 = 4 * MIB;
/// What the read-ahead control must exceed for the probe to count as discriminating.
const CONTROL_FLOOR: u64 = 16 * BOUND;
/// Body the upstream offers: far past anything a stall could absorb.
const TOTAL: u64 = 512 * MIB;
const SOCKET_BUF: u32 = 128 * 1024;

#[derive(Clone, Copy, Debug)]
enum Framing {
    ContentLength,
    Chunked,
}

/// After the stall is detected, the upstream's written count must stay unchanged for this long.
const HOLD: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Reader {
    /// The engine model: poll the body only while credit lasts.
    CreditGated,
    /// MUTATION / negative control: a task drains the body as fast as it arrives, credit or not.
    ReadAhead,
    /// MUTATION / negative control (review F-2): credit is honoured, then a task keeps reading past
    /// it, one frame per period — a read-ahead that is merely SLOW.
    SlowReadAhead(Duration),
}

struct Probe {
    /// Body bytes delivered to the engine under credit (CreditGated) — what the engine has handed on.
    delivered: u64,
    /// Bytes (head + body framing included) the upstream's kernel accepted, once stable.
    upstream_written: u64,
    stalled: bool,
    /// The written count did not move for [`HOLD`] after the stall.
    held: bool,
    resumed: bool,
}

async fn upstream(framing: Framing) -> (std::net::SocketAddr, Arc<AtomicU64>) {
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_send_buffer_size(SOCKET_BUF).unwrap(); // inherited by the accepted socket
    sock.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = sock.listen(1).unwrap();
    let addr = listener.local_addr().unwrap();
    let written = Arc::new(AtomicU64::new(0));
    let w = written.clone();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        read_request(&mut s, &AtomicU64::new(0), 0).await;
        let head = match framing {
            Framing::ContentLength => format!("HTTP/1.1 200 OK\r\ncontent-length: {TOTAL}\r\n\r\n"),
            Framing::Chunked => "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".into(),
        };
        if s.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        let payload = vec![b'z'; 64 * 1024];
        let piece: Vec<u8> = match framing {
            Framing::ContentLength => payload,
            Framing::Chunked => {
                let mut p = format!("{:x}\r\n", payload.len()).into_bytes();
                p.extend_from_slice(&payload);
                p.extend_from_slice(b"\r\n");
                p
            }
        };
        let mut sent_body = 0u64;
        while sent_body < TOTAL {
            let mut off = 0;
            while off < piece.len() {
                match s.write(&piece[off..]).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        off += n;
                        w.fetch_add(n as u64, Ordering::SeqCst);
                    }
                }
            }
            sent_body += 64 * 1024;
        }
        if let Framing::Chunked = framing {
            let _ = s.write_all(b"0\r\n\r\n").await;
        }
    });
    (addr, written)
}

async fn next_data(body: &mut hyper::body::Incoming) -> Option<u64> {
    loop {
        let frame = futures::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await?;
        let frame = frame.expect("body frame");
        if let Ok(data) = frame.into_data() {
            return Some(data.len() as u64);
        }
    }
}

/// Wait until the upstream's written count stops moving for 300 ms (or 5 s pass).
async fn settle(written: &AtomicU64) -> (u64, bool) {
    let start = Instant::now();
    let mut last = written.load(Ordering::SeqCst);
    let mut still_since = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = written.load(Ordering::SeqCst);
        if now != last {
            last = now;
            still_since = Instant::now();
        } else if still_since.elapsed() >= Duration::from_millis(300) {
            return (now, true);
        }
    }
    (written.load(Ordering::SeqCst), false)
}

/// Once stalled at `at`, does the upstream's written count stay at `at` for [`HOLD`]?
async fn hold(written: &AtomicU64, at: u64) -> bool {
    let start = Instant::now();
    while start.elapsed() < HOLD {
        tokio::time::sleep(Duration::from_millis(25)).await;
        // MUTATION SITE (M-P7c): returning `true` here (no hold) lets a 350 ms-per-frame read-ahead
        // pass the positive test — review F-2's reproduction.
        if written.load(Ordering::SeqCst) != at {
            return false;
        }
    }
    true
}

async fn probe(framing: Framing, reader: Reader) -> Probe {
    let (addr, written) = upstream(framing).await;
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(SOCKET_BUF).unwrap();
    let tcp = sock.connect(addr).await.unwrap();
    let (mut send, track, _conn) = h1_over(tcp, &h1_builder()).await;
    within("ready", send.ready()).await.unwrap();
    track.arm();
    let resp = within("head", send.send_request(get())).await.unwrap();
    let mut body = resp.into_body();

    let mut delivered = 0u64;
    match reader {
        Reader::CreditGated => {
            while delivered < CREDIT {
                delivered += within("credit frame", next_data(&mut body)).await.unwrap();
            }
            // Credit exhausted: the engine stops polling. Nothing else touches `body`.
            let (upstream_written, stalled) = settle(&written).await;
            let held = stalled && hold(&written, upstream_written).await;
            // Credit replenished (WINDOW_UPDATE): the engine polls again, and the upstream must
            // resume — the stall was backpressure, not a dead connection.
            let target = delivered + CREDIT;
            while delivered < target {
                delivered += within("resume frame", next_data(&mut body)).await.unwrap();
            }
            let resumed = within("resume", async {
                loop {
                    if written.load(Ordering::SeqCst) > upstream_written {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            Probe {
                delivered: target - CREDIT,
                upstream_written,
                stalled,
                held,
                resumed,
            }
        }
        Reader::ReadAhead => {
            // MUTATION: drain ahead of credit (an "engine" that buffers the response internally).
            tokio::spawn(async move { while next_data(&mut body).await.is_some() {} });
            let (upstream_written, stalled) = settle(&written).await;
            let held = stalled && hold(&written, upstream_written).await;
            Probe {
                delivered: CREDIT,
                upstream_written,
                stalled,
                held,
                resumed: true,
            }
        }
        Reader::SlowReadAhead(period) => {
            while delivered < CREDIT {
                delivered += within("credit frame", next_data(&mut body)).await.unwrap();
            }
            // MUTATION: keep reading past credit, slowly.
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(period).await;
                    if next_data(&mut body).await.is_none() {
                        break;
                    }
                }
            });
            let (upstream_written, stalled) = settle(&written).await;
            let held = stalled && hold(&written, upstream_written).await;
            Probe {
                delivered,
                upstream_written,
                stalled,
                held,
                resumed: true,
            }
        }
    }
}

fn excess(p: &Probe) -> u64 {
    p.upstream_written.saturating_sub(p.delivered)
}

/// **P7 (h1).** Credit-gated reading stalls the upstream within the bound, for both body framings,
/// and replenished credit resumes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p7_h1_credit_gated_reading_backpressures_the_upstream() {
    for framing in [Framing::ContentLength, Framing::Chunked] {
        let p = probe(framing, Reader::CreditGated).await;
        eprintln!(
            "P7 {framing:?}: delivered {} B, upstream wrote {} B, excess {} B (bound {BOUND} B), \
             stalled={}, held={}, resumed={}",
            p.delivered,
            p.upstream_written,
            excess(&p),
            p.stalled,
            p.held,
            p.resumed
        );
        assert!(p.stalled, "{framing:?}: the upstream never stalled");
        assert!(
            p.held,
            "{framing:?}: the upstream moved during the {HOLD:?} hold — something reads past credit"
        );
        assert!(
            excess(&p) <= BOUND,
            "{framing:?}: {} B written past the credit — hyper buffers beyond the bound",
            excess(&p)
        );
        assert!(
            p.resumed,
            "{framing:?}: replenished credit did not resume the upstream"
        );
    }
}

/// **P7, the negative control.** The same probe, pointed at a client that reads ahead of credit,
/// must report a violation by a wide margin — otherwise the positive test above would prove nothing
/// (§22.2 (bj): v1 of C3-5's stall probe passed under its own mutation).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p7_control_a_read_ahead_client_fails_the_probe() {
    for framing in [Framing::ContentLength, Framing::Chunked] {
        let p = probe(framing, Reader::ReadAhead).await;
        eprintln!(
            "P7 control {framing:?}: upstream wrote {} B, excess {} B",
            p.upstream_written,
            excess(&p)
        );
        assert!(
            excess(&p) >= CONTROL_FLOOR,
            "{framing:?}: the read-ahead control only overshot by {} B — the probe cannot tell \
             buffering from backpressure",
            excess(&p)
        );
    }
}

/// **P7, the "merely slow" negative control (review F-2; §22.2 (bj)'s rule).** A client that honours
/// the credit and then keeps reading past it slowly — one frame per 350 ms (the review's mutation,
/// slower than the 300 ms quiet window) — must FAIL the probe: the upstream may look stalled for a
/// moment, but it moves during the hold. (A 2 s period was dropped: whether its reads reach the socket
/// within the hold depends on buffer sizes, and a GitHub runner missed it — see the module doc.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p7_control_a_slow_read_ahead_client_fails_the_probe() {
    for period in [Duration::from_millis(350)] {
        let p = probe(Framing::ContentLength, Reader::SlowReadAhead(period)).await;
        eprintln!(
            "P7 slow control {period:?}: upstream wrote {} B, excess {} B, stalled={}, held={}",
            p.upstream_written,
            excess(&p),
            p.stalled,
            p.held
        );
        assert!(
            !(p.stalled && p.held),
            "a read-ahead at one frame per {period:?} passed the stall probe"
        );
    }
}
