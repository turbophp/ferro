//! **M6-F6 — Ferro HTTP's host-level limits (SPEC §23.6 step 4 (b), (c), (e) and step 5;
//! §23.8.6; §23.8.7; §23.14 cases 9 and 10).**
//!
//! The breaker (with its RAII half-open probe), the Retry-After hold, the rate limit, the
//! `MAX_REQUESTS` queue and the `MAX_CONNECTIONS`/`MAX_DIALS` caps, every one driven through the
//! REAL handler factory over a real Unix socket against loopback upstreams that record what they
//! received. **Every refusal is unsent and Retryable**: each one asserts its code, branch and cause
//! token, and that the upstream's receive count did not move (charter rule 3). Dials are counted at
//! the connector seam, so "zero dials" is a read-back too.
//!
//! Timing: no assertion depends on a sleep being SHORT enough. Waits are on events (an upstream
//! that saw a head, a queue that has a waiter, a breaker that reports half-open); where time is the
//! subject (a `retry_after_ms`, a rate wait), the bounds are derived from instants the test took
//! itself, so a slow runner widens them rather than flipping them, and a sleep only ever waits
//! LONGER than the engine needs.
#![cfg(feature = "http")]

mod common;
mod http_support;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ferro_http::engine::limits::BreakerState;
use ferro_http::engine::{BoxIo, Connect};
use ferro_proto::consts::{branch, errc, http_cause};
use ferro_proto::messages::{HttpRequest, Outcome};
use http_support::*;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use crate::common::TestClient;

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

// =================================================================================================
// Seams
// =================================================================================================

const TCP: usize = 0;
const REFUSE: usize = 1;
const HANG: usize = 2;
const STALL: usize = 3;

/// A connector whose behaviour the test switches: plain TCP, refuse every connect
/// (`connect_refused`, counted by the breaker), hang every connect, or connect over TCP and never
/// write a byte (the request is dispatched and stalls, holding its body charge). Every attempt is
/// counted.
struct ScriptConnect {
    mode: AtomicUsize,
    attempts: AtomicUsize,
}

impl ScriptConnect {
    fn new(mode: usize) -> Arc<Self> {
        Arc::new(ScriptConnect {
            mode: AtomicUsize::new(mode),
            attempts: AtomicUsize::new(0),
        })
    }
    fn set(&self, mode: usize) {
        self.mode.store(mode, Ordering::SeqCst);
    }
    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

impl Connect for ScriptConnect {
    fn connect<'a>(
        &'a self,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                REFUSE => Err(io::ErrorKind::ConnectionRefused.into()),
                HANG => std::future::pending().await,
                STALL => {
                    let inner = TcpStream::connect(peer).await?;
                    Ok(Box::new(StallIo {
                        inner,
                        budget: Some(0),
                    }) as BoxIo)
                }
                _ => {
                    let s = TcpStream::connect(peer).await?;
                    s.set_nodelay(true)?;
                    Ok(Box::new(s) as BoxIo)
                }
            }
        })
    }
}

/// What a scripted upstream does with its n-th request (counted across connections).
#[derive(Clone, Copy)]
enum Act {
    /// Answer at once.
    Answer(&'static [u8]),
    /// Answer `OK` once the test releases a permit on the gate.
    Gated,
    /// Close the connection without answering (`eof_empty` for the request).
    Close,
    /// Never answer; hold the connection until the engine closes it.
    Hold,
}

/// An upstream that reads each whole request (recording it) and then does `acts[n]` for the n-th
/// one; past the list it answers `OK`.
async fn scripted(acts: Vec<Act>) -> (Upstream, Arc<Semaphore>) {
    let gate = Arc::new(Semaphore::new(0));
    let g = Arc::clone(&gate);
    let acts = Arc::new(acts);
    let up = upstream(move |mut s, rec| {
        let g = Arc::clone(&g);
        let acts = Arc::clone(&acts);
        async move {
            while rec.read_request(&mut s).await.is_some() {
                let n = rec.requests().len() - 1;
                match acts.get(n).copied().unwrap_or(Act::Answer(OK)) {
                    Act::Answer(r) => {
                        if rec.write(&mut s, r).await.is_err() {
                            return;
                        }
                    }
                    Act::Gated => {
                        let Ok(p) = g.acquire().await else { return };
                        p.forget();
                        if rec.write(&mut s, OK).await.is_err() {
                            return;
                        }
                    }
                    Act::Close => return,
                    Act::Hold => {
                        rec.hold_until_closed(&mut s, Duration::from_secs(60)).await;
                        return;
                    }
                }
            }
        }
    })
    .await;
    (up, gate)
}

fn daemon_on(env: Vec<(String, String)>, connector: Arc<ScriptConnect>) -> Daemon {
    daemon_with(env, connector, |_| {})
}

/// One request on its own session, sent; the session is returned for collecting it later.
async fn start(d: &Daemon, r: &HttpRequest) -> TestClient {
    let mut c = d.client().await;
    send(&mut c, 1, r).await;
    c
}

async fn one(d: &Daemon, r: &HttpRequest) -> Reply {
    let mut c = d.client().await;
    exchange(&mut c, 1, r).await
}

fn received(up: &Upstream) -> usize {
    up.rec.requests().len()
}

fn status(r: &Reply) -> u16 {
    r.done();
    r.head.as_ref().expect("a HEAD").status
}

fn assert_unavailable(r: &Reply, cause: &str) -> Option<u32> {
    r.assert_error(errc::UPSTREAM_UNAVAILABLE, branch::RETRYABLE, cause)
        .retry_after_ms
}

// =================================================================================================
// Chaos 9: the breaker (§23.8.6)
// =================================================================================================

/// **Chaos 9, first half.** K consecutive connect failures open the breaker; every request is then
/// refused at once (`breaker_open`, Retryable, `retry_after_ms` the time left) with ZERO dials and
/// nothing received, whatever its method; a request the policy refuses still gets its `forbidden_*`
/// (validation precedes admission). Once `BREAKER_OPEN_MS` has passed the breaker is half-open: the
/// next request is the probe, a request beside it is refused `breaker_probe_busy` (with no
/// `retry_after_ms`), and the probe's final head closes the breaker.
#[tokio::test]
async fn chaos9_k_connect_failures_open_the_breaker_and_a_probe_closes_it() {
    let (up, gate) = scripted(vec![Act::Gated]).await;
    let conn = ScriptConnect::new(REFUSE);
    let d = daemon_on(
        upstreams(
            &[("b", up.addr)],
            &[
                ("b", "BREAKER_FAILURES", "3"),
                ("b", "BREAKER_OPEN_MS", "600"),
            ],
        ),
        Arc::clone(&conn),
    );
    for i in 0..3 {
        assert_eq!(
            d.engine.breaker_state("b"),
            Some(BreakerState::Closed),
            "failure {i}"
        );
        let r = one(&d, &post("b", b"x")).await;
        assert_unavailable(&r, http_cause::CONNECT_REFUSED);
    }
    assert_eq!(conn.attempts(), 3);
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::Open));

    let opened = Instant::now();
    for r in [
        post("b", b"x"),
        idempotent_get("b"),
        request("b", "GET", "/"),
    ] {
        let before = Instant::now();
        let reply = one(&d, &r).await;
        let left = assert_unavailable(&reply, http_cause::BREAKER_OPEN)
            .expect("breaker_open carries retry_after_ms");
        assert!(left >= 1, "never 0");
        let upper = 600u128.saturating_sub(before.duration_since(opened).as_millis()) + 1;
        assert!(
            u128::from(left) <= upper,
            "the time left, at most {upper} ms: {left}"
        );
    }
    let forbidden = one(&d, &request("b", "TRACE", "/")).await;
    forbidden.assert_error(
        errc::FORBIDDEN,
        branch::NON_RETRYABLE,
        http_cause::FORBIDDEN_METHOD,
    );
    assert_eq!(conn.attempts(), 3, "zero dials while open");
    assert_eq!(received(&up), 0);

    conn.set(TCP);
    wait_for("the breaker to half-open", || {
        d.engine.breaker_state("b") == Some(BreakerState::HalfOpen)
    })
    .await;
    // The probe: held at the upstream until the test releases it.
    let mut probe = start(&d, &post("b", b"probe")).await;
    wait_for("the probe at the upstream", || received(&up) == 1).await;
    let busy = one(&d, &idempotent_get("b")).await;
    assert_eq!(
        assert_unavailable(&busy, http_cause::BREAKER_PROBE_BUSY),
        None,
        "breaker_probe_busy carries no retry_after_ms"
    );
    assert_eq!(received(&up), 1, "the refused request was not sent");
    gate.add_permits(1);
    assert_eq!(status(&collect(&mut probe, 1).await), 200);
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::Closed));
    for _ in 0..3 {
        assert_eq!(status(&one(&d, &post("b", b"y")).await), 200);
    }
    assert_eq!(received(&up), 4);
}

/// Opens `name`'s breaker with ONE refused connect (`BREAKER_FAILURES=1`) and waits for it to
/// half-open; the connector is left in `mode`. The upstream must not hand a POST an idle
/// connection (`H1_UNSAFE_REUSE_MAX_IDLE_MS=0`), so the POST really dials.
async fn half_open(d: &Daemon, conn: &ScriptConnect, name: &str, mode: usize) {
    conn.set(REFUSE);
    let r = one(d, &post(name, b"x")).await;
    assert_unavailable(&r, http_cause::CONNECT_REFUSED);
    assert_eq!(d.engine.breaker_state(name), Some(BreakerState::Open));
    conn.set(mode);
    wait_for("the breaker to half-open", || {
        d.engine.breaker_state(name) == Some(BreakerState::HalfOpen)
    })
    .await;
}

/// Proves the breaker is half-open with a FREE probe slot: the next request is admitted as the
/// probe (it reaches the gated upstream) and a request beside it is `breaker_probe_busy`; the probe
/// is then released and closes the breaker.
async fn next_request_probes(d: &Daemon, up: &Upstream, gate: &Semaphore, name: &str) {
    let before = received(up);
    let mut probe = start(d, &post(name, b"next")).await;
    wait_for("the next probe at the upstream", || {
        received(up) == before + 1
    })
    .await;
    let busy = one(d, &post(name, b"busy")).await;
    assert_unavailable(&busy, http_cause::BREAKER_PROBE_BUSY);
    gate.add_permits(1);
    assert_eq!(status(&collect(&mut probe, 1).await), 200);
    assert_eq!(d.engine.breaker_state(name), Some(BreakerState::Closed));
    assert_eq!(received(up), before + 1, "the busy request was not sent");
}

/// **Chaos 9, second half.** A probe that ends Indeterminate (the upstream closes after reading
/// the POST: `eof_empty`, not a counted failure under the default `connect` class) leaves the
/// breaker HALF-OPEN with the slot released — not open, not closed — and the next request probes.
#[tokio::test]
async fn chaos9_an_indeterminate_probe_leaves_half_open_with_the_slot_released() {
    let (up, gate) = scripted(vec![Act::Close, Act::Gated]).await;
    let conn = ScriptConnect::new(TCP);
    let d = daemon_on(
        upstreams(
            &[("b", up.addr)],
            &[
                ("b", "BREAKER_FAILURES", "1"),
                ("b", "BREAKER_OPEN_MS", "200"),
                ("b", "H1_UNSAFE_REUSE_MAX_IDLE_MS", "0"),
            ],
        ),
        Arc::clone(&conn),
    );
    half_open(&d, &conn, "b", TCP).await;
    let r = one(&d, &post("b", b"probe")).await;
    r.assert_error(
        errc::WRITE_UNCONFIRMED,
        branch::INDETERMINATE,
        http_cause::EOF_EMPTY,
    );
    assert_eq!(received(&up), 1, "received exactly once");
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::HalfOpen));
    next_request_probes(&d, &up, &gate, "b").await;
    assert_eq!(received(&up), 2);
}

/// **The probe slot cannot leak (review F24's RAII guard), end to end.** A probe ends without an
/// answer in every way a request can, and each time the breaker stays half-open with the slot
/// released: (1) `CANCEL`led while its dial hangs (before dispatch: `Cancelled`, nothing sent);
/// (2) `CANCEL`led after it was sent (a POST: Indeterminate, cause `cancelled`); (3) its session
/// dies with it in flight. After each, the next request is the probe.
#[tokio::test]
async fn the_probe_slot_is_released_however_the_probe_ends() {
    let (up, gate) = scripted(vec![Act::Hold, Act::Gated, Act::Hold, Act::Gated]).await;
    let conn = ScriptConnect::new(TCP);
    let d = daemon_on(
        upstreams(
            &[("b", up.addr)],
            &[
                ("b", "BREAKER_FAILURES", "1"),
                ("b", "BREAKER_OPEN_MS", "100"),
                ("b", "H1_UNSAFE_REUSE_MAX_IDLE_MS", "0"),
            ],
        ),
        Arc::clone(&conn),
    );

    // (1) Cancelled before dispatch: the probe's dial hangs.
    half_open(&d, &conn, "b", HANG).await;
    let attempts = conn.attempts();
    let mut probe = start(&d, &post("b", b"p1")).await;
    wait_for("the probe's dial", || conn.attempts() == attempts + 1).await;
    let busy = one(&d, &post("b", b"busy")).await;
    assert_unavailable(&busy, http_cause::BREAKER_PROBE_BUSY);
    probe.cancel(1).await;
    assert!(matches!(
        collect(&mut probe, 1).await.end,
        Outcome::Cancelled
    ));
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::HalfOpen));
    assert_eq!(received(&up), 0);
    conn.set(TCP);

    // (2) Cancelled after it was sent: the upstream holds it.
    let mut probe = start(&d, &post("b", b"p2")).await;
    wait_for("the probe at the upstream", || received(&up) == 1).await;
    probe.cancel(1).await;
    collect(&mut probe, 1).await.assert_error(
        errc::WRITE_UNCONFIRMED,
        branch::INDETERMINATE,
        http_cause::CANCELLED,
    );
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::HalfOpen));
    next_request_probes(&d, &up, &gate, "b").await; // request 2, gated

    // (3) The session dies with the probe in flight.
    half_open(&d, &conn, "b", TCP).await;
    let probe = start(&d, &post("b", b"p3")).await;
    wait_for("the probe at the upstream", || received(&up) == 3).await;
    drop(probe);
    wait_for("the probe's exchange ended", || d.engine.in_flight() == 0).await;
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::HalfOpen));
    next_request_probes(&d, &up, &gate, "b").await; // request 4, gated
    assert_eq!(received(&up), 4, "every probe was received at most once");
}

/// **A probe refused by a LATER admission step releases the slot.** The breaker admits the probe at
/// step (b); the rate limit (c) — or the body budget (d) — then refuses it, unsent. Each refused
/// probe is followed by another request that is ALSO admitted as the probe and refused by the same
/// later step: had the first leaked the slot, the second would be `breaker_probe_busy`.
#[tokio::test]
async fn a_probe_refused_by_a_later_admission_step_releases_the_slot() {
    let (up, gate) = scripted(vec![Act::Gated]).await;
    let (rated, _rgate) = scripted(vec![]).await;
    let (hog, _hgate) = scripted(vec![]).await;
    let conn = ScriptConnect::new(TCP);
    let d = daemon_on(
        upstreams(
            &[("b", up.addr), ("r", rated.addr), ("hog", hog.addr)],
            &[
                ("", "MAX_BODY_BYTES", "8"),
                ("b", "BREAKER_FAILURES", "1"),
                ("b", "BREAKER_OPEN_MS", "100"),
                ("b", "H1_UNSAFE_REUSE_MAX_IDLE_MS", "0"),
                ("r", "BREAKER_FAILURES", "1"),
                ("r", "BREAKER_OPEN_MS", "100"),
                ("r", "H1_UNSAFE_REUSE_MAX_IDLE_MS", "0"),
                // One token per 100 s, burst 1: the refused connect below takes the only one.
                ("r", "RATE_PER_SEC", "0.01"),
            ],
        ),
        Arc::clone(&conn),
    );
    // (c) The rate limit refuses the probe.
    half_open(&d, &conn, "r", TCP).await;
    for _ in 0..2 {
        let r = one(&d, &post("r", b"x")).await;
        let ep = r.assert_error(
            errc::RATE_LIMITED,
            branch::RETRYABLE,
            http_cause::RATE_LIMITED,
        );
        assert!(ep.retry_after_ms.is_some());
        assert_eq!(d.engine.breaker_state("r"), Some(BreakerState::HalfOpen));
    }
    assert_eq!(received(&rated), 0);

    // (d) The body budget refuses the probe: another upstream's request holds the whole daemon
    // budget (8 bytes) on a connection that never takes a byte.
    half_open(&d, &conn, "b", STALL).await;
    let mut hogging = start(&d, &post("hog", b"12345678")).await;
    wait_for("the daemon budget held", || {
        d.engine.body_budget_daemon_in_use() == 8
    })
    .await;
    for _ in 0..2 {
        let r = one(&d, &post("b", b"abcd")).await;
        r.assert_error(
            errc::POOL_TIMEOUT,
            branch::RETRYABLE,
            http_cause::BODY_BUDGET,
        );
        assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::HalfOpen));
    }
    hogging.cancel(1).await;
    assert!(matches!(
        collect(&mut hogging, 1).await.end,
        Outcome::Cancelled
    ));
    assert_eq!(d.engine.body_budget_daemon_in_use(), 0);
    conn.set(TCP);
    assert_eq!(received(&up), 0);
    next_request_probes(&d, &up, &gate, "b").await;
}

/// A probe that fails with a COUNTED failure opens the breaker again for `BREAKER_OPEN_MS`.
#[tokio::test]
async fn a_counted_probe_failure_opens_the_breaker_again() {
    let (up, _gate) = scripted(vec![]).await;
    let conn = ScriptConnect::new(TCP);
    let d = daemon_on(
        upstreams(
            &[("b", up.addr)],
            &[
                ("b", "BREAKER_FAILURES", "1"),
                ("b", "BREAKER_OPEN_MS", "300"),
            ],
        ),
        Arc::clone(&conn),
    );
    half_open(&d, &conn, "b", REFUSE).await;
    let r = one(&d, &post("b", b"probe")).await;
    assert_unavailable(&r, http_cause::CONNECT_REFUSED);
    assert_eq!(d.engine.breaker_state("b"), Some(BreakerState::Open));
    let attempts = conn.attempts();
    let before = Instant::now();
    let r = one(&d, &post("b", b"x")).await;
    let left = assert_unavailable(&r, http_cause::BREAKER_OPEN).unwrap();
    assert!(
        u128::from(left) <= 300 && u128::from(left) + before.elapsed().as_millis() + 1 >= 1,
        "{left}"
    );
    assert_eq!(conn.attempts(), attempts, "no dial while open");
    assert_eq!(received(&up), 0);
}

/// `BREAKER_COUNTS`: the default `connect` counts neither a timeout nor a 5xx; `connect+timeout`
/// counts the "sent, no head" `timeout`; `connect+timeout+5xx` also counts a completed 502/503/504
/// — which is still delivered to the client as the `Ok` exchange it is (§23.7.4). A success resets
/// the consecutive count.
#[tokio::test]
async fn breaker_counts_selects_what_opens_the_breaker() {
    const BAD: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    let (fivexx, _g1) = scripted(vec![Act::Answer(BAD); 8]).await;
    let (slow, _g2) = scripted(vec![Act::Hold; 8]).await;
    let conn = ScriptConnect::new(TCP);
    let d = daemon_on(
        upstreams(
            &[
                ("plain5", fivexx.addr),
                ("five", fivexx.addr),
                ("plaint", slow.addr),
                ("timed", slow.addr),
            ],
            &[
                ("plain5", "BREAKER_FAILURES", "2"),
                ("five", "BREAKER_FAILURES", "2"),
                ("five", "BREAKER_COUNTS", "connect+timeout+5xx"),
                ("plaint", "BREAKER_FAILURES", "2"),
                ("timed", "BREAKER_FAILURES", "2"),
                ("timed", "BREAKER_COUNTS", "connect+timeout"),
            ],
        ),
        conn,
    );
    for name in ["plain5", "plain5", "plain5", "five", "five"] {
        assert_eq!(status(&one(&d, &post(name, b"x")).await), 503, "{name}");
    }
    assert_eq!(d.engine.breaker_state("plain5"), Some(BreakerState::Closed));
    assert_eq!(d.engine.breaker_state("five"), Some(BreakerState::Open));
    let r = one(&d, &post("five", b"x")).await;
    assert_unavailable(&r, http_cause::BREAKER_OPEN);
    assert_eq!(received(&fivexx), 5);

    let timing_out = |u: &str| HttpRequest {
        timeout_ms: Some(150),
        ..post(u, b"t")
    };
    for name in ["plaint", "plaint", "plaint", "timed", "timed"] {
        one(&d, &timing_out(name)).await.assert_error(
            errc::WRITE_UNCONFIRMED,
            branch::INDETERMINATE,
            http_cause::TIMEOUT,
        );
    }
    assert_eq!(d.engine.breaker_state("plaint"), Some(BreakerState::Closed));
    assert_eq!(d.engine.breaker_state("timed"), Some(BreakerState::Open));
    assert_eq!(received(&slow), 5);
}

// =================================================================================================
// The queue: MAX_REQUESTS, MAX_QUEUED, QUEUE_TIMEOUT_MS (§23.6 step 4 (e))
// =================================================================================================

/// `MAX_REQUESTS` in flight; one more waits (`MAX_QUEUED=1`); the next is refused at once
/// (`queue_full`); a waiter past `QUEUE_TIMEOUT_MS` is refused (`queue_timeout`); a waiter whose
/// own deadline is nearer is refused `deadline`; a `CANCEL`led waiter is `Cancelled`. All are
/// Retryable `PoolTimeout` and none reached the upstream. A freed slot goes to the waiter.
#[tokio::test]
async fn the_queue_refuses_unsent_and_serves_its_waiter() {
    let (up, gate) = scripted(vec![Act::Gated; 4]).await;
    let d = daemon_on(
        upstreams(
            &[("q", up.addr)],
            &[
                ("q", "MAX_REQUESTS", "1"),
                ("q", "MAX_QUEUED", "1"),
                ("q", "QUEUE_TIMEOUT_MS", "300"),
            ],
        ),
        ScriptConnect::new(TCP),
    );
    let mut first = start(&d, &post("q", b"1")).await;
    wait_for("the first request at the upstream", || received(&up) == 1).await;
    assert_eq!(d.engine.requests_in_flight("q"), 1);

    // queue_timeout: waits its 300 ms, then refused.
    let queued_at = Instant::now();
    let mut waiter = start(&d, &post("q", b"2")).await;
    wait_for("a waiter", || d.engine.queue_waiting("q") == 1).await;
    // queue_full: refused at once while the one place is taken.
    let full = one(&d, &post("q", b"3")).await;
    full.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::QUEUE_FULL,
    );
    let r = collect(&mut waiter, 1).await;
    r.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::QUEUE_TIMEOUT,
    );
    assert!(queued_at.elapsed() >= Duration::from_millis(300));
    assert_eq!(d.engine.queue_waiting("q"), 0);

    // deadline: the request's own bound is nearer than the queue's.
    let r = one(
        &d,
        &HttpRequest {
            timeout_ms: Some(100),
            ..post("q", b"4")
        },
    )
    .await;
    r.assert_error(errc::POOL_TIMEOUT, branch::RETRYABLE, http_cause::DEADLINE);

    // CANCEL while queued.
    let mut cancelled = start(&d, &post("q", b"5")).await;
    wait_for("a waiter", || d.engine.queue_waiting("q") == 1).await;
    cancelled.cancel(1).await;
    assert!(matches!(
        collect(&mut cancelled, 1).await.end,
        Outcome::Cancelled
    ));
    assert_eq!(d.engine.queue_waiting("q"), 0);
    assert_eq!(received(&up), 1, "no refused or cancelled request was sent");

    // A freed slot goes to the waiter.
    let mut served = start(
        &d,
        &HttpRequest {
            timeout_ms: Some(30_000),
            ..post("q", b"6")
        },
    )
    .await;
    wait_for("a waiter", || d.engine.queue_waiting("q") == 1).await;
    gate.add_permits(1);
    assert_eq!(status(&collect(&mut first, 1).await), 200);
    wait_for("the waiter at the upstream", || received(&up) == 2).await;
    gate.add_permits(1);
    assert_eq!(status(&collect(&mut served, 1).await), 200);
    assert_eq!(d.engine.requests_in_flight("q"), 0);
    let bodies: Vec<Vec<u8>> = up.rec.requests().into_iter().map(|s| s.body).collect();
    assert_eq!(bodies, vec![b"1".to_vec(), b"6".to_vec()]);
}

/// `MAX_QUEUED=0`: nothing waits — a request that cannot have a slot at once is `queue_full`.
#[tokio::test]
async fn max_queued_zero_refuses_every_wait() {
    let (up, gate) = scripted(vec![Act::Gated]).await;
    let d = daemon_on(
        upstreams(
            &[("q", up.addr)],
            &[("q", "MAX_REQUESTS", "1"), ("q", "MAX_QUEUED", "0")],
        ),
        ScriptConnect::new(TCP),
    );
    let mut first = start(&d, &idempotent_get("q")).await;
    wait_for("the first request at the upstream", || received(&up) == 1).await;
    let r = one(&d, &idempotent_get("q")).await;
    r.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::QUEUE_FULL,
    );
    gate.add_permits(1);
    assert_eq!(status(&collect(&mut first, 1).await), 200);
    assert_eq!(received(&up), 1);
}

// =================================================================================================
// Step 5: MAX_CONNECTIONS and MAX_DIALS
// =================================================================================================

/// `MAX_CONNECTIONS` bounds the connections that EXIST, not only the idle ones: with one allowed,
/// a second concurrent request waits for the first's connection and reuses it — one connection at
/// the upstream throughout. A waiter past `QUEUE_TIMEOUT_MS` is `queue_timeout`, unsent.
#[tokio::test]
async fn max_connections_bounds_live_connections() {
    let (up, gate) = scripted(vec![Act::Gated; 2]).await;
    let d = daemon_on(
        upstreams(
            &[("c", up.addr)],
            &[
                ("c", "MAX_CONNECTIONS", "1"),
                ("c", "QUEUE_TIMEOUT_MS", "300"),
            ],
        ),
        ScriptConnect::new(TCP),
    );
    let mut a = start(&d, &idempotent_get("c")).await;
    wait_for("the first request at the upstream", || received(&up) == 1).await;
    let r = one(&d, &idempotent_get("c")).await;
    r.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::QUEUE_TIMEOUT,
    );
    assert_eq!(received(&up), 1);
    let mut b = start(
        &d,
        &HttpRequest {
            timeout_ms: Some(30_000),
            ..idempotent_get("c")
        },
    )
    .await;
    // `b` holds a MAX_REQUESTS slot and waits for the connection, so it is not "queued".
    wait_for("b in flight", || d.engine.requests_in_flight("c") == 2).await;
    assert_eq!(d.engine.connections("c"), 1);
    gate.add_permits(2);
    let (ra, rb) = (collect(&mut a, 1).await, collect(&mut b, 1).await);
    assert_eq!((status(&ra), status(&rb)), (200, 200));
    assert!(
        rb.done().stats.reused,
        "the second request reused the one connection"
    );
    assert_eq!(
        up.rec.conns(),
        1,
        "one connection at the upstream throughout"
    );
    assert_eq!(d.engine.connections("c"), 1);
}

/// `MAX_DIALS` bounds dials in progress: with one allowed and the first dial hanging, a second
/// request does not dial (one attempt at the connector) and ends `queue_timeout`, unsent.
#[tokio::test]
async fn max_dials_bounds_dials_in_progress() {
    let (up, _gate) = scripted(vec![]).await;
    let conn = ScriptConnect::new(HANG);
    let d = daemon_on(
        upstreams(
            &[("c", up.addr)],
            &[("c", "MAX_DIALS", "1"), ("c", "QUEUE_TIMEOUT_MS", "300")],
        ),
        Arc::clone(&conn),
    );
    let mut hanging = start(&d, &idempotent_get("c")).await;
    wait_for("the first dial", || conn.attempts() == 1).await;
    assert_eq!(d.engine.dials_in_progress("c"), 1);
    let r = one(&d, &idempotent_get("c")).await;
    r.assert_error(
        errc::POOL_TIMEOUT,
        branch::RETRYABLE,
        http_cause::QUEUE_TIMEOUT,
    );
    assert_eq!(conn.attempts(), 1, "the second request never dialled");
    hanging.cancel(1).await;
    assert!(matches!(
        collect(&mut hanging, 1).await.end,
        Outcome::Cancelled
    ));
    wait_for("the dial slot freed", || {
        d.engine.dials_in_progress("c") == 0
    })
    .await;
    assert_eq!(
        d.engine.connections("c"),
        0,
        "a cancelled dial frees its connection slot"
    );
    conn.set(TCP);
    assert_eq!(status(&one(&d, &idempotent_get("c")).await), 200);
    assert_eq!(received(&up), 1);
}

// =================================================================================================
// Chaos 10: the rate limit (§23.8.7)
// =================================================================================================

/// **Chaos 10.** One bucket per upstream, shared host-wide: a burst of 3 is spent across TWO
/// sessions, the fourth request (on either) is refused at once — `RateLimited`, Retryable,
/// `rate_limited` — with `retry_after_ms` the time to the next token, and nothing is sent. After
/// that time one more request is admitted, and the bucket is empty again.
///
/// `RATE_PER_SEC=0.5` (one token per 2 s), `RATE_BURST=3`: by GCRA the fourth token is due 2 s
/// after the FIRST request was admitted, so `retry_after_ms = 2000 − (t₄ − t₁)` — bounded here by
/// instants the test took around those requests, never by how fast the runner is.
#[tokio::test]
async fn chaos10_the_rate_limit_is_shared_across_sessions_with_the_right_retry_after() {
    let (up, _gate) = scripted(vec![]).await;
    let d = daemon_on(
        upstreams(
            &[("r", up.addr)],
            &[("r", "RATE_PER_SEC", "0.5"), ("r", "RATE_BURST", "3")],
        ),
        ScriptConnect::new(TCP),
    );
    let mut a = d.client().await;
    let mut b = d.client().await;
    let t1_before = Instant::now();
    assert_eq!(
        status(&exchange(&mut a, 1, &idempotent_get("r")).await),
        200
    );
    let t1_after = Instant::now();
    assert_eq!(
        status(&exchange(&mut b, 1, &idempotent_get("r")).await),
        200
    );
    assert_eq!(status(&exchange(&mut a, 2, &post("r", b"x")).await), 200);
    let t4_before = Instant::now();
    let r = exchange(&mut b, 2, &post("r", b"y")).await;
    let t4_after = Instant::now();
    let ra = r
        .assert_error(
            errc::RATE_LIMITED,
            branch::RETRYABLE,
            http_cause::RATE_LIMITED,
        )
        .retry_after_ms
        .expect("rate_limited carries retry_after_ms");
    let ms = |d: Duration| d.as_millis();
    let lo = 2_000u128.saturating_sub(ms(t4_after - t1_before));
    let hi = 2_000u128.saturating_sub(ms(t4_before - t1_after)) + 1;
    assert!(
        (lo..=hi).contains(&u128::from(ra)),
        "retry_after_ms {ra} not in {lo}..={hi}"
    );
    assert_eq!(received(&up), 3, "the refused request was not sent");
    let r = exchange(&mut a, 3, &idempotent_get("r")).await;
    r.assert_error(
        errc::RATE_LIMITED,
        branch::RETRYABLE,
        http_cause::RATE_LIMITED,
    );

    // Refill: wait AT LEAST the time named, then one token.
    tokio::time::sleep(Duration::from_millis(u64::from(ra) + 50)).await;
    assert_eq!(
        status(&exchange(&mut b, 3, &idempotent_get("r")).await),
        200
    );
    let r = exchange(&mut a, 4, &idempotent_get("r")).await;
    let again = r
        .assert_error(
            errc::RATE_LIMITED,
            branch::RETRYABLE,
            http_cause::RATE_LIMITED,
        )
        .retry_after_ms
        .unwrap();
    assert!(again <= 2_000, "{again}");
    assert_eq!(received(&up), 4);
}

/// `RATE_MAX_WAIT_MS`: a request with no token waits for it — admitted no earlier than the token
/// is due — unless the wait exceeds `min(RATE_MAX_WAIT_MS, its remaining deadline)`, which refuses
/// it at once; a request `CANCEL`led while it waits is `Cancelled` and never sent.
#[tokio::test]
async fn a_rate_wait_is_bounded_by_the_max_wait_and_the_deadline() {
    let (up, _gate) = scripted(vec![]).await;
    let d = daemon_on(
        upstreams(
            &[("w", up.addr), ("slow", up.addr)],
            &[
                ("w", "RATE_PER_SEC", "1"),
                ("w", "RATE_MAX_WAIT_MS", "5000"),
                // One token per 5 s, burst 1.
                ("slow", "RATE_PER_SEC", "0.2"),
                ("slow", "RATE_MAX_WAIT_MS", "10000"),
            ],
        ),
        ScriptConnect::new(TCP),
    );
    let t1_before = Instant::now();
    assert_eq!(status(&one(&d, &idempotent_get("w")).await), 200);
    // The next token is ~1 s away; a 300 ms deadline cannot wait for it.
    let r = one(
        &d,
        &HttpRequest {
            timeout_ms: Some(300),
            ..idempotent_get("w")
        },
    )
    .await;
    r.assert_error(
        errc::RATE_LIMITED,
        branch::RETRYABLE,
        http_cause::RATE_LIMITED,
    );
    assert_eq!(received(&up), 1);
    // This one waits for the token.
    assert_eq!(status(&one(&d, &idempotent_get("w")).await), 200);
    assert!(
        t1_before.elapsed() >= Duration::from_millis(1_000),
        "admitted no earlier than its token: {:?}",
        t1_before.elapsed()
    );
    // CANCEL while waiting for the next token (~5 s away): answered at once, not when the token
    // would have been due, and never sent.
    assert_eq!(status(&one(&d, &idempotent_get("slow")).await), 200);
    let mut c = start(&d, &post("slow", b"x")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let cancelled_at = Instant::now();
    c.cancel(1).await;
    assert!(matches!(collect(&mut c, 1).await.end, Outcome::Cancelled));
    assert!(
        cancelled_at.elapsed() < Duration::from_millis(2_500),
        "the wait is abandoned on CANCEL: {:?}",
        cancelled_at.elapsed()
    );
    assert_eq!(received(&up), 3, "the cancelled waiter was not sent");
}

// =================================================================================================
// The Retry-After hold (§23.8.7)
// =================================================================================================

/// **`HONOR_RETRY_AFTER=1`.** A completed 429 with `Retry-After: 1` is delivered to its caller as
/// the response it is, and HOLDS the upstream: later requests — on any session — are refused at
/// once (`RateLimited`, `retry_after_hold`, `retry_after_ms` ≤ 1000), unsent; once the hold has
/// passed, requests flow again. A 503 with a `Retry-After` holds too.
#[tokio::test]
async fn the_retry_after_hold_is_honoured_and_expires() {
    const LIMITED: &[u8] =
        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n";
    const UNAVAILABLE: &[u8] =
        b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n";
    let (up, _gate) = scripted(vec![
        Act::Answer(LIMITED),
        Act::Answer(OK),
        Act::Answer(UNAVAILABLE),
    ])
    .await;
    let d = daemon_on(
        upstreams(&[("h", up.addr)], &[("h", "HONOR_RETRY_AFTER", "1")]),
        ScriptConnect::new(TCP),
    );
    for (first, code) in [(true, 429), (false, 503)] {
        let r = one(&d, &post("h", b"x")).await;
        assert_eq!(status(&r), code);
        let held_at = Instant::now();
        let r = one(&d, &idempotent_get("h")).await;
        let left = r
            .assert_error(
                errc::RATE_LIMITED,
                branch::RETRYABLE,
                http_cause::RETRY_AFTER_HOLD,
            )
            .retry_after_ms
            .expect("retry_after_hold carries retry_after_ms");
        assert!((1..=1_000).contains(&left), "{left}");
        let n = received(&up);
        assert_eq!(
            n,
            if first { 1 } else { 3 },
            "the held request was not sent"
        );
        tokio::time::sleep(Duration::from_millis(u64::from(left) + 50)).await;
        assert!(held_at.elapsed() >= Duration::from_millis(u64::from(left)));
        if first {
            assert_eq!(status(&one(&d, &post("h", b"y")).await), 200);
        }
    }
    assert_eq!(status(&one(&d, &idempotent_get("h")).await), 200);
    assert_eq!(received(&up), 4);
}

/// What does NOT hold: the default (`HONOR_RETRY_AFTER=0`); a 429 with no `Retry-After` (no
/// duration to hold for); a `Retry-After` above `RETRY_AFTER_MAX_MS`; a status other than 429/503.
#[tokio::test]
async fn what_does_not_hold() {
    const LIMITED: &[u8] =
        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n";
    const BARE: &[u8] = b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n";
    const FAR: &[u8] =
        b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 9\r\nContent-Length: 0\r\n\r\n";
    const OTHER: &[u8] =
        b"HTTP/1.1 500 Internal Server Error\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n";
    let (off, _g) = scripted(vec![Act::Answer(LIMITED)]).await;
    let (on, _g2) = scripted(vec![
        Act::Answer(BARE),
        Act::Answer(FAR),
        Act::Answer(OTHER),
    ])
    .await;
    let d = daemon_on(
        upstreams(
            &[("off", off.addr), ("on", on.addr)],
            &[
                ("on", "HONOR_RETRY_AFTER", "1"),
                ("on", "RETRY_AFTER_MAX_MS", "5000"),
            ],
        ),
        ScriptConnect::new(TCP),
    );
    assert_eq!(status(&one(&d, &post("off", b"x")).await), 429);
    assert_eq!(status(&one(&d, &post("off", b"x")).await), 200);
    for code in [429, 503, 500] {
        assert_eq!(status(&one(&d, &post("on", b"x")).await), code);
    }
    assert_eq!(status(&one(&d, &post("on", b"x")).await), 200);
    assert_eq!((received(&off), received(&on)), (2, 4));
}

// =================================================================================================
// The admission order (§23.6 step 4)
// =================================================================================================

/// **(b) before (c), and a refusal takes nothing from a later step.** A held upstream refuses with
/// `retry_after_hold` even with its rate bucket empty, and an open breaker refuses `breaker_open`
/// before the hold or the bucket is consulted — so the requests refused at (b) consume no token: the
/// bucket's one token is still there for the first request after the breaker closes.
#[tokio::test]
async fn the_admission_order_is_breaker_hold_then_rate() {
    const LIMITED: &[u8] =
        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n";
    let (up, _gate) = scripted(vec![Act::Answer(LIMITED)]).await;
    let conn = ScriptConnect::new(TCP);
    let d = daemon_on(
        upstreams(
            &[("o", up.addr)],
            &[
                ("o", "HONOR_RETRY_AFTER", "1"),
                // Burst 2, one token per 100 s: the 429 and the refused connect below take both.
                ("o", "RATE_PER_SEC", "0.01"),
                ("o", "RATE_BURST", "3"),
                ("o", "BREAKER_FAILURES", "1"),
                ("o", "BREAKER_OPEN_MS", "300"),
                ("o", "H1_UNSAFE_REUSE_MAX_IDLE_MS", "0"),
            ],
        ),
        Arc::clone(&conn),
    );
    assert_eq!(status(&one(&d, &post("o", b"x")).await), 429); // token 1; holds 1 s
    let r = one(&d, &post("o", b"x")).await;
    r.assert_error(
        errc::RATE_LIMITED,
        branch::RETRYABLE,
        http_cause::RETRY_AFTER_HOLD,
    );
    tokio::time::sleep(Duration::from_millis(1_050)).await;
    conn.set(REFUSE);
    let r = one(&d, &post("o", b"x")).await; // token 2; opens the breaker
    assert_unavailable(&r, http_cause::CONNECT_REFUSED);
    for _ in 0..3 {
        let r = one(&d, &post("o", b"x")).await;
        assert_unavailable(&r, http_cause::BREAKER_OPEN);
    }
    conn.set(TCP);
    wait_for("the breaker to half-open", || {
        d.engine.breaker_state("o") == Some(BreakerState::HalfOpen)
    })
    .await;
    // Token 3 is still there: the breaker_open refusals took none.
    assert_eq!(status(&one(&d, &post("o", b"x")).await), 200);
    let r = one(&d, &post("o", b"x")).await;
    r.assert_error(
        errc::RATE_LIMITED,
        branch::RETRYABLE,
        http_cause::RATE_LIMITED,
    );
    assert_eq!(received(&up), 2);
}
