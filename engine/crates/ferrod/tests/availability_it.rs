//! M1-S9a Task 11 — the availability knobs (M0-core-review finding 5). All OFFLINE: the attacks
//! are wire-shaped, so no database is involved and nothing here skips.
//!
//! Timing is REAL (these sessions do genuine socket IO on a real `UnixListener`), so the configs
//! use sub-second knobs and every assertion is a generous multiple — never an equality on elapsed
//! time. Enforcement runs at `config::LIVENESS_TICK` (1 s) granularity, so an expiry lands
//! somewhere in `[timeout, timeout + 2 x tick]`; the waits below are sized for the upper end.
//!
//! Two of the tests exist specifically to pin the semantics the plan got wrong (S5), and they are
//! the ones to read first if this file ever goes red:
//! `a_slow_but_progressing_client_is_never_killed` and
//! `an_enabled_idle_timeout_never_reaps_a_session_mid_frame`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{connect, spawn_one_session_with_config, spawn_serve_with_config};
use ferro_proto::consts::{MAX_FRAME_PAYLOAD, branch, errc, flags, service};
use ferro_proto::header::Header;
use ferro_proto::messages::Outcome;
use ferrod::config::Config;
use ferrod::epoch::{EpochSource, RandomEpoch};
use ferrod::shutdown::Drain;

/// Answers every request-bearing frame with an empty Ok terminal, immediately. There is no shared
/// `default_handler` in `common/` (each test file defines its own), and a trivial one is what
/// isolates the SESSION-liveness properties from anything a handler might do.
fn stub_handler() -> ferrod::session::HandlerFn {
    use futures::FutureExt;
    Arc::new(|_frame, responder, _cancel| {
        async move {
            responder.end_ok(bytes::Bytes::new());
        }
        .boxed()
    })
}

/// A handler that takes `d` to declare its terminal, ignoring cancellation — a stand-in for a long
/// upstream query. Used to prove in-flight work vetoes the idle reaper.
fn slow_handler(d: Duration) -> ferrod::session::HandlerFn {
    use futures::FutureExt;
    Arc::new(move |_frame, responder, _cancel| {
        async move {
            tokio::time::sleep(d).await;
            responder.end_ok(bytes::Bytes::new());
        }
        .boxed()
    })
}

fn header_bytes(request_id: u32, payload_len: u32) -> Vec<u8> {
    Header {
        flags: 0,
        service: service::SQL,
        method: 1,
        request_id,
        payload_len,
    }
    .encode()
    .to_vec()
}

/// finding 5c: a partial frame that stops making progress is session-fatal within
/// `frame_read_timeout` (+ tick granularity) — a 17-byte send can no longer hold a session, a
/// writer task and an fd hostage indefinitely (measured at HEAD: no reply, no close, still
/// writable after 1.2 s).
#[tokio::test]
async fn a_stalled_partial_frame_is_session_fatal_within_the_deadline() {
    let config = Config {
        frame_read_timeout: Duration::from_millis(200),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(config, epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;

    // A header declaring the 16 MiB maximum, one body byte, then silence.
    let mut bytes = header_bytes(9, MAX_FRAME_PAYLOAD);
    bytes.push(0);
    c.send_raw_bytes(&bytes).await;

    let fatal = c
        .recv_or_none(Duration::from_secs(5))
        .await
        .expect("a stalled partial frame must be answered with a session-fatal frame, not held");
    assert_eq!(
        fatal.header.request_id, 0,
        "session-fatal terminals are rid=0 (the frame was never a request)"
    );
    assert_eq!(fatal.header.flags & flags::END, flags::END);
    match Outcome::decode(&fatal.payload).expect("Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL, "got {:#06x}", ep.code),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
    c.recv_eof().await;
}

/// A truncated HEADER is the cheapest hostage of all — three bytes, no declared length, nothing to
/// reserve. It must be bounded by the same deadline; the obvious implementation returns early
/// before any bookkeeping and leaves this shape completely invisible.
#[tokio::test]
async fn a_stalled_partial_header_is_session_fatal_too() {
    let config = Config {
        frame_read_timeout: Duration::from_millis(200),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(config, epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;

    c.send_raw_bytes(&header_bytes(9, 0)[..3]).await;

    let fatal = c
        .recv_or_none(Duration::from_secs(5))
        .await
        .expect("three bytes of a header must not hold the session either");
    assert_eq!(fatal.header.request_id, 0);
    c.recv_eof().await;
}

/// **The S5 guard.** A client that delivers a frame SLOWLY but continuously must never be killed:
/// the deadline is on PROGRESS, not on completion. Under the started-based shape the plan
/// originally specified, this session dies at ~2 s with a fatal PROTOCOL frame while its bytes are
/// still arriving — which is the same shape a healthy client takes on a loaded host, since the rate
/// it achieves is bounded by how often the daemon's reader task is scheduled.
///
/// Trickles a 64-byte payload in 4-byte chunks 150 ms apart (~2.4 s total, i.e. several ticks and
/// 8x the 300 ms deadline), then asserts the frame was reassembled and answered.
#[tokio::test]
async fn a_slow_but_progressing_client_is_never_killed() {
    let config = Config {
        frame_read_timeout: Duration::from_millis(300),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(config, epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;

    const BODY: usize = 64;
    c.send_raw_bytes(&header_bytes(7, BODY as u32)).await;
    for chunk in 0..(BODY / 4) {
        tokio::time::sleep(Duration::from_millis(150)).await;
        c.send_raw_bytes(&[chunk as u8; 4]).await;
    }

    let t = c
        .recv_or_none(Duration::from_secs(5))
        .await
        .expect("a client that never stops sending must never be killed for being slow");
    assert_eq!(
        t.header.request_id, 7,
        "expected the trickled request's own terminal; rid=0 here means the session was killed \
         mid-frame — the deadline is measuring COMPLETION, not progress"
    );
    assert_eq!(t.header.flags & flags::END, flags::END);
    match Outcome::decode(&t.payload).expect("Outcome") {
        Outcome::Ok(_) => {}
        other => panic!("expected the stub handler's Ok terminal, got {other:?}"),
    }
}

/// finding 5c, the other half: `idle_timeout` defaults DISABLED, so the normal PHP-FPM shape — a
/// worker that is quiet for minutes between web requests, and CANNOT ping in the meantime because
/// the sync client has no background thread — is never severed by default.
#[tokio::test]
async fn idle_timeout_defaults_off_and_a_quiet_session_survives() {
    assert!(
        Config::default().idle_timeout.is_none(),
        "idle_timeout MUST default disabled: the sync PHP client cannot ping while blocked \
         between requests, so a nonzero default severs every quiet worker on the host"
    );

    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(Config::default(), epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;
    tokio::time::sleep(Duration::from_millis(2500)).await; // several ticks of silence
    c.ping(2, 77).await; // asserts its own PONG internally
}

/// When an operator DOES enable it, a genuinely quiet session closes — silently, no terminal, since
/// there is no request to fail.
#[tokio::test]
async fn an_enabled_idle_timeout_reaps_a_quiet_session() {
    let config = Config {
        idle_timeout: Some(Duration::from_millis(300)),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(config, epoch, stub_handler());
    let mut quiet = connect(&sock).await;
    quiet.hello(1).await;

    tokio::time::timeout(Duration::from_secs(5), quiet.recv_eof())
        .await
        .expect("an enabled idle_timeout must close a quiet session");
}

/// An enabled idle_timeout must NEVER cut an in-flight request: a streaming or slow response
/// consumes no INBOUND frames, so on the read side a hard-working session is indistinguishable from
/// a silent one.
///
/// The vantage point matters, and the obvious one does not work: with a default `drain_deadline`
/// the session's own cleanup path still waits for the handler and still delivers its terminal, so
/// dropping the in-flight veto is INVISIBLE from "did the terminal arrive". `drain_deadline` is
/// therefore set BELOW the handler's runtime, which makes the difference observable: veto intact ⇒
/// the terminal arrives at ~1.2 s; veto removed ⇒ the session breaks at the first tick, the drain
/// expires, the supervisor is aborted before the handler declares, and the client gets EOF instead.
#[tokio::test]
async fn an_enabled_idle_timeout_never_cuts_an_in_flight_request() {
    let config = Config {
        idle_timeout: Some(Duration::from_millis(300)),
        drain_deadline: Duration::from_millis(100),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) =
        spawn_one_session_with_config(config, epoch, slow_handler(Duration::from_millis(2000)));
    let mut busy = connect(&sock).await;
    busy.hello(1).await;
    busy.send_request(2, service::SQL, 1, Vec::new()).await;

    let t = busy
        .recv_or_none(Duration::from_secs(6))
        .await
        .expect("in-flight work must veto the idle close — the request's terminal must arrive");
    assert_eq!(t.header.request_id, 2);
    assert_eq!(t.header.flags & flags::END, flags::END);
}

/// The same veto, for the OTHER kind of activity the read side cannot see as activity: a
/// half-received frame. A client mid-send is not idle, and reaping it would sever exactly the
/// large-payload client that takes the longest to say anything.
#[tokio::test]
async fn an_enabled_idle_timeout_never_reaps_a_session_mid_frame() {
    let config = Config {
        idle_timeout: Some(Duration::from_millis(300)),
        // Deliberately far away, so ONLY the idle path can end this session.
        frame_read_timeout: Duration::from_secs(30),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(config, epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;

    const BODY: usize = 64;
    c.send_raw_bytes(&header_bytes(7, BODY as u32)).await;
    for chunk in 0..(BODY / 4) {
        tokio::time::sleep(Duration::from_millis(150)).await;
        c.send_raw_bytes(&[chunk as u8; 4]).await;
    }

    let t = c
        .recv_or_none(Duration::from_secs(5))
        .await
        .expect("a session with a half-received frame is not idle");
    assert_eq!(
        t.header.request_id, 7,
        "expected the request's terminal; EOF/none here means the idle reaper fired mid-frame"
    );
}

/// finding 5b: the `max_connections` cap — connection N+1 gets ONE loud, RETRYABLE frame and a
/// close (SPEC G-4: never a silent drop), and never becomes a session. The branch is the load-
/// bearing half: a client that reads this as fatal turns a momentary cap into an application error.
#[tokio::test]
async fn the_connection_cap_rejects_the_overflow_connection_loudly() {
    let config = Config {
        max_connections: 1,
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let drain = Drain::new();
    let (sock, _task) = spawn_serve_with_config(config, epoch, drain, stub_handler());

    let mut first = connect(&sock).await;
    first.hello(1).await; // occupies the one slot

    let mut second = connect(&sock).await;
    let frame = second
        .recv_or_none(Duration::from_secs(3))
        .await
        .expect("the overflow connection must be answered, not silently dropped (SPEC G-4)");
    assert_eq!(frame.header.request_id, 0);
    assert_eq!(frame.header.flags & flags::END, flags::END);
    match Outcome::decode(&frame.payload).expect("Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(
                ep.code,
                errc::POOL_TIMEOUT,
                "the overflow reject rides POOL_TIMEOUT, got {:#06x}",
                ep.code
            );
            assert_eq!(
                ep.branch,
                branch::RETRYABLE,
                "being at the cap is momentary — a non-retryable answer would turn a load spike \
                 into an application error"
            );
        }
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
    second.recv_eof().await;

    // The session already running is untouched by the rejection.
    first.ping(2, 42).await;
}

/// The cap must RELEASE: a slot freed by a departing client is usable again. Without the accept
/// loop's reap arm feeding `JoinSet::len()`, the cap would be a one-way ratchet that turns any
/// connection churn into a permanent outage — the exact failure shape this slice exists to close
/// elsewhere in the pool.
#[tokio::test]
async fn a_departed_session_frees_its_slot() {
    let config = Config {
        max_connections: 1,
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_serve_with_config(config, epoch, Drain::new(), stub_handler());

    let mut first = connect(&sock).await;
    first.hello(1).await;
    drop(first); // the session task observes EOF and ends

    // The accept loop reaps opportunistically, so allow a few scheduling turns.
    let mut accepted = false;
    for _ in 0..40 {
        let mut next = connect(&sock).await;
        match next.recv_or_none(Duration::from_millis(150)).await {
            // Still rejected (the previous session has not been reaped yet): try again.
            Some(_rejection) => tokio::time::sleep(Duration::from_millis(50)).await,
            // Nothing pushed at us: this connection became a real session — prove it handshakes.
            None => {
                next.hello(1).await;
                accepted = true;
                break;
            }
        }
    }
    assert!(
        accepted,
        "a departed session must free its slot — the cap is not a one-way ratchet"
    );
}
