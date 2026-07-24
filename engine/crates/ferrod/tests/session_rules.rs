//! S3 Task 4: in-flight registry + consuming-typestate `Responder` + supervisor tests — the
//! airtight exactly-one-`END` core.
//!
//! Unit-level tests construct the registry/`Responder`/supervisor pieces directly and drain a
//! plain `tokio::sync::mpsc` control receiver to observe terminals, without going through a real
//! `Session`. Integration-level tests drive a real `Session::run_with_handler` connection via the
//! `tests/common` harness (real bound `UnixListener`, timeout-guarded `recv`).
//!
//! Every test is barrier-gated with `tokio::sync::Notify` where concurrency matters — no sleeps.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use std::time::Duration;

use common::TestServer;
use ferro_proto::consts::{MAX_FRAME_PAYLOAD, errc, flags, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{Outcome, Pong};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::session::HandlerFn;
use ferrod::session::codec::{InFrame, OutFrame};
use ferrod::session::flow::Credit;
use ferrod::session::registry::{InsertErr, Registry};
use ferrod::session::responder::Responder;
use ferrod::session::supervisor;
use ferrod::shutdown::Drain;

/// A default-ish credit window for tests that don't care about flow control, only registry
/// membership/lifecycle.
fn test_credit() -> Credit {
    Credit::new(
        ferro_proto::consts::DEFAULT_CREDIT_FRAMES,
        ferro_proto::consts::DEFAULT_CREDIT_BYTES,
    )
}

/// A stable placeholder method id for request-bearing test frames. SQL/TX/STREAM don't have
/// registry-defined method ids yet (that lands with the real S4/S5 handlers); the mechanism under
/// test here only routes on `service`.
const SOME_METHOD: u16 = 1;

// ---------------------------------------------------------------------------------------------
// Unit-level: registry
// ---------------------------------------------------------------------------------------------

#[test]
fn registry_reuse_and_full() {
    let max_inflight = 2;
    let registry = Registry::new(max_inflight);

    assert!(registry.insert(1, test_credit()).is_ok());
    assert_eq!(
        registry.insert(1, test_credit()).map(|_| ()),
        Err(InsertErr::Reused)
    );

    assert!(registry.insert(2, test_credit()).is_ok());
    assert_eq!(registry.len(), 2);
    assert_eq!(
        registry.insert(3, test_credit()).map(|_| ()),
        Err(InsertErr::Full)
    );
}

// ---------------------------------------------------------------------------------------------
// Unit-level: supervisor (construct the pieces directly, drain a control receiver)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn supervisor_sends_declared_ok_exactly_once() {
    let (control_tx, mut control_rx) = mpsc::channel::<OutFrame>(8);
    let registry = Arc::new(Registry::new(4));
    registry.insert(7, test_credit()).unwrap();

    let (responder, cell) = Responder::new_pair();
    let permit = control_tx.clone().reserve_owned().await.unwrap();
    let handle = tokio::spawn(async move {
        responder.end_ok(Bytes::from_static(b"x"));
    });

    supervisor::supervise(
        7,
        service::SQL,
        SOME_METHOD,
        permit,
        cell,
        handle,
        registry.clone(),
    )
    .await;

    let frame = control_rx.try_recv().expect("exactly one terminal frame");
    assert_eq!(frame.header.request_id, 7);
    assert_eq!(frame.header.flags, flags::END);
    // The terminal frame must carry the ORIGINAL request's service/method, not a hard-coded
    // value — a regression that hard-codes these (e.g. to CORE/0) would slip past a test that
    // only checks request_id/flags/payload.
    assert_eq!(frame.header.service, service::SQL);
    assert_eq!(frame.header.method, SOME_METHOD);
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Ok(body) => assert_eq!(body, b"x"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }

    // At-most-one: nothing else was ever sent.
    assert!(control_rx.try_recv().is_err(), "expected no second frame");
    assert_eq!(
        registry.len(),
        0,
        "supervisor must remove the registry entry"
    );
}

#[tokio::test]
async fn supervisor_synthesizes_on_panic_with_distinct_code() {
    let (control_tx, mut control_rx) = mpsc::channel::<OutFrame>(8);
    let registry = Arc::new(Registry::new(4));
    registry.insert(11, test_credit()).unwrap();

    let (responder, cell) = Responder::new_pair();
    let permit = control_tx.clone().reserve_owned().await.unwrap();
    let handle = tokio::spawn(async move {
        // Moved in but never declared — the panic is the ONLY thing that happens.
        let _responder = responder;
        panic!("intentional: handler panics without declaring (panic-isolation test)");
    });

    supervisor::supervise(
        11,
        service::SQL,
        SOME_METHOD,
        permit,
        cell,
        handle,
        registry.clone(),
    )
    .await;

    let frame = control_rx
        .try_recv()
        .expect("exactly one synthesized terminal");
    assert_eq!(frame.header.flags, flags::END);
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::PROTOCOL);
            assert_eq!(ep.detail.as_deref(), Some(supervisor::NO_TERMINAL_DETAIL));
        }
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
    assert!(control_rx.try_recv().is_err(), "expected no second frame");
    assert_eq!(registry.len(), 0);
}

#[tokio::test]
async fn declare_then_panic_yields_single_synth_terminal() {
    // A handler that DOES declare an outcome, then panics afterward. `supervise`'s
    // `Err(join_err)` branch (a panicked JoinHandle) synthesizes unconditionally — it never
    // inspects the cell — so the declared `Ok` is discarded in favor of the synthetic error.
    // This pins that semantic explicitly: a future refactor that tried to "recover" the
    // declared outcome on panic would be a behavior change, and this test would catch it.
    let (control_tx, mut control_rx) = mpsc::channel::<OutFrame>(8);
    let registry = Arc::new(Registry::new(4));
    registry.insert(31, test_credit()).unwrap();

    let (responder, cell) = Responder::new_pair();
    let permit = control_tx.clone().reserve_owned().await.unwrap();
    let handle = tokio::spawn(async move {
        responder.end_ok(Bytes::from_static(b"declared-before-panic"));
        panic!("intentional: handler declares Ok, then panics anyway");
    });

    supervisor::supervise(
        31,
        service::SQL,
        SOME_METHOD,
        permit,
        cell,
        handle,
        registry.clone(),
    )
    .await;

    let frame = control_rx
        .try_recv()
        .expect("exactly one synthesized terminal");
    assert_eq!(frame.header.flags, flags::END);
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::PROTOCOL);
            assert!(
                ep.detail
                    .as_deref()
                    .is_some_and(|d| d.contains(supervisor::NO_TERMINAL_DETAIL)),
                "expected detail to contain {:?}, got {:?}",
                supervisor::NO_TERMINAL_DETAIL,
                ep.detail
            );
        }
        other => panic!(
            "expected the synthetic Outcome::Error (declared Ok must be discarded on panic), got {other:?}"
        ),
    }
    assert!(control_rx.try_recv().is_err(), "expected no second frame");
    assert_eq!(registry.len(), 0);
}

#[tokio::test]
async fn supervisor_synthesizes_on_no_terminal() {
    let (control_tx, mut control_rx) = mpsc::channel::<OutFrame>(8);
    let registry = Arc::new(Registry::new(4));
    registry.insert(21, test_credit()).unwrap();

    let (responder, cell) = Responder::new_pair();
    let permit = control_tx.clone().reserve_owned().await.unwrap();
    let handle = tokio::spawn(async move {
        // Returns normally without ever calling any end_* — same bug path as a panic.
        drop(responder);
    });

    supervisor::supervise(
        21,
        service::SQL,
        SOME_METHOD,
        permit,
        cell,
        handle,
        registry.clone(),
    )
    .await;

    let frame = control_rx
        .try_recv()
        .expect("exactly one synthesized terminal");
    assert_eq!(frame.header.flags, flags::END);
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::PROTOCOL);
            assert_eq!(ep.detail.as_deref(), Some(supervisor::NO_TERMINAL_DETAIL));
        }
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
    assert!(control_rx.try_recv().is_err(), "expected no second frame");
    assert_eq!(registry.len(), 0);
}

// ---------------------------------------------------------------------------------------------
// Integration-level: through Session::run_with_handler + the Task 3 harness
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn panic_isolation_session_survives() {
    let handler: HandlerFn = Arc::new(
        |_frame: InFrame, _responder: Responder, _cancel: CancellationToken| {
            async move {
                panic!("intentional: handler panics without declaring (panic-isolation test)");
            }
            .boxed()
        },
    );

    let server = TestServer::spawn_with_handler(BootEpoch(1), handler);
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send_request(100, service::SQL, SOME_METHOD, vec![])
        .await;

    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 100);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::PROTOCOL);
            assert_eq!(ep.detail.as_deref(), Some(supervisor::NO_TERMINAL_DETAIL));
        }
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // The session survived the handler panic: PING still gets a PONG.
    client.ping(9, 42).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.service, service::CORE);
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
    let pong = Pong::decode(&pong_frame.payload).expect("decode PONG");
    assert_eq!(pong.token, 42);
}

#[tokio::test]
async fn reused_inflight_id_diagnostic_original_undisturbed() {
    let notify = Arc::new(Notify::new());
    let handler_notify = notify.clone();
    let handler: HandlerFn = Arc::new(
        move |_frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            let notify = handler_notify.clone();
            async move {
                notify.notified().await;
                responder.end_ok(Bytes::from_static(b"done"));
            }
            .boxed()
        },
    );

    let server = TestServer::spawn_with_handler(BootEpoch(1), handler);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Request A: the handler blocks on `notify` until we release it below.
    client
        .send_request(50, service::SQL, SOME_METHOD, vec![])
        .await;
    // Reuse A's request_id before A has terminated.
    client
        .send_request(50, service::SQL, SOME_METHOD, vec![])
        .await;

    // The reader loop processes frames strictly in order and awaits registry-insertion (a fast,
    // synchronous step) before moving on to the next frame, so by the time frame 2 is read, A's
    // id is already registered — no sleep needed for this ordering.
    let diagnostic = client.recv().await;
    assert_eq!(diagnostic.header.request_id, 50);
    assert_eq!(diagnostic.header.flags, flags::END);
    match Outcome::decode(&diagnostic.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // Release A: it must still complete with its own single terminal END.
    notify.notify_one();
    let original = client.recv().await;
    assert_eq!(original.header.request_id, 50);
    assert_eq!(original.header.flags, flags::END);
    match Outcome::decode(&original.payload).expect("decode Outcome") {
        Outcome::Ok(body) => assert_eq!(body, b"done"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }

    // Exactly once: the session is still healthy and produces nothing further for id 50.
    client.ping(9, 7).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
}

#[tokio::test]
async fn max_inflight_exceeded_is_per_request_error() {
    let max_inflight = 2;
    // One release signal PER held handler, keyed by request_id -- NOT one shared `Notify`.
    // `tokio::sync::Notify` stores at most one permit: if both handler tasks aren't already
    // parked in `.notified().await` by the time the test fires two consecutive `notify_one()`
    // calls, the second call can be dropped on the floor (whichever handler calls `.notified()`
    // second then waits forever, hanging the test) rather than deterministically waking both.
    // Giving each in-flight handler its own `Notify` makes the release deterministic regardless
    // of scheduling.
    let notify_1 = Arc::new(Notify::new());
    let notify_2 = Arc::new(Notify::new());
    let notify_by_id: Arc<HashMap<u32, Arc<Notify>>> = Arc::new(HashMap::from([
        (1, notify_1.clone()),
        (2, notify_2.clone()),
    ]));
    let handler: HandlerFn = Arc::new(
        move |frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            let notify_by_id = notify_by_id.clone();
            async move {
                let notify = notify_by_id
                    .get(&frame.header.request_id)
                    .expect("test only ever sends request ids 1 and 2 to this handler")
                    .clone();
                notify.notified().await;
                responder.end_ok(Bytes::new());
            }
            .boxed()
        },
    );

    let server =
        TestServer::spawn_with_handler_and_max_inflight(BootEpoch(1), max_inflight, handler);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Hold `max_inflight` requests in-flight.
    client
        .send_request(1, service::SQL, SOME_METHOD, vec![])
        .await;
    client
        .send_request(2, service::SQL, SOME_METHOD, vec![])
        .await;
    // One more: over capacity.
    client
        .send_request(3, service::SQL, SOME_METHOD, vec![])
        .await;

    let diagnostic = client.recv().await;
    assert_eq!(diagnostic.header.request_id, 3);
    assert_eq!(diagnostic.header.flags, flags::END);
    match Outcome::decode(&diagnostic.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // Session still answers PING while 1 and 2 remain in-flight.
    client.ping(9, 5).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);

    // Release both held requests, deterministically (each on its own signal), so the connection
    // winds down cleanly.
    notify_1.notify_one();
    notify_2.notify_one();
    let mut seen: HashSet<u32> = HashSet::new();
    for _ in 0..2 {
        let frame = client.recv().await;
        assert_eq!(frame.header.flags, flags::END);
        seen.insert(frame.header.request_id);
    }
    assert_eq!(seen, HashSet::from([1, 2]));
}

// ---------------------------------------------------------------------------------------------
// S3 Task 5: PING/PONG multiplexing, flag-based CANCEL, GOODBYE drain, WINDOW_UPDATE + Credit.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn multiplexed_ping_while_inflight() {
    let notify = Arc::new(Notify::new());
    let handler_notify = notify.clone();
    let handler: HandlerFn = Arc::new(
        move |_frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            let notify = handler_notify.clone();
            async move {
                notify.notified().await;
                responder.end_ok(Bytes::new());
            }
            .boxed()
        },
    );

    let server = TestServer::spawn_with_handler(BootEpoch(1), handler);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Start request A: the handler blocks on `notify` until we release it below.
    client
        .send_request(100, service::SQL, SOME_METHOD, vec![])
        .await;

    // PING (a distinct request_id/token from A) must be answered BEFORE A's terminal — the
    // reader loop answers PING synchronously per-frame, independent of the spawned handler task
    // that owns A, so this is deterministic without any sleep: if PONG were somehow queued
    // behind A, this recv would time out instead of returning PONG.
    client.ping(9, 7).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.service, service::CORE);
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
    let pong = Pong::decode(&pong_frame.payload).expect("decode PONG");
    assert_eq!(pong.token, 7);

    // Only now release A: it must still terminate normally.
    notify.notify_one();
    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 100);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Ok(_) => {}
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}

#[tokio::test]
async fn cancel_inflight_yields_cancelled_terminal() {
    let handler: HandlerFn = Arc::new(
        |_frame: InFrame, responder: Responder, cancel: CancellationToken| {
            async move {
                // The CancellationToken itself provides the synchronization: `cancelled()`
                // resolves immediately if the token is already cancelled by the time this task
                // gets to run, or waits until it is — either way, no sleep/barrier needed here.
                cancel.cancelled().await;
                responder.end_cancelled();
            }
            .boxed()
        },
    );

    let server = TestServer::spawn_with_handler(BootEpoch(1), handler);
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send_request(5, service::SQL, SOME_METHOD, vec![])
        .await;
    client.cancel(5).await;

    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 5);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Cancelled => {}
        other => panic!("expected Outcome::Cancelled (not Error), got {other:?}"),
    }

    // A second CANCEL on the now-completed id 5 is a no-op: no extra frame, session stays
    // healthy (PING still answered).
    client.cancel(5).await;
    client.ping(9, 3).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

#[tokio::test]
async fn cancel_unknown_id_is_noop() {
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    // rid 999 was never started — CANCEL on it is a silent no-op (no frame at all).
    client.cancel(999).await;

    // The session must still answer PING (proving it did not misbehave/close on the unknown
    // CANCEL).
    client.ping(9, 11).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

#[tokio::test]
async fn goodbye_drains_inflight_then_closes() {
    let notify = Arc::new(Notify::new());
    let handler_notify = notify.clone();
    let handler: HandlerFn = Arc::new(
        move |_frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            let notify = handler_notify.clone();
            async move {
                notify.notified().await;
                responder.end_ok(Bytes::new());
            }
            .boxed()
        },
    );

    let server = TestServer::spawn_with_handler(BootEpoch(1), handler);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Request A: held in-flight by `notify`.
    client
        .send_request(1, service::SQL, SOME_METHOD, vec![])
        .await;
    client.goodbye().await;
    // A NEW request-bearing frame sent after GOODBYE: the reader loop already broke out of its
    // accept-new loop on GOODBYE (before this frame is ever read), so it is never dispatched —
    // no handler starts for id 2, and no terminal for id 2 will ever arrive.
    client
        .send_request(2, service::SQL, SOME_METHOD, vec![])
        .await;

    // Release A: its terminal must still arrive (draining lets outstanding requests finish).
    notify.notify_one();
    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 1);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Ok(_) => {}
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }

    // Then, and only then, the connection closes — no terminal for id 2 ever showed up; the
    // very next thing the client observes is EOF.
    client.recv_eof().await;
}

// `flow::Credit` debit/replenish is covered directly by `flow.rs`'s own unit tests, and
// `WINDOW_UPDATE` routing at the `Registry` layer (a known in-flight id's credit is updated; an
// unknown id is silently a no-op) is covered directly by `registry.rs`'s own unit tests -- both
// exercised at the same layer a duplicate integration test here would only restate. Live-session
// coverage of an actual credit DEBIT (i.e. `WINDOW_UPDATE` replenishing a window some in-flight
// consumer is drawing down through a real `Session`) is deferred to S5, when a DATA-frame stream
// producer first exists to consume credit; today nothing in the live session reads it back.

#[tokio::test]
async fn window_update_through_live_session_survives_unknown_target() {
    // End-to-end through a real `Session`: an unknown-target `WINDOW_UPDATE` must not crash or
    // hang the reader loop (it produces no reply frame of its own either way), and the session
    // must remain fully responsive afterward.
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client.window_update(123, 4, 4096).await;

    client.ping(9, 5).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

// ---------------------------------------------------------------------------------------------
// S3 Task 6: pure reader classification (session::classify) + flag validation + the dispatch
// table (session::dispatch::route) wired into a live Session — the session-fatal-vs-per-request
// split from the plan's Global Constraints, end to end.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn unknown_service_method_is_per_request_unsupported() {
    // ADMIN has no route at all in this build (no admin handlers exist yet, and it is never a
    // request-bearing service) — `dispatch::route` sends it straight to `Route::Unsupported`,
    // which produces a per-request `Unsupported` error `END` directly, without ever touching the
    // registry (nothing was spawned for it).
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send_request(77, service::ADMIN, SOME_METHOD, vec![])
        .await;

    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 77);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // The session survived: PING still gets a PONG.
    client.ping(9, 41).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

#[tokio::test]
async fn reserved_flag_is_session_fatal() {
    // A RESERVED-but-known bit (OOB_FD) actually set is session-fatal: `classify` maps it to
    // `errc::UNSUPPORTED` (M0 recognizes the bit but implements neither OOB_FD nor COMPRESSED —
    // see `ferro_proto::flags::validate`'s own doc comment for that split), one rid=0 frame, then
    // close.
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send(OutFrame {
            header: Header {
                flags: flags::OOB_FD,
                service: service::SQL,
                method: SOME_METHOD,
                request_id: 55,
                payload_len: 0,
            },
            payload: Bytes::new(),
        })
        .await;

    let fatal = client.recv().await;
    assert_eq!(fatal.header.request_id, 0);
    assert_eq!(fatal.header.flags, flags::END);
    match Outcome::decode(&fatal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // Session-fatal: the connection closes right after, nothing else is ever sent.
    client.recv_eof().await;
}

#[tokio::test]
async fn unknown_flag_bit_is_per_request() {
    // A non-reserved, non-`KNOWN` bit (0x8000) is a per-request skip, not session-fatal:
    // `payload_len` was already known from the header, so the one offending frame is cleanly
    // skippable and the session survives.
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send(OutFrame {
            header: Header {
                flags: 0x8000,
                service: service::SQL,
                method: SOME_METHOD,
                request_id: 66,
                payload_len: 0,
            },
            payload: Bytes::new(),
        })
        .await;

    let diagnostic = client.recv().await;
    assert_eq!(diagnostic.header.request_id, 66);
    assert_eq!(diagnostic.header.flags, flags::END);
    match Outcome::decode(&diagnostic.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // Session survives: PING still gets a PONG.
    client.ping(9, 13).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

#[tokio::test]
async fn oversize_payload_len_is_fatal() {
    // Craft a bare 16-byte header claiming a `payload_len` beyond `MAX_FRAME_PAYLOAD`, with NO
    // body bytes at all, and write it straight to the socket (bypassing `FrameCodec`'s encoder,
    // which would refuse to build such a frame). `Header::decode`'s S1 guard must reject this
    // from the header alone — before ever trying to read/allocate a payload of that declared
    // size — which is exactly why this test can complete without ever sending a body: if the
    // codec instead waited for `payload_len` more bytes, `recv()` would time out rather than
    // observe a fatal frame immediately.
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    let header = Header {
        flags: 0,
        service: service::SQL,
        method: SOME_METHOD,
        request_id: 99,
        payload_len: MAX_FRAME_PAYLOAD + 1,
    };
    client.send_raw_bytes(&header.encode()).await;

    let fatal = client.recv().await;
    assert_eq!(fatal.header.request_id, 0);
    assert_eq!(fatal.header.flags, flags::END);
    match Outcome::decode(&fatal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    client.recv_eof().await;
}

#[tokio::test]
async fn sql_service_returns_unsupported_stub() {
    // A `service=SQL` request frame goes through `Route::Request` (the registry/handler/
    // supervisor mechanism) exactly like any other request-bearing frame; with the default
    // handler in place (no real SQL handler until S4/S5), it declares `Unsupported` — no panic,
    // exactly one terminal.
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send_request(88, service::SQL, SOME_METHOD, vec![])
        .await;

    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 88);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // Session survives (no panic): PING still gets a PONG.
    client.ping(9, 3).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

// ---------------------------------------------------------------------------------------------
// S3 fix pass, m3: request_id 0 is reserved for session-context terminals (session-fatal /
// GOODBYE / no-request-context) -- a request-bearing frame must never be allowed to claim it.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn request_bearing_frame_with_rid_zero_is_rejected_without_touching_registry() {
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client
        .send_request(0, service::SQL, SOME_METHOD, vec![])
        .await;

    let diagnostic = client.recv().await;
    assert_eq!(diagnostic.header.request_id, 0);
    assert_eq!(diagnostic.header.flags, flags::END);
    match Outcome::decode(&diagnostic.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // The registry was never touched for this frame (nothing was spawned) -- the session is
    // completely unaffected and keeps answering normally.
    client.ping(9, 4).await;
    let pong_frame = client.recv().await;
    assert_eq!(pong_frame.header.method, method_core::PONG);
    assert_eq!(pong_frame.header.request_id, 9);
}

// ---------------------------------------------------------------------------------------------
// S3 fix pass, MAJOR 3 + m1: client disconnect mid-request must not orphan the per-request
// handler/supervisor tasks -- `run_with_handler` owns them in a per-session `JoinSet` and drains
// it (bounded by `config.drain_deadline`) before returning, so the session task itself always
// finishes rather than leaving detached work unaccounted for.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn client_disconnect_midrequest_cleans_up() {
    let notify = Arc::new(Notify::new());
    let handler_notify = notify.clone();
    let handler: HandlerFn = Arc::new(
        move |_frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            let notify = handler_notify.clone();
            async move {
                notify.notified().await;
                responder.end_ok(Bytes::new());
            }
            .boxed()
        },
    );

    let (socket_path, mut session) = common::spawn_one_session(BootEpoch(1), handler);
    let mut client = common::connect(&socket_path).await;
    client.hello(1).await;
    client
        .send_request(42, service::SQL, SOME_METHOD, vec![])
        .await;

    // The handler is genuinely still blocked on `notify` -- the session task must not have
    // finished yet (proves this isn't a vacuously-fast test).
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut session)
            .await
            .is_err(),
        "session must still be running while its one handler is blocked"
    );

    // Simulate an abrupt client disconnect MID-request: drop the client's framed UnixStream
    // entirely (no GOODBYE, no clean shutdown) while request 42 is still in flight.
    drop(client);

    // Disconnecting alone must not abandon the in-flight request -- the session keeps waiting for
    // its handler exactly as before (this is what distinguishes "drains" from "abandons").
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut session)
            .await
            .is_err(),
        "session must still be waiting on its handler right after the client disconnects"
    );

    // Release the handler: it completes normally, its supervisor sends the (now unreadable, since
    // the peer is gone) terminal and removes the registry entry, the per-session `JoinSet` drains,
    // and the session task returns -- no hang, no orphaned task.
    notify.notify_one();
    tokio::time::timeout(Duration::from_secs(2), session)
        .await
        .expect(
            "the session task must return once its handler completes, even after the client \
             already disconnected -- no leaked/orphaned per-request task",
        )
        .expect("session task must not panic");
}

#[tokio::test]
async fn disconnect_with_stuck_handler_hard_closes() {
    // A handler that never completes and never observes cancellation -- the ONLY way its session
    // winds down is the bounded drain-then-abort path (m1), racing `config.drain_deadline`, and/or
    // `serve`'s own SIGTERM-triggered hard-close (`drain_sessions`) one level up.
    let handler: HandlerFn = Arc::new(
        |_frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            async move {
                let _responder = responder; // held, never declares, never observes cancellation
                futures::future::pending::<()>().await;
            }
            .boxed()
        },
    );

    let drain = Drain::new();
    let config = Config {
        drain_deadline: Duration::from_millis(100),
        ..Config::default()
    };
    let (socket_path, served) =
        common::spawn_serve_with_config(config, BootEpoch(1), drain.clone(), handler);

    let mut client = common::connect(&socket_path).await;
    client.hello(1).await;
    client
        .send_request(1, service::SQL, SOME_METHOD, vec![])
        .await;

    // Abrupt mid-request disconnect -- no GOODBYE.
    drop(client);

    drain.trigger();

    // Even with a stuck handler AND a client that already vanished mid-request, `serve` must
    // still return within its (short) `drain_deadline` rather than hang forever.
    tokio::time::timeout(Duration::from_secs(2), served)
        .await
        .expect("serve must return even after a mid-request disconnect with a stuck handler")
        .expect("serve's task must not panic");
}
