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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use tokio::sync::{Notify, mpsc};

use common::TestServer;
use ferro_proto::consts::{errc, flags, method_core, service};
use ferro_proto::messages::{Outcome, Pong};
use ferrod::epoch::BootEpoch;
use ferrod::session::HandlerFn;
use ferrod::session::codec::{InFrame, OutFrame};
use ferrod::session::registry::{InsertErr, Registry};
use ferrod::session::responder::Responder;
use ferrod::session::supervisor;

/// A stable placeholder method id for request-bearing test frames. SQL/TX/STREAM don't have
/// registry-defined method ids yet (that lands with the dispatch table in Task 6); the mechanism
/// under test here only routes on `service`.
const SOME_METHOD: u16 = 1;

// ---------------------------------------------------------------------------------------------
// Unit-level: registry
// ---------------------------------------------------------------------------------------------

#[test]
fn registry_reuse_and_full() {
    let max_inflight = 2;
    let registry = Registry::new(max_inflight);

    assert!(registry.insert(1).is_ok());
    assert_eq!(registry.insert(1), Err(InsertErr::Reused));

    assert!(registry.insert(2).is_ok());
    assert_eq!(registry.len(), 2);
    assert_eq!(registry.insert(3), Err(InsertErr::Full));
}

// ---------------------------------------------------------------------------------------------
// Unit-level: supervisor (construct the pieces directly, drain a control receiver)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn supervisor_sends_declared_ok_exactly_once() {
    let (control_tx, mut control_rx) = mpsc::channel::<OutFrame>(8);
    let registry = Arc::new(Registry::new(4));
    registry.insert(7).unwrap();

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
    registry.insert(11).unwrap();

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
async fn supervisor_synthesizes_on_no_terminal() {
    let (control_tx, mut control_rx) = mpsc::channel::<OutFrame>(8);
    let registry = Arc::new(Registry::new(4));
    registry.insert(21).unwrap();

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
    let handler: HandlerFn = Arc::new(|_frame: InFrame, _responder: Responder| {
        async move {
            panic!("intentional: handler panics without declaring (panic-isolation test)");
        }
        .boxed()
    });

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
    let handler: HandlerFn = Arc::new(move |_frame: InFrame, responder: Responder| {
        let notify = handler_notify.clone();
        async move {
            notify.notified().await;
            responder.end_ok(Bytes::from_static(b"done"));
        }
        .boxed()
    });

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
    let notify = Arc::new(Notify::new());
    let handler_notify = notify.clone();
    let handler: HandlerFn = Arc::new(move |_frame: InFrame, responder: Responder| {
        let notify = handler_notify.clone();
        async move {
            notify.notified().await;
            responder.end_ok(Bytes::new());
        }
        .boxed()
    });

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

    // Release both held requests so the connection winds down cleanly.
    notify.notify_one();
    notify.notify_one();
    let mut seen: HashSet<u32> = HashSet::new();
    for _ in 0..2 {
        let frame = client.recv().await;
        assert_eq!(frame.header.flags, flags::END);
        seen.insert(frame.header.request_id);
    }
    assert_eq!(seen, HashSet::from([1, 2]));
}
