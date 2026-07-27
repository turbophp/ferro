//! S3 Task 7: SIGTERM drain + `main` wiring, exercised through the REAL `serve` accept loop (not
//! `Session::run` directly) with an injected `shutdown::Drain` — no real signal needed.
//!
//! Every test is barrier-gated with `tokio::sync::Notify` where concurrency matters — no sleeps.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use ferro_proto::consts::{TYPE_REGISTRY_HASH, flags, service};
use ferro_proto::messages::Outcome;
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::session::HandlerFn;
use ferrod::session::codec::InFrame;
use ferrod::session::responder::Responder;
use ferrod::shutdown::Drain;

/// A stable placeholder method id for request-bearing test frames (mirrors `tests/session_rules.rs`
/// — SQL/TX/STREAM don't have registry-defined method ids yet).
const SOME_METHOD: u16 = 1;

/// How long to wait, per `recv_or_none`, when asserting "nothing arrives" for a connection the
/// accept loop never got around to spawning a session for. Short, because the point is proving
/// the accept loop already stopped -- not timing anything precisely.
const REFUSED_WAIT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn drain_refuses_new_but_finishes_inflight() {
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

    let drain = Drain::new();
    let (socket_path, served) = common::spawn_serve(BootEpoch(1), drain.clone(), handler);

    // Client A: HELLO, then a request the handler holds in-flight on `notify`.
    let mut client_a = common::connect(&socket_path).await;
    client_a.hello(1).await;
    client_a
        .send_request(100, service::SQL, SOME_METHOD, vec![])
        .await;

    // Trigger the drain -- the accept loop must stop taking new connections immediately.
    drain.trigger();

    // A NEW connection: even if the kernel-level `connect()` itself succeeds (queued in the
    // listen backlog), `serve`'s accept loop already broke out of its select on `drain.wait()`,
    // so nothing is ever spawned for it -- no HELLO_ACK, no reply of any kind, ever.
    let mut new_client = common::connect(&socket_path).await;
    new_client
        .try_send(common::TestClient::hello_out_frame(1, TYPE_REGISTRY_HASH))
        .await
        .ok(); // a send failure here is ALSO consistent with "refused"; either way, no ACK below.
    assert!(
        new_client.recv_or_none(REFUSED_WAIT).await.is_none(),
        "a new connection after drain must never get a HELLO_ACK (accept loop stopped)"
    );

    // Release A: its in-flight request must still complete with its one terminal END --
    // draining lets outstanding requests finish, it does not abort them.
    notify.notify_one();
    let terminal = client_a.recv().await;
    assert_eq!(terminal.header.request_id, 100);
    assert_eq!(terminal.header.flags, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Ok(body) => assert_eq!(body, b"done"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }

    // A's client, having gotten its response, now ends its own connection (mirrors a real
    // client's GOODBYE) -- this is what lets `serve`'s per-connection session task return on its
    // own promptly, rather than needing to wait out the (default, multi-second) `drain_deadline`.
    client_a.goodbye().await;
    client_a.recv_eof().await;

    // `serve` itself must return promptly now that its only session has ended cleanly.
    tokio::time::timeout(Duration::from_secs(2), served)
        .await
        .expect("serve must return once its sessions have all finished")
        .expect("serve's task must not panic");
}

#[tokio::test]
async fn drain_deadline_hard_closes() {
    // A handler that never completes and never declares a terminal -- the ONLY way its session
    // ever winds down is `serve`'s hard-close-past-the-deadline path.
    let handler: HandlerFn = Arc::new(
        |_frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            async move {
                let _responder = responder; // held, never declares
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

    drain.trigger();

    // The in-flight request never finishes, so `serve` must give up at its `drain_deadline`
    // (~100ms) rather than hang forever -- give it generous slack over that to stay non-flaky.
    tokio::time::timeout(Duration::from_secs(2), served)
        .await
        .expect("serve must return within its drain_deadline, not hang forever")
        .expect("serve's task must not panic");
}
