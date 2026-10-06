//! P17 (SPEC §23.19): "During `ferrod`'s drain, an existing session keeps reading frames, so a new
//! HTTP `REQUEST` can be refused with `draining` rather than lost. Measured against
//! `serve.rs`/`session`."
//!
//! Measured through the REAL `ferrod::serve::serve` accept loop and `Session`, triggered by the
//! injectable `ferrod::shutdown::Drain` the daemon's SIGTERM watcher drives. `HTTP = 6` is not in
//! `/proto` until slice F2, so the "HTTP service" here is a handler on a route that exists today
//! (`SQL`) which does what §23.6.1 asks of the HTTP service: refuse a request that arrives during
//! the drain. A raw frame on service 6 is ALSO sent, to show the session reads and answers it today
//! (`Unsupported`, one `END`).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ferro_proto::consts::{TYPE_REGISTRY_HASH, flags, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{Hello, Outcome};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::session::codec::{FrameCodec, InFrame, OutFrame};
use ferrod::session::responder::Responder;
use ferrod::session::{HandlerFactory, HandlerFn};
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

/// SPEC §23.5 allocates `HTTP = 6`; it is a comment in `/proto` until slice F2, so there is no
/// generated constant to use yet (and hand-writing one outside a test would be a charter-rule-2
/// defect). Today `dispatch::route` answers it `Unsupported`.
const HTTP_SERVICE_ID: u16 = 6;
const SOME_METHOD: u16 = 1;

fn socket_path() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let mut p = std::env::temp_dir();
    p.push(format!(
        "ferro-http-spike-p17-{}-{}.sock",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    p
}

/// Spawn the REAL `serve` with `handler`, as `ferrod`'s `main` does (pool-less config).
fn spawn_serve(
    drain_deadline: Duration,
    drain: Drain,
    handler: HandlerFn,
) -> (PathBuf, JoinHandle<()>) {
    let path = socket_path();
    let config = Config {
        socket_path: path.clone(),
        drain_deadline,
        ..Config::default()
    };
    let listener = ferrod::listener::bind_uds(&config).unwrap();
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let pool_registry = PoolRegistry::build(&config);
    let factory: HandlerFactory = Arc::new(move |_sid, _info| handler.clone());
    let served = tokio::spawn(ferrod::serve::serve(
        listener,
        config,
        BootEpoch(7),
        drain,
        pool_registry,
        tx_registry,
        factory,
    ));
    (path, served)
}

struct Client(Framed<UnixStream, FrameCodec>);

impl Client {
    async fn connect(path: &PathBuf) -> Self {
        let mut c = Client(Framed::new(
            UnixStream::connect(path).await.unwrap(),
            FrameCodec,
        ));
        let payload = Hello {
            client_version: 1,
            type_registry_hash: TYPE_REGISTRY_HASH.to_string(),
            manifest_hash: None,
            pid: std::process::id(),
            features: 0,
        }
        .encode();
        c.send(service::CORE, method_core::HELLO, 1, payload).await;
        let ack = c.recv().await.expect("HELLO_ACK");
        assert_eq!(ack.header.method, method_core::HELLO_ACK);
        c
    }
    async fn send(&mut self, svc: u16, method: u16, rid: u32, payload: Vec<u8>) {
        self.0
            .send(OutFrame {
                header: Header {
                    flags: 0,
                    service: svc,
                    method,
                    request_id: rid,
                    payload_len: payload.len() as u32,
                },
                payload: payload.into(),
            })
            .await
            .unwrap();
    }
    /// The next frame, or `None` on EOF/timeout.
    async fn recv(&mut self) -> Option<InFrame> {
        match tokio::time::timeout(Duration::from_secs(3), self.0.next()).await {
            Ok(Some(Ok(f))) => Some(f),
            _ => None,
        }
    }
}

fn ok_body(f: &InFrame) -> Vec<u8> {
    assert_eq!(f.header.flags, flags::END, "a terminal");
    match Outcome::decode(&f.payload).unwrap() {
        Outcome::Ok(b) => b.to_vec(),
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// The "HTTP service" model: refuses with `draining` once the drain has started (the `Drain` handle
/// reaches it by closure capture, from wherever `main` builds the handler factory), serves
/// otherwise, and parks request 1 until released.
fn handler(drain: Drain, park: Arc<Notify>, invoked: Arc<AtomicUsize>) -> HandlerFn {
    Arc::new(
        move |frame: InFrame, responder: Responder, _cancel: CancellationToken| {
            let drain = drain.clone();
            let park = park.clone();
            let invoked = invoked.clone();
            async move {
                invoked.fetch_add(1, Ordering::SeqCst);
                if frame.header.request_id == 1 {
                    park.notified().await;
                    responder.end_ok(Bytes::from_static(b"served"));
                } else if drain.is_draining() {
                    responder.end_ok(Bytes::from_static(b"draining"));
                } else {
                    responder.end_ok(Bytes::from_static(b"served"));
                }
            }
            .boxed()
        },
    )
}

/// **P17.** After the drain starts, BOTH a session with a request in flight AND an idle session
/// keep reading frames: a new request on either is read and answered with exactly one `END`
/// (`draining`), and a frame on service 6 is answered too. The parked in-flight request still
/// completes. Nothing is lost while `serve` waits out `drain_deadline`.
#[tokio::test]
async fn p17_existing_sessions_keep_reading_frames_during_the_drain() {
    let drain = Drain::new();
    let park = Arc::new(Notify::new());
    let invoked = Arc::new(AtomicUsize::new(0));
    let (path, served) = spawn_serve(
        Duration::from_secs(3),
        drain.clone(),
        handler(drain.clone(), park.clone(), invoked.clone()),
    );

    let mut busy = Client::connect(&path).await;
    let mut idle = Client::connect(&path).await;
    busy.send(service::SQL, SOME_METHOD, 1, vec![]).await; // parked in flight
    tokio::time::sleep(Duration::from_millis(50)).await;

    drain.trigger();
    tokio::time::sleep(Duration::from_millis(100)).await; // the accept loop has broken by now

    // New requests on both existing sessions, AFTER the drain began.
    busy.send(service::SQL, SOME_METHOD, 2, vec![]).await;
    idle.send(service::SQL, SOME_METHOD, 10, vec![]).await;
    let b = busy.recv().await.expect("busy session answered rid 2");
    assert_eq!(b.header.request_id, 2);
    assert_eq!(ok_body(&b), b"draining");
    let i = idle.recv().await.expect("idle session answered rid 10");
    assert_eq!(i.header.request_id, 10);
    assert_eq!(ok_body(&i), b"draining");

    // A frame on the HTTP service id (6/REQUEST, routed to the handler since M6-F2) is read during
    // the drain, reaches the handler, and is refused with `draining` — exactly one END. (Before F2
    // the session answered service 6 with `Unsupported`; either way the frame is read, which is
    // P17's claim.)
    idle.send(HTTP_SERVICE_ID, SOME_METHOD, 11, vec![]).await;
    let u = idle.recv().await.expect("service 6 answered");
    assert_eq!(u.header.request_id, 11);
    assert_eq!(ok_body(&u), b"draining");

    // The in-flight request still completes.
    park.notify_one();
    let r1 = busy.recv().await.expect("rid 1 completes");
    assert_eq!(r1.header.request_id, 1);
    assert_eq!(ok_body(&r1), b"served");
    // Three SQL requests plus the service-6 REQUEST, which reaches the handler since M6-F2.
    assert_eq!(invoked.load(Ordering::SeqCst), 4);

    drop(busy);
    drop(idle);
    tokio::time::timeout(Duration::from_secs(2), served)
        .await
        .expect("serve returns once its sessions end")
        .unwrap();
}

/// **P17, the negative control: the window is bounded by `drain_deadline`.** Once `serve`'s wait
/// ends it ABORTS every remaining session task, so a request sent after that is LOST — no `END`,
/// the handler never runs, the client sees EOF. That is what the premise's "rather than lost" is
/// measured against, and why §23.6.1's chassis change 2 (`serve` waits `FERRO_HTTP_DRAIN_MS +
/// drain_deadline` while HTTP exchanges are in flight) is needed: today an idle session is
/// readable only until the SQL `drain_deadline` (5 s by default).
#[tokio::test]
async fn p17_control_after_the_drain_deadline_a_request_is_lost() {
    let drain = Drain::new();
    let invoked = Arc::new(AtomicUsize::new(0));
    let (path, served) = spawn_serve(
        Duration::from_millis(200),
        drain.clone(),
        handler(drain.clone(), Arc::new(Notify::new()), invoked.clone()),
    );
    let mut idle = Client::connect(&path).await;

    drain.trigger();
    tokio::time::timeout(Duration::from_secs(2), served)
        .await
        .expect("serve hard-closes at its drain_deadline")
        .unwrap();

    // The session task is gone. Whether the write itself succeeds is a socket detail; no answer
    // can come back.
    let _ = idle
        .0
        .send(OutFrame {
            header: Header {
                flags: 0,
                service: service::SQL,
                method: SOME_METHOD,
                request_id: 20,
                payload_len: 0,
            },
            payload: Bytes::new(),
        })
        .await;
    assert!(idle.recv().await.is_none(), "no END after the hard close");
    assert_eq!(invoked.load(Ordering::SeqCst), 0, "the handler never ran");
}
