//! In-process integration harness for `ferrod` session tests: binds a REAL
//! `tokio::net::UnixListener` on a fresh temp path, spawns `Session::run` on every accepted
//! connection (all sharing one `BootEpoch`, mirroring a single running daemon instance per SPEC
//! §19.1), and connects a client `UnixStream` against it.
//!
//! `TestClient::recv` wraps every read in a real-time `tokio::time::timeout` so a missing,
//! mis-ordered, or deadlocked frame fails the test fast with a clear panic instead of hanging
//! CI indefinitely.

use ferro_proto::consts::{TYPE_REGISTRY_HASH, flags, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{Goodbye, Hello, HelloAck, Ping, WindowUpdate};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::session::codec::{FrameCodec, InFrame, OutFrame};
use ferrod::session::{HandlerFn, Session};
use futures::{SinkExt, StreamExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio_util::codec::Framed;

/// How long `recv`/`recv_eof` wait before panicking. A real deadline (not a sleep loop) so a
/// hung write path or a missing frame turns into a fast, clear test failure.
const RECV_TIMEOUT: Duration = Duration::from_secs(2);

static SOCK_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_socket_path() -> PathBuf {
    let n = SOCK_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!(
        "ferrod-handshake-test-{}-{nanos}-{n}.sock",
        std::process::id()
    ));
    p
}

/// A running test instance of the `ferrod` session server: a real bound `UnixListener` with an
/// accept loop spawning `Session::run` per connection.
pub struct TestServer {
    socket_path: PathBuf,
}

impl TestServer {
    /// Bind a real `UnixListener` at a fresh temp path and spawn `Session::run` on every accepted
    /// connection, all sharing `epoch` — exactly like one running daemon instance draws its
    /// `boot_epoch` once and hands the same value to every connection it serves.
    pub fn spawn(epoch: BootEpoch) -> Self {
        let socket_path = tmp_socket_path();
        let config = Config {
            socket_path: socket_path.clone(),
            ..Config::default()
        };
        let listener = ferrod::listener::bind_uds(&config).expect("bind_uds in test harness");

        tokio::spawn(async move {
            loop {
                let (stream, _addr) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                tokio::spawn(Session::run(stream, config.clone(), epoch));
            }
        });

        TestServer { socket_path }
    }

    /// Like `spawn`, but drives every accepted connection through
    /// `Session::run_with_handler(.., handler)` instead of the default dispatch stub — the seam
    /// S3 Task 4 tests use to script handler behaviour (panic, block on a `Notify`, declare
    /// immediately) without a real SQL/TX backend.
    pub fn spawn_with_handler(epoch: BootEpoch, handler: HandlerFn) -> Self {
        Self::spawn_with_handler_and_max_inflight(epoch, Config::default().max_inflight, handler)
    }

    /// Like `spawn_with_handler`, but also overrides `max_inflight` — for tests that need to
    /// exercise the registry's capacity limit directly.
    pub fn spawn_with_handler_and_max_inflight(
        epoch: BootEpoch,
        max_inflight: usize,
        handler: HandlerFn,
    ) -> Self {
        let socket_path = tmp_socket_path();
        let config = Config {
            socket_path: socket_path.clone(),
            max_inflight,
            ..Config::default()
        };
        let listener = ferrod::listener::bind_uds(&config).expect("bind_uds in test harness");

        tokio::spawn(async move {
            loop {
                let (stream, _addr) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                tokio::spawn(Session::run_with_handler(
                    stream,
                    config.clone(),
                    epoch,
                    handler.clone(),
                ));
            }
        });

        TestServer { socket_path }
    }

    /// Connect a new client to this server.
    pub async fn connect(&self) -> TestClient {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .expect("client connect to test server");
        TestClient {
            framed: Framed::new(stream, FrameCodec),
        }
    }
}

/// The result of a successful HELLO/HELLO_ACK exchange: the decoded `HelloAck` plus the raw
/// frame header's `request_id` (the wire-level echo, distinct from anything inside the payload).
pub struct HelloResult {
    pub request_id: u32,
    pub ack: HelloAck,
}

pub struct TestClient {
    framed: Framed<UnixStream, FrameCodec>,
}

impl TestClient {
    /// Encode and send `frame` to the server.
    pub async fn send(&mut self, frame: OutFrame) {
        self.framed.send(frame).await.expect("client send");
    }

    /// Write raw bytes directly to the underlying socket, bypassing `FrameCodec`'s encoder
    /// entirely (which asserts `header.payload_len == payload.len()` and would reject/assert on
    /// a deliberately malformed frame). For tests that need to put bytes on the wire the encoder
    /// itself would never let through — e.g. a header claiming an oversize `payload_len` with no
    /// body at all, to exercise the S1 zero-allocation header guard end to end.
    pub async fn send_raw_bytes(&mut self, bytes: &[u8]) {
        self.framed
            .get_mut()
            .write_all(bytes)
            .await
            .expect("client send_raw_bytes");
    }

    /// Receive the next frame, panicking if none arrives within `RECV_TIMEOUT`.
    pub async fn recv(&mut self) -> InFrame {
        match tokio::time::timeout(RECV_TIMEOUT, self.framed.next()).await {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(e))) => panic!("client recv: codec error: {e}"),
            Ok(None) => panic!("client recv: connection closed with no frame"),
            Err(_) => panic!("client recv: timed out after {RECV_TIMEOUT:?} waiting for a frame"),
        }
    }

    /// Assert the connection closes (next read is EOF) within `RECV_TIMEOUT`.
    pub async fn recv_eof(&mut self) {
        match tokio::time::timeout(RECV_TIMEOUT, self.framed.next()).await {
            Ok(None) => {}
            Ok(Some(Ok(frame))) => panic!("expected EOF, got a frame: {:?}", frame.header),
            Ok(Some(Err(e))) => panic!("expected EOF, got a codec error: {e}"),
            Err(_) => panic!("timed out after {RECV_TIMEOUT:?} waiting for EOF"),
        }
    }

    /// Send a `HELLO` with an explicit (possibly bogus) `type_registry_hash`, without asserting
    /// anything about the reply — for tests that expect the handshake to fail.
    pub async fn send_hello(&mut self, request_id: u32, type_registry_hash: &str) {
        let hello = Hello {
            client_version: 1,
            type_registry_hash: type_registry_hash.to_string(),
            manifest_hash: None,
            pid: std::process::id(),
            features: 0,
        };
        let payload = hello.encode();
        self.send(OutFrame {
            header: Header {
                flags: 0,
                service: service::CORE,
                method: method_core::HELLO,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        })
        .await;
    }

    /// Send a well-formed `HELLO` (the correct `TYPE_REGISTRY_HASH`) and assert the `HELLO_ACK`
    /// round-trip, returning the decoded ack and the frame's echoed `request_id`.
    pub async fn hello(&mut self, request_id: u32) -> HelloResult {
        self.send_hello(request_id, TYPE_REGISTRY_HASH).await;
        let frame = self.recv().await;
        assert_eq!(frame.header.service, service::CORE);
        assert_eq!(frame.header.method, method_core::HELLO_ACK);
        let ack = HelloAck::decode(&frame.payload).expect("decode HELLO_ACK");
        HelloResult {
            request_id: frame.header.request_id,
            ack,
        }
    }

    /// Send a request-bearing frame with an arbitrary `service`/`method`/payload — for driving
    /// the registry + spawned-handler + supervisor mechanism directly. `method` has no
    /// registry-defined meaning yet for SQL/TX/STREAM (real method ids land with the dispatch
    /// table in Task 6); tests just need a stable placeholder to route on `service`.
    pub async fn send_request(
        &mut self,
        request_id: u32,
        service: u16,
        method: u16,
        payload: Vec<u8>,
    ) {
        self.send(OutFrame {
            header: Header {
                flags: 0,
                service,
                method,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        })
        .await;
    }

    /// Send a `PING` with the given `request_id`/`token`.
    pub async fn ping(&mut self, request_id: u32, token: u64) {
        let payload = Ping { token }.encode();
        self.send(OutFrame {
            header: Header {
                flags: 0,
                service: service::CORE,
                method: method_core::PING,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        })
        .await;
    }

    /// Send a flag-based `CANCEL` targeting `request_id` (empty payload) — advisory and
    /// idempotent (SPEC §5.2). Routed purely by the `CANCEL` flag bit + `request_id`; the frame's
    /// own `service`/`method` are otherwise unused by the routing, so `CORE`/`0` is as good a
    /// placeholder as any.
    pub async fn cancel(&mut self, request_id: u32) {
        self.send(OutFrame {
            header: Header {
                flags: flags::CANCEL,
                service: service::CORE,
                method: 0,
                request_id,
                payload_len: 0,
            },
            payload: Vec::new().into(),
        })
        .await;
    }

    /// Send `core/GOODBYE` (the graceful-drain announcement), `request_id = 0` per convention.
    pub async fn goodbye(&mut self) {
        let payload = Goodbye {}.encode();
        self.send(OutFrame {
            header: Header {
                flags: 0,
                service: service::CORE,
                method: method_core::GOODBYE,
                request_id: 0,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        })
        .await;
    }

    /// Send `core/WINDOW_UPDATE {frames, bytes}` targeting `request_id`.
    pub async fn window_update(&mut self, request_id: u32, frames: u32, bytes: u32) {
        let payload = WindowUpdate { frames, bytes }.encode();
        self.send(OutFrame {
            header: Header {
                flags: 0,
                service: service::CORE,
                method: method_core::WINDOW_UPDATE,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        })
        .await;
    }
}
