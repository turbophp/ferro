//! In-process integration harness for `ferrod` session tests: binds a REAL
//! `tokio::net::UnixListener` on a fresh temp path, spawns `Session::run` on every accepted
//! connection (all sharing one `BootEpoch`, mirroring a single running daemon instance per SPEC
//! §19.1), and connects a client `UnixStream` against it.
//!
//! `TestClient::recv` wraps every read in a real-time `tokio::time::timeout` so a missing,
//! mis-ordered, or deadlocked frame fails the test fast with a clear panic instead of hanging
//! CI indefinitely.

use ferro_proto::consts::{TYPE_REGISTRY_HASH, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{Hello, HelloAck, Ping};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::session::Session;
use ferrod::session::codec::{FrameCodec, InFrame, OutFrame};
use futures::{SinkExt, StreamExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
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
}
