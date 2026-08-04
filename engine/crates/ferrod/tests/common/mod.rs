//! In-process integration harness for `ferrod` session tests: binds a REAL
//! `tokio::net::UnixListener` on a fresh temp path, spawns `Session::run` on every accepted
//! connection (all sharing one `BootEpoch`, mirroring a single running daemon instance per SPEC
//! §19.1), and connects a client `UnixStream` against it.
//!
//! `TestClient::recv` wraps every read in a real-time `tokio::time::timeout` so a missing,
//! mis-ordered, or deadlocked frame fails the test fast with a clear panic instead of hanging
//! CI indefinitely.
//!
//! This module is compiled independently into EVERY `tests/*.rs` binary that `mod common;`s it, and
//! each binary uses only the subset of helpers it needs — so `dead_code` (per-binary) is expected
//! and allowed here rather than forcing every helper to be used by every binary.
#![allow(dead_code)]

use ferro_proto::consts::{TYPE_REGISTRY_HASH, flags, method_core, method_sql, service};
use ferro_proto::header::Header;
use ferro_proto::messages::sql::{ExecOk, ExecRequest};
use ferro_proto::messages::{ErrorPayload, Goodbye, Hello, HelloAck, Outcome, Ping, WindowUpdate};
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::serve::serve;
use ferrod::services::sql;
use ferrod::session::codec::{FrameCodec, FrameError, InFrame, OutFrame};
use ferrod::session::{HandlerFactory, HandlerFn, Session};
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;
use futures::{SinkExt, StreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
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

        // Wrap the plain `HandlerFn` as a session-agnostic factory + mint a throwaway `TxRegistry`
        // (S6 seam): these scripted-handler tests never open a transaction, so `abort_session` at
        // cleanup is a no-op and behaviour is identical to the pre-seam harness.
        let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
        let factory: HandlerFactory = Arc::new(move |_sid| handler.clone());

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
                    tx_registry.clone(),
                    factory.clone(),
                ));
            }
        });

        TestServer { socket_path }
    }

    /// Like `spawn_with_handler`, but drives every accepted connection through a full
    /// `HandlerFactory` + a shared `Arc<TxRegistry>` — the real S6 wiring the SQL/TX services use
    /// (a fresh per-connection `HandlerFn` per `SessionId`, all sharing one tx registry).
    /// `exec_server` uses this so its EXEC handler is built exactly as `main` builds it.
    pub fn spawn_with_factory(
        epoch: BootEpoch,
        tx_registry: Arc<TxRegistry>,
        factory: HandlerFactory,
    ) -> Self {
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
                tokio::spawn(Session::run_with_handler(
                    stream,
                    config.clone(),
                    epoch,
                    tx_registry.clone(),
                    factory.clone(),
                ));
            }
        });

        TestServer { socket_path }
    }

    /// Like `spawn_with_factory`, but threads a caller-supplied `Config` into EVERY session (its
    /// `socket_path` is always overwritten with a fresh temp path). `spawn_with_factory` hardcodes
    /// `Config::default()` for the session, which fixes the credit window at the default
    /// `credit_frames` — so a streaming-backpressure test (M1-S5 Task 7) that needs a SMALL window
    /// (`credit_frames: 2`) uses this variant instead, and `stream_server` builds on it.
    pub fn spawn_with_factory_and_config(
        epoch: BootEpoch,
        config: Config,
        tx_registry: Arc<TxRegistry>,
        factory: HandlerFactory,
    ) -> Self {
        let socket_path = tmp_socket_path();
        let config = Config {
            socket_path: socket_path.clone(),
            ..config
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
                    tx_registry.clone(),
                    factory.clone(),
                ));
            }
        });

        TestServer { socket_path }
    }

    /// Connect a new client to this server.
    pub async fn connect(&self) -> TestClient {
        connect(&self.socket_path).await
    }
}

/// Connect a new client directly to a bound socket path (for use with `spawn_serve*`, which —
/// unlike `TestServer` — has no `connect` method of its own since a `serve` call, not a
/// `TestServer`, owns the listener).
pub async fn connect(socket_path: &Path) -> TestClient {
    let stream = UnixStream::connect(socket_path)
        .await
        .expect("client connect to test server");
    TestClient {
        framed: Framed::new(stream, FrameCodec),
    }
}

/// Bind a fresh listener, accept exactly ONE connection, and drive it directly via
/// `Session::run_with_handler`, returning that session's OWN `JoinHandle<()>` — unlike
/// `TestServer` (whose internal accept loop spawns a session per connection with the `JoinHandle`
/// dropped, mirroring how the real daemon treats every connection independently), a test that
/// needs to observe the SESSION TASK ITSELF finishing (e.g. after the client disconnects
/// mid-request, leaving no more frames to read and thus no way to infer session health from
/// `TestClient::recv`) needs this handle directly. Uses `Config::default()`.
pub fn spawn_one_session(epoch: BootEpoch, handler: HandlerFn) -> (PathBuf, JoinHandle<()>) {
    spawn_one_session_with_config(Config::default(), epoch, handler)
}

/// Like `spawn_one_session`, but starting from a caller-supplied `Config` (e.g. to override
/// `handshake_timeout` or `drain_deadline`). The `socket_path` field of `config` is always
/// overwritten with a fresh temp path.
pub fn spawn_one_session_with_config(
    config: Config,
    epoch: BootEpoch,
    handler: HandlerFn,
) -> (PathBuf, JoinHandle<()>) {
    let socket_path = tmp_socket_path();
    let config = Config {
        socket_path: socket_path.clone(),
        ..config
    };
    let listener = ferrod::listener::bind_uds(&config).expect("bind_uds in test harness");
    // Wrap the plain `HandlerFn` as a session-agnostic factory + a throwaway `TxRegistry` (S6 seam).
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory: HandlerFactory = Arc::new(move |_sid| handler.clone());
    let handle = tokio::spawn(async move {
        let (stream, _addr) = listener
            .accept()
            .await
            .expect("accept the one test connection");
        Session::run_with_handler(stream, config, epoch, tx_registry, factory).await;
    });
    (socket_path, handle)
}

/// Spawn the REAL `serve` accept loop (peercred-gated + drain-aware, S3 Task 7) on a fresh bound
/// listener, via `tokio::spawn`, using `Config::default()` plus a fresh temp socket path. Returns
/// the socket path (pass to `connect` above) and `serve`'s own `JoinHandle` so a test can await it
/// — after triggering `drain` — and assert it returns within `config.drain_deadline`.
pub fn spawn_serve(
    epoch: BootEpoch,
    drain: Drain,
    handler: HandlerFn,
) -> (PathBuf, JoinHandle<()>) {
    spawn_serve_with_config(Config::default(), epoch, drain, handler)
}

/// Like `spawn_serve`, but starting from a caller-supplied `Config` (e.g. to override
/// `peer_allow_uids` for a peercred-deny test, or `drain_deadline` for a hard-close test). The
/// `socket_path` field of `config` is always overwritten with a fresh temp path.
pub fn spawn_serve_with_config(
    config: Config,
    epoch: BootEpoch,
    drain: Drain,
    handler: HandlerFn,
) -> (PathBuf, JoinHandle<()>) {
    let socket_path = tmp_socket_path();
    let config = Config {
        socket_path: socket_path.clone(),
        ..config
    };
    let listener = ferrod::listener::bind_uds(&config).expect("bind_uds in test harness");
    // Wrap the plain `HandlerFn` as a session-agnostic factory + a throwaway `TxRegistry` (S6 seam).
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory: HandlerFactory = Arc::new(move |_sid| handler.clone());
    let handle = tokio::spawn(serve(listener, config, epoch, drain, tx_registry, factory));
    (socket_path, handle)
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

    /// Like `send`, but returns the encode/write result instead of panicking on failure — for
    /// tests where the send itself might fail (e.g. the server already dropped its side of the
    /// socket right after a peercred deny; that's an EXPECTED outcome there, not a test bug).
    pub async fn try_send(&mut self, frame: OutFrame) -> Result<(), FrameError> {
        self.framed.send(frame).await
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

    /// Wait up to `dur` for a frame. Returns `Some(frame)` if a well-formed frame arrived in time;
    /// `None` on timeout, EOF, OR a codec error alike — for tests asserting "nothing ever arrives"
    /// (e.g. a connection `serve`'s accept loop never got around to spawning a session for), where
    /// the exact failure mode isn't the point, only that no valid reply ever showed up.
    pub async fn recv_or_none(&mut self, dur: Duration) -> Option<InFrame> {
        match tokio::time::timeout(dur, self.framed.next()).await {
            Ok(Some(Ok(frame))) => Some(frame),
            _ => None,
        }
    }

    /// Build a `HELLO` `OutFrame` with an explicit (possibly bogus) `type_registry_hash`, without
    /// sending it — shared by `send_hello` and tests that need `try_send`'s non-panicking send
    /// (e.g. a peercred-deny test, where the send itself may fail because the server already
    /// closed its side of the socket).
    pub fn hello_out_frame(request_id: u32, type_registry_hash: &str) -> OutFrame {
        let hello = Hello {
            client_version: 1,
            type_registry_hash: type_registry_hash.to_string(),
            manifest_hash: None,
            pid: std::process::id(),
            features: 0,
        };
        let payload = hello.encode();
        OutFrame {
            header: Header {
                flags: 0,
                service: service::CORE,
                method: method_core::HELLO,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        }
    }

    /// Send a `HELLO` with an explicit (possibly bogus) `type_registry_hash`, without asserting
    /// anything about the reply — for tests that expect the handshake to fail.
    pub async fn send_hello(&mut self, request_id: u32, type_registry_hash: &str) {
        self.send(Self::hello_out_frame(request_id, type_registry_hash))
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

// -------------------------------------------------------------------------------------------------
// Shared EXEC helpers (lifted from `sql_exec_it.rs` so `sql_e2e_scenarios.rs` and any later SQL
// integration binary consume ONE copy — each `tests/*.rs` is a separate crate, so a sibling file
// cannot import another binary's private fns). All PG-touching helpers key off `pg_url()`'s skip
// idiom so `cargo test --workspace` stays green offline.
// -------------------------------------------------------------------------------------------------

/// The DSN under test, or `None` (→ the caller returns early / skips) when `FERRO_TEST_PG_URL` is
/// unset — the same discipline as `ferro-backend-pg`'s `pg_query_it.rs`.
pub fn pg_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

/// The MySQL DSN under test (M1-S6), or `None` (→ skip) when `FERRO_TEST_MYSQL_URL` is unset —
/// mirrors [`pg_url`]. Points at the `testkit` `mysql` service (default `33060:3306`), e.g.
/// `mysql://ferro:ferro@127.0.0.1:33060/ferro`.
pub fn mysql_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MYSQL_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_MYSQL_URL unset");
            None
        }
    }
}

/// The MariaDB DSN under test (M1-S6), or `None` (→ skip) when `FERRO_TEST_MARIADB_URL` is unset —
/// mirrors [`pg_url`]. Points at the `testkit` `mariadb` service (default `33061:3306`), e.g.
/// `mysql://ferro:ferro@127.0.0.1:33061/ferro`. The second dialect the S2 assist lexer's tracker
/// verification runs against.
pub fn mariadb_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MARIADB_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_MARIADB_URL unset");
            None
        }
    }
}

/// A live `ferrod` session server whose EXEC handler owns a real `Pool<PgBackend>` named "default"
/// pointing at `url`. Uses `TestServer::spawn_with_factory` (no peercred gate) with the real
/// `sql::make_handler` + a shared `Arc<TxRegistry>` — built exactly as `main` builds it — so this
/// is a genuine client→ferrod→pool→PG round trip.
pub fn exec_server(url: String) -> TestServer {
    // Kind is inferred from the DSN scheme (M1-S6), so `exec_server(mysql_url())` builds a MySQL
    // pool and `exec_server(pg_url())` a Postgres one — the SAME helper drives both dialects.
    let kind = ferrod::config::infer_pool_kind(&url);
    let config = Config {
        pools: vec![PoolSpec {
            name: "default".to_string(),
            dsn: url,
            kind,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }],
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry,
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    TestServer::spawn_with_factory(BootEpoch(1), tx_registry, factory)
}

/// Like [`exec_server`], but with a caller-chosen SMALL per-request credit window
/// (`credit_frames`) so a large `fetch:stream` result MUST park on backpressure and resume only
/// when the client sends a `WINDOW_UPDATE` (M1-S5 Task 7's live gate). `credit_bytes` is left at the
/// default (== `MAX_FRAME_PAYLOAD`, 16 MiB — `Config::validate` rejects anything smaller), so the
/// backpressure comes purely from the FRAMES dimension. The SAME `config` seeds both the session's
/// credit window (via `spawn_with_factory_and_config`) and the EXEC handler's pool, so the small
/// window is actually in force end to end (`spawn_with_factory` would silently reset it to default).
pub fn stream_server(url: String, credit_frames: u32) -> TestServer {
    let kind = ferrod::config::infer_pool_kind(&url);
    let config = Config {
        credit_frames,
        pools: vec![PoolSpec {
            name: "default".to_string(),
            dsn: url,
            kind,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }],
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry,
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    TestServer::spawn_with_factory_and_config(BootEpoch(1), config, tx_registry, factory)
}

/// A base read-only `EXEC "sql"` against the "default" pool, fetch=rows, no params.
pub fn req(sql: &str) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params: Vec::new(),
        timeout_ms: None,
        readonly: true,
        fetch: 0,
        tx_id: None,
    }
}

/// Send an EXEC and read back its single terminal, asserting the one-END frame shape (flags::END,
/// service SQL, method EXEC, echoed request_id). Returns the decoded `Outcome`.
pub async fn exec(client: &mut TestClient, rid: u32, req: &ExecRequest) -> Outcome {
    client
        .send_request(rid, service::SQL, method_sql::EXEC, req.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "terminal echoes the request id");
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "the EXEC terminal carries flags::END (exactly one END)"
    );
    assert_eq!(t.header.service, service::SQL);
    assert_eq!(t.header.method, method_sql::EXEC);
    Outcome::decode(&t.payload).expect("decode terminal Outcome")
}

/// Unwrap an EXEC terminal expected to be `Outcome::Ok(ExecOk)`.
pub async fn exec_ok(client: &mut TestClient, rid: u32, req: &ExecRequest) -> ExecOk {
    match exec(client, rid, req).await {
        Outcome::Ok(body) => ExecOk::decode(&body).expect("decode ExecOk"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}

/// Unwrap an EXEC terminal expected to be `Outcome::Error(ErrorPayload)`.
pub async fn exec_err(client: &mut TestClient, rid: u32, req: &ExecRequest) -> ErrorPayload {
    match exec(client, rid, req).await {
        Outcome::Error(ep) => ep,
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
}

/// Prove the session is still alive after a terminal (⇒ exactly one END was produced): PING→PONG.
pub async fn assert_session_alive(client: &mut TestClient, token: u64) {
    client.ping(9, token).await;
    let pong = client.recv().await;
    assert_eq!(pong.header.service, service::CORE);
    assert_eq!(pong.header.method, method_core::PONG);
    assert_eq!(pong.header.request_id, 9);
}
