//! Shared harness for the M6-F4b HTTP suites (`http_f4b_it.rs`, `http_rss_it.rs`): recording
//! loopback upstreams, a daemon built through the REAL handler factory (and, for the drain, the
//! REAL `serve`), and a client that collects one request's frames.
//!
//! These are deliberately COPIES of the shapes `http_engine_it.rs` (F4a) uses, not a refactor of
//! that file: F4a was under review while F4b was built, and moving its helpers would have collided
//! with the reviewer's fixes. Folding the two into one module is a follow-up, not a behaviour.
#![allow(dead_code)]

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use ferro_http::HttpConfig;
use ferro_http::engine::{BoxIo, Connect, HttpEngine, Resolve, StaticResolver, TcpConnect};
use ferro_proto::consts::{feature_engine, flags, http_cause, method_http, service};
use ferro_proto::messages::{ErrorPayload, HttpBody, HttpDone, HttpHead, HttpRequest, Outcome};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::common::{self, TestClient, TestServer};

// =================================================================================================
// Recording upstreams
// =================================================================================================

/// One request as the upstream received it.
#[derive(Clone, Debug)]
pub struct Seen {
    pub head: String,
    pub body: Vec<u8>,
}

impl Seen {
    pub fn header(&self, name: &str) -> Option<String> {
        self.head.split("\r\n").skip(1).find_map(|l| {
            let (n, v) = l.split_once(':')?;
            n.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
        })
    }
}

/// What an upstream observed, shared with the test.
#[derive(Clone, Default)]
pub struct Rec {
    pub conns: Arc<AtomicU64>,
    pub requests: Arc<Mutex<Vec<Seen>>>,
    /// Request HEADS fully read (counted before the body is read).
    pub heads: Arc<AtomicU64>,
    pub bytes_in: Arc<AtomicU64>,
    pub written: Arc<AtomicU64>,
    pub closed_by_peer: Arc<AtomicU64>,
}

impl Rec {
    pub fn conns(&self) -> u64 {
        self.conns.load(Ordering::SeqCst)
    }
    pub fn requests(&self) -> Vec<Seen> {
        self.requests.lock().unwrap().clone()
    }
    pub fn heads(&self) -> u64 {
        self.heads.load(Ordering::SeqCst)
    }
    pub fn written(&self) -> u64 {
        self.written.load(Ordering::SeqCst)
    }
    pub fn closed_by_peer(&self) -> u64 {
        self.closed_by_peer.load(Ordering::SeqCst)
    }

    /// Read one request's head only. `None` on EOF or error first.
    pub async fn read_head(&self, s: &mut TcpStream, buf: &mut Vec<u8>) -> Option<(String, usize)> {
        let mut chunk = vec![0u8; 64 * 1024];
        let end = loop {
            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break p + 4;
            }
            let n = s.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
            self.bytes_in.fetch_add(n as u64, Ordering::SeqCst);
            buf.extend_from_slice(&chunk[..n]);
        };
        self.heads.fetch_add(1, Ordering::SeqCst);
        let head = String::from_utf8_lossy(&buf[..end]).into_owned();
        let len = Seen {
            head: head.clone(),
            body: Vec::new(),
        }
        .header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
        buf.drain(..end);
        Some((head, len))
    }

    /// Read one whole request (head + `Content-Length` body). `None` on EOF or error first.
    pub async fn read_request(&self, s: &mut TcpStream) -> Option<Seen> {
        let mut buf = Vec::new();
        let (head, len) = self.read_head(s, &mut buf).await?;
        let mut chunk = vec![0u8; 64 * 1024];
        while buf.len() < len {
            let n = s.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
            self.bytes_in.fetch_add(n as u64, Ordering::SeqCst);
            buf.extend_from_slice(&chunk[..n]);
        }
        let seen = Seen { head, body: buf };
        self.requests.lock().unwrap().push(seen.clone());
        Some(seen)
    }

    /// Write `bytes`, counting what the kernel accepted.
    pub async fn write(&self, s: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
        let mut off = 0;
        while off < bytes.len() {
            let n = s.write(&bytes[off..]).await?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            off += n;
            self.written.fetch_add(n as u64, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Hold the connection until the engine closes it (or `limit` passes); records a close.
    pub async fn hold_until_closed(&self, s: &mut TcpStream, limit: Duration) {
        let mut b = [0u8; 64 * 1024];
        let r = tokio::time::timeout(limit, async {
            loop {
                match s.read(&mut b).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        self.bytes_in.fetch_add(n as u64, Ordering::SeqCst);
                    }
                }
            }
        })
        .await;
        if r.is_ok() {
            self.closed_by_peer.fetch_add(1, Ordering::SeqCst);
        }
    }
}

pub struct Upstream {
    pub addr: SocketAddr,
    pub rec: Rec,
}

/// Serve every accepted connection with `script`.
pub async fn upstream<F, Fut>(script: F) -> Upstream
where
    F: Fn(TcpStream, Rec) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rec = Rec::default();
    let r = rec.clone();
    let script = Arc::new(script);
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            r.conns.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(script(s, r.clone()));
        }
    });
    Upstream { addr, rec }
}

/// A keep-alive upstream answering every request with `response`.
pub async fn responder(response: &'static [u8]) -> Upstream {
    upstream(move |mut s, rec| async move {
        while rec.read_request(&mut s).await.is_some() {
            if rec.write(&mut s, response).await.is_err() {
                return;
            }
        }
    })
    .await
}

/// An upstream that reads the whole request and then never answers, holding the connection until
/// the engine closes it.
pub async fn silent() -> Upstream {
    upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            rec.hold_until_closed(&mut s, Duration::from_secs(60)).await;
        }
    })
    .await
}

pub fn vars(list: &[(&str, String)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

pub fn origin(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

/// `FERRO_UPSTREAMS` plus each named upstream's `ORIGIN` (an IP-literal origin admits its own
/// literal, §23.8.5) and any extra `(name, KEY, value)` keys.
pub fn upstreams(
    list: &[(&str, SocketAddr)],
    extra: &[(&str, &str, &str)],
) -> Vec<(String, String)> {
    let names: Vec<&str> = list.iter().map(|(n, _)| *n).collect();
    let mut v = vec![("FERRO_UPSTREAMS".to_string(), names.join(","))];
    for (n, a) in list {
        v.push((
            format!("FERRO_UPSTREAM_{}_ORIGIN", n.to_ascii_uppercase()),
            origin(*a),
        ));
    }
    for (n, k, val) in extra {
        if n.is_empty() {
            v.push((format!("FERRO_HTTP_{k}"), (*val).to_string()));
        } else {
            v.push((
                format!("FERRO_UPSTREAM_{}_{k}", n.to_ascii_uppercase()),
                (*val).to_string(),
            ));
        }
    }
    v
}

// =================================================================================================
// Connectors
// =================================================================================================

/// A TCP connector whose FIRST `stall_conns` connections accept `stall_after` request bytes and then
/// never accept another (`poll_write` stays `Pending` until the stream is dropped) — a body that can
/// never be fully written, deterministically, whatever the kernel's socket buffers would absorb.
/// Later connections are plain TCP.
pub struct StallConnect {
    pub stall_after: usize,
    pub stall_conns: usize,
    /// Connection attempts from this index on never complete (a connect that hangs).
    pub hang_from: usize,
    pub opened: AtomicUsize,
}

impl StallConnect {
    pub fn new(stall_after: usize, stall_conns: usize) -> Arc<Self> {
        Self::hanging(stall_after, stall_conns, usize::MAX)
    }

    pub fn hanging(stall_after: usize, stall_conns: usize, hang_from: usize) -> Arc<Self> {
        Arc::new(StallConnect {
            stall_after,
            stall_conns,
            hang_from,
            opened: AtomicUsize::new(0),
        })
    }
}

struct StallIo {
    inner: TcpStream,
    budget: Option<usize>,
}

impl AsyncRead for StallIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for StallIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.budget {
            None => Pin::new(&mut self.inner).poll_write(cx, buf),
            Some(0) => Poll::Pending,
            Some(left) => {
                let n = buf.len().min(left);
                match Pin::new(&mut self.inner).poll_write(cx, &buf[..n]) {
                    Poll::Ready(Ok(k)) => {
                        self.budget = Some(left - k);
                        Poll::Ready(Ok(k))
                    }
                    other => other,
                }
            }
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connect for StallConnect {
    fn connect<'a>(
        &'a self,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        Box::pin(async move {
            let n = self.opened.fetch_add(1, Ordering::SeqCst);
            if n >= self.hang_from {
                std::future::pending::<()>().await;
            }
            let inner = TcpStream::connect(peer).await?;
            inner.set_nodelay(true)?;
            let budget = (n < self.stall_conns).then_some(self.stall_after);
            Ok(Box::new(StallIo { inner, budget }) as BoxIo)
        })
    }
}

// =================================================================================================
// The daemon
// =================================================================================================

pub fn engine_with(env: Vec<(String, String)>, connector: Arc<dyn Connect>) -> Arc<HttpEngine> {
    let cfg = HttpConfig::load(env.into_iter().map(|(k, v)| (k.into(), v.into())), &|_| {
        Err(io::Error::from(io::ErrorKind::NotFound))
    });
    let errors: Vec<String> = cfg.errors().iter().map(|e| e.to_string()).collect();
    assert!(errors.is_empty(), "test configuration refused: {errors:?}");
    let resolver: Arc<dyn Resolve> = StaticResolver::new();
    Arc::new(HttpEngine::with_seams(Arc::new(cfg), resolver, connector))
}

/// A daemon (sessions without `serve`, so never drained) whose HTTP engine the test can inspect.
pub struct Daemon {
    pub server: TestServer,
    pub engine: Arc<HttpEngine>,
}

pub fn daemon_with(
    env: Vec<(String, String)>,
    connector: Arc<dyn Connect>,
    tune: impl FnOnce(&mut Config),
) -> Daemon {
    let engine = engine_with(env, connector);
    let mut config = Config::default();
    tune(&mut config);
    config.validate().expect("valid daemon config");
    let registry = PoolRegistry::build_with_http(&config, Some(engine.clone()));
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    Daemon {
        server: TestServer::spawn_with_factory_and_config(
            BootEpoch(41),
            config,
            registry,
            tx_registry,
            factory,
        ),
        engine,
    }
}

pub fn daemon(env: Vec<(String, String)>) -> Daemon {
    daemon_with(env, Arc::new(TcpConnect), |_| {})
}

impl Daemon {
    pub async fn client(&self) -> TestClient {
        hello(self.server.connect().await).await
    }
}

pub async fn hello(mut c: TestClient) -> TestClient {
    let hello = c.hello(1).await;
    assert_ne!(
        hello.ack.features & u32::from(feature_engine::HTTP),
        0,
        "a daemon serving HTTP advertises it (§23.5)"
    );
    c
}

static SOCK: AtomicU64 = AtomicU64::new(0);

/// A daemon driven by the REAL `serve` (the accept loop `main` runs), so its sessions are told
/// about the drain (§23.6.1).
pub struct Served {
    pub socket: PathBuf,
    pub drain: Drain,
    pub engine: Arc<HttpEngine>,
    pub served: JoinHandle<()>,
}

pub fn served(env: Vec<(String, String)>, drain_deadline: Duration) -> Served {
    served_with(env, drain_deadline, Arc::new(TcpConnect))
}

pub fn served_with(
    env: Vec<(String, String)>,
    drain_deadline: Duration,
    connector: Arc<dyn Connect>,
) -> Served {
    let engine = engine_with(env, connector);
    let mut socket = std::env::temp_dir();
    socket.push(format!(
        "ferrod-f4b-{}-{}.sock",
        std::process::id(),
        SOCK.fetch_add(1, Ordering::Relaxed)
    ));
    let config = Config {
        socket_path: socket.clone(),
        drain_deadline,
        ..Config::default()
    };
    let listener = ferrod::listener::bind_uds(&config).unwrap();
    let registry = PoolRegistry::build_with_http(&config, Some(engine.clone()));
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    let drain = Drain::new();
    let served = tokio::spawn(ferrod::serve::serve(
        listener,
        config,
        BootEpoch(43),
        drain.clone(),
        registry,
        tx_registry,
        factory,
    ));
    Served {
        socket,
        drain,
        engine,
        served,
    }
}

impl Served {
    pub async fn client(&self) -> TestClient {
        hello(common::connect(&self.socket).await).await
    }
}

// =================================================================================================
// Requests and replies
// =================================================================================================

pub fn request(upstream: &str, method: &str, target: &str) -> HttpRequest {
    HttpRequest {
        upstream: upstream.into(),
        method: method.into(),
        target: target.into(),
        origin: None,
        headers: Vec::new(),
        body: None,
        timeout_ms: Some(10_000),
        connect_timeout_ms: None,
        read_timeout_ms: None,
        idempotent: None,
        decode: false,
        route: None,
        traceparent: None,
    }
}

pub fn post(upstream: &str, body: &[u8]) -> HttpRequest {
    HttpRequest {
        body: Some(body.to_vec()),
        ..request(upstream, "POST", "/v1/charges")
    }
}

pub fn idempotent_get(upstream: &str) -> HttpRequest {
    HttpRequest {
        idempotent: Some(true),
        ..request(upstream, "GET", "/v1/items")
    }
}

/// Everything one request produced on the session, in order.
#[derive(Debug)]
pub struct Reply {
    pub head: Option<HttpHead>,
    pub body: Vec<u8>,
    pub body_frames: usize,
    pub end: Outcome,
}

impl Reply {
    pub fn done(&self) -> HttpDone {
        match &self.end {
            Outcome::Ok(b) => HttpDone::decode(b).expect("HttpDone"),
            other => panic!("expected Ok(HttpDone), got {other:?}"),
        }
    }
    pub fn error(&self) -> &ErrorPayload {
        match &self.end {
            Outcome::Error(ep) => ep,
            other => panic!("expected an error, got {other:?}"),
        }
    }
    /// Assert an error terminal: code, branch, exactly one registry cause token, nil
    /// `sqlstate`/`errno` (§23.5.6).
    pub fn assert_error(&self, code: u16, branch: u8, cause: &str) -> &ErrorPayload {
        let ep = self.error();
        assert_eq!(
            (ep.code, ep.branch, ep.detail.as_deref()),
            (code, branch, Some(cause)),
            "{ep:?}"
        );
        assert!(http_cause::ALL.contains(&cause));
        assert_eq!((ep.sqlstate.as_deref(), ep.errno), (None, None));
        ep
    }
}

/// Collect `rid`'s frames until its ONE terminal, replenishing credit as each frame is consumed.
/// `wait` bounds each read.
pub async fn collect_within(c: &mut TestClient, rid: u32, wait: Duration) -> Reply {
    let mut head = None;
    let mut body = Vec::new();
    let mut body_frames = 0;
    loop {
        let f = c
            .recv_or_none(wait)
            .await
            .unwrap_or_else(|| panic!("no frame for request {rid} within {wait:?}"));
        assert_eq!(f.header.request_id, rid, "a frame for another request");
        assert_eq!(f.header.service, service::HTTP);
        if f.header.flags & flags::END != 0 {
            let end = Outcome::decode(&f.payload).expect("terminal");
            return Reply {
                head,
                body,
                body_frames,
                end,
            };
        }
        match f.header.method {
            m if m == method_http::HEAD => {
                assert!(head.is_none(), "HEAD is sent once");
                head = Some(HttpHead::decode(&f.payload).expect("HttpHead"));
            }
            m if m == method_http::BODY => {
                let chunk = HttpBody::decode(&f.payload).expect("HttpBody").chunk;
                assert!(!chunk.is_empty() && chunk.len() <= 256 * 1024);
                body.extend_from_slice(&chunk);
                body_frames += 1;
            }
            other => panic!("unexpected HTTP method {other}"),
        }
        c.window_update(rid, 1, f.payload.len() as u32).await;
    }
}

pub async fn collect(c: &mut TestClient, rid: u32) -> Reply {
    collect_within(c, rid, Duration::from_secs(10)).await
}

pub async fn send(c: &mut TestClient, rid: u32, r: &HttpRequest) {
    c.send_request(rid, service::HTTP, method_http::REQUEST, r.encode())
        .await;
}

pub async fn exchange(c: &mut TestClient, rid: u32, r: &HttpRequest) -> Reply {
    send(c, rid, r).await;
    collect(c, rid).await
}

/// Poll `cond` until it holds (5 s cap).
pub async fn wait_for(what: &str, cond: impl Fn() -> bool) {
    let start = std::time::Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
