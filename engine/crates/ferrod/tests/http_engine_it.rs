//! **M6-F4a — Ferro HTTP's HTTP/1.1 plaintext engine, end to end through a real `ferrod` session
//! (SPEC §23.6–§23.9, §23.14).**
//!
//! Every test drives the REAL per-connection handler factory (`services::sql::make_handler`) over a
//! real Unix socket, with a real `HttpEngine` (`ferro_http::engine`) behind it, against loopback
//! upstreams this file starts in-process and scripts byte by byte — so a misbehaving upstream is a
//! script, and **every upstream records what it received**: at-most-once is a read-back assertion
//! (charter rule 3), and "refused before any dial" is an accepted-connection count of zero.
//!
//! Two seams are injected, both below the engine's decisions: the resolver (a hostname has to
//! resolve to `fd00:ec2::254` on a test host somehow), and — for the "dispatched, not sent" cells
//! only — the connector, because on real TCP `hyper`'s idle connection task notices a dead socket
//! before a request can be written to it (premise P1), so that cell is reachable only through a
//! socket that refuses the write.
//!
//! Each fate cell F4a implements has a test here; `ferro_http::fate`'s unit tests prove the table
//! total. The mutation table is in SPEC §22.2 (cz).
#![cfg(feature = "http")]

mod common;

use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use common::{TestClient, TestServer, assert_session_alive};
use ferro_http::HttpConfig;
use ferro_http::engine::{
    BoxIo, Connect, HttpEngine, Resolve, ResponseSink, SinkError, SinkFrame, StaticResolver,
    TcpConnect, Terminal,
};
use ferro_proto::consts::{
    MAX_FRAME_PAYLOAD, errc, feature_engine, flags, http_cause, method_http, service,
};
use ferro_proto::messages::{
    ErrorPayload, HttpBody, HttpDone, HttpHead, HttpHeaderField, HttpRequest, Outcome,
};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

// =================================================================================================
// Loopback upstreams that record what they received
// =================================================================================================

/// One request as the upstream received it.
#[derive(Clone, Debug)]
struct Seen {
    /// The raw request head, up to and including the blank line (lossily decoded).
    head: String,
    /// The same head, as bytes.
    raw: Vec<u8>,
    body: Vec<u8>,
}

impl Seen {
    fn request_line(&self) -> &str {
        self.head.split("\r\n").next().unwrap_or("")
    }
    /// Header lines as written (name case preserved), in order.
    fn header_lines(&self) -> Vec<&str> {
        self.head
            .split("\r\n")
            .skip(1)
            .filter(|l| !l.is_empty())
            .collect()
    }
    fn header(&self, name: &str) -> Option<String> {
        self.header_lines().iter().find_map(|l| {
            let (n, v) = l.split_once(':')?;
            n.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
        })
    }
}

/// What an upstream observed, shared with the test.
#[derive(Clone, Default)]
struct Rec {
    /// Connections accepted.
    conns: Arc<AtomicU64>,
    /// Requests whose head was fully read.
    requests: Arc<Mutex<Vec<Seen>>>,
    /// Every request byte read, on every connection.
    bytes_in: Arc<AtomicU64>,
    /// Response bytes the upstream's kernel accepted.
    written: Arc<AtomicU64>,
    /// Connections the upstream saw closed by the engine (EOF or error while it waited).
    closed_by_peer: Arc<AtomicU64>,
}

impl Rec {
    fn conns(&self) -> u64 {
        self.conns.load(Ordering::SeqCst)
    }
    fn requests(&self) -> Vec<Seen> {
        self.requests.lock().unwrap().clone()
    }
    fn written(&self) -> u64 {
        self.written.load(Ordering::SeqCst)
    }
    fn closed_by_peer(&self) -> u64 {
        self.closed_by_peer.load(Ordering::SeqCst)
    }

    /// Read one request (head + `Content-Length` body — the engine always sends one for a body).
    /// `None` on EOF or error before a complete request.
    async fn read_request(&self, s: &mut TcpStream) -> Option<Seen> {
        let mut buf = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        let head_end = loop {
            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break p + 4;
            }
            let n = s.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
            self.bytes_in.fetch_add(n as u64, Ordering::SeqCst);
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let mut seen = Seen {
            head,
            raw: buf[..head_end].to_vec(),
            body: Vec::new(),
        };
        let len: usize = seen
            .header("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut body = buf[head_end..].to_vec();
        while body.len() < len {
            let n = s.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
            self.bytes_in.fetch_add(n as u64, Ordering::SeqCst);
            body.extend_from_slice(&chunk[..n]);
        }
        seen.body = body;
        self.requests.lock().unwrap().push(seen.clone());
        Some(seen)
    }

    /// Write `bytes`, counting what the kernel accepted.
    async fn write(&self, s: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
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
    async fn hold_until_closed(&self, s: &mut TcpStream, limit: Duration) {
        let mut b = [0u8; 4096];
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

struct Upstream {
    addr: SocketAddr,
    rec: Rec,
}

/// Serve every accepted connection with `script`.
async fn upstream_on<F, Fut>(listener: TcpListener, script: F) -> Upstream
where
    F: Fn(TcpStream, Rec) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
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

async fn upstream<F, Fut>(script: F) -> Upstream
where
    F: Fn(TcpStream, Rec) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    upstream_on(TcpListener::bind("127.0.0.1:0").await.unwrap(), script).await
}

/// A keep-alive upstream answering every request with `response`.
async fn responder(response: &'static [u8]) -> Upstream {
    upstream(move |mut s, rec| async move {
        while rec.read_request(&mut s).await.is_some() {
            if rec.write(&mut s, response).await.is_err() {
                return;
            }
        }
    })
    .await
}

/// Close with RST rather than FIN (`SO_LINGER = 0`).
#[allow(deprecated)]
fn rst(s: TcpStream) {
    s.set_linger(Some(Duration::ZERO)).unwrap();
    drop(s);
}

const OK_HELLO: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Up: a\r\nX-Up: b\r\n\r\nhello";

// =================================================================================================
// The daemon
// =================================================================================================

fn vars(list: &[(&str, String)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn origin(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

struct Daemon {
    server: TestServer,
}

fn daemon_with(
    env: Vec<(String, String)>,
    resolver: Arc<dyn Resolve>,
    connector: Arc<dyn Connect>,
    tune: impl FnOnce(&mut Config),
) -> Daemon {
    let cfg = HttpConfig::load(env.into_iter().map(|(k, v)| (k.into(), v.into())), &|_| {
        Err(io::Error::from(io::ErrorKind::NotFound))
    });
    let errors: Vec<String> = cfg.errors().iter().map(|e| e.to_string()).collect();
    assert!(errors.is_empty(), "test configuration refused: {errors:?}");
    let engine = Arc::new(HttpEngine::with_seams(Arc::new(cfg), resolver, connector));
    let mut config = Config::default();
    tune(&mut config);
    config.validate().expect("valid daemon config");
    let registry = PoolRegistry::build_with_http(&config, Some(engine));
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
            BootEpoch(23),
            config,
            registry,
            tx_registry,
            factory,
        ),
    }
}

fn daemon(env: Vec<(String, String)>) -> Daemon {
    daemon_with(env, StaticResolver::new(), Arc::new(TcpConnect), |_| {})
}

impl Daemon {
    async fn client(&self) -> TestClient {
        let mut c = self.server.connect().await;
        let hello = c.hello(1).await;
        assert_ne!(
            hello.ack.features & u32::from(feature_engine::HTTP),
            0,
            "a daemon serving HTTP advertises it (§23.5)"
        );
        c
    }
}

// =================================================================================================
// The client side
// =================================================================================================

fn request(upstream: &str, method: &str, target: &str) -> HttpRequest {
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

fn post(upstream: &str, body: &[u8]) -> HttpRequest {
    HttpRequest {
        body: Some(body.to_vec()),
        ..request(upstream, "POST", "/v1/charges")
    }
}

fn idempotent_get(upstream: &str) -> HttpRequest {
    HttpRequest {
        idempotent: Some(true),
        ..request(upstream, "GET", "/v1/items")
    }
}

/// Everything one request produced on the session, in order.
#[derive(Debug)]
struct Reply {
    head: Option<HttpHead>,
    body: Vec<u8>,
    body_frames: usize,
    end: Outcome,
}

impl Reply {
    fn done(&self) -> HttpDone {
        match &self.end {
            Outcome::Ok(b) => HttpDone::decode(b).expect("HttpDone"),
            other => panic!(
                "expected Ok(HttpDone), got {other:?} (head: {:?})",
                self.head
            ),
        }
    }
    fn error(&self) -> &ErrorPayload {
        match &self.end {
            Outcome::Error(ep) => ep,
            other => panic!("expected an error, got {other:?}"),
        }
    }
    /// Assert an error terminal: code, branch, exactly one registry cause token, nil
    /// `sqlstate`/`errno` (§23.5.6, C11).
    fn assert_error(&self, code: u16, branch: u8, cause: &str) -> &ErrorPayload {
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
    fn assert_cancelled(&self) {
        assert!(
            matches!(self.end, Outcome::Cancelled),
            "expected Cancelled, got {:?}",
            self.end
        );
    }
}

/// Collect `rid`'s frames until its ONE terminal, replenishing credit as each frame is consumed
/// (what the PHP client does). Asserts every frame's header shape.
async fn collect(c: &mut TestClient, rid: u32) -> Reply {
    collect_with(c, rid, true).await
}

async fn collect_with(c: &mut TestClient, rid: u32, replenish: bool) -> Reply {
    let mut head = None;
    let mut body = Vec::new();
    let mut body_frames = 0;
    loop {
        let f = c.recv().await;
        assert_eq!(f.header.request_id, rid, "a frame for another request");
        assert_eq!(f.header.service, service::HTTP);
        if f.header.flags & flags::END != 0 {
            assert_eq!(
                f.header.method,
                method_http::REQUEST,
                "the terminal rides REQUEST"
            );
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
                assert_eq!(f.header.flags, 0, "HEAD is not STREAM-flagged (§23.5.2)");
                head = Some(HttpHead::decode(&f.payload).expect("HttpHead"));
            }
            m if m == method_http::BODY => {
                assert!(head.is_some(), "BODY follows HEAD");
                assert_eq!(
                    f.header.flags,
                    flags::STREAM,
                    "BODY carries STREAM (§23.5.3)"
                );
                let chunk = HttpBody::decode(&f.payload).expect("HttpBody").chunk;
                assert!(
                    !chunk.is_empty() && chunk.len() <= 256 * 1024,
                    "a BODY chunk is non-empty and at most 256 KiB: {}",
                    chunk.len()
                );
                body.extend_from_slice(&chunk);
                body_frames += 1;
            }
            other => panic!("unexpected HTTP method {other}"),
        }
        if replenish {
            c.window_update(rid, 1, f.payload.len() as u32).await;
        }
    }
}

async fn exchange(c: &mut TestClient, rid: u32, r: &HttpRequest) -> Reply {
    c.send_request(rid, service::HTTP, method_http::REQUEST, r.encode())
        .await;
    collect(c, rid).await
}

fn header<'a>(h: &'a HttpHead, name: &str) -> Vec<&'a [u8]> {
    h.headers
        .iter()
        .filter(|f| f.name == name)
        .map(|f| f.value.as_slice())
        .collect()
}

// =================================================================================================
// The vertical: GET and POST, Content-Length and chunked
// =================================================================================================

/// **A GET and a POST through the engine, end to end** (§23.6): the bytes the upstream received
/// are exactly the accepted target and `Host` = the authority, the engine adds nothing but `Host`
/// and `Content-Length`, the response arrives as one `HEAD` (lowercase names, duplicates kept, the
/// hop-by-hop ones gone) + `BODY` frames + one `END` carrying `HttpDone`, and the stats agree with
/// the upstream's own byte counts.
#[tokio::test]
async fn a_get_and_a_post_round_trip_with_exact_bytes_and_one_terminal() {
    let up = responder(OK_HELLO).await;
    let chunked = responder(
        b"HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\nX-Checksum: c1\r\n\r\n",
    )
    .await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "api,chunky".into()),
        ("FERRO_UPSTREAM_API_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_CHUNKY_ORIGIN", origin(chunked.addr)),
    ]));
    let mut c = d.client().await;

    // ---- GET, no body --------------------------------------------------------------------------
    let mut get = request("api", "GET", "/v1/items?q=a%20b&x=1");
    get.headers = vec![HttpHeaderField {
        name: "x-trace".into(),
        value: b"t-1".to_vec(),
    }];
    let r = exchange(&mut c, 10, &get).await;
    let head = r.head.as_ref().expect("HEAD");
    assert_eq!(
        (head.status, head.version, head.reason.as_deref()),
        (200, 11, Some(&b"OK"[..]))
    );
    assert_eq!(header(head, "content-length"), [b"5"]);
    assert_eq!(
        header(head, "x-up"),
        [&b"a"[..], b"b"],
        "duplicates kept, in order"
    );
    assert!(
        !head.idempotent,
        "undeclared, and IDEMPOTENT_METHODS is empty"
    );
    assert_eq!(head.decoded, None, "F4a never decodes");
    assert_eq!(r.body, b"hello");
    let done = r.done();
    assert!(!done.stats.reused);
    assert!(done.trailers.is_empty());

    let seen = up.rec.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].request_line(), "GET /v1/items?q=a%20b&x=1 HTTP/1.1");
    assert_eq!(
        seen[0].header("host").as_deref(),
        Some(up.addr.to_string().as_str())
    );
    // The engine adds nothing but `Host` (and `Content-Length` for a body): the PHP header,
    // title-cased by hyper on HTTP/1.1 (§23.4.3, P6).
    let names: Vec<&str> = seen[0]
        .header_lines()
        .iter()
        .map(|l| l.split(':').next().unwrap())
        .collect();
    assert_eq!(names, ["Host", "X-Trace"]);
    assert_eq!(
        done.stats.bytes_sent,
        up.rec.bytes_in.load(Ordering::SeqCst),
        "bytes_sent is what the upstream received (P1 exactness)"
    );
    assert_eq!(done.stats.bytes_received, OK_HELLO.len() as u64);

    // ---- POST, chunked response ------------------------------------------------------------------
    let mut p = post("chunky", br#"{"amount":100}"#);
    p.headers = vec![HttpHeaderField {
        name: "Content-Type".into(),
        value: b"application/json".to_vec(),
    }];
    let r = exchange(&mut c, 11, &p).await;
    let head = r.head.as_ref().expect("HEAD");
    assert_eq!(head.status, 201);
    assert!(
        head.headers
            .iter()
            .all(|f| f.name != "transfer-encoding" && f.name != "connection"),
        "hop-by-hop fields removed: {:?}",
        head.headers
    );
    assert_eq!(r.body, b"abcde", "chunked framing removed, content intact");
    let done = r.done();
    assert_eq!(
        done.trailers,
        [HttpHeaderField {
            name: "x-checksum".into(),
            value: b"c1".to_vec()
        }],
        "the chunked trailer section reaches HttpDone, name lowercased"
    );
    let seen = chunked.rec.requests();
    assert_eq!(seen[0].request_line(), "POST /v1/charges HTTP/1.1");
    assert_eq!(seen[0].header("content-length").as_deref(), Some("14"));
    assert_eq!(seen[0].body, br#"{"amount":100}"#);
    assert_eq!(
        seen[0].header("content-type").as_deref(),
        Some("application/json")
    );

    assert_session_alive(&mut c, 77).await;
}

/// **A large body streams in ≤ 256 KiB `BODY` frames under credit**, in order and complete, and
/// the request's ONE terminal comes after the last of them (B4 on the HTTP service).
#[tokio::test]
async fn a_large_body_streams_in_bounded_chunks_in_order() {
    const LEN: usize = 3 * 1024 * 1024 + 17;
    let up = upstream(|mut s, rec| async move {
        while rec.read_request(&mut s).await.is_some() {
            let body: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {LEN}\r\n\r\n");
            if rec.write(&mut s, head.as_bytes()).await.is_err()
                || rec.write(&mut s, &body).await.is_err()
            {
                return;
            }
        }
    })
    .await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "big".into()),
        ("FERRO_UPSTREAM_BIG_ORIGIN", origin(up.addr)),
    ]));
    let mut c = d.client().await;
    let r = exchange(&mut c, 5, &idempotent_get("big")).await;
    assert_eq!(r.body.len(), LEN);
    assert!(
        r.body
            .iter()
            .enumerate()
            .all(|(i, b)| *b == (i % 251) as u8)
    );
    assert!(
        r.body_frames >= LEN / (256 * 1024),
        "{} frames",
        r.body_frames
    );
    assert!(
        r.head.as_ref().unwrap().idempotent,
        "declared by the caller"
    );
    r.done();
}

// =================================================================================================
// Keep-alive reuse, and the non-idempotent reuse rule (§23.8.2)
// =================================================================================================

/// **Reuse and `H1_UNSAFE_REUSE_MAX_IDLE_MS`.** Back-to-back requests share one connection
/// (`reused`); a NON-idempotent request refuses a connection idle longer than the threshold and
/// dials fresh, while a declared-idempotent one still takes an idle connection.
#[tokio::test]
async fn keep_alive_reuse_and_the_unsafe_reuse_threshold() {
    let up = responder(OK_HELLO).await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "ka".into()),
        ("FERRO_UPSTREAM_KA_ORIGIN", origin(up.addr)),
        (
            "FERRO_UPSTREAM_KA_H1_UNSAFE_REUSE_MAX_IDLE_MS",
            "150".into(),
        ),
    ]));
    let mut c = d.client().await;

    assert!(
        !exchange(&mut c, 1, &request("ka", "GET", "/a"))
            .await
            .done()
            .stats
            .reused
    );
    assert!(
        exchange(&mut c, 2, &request("ka", "GET", "/b"))
            .await
            .done()
            .stats
            .reused
    );
    // A POST within the threshold reuses (control for the next one).
    assert!(
        exchange(&mut c, 3, &post("ka", b"x"))
            .await
            .done()
            .stats
            .reused
    );
    assert_eq!(up.rec.conns(), 1);

    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = exchange(&mut c, 4, &post("ka", b"y")).await.done();
    assert!(
        !r.stats.reused,
        "a POST must not reuse a connection idle past H1_UNSAFE_REUSE_MAX_IDLE_MS"
    );
    assert_eq!(up.rec.conns(), 2, "it dialled fresh");

    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = exchange(&mut c, 5, &idempotent_get("ka")).await.done();
    assert!(
        r.stats.reused,
        "a declared-idempotent GET may take a connection idle past it"
    );
    assert_eq!(up.rec.conns(), 2);
    assert_eq!(
        up.rec.requests().len(),
        5,
        "every request reached the upstream exactly once"
    );
}

// =================================================================================================
// Refusals (§23.4): Forbidden, one cause token, nothing dialled
// =================================================================================================

/// **Every validator refusal is NonRetryable `Forbidden` with its one `forbidden_*` token, made
/// before any dial** (the upstream accepts no connection at all), and its message quotes nothing the
/// request carried. An upstream whose `ALLOW_UIDS` excludes the peer's kernel-attested uid is the
/// SAME refusal as an unknown name (D15).
#[tokio::test]
async fn refusals_are_forbidden_with_their_cause_and_dial_nothing() {
    let up = responder(OK_HELLO).await;
    let own = nix::unistd::geteuid().as_raw();
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "api,mine,theirs".into()),
        ("FERRO_UPSTREAM_API_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_API_MAX_BODY_BYTES", "8".into()),
        ("FERRO_UPSTREAM_MINE_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_MINE_ALLOW_UIDS", own.to_string()),
        ("FERRO_UPSTREAM_THEIRS_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_THEIRS_ALLOW_UIDS", (own + 1).to_string()),
    ]));
    let mut c = d.client().await;

    let with_header = |name: &str| HttpRequest {
        headers: vec![HttpHeaderField {
            name: name.into(),
            value: b"SECRET-CANARY".to_vec(),
        }],
        ..request("api", "GET", "/v1/x")
    };
    let cases: Vec<(&str, HttpRequest, &str)> = vec![
        (
            "unknown upstream",
            request("nope", "GET", "/"),
            http_cause::FORBIDDEN_UPSTREAM,
        ),
        (
            "uid not allowed",
            request("theirs", "GET", "/"),
            http_cause::FORBIDDEN_UPSTREAM,
        ),
        (
            "dot segment",
            request("api", "GET", "/v1/../SECRET-CANARY"),
            http_cause::FORBIDDEN_TARGET,
        ),
        (
            "TRACE",
            request("api", "TRACE", "/"),
            http_cause::FORBIDDEN_METHOD,
        ),
        (
            "forwarding header",
            with_header("X-Forwarded-For"),
            http_cause::FORBIDDEN_HEADER,
        ),
        (
            "origin mismatch",
            HttpRequest {
                origin: Some("http://SECRET-CANARY.example".into()),
                ..request("api", "GET", "/")
            },
            http_cause::FORBIDDEN_ORIGIN,
        ),
        (
            "body over budget",
            post("api", b"SECRET-CANARY"),
            http_cause::FORBIDDEN_BODY,
        ),
    ];
    for (i, (what, req, cause)) in cases.into_iter().enumerate() {
        let r = exchange(&mut c, 100 + i as u32, &req).await;
        assert!(
            r.head.is_none(),
            "{what}: nothing streamed before the refusal"
        );
        let ep = r.assert_error(errc::FORBIDDEN, errc::FORBIDDEN_BRANCH, cause);
        assert!(
            !ep.message.contains("SECRET-CANARY"),
            "{what}: the refusal quotes a request value: {}",
            ep.message
        );
    }
    assert_eq!(up.rec.conns(), 0, "a refusal dials nothing");

    // Control: the uid the kernel attested IS allowed on `mine` — so the refusal above is the
    // ALLOW_UIDS rule reading the session's peer uid, not a blanket refusal.
    exchange(&mut c, 200, &request("mine", "GET", "/"))
        .await
        .done();
    assert_eq!(up.rec.conns(), 1);
}

// =================================================================================================
// The address guard and pinning (§23.8.5, chaos 14)
// =================================================================================================

/// A resolver that never answers (a slow DNS), for "before dispatch" cancel and deadline cells.
struct PendingResolver;
impl Resolve for PendingResolver {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<IpAddr>>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// **Chaos 14.** A name resolving to `127.0.0.1` is `forbidden_address` with no dial under the
/// default class and allowed with `ADDRESS_CLASSES=loopback`; `fd00:ec2::254` (AWS IPv6 IMDS) and
/// `64:ff9b::a9fe:a9fe` (NAT64 of `169.254.169.254`) are refused whatever the classes; a mixed answer
/// skips the refused address and connects to EXACTLY the admitted one.
#[tokio::test]
async fn the_address_guard_refuses_before_dialling_and_skips_refused_answers() {
    let up = responder(OK_HELLO).await;
    let port = up.addr.port();
    // A second listener on 127.0.0.2 for the mixed answer (pinning: the connect goes there).
    let up2 = upstream_on(
        TcpListener::bind("127.0.0.2:0").await.unwrap(),
        |mut s, rec| async move {
            while rec.read_request(&mut s).await.is_some() {
                let _ = rec.write(&mut s, OK_HELLO).await;
            }
        },
    )
    .await;
    let resolver = StaticResolver::new();
    resolver.set("api.test", vec![vec![ip("127.0.0.1")]]);
    resolver.set("imds.test", vec![vec![ip("fd00:ec2::254")]]);
    resolver.set("nat64.test", vec![vec![ip("64:ff9b::a9fe:a9fe")]]);
    resolver.set(
        "mixed.test",
        vec![vec![ip("fd00:ec2::254"), ip("127.0.0.2")]],
    );
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "pub,lo,imds,nat,mixed".into()),
            (
                "FERRO_UPSTREAM_PUB_ORIGIN",
                format!("http://api.test:{port}"),
            ),
            (
                "FERRO_UPSTREAM_LO_ORIGIN",
                format!("http://api.test:{port}"),
            ),
            ("FERRO_UPSTREAM_LO_ADDRESS_CLASSES", "loopback".into()),
            (
                "FERRO_UPSTREAM_IMDS_ORIGIN",
                format!("http://imds.test:{port}"),
            ),
            (
                "FERRO_UPSTREAM_IMDS_ADDRESS_CLASSES",
                "public,private,loopback".into(),
            ),
            (
                "FERRO_UPSTREAM_NAT_ORIGIN",
                format!("http://nat64.test:{port}"),
            ),
            (
                "FERRO_UPSTREAM_NAT_ADDRESS_CLASSES",
                "public,private,loopback".into(),
            ),
            (
                "FERRO_UPSTREAM_MIXED_ORIGIN",
                format!("http://mixed.test:{}", up2.addr.port()),
            ),
            ("FERRO_UPSTREAM_MIXED_ADDRESS_CLASSES", "loopback".into()),
        ]),
        resolver.clone(),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;

    for (rid, name) in [(1, "pub"), (2, "imds"), (3, "nat")] {
        let r = exchange(&mut c, rid, &request(name, "GET", "/")).await;
        r.assert_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            http_cause::FORBIDDEN_ADDRESS,
        );
    }
    assert_eq!(up.rec.conns(), 0, "a refused address is never dialled");
    assert!(
        resolver.calls() >= 3,
        "the names were resolved, then refused"
    );

    let r = exchange(&mut c, 4, &request("lo", "GET", "/")).await;
    assert_eq!(r.body, b"hello");
    assert_eq!(up.rec.conns(), 1, "ADDRESS_CLASSES=loopback admits it");
    assert_eq!(
        up.rec.requests()[0].header("host").as_deref(),
        Some(format!("api.test:{port}").as_str()),
        "Host comes from ORIGIN, never from the resolved address"
    );

    let r = exchange(&mut c, 5, &request("mixed", "GET", "/")).await;
    assert_eq!(r.body, b"hello");
    assert_eq!(
        up2.rec.conns(),
        1,
        "the admitted address of a mixed answer was dialled"
    );
}

/// **Re-resolution happens on a new dial, and is checked again** (§23.8.5 pinning, DNS rebinding):
/// the first dial's answer is admitted and served; after `DNS_TTL_MS`, the next dial's answer is a
/// different loopback address, and the connect goes to EXACTLY that one; when the answer then
/// rebinds to the metadata address, the request is `forbidden_address` and nothing is dialled.
#[tokio::test]
async fn a_new_dial_re_resolves_re_checks_and_connects_to_the_checked_address() {
    // `Connection: close`, so every request needs a new dial (and so a new resolution).
    const CLOSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let script = |mut s: TcpStream, rec: Rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            let _ = rec.write(&mut s, CLOSE).await;
        }
    };
    let a = upstream(script).await;
    let port = a.addr.port();
    let b = upstream_on(
        TcpListener::bind(("127.0.0.2", port)).await.unwrap(),
        script,
    )
    .await;
    let resolver = StaticResolver::new();
    resolver.set(
        "rebind.test",
        vec![
            vec![ip("127.0.0.1")],
            vec![ip("127.0.0.2")],
            vec![ip("169.254.169.254")],
        ],
    );
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "rb".into()),
            (
                "FERRO_UPSTREAM_RB_ORIGIN",
                format!("http://rebind.test:{port}"),
            ),
            ("FERRO_UPSTREAM_RB_ADDRESS_CLASSES", "loopback".into()),
            ("FERRO_UPSTREAM_RB_DNS_TTL_MS", "1".into()),
        ]),
        resolver.clone(),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;
    assert_eq!(
        exchange(&mut c, 1, &request("rb", "GET", "/")).await.body,
        b"ok"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        exchange(&mut c, 2, &request("rb", "GET", "/")).await.body,
        b"ok"
    );
    assert_eq!(
        (a.rec.conns(), b.rec.conns()),
        (1, 1),
        "each dial went to its checked answer"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    exchange(&mut c, 3, &request("rb", "GET", "/"))
        .await
        .assert_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            http_cause::FORBIDDEN_ADDRESS,
        );
    assert_eq!(resolver.calls(), 3);
    assert_eq!(
        (a.rec.conns(), b.rec.conns()),
        (1, 1),
        "the rebinding dialled nothing"
    );
}

// =================================================================================================
// Dispatched, not sent (§23.7.1, review F-3) — through the connector seam
// =================================================================================================

#[derive(Clone, Copy)]
enum Fault {
    /// Every write fails with no byte accepted: a socket that refuses the request.
    Refuse,
    /// Writes never complete: the request never leaves.
    Stall,
    /// The first write waits this long, then every write passes through: a request that is slow
    /// to leave but WILL leave if nothing stops it.
    Delay(Duration),
    /// The socket accepts this many bytes in total, then every write fails: a link that dies on
    /// the WRITE side part-way through a request (P14's `write` control).
    FailAfter(usize),
}

struct FaultConnect(Fault);

struct FaultIo {
    inner: TcpStream,
    fault: Fault,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
    accepted: usize,
}

impl AsyncRead for FaultIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for FaultIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.fault {
            Fault::Refuse => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Fault::Stall => Poll::Pending,
            Fault::Delay(d) => {
                let sleep = self
                    .delay
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(d)));
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                Pin::new(&mut self.inner).poll_write(cx, buf)
            }
            Fault::FailAfter(budget) => {
                if self.accepted >= budget {
                    return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                }
                let n = buf.len().min(budget - self.accepted);
                match Pin::new(&mut self.inner).poll_write(cx, &buf[..n]) {
                    Poll::Ready(Ok(k)) => {
                        self.accepted += k;
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
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Connect for FaultConnect {
    fn connect<'a>(
        &'a self,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        let fault = self.0;
        Box::pin(async move {
            let inner = TcpStream::connect(peer).await?;
            Ok(Box::new(FaultIo {
                inner,
                fault,
                delay: None,
                accepted: 0,
            }) as BoxIo)
        })
    }
}

/// **Dispatched, not sent: never Indeterminate, even for a POST.** A socket that refuses the first
/// write is Retryable `ConnectionLost` (`unsent_write`); a request whose bytes never leave is
/// Retryable `PoolTimeout` (`deadline`) when its deadline passes, and `Cancelled` on `CANCEL`. The
/// upstream received zero bytes in every case.
#[tokio::test]
async fn dispatched_not_sent_is_retryable_for_a_post() {
    let up = upstream(|mut s, rec| async move {
        rec.hold_until_closed(&mut s, Duration::from_secs(20)).await;
    })
    .await;
    let env = || {
        vars(&[
            ("FERRO_UPSTREAMS", "f".into()),
            ("FERRO_UPSTREAM_F_ORIGIN", origin(up.addr)),
        ])
    };

    let d = daemon_with(
        env(),
        StaticResolver::new(),
        Arc::new(FaultConnect(Fault::Refuse)),
        |_| {},
    );
    let mut c = d.client().await;
    exchange(&mut c, 1, &post("f", b"charge"))
        .await
        .assert_error(
            errc::CONNECTION_LOST,
            errc::CONNECTION_LOST_BRANCH,
            http_cause::UNSENT_WRITE,
        );

    let d = daemon_with(
        env(),
        StaticResolver::new(),
        Arc::new(FaultConnect(Fault::Stall)),
        |_| {},
    );
    let mut c = d.client().await;
    let mut p = post("f", b"charge");
    p.timeout_ms = Some(300);
    exchange(&mut c, 2, &p).await.assert_error(
        errc::POOL_TIMEOUT,
        errc::POOL_TIMEOUT_BRANCH,
        http_cause::DEADLINE,
    );

    c.send_request(
        3,
        service::HTTP,
        method_http::REQUEST,
        post("f", b"charge").encode(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.cancel(3).await;
    collect(&mut c, 3).await.assert_cancelled();

    assert_eq!(
        up.rec.bytes_in.load(Ordering::SeqCst),
        0,
        "no request byte reached the upstream"
    );
}

/// **`sent` is read only after `hyper`'s connection task is aborted and awaited (§23.7.1, F1a).**
/// The request is dispatched onto a socket whose first write is slow (400 ms) but WILL succeed. A
/// `CANCEL` (or the deadline) at ~150 ms finds nothing sent — and the engine must make that FINAL
/// before classifying it, because `hyper`'s task writes independently of the response future: left
/// running, it would put the POST on the wire 250 ms after the client was told `Cancelled` (a
/// "not applied" a client may act on). The upstream must receive ZERO bytes, held well past the
/// delay.
#[tokio::test]
async fn an_unsent_request_is_stopped_before_its_fate_is_read() {
    let up = upstream(|mut s, rec| async move {
        rec.hold_until_closed(&mut s, Duration::from_secs(20)).await;
    })
    .await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "f".into()),
            ("FERRO_UPSTREAM_F_ORIGIN", origin(up.addr)),
        ]),
        StaticResolver::new(),
        Arc::new(FaultConnect(Fault::Delay(Duration::from_millis(400)))),
        |_| {},
    );
    let mut c = d.client().await;
    c.send_request(
        1,
        service::HTTP,
        method_http::REQUEST,
        post("f", b"charge").encode(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    c.cancel(1).await;
    collect(&mut c, 1).await.assert_cancelled();

    let mut p = post("f", b"charge");
    p.timeout_ms = Some(150);
    exchange(&mut c, 2, &p).await.assert_error(
        errc::POOL_TIMEOUT,
        errc::POOL_TIMEOUT_BRANCH,
        http_cause::DEADLINE,
    );

    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(
        up.rec.bytes_in.load(Ordering::SeqCst),
        0,
        "a request reported unsent reached the upstream afterwards"
    );
    assert_eq!(
        up.rec.closed_by_peer(),
        2,
        "both connections were torn down"
    );
}

// =================================================================================================
// Sent, no head (§23.7.1, chaos 1 and 2) — at most once, never re-sent
// =================================================================================================

async fn fault_upstream(kind: &'static str) -> Upstream {
    upstream(move |mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        match kind {
            "eof_empty" => drop(s),
            "reset" => rst(s),
            "eof_partial_head" => {
                let _ = rec.write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Le").await;
                drop(s);
            }
            "malformed_head" => {
                let _ = rec.write(&mut s, b"XTTP/1.1 200 OK\r\n\r\n").await;
                rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
            }
            "informational_101" => {
                let _ = rec
                    .write(&mut s, b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\nConnection: upgrade\r\n\r\n")
                    .await;
                rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
            }
            other => unreachable!("{other}"),
        }
    })
    .await
}

/// **Chaos 1 (and the head faults beside it).** After the upstream has read the whole request:
/// a POST is Indeterminate `WriteUnconfirmed` with the cause token; a caller-declared-idempotent GET
/// is Retryable `ConnectionLost`; an UNDECLARED GET is Indeterminate (the §23.7.2 cost, asserted);
/// an operator-declared method (`IDEMPOTENT_METHODS`) is Retryable. The upstream received EXACTLY one
/// request each time — the engine never re-sends.
#[tokio::test]
async fn sent_no_head_is_indeterminate_unless_declared_idempotent() {
    for cause in [
        http_cause::EOF_EMPTY,
        http_cause::RESET,
        http_cause::EOF_PARTIAL_HEAD,
        http_cause::MALFORMED_HEAD,
        http_cause::INFORMATIONAL_101,
    ] {
        let up = fault_upstream(cause).await;
        let d = daemon(vars(&[
            ("FERRO_UPSTREAMS", "w,op".into()),
            ("FERRO_UPSTREAM_W_ORIGIN", origin(up.addr)),
            ("FERRO_UPSTREAM_OP_ORIGIN", origin(up.addr)),
            ("FERRO_UPSTREAM_OP_IDEMPOTENT_METHODS", "GET".into()),
        ]));
        let mut c = d.client().await;
        let cases: [(HttpRequest, bool); 4] = [
            (post("w", b"charge"), false),
            (idempotent_get("w"), true),
            (request("w", "GET", "/"), false),
            (request("op", "GET", "/"), true),
        ];
        for (i, (req, idem)) in cases.into_iter().enumerate() {
            let before = up.rec.requests().len();
            let r = exchange(&mut c, 10 + i as u32, &req).await;
            assert!(r.head.is_none(), "{cause}: no HEAD was sent");
            if idem {
                r.assert_error(errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH, cause);
            } else {
                r.assert_error(
                    errc::WRITE_UNCONFIRMED,
                    errc::WRITE_UNCONFIRMED_BRANCH,
                    cause,
                );
            }
            assert_eq!(
                up.rec.requests().len(),
                before + 1,
                "{cause} case {i}: received exactly once"
            );
        }
        // Never a second request on any connection: every request had its own, and only one.
        assert_eq!(
            up.rec.conns(),
            4,
            "{cause}: one connection per request, none re-sent"
        );
    }
}

/// **The read count that tells `eof_empty` from `eof_partial_head` starts at dispatch** (review
/// F-4): on a REUSED keep-alive connection whose first exchange read a whole response, a second
/// request closed with zero response bytes is `eof_empty`, not `eof_partial_head`.
#[tokio::test]
async fn eof_empty_on_a_reused_connection_counts_reads_from_dispatch() {
    let up = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            let _ = rec.write(&mut s, OK_HELLO).await;
        }
        if rec.read_request(&mut s).await.is_some() {
            drop(s); // FIN, zero response bytes
        }
    })
    .await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "r".into()),
        ("FERRO_UPSTREAM_R_ORIGIN", origin(up.addr)),
    ]));
    let mut c = d.client().await;
    exchange(&mut c, 1, &post("r", b"one")).await.done();
    let r = exchange(&mut c, 2, &post("r", b"two")).await;
    r.assert_error(
        errc::WRITE_UNCONFIRMED,
        errc::WRITE_UNCONFIRMED_BRANCH,
        http_cause::EOF_EMPTY,
    );
    assert_eq!(
        up.rec.conns(),
        1,
        "the second request rode the reused connection"
    );
}

/// **Chaos 2.** The upstream resets after reading part of a large body: the POST is Indeterminate
/// (`reset`, or `write` if the write side failed first — same class, same fate, P14), and no second
/// request exists anywhere.
#[tokio::test]
async fn a_reset_mid_body_is_indeterminate_and_never_re_sent() {
    let up = upstream(|mut s, rec| async move {
        let mut b = vec![0u8; 64 * 1024];
        if let Ok(n) = s.read(&mut b).await {
            rec.bytes_in.fetch_add(n as u64, Ordering::SeqCst);
        }
        rst(s);
    })
    .await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "big".into()),
        ("FERRO_UPSTREAM_BIG_ORIGIN", origin(up.addr)),
    ]));
    let mut c = d.client().await;
    let r = exchange(&mut c, 1, &post("big", &vec![7u8; 8 * 1024 * 1024])).await;
    let ep = r.error();
    assert_eq!(
        (ep.code, ep.branch),
        (errc::WRITE_UNCONFIRMED, errc::WRITE_UNCONFIRMED_BRANCH)
    );
    assert!(
        [http_cause::RESET, http_cause::WRITE].contains(&ep.detail.as_deref().unwrap()),
        "{ep:?}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(up.rec.conns(), 1, "never a second connection");
}

// =================================================================================================
// CANCEL and deadlines in each phase (§23.7.1, §23.11; chaos 11, 5)
// =================================================================================================

/// An upstream that reads the request and then, per `then`, holds (no head), or sends a head and
/// part of a body and holds — until the engine closes the connection.
async fn holding_upstream(then: &'static str) -> Upstream {
    upstream(move |mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        if then == "head" {
            let _ = rec
                .write(
                    &mut s,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\npartial",
                )
                .await;
        }
        rec.hold_until_closed(&mut s, Duration::from_secs(20)).await;
    })
    .await
}

/// **Chaos 11: `CANCEL` before dispatch / after dispatch / after `HEAD`.** Before: `Cancelled`, the
/// upstream received nothing. After dispatch with no head: a POST is Indeterminate (cause token
/// `cancelled`), a declared-idempotent GET is `Cancelled`. After `HEAD`: `Cancelled`. In every case
/// after dispatch the connection is DISCARDED, never returned (the upstream sees it closed).
#[tokio::test]
async fn cancel_in_each_phase() {
    // Before dispatch: a resolver that never answers.
    let up = holding_upstream("none").await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "slow".into()),
            ("FERRO_UPSTREAM_SLOW_ORIGIN", "http://never.test".into()),
            ("FERRO_UPSTREAM_SLOW_ADDRESS_CLASSES", "loopback".into()),
        ]),
        Arc::new(PendingResolver),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;
    c.send_request(
        1,
        service::HTTP,
        method_http::REQUEST,
        post("slow", b"x").encode(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    c.cancel(1).await;
    collect(&mut c, 1).await.assert_cancelled();

    // After dispatch, sent, no head.
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "h".into()),
        ("FERRO_UPSTREAM_H_ORIGIN", origin(up.addr)),
    ]));
    let mut c = d.client().await;
    for (rid, req, idem) in [
        (2, post("h", b"charge"), false),
        (3, idempotent_get("h"), true),
    ] {
        let before = up.rec.requests().len();
        c.send_request(rid, service::HTTP, method_http::REQUEST, req.encode())
            .await;
        wait_for(|| up.rec.requests().len() > before).await;
        c.cancel(rid).await;
        let r = collect(&mut c, rid).await;
        assert!(r.head.is_none());
        if idem {
            r.assert_cancelled();
        } else {
            r.assert_error(
                errc::WRITE_UNCONFIRMED,
                errc::WRITE_UNCONFIRMED_BRANCH,
                http_cause::CANCELLED,
            );
        }
    }
    wait_for(|| up.rec.closed_by_peer() == 2).await;

    // After HEAD.
    let up = holding_upstream("head").await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "h".into()),
        ("FERRO_UPSTREAM_H_ORIGIN", origin(up.addr)),
    ]));
    let mut c = d.client().await;
    c.send_request(
        4,
        service::HTTP,
        method_http::REQUEST,
        post("h", b"x").encode(),
    )
    .await;
    let f = c.recv().await;
    assert_eq!(f.header.method, method_http::HEAD);
    let f = c.recv().await;
    assert_eq!(f.header.method, method_http::BODY);
    c.cancel(4).await;
    let r = collect(&mut c, 4).await;
    r.assert_cancelled();
    wait_for(|| up.rec.closed_by_peer() == 1).await;
    assert_eq!(up.rec.requests().len(), 1);
}

/// **Deadlines, per phase, and the ceiling.** Before dispatch: Retryable `PoolTimeout`
/// (`deadline`). Sent with no head: a POST is Indeterminate (`timeout`), a declared GET is
/// NonRetryable `QueryTimeout`. After the head: `QueryTimeout` (`timeout`), and an idle body past
/// `read_timeout_ms` is `QueryTimeout` (`read_idle`). A request asking for more than the upstream's
/// `TIMEOUT_MS` is held to it (§23.8.4).
#[tokio::test]
async fn deadlines_in_each_phase_and_the_ceiling() {
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "slow".into()),
            ("FERRO_UPSTREAM_SLOW_ORIGIN", "http://never.test".into()),
        ]),
        Arc::new(PendingResolver),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;
    let mut p = post("slow", b"x");
    p.timeout_ms = Some(200);
    exchange(&mut c, 1, &p).await.assert_error(
        errc::POOL_TIMEOUT,
        errc::POOL_TIMEOUT_BRANCH,
        http_cause::DEADLINE,
    );

    let none = holding_upstream("none").await;
    let head = holding_upstream("head").await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "none,head,capped".into()),
        ("FERRO_UPSTREAM_NONE_ORIGIN", origin(none.addr)),
        ("FERRO_UPSTREAM_HEAD_ORIGIN", origin(head.addr)),
        ("FERRO_UPSTREAM_CAPPED_ORIGIN", origin(none.addr)),
        ("FERRO_UPSTREAM_CAPPED_TIMEOUT_MS", "300".into()),
    ]));
    let mut c = d.client().await;

    let mut p = post("none", b"charge");
    p.timeout_ms = Some(300);
    exchange(&mut c, 2, &p).await.assert_error(
        errc::WRITE_UNCONFIRMED,
        errc::WRITE_UNCONFIRMED_BRANCH,
        http_cause::TIMEOUT,
    );
    let mut g = idempotent_get("none");
    g.timeout_ms = Some(300);
    exchange(&mut c, 3, &g).await.assert_error(
        errc::QUERY_TIMEOUT,
        errc::QUERY_TIMEOUT_BRANCH,
        http_cause::TIMEOUT,
    );

    let mut p = post("head", b"charge");
    p.timeout_ms = Some(300);
    let r = exchange(&mut c, 4, &p).await;
    assert!(r.head.is_some());
    r.assert_error(
        errc::QUERY_TIMEOUT,
        errc::QUERY_TIMEOUT_BRANCH,
        http_cause::TIMEOUT,
    );

    let mut p = post("head", b"charge");
    p.read_timeout_ms = Some(200);
    let r = exchange(&mut c, 5, &p).await;
    assert_eq!(r.body, b"partial");
    r.assert_error(
        errc::QUERY_TIMEOUT,
        errc::QUERY_TIMEOUT_BRANCH,
        http_cause::READ_IDLE,
    );

    let started = Instant::now();
    let mut g = idempotent_get("capped");
    g.timeout_ms = Some(60_000);
    exchange(&mut c, 6, &g).await.assert_error(
        errc::QUERY_TIMEOUT,
        errc::QUERY_TIMEOUT_BRANCH,
        http_cause::TIMEOUT,
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the upstream's TIMEOUT_MS caps the request's own: {:?}",
        started.elapsed()
    );
}

async fn wait_for(cond: impl Fn() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "condition never held"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// =================================================================================================
// Body failures after the head, and MAX_RESPONSE_BYTES
// =================================================================================================

/// **After the head the transport fate is known**: a body that ends early (`body_eof`) or is reset
/// (`body_reset`) is Retryable `ConnectionLost` for a declared-idempotent request and NonRetryable
/// `ResponseIncomplete` otherwise; bad chunked framing is `body_framing`; a response past
/// `MAX_RESPONSE_BYTES` is `ResponseIncomplete` (`max_response_bytes`) either way.
#[tokio::test]
async fn body_failures_after_the_head() {
    async fn up(kind: &'static str) -> Upstream {
        upstream(move |mut s, rec| async move {
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            match kind {
                "eof" => {
                    let _ = rec
                        .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
                        .await;
                    drop(s);
                }
                "reset" => {
                    let _ = rec
                        .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
                        .await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    rst(s);
                }
                "framing" => {
                    let _ = rec
                        .write(&mut s, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nnot-hex\r\n")
                        .await;
                    rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
                }
                "big" => {
                    let body = vec![b'b'; 5000];
                    let _ = rec
                        .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 5000\r\n\r\n")
                        .await;
                    let _ = rec.write(&mut s, &body).await;
                    rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
                }
                other => unreachable!("{other}"),
            }
        })
        .await
    }
    for (kind, cause) in [
        ("eof", http_cause::BODY_EOF),
        ("reset", http_cause::BODY_RESET),
        ("framing", http_cause::BODY_FRAMING),
    ] {
        for idem in [false, true] {
            let u = up(kind).await;
            let d = daemon(vars(&[
                ("FERRO_UPSTREAMS", "b".into()),
                ("FERRO_UPSTREAM_B_ORIGIN", origin(u.addr)),
            ]));
            let mut c = d.client().await;
            let req = if idem {
                idempotent_get("b")
            } else {
                post("b", b"x")
            };
            let r = exchange(&mut c, 1, &req).await;
            assert!(r.head.is_some(), "{kind}: the head was delivered");
            if idem {
                r.assert_error(errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH, cause);
            } else {
                r.assert_error(
                    errc::RESPONSE_INCOMPLETE,
                    errc::RESPONSE_INCOMPLETE_BRANCH,
                    cause,
                );
            }
        }
    }
    let u = up("big").await;
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "b".into()),
        ("FERRO_UPSTREAM_B_ORIGIN", origin(u.addr)),
        ("FERRO_UPSTREAM_B_MAX_RESPONSE_BYTES", "1000".into()),
    ]));
    let mut c = d.client().await;
    for (rid, req) in [(1, idempotent_get("b")), (2, post("b", b"x"))] {
        exchange(&mut c, rid, &req).await.assert_error(
            errc::RESPONSE_INCOMPLETE,
            errc::RESPONSE_INCOMPLETE_BRANCH,
            http_cause::MAX_RESPONSE_BYTES,
        );
    }
}

// =================================================================================================
// Head limits at the credit floor (§23.9.1, chaos 17)
// =================================================================================================

/// A response head of exactly `total` bytes by the engine's measure (§23.9.1): the status line,
/// `content-length: 0`, one padding field, the blank line.
const PAD_PREFIX: &str = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nx-pad: ";

fn head_of_size(total: usize) -> Vec<u8> {
    let suffix = "\r\n\r\n";
    let mut h = PAD_PREFIX.as_bytes().to_vec();
    h.resize(total - suffix.len(), b'a');
    h.extend_from_slice(suffix.as_bytes());
    assert_eq!(h.len(), total);
    h
}

/// **Chaos 17.** With `credit_bytes` at its validated floor (`MAX_FRAME_PAYLOAD`), a response
/// carrying the MAXIMAL permitted head (256 KiB by the engine's exact measure) is delivered, and a
/// head ONE BYTE over is `oversize_head` — the engine's check, since `hyper`'s `max_buf_size` is not
/// exact (P15). A head of 2 × the limit, which `hyper` itself refuses, and 257 fields
/// (`max_headers`) are `oversize_head` too — the first through `hyper`'s error TEXT, which pins it
/// (P14: an upgrade that changes the text turns this into `malformed_head` and fails here).
#[tokio::test]
async fn head_limits_are_exact_at_the_credit_floor() {
    const MAX: usize = 256 * 1024;
    let mut fields = String::from("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n");
    for i in 1..=256 {
        fields.push_str(&format!("x-f{i}: v\r\n"));
    }
    fields.push_str("\r\n");
    let fields: &'static [u8] = Box::leak(fields.into_bytes().into_boxed_slice());
    let exact: &'static [u8] = Box::leak(head_of_size(MAX).into_boxed_slice());
    let over: &'static [u8] = Box::leak(head_of_size(MAX + 1).into_boxed_slice());
    let double: &'static [u8] = Box::leak(head_of_size(2 * MAX).into_boxed_slice());
    for (what, resp, ok) in [
        ("exact", exact, true),
        ("one over", over, false),
        ("twice", double, false),
        ("257 fields", fields, false),
    ] {
        let u = upstream(move |mut s, rec| async move {
            if rec.read_request(&mut s).await.is_some() {
                let _ = rec.write(&mut s, resp).await;
                rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
            }
        })
        .await;
        let d = daemon_with(
            vars(&[
                ("FERRO_UPSTREAMS", "h".into()),
                ("FERRO_UPSTREAM_H_ORIGIN", origin(u.addr)),
            ]),
            StaticResolver::new(),
            Arc::new(TcpConnect),
            |c| c.credit_bytes = MAX_FRAME_PAYLOAD,
        );
        let mut c = d.client().await;
        let r = exchange(&mut c, 1, &idempotent_get("h")).await;
        if ok {
            let head = r
                .head
                .as_ref()
                .unwrap_or_else(|| panic!("{what}: {:?}", r.end));
            assert_eq!(
                header(head, "x-pad")[0].len(),
                MAX - PAD_PREFIX.len() - 4,
                "{what}"
            );
            r.done();
        } else {
            assert!(r.head.is_none(), "{what}: no HEAD");
            r.assert_error(
                errc::CONNECTION_LOST,
                errc::CONNECTION_LOST_BRANCH,
                http_cause::OVERSIZE_HEAD,
            );
        }
    }
}

// =================================================================================================
// Credit backpressure (§23.6 step 8, P7 through the engine; chaos 6's first half)
// =================================================================================================

/// **The engine stops reading when credit is exhausted, so the upstream sees TCP backpressure** —
/// measured with the stall probe of §22.2 (bj) as spike F1a calibrated it: the client READS every
/// frame that arrives (so the Unix socket is never what stalls) but sends no `WINDOW_UPDATE`; it
/// must receive exactly the credit's worth of frames and no more, the upstream's written count must
/// settle within a bound of what was delivered AND then not move for a 3 s hold (a credit-ignoring
/// read-ahead that is merely SLOW moves during the hold), and replenished credit must resume it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credit_exhaustion_backpressures_the_upstream() {
    const CREDIT_FRAMES: u32 = 4;
    const TOTAL: u64 = 512 * 1024 * 1024;
    // Measured locally: ~0.5 MB past the credit (the engine's socket buffer is NOT pinned, unlike
    // P7's client, so the kernel's receive buffer is part of the honest excess).
    const BOUND: u64 = 8 * 1024 * 1024;
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_send_buffer_size(128 * 1024).unwrap();
    sock.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = sock.listen(8).unwrap();
    let up = upstream_on(listener, |mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {TOTAL}\r\n\r\n");
        if rec.write(&mut s, head.as_bytes()).await.is_err() {
            return;
        }
        let piece = vec![b'z'; 64 * 1024];
        let mut sent = 0;
        while sent < TOTAL {
            if rec.write(&mut s, &piece).await.is_err() {
                return;
            }
            sent += piece.len() as u64;
        }
    })
    .await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "s".into()),
            ("FERRO_UPSTREAM_S_ORIGIN", origin(up.addr)),
        ]),
        StaticResolver::new(),
        Arc::new(TcpConnect),
        |c| c.credit_frames = CREDIT_FRAMES,
    );
    let mut c = d.client().await;
    c.send_request(
        1,
        service::HTTP,
        method_http::REQUEST,
        idempotent_get("s").encode(),
    )
    .await;

    // Read whatever arrives, never replenishing.
    let mut frames = 0u32;
    let mut delivered = 0u64;
    while let Some(f) = c.recv_or_none(Duration::from_millis(500)).await {
        assert_eq!(
            f.header.flags & flags::END,
            0,
            "no terminal while streaming"
        );
        frames += 1;
        if f.header.method == method_http::BODY {
            delivered += HttpBody::decode(&f.payload).unwrap().chunk.len() as u64;
        }
    }
    assert_eq!(
        frames, CREDIT_FRAMES,
        "exactly the credit's worth of frames (HEAD included)"
    );

    // Settle, then hold: the upstream must not move at all for 3 s.
    let settled = settle(&up.rec).await;
    let excess = settled.saturating_sub(delivered);
    eprintln!("F4a P7: delivered {delivered} B, upstream wrote {settled} B, excess {excess} B");
    assert!(
        excess <= BOUND,
        "{excess} B written past the credit (bound {BOUND} B)"
    );
    let hold_start = Instant::now();
    while hold_start.elapsed() < Duration::from_secs(3) {
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            up.rec.written(),
            settled,
            "the upstream moved during the hold"
        );
        assert!(
            c.recv_or_none(Duration::from_millis(1)).await.is_none(),
            "a frame past credit"
        );
    }

    // Replenish: frames flow again and the upstream resumes.
    c.window_update(1, 16, 16 * 256 * 1024).await;
    let mut more = 0;
    while more < 16 {
        let f = c.recv().await;
        assert_eq!(f.header.method, method_http::BODY);
        more += 1;
    }
    wait_for(|| up.rec.written() > settled).await;
    c.cancel(1).await;
    // Drain to the terminal (frames already granted may precede it).
    loop {
        let f = c.recv().await;
        if f.header.flags & flags::END != 0 {
            assert!(matches!(
                Outcome::decode(&f.payload).unwrap(),
                Outcome::Cancelled
            ));
            break;
        }
        c.window_update(1, 1, f.payload.len() as u32).await;
    }
}

async fn settle(rec: &Rec) -> u64 {
    let start = Instant::now();
    let mut last = rec.written();
    let mut still = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = rec.written();
        if now != last {
            last = now;
            still = Instant::now();
        } else if still.elapsed() >= Duration::from_millis(300) {
            return now;
        }
    }
    panic!("the upstream never stalled (wrote {} B)", rec.written());
}

// =================================================================================================
// Configuration states and the feature bit
// =================================================================================================

/// **`HELLO_ACK` advertises `HTTP` only when it is served**: a daemon-wide configuration error
/// disables the whole service (§23.3.1) — the bit is clear, and a request is refused
/// `forbidden_upstream`, never the daemon. (No `FERRO_UPSTREAMS` at all is `http_route_it`'s case:
/// bit clear, `Unsupported`.)
#[tokio::test]
async fn a_disabled_service_is_not_advertised_and_refuses() {
    let up = responder(OK_HELLO).await;
    let cfg = HttpConfig::load(
        vars(&[
            ("FERRO_UPSTREAMS", "api".into()),
            ("FERRO_UPSTREAM_API_ORIGIN", origin(up.addr)),
            ("FERRO_HTTP_NOT_A_KEY", "1".into()),
        ])
        .into_iter()
        .map(|(k, v)| (k.into(), v.into())),
        &|_| Err(io::Error::from(io::ErrorKind::NotFound)),
    );
    assert!(cfg.service_disabled());
    let engine = Arc::new(HttpEngine::new(Arc::new(cfg)));
    let config = Config::default();
    let registry = PoolRegistry::build_with_http(&config, Some(engine));
    let tx = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    let server =
        TestServer::spawn_with_factory_and_config(BootEpoch(3), config, registry, tx, factory);
    let mut c = server.connect().await;
    let hello = c.hello(1).await;
    assert_eq!(hello.ack.features & u32::from(feature_engine::HTTP), 0);
    exchange(&mut c, 2, &request("api", "GET", "/"))
        .await
        .assert_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            http_cause::FORBIDDEN_UPSTREAM,
        );
    assert_eq!(up.rec.conns(), 0);
}

/// An `https` upstream was `Unsupported` until TLS landed (F4a); from slice M6-F5a it is SERVED, so
/// it is dialled like any other: nothing listens on port 1, so `connect_refused`, never
/// `Unsupported`. TLS itself is `http_tls_it.rs`'s.
#[tokio::test]
async fn an_https_upstream_is_served_from_f5a() {
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "tls".into()),
        ("FERRO_UPSTREAM_TLS_ORIGIN", "https://127.0.0.1:1".into()),
    ]));
    let mut c = d.client().await;
    let r = exchange(&mut c, 1, &request("tls", "GET", "/")).await;
    let ep = r.error();
    assert_eq!(
        (ep.code, ep.detail.as_deref()),
        (
            errc::UPSTREAM_UNAVAILABLE,
            Some(http_cause::CONNECT_REFUSED)
        ),
        "{ep:?}"
    );
}

// =================================================================================================
// The wire half of F3's property gate (§23.4.2, owed to F4 by §22.2 (cw))
// =================================================================================================

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.below(v.len())]
    }
}

/// Path and query fragments, most of them one byte from a refusal.
const FRAGMENTS: &[&str] = &[
    "api",
    "v1",
    "x",
    "a-b",
    "_",
    "~u",
    "%41",
    "%2F",
    "%2f",
    "%2e",
    ".",
    "..",
    "...",
    ";v=1",
    "!",
    "$",
    "&",
    "'",
    "(",
    ")",
    "*",
    "+",
    ",",
    "=",
    ":",
    "@",
    "%25",
    "%00",
    "%c3%a9",
    "%7e",
    "%3B",
    "%3F",
    "%20",
    "%7F",
    "?q=1",
    "?a=b&c=%22",
    "?",
    "x.y",
    "%",
    "%zz",
    "/",
    "//",
];

/// **The wire half of the §23.4.2 property gate.** For every generated request the engine either
/// refuses it before dialling — exactly when the validator refuses it — or sends it, and then the
/// request line the upstream RECEIVED carries the accepted target byte for byte, `Host` occurs once
/// and is exactly the upstream authority, and every kept header arrives with its value byte for
/// byte. This is where a normalisation in the `http` crate or in `hyper` would show (the engine
/// refuses such a target rather than rewrite it — and this gate says it never has to). Both classes
/// must be substantial, so a generator that produced only one could not pass vacuously.
#[tokio::test]
async fn the_wire_half_of_the_property_gate() {
    let up = responder(b"HTTP/1.1 204 No Content\r\n\r\n").await;
    let env = vars(&[
        ("FERRO_UPSTREAMS", "g,gp,gu".into()),
        ("FERRO_UPSTREAM_G_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_G_ALLOW_PATHS", "/api/,/v1".into()),
        ("FERRO_UPSTREAM_GP_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_GP_PATH_PARAMS", "allow".into()),
        ("FERRO_UPSTREAM_GU_ORIGIN", origin(up.addr)),
        ("FERRO_UPSTREAM_GU_PATH_ENCODING", "utf8".into()),
    ]);
    let oracle = HttpConfig::load(
        env.clone().into_iter().map(|(k, v)| (k.into(), v.into())),
        &|_| Err(io::Error::from(io::ErrorKind::NotFound)),
    );
    let d = daemon(env);
    let mut c = d.client().await;
    let authority = up.addr.to_string();
    let mut rng = Rng(0x00F4_A5EE_D000_0001);
    let (mut accepted, mut refused) = (0, 0);
    for i in 0..1500u32 {
        let upstream = *rng.pick(&["g", "gp", "gu"]);
        let mut target = String::from(if rng.below(2) == 0 { "/api/" } else { "/" });
        for _ in 0..rng.below(6) {
            let fragment: &&str = rng.pick(FRAGMENTS);
            target.push_str(fragment);
            if rng.below(3) == 0 {
                target.push('/');
            }
        }
        let headers: Vec<HttpHeaderField> = (0..rng.below(3))
            .map(|k| HttpHeaderField {
                name: format!("X-Gen-{k}"),
                value: (0..rng.below(12))
                    .map(|_| *rng.pick(&[b'a', b' ', b'\t', 0x80, 0xff, b'"', b'%', b'z']))
                    .collect(),
            })
            .collect();
        let req = HttpRequest {
            headers: headers.clone(),
            ..request(upstream, "GET", &target)
        };
        let pairs: Vec<(String, Vec<u8>)> = headers
            .iter()
            .map(|h| (h.name.clone(), h.value.clone()))
            .collect();
        let verdict = ferro_http::validate(
            &oracle,
            Some(nix::unistd::geteuid().as_raw()),
            &ferro_http::Request {
                upstream,
                method: "GET",
                target: &target,
                origin: None,
                headers: &pairs,
                body: None,
                idempotent: None,
                decode: false,
            },
        );
        let before = up.rec.requests().len();
        let r = exchange(&mut c, 1000 + i, &req).await;
        match verdict {
            Err(refusal) => {
                refused += 1;
                let ep = r.error();
                assert_eq!(ep.code, errc::FORBIDDEN, "{target:?}: {ep:?}");
                assert_eq!(
                    ep.detail.as_deref(),
                    Some(ferro_http::fate::policy_cause_token(refusal.cause()))
                );
                assert_eq!(
                    up.rec.requests().len(),
                    before,
                    "{target:?}: refused yet sent"
                );
            }
            Ok(_) => {
                accepted += 1;
                assert!(
                    matches!(r.end, Outcome::Ok(_)),
                    "{target:?}: the validator accepted it and the engine did not send it: {:?}",
                    r.end
                );
                let seen = up.rec.requests().pop().expect("received");
                assert_eq!(
                    seen.request_line(),
                    format!("GET {target} HTTP/1.1"),
                    "the target on the wire is the accepted target, byte for byte"
                );
                let hosts: Vec<String> = seen
                    .header_lines()
                    .iter()
                    .filter(|l| l.to_ascii_lowercase().starts_with("host:"))
                    .map(|l| l[5..].trim().to_string())
                    .collect();
                assert_eq!(
                    hosts,
                    std::slice::from_ref(&authority),
                    "{target:?}: exactly one Host"
                );
                for h in &headers {
                    let line = [h.name.as_bytes(), b": ", &h.value, b"\r\n"].concat();
                    let trimmed_value_line = [h.name.as_bytes(), b":"].concat();
                    assert!(
                        seen.raw.windows(line.len()).any(|w| w == line.as_slice())
                            || (h.value.is_empty()
                                && seen
                                    .raw
                                    .windows(trimmed_value_line.len())
                                    .any(|w| w == trimmed_value_line.as_slice())),
                        "{target:?}: header {} not sent byte for byte",
                        h.name
                    );
                }
            }
        }
    }
    eprintln!("F4a wire gate: {accepted} accepted, {refused} refused");
    assert!(
        accepted >= 300 && refused >= 300,
        "{accepted} accepted / {refused} refused"
    );
}

// =================================================================================================
// Review round (M6-F4a): the findings, each pinned by a test that failed before its fix
// =================================================================================================

/// A connector that records every peer it was asked to dial, then dials it.
struct RecConnect(Arc<Mutex<Vec<SocketAddr>>>);
impl Connect for RecConnect {
    fn connect<'a>(
        &'a self,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        self.0.lock().unwrap().push(peer);
        Box::pin(async move {
            let s = TcpStream::connect(peer).await?;
            Ok(Box::new(s) as BoxIo)
        })
    }
}

/// A connector that never connects (only the connect bound can end it).
struct NeverConnect;
impl Connect for NeverConnect {
    fn connect<'a>(
        &'a self,
        _peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

/// A connector whose connect fails with one I/O error kind.
struct FailConnect(io::ErrorKind);
impl Connect for FailConnect {
    fn connect<'a>(
        &'a self,
        _peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        let k = self.0;
        Box::pin(async move { Err(io::Error::from(k)) })
    }
}

/// **Review P2 (finding 2): a mixed answer never DIALS a refused address.** The earlier test could
/// not tell: `fd00:ec2::254` simply fails to connect on a test host and the dial falls through, so
/// "dial every resolved address, refused ones first" passed it. A recording connector sees every
/// peer the engine dials: exactly the admitted one.
#[tokio::test]
async fn a_mixed_answer_never_dials_the_refused_address() {
    let up2 = upstream_on(
        TcpListener::bind("127.0.0.2:0").await.unwrap(),
        |mut s, rec| async move {
            while rec.read_request(&mut s).await.is_some() {
                let _ = rec.write(&mut s, OK_HELLO).await;
            }
        },
    )
    .await;
    let resolver = StaticResolver::new();
    resolver.set(
        "mixed.test",
        vec![vec![
            ip("fd00:ec2::254"),
            ip("169.254.169.254"),
            ip("127.0.0.2"),
        ]],
    );
    let peers = Arc::new(Mutex::new(Vec::new()));
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "mixed".into()),
            (
                "FERRO_UPSTREAM_MIXED_ORIGIN",
                format!("http://mixed.test:{}", up2.addr.port()),
            ),
            ("FERRO_UPSTREAM_MIXED_ADDRESS_CLASSES", "loopback".into()),
        ]),
        resolver,
        Arc::new(RecConnect(peers.clone())),
        |_| {},
    );
    let mut c = d.client().await;
    let r = exchange(&mut c, 1, &request("mixed", "GET", "/")).await;
    assert_eq!(r.body, b"hello");
    assert_eq!(
        peers.lock().unwrap().clone(),
        vec![SocketAddr::new(ip("127.0.0.2"), up2.addr.port())],
        "only the checked, admitted address is ever dialled"
    );
}

/// **Review P3 (finding 1): a head that arrives before ANY byte of the request was sent is not an
/// answer to it.** The upstream speaks first — a 101, a 200, or (with `MAX_RESPONSE_BYTES=10`) a
/// head already past the cap — on a socket whose first write is slow; `hyper` reads that head once
/// the request is queued. The request never left, so a POST is Retryable `unsent_closed` — never
/// Indeterminate, never `Ok` — and the upstream receives zero bytes.
#[tokio::test]
async fn a_head_before_any_byte_was_sent_is_unsent_not_indeterminate() {
    let heads: [(&str, &'static [u8]); 2] = [
        (
            "101",
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\nConnection: upgrade\r\n\r\n",
        ),
        ("200", b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"),
    ];
    for (what, head) in heads {
        for cap in [false, true] {
            let up = upstream(move |mut s, rec| async move {
                let _ = rec.write(&mut s, head).await;
                rec.hold_until_closed(&mut s, Duration::from_secs(8)).await;
            })
            .await;
            let mut env = vars(&[
                ("FERRO_UPSTREAMS", "f".into()),
                ("FERRO_UPSTREAM_F_ORIGIN", origin(up.addr)),
            ]);
            if cap {
                env.push(("FERRO_UPSTREAM_F_MAX_RESPONSE_BYTES".into(), "10".into()));
            }
            let d = daemon_with(
                env,
                StaticResolver::new(),
                Arc::new(FaultConnect(Fault::Delay(Duration::from_millis(400)))),
                |_| {},
            );
            let mut c = d.client().await;
            let r = exchange(&mut c, 1, &post("f", b"charge")).await;
            assert!(
                r.head.is_none(),
                "{what} cap={cap}: no HEAD for an unsent request"
            );
            r.assert_error(
                errc::CONNECTION_LOST,
                errc::CONNECTION_LOST_BRANCH,
                http_cause::UNSENT_CLOSED,
            );
            tokio::time::sleep(Duration::from_millis(700)).await;
            assert_eq!(
                up.rec.bytes_in.load(Ordering::SeqCst),
                0,
                "{what} cap={cap}: the request never reached the upstream"
            );
        }
    }
}

/// **Review P4 (finding 6): a head of EXACTLY the limit with an empty reason and no SP**
/// (`HTTP/1.1 200\r\n`, which `httparse` accepts) is delivered. The measure counted a space that
/// was never sent and refused it `oversize_head` — which makes a POST Indeterminate.
#[tokio::test]
async fn a_head_at_the_limit_with_no_reason_phrase_is_delivered() {
    const MAX: usize = 256 * 1024;
    let prefix = "HTTP/1.1 200\r\ncontent-length: 0\r\nx-pad: ";
    let mut h = prefix.as_bytes().to_vec();
    h.resize(MAX - 4, b'a');
    h.extend_from_slice(b"\r\n\r\n");
    assert_eq!(h.len(), MAX);
    let resp: &'static [u8] = Box::leak(h.into_boxed_slice());
    let u = upstream(move |mut s, rec| async move {
        if rec.read_request(&mut s).await.is_some() {
            let _ = rec.write(&mut s, resp).await;
            rec.hold_until_closed(&mut s, Duration::from_secs(10)).await;
        }
    })
    .await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "h".into()),
            ("FERRO_UPSTREAM_H_ORIGIN", origin(u.addr)),
        ]),
        StaticResolver::new(),
        Arc::new(TcpConnect),
        |c| c.credit_bytes = MAX_FRAME_PAYLOAD,
    );
    let mut c = d.client().await;
    let r = exchange(&mut c, 1, &post("h", b"x")).await;
    let head = r.head.as_ref().unwrap_or_else(|| panic!("{:?}", r.end));
    assert_eq!((head.status, head.reason.as_deref()), (200, Some(&b""[..])));
    assert_eq!(header(head, "x-pad")[0].len(), MAX - prefix.len() - 4);
    r.done();
}

/// **Review P1 (finding 4): a BODY parked on credit, then `CANCEL`, discards the connection** —
/// though the rest of the body is readable. `hyper` 1.11.1 DRAINS a dropped body when the remainder
/// is readable and returns the connection to keep-alive, so a connection the engine failed to
/// discard (or leaked) would stay open: one socket and one task per such cancel. The upstream must
/// see the close.
#[tokio::test]
async fn a_cancel_while_a_body_frame_waits_for_credit_discards_the_connection() {
    let up = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let _ = rec
            .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nAAAA")
            .await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let _ = rec.write(&mut s, b"BBBB").await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let _ = rec.write(&mut s, b"CCCC").await;
        rec.hold_until_closed(&mut s, Duration::from_secs(8)).await;
    })
    .await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "s".into()),
            ("FERRO_UPSTREAM_S_ORIGIN", origin(up.addr)),
        ]),
        StaticResolver::new(),
        Arc::new(TcpConnect),
        |c| c.credit_frames = 2,
    );
    let mut c = d.client().await;
    c.send_request(
        1,
        service::HTTP,
        method_http::REQUEST,
        idempotent_get("s").encode(),
    )
    .await;
    assert_eq!(c.recv().await.header.method, method_http::HEAD);
    assert_eq!(c.recv().await.header.method, method_http::BODY);
    // The next BODY is parked on credit (2 frames granted, 2 spent); the remainder is readable.
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.cancel(1).await;
    collect(&mut c, 1).await.assert_cancelled();
    wait_for(|| up.rec.closed_by_peer() == 1).await;
}

/// **Review P5 (finding 4): `CANCEL` racing the last body bytes.** Whichever wins, a `Cancelled`
/// exchange's connection is closed (left alone, `hyper` would drain the readable remainder and keep
/// it alive). Forty iterations on a multi-threaded runtime, so both orderings occur.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_racing_the_last_body_bytes_never_leaves_a_connection_open() {
    let mut cancelled = 0;
    for _ in 0..40 {
        let go = Arc::new(tokio::sync::Notify::new());
        let g = go.clone();
        let up = upstream(move |mut s, rec| {
            let g = g.clone();
            async move {
                if rec.read_request(&mut s).await.is_none() {
                    return;
                }
                let _ = rec
                    .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nAAAA")
                    .await;
                g.notified().await;
                let _ = rec.write(&mut s, b"BBBB").await;
                rec.hold_until_closed(&mut s, Duration::from_millis(1500))
                    .await;
            }
        })
        .await;
        let d = daemon(vars(&[
            ("FERRO_UPSTREAMS", "s".into()),
            ("FERRO_UPSTREAM_S_ORIGIN", origin(up.addr)),
        ]));
        let mut c = d.client().await;
        c.send_request(
            1,
            service::HTTP,
            method_http::REQUEST,
            idempotent_get("s").encode(),
        )
        .await;
        let _ = c.recv().await; // HEAD
        let _ = c.recv().await; // BODY "AAAA"
        go.notify_one();
        c.cancel(1).await;
        let end = loop {
            let f = c.recv().await;
            if f.header.flags & flags::END != 0 {
                break Outcome::decode(&f.payload).expect("terminal");
            }
        };
        if matches!(end, Outcome::Cancelled) {
            cancelled += 1;
            let t = Instant::now();
            while up.rec.closed_by_peer() == 0 && t.elapsed() < Duration::from_millis(1400) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                up.rec.closed_by_peer(),
                1,
                "a Cancelled exchange left its connection open"
            );
        }
    }
    assert!(
        cancelled >= 5,
        "the race was exercised ({cancelled} cancels)"
    );
}

fn bounded(name: &str, host: &str) -> Vec<(String, String)> {
    let n = name.to_uppercase();
    vec![
        ("FERRO_UPSTREAMS".into(), name.into()),
        (
            format!("FERRO_UPSTREAM_{n}_ORIGIN"),
            format!("http://{host}"),
        ),
        (
            format!("FERRO_UPSTREAM_{n}_CONNECT_TIMEOUT_MS"),
            "200".into(),
        ),
        (
            format!("FERRO_UPSTREAM_{n}_ADDRESS_CLASSES"),
            "loopback".into(),
        ),
    ]
}

fn assert_unavailable(r: &Reply, cause: &str) {
    r.assert_error(
        errc::UPSTREAM_UNAVAILABLE,
        errc::UPSTREAM_UNAVAILABLE_BRANCH,
        cause,
    );
}

/// **Review P6 and finding 5: the dial cells, end to end, and the connect bound.**
/// `connect_refused` (a dead port); `dns` (an unknown name, and a resolver that never answers —
/// ended by `CONNECT_TIMEOUT_MS`, not by the 10 s total deadline); `connect_timeout` (a connect that
/// never completes, ended by the same bound); `connect_unreachable`; each Retryable
/// `UpstreamUnavailable` with its token. And a socket that accepts part of a request and then fails
/// on the WRITE side is `write` (Indeterminate for a POST).
#[tokio::test]
async fn the_dial_cells_and_the_connect_bound() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = l.local_addr().unwrap();
    drop(l);
    let d = daemon(vars(&[
        ("FERRO_UPSTREAMS", "r,n".into()),
        ("FERRO_UPSTREAM_R_ORIGIN", origin(dead)),
        ("FERRO_UPSTREAM_N_ORIGIN", "http://nosuch.test".into()),
    ]));
    let mut c = d.client().await;
    assert_unavailable(
        &exchange(&mut c, 1, &post("r", b"x")).await,
        http_cause::CONNECT_REFUSED,
    );
    assert_unavailable(
        &exchange(&mut c, 2, &post("n", b"x")).await,
        http_cause::DNS,
    );

    let d = daemon_with(
        bounded("p", "slow.test"),
        Arc::new(PendingResolver),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;
    let t = Instant::now();
    assert_unavailable(
        &exchange(&mut c, 3, &post("p", b"x")).await,
        http_cause::DNS,
    );
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());

    let resolver = StaticResolver::new();
    resolver.set("slow.test", vec![vec![ip("127.0.0.1")]]);
    let d = daemon_with(
        bounded("t", "slow.test"),
        resolver.clone(),
        Arc::new(NeverConnect),
        |_| {},
    );
    let mut c = d.client().await;
    let t = Instant::now();
    assert_unavailable(
        &exchange(&mut c, 4, &post("t", b"x")).await,
        http_cause::CONNECT_TIMEOUT,
    );
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());

    let d = daemon_with(
        bounded("u", "slow.test"),
        resolver.clone(),
        Arc::new(FailConnect(io::ErrorKind::HostUnreachable)),
        |_| {},
    );
    let mut c = d.client().await;
    assert_unavailable(
        &exchange(&mut c, 5, &post("u", b"x")).await,
        http_cause::CONNECT_UNREACHABLE,
    );

    // `write`: the socket accepts 100 bytes of the request, then fails on the write side.
    let up = upstream(|mut s, rec| async move {
        rec.hold_until_closed(&mut s, Duration::from_secs(8)).await;
    })
    .await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "w".into()),
            ("FERRO_UPSTREAM_W_ORIGIN", origin(up.addr)),
        ]),
        StaticResolver::new(),
        Arc::new(FaultConnect(Fault::FailAfter(100))),
        |_| {},
    );
    let mut c = d.client().await;
    exchange(&mut c, 6, &post("w", &[7u8; 64 * 1024]))
        .await
        .assert_error(
            errc::WRITE_UNCONFIRMED,
            errc::WRITE_UNCONFIRMED_BRANCH,
            http_cause::WRITE,
        );
}

/// **Review R7 (finding 5): a failed dial forgets the cached DNS answer**, so the next dial
/// re-resolves instead of retrying a dead answer for `DNS_TTL_MS` (60 s by default).
#[tokio::test]
async fn a_failed_dial_forgets_the_cached_answer() {
    let up = responder(OK_HELLO).await;
    let port = up.addr.port();
    let resolver = StaticResolver::new();
    // 127.0.0.2:port has no listener (refused); 127.0.0.1:port is the upstream.
    resolver.set(
        "flaky.test",
        vec![vec![ip("127.0.0.2")], vec![ip("127.0.0.1")]],
    );
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "fl".into()),
            (
                "FERRO_UPSTREAM_FL_ORIGIN",
                format!("http://flaky.test:{port}"),
            ),
            ("FERRO_UPSTREAM_FL_ADDRESS_CLASSES", "loopback".into()),
        ]),
        resolver.clone(),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;
    assert_unavailable(
        &exchange(&mut c, 1, &request("fl", "GET", "/")).await,
        http_cause::CONNECT_REFUSED,
    );
    assert_eq!(
        exchange(&mut c, 2, &request("fl", "GET", "/")).await.body,
        b"hello"
    );
    assert_eq!(
        resolver.calls(),
        2,
        "the failed dial's answer was not reused"
    );
}

// -------------------------------------------------------------------------------------------------
// Pool rules (§23.8.1, §23.8.2), driven on the engine directly. Two ATTESTED uids are what the
// session hands the engine, and a test process cannot connect to a Unix socket as a second uid
// without privileges; the session-to-engine plumbing of the uid is pinned separately
// (`refusals_are_forbidden_with_their_cause_and_dial_nothing`, through `ALLOW_UIDS`).
// -------------------------------------------------------------------------------------------------

/// A sink that keeps every frame.
#[derive(Default)]
struct VecSink(Mutex<Vec<(SinkFrame, Vec<u8>)>>);
impl ResponseSink for VecSink {
    fn send<'a>(
        &'a self,
        frame: SinkFrame,
        payload: Vec<u8>,
        _deadline: tokio::time::Instant,
        _cancel: &'a tokio_util::sync::CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.0.lock().unwrap().push((frame, payload));
        Box::pin(async { Ok(()) })
    }
}

fn engine_for(env: Vec<(String, String)>) -> HttpEngine {
    let cfg = HttpConfig::load(env.into_iter().map(|(k, v)| (k.into(), v.into())), &|_| {
        Err(io::Error::from(io::ErrorKind::NotFound))
    });
    assert!(cfg.errors().is_empty());
    HttpEngine::new(Arc::new(cfg))
}

/// One exchange on the engine: its terminal and the `x-conn` the upstream stamped on the head.
async fn on_engine(e: &HttpEngine, req: &HttpRequest, uid: Option<u32>) -> (Terminal, Option<u64>) {
    let sink = VecSink::default();
    let cancel = tokio_util::sync::CancellationToken::new();
    let t = e.exchange(req, uid, Instant::now(), &cancel, &sink).await;
    let frames = sink.0.lock().unwrap();
    let conn = frames
        .iter()
        .filter(|(f, _)| *f == SinkFrame::Head)
        .find_map(|(_, p)| {
            let h = HttpHead::decode(p).unwrap();
            let v = header(&h, "x-conn").first()?.to_vec();
            std::str::from_utf8(&v).ok()?.parse().ok()
        });
    (t, conn)
}

/// A keep-alive upstream that stamps every response with its connection's accept index, delays
/// `/slow` by 200 ms, and adds `extra` header lines. Returns its address and its accept count.
async fn numbered(extra: &'static str) -> (SocketAddr, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(AtomicU64::new(0));
    let c = count.clone();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::spawn(async move {
                let rec = Rec::default();
                while let Some(seen) = rec.read_request(&mut s).await {
                    if seen.request_line().contains("/slow") {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nX-Conn: {n}\r\n{extra}Content-Length: 0\r\n\r\n"
                    );
                    if s.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, count)
}

fn conn_of(r: (Terminal, Option<u64>)) -> u64 {
    assert!(matches!(r.0, Terminal::Done(_)), "{:?}", r.0);
    r.1.expect("x-conn")
}

/// **`PARTITION=uid` (§23.8.1): two attested uids never share a connection** — the cross-tenant
/// isolation the key exists for — while each uid reuses its own; without the key they share. An
/// UNATTESTED peer on a partitioned upstream is refused `forbidden_upstream` (fail closed).
#[tokio::test]
async fn partition_by_uid_never_shares_a_connection_across_uids() {
    let (addr, _) = numbered("").await;
    let e = engine_for(vars(&[
        ("FERRO_UPSTREAMS", "part,shared".into()),
        ("FERRO_UPSTREAM_PART_ORIGIN", origin(addr)),
        ("FERRO_UPSTREAM_PART_PARTITION", "uid".into()),
        ("FERRO_UPSTREAM_SHARED_ORIGIN", origin(addr)),
    ]));
    let a1 = conn_of(on_engine(&e, &idempotent_get("part"), Some(1000)).await);
    let b1 = conn_of(on_engine(&e, &idempotent_get("part"), Some(2000)).await);
    assert_ne!(a1, b1, "uid 2000 must not ride uid 1000's connection");
    assert_eq!(
        conn_of(on_engine(&e, &idempotent_get("part"), Some(1000)).await),
        a1
    );
    assert_eq!(
        conn_of(on_engine(&e, &idempotent_get("part"), Some(2000)).await),
        b1
    );
    // Control: without the key, the second uid takes the first uid's idle connection.
    let s1 = conn_of(on_engine(&e, &idempotent_get("shared"), Some(1000)).await);
    assert_eq!(
        conn_of(on_engine(&e, &idempotent_get("shared"), Some(2000)).await),
        s1
    );
    match on_engine(&e, &idempotent_get("part"), None).await.0 {
        Terminal::Error(ep) => assert_eq!(
            (ep.code, ep.detail.as_deref()),
            (errc::FORBIDDEN, Some(http_cause::FORBIDDEN_UPSTREAM))
        ),
        other => panic!("an unattested peer on a partitioned upstream: {other:?}"),
    }
}

/// **`MAX_LIFETIME_MS`, `IDLE_TIMEOUT_MS` and the `Keep-Alive: timeout` clamp (§23.8.2), each
/// alone.** A connection past its lifetime is retired although it is barely idle; one idle past
/// `IDLE_TIMEOUT_MS` is retired although it is young; a server's `Keep-Alive: timeout=1` clamps the
/// idle limit to 0 s, so even an immediate request dials fresh. The controls reuse.
#[tokio::test]
async fn lifetime_idle_limit_and_the_keep_alive_clamp_each_retire_a_connection() {
    let (addr, _) = numbered("").await;
    let (ka, _) = numbered("Keep-Alive: timeout=1\r\n").await;
    let e = engine_for(vars(&[
        ("FERRO_UPSTREAMS", "life,idle,ka".into()),
        ("FERRO_UPSTREAM_LIFE_ORIGIN", origin(addr)),
        ("FERRO_UPSTREAM_LIFE_MAX_LIFETIME_MS", "600".into()),
        ("FERRO_UPSTREAM_IDLE_ORIGIN", origin(addr)),
        ("FERRO_UPSTREAM_IDLE_IDLE_TIMEOUT_MS", "200".into()),
        ("FERRO_UPSTREAM_KA_ORIGIN", origin(ka)),
    ]));
    let get = |u: &str| idempotent_get(u);
    // Lifetime: requests 150 ms apart (never idle long; the idle limit defaults to 15 s); reused
    // while younger than 600 ms, retired once older. The margins are ~300 ms on each side so a
    // loaded runner cannot push a reuse past the lifetime (review round 2).
    let l0 = conn_of(on_engine(&e, &get("life"), None).await);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(conn_of(on_engine(&e, &get("life"), None).await), l0);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(conn_of(on_engine(&e, &get("life"), None).await), l0);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_ne!(
        conn_of(on_engine(&e, &get("life"), None).await),
        l0,
        "past MAX_LIFETIME_MS"
    );
    // Idle limit: young, but idle 300 ms > 200 ms.
    let i0 = conn_of(on_engine(&e, &get("idle"), None).await);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(conn_of(on_engine(&e, &get("idle"), None).await), i0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_ne!(
        conn_of(on_engine(&e, &get("idle"), None).await),
        i0,
        "idle past IDLE_TIMEOUT_MS"
    );
    // The clamp: `timeout=1` → idle limit 0 → never reused.
    let k0 = conn_of(on_engine(&e, &get("ka"), None).await);
    assert_ne!(
        conn_of(on_engine(&e, &get("ka"), None).await),
        k0,
        "Keep-Alive: timeout=1 clamps the idle limit to 0"
    );
}

/// **`MAX_CONNECTIONS` bounds the connections that exist, and reuse is LIFO (§23.8.2).** With
/// `MAX_CONNECTIONS=1` two concurrent requests share ONE connection — the second waits for the
/// first's (M6-F6; until F6 the key bounded only idle retention, and the two opened two). With room
/// for both, the one returned LAST (the slow exchange's) is the one the next request takes.
#[tokio::test]
async fn idle_retention_is_bounded_and_reuse_is_lifo() {
    let (addr, count) = numbered("").await;
    let e = engine_for(vars(&[
        ("FERRO_UPSTREAMS", "one,many".into()),
        ("FERRO_UPSTREAM_ONE_ORIGIN", origin(addr)),
        ("FERRO_UPSTREAM_ONE_MAX_CONNECTIONS", "1".into()),
        ("FERRO_UPSTREAM_MANY_ORIGIN", origin(addr)),
    ]));
    let slow = |u: &str| HttpRequest {
        idempotent: Some(true),
        ..request(u, "GET", "/slow")
    };
    let (r1, r2) = (slow("one"), idempotent_get("one"));
    let (a, b) = tokio::join!(on_engine(&e, &r1, None), on_engine(&e, &r2, None));
    assert_eq!(
        conn_of(a),
        conn_of(b),
        "the second waited for the one connection"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(e.idle_connections("one"), 1);
    assert_eq!(e.connections("one"), 1);

    let before = count.load(Ordering::SeqCst);
    let (r1, r2) = (slow("many"), idempotent_get("many"));
    let (s, f) = tokio::join!(on_engine(&e, &r1, None), on_engine(&e, &r2, None));
    let (s, f) = (conn_of(s), conn_of(f));
    assert_ne!(s, f);
    assert_eq!(count.load(Ordering::SeqCst), before + 2);
    assert_eq!(e.idle_connections("many"), 2);
    assert_eq!(
        conn_of(on_engine(&e, &idempotent_get("many"), None).await),
        s,
        "LIFO: the most recently returned connection (the slow one) is taken first"
    );
}

/// **Review R11: a `HEAD` that cannot be delivered discards the connection.** With no credit at all
/// (`credit_frames = 0`, which `Config::validate` admits), the `HEAD` parks; a `CANCEL` then ends
/// the request `Cancelled`, and the upstream must see the connection close — the whole response is
/// readable, so a connection the engine kept (or leaked) would be drained by `hyper` and stay open.
#[tokio::test]
async fn a_cancel_while_the_head_waits_for_credit_discards_the_connection() {
    let up = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let _ = rec
            .write(&mut s, b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nAAAA")
            .await;
        rec.hold_until_closed(&mut s, Duration::from_secs(8)).await;
    })
    .await;
    let d = daemon_with(
        vars(&[
            ("FERRO_UPSTREAMS", "s".into()),
            ("FERRO_UPSTREAM_S_ORIGIN", origin(up.addr)),
        ]),
        StaticResolver::new(),
        Arc::new(TcpConnect),
        |c| c.credit_frames = 0,
    );
    let mut c = d.client().await;
    c.send_request(
        1,
        service::HTTP,
        method_http::REQUEST,
        idempotent_get("s").encode(),
    )
    .await;
    wait_for(|| up.rec.written() > 0).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.cancel(1).await;
    let r = collect_with(&mut c, 1, false).await;
    assert!(r.head.is_none(), "no credit, so no HEAD");
    r.assert_cancelled();
    wait_for(|| up.rec.closed_by_peer() == 1).await;
}

/// **Review R10: a connection is pooled only once `hyper` will take its next request.** Back to
/// back, 300 exchanges on one keep-alive connection, driven on the engine with no session round
/// trip between them (the tightest the engine can be asked): every one completes and every one
/// after the first reuses the connection. Pooled before `hyper`'s dispatcher wants the next
/// request, `try_send_request` hands the request back and it would fail `unsent_closed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn back_to_back_reuse_never_finds_a_connection_unready() {
    let (addr, count) = numbered("").await;
    let e = engine_for(vars(&[
        ("FERRO_UPSTREAMS", "bb".into()),
        ("FERRO_UPSTREAM_BB_ORIGIN", origin(addr)),
    ]));
    let first = conn_of(on_engine(&e, &idempotent_get("bb"), None).await);
    for i in 0..300 {
        let (t, conn) = on_engine(&e, &post("bb", b"x"), None).await;
        match t {
            Terminal::Done(d) => assert!(d.stats.reused, "iteration {i}: not reused"),
            other => panic!("iteration {i}: {other:?}"),
        }
        assert_eq!(conn, Some(first), "iteration {i}");
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
