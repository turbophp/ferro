//! **M6-F5a — Ferro HTTP originates TLS (SPEC §23.8.8, §23.7.1's TLS rows, §23.14 chaos 3).**
//!
//! Every test runs against a LOCAL TLS upstream whose certificates are generated in-test
//! (`ferro_http::testcert`), and that records what it received at the HTTP layer — so "received 0"
//! is a read-back assertion (charter rule 3). Most tests go through real sessions over a Unix
//! socket; the per-partition resumption test is engine-level, because two attested peer uids need
//! two OS users; one test spawns a real `ferrod` process, because the OS root store is read from
//! `ferrod`'s own environment (`SSL_CERT_FILE`).
#![cfg(feature = "http")]

mod common;
mod http_support;

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ferro_http::HttpConfig;
use ferro_http::engine::{
    BoxIo, Connect, HttpEngine, OsRoots, Resolve, ResponseSink, SinkError, SinkFrame,
    StaticResolver, SystemResolver, TcpConnect, Terminal,
};
use ferro_http::testcert::{self, Issued, Spec, Usage};
use ferro_proto::consts::{branch, errc, http_cause};
use ferro_proto::messages::{HttpDone, HttpRequest};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;
use http_support::{Daemon, Reply, exchange, idempotent_get, post, request};
use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

const OK_HELLO: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";

// =================================================================================================
// PKI
// =================================================================================================

/// A test CA, written to `CA_FILE` in a temp directory.
struct Pki {
    ca: Issued,
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Pki {
        Pki {
            ca: testcert::ca("Ferro F5a Test CA"),
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let p = self.dir.path().join(name);
        std::fs::write(&p, contents).unwrap();
        p
    }

    fn ca_file(&self) -> PathBuf {
        self.write("ca.pem", &self.ca.cert_pem())
    }

    /// A server leaf for `api.test` from this CA.
    fn leaf(&self) -> Issued {
        self.ca
            .sign(&Spec::new("api.test", Usage::Server).dns("api.test"))
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

static TLS12_ONLY: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS12];
static TLS13_AND_12: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS13, &rustls::version::TLS12];

fn versions(tls12_only: bool) -> &'static [&'static rustls::SupportedProtocolVersion] {
    if tls12_only { TLS12_ONLY } else { TLS13_AND_12 }
}

fn server_cfg(leaf: &Issued, tls12_only: bool, alpn: &[&[u8]]) -> Arc<ServerConfig> {
    let mut c = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(versions(tls12_only))
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![leaf.cert()], leaf.key())
        .unwrap();
    c.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
    Arc::new(c)
}

fn mtls_server_cfg(leaf: &Issued, client_ca: &Issued, tls12_only: bool) -> Arc<ServerConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(client_ca.cert()).unwrap();
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
        .build()
        .unwrap();
    let v: &[&'static rustls::SupportedProtocolVersion] = if tls12_only {
        &[&rustls::version::TLS12]
    } else {
        &[&rustls::version::TLS13]
    };
    Arc::new(
        ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(v)
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![leaf.cert()], leaf.key())
            .unwrap(),
    )
}

// =================================================================================================
// Upstreams
// =================================================================================================

/// One request as the upstream's HTTP layer received it (after TLS).
#[derive(Clone, Debug)]
struct Seen {
    head: String,
    body: Vec<u8>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<String> {
        self.head.split("\r\n").skip(1).find_map(|l| {
            let (n, v) = l.split_once(':')?;
            n.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
        })
    }
}

/// What a TLS upstream observed.
#[derive(Clone, Default)]
struct TlsRec {
    tcp: Arc<AtomicU64>,
    handshakes: Arc<AtomicU64>,
    resumed: Arc<AtomicU64>,
    sni: Arc<Mutex<Vec<Option<String>>>>,
    requests: Arc<Mutex<Vec<Seen>>>,
}

impl TlsRec {
    fn tcp(&self) -> u64 {
        self.tcp.load(Ordering::SeqCst)
    }
    fn handshakes(&self) -> u64 {
        self.handshakes.load(Ordering::SeqCst)
    }
    fn resumed(&self) -> u64 {
        self.resumed.load(Ordering::SeqCst)
    }
    fn requests(&self) -> Vec<Seen> {
        self.requests.lock().unwrap().clone()
    }
}

async fn read_request<S: AsyncRead + Unpin>(s: &mut S) -> Option<Seen> {
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    let end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
        let n = s.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let mut seen = Seen {
        head,
        body: Vec::new(),
    };
    let len: usize = seen
        .header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    buf.drain(..end);
    while buf.len() < len {
        let n = s.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
        buf.extend_from_slice(&chunk[..n]);
    }
    seen.body = buf;
    Some(seen)
}

/// What a TLS upstream does after the handshake.
#[derive(Clone, Copy)]
enum Script {
    /// Keep-alive: answer every request with this response.
    Respond(&'static [u8]),
    /// Read the request and never answer.
    Silent,
}

/// A TLS upstream on `127.0.0.1`.
async fn tls_upstream(cfg: Arc<ServerConfig>, script: Script) -> (SocketAddr, TlsRec) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rec = TlsRec::default();
    let r = rec.clone();
    let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            r.tcp.fetch_add(1, Ordering::SeqCst);
            let r = r.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut t) = acceptor.accept(s).await else {
                    return;
                };
                {
                    let conn = t.get_ref().1;
                    r.handshakes.fetch_add(1, Ordering::SeqCst);
                    if conn.handshake_kind() == Some(rustls::HandshakeKind::Resumed) {
                        r.resumed.fetch_add(1, Ordering::SeqCst);
                    }
                    r.sni
                        .lock()
                        .unwrap()
                        .push(conn.server_name().map(str::to_string));
                }
                while let Some(seen) = read_request(&mut t).await {
                    r.requests.lock().unwrap().push(seen);
                    match script {
                        Script::Respond(resp) => {
                            if t.write_all(resp).await.is_err() || t.flush().await.is_err() {
                                return;
                            }
                        }
                        Script::Silent => {
                            let mut b = [0u8; 1024];
                            while matches!(t.read(&mut b).await, Ok(n) if n > 0) {}
                            return;
                        }
                    }
                }
            });
        }
    });
    (addr, rec)
}

/// A plain TCP upstream on `127.0.0.1` that runs `f` on every accepted socket (handshake faults).
async fn raw_upstream<F, Fut>(f: F) -> (SocketAddr, Arc<AtomicU64>)
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conns = Arc::new(AtomicU64::new(0));
    let c = conns.clone();
    let f = Arc::new(f);
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            c.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(f(s));
        }
    });
    (addr, conns)
}

/// Close with RST rather than FIN (`SO_LINGER = 0`).
#[allow(deprecated)]
fn rst(s: TcpStream) {
    s.set_linger(Some(Duration::ZERO)).unwrap();
    drop(s);
}

// =================================================================================================
// The engine and the daemon
// =================================================================================================

fn resolver_to_loopback() -> Arc<StaticResolver> {
    let r = StaticResolver::new();
    r.set("api.test", vec![vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]]);
    r
}

/// `FERRO_UPSTREAMS=api` at `https://api.test:<port>` (resolved to loopback), plus `extra` keys.
fn env(port: u16, extra: &[(&str, String)]) -> Vec<(String, String)> {
    let mut v = vec![
        ("FERRO_UPSTREAMS".to_string(), "api".to_string()),
        (
            "FERRO_UPSTREAM_API_ORIGIN".to_string(),
            format!("https://api.test:{port}"),
        ),
        (
            "FERRO_UPSTREAM_API_ADDRESS_CLASSES".to_string(),
            "loopback".to_string(),
        ),
    ];
    for (k, val) in extra {
        v.push((format!("FERRO_UPSTREAM_API_{k}"), val.clone()));
    }
    v
}

fn read_real(p: &Path) -> io::Result<Vec<u8>> {
    std::fs::read(p)
}

fn engine_full(
    env: Vec<(String, String)>,
    resolver: Arc<dyn Resolve>,
    connector: Arc<dyn Connect>,
    os: OsRoots,
) -> Arc<HttpEngine> {
    let cfg = HttpConfig::load(
        env.into_iter().map(|(k, v)| (k.into(), v.into())),
        &read_real,
    );
    let errors: Vec<String> = cfg.errors().iter().map(|e| e.to_string()).collect();
    assert!(errors.is_empty(), "test configuration refused: {errors:?}");
    Arc::new(HttpEngine::with_tls_seams(
        Arc::new(cfg),
        resolver,
        connector,
        &os,
        &read_real,
    ))
}

fn engine(env: Vec<(String, String)>) -> Arc<HttpEngine> {
    engine_full(
        env,
        resolver_to_loopback(),
        Arc::new(TcpConnect),
        OsRoots::Fixed(Vec::new()),
    )
}

/// A daemon (real handler factory, real sessions) over `engine`.
fn daemon_over(engine: Arc<HttpEngine>) -> Daemon {
    let config = Config::default();
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
        server: common::TestServer::spawn_with_factory_and_config(
            BootEpoch(51),
            config,
            registry,
            tx_registry,
            factory,
        ),
        engine,
    }
}

async fn one(d: &Daemon, req: &HttpRequest) -> Reply {
    let mut c = d.client().await;
    exchange(&mut c, 1, req).await
}

/// A sink that accepts every frame (engine-level tests).
#[derive(Default)]
struct NullSink;
impl ResponseSink for NullSink {
    fn send<'a>(
        &'a self,
        _frame: SinkFrame,
        _payload: Vec<u8>,
        _deadline: tokio::time::Instant,
        _cancel: &'a tokio_util::sync::CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

async fn on_engine(e: &HttpEngine, req: &HttpRequest, uid: Option<u32>) -> Terminal {
    e.exchange(
        req,
        uid,
        Instant::now(),
        &tokio_util::sync::CancellationToken::new(),
        &NullSink,
    )
    .await
}

fn done(t: &Terminal) -> &HttpDone {
    match t {
        Terminal::Done(d) => d,
        other => panic!("expected Done, got {other:?}"),
    }
}

/// A POST that dials fresh every time (`H1_UNSAFE_REUSE_MAX_IDLE_MS=0`), so every one handshakes.
fn fresh_post() -> HttpRequest {
    post("api", b"charge")
}

// =================================================================================================
// Round trips: SNI and verification by NAME, connect to the CHECKED address
// =================================================================================================

/// §23.8.8 + §23.8.5: the name `api.test` resolves (through the injected resolver) to 127.0.0.1;
/// the certificate names only `api.test` (no IP SAN), so verification used the configured NAME
/// while the TCP connect went to the checked address; the server received SNI `api.test`.
#[tokio::test]
async fn https_round_trip_verifies_the_name_while_the_connect_goes_to_the_checked_address() {
    let pki = Pki::new();
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(
        addr.port(),
        &[("CA_FILE", pki.ca_file().display().to_string())],
    )));
    let mut c = d.client().await;
    let r = exchange(&mut c, 1, &post("api", b"charge")).await;
    assert_eq!(r.head.as_ref().expect("a head").status, 200);
    assert_eq!(r.body, b"hello");
    let stats = r.done().stats;
    assert!(!stats.reused);
    assert!(
        stats.tls_us > 0,
        "the handshake is timed (HttpStats.tls_us)"
    );
    let seen = rec.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].body, b"charge");
    assert_eq!(
        seen[0].header("host").as_deref(),
        Some(&*format!("api.test:{}", addr.port()))
    );
    assert_eq!(
        stats.bytes_sent as usize,
        seen[0].head.len() + seen[0].body.len(),
        "bytes_sent is the plaintext the upstream decrypted (§23.5.4)"
    );
    assert_eq!(
        rec.sni.lock().unwrap().clone(),
        vec![Some("api.test".to_string())]
    );
    assert_eq!(d.engine.tls_handshakes("api"), Some((1, 0)));
    // The keep-alive pool reuses the TLS connection: no second handshake.
    let r = exchange(&mut c, 3, &idempotent_get("api")).await;
    assert!(r.done().stats.reused);
    assert_eq!(r.done().stats.tls_us, 0);
    assert_eq!((rec.tcp(), rec.handshakes()), (1, 1));
    assert_eq!(d.engine.tls_handshakes("api"), Some((1, 0)));
}

/// An IP-literal `ORIGIN` sends no SNI and needs an IP SAN (§23.8.8); the control is a leaf that
/// names only `api.test`, refused `tls_verify` before anything is sent.
#[tokio::test]
async fn an_ip_literal_origin_sends_no_sni_and_verifies_the_ip_san() {
    let pki = Pki::new();
    let ip_leaf = pki
        .ca
        .sign(&Spec::new("loopback", Usage::Server).ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let (addr, rec) =
        tls_upstream(server_cfg(&ip_leaf, false, &[]), Script::Respond(OK_HELLO)).await;
    let ip_env = |port: u16| {
        vec![
            ("FERRO_UPSTREAMS".to_string(), "api".to_string()),
            (
                "FERRO_UPSTREAM_API_ORIGIN".to_string(),
                format!("https://127.0.0.1:{port}"),
            ),
            (
                "FERRO_UPSTREAM_API_CA_FILE".to_string(),
                pki.ca_file().display().to_string(),
            ),
        ]
    };
    let d = daemon_over(engine(ip_env(addr.port())));
    let r = one(&d, &post("api", b"x")).await;
    assert_eq!(r.head.expect("a head").status, 200);
    assert_eq!(
        rec.sni.lock().unwrap().clone(),
        vec![None],
        "no SNI for an IP"
    );

    let (addr2, rec2) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d2 = daemon_over(engine(ip_env(addr2.port())));
    one(&d2, &post("api", b"x")).await.assert_error(
        errc::TLS_REFUSED,
        branch::NON_RETRYABLE,
        http_cause::TLS_VERIFY,
    );
    assert!(rec2.requests().is_empty());
}

// =================================================================================================
// §23.7.1's TLS rows, end to end: every `tls_*` cause, for a POST and a declared GET
// =================================================================================================

/// One failing upstream, asked twice: a non-idempotent POST and a declared-idempotent GET. A
/// handshake failure precedes dispatch, so BOTH get the same "before dispatch" row — never
/// `Indeterminate` — and the upstream's HTTP layer receives nothing.
async fn both(d: &Daemon) -> (Reply, Reply) {
    let mut c = d.client().await;
    let p = exchange(&mut c, 1, &post("api", b"charge")).await;
    let g = exchange(&mut c, 3, &idempotent_get("api")).await;
    (p, g)
}

fn assert_both(p: &Reply, g: &Reply, code: u16, br: u8, cause: &str) {
    for r in [p, g] {
        assert!(r.head.is_none());
        r.assert_error(code, br, cause);
    }
}

#[tokio::test]
async fn certificate_refusals_are_tls_verify_for_every_request_and_nothing_is_received() {
    let pki = Pki::new();
    let ca_file = pki.ca_file().display().to_string();
    let stranger = testcert::ca("Stranger CA");
    let leaves = [
        (
            "unknown CA",
            stranger.sign(&Spec::new("api.test", Usage::Server).dns("api.test")),
        ),
        (
            "name mismatch",
            pki.ca
                .sign(&Spec::new("other.test", Usage::Server).dns("other.test")),
        ),
        (
            "expired",
            pki.ca.sign(
                &Spec::new("api.test", Usage::Server)
                    .dns("api.test")
                    .expired(),
            ),
        ),
    ];
    for (what, leaf) in leaves {
        let (addr, rec) =
            tls_upstream(server_cfg(&leaf, false, &[]), Script::Respond(OK_HELLO)).await;
        let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
        let (p, g) = both(&d).await;
        assert_both(
            &p,
            &g,
            errc::TLS_REFUSED,
            branch::NON_RETRYABLE,
            http_cause::TLS_VERIFY,
        );
        assert!(
            rec.requests().is_empty(),
            "{what}: nothing reached the HTTP layer"
        );
        assert_eq!(rec.handshakes(), 0, "{what}: no handshake completed");
        assert!(
            rec.tcp() >= 1,
            "{what}: it was dialled (the refusal is TLS's, not the guard's)"
        );
    }
}

#[tokio::test]
async fn version_and_alpn_refusals_are_tls_refused() {
    let pki = Pki::new();
    let ca_file = pki.ca_file().display().to_string();
    // MIN_TLS=1.3 against a TLS 1.2-only server.
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), true, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(
        addr.port(),
        &[("CA_FILE", ca_file.clone()), ("MIN_TLS", "1.3".into())],
    )));
    let (p, g) = both(&d).await;
    assert_both(
        &p,
        &g,
        errc::TLS_REFUSED,
        branch::NON_RETRYABLE,
        http_cause::TLS_VERSION,
    );
    assert!(rec.requests().is_empty());
    // Control: the same server at the default MIN_TLS=1.2.
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
    assert_eq!(
        one(&d, &post("api", b"x"))
            .await
            .head
            .expect("a head")
            .status,
        200
    );

    // An h2-only server refuses the HTTP/1.1 sub-pool's ALPN offer (§23.8.3).
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[b"h2"]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
    let (p, g) = both(&d).await;
    assert_both(
        &p,
        &g,
        errc::TLS_REFUSED,
        branch::NON_RETRYABLE,
        http_cause::TLS_ALPN,
    );
    assert!(rec.requests().is_empty());
    // Control: a server that also speaks http/1.1 is served over it.
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[b"h2", b"http/1.1"]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file)])));
    assert_eq!(
        one(&d, &post("api", b"x"))
            .await
            .head
            .expect("a head")
            .status,
        200
    );
    assert_eq!(rec.requests().len(), 1);
}

#[tokio::test]
async fn handshake_transport_failures_are_retryable_tls_handshake() {
    let pki = Pki::new();
    let ca_file = pki.ca_file().display().to_string();
    // EOF: read part of the ClientHello, then close.
    let (addr, conns) = raw_upstream(|mut s| async move {
        let mut b = [0u8; 8];
        let _ = s.read(&mut b).await;
    })
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
    let (p, g) = both(&d).await;
    assert_both(
        &p,
        &g,
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::TLS_HANDSHAKE,
    );
    assert_eq!(
        conns.load(Ordering::SeqCst),
        2,
        "each request dialled once, never re-dialled"
    );
    // RST mid-handshake.
    let (addr, _) = raw_upstream(|mut s| async move {
        let mut b = [0u8; 8];
        let _ = s.read(&mut b).await;
        rst(s);
    })
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
    let (p, g) = both(&d).await;
    assert_both(
        &p,
        &g,
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::TLS_HANDSHAKE,
    );
    // Garbage instead of a ServerHello.
    let (addr, _) = raw_upstream(|mut s| async move {
        let mut b = [0u8; 8];
        let _ = s.read(&mut b).await;
        let _ = s.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
    })
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file)])));
    let (p, g) = both(&d).await;
    assert_both(
        &p,
        &g,
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::TLS_HANDSHAKE,
    );
}

/// `CONNECT_TIMEOUT_MS` bounds DNS + TCP + TLS (§23.8.4): a server that accepts TCP and never
/// answers the ClientHello is `connect_timeout` at the bound, not at the request's deadline.
#[tokio::test]
async fn the_handshake_counts_against_the_connect_bound() {
    let pki = Pki::new();
    let (addr, _) = raw_upstream(|mut s| async move {
        let mut b = [0u8; 4096];
        while matches!(s.read(&mut b).await, Ok(n) if n > 0) {}
    })
    .await;
    let d = daemon_over(engine(env(
        addr.port(),
        &[
            ("CA_FILE", pki.ca_file().display().to_string()),
            ("CONNECT_TIMEOUT_MS", "300".into()),
        ],
    )));
    let start = Instant::now();
    let r = one(&d, &post("api", b"x")).await;
    r.assert_error(
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::CONNECT_TIMEOUT,
    );
    let took = start.elapsed();
    assert!(
        took >= Duration::from_millis(250) && took < Duration::from_secs(5),
        "{took:?}"
    );
}

// =================================================================================================
// `sent` on https (§23.7.1 as amended by M6-F5a): the SOCKET decides
// =================================================================================================

/// A TCP connector for a TLS 1.2 upstream that acts on the client's application-data records
/// (content type 0x17 — in TLS 1.2 the handshake's own records are 0x16 and 0x14, so the first
/// 0x17 write is the request). `Hold`: such a write is `Pending` forever (the socket takes
/// nothing). `PartialThenReset`: the first such write is accepted for 16 bytes, the next write
/// fails with a reset.
#[derive(Clone, Copy)]
enum AppData {
    Hold,
    PartialThenReset,
}

struct AppDataConnect(AppData);

struct AppDataIo {
    inner: TcpStream,
    mode: AppData,
    seen_app: bool,
}

impl AsyncRead for AppDataIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for AppDataIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let app = buf.first() == Some(&0x17);
        match (self.mode, self.seen_app, app) {
            (_, false, false) => Pin::new(&mut self.inner).poll_write(cx, buf),
            (AppData::Hold, _, _) => {
                self.seen_app = true;
                Poll::Pending
            }
            (AppData::PartialThenReset, false, true) => {
                self.seen_app = true;
                let n = buf.len().min(16);
                Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
            }
            (AppData::PartialThenReset, true, _) => {
                Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
            }
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if matches!(self.mode, AppData::Hold) && self.seen_app {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connect for AppDataConnect {
    fn connect<'a>(
        &'a self,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BoxIo>> + Send + 'a>> {
        let mode = self.0;
        Box::pin(async move {
            let inner = TcpStream::connect(peer).await?;
            inner.set_nodelay(true)?;
            Ok(Box::new(AppDataIo {
                inner,
                mode,
                seen_app: false,
            }) as BoxIo)
        })
    }
}

/// **The coordinator's rule, end to end: bytes in `rustls`'s buffer that never reached the wire
/// are UNSENT.** The socket takes none of the request's records; `tokio-rustls` still accepts the
/// plaintext (premise P-C), so F1a's "plaintext OR ciphertext" would have called it sent and made
/// the POST `Indeterminate`. It is "dispatched, not sent": Retryable `PoolTimeout` (`deadline`),
/// and the upstream received nothing.
#[tokio::test]
async fn rustls_buffered_bytes_that_never_reached_the_socket_are_unsent() {
    let pki = Pki::new();
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), true, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine_full(
        env(
            addr.port(),
            &[("CA_FILE", pki.ca_file().display().to_string())],
        ),
        resolver_to_loopback(),
        Arc::new(AppDataConnect(AppData::Hold)),
        OsRoots::Fixed(Vec::new()),
    ));
    for (rid, req) in [
        (
            1,
            HttpRequest {
                timeout_ms: Some(400),
                ..post("api", b"charge")
            },
        ),
        (
            3,
            HttpRequest {
                timeout_ms: Some(400),
                ..idempotent_get("api")
            },
        ),
    ] {
        let mut c = d.client().await;
        let r = exchange(&mut c, rid, &req).await;
        assert!(r.head.is_none());
        r.assert_error(errc::POOL_TIMEOUT, branch::RETRYABLE, http_cause::DEADLINE);
    }
    assert_eq!(
        rec.handshakes(),
        2,
        "both handshakes completed: the hold is after dispatch"
    );
    assert!(rec.requests().is_empty());
}

/// F1a's review F-1 through the engine: part of the request's first record reaches the socket and
/// the next socket write fails, so the plaintext layer accepted NOTHING (`tokio-rustls` returned
/// the error) — the ciphertext count alone makes it sent: the POST is `Indeterminate`, the declared
/// GET Retryable `ConnectionLost`, never `unsent_write`.
#[tokio::test]
async fn a_record_on_the_socket_makes_the_request_sent_although_the_plaintext_write_failed() {
    let pki = Pki::new();
    let (addr, _rec) = tls_upstream(
        server_cfg(&pki.leaf(), true, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine_full(
        env(
            addr.port(),
            &[("CA_FILE", pki.ca_file().display().to_string())],
        ),
        resolver_to_loopback(),
        Arc::new(AppDataConnect(AppData::PartialThenReset)),
        OsRoots::Fixed(Vec::new()),
    ));
    let (p, g) = both(&d).await;
    let ep = p.error();
    assert_eq!(
        (ep.code, ep.branch),
        (errc::WRITE_UNCONFIRMED, branch::INDETERMINATE),
        "{ep:?}"
    );
    assert!(
        [http_cause::WRITE, http_cause::RESET].contains(&ep.detail.as_deref().unwrap_or("")),
        "{ep:?}"
    );
    let eg = g.error();
    assert_eq!(
        (eg.code, eg.branch),
        (errc::CONNECTION_LOST, branch::RETRYABLE),
        "{eg:?}"
    );
    assert_ne!(eg.detail.as_deref(), Some(http_cause::UNSENT_WRITE));
}

/// The ordinary sent path over TLS: the upstream decrypts the whole POST and never answers. The
/// deadline makes it "sent, no head" — `Indeterminate` (`timeout`) for the POST, `QueryTimeout` for
/// the declared GET — and each was received exactly once.
#[tokio::test]
async fn a_sent_request_without_a_head_over_tls_is_classified_by_the_sent_rows() {
    let pki = Pki::new();
    let (addr, rec) = tls_upstream(server_cfg(&pki.leaf(), false, &[]), Script::Silent).await;
    let d = daemon_over(engine(env(
        addr.port(),
        &[("CA_FILE", pki.ca_file().display().to_string())],
    )));
    let mut c = d.client().await;
    let p = exchange(
        &mut c,
        1,
        &HttpRequest {
            timeout_ms: Some(400),
            ..post("api", b"charge")
        },
    )
    .await;
    let ep = p.error();
    assert_eq!(
        (ep.code, ep.branch, ep.detail.as_deref()),
        (
            errc::WRITE_UNCONFIRMED,
            branch::INDETERMINATE,
            Some(http_cause::TIMEOUT)
        ),
        "{ep:?}"
    );
    let g = exchange(
        &mut c,
        3,
        &HttpRequest {
            timeout_ms: Some(400),
            ..idempotent_get("api")
        },
    )
    .await;
    g.assert_error(
        errc::QUERY_TIMEOUT,
        branch::NON_RETRYABLE,
        http_cause::TIMEOUT,
    );
    let seen = rec.requests();
    assert_eq!(seen.len(), 2, "each received exactly once");
    assert_eq!(seen[0].body, b"charge");
}

// =================================================================================================
// Chaos 3 (§23.14)
// =================================================================================================

/// Refused; `.invalid` NXDOMAIN (through the REAL resolver); wrong-host certificate; expired
/// certificate; reset during the handshake — `UpstreamUnavailable` (`connect_refused` / `dns` /
/// `tls_handshake`) or `TlsRefused` (`tls_verify`), each a POST, each received 0.
#[tokio::test]
async fn chaos3_refused_nxdomain_wrong_host_expired_and_reset_during_the_handshake() {
    let pki = Pki::new();
    let ca_file = pki.ca_file().display().to_string();

    // Refused: a port nothing listens on.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let d = daemon_over(engine(env(port, &[("CA_FILE", ca_file.clone())])));
    one(&d, &post("api", b"x")).await.assert_error(
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::CONNECT_REFUSED,
    );

    // NXDOMAIN: `.invalid` never resolves (RFC 6761), through getaddrinfo.
    let nx = vec![
        ("FERRO_UPSTREAMS".to_string(), "api".to_string()),
        (
            "FERRO_UPSTREAM_API_ORIGIN".to_string(),
            "https://ferro-f5a.invalid".to_string(),
        ),
        ("FERRO_UPSTREAM_API_CA_FILE".to_string(), ca_file.clone()),
    ];
    let d = daemon_over(engine_full(
        nx,
        Arc::new(SystemResolver),
        Arc::new(TcpConnect),
        OsRoots::Fixed(Vec::new()),
    ));
    one(&d, &post("api", b"x")).await.assert_error(
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::DNS,
    );

    // Wrong host, expired.
    for leaf in [
        pki.ca
            .sign(&Spec::new("wrong.test", Usage::Server).dns("wrong.test")),
        pki.ca.sign(
            &Spec::new("api.test", Usage::Server)
                .dns("api.test")
                .expired(),
        ),
    ] {
        let (addr, rec) =
            tls_upstream(server_cfg(&leaf, false, &[]), Script::Respond(OK_HELLO)).await;
        let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
        one(&d, &post("api", b"x")).await.assert_error(
            errc::TLS_REFUSED,
            branch::NON_RETRYABLE,
            http_cause::TLS_VERIFY,
        );
        assert!(rec.requests().is_empty(), "received 0");
    }

    // Reset during the handshake: after the ServerHello flight would be due.
    let (addr, conns) = raw_upstream(|mut s| async move {
        let mut b = [0u8; 4096];
        let _ = s.read(&mut b).await;
        rst(s);
    })
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file)])));
    one(&d, &post("api", b"x")).await.assert_error(
        errc::UPSTREAM_UNAVAILABLE,
        branch::RETRYABLE,
        http_cause::TLS_HANDSHAKE,
    );
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "dialled once, never re-sent"
    );
}

// =================================================================================================
// TLS material, mTLS (not served until F5c), and the open item
// =================================================================================================

/// An upstream whose `CA_FILE` cannot be loaded is DISABLED at start (§23.3.1's rule for an
/// unparseable value) and refused EXACTLY as an unknown upstream — same code, cause and sentence,
/// even for a request another rule would refuse first — and never dialled.
#[tokio::test]
async fn unloadable_tls_material_disables_the_upstream_indistinguishably() {
    let pki = Pki::new();
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let missing = pki.dir.path().join("missing.pem").display().to_string();
    let e = engine(env(addr.port(), &[("CA_FILE", missing.clone())]));
    let errs = e.tls_errors();
    assert_eq!(errs.len(), 1);
    assert!(!errs[0].to_string().contains(&missing), "never the value");
    let d = daemon_over(e);
    let mut c = d.client().await;
    let a = exchange(&mut c, 1, &post("api", b"x")).await;
    let b = exchange(&mut c, 3, &request("nope", "GET", "/")).await;
    let bad_target = exchange(&mut c, 5, &request("api", "GET", "/../x")).await;
    for r in [&a, &b, &bad_target] {
        r.assert_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            http_cause::FORBIDDEN_UPSTREAM,
        );
    }
    assert_eq!(a.error().message, b.error().message);
    assert_eq!(bad_target.error().message, b.error().message);
    assert_eq!(rec.tcp(), 0);
}

/// mTLS is slice F5c: an upstream configured with a client certificate is `Unsupported` (no cause
/// token) and never dialled — never a connection that silently omits the configured certificate.
#[tokio::test]
async fn an_mtls_upstream_is_not_served_until_f5c() {
    let pki = Pki::new();
    let client = pki.ca.sign(&Spec::new("ferro-client", Usage::Client));
    let cert = pki.write("client.pem", &client.cert_pem());
    let key = pki.write("client.key", &client.key_pem());
    let (addr, rec) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(
        addr.port(),
        &[
            ("CA_FILE", pki.ca_file().display().to_string()),
            ("CLIENT_CERT_FILE", cert.display().to_string()),
            ("CLIENT_KEY_FILE", key.display().to_string()),
        ],
    )));
    let r = one(&d, &post("api", b"x")).await;
    let ep = r.error();
    assert_eq!(
        (ep.code, ep.detail.as_deref()),
        (errc::UNSUPPORTED, None),
        "{ep:?}"
    );
    assert_eq!(rec.tcp(), 0);
}

/// **SPEC §21 open item O-F5c, pinned as it stands.** An upstream that REQUIRES a client
/// certificate, configured without one: under TLS 1.2 the refusal fails the handshake (before
/// dispatch, `tls_verify`, never Indeterminate); under TLS 1.3 the client's handshake completes
/// first (premise P-M), the request's records reach the socket, and the server's
/// `certificate_required` alert arrives after dispatch — so by the measured `sent` rule the POST is
/// "sent, no head" (`Indeterminate`), although the server's HTTP layer received nothing. Never
/// re-sent. F5c decides this row; this test is expected to change with it.
#[tokio::test]
async fn mtls_required_but_absent_tls12_is_tls_verify_and_tls13_is_the_open_item() {
    let pki = Pki::new();
    let client_ca = testcert::ca("Client CA");
    let ca_file = pki.ca_file().display().to_string();

    let (addr, rec) = tls_upstream(
        mtls_server_cfg(&pki.leaf(), &client_ca, true),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file.clone())])));
    let (p, g) = both(&d).await;
    assert_both(
        &p,
        &g,
        errc::TLS_REFUSED,
        branch::NON_RETRYABLE,
        http_cause::TLS_VERIFY,
    );
    assert!(rec.requests().is_empty());

    let (addr, rec) = tls_upstream(
        mtls_server_cfg(&pki.leaf(), &client_ca, false),
        Script::Respond(OK_HELLO),
    )
    .await;
    let d = daemon_over(engine(env(addr.port(), &[("CA_FILE", ca_file)])));
    let (p, g) = both(&d).await;
    let ep = p.error();
    assert_eq!(
        (ep.code, ep.branch),
        (errc::WRITE_UNCONFIRMED, branch::INDETERMINATE),
        "{ep:?}"
    );
    let eg = g.error();
    assert_eq!(
        (eg.code, eg.branch),
        (errc::CONNECTION_LOST, branch::RETRYABLE),
        "{eg:?}"
    );
    assert!(
        rec.requests().is_empty(),
        "the server's HTTP layer received nothing"
    );
    assert_eq!(
        rec.handshakes(),
        0,
        "the server never completed a handshake"
    );
}

// =================================================================================================
// Resumption (§23.8.8, P5) and PARTITION=uid
// =================================================================================================

/// Fresh dials to one upstream resume: one full handshake, then resumed ones — counted by the
/// engine AND observed by the server — over TLS 1.3 (tickets) and TLS 1.2 (session id / ticket).
#[tokio::test]
async fn fresh_dials_resume_the_session_and_the_counts_agree_with_the_server() {
    for tls12_only in [false, true] {
        let pki = Pki::new();
        let (addr, rec) = tls_upstream(
            server_cfg(&pki.leaf(), tls12_only, &[]),
            Script::Respond(OK_HELLO),
        )
        .await;
        let d = daemon_over(engine(env(
            addr.port(),
            &[
                ("CA_FILE", pki.ca_file().display().to_string()),
                ("H1_UNSAFE_REUSE_MAX_IDLE_MS", "0".into()),
            ],
        )));
        let mut c = d.client().await;
        for i in 0..3u32 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let r = exchange(&mut c, 1 + 2 * i, &fresh_post()).await;
            assert!(
                !r.done().stats.reused,
                "a POST past the unsafe-reuse bound dials fresh"
            );
        }
        assert_eq!(rec.tcp(), 3);
        assert_eq!(
            d.engine.tls_handshakes("api"),
            Some((1, 2)),
            "tls12_only={tls12_only}"
        );
        assert_eq!(
            (rec.handshakes(), rec.resumed()),
            (3, 2),
            "tls12_only={tls12_only}"
        );
    }
}

/// **Decided at M6-F5a (§23.8.8 amended): one session cache per POOL.** Under `PARTITION=uid` a
/// session one uid's connection established never resumes another uid's connection; within a
/// partition it does. The control runs the same sequence with no partition: everything after the
/// first handshake resumes.
#[tokio::test]
async fn partition_uid_keeps_resumption_inside_its_partition() {
    for partition in [true, false] {
        let pki = Pki::new();
        let (addr, rec) = tls_upstream(
            server_cfg(&pki.leaf(), false, &[]),
            Script::Respond(OK_HELLO),
        )
        .await;
        let mut extra = vec![
            ("CA_FILE", pki.ca_file().display().to_string()),
            ("H1_UNSAFE_REUSE_MAX_IDLE_MS", "0".into()),
        ];
        if partition {
            extra.push(("PARTITION", "uid".into()));
        }
        let e = engine(env(addr.port(), &extra));
        for uid in [1000, 1000, 2000, 2000] {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let t = on_engine(&e, &fresh_post(), Some(uid)).await;
            assert!(!done(&t).stats.reused);
        }
        let want = if partition { (2, 2) } else { (1, 3) };
        assert_eq!(e.tls_handshakes("api"), Some(want), "partition={partition}");
        assert_eq!(
            rec.resumed(),
            want.1,
            "the server agrees (partition={partition})"
        );
    }
}

// =================================================================================================
// Roots: the OS store, CA_FILE replacing it, and a real ferrod reading SSL_CERT_FILE
// =================================================================================================

/// Without `CA_FILE` the upstream trusts the OS store; with it, `CA_FILE` REPLACES the store — a
/// server the OS store trusts is then refused (§23.3.1, §23.8.8).
#[tokio::test]
async fn the_os_store_is_used_without_ca_file_and_ca_file_replaces_it() {
    let pki = Pki::new();
    let (addr, _rec) = tls_upstream(
        server_cfg(&pki.leaf(), false, &[]),
        Script::Respond(OK_HELLO),
    )
    .await;
    let os = || OsRoots::Fixed(vec![pki.ca.cert()]);
    let d = daemon_over(engine_full(
        env(addr.port(), &[]),
        resolver_to_loopback(),
        Arc::new(TcpConnect),
        os(),
    ));
    assert_eq!(
        one(&d, &post("api", b"x"))
            .await
            .head
            .expect("a head")
            .status,
        200
    );
    let other = testcert::ca("Other CA");
    let other_file = pki.write("other.pem", &other.cert_pem());
    let d = daemon_over(engine_full(
        env(
            addr.port(),
            &[("CA_FILE", other_file.display().to_string())],
        ),
        resolver_to_loopback(),
        Arc::new(TcpConnect),
        os(),
    ));
    one(&d, &post("api", b"x")).await.assert_error(
        errc::TLS_REFUSED,
        branch::NON_RETRYABLE,
        http_cause::TLS_VERIFY,
    );
}

struct KillOnDrop(std::process::Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The PRODUCTION root path: a real `ferrod` process, no `CA_FILE`, the test CA reachable only
/// through `SSL_CERT_FILE` in `ferrod`'s environment (§23.8.8: `rustls-native-certs` honours it).
/// The control is the same daemon with `SSL_CERT_FILE` pointing at an unrelated CA: `tls_verify`.
#[tokio::test]
async fn a_real_ferrod_trusts_the_os_store_it_reads_from_ssl_cert_file() {
    let pki = Pki::new();
    let ip_leaf = pki
        .ca
        .sign(&Spec::new("loopback", Usage::Server).ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let (addr, rec) =
        tls_upstream(server_cfg(&ip_leaf, false, &[]), Script::Respond(OK_HELLO)).await;
    let other = pki.write("other.pem", &testcert::ca("Other CA").cert_pem());
    for (store, ok) in [(pki.ca_file(), true), (other, false)] {
        let sock = pki.dir.path().join(format!("ferrod-{}.sock", u8::from(ok)));
        let log = pki.dir.path().join(format!("ferrod-{}.log", u8::from(ok)));
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_ferrod"))
            .env_clear()
            .env("FERRO_SOCK", &sock)
            .env("FERRO_ALLOW_UIDS", nix::unistd::geteuid().to_string())
            .env("FERRO_UPSTREAMS", "api")
            .env(
                "FERRO_UPSTREAM_API_ORIGIN",
                format!("https://127.0.0.1:{}", addr.port()),
            )
            .env("SSL_CERT_FILE", &store)
            .env("RUST_LOG", "info")
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(std::fs::File::create(log.with_extension("err")).unwrap())
            .spawn()
            .expect("spawn ferrod");
        let _child = KillOnDrop(child);
        let deadline = Instant::now() + Duration::from_secs(20);
        while tokio::net::UnixStream::connect(&sock).await.is_err() {
            assert!(Instant::now() < deadline, "ferrod never listened");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut c = http_support::hello(common::connect(&sock).await).await;
        let r = exchange(&mut c, 1, &post("api", b"x")).await;
        if ok {
            assert_eq!(r.head.expect("a head").status, 200);
        } else {
            r.assert_error(
                errc::TLS_REFUSED,
                branch::NON_RETRYABLE,
                http_cause::TLS_VERIFY,
            );
        }
    }
    assert_eq!(rec.requests().len(), 1);
}
