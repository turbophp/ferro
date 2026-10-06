//! TLS origination (SPEC §23.8.8; slice M6-F5a): `rustls` + `ring`, TLS 1.2 and 1.3.
//!
//! **Built once, at start** ([`TlsContexts::build`]): every enabled `https` upstream gets one
//! `rustls` client configuration — its roots (the OS store, loaded ONCE through
//! `rustls-native-certs`, which honours `SSL_CERT_FILE`/`SSL_CERT_DIR` from `ferrod`'s environment;
//! or `CA_FILE`, which REPLACES the OS store for that upstream), its `MIN_TLS`, and ALPN `http/1.1`
//! only (the HTTP/1.1 sub-pool's offer, §23.8.3). There is no reload (§23.18 Q9). An upstream whose
//! TLS material cannot be loaded is DISABLED — logged at `error`, naming the upstream and the key,
//! never the value — and refused exactly as a disabled upstream is (`forbidden_upstream`, D15's
//! indistinguishability), before any other check.
//!
//! **Verification is never optional** (§23.8.8): no key turns it off; development uses `CA_FILE`.
//! **SNI and certificate verification use the configured upstream NAME** ([`UpstreamTls::server_name`],
//! from `ORIGIN`), while the TCP connect goes to the address the guard checked (§23.8.5's pinning,
//! `dial`); an IP-literal `ORIGIN` sends no SNI and needs an IP SAN.
//!
//! **Nothing of a request precedes dispatch.** 0-RTT early data stays OFF (`enable_early_data` is
//! `rustls`'s default `false`, and [`UpstreamTls::config_for`] asserts it): early data would put
//! request bytes inside the handshake, before the write tracker is armed — the one place `sent`
//! could not see them — and would make them replayable. `tokio-rustls`'s connect future flushes the
//! client's whole handshake before it resolves (`common/handshake.rs`), so every handshake byte
//! precedes arming.
//!
//! **Resumption** (§23.8.8, P5): one client session cache PER POOL — per upstream, or per
//! (upstream, peer uid) under `PARTITION=uid` (decided at M6-F5a, SPEC §22.2 (dc)): a resumed
//! session continues an earlier connection's cryptographic session, so sharing a cache across
//! partitions would link one tenant's connections to another's at the upstream, which is the
//! connection-level separation `PARTITION=uid` exists to give. The cost is §23.8.1's already-stated
//! "up to N× connections and handshakes". Full and resumed handshakes are counted per upstream
//! ([`HandshakeCounts`]; the `ferro_http_tls_handshakes_total{resumed}` metric is F7's).
//!
//! **mTLS is slice M6-F5c, not here.** Under TLS 1.3 a server's rejection of the client's
//! certificate (or of its absence) is an alert the client receives AFTER its own handshake has
//! completed — measured by `a_tls13_client_auth_refusal_arrives_after_connect_resolves` below — so
//! it would surface after dispatch, with request bytes already on the socket. Classifying that
//! without making a handshake refusal `Indeterminate` is a fate-rule decision (SPEC §21 open item
//! O-F5c), so an upstream configured with `CLIENT_CERT_FILE` is not served by this build
//! (`Unsupported`), as `https` was not before F5a.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use rustls::client::Resumption;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{AlertDescription, ClientConfig, RootCertStore};

use crate::config::{HttpConfig, MinTls, Upstream, UpstreamEntry, env_name};
use crate::origin::{Host, Scheme};

/// The ALPN the HTTP/1.1 sub-pool offers (§23.8.3).
pub const ALPN_HTTP11: &[u8] = b"http/1.1";

/// The size each pool's `rustls` session cache is created with. One pool talks to one server name.
/// **Measured, not obvious:** `ClientSessionMemoryCache::new(n)` keeps `ceil(n / 8)` server names
/// (8 TLS 1.3 tickets each) in a `LimitedCache` that evicts as soon as its length reaches its
/// capacity — so with `n <= 8` (one name) the first insert is evicted at once and NOTHING ever
/// resumes (`a_second_connection_on_one_pool_resumes` fails at 8). 32 keeps 4 names' worth, which
/// leaves the one name this pool uses resident.
const SESSIONS_PER_POOL: usize = 32;

/// A `CA_FILE` larger than this is refused (an OS bundle is ~200 KiB).
pub const MAX_CA_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// Where the OS root store comes from.
#[derive(Clone)]
pub enum OsRoots {
    /// `rustls-native-certs` (production): the platform store, or `SSL_CERT_FILE`/`SSL_CERT_DIR`.
    Native,
    /// A fixed store (engine-level tests that exercise the "no `CA_FILE`" path without the host's).
    Fixed(Vec<CertificateDer<'static>>),
}

/// Which §23.5.6 dial cause a failed TLS handshake is (§23.7.1, "before dispatch").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsCause {
    /// A transport failure during the handshake (EOF, reset, a malformed or undecryptable record,
    /// a negotiation the peer aborted): Retryable `UpstreamUnavailable`.
    Handshake,
    /// The certificate was refused — by us (unknown issuer, wrong name, expired …) or by the peer's
    /// alert about OUR certificate: NonRetryable `TlsRefused`.
    Verify,
    /// No TLS version both sides accept: NonRetryable `TlsRefused`.
    Version,
    /// No application protocol both sides accept (an `h2`-only server): NonRetryable `TlsRefused`.
    Alpn,
}

/// Classify a failed `tokio-rustls` connect. `rustls`'s own errors arrive as
/// `io::Error(InvalidData)` wrapping a `rustls::Error` (`tokio-rustls` `common/mod.rs`); anything
/// else is the socket's (EOF, reset), a transport failure.
pub fn classify_handshake_error(e: &io::Error) -> TlsCause {
    let Some(err) = e.get_ref().and_then(|r| r.downcast_ref::<rustls::Error>()) else {
        return TlsCause::Handshake;
    };
    classify_rustls(err)
}

fn classify_rustls(err: &rustls::Error) -> TlsCause {
    use rustls::{Error as E, PeerIncompatible as PI, PeerMisbehaved as PM};
    match err {
        E::InvalidCertificate(_) | E::NoCertificatesPresented | E::UnsupportedNameType => {
            TlsCause::Verify
        }
        E::AlertReceived(a) => match a {
            AlertDescription::BadCertificate
            | AlertDescription::UnsupportedCertificate
            | AlertDescription::CertificateRevoked
            | AlertDescription::CertificateExpired
            | AlertDescription::CertificateUnknown
            | AlertDescription::UnknownCA
            | AlertDescription::AccessDenied
            | AlertDescription::CertificateRequired => TlsCause::Verify,
            AlertDescription::ProtocolVersion => TlsCause::Version,
            AlertDescription::NoApplicationProtocol => TlsCause::Alpn,
            _ => TlsCause::Handshake,
        },
        E::PeerIncompatible(
            PI::ServerDoesNotSupportTls12Or13
            | PI::ServerTlsVersionIsDisabledByOurConfig
            | PI::Tls12NotOffered
            | PI::Tls12NotOfferedOrEnabled
            | PI::SupportedVersionsExtensionRequired,
        ) => TlsCause::Version,
        E::PeerMisbehaved(PM::SelectedTls12UsingTls13VersionExtension) => TlsCause::Version,
        E::PeerMisbehaved(PM::SelectedUnofferedApplicationProtocol) | E::NoApplicationProtocol => {
            TlsCause::Alpn
        }
        _ => TlsCause::Handshake,
    }
}

/// Why an upstream's TLS material could not be loaded. Its `Display` names the upstream and the
/// key, never the value (a path or a certificate).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsSetupError {
    pub upstream: String,
    pub key: &'static str,
    pub reason: String,
}

impl std::fmt::Display for TlsSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "upstream {}: FERRO_UPSTREAM_{}_{} {}; the upstream is disabled",
            self.upstream,
            env_name(&self.upstream),
            self.key,
            self.reason
        )
    }
}

/// Full and resumed handshakes on one upstream (§23.8.8; P5).
#[derive(Debug, Default)]
pub struct HandshakeCounts {
    full: AtomicU64,
    resumed: AtomicU64,
}

impl HandshakeCounts {
    pub fn full(&self) -> u64 {
        self.full.load(Ordering::SeqCst)
    }

    pub fn resumed(&self) -> u64 {
        self.resumed.load(Ordering::SeqCst)
    }

    pub(crate) fn record(&self, kind: Option<rustls::HandshakeKind>) {
        match kind {
            Some(rustls::HandshakeKind::Resumed) => self.resumed.fetch_add(1, Ordering::SeqCst),
            _ => self.full.fetch_add(1, Ordering::SeqCst),
        };
    }
}

/// One `https` upstream's TLS context.
pub struct UpstreamTls {
    base: ClientConfig,
    server_name: ServerName<'static>,
    per_pool: Mutex<HashMap<Option<u32>, Arc<ClientConfig>>>,
    pub counts: HandshakeCounts,
}

impl UpstreamTls {
    /// The name SNI and verification use: `ORIGIN`'s host, never the dialled address.
    pub fn server_name(&self) -> ServerName<'static> {
        self.server_name.clone()
    }

    /// The configuration for one pool (`partition`: the peer uid under `PARTITION=uid`), carrying
    /// that pool's own session cache.
    pub fn config_for(&self, partition: Option<u32>) -> Arc<ClientConfig> {
        let mut map = self.per_pool.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(partition)
            .or_insert_with(|| {
                let mut c = self.base.clone();
                c.resumption = Resumption::in_memory_sessions(SESSIONS_PER_POOL);
                assert!(
                    !c.enable_early_data,
                    "0-RTT would send request bytes before dispatch"
                );
                Arc::new(c)
            })
            .clone()
    }
}

/// Every `https` upstream's TLS context, or why it is disabled.
#[derive(Default)]
pub struct TlsContexts {
    by_upstream: HashMap<String, Result<Arc<UpstreamTls>, TlsSetupError>>,
}

impl TlsContexts {
    /// Load every enabled `https` upstream's roots and build its configuration. The OS store is
    /// loaded at most once, and only when an `https` upstream has no `CA_FILE`.
    pub fn build(
        cfg: &HttpConfig,
        os_roots: &OsRoots,
        read_file: &dyn Fn(&Path) -> io::Result<Vec<u8>>,
    ) -> TlsContexts {
        let os_store: OnceLock<Arc<RootCertStore>> = OnceLock::new();
        let mut by_upstream = HashMap::new();
        for (name, entry) in cfg.entries() {
            let UpstreamEntry::Enabled(up) = entry else {
                continue;
            };
            if up.origin.scheme() != Scheme::Https {
                continue;
            }
            let roots = match &up.tls.ca_file {
                Some(path) => load_ca_file(up, path, read_file),
                None => Ok(os_store
                    .get_or_init(|| Arc::new(load_os_roots(os_roots)))
                    .clone()),
            };
            let built = roots.and_then(|roots| build_upstream(up, roots));
            by_upstream.insert(name.to_string(), built.map(Arc::new));
        }
        TlsContexts { by_upstream }
    }

    /// The upstream's context: `None` when it is not an `https` upstream of this configuration;
    /// `Some(Err)` when its TLS material could not be loaded (the upstream is disabled).
    pub fn get(&self, upstream: &str) -> Option<&Result<Arc<UpstreamTls>, TlsSetupError>> {
        self.by_upstream.get(upstream)
    }

    /// Every upstream disabled by its TLS material (logged at start).
    pub fn errors(&self) -> Vec<&TlsSetupError> {
        let mut v: Vec<_> = self
            .by_upstream
            .values()
            .filter_map(|r| r.as_ref().err())
            .collect();
        v.sort_by(|a, b| a.upstream.cmp(&b.upstream));
        v
    }
}

fn load_os_roots(src: &OsRoots) -> RootCertStore {
    let mut store = RootCertStore::empty();
    match src {
        OsRoots::Native => {
            let r = rustls_native_certs::load_native_certs();
            for e in &r.errors {
                tracing::warn!(error = %e, "http: an OS root certificate source could not be read");
            }
            let (added, ignored) = store.add_parsable_certificates(r.certs);
            if ignored > 0 {
                tracing::warn!(
                    ignored,
                    "http: OS root certificates rustls cannot use were skipped"
                );
            }
            if added == 0 {
                tracing::warn!(
                    "http: the OS root store is EMPTY; every https upstream without CA_FILE will \
                     fail certificate verification (tls_verify)"
                );
            }
        }
        OsRoots::Fixed(certs) => {
            store.add_parsable_certificates(certs.iter().cloned());
        }
    }
    store
}

fn load_ca_file(
    up: &Upstream,
    path: &Path,
    read_file: &dyn Fn(&Path) -> io::Result<Vec<u8>>,
) -> Result<Arc<RootCertStore>, TlsSetupError> {
    use rustls::pki_types::pem::PemObject;
    let err = |reason: String| TlsSetupError {
        upstream: up.name.clone(),
        key: "CA_FILE",
        reason,
    };
    let bytes = read_file(path).map_err(|e| err(format!("cannot be read ({})", e.kind())))?;
    let mut store = RootCertStore::empty();
    for (i, c) in CertificateDer::pem_slice_iter(&bytes).enumerate() {
        let c = c.map_err(|_| err(format!("PEM block {} is malformed", i + 1)))?;
        store.add(c).map_err(|_| {
            err(format!(
                "certificate {} is not usable as a trust anchor",
                i + 1
            ))
        })?;
    }
    if store.is_empty() {
        return Err(err("holds no PEM certificate".into()));
    }
    Ok(Arc::new(store))
}

fn build_upstream(up: &Upstream, roots: Arc<RootCertStore>) -> Result<UpstreamTls, TlsSetupError> {
    let err = |key, reason: &str| TlsSetupError {
        upstream: up.name.clone(),
        key,
        reason: reason.to_string(),
    };
    let versions: &[&'static rustls::SupportedProtocolVersion] = match up.tls.min_tls {
        MinTls::V1_2 => &[&rustls::version::TLS13, &rustls::version::TLS12],
        MinTls::V1_3 => &[&rustls::version::TLS13],
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut base = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(versions)
        .map_err(|_| err("MIN_TLS", "selects no protocol version this build supports"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    base.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
    base.resumption = Resumption::disabled(); // replaced per pool (`config_for`)
    let server_name = match up.origin.host() {
        Host::Name(n) => ServerName::try_from(n.clone())
            .map_err(|_| err("ORIGIN", "host is not a valid TLS server name"))?,
        Host::V4(a) => ServerName::IpAddress(std::net::IpAddr::V4(*a).into()),
        Host::V6(a) => ServerName::IpAddress(std::net::IpAddr::V6(*a).into()),
    };
    Ok(UpstreamTls {
        base,
        server_name,
        per_pool: Mutex::new(HashMap::new()),
        counts: HandshakeCounts::default(),
    })
}

/// The production file reader: `CA_FILE` with a size cap.
pub fn read_capped(path: &Path) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_CA_FILE_BYTES + 1)
        .read_to_end(&mut buf)?;
    if buf.len() as u64 > MAX_CA_FILE_BYTES {
        return Err(io::ErrorKind::FileTooLarge.into());
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::track::{CipherTap, Tracker};
    use crate::testcert::{self, Issued, Spec, Usage};
    use rustls::ServerConfig;
    use rustls::server::WebPkiClientVerifier;
    use std::ffi::OsString;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    /// A client context for `https://api.test` trusting `ca`, built through the production path
    /// (`HttpConfig::load` → `TlsContexts::build`, `CA_FILE` read through the seam).
    fn client_for(ca: &Issued, extra: &[(&str, &str)]) -> Arc<UpstreamTls> {
        let pem = ca.cert_pem().into_bytes();
        let mut vars: Vec<(OsString, OsString)> = vec![
            ("FERRO_UPSTREAMS".into(), "api".into()),
            (
                "FERRO_UPSTREAM_API_ORIGIN".into(),
                "https://api.test".into(),
            ),
            ("FERRO_UPSTREAM_API_CA_FILE".into(), "/ca.pem".into()),
        ];
        for (k, v) in extra {
            vars.push((format!("FERRO_UPSTREAM_API_{k}").into(), (*v).into()));
        }
        let read = move |p: &Path| {
            if p == Path::new("/ca.pem") {
                Ok(pem.clone())
            } else {
                Err(io::ErrorKind::NotFound.into())
            }
        };
        let cfg = HttpConfig::load(vars, &read);
        assert!(cfg.errors().is_empty(), "{:?}", cfg.errors());
        let ctxs = TlsContexts::build(&cfg, &OsRoots::Fixed(Vec::new()), &read);
        ctxs.get("api").unwrap().as_ref().unwrap().clone()
    }

    fn server(leaf: &Issued, tls12_only: bool, alpn: &[&[u8]]) -> Arc<ServerConfig> {
        let versions: &[&'static rustls::SupportedProtocolVersion] = if tls12_only {
            &[&rustls::version::TLS12]
        } else {
            &[&rustls::version::TLS13, &rustls::version::TLS12]
        };
        let mut c = ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(versions)
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![leaf.cert()], leaf.key())
            .unwrap();
        c.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
        Arc::new(c)
    }

    fn mtls_server(leaf: &Issued, client_ca: &Issued, tls12_only: bool) -> Arc<ServerConfig> {
        let versions: &[&'static rustls::SupportedProtocolVersion] = if tls12_only {
            &[&rustls::version::TLS12]
        } else {
            &[&rustls::version::TLS13]
        };
        let mut roots = RootCertStore::empty();
        roots.add(client_ca.cert()).unwrap();
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .unwrap();
        Arc::new(
            ServerConfig::builder_with_provider(provider())
                .with_protocol_versions(versions)
                .unwrap()
                .with_client_cert_verifier(verifier)
                .with_single_cert(vec![leaf.cert()], leaf.key())
                .unwrap(),
        )
    }

    /// Serve one TLS connection on `s`: complete the handshake, answer the first read, then hold
    /// the connection until the client goes.
    fn spawn_server(cfg: Arc<ServerConfig>, s: DuplexStream) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let Ok(mut t) = tokio_rustls::TlsAcceptor::from(cfg).accept(s).await else {
                return;
            };
            let mut b = [0u8; 1024];
            if let Ok(n) = t.read(&mut b).await
                && n > 0
            {
                let _ = t
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
                let _ = t.flush().await;
            }
            let _ = t.read(&mut b).await;
        })
    }

    async fn connect(
        ctx: &UpstreamTls,
        io: DuplexStream,
    ) -> io::Result<tokio_rustls::client::TlsStream<CipherTap<DuplexStream>>> {
        let (tap, _state) = CipherTap::new(io);
        tokio_rustls::TlsConnector::from(ctx.config_for(None))
            .connect(ctx.server_name(), tap)
            .await
    }

    fn cert_error(e: &io::Error) -> rustls::CertificateError {
        match e.get_ref().and_then(|r| r.downcast_ref::<rustls::Error>()) {
            Some(rustls::Error::InvalidCertificate(c)) => c.clone(),
            other => panic!("expected InvalidCertificate, got {other:?} ({e})"),
        }
    }

    /// The three verification refusals the suites rely on each produce THE `rustls` error they are
    /// named for — so a malformed test certificate cannot pass them for the wrong reason — and a
    /// good leaf from the same CA is the positive control.
    #[tokio::test]
    async fn verification_refusals_are_the_named_rustls_errors_and_tls_verify() {
        let ca = testcert::ca("Ferro Test CA");
        let ctx = client_for(&ca, &[]);
        let run = |leaf: Issued| {
            let ctx = ctx.clone();
            async move {
                let (c, s) = tokio::io::duplex(64 * 1024);
                let _srv = spawn_server(server(&leaf, false, &[]), s);
                connect(&ctx, c).await
            }
        };
        // Positive control.
        let good = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        run(good)
            .await
            .expect("a good leaf from the trusted CA is accepted");

        let stranger = testcert::ca("Stranger CA");
        let e = run(stranger.sign(&Spec::new("api.test", Usage::Server).dns("api.test")))
            .await
            .unwrap_err();
        assert!(matches!(
            cert_error(&e),
            rustls::CertificateError::UnknownIssuer
        ));
        assert_eq!(classify_handshake_error(&e), TlsCause::Verify);

        let e = run(ca.sign(&Spec::new("other.test", Usage::Server).dns("other.test")))
            .await
            .unwrap_err();
        assert!(
            matches!(
                cert_error(&e),
                rustls::CertificateError::NotValidForName
                    | rustls::CertificateError::NotValidForNameContext { .. }
            ),
            "{e}"
        );
        assert_eq!(classify_handshake_error(&e), TlsCause::Verify);

        let e = run(ca.sign(
            &Spec::new("api.test", Usage::Server)
                .dns("api.test")
                .expired(),
        ))
        .await
        .unwrap_err();
        assert!(
            matches!(
                cert_error(&e),
                rustls::CertificateError::Expired | rustls::CertificateError::ExpiredContext { .. }
            ),
            "{e}"
        );
        assert_eq!(classify_handshake_error(&e), TlsCause::Verify);
    }

    #[tokio::test]
    async fn version_alpn_and_eof_map_to_their_causes() {
        let ca = testcert::ca("Ferro Test CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        // MIN_TLS=1.3 against a TLS 1.2-only server.
        let ctx13 = client_for(&ca, &[("MIN_TLS", "1.3")]);
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server(&leaf, true, &[]), s);
        let e = connect(&ctx13, c).await.unwrap_err();
        assert_eq!(classify_handshake_error(&e), TlsCause::Version, "{e}");
        // Control: the default MIN_TLS=1.2 accepts the same server.
        let ctx = client_for(&ca, &[]);
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server(&leaf, true, &[]), s);
        connect(&ctx, c)
            .await
            .expect("TLS 1.2 is accepted at MIN_TLS=1.2");
        // An h2-only server refuses our `http/1.1` offer.
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server(&leaf, false, &[b"h2"]), s);
        let e = connect(&ctx, c).await.unwrap_err();
        assert_eq!(classify_handshake_error(&e), TlsCause::Alpn, "{e}");
        // Control: a server that speaks http/1.1 is accepted, and the negotiated ALPN is ours.
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server(&leaf, false, &[b"h2", b"http/1.1"]), s);
        let t = connect(&ctx, c).await.expect("http/1.1 negotiated");
        assert_eq!(t.get_ref().1.alpn_protocol(), Some(ALPN_HTTP11));
        // The peer hangs up mid-handshake: a transport failure.
        let (c, mut s) = tokio::io::duplex(64 * 1024);
        let srv = tokio::spawn(async move {
            let mut b = [0u8; 16];
            let _ = s.read(&mut b).await; // part of the ClientHello, then EOF
            drop(s);
        });
        let e = connect(&ctx, c).await.unwrap_err();
        assert_eq!(classify_handshake_error(&e), TlsCause::Handshake, "{e}");
        srv.await.unwrap();
    }

    /// **Premise P-M (the mTLS split, SPEC §21 O-F5c).** Under TLS 1.3 the client's handshake is
    /// complete once it has SENT its Finished, so a server's refusal of the client certificate — or
    /// of its absence — is an alert the client reads only afterwards: `connect` RESOLVES, and the
    /// refusal surfaces on the first read, after request bytes are already on the socket. Under
    /// TLS 1.2 the server's Finished follows its verdict, so the same refusal fails `connect` (the
    /// control). This is why mTLS is not in M6-F5a.
    #[tokio::test]
    async fn a_tls13_client_auth_refusal_arrives_after_connect_resolves() {
        let ca = testcert::ca("Ferro Test CA");
        let client_ca = testcert::ca("Client CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let ctx = client_for(&ca, &[]);

        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(mtls_server(&leaf, &client_ca, false), s);
        let mut t = connect(&ctx, c)
            .await
            .expect("TLS 1.3: connect resolves before the server's verdict");
        // The "request" is accepted by the socket …
        t.write_all(b"POST / HTTP/1.1\r\nhost: api.test\r\n\r\n")
            .await
            .unwrap();
        t.flush().await.unwrap();
        // … and the refusal arrives on the read.
        let mut b = [0u8; 64];
        let e = t.read(&mut b).await.unwrap_err();
        match e.get_ref().and_then(|r| r.downcast_ref::<rustls::Error>()) {
            Some(rustls::Error::AlertReceived(a)) => {
                assert_eq!(*a, AlertDescription::CertificateRequired)
            }
            other => panic!("expected certificate_required, got {other:?}"),
        }

        // Control: TLS 1.2 — the refusal fails `connect` itself.
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(mtls_server(&leaf, &client_ca, true), s);
        let e = connect(&ctx, c).await.unwrap_err();
        assert_eq!(classify_handshake_error(&e), TlsCause::Verify, "{e}");
    }

    /// Writes PASS until `hold` is set; then every write and flush is `Pending` (a socket that
    /// takes nothing more). Reads pass through.
    struct Holdable {
        inner: DuplexStream,
        hold: Arc<AtomicBool>,
    }
    impl AsyncRead for Holdable {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for Holdable {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.hold.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            if self.hold.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    type Stack = Tracker<tokio_rustls::client::TlsStream<CipherTap<Holdable>>>;

    /// A full tracked stack over a holdable socket: (plaintext tracker, shared state, hold switch).
    async fn tracked_stack(
        ctx: &UpstreamTls,
        leaf: &Issued,
    ) -> (
        Stack,
        Arc<crate::engine::track::TrackState>,
        Arc<AtomicBool>,
    ) {
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server(leaf, false, &[]), s);
        let hold = Arc::new(AtomicBool::new(false));
        let (tap, state) = CipherTap::new(Holdable {
            inner: c,
            hold: hold.clone(),
        });
        let tls = tokio_rustls::TlsConnector::from(ctx.config_for(None))
            .connect(ctx.server_name(), tap)
            .await
            .unwrap();
        (Tracker::over_tls(tls, state.clone()), state, hold)
    }

    /// **Premises P-A and P-C, and the `sent` rule on `https` (§23.7.1 as amended by M6-F5a).**
    /// P-A: the whole handshake is on the socket before `connect` resolves, so arming after it
    /// finds nothing pending. P-C: with the socket taking nothing, `tokio-rustls` still ACCEPTS a
    /// plaintext write into `rustls`'s buffer — the plaintext layer counts it, the wire never sees
    /// it — and `sent` stays false; once the socket takes a record, `sent` is true.
    #[tokio::test]
    async fn sent_on_https_is_the_socket_count_not_rustls_buffered_plaintext() {
        let ca = testcert::ca("Ferro Test CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let ctx = client_for(&ca, &[]);
        let (mut t, state, hold) = tracked_stack(&ctx, &leaf).await;
        assert!(state.is_tls());
        assert!(
            state.cipher_unarmed() > 0,
            "the handshake was counted unarmed"
        );
        state.arm();
        tokio::task::yield_now().await;
        assert_eq!(
            state.cipher_armed(),
            0,
            "P-A: nothing of the handshake follows arming"
        );
        hold.store(true, Ordering::SeqCst);
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            t.write(b"POST / HTTP/1.1\r\nhost: api.test\r\ncontent-length: 5\r\n\r\nhello"),
        )
        .await
        .expect("tokio-rustls answers the write without the socket")
        .unwrap();
        assert!(
            n > 0,
            "P-C: the plaintext write was ACCEPTED into rustls's buffer"
        );
        assert!(state.plaintext_sent(), "the plaintext layer counted it");
        assert_eq!(state.cipher_armed(), 0, "the socket took nothing");
        assert!(
            !state.sent(),
            "bytes in rustls's buffer never reached the wire: unsent"
        );
        // The socket takes the record: now it is sent.
        hold.store(false, Ordering::SeqCst);
        t.flush().await.unwrap();
        assert!(state.cipher_armed() > 0);
        assert!(state.sent());
    }

    /// F1a's review F-1, on the engine's stack: a plaintext write that FAILS after an earlier part
    /// of its encrypted record reached the socket is `sent`, although the plaintext layer counted
    /// nothing. (After arming the socket accepts 16 bytes once, then resets.)
    #[tokio::test]
    async fn a_failed_tls_write_after_a_record_reached_the_socket_is_sent() {
        struct OneThenFail {
            inner: DuplexStream,
            armed: Arc<AtomicBool>,
            used: bool,
        }
        impl AsyncRead for OneThenFail {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
            }
        }
        impl AsyncWrite for OneThenFail {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                if !self.armed.load(Ordering::SeqCst) {
                    return std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
                }
                if self.used {
                    return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
                }
                self.used = true;
                let n = buf.len().min(16);
                std::pin::Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
            }
            fn poll_flush(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                std::pin::Pin::new(&mut self.inner).poll_flush(cx)
            }
            fn poll_shutdown(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
            }
        }
        let ca = testcert::ca("Ferro Test CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let ctx = client_for(&ca, &[]);
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server(&leaf, false, &[]), s);
        let armed = Arc::new(AtomicBool::new(false));
        let (tap, state) = CipherTap::new(OneThenFail {
            inner: c,
            armed: armed.clone(),
            used: false,
        });
        let tls = tokio_rustls::TlsConnector::from(ctx.config_for(None))
            .connect(ctx.server_name(), tap)
            .await
            .unwrap();
        let mut t = Tracker::over_tls(tls, state.clone());
        state.arm();
        armed.store(true, Ordering::SeqCst);
        assert!(t.write(b"POST / HTTP/1.1\r\n\r\n").await.is_err());
        assert!(
            !state.plaintext_sent(),
            "the plaintext layer saw only the error"
        );
        assert_eq!(state.cipher_armed(), 16);
        assert!(state.sent(), "part of a record reached the socket");
    }

    /// Rule 5 (`track`): the plaintext tracker above TLS never sends `close_notify`. `hyper` shuts
    /// its I/O down on its own after some errors; forwarding that would write an alert record — a
    /// ciphertext byte after dispatch with no request byte in it, i.e. a never-sent request
    /// counted as sent. The control shuts TLS down directly and shows the alert WOULD count.
    #[tokio::test]
    async fn the_tracker_never_writes_close_notify() {
        let ca = testcert::ca("Ferro Test CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let ctx = client_for(&ca, &[]);
        let (mut t, state, _hold) = tracked_stack(&ctx, &leaf).await;
        state.arm();
        t.shutdown().await.unwrap();
        assert_eq!(state.cipher_armed(), 0);
        assert!(!state.sent());
        // Control: TLS's own shutdown writes the alert, and it would count.
        let (t2, state2, _hold2) = tracked_stack(&ctx, &leaf).await;
        state2.arm();
        let mut inner = t2.into_inner();
        inner.shutdown().await.unwrap();
        assert!(
            state2.cipher_armed() > 0,
            "close_notify is a ciphertext write"
        );
    }

    /// Premise P-E: a second connection on the same pool configuration resumes, once the first
    /// has read the server's tickets.
    #[tokio::test]
    async fn a_second_connection_on_one_pool_resumes() {
        let ca = testcert::ca("Ferro Test CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let ctx = client_for(&ca, &[]);
        let srv = server(&leaf, false, &[]);
        let mut kinds = Vec::new();
        for _ in 0..2 {
            let (c, s) = tokio::io::duplex(64 * 1024);
            let _srv = spawn_server(srv.clone(), s);
            let mut t = connect(&ctx, c).await.unwrap();
            kinds.push(t.get_ref().1.handshake_kind());
            t.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
            t.flush().await.unwrap();
            let mut b = [0u8; 256];
            let n = t.read(&mut b).await.unwrap();
            assert!(n > 0);
        }
        assert_eq!(
            kinds,
            vec![
                Some(rustls::HandshakeKind::Full),
                Some(rustls::HandshakeKind::Resumed)
            ]
        );
    }

    #[tokio::test]
    async fn resumption_caches_are_per_pool_and_early_data_is_off() {
        let ca = testcert::ca("Ferro Test CA");
        let ctx = client_for(&ca, &[]);
        let a = ctx.config_for(None);
        let b = ctx.config_for(None);
        let u1 = ctx.config_for(Some(1000));
        let u2 = ctx.config_for(Some(1001));
        assert!(Arc::ptr_eq(&a, &b), "one configuration per pool");
        assert!(!Arc::ptr_eq(&u1, &u2));
        assert!(!Arc::ptr_eq(&a, &u1));
        for c in [&a, &u1, &u2] {
            assert!(!c.enable_early_data);
            assert_eq!(c.alpn_protocols, vec![ALPN_HTTP11.to_vec()]);
        }
    }

    #[test]
    fn unloadable_ca_files_disable_the_upstream_without_quoting_the_value() {
        let mk = |bytes: Option<&'static [u8]>| {
            let vars: Vec<(OsString, OsString)> = vec![
                ("FERRO_UPSTREAMS".into(), "api".into()),
                (
                    "FERRO_UPSTREAM_API_ORIGIN".into(),
                    "https://api.test".into(),
                ),
                ("FERRO_UPSTREAM_API_CA_FILE".into(), "/secret/ca.pem".into()),
            ];
            let read = move |_: &Path| {
                bytes
                    .map(<[u8]>::to_vec)
                    .ok_or(io::Error::from(io::ErrorKind::NotFound))
            };
            let cfg = HttpConfig::load(vars, &read);
            let ctxs = TlsContexts::build(&cfg, &OsRoots::Fixed(Vec::new()), &read);
            let e = ctxs.errors();
            assert_eq!(e.len(), 1);
            let s = e[0].to_string();
            assert!(s.contains("FERRO_UPSTREAM_API_CA_FILE"), "{s}");
            assert!(!s.contains("/secret"), "never the value: {s}");
            assert!(s.ends_with("the upstream is disabled"));
            assert!(matches!(ctxs.get("api"), Some(Err(_))));
        };
        mk(None);
        mk(Some(b"not pem at all"));
        mk(Some(
            b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        ));
    }
}
