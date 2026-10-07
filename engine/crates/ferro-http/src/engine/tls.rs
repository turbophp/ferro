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
//! **mTLS (slice M6-F5c, SPEC D23).** `CLIENT_CERT_FILE` (a PEM chain, end-entity first) and
//! `CLIENT_KEY_FILE` (one PEM private key) are read HERE, at start, with the roots — never per
//! dial — and become the base configuration's client-certificate resolver, so every pool's
//! configuration (each a clone of the base) presents the same certificate on every full handshake,
//! fresh dial or new partition alike. Material that cannot be loaded — unreadable, not PEM, no
//! certificate or no key, a certificate that does not parse, a key `ring` cannot use, or a key that
//! does not match the end-entity certificate — DISABLES the upstream under the `CA_FILE` rule above
//! (`forbidden_upstream`, before validation), naming the key and never the value. **Key custody**
//! (§23.3.2): the key's bytes exist only inside this load and inside `rustls`'s signing key; the
//! file buffer is overwritten before it is freed (best effort — the DER copy `rustls` consumes and
//! the live signing key are not ours to wipe), nothing here derives `Debug` over them, and no error
//! quotes a path or the file's contents.
//!
//! **A refusal of the client certificate** (or of its absence) differs by version, measured by
//! `a_tls13_client_auth_refusal_arrives_after_connect_resolves` below. Under TLS 1.2 the server's
//! verdict precedes its Finished, so the refusal fails `connect`: before dispatch, `tls_verify`.
//! Under TLS 1.3 the client's handshake is complete once it has sent its Finished, so the
//! refusal is an alert read AFTER dispatch, with the request's records already on the socket — and
//! by SPEC D23 it is "sent, no head": a POST `Indeterminate`, a declared-idempotent request
//! `Retryable`, never re-sent, and its cause token is the one §23.5.6's "sent, no head" group
//! derives for that read failure (`reset`), NEVER a `tls_*` token (§23.11.3 maps `tls_*` to
//! `ConnectException`, which naive deciders retry). [`client_auth_alert`] recognises the alert so
//! the engine can SAY what happened (the terminal's message and a `warn` naming the upstream); it
//! never changes the fate or the token.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use rustls::client::Resumption;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{AlertDescription, ClientConfig, RootCertStore};

use crate::config::{
    HttpConfig, MinTls, NotRegularFile, Upstream, UpstreamEntry, env_name, open_regular,
};
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

/// A TLS file (`CA_FILE`, `CLIENT_CERT_FILE`, `CLIENT_KEY_FILE`) larger than this is refused (an OS
/// bundle is ~200 KiB).
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
            a if is_client_auth_alert(*a) => TlsCause::Verify,
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

/// Whether a peer's alert refuses OUR certificate, or its absence (`bad_certificate`,
/// `unknown_ca`, `certificate_required`, …). Inside a handshake that is `tls_verify`
/// ([`classify_handshake_error`]); read after dispatch (TLS 1.3 client authentication, SPEC D23) it
/// is only NAMED ([`client_auth_alert`]) — the fate and the cause token stay "sent, no head"'s.
pub fn is_client_auth_alert(a: AlertDescription) -> bool {
    matches!(
        a,
        AlertDescription::BadCertificate
            | AlertDescription::UnsupportedCertificate
            | AlertDescription::CertificateRevoked
            | AlertDescription::CertificateExpired
            | AlertDescription::CertificateUnknown
            | AlertDescription::UnknownCA
            | AlertDescription::AccessDenied
            | AlertDescription::CertificateRequired
    )
}

/// The client-certificate refusal somewhere in an exchange's error chain (a `hyper` error whose
/// I/O cause is `tokio-rustls`'s `io::Error` wrapping `rustls::Error::AlertReceived`), if any.
/// `io::Error::source` skips a custom error's own payload, so each `io::Error` is opened with
/// `get_ref` rather than only followed through `source`.
pub fn client_auth_alert(err: &(dyn std::error::Error + 'static)) -> Option<AlertDescription> {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        let tls = match e.downcast_ref::<io::Error>() {
            Some(io) => io.get_ref().and_then(|r| r.downcast_ref::<rustls::Error>()),
            None => e.downcast_ref::<rustls::Error>(),
        };
        if let Some(rustls::Error::AlertReceived(a)) = tls
            && is_client_auth_alert(*a)
        {
            return Some(*a);
        }
        cur = e.source();
    }
    None
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
            // The client certificate and key are TLS material too: loaded here, at start, beside
            // the roots, and a failure disables the upstream under the same rule (M6-F5c).
            let built = roots.and_then(|roots| {
                let client = load_client_auth(up, read_file)?;
                build_upstream(up, roots, client)
            });
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
    let bytes = read_file(path).map_err(|e| err(read_reason(&e)))?;
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

/// An upstream's client certificate and key (mTLS, slice M6-F5c), loaded at start. Deliberately not
/// `Debug`: the key is credential material (§23.3.2).
struct ClientAuth {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

/// Overwrite a buffer that held key material before it is freed. Best effort: `black_box` keeps the
/// writes from being elided as dead stores. [`read_capped`] pre-sizes its buffer so a file that does
/// not change while it is read leaves no freed copy behind. Out of reach: a reallocation if the file
/// grows mid-read, the DER copy the PEM decoder makes and hands to `rustls`, and `rustls`'s own
/// signing key, which stays resident for the daemon's life (this crate forbids `unsafe` and adds no
/// crate for this).
fn wipe(buf: &mut [u8]) {
    buf.fill(0);
    std::hint::black_box(&*buf);
}

/// Load `CLIENT_CERT_FILE` and `CLIENT_KEY_FILE` (both set or neither: `config` refuses a half
/// pair). `None` when the upstream has no client certificate. Every failure names the key, never
/// the path or the file's contents.
fn load_client_auth(
    up: &Upstream,
    read_file: &dyn Fn(&Path) -> io::Result<Vec<u8>>,
) -> Result<Option<ClientAuth>, TlsSetupError> {
    let (Some(cert_path), Some(key_path)) = (&up.tls.client_cert_file, &up.tls.client_key_file)
    else {
        return Ok(None);
    };
    let err = |key: &'static str, reason: String| TlsSetupError {
        upstream: up.name.clone(),
        key,
        reason,
    };
    let mut bytes = read_file(cert_path).map_err(|e| err("CLIENT_CERT_FILE", read_reason(&e)))?;
    let chain = take_chain(&mut bytes).map_err(|r| err("CLIENT_CERT_FILE", r))?;
    let mut bytes = read_file(key_path).map_err(|e| err("CLIENT_KEY_FILE", read_reason(&e)))?;
    let key = take_key(&mut bytes).map_err(|r| err("CLIENT_KEY_FILE", r.into()))?;
    Ok(Some(ClientAuth { chain, key }))
}

/// Why a TLS file could not be read, for [`TlsSetupError::reason`]: the I/O error's KIND only, or
/// "is not a regular file" ([`read_capped`]'s refusal of a FIFO, a device, a directory).
fn read_reason(e: &io::Error) -> String {
    if e.get_ref().is_some_and(|r| r.is::<NotRegularFile>()) {
        "is not a regular file".into()
    } else {
        format!("cannot be read ({})", e.kind())
    }
}

/// Parse `CLIENT_CERT_FILE`'s bytes — one or more PEM certificates, end-entity first — and
/// [`wipe`] them on every path (a combined file, certificate and key in one, is common, and its
/// key is skipped here but must not linger).
///
/// **Only what `rustls` itself needs is checked** (M6-F5c review F2). Every block must be
/// well-formed DER — one outer `SEQUENCE` covering the block exactly — and the END-ENTITY must
/// parse as the X.509 v3 certificate `rustls` matches the key against (`webpki`'s strict parser;
/// `rustls` would refuse the pair at the same point). The rest of the chain is sent as the operator
/// wrote it and judged by the SERVER: `webpki`'s strict parser refuses a v1 root, which real
/// servers (OpenSSL's, measured by the review) accept in a client chain.
fn take_chain(bytes: &mut [u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    use rustls::pki_types::pem::PemObject;
    let mut chain = Vec::new();
    let mut failure = None;
    for (i, c) in CertificateDer::pem_slice_iter(bytes).enumerate() {
        let Ok(c) = c else {
            failure = Some(format!("PEM block {} is malformed", i + 1));
            break;
        };
        if !is_der_sequence(&c) {
            failure = Some(format!("certificate {} is not DER-encoded", i + 1));
            break;
        }
        if i == 0 && rustls::server::ParsedCertificate::try_from(&c).is_err() {
            failure = Some(
                "certificate 1 (the end-entity) is not an X.509 v3 certificate rustls can match \
                 to the key"
                    .into(),
            );
            break;
        }
        chain.push(c);
    }
    wipe(bytes);
    match failure {
        Some(f) => Err(f),
        None if chain.is_empty() => Err("holds no PEM certificate".into()),
        None => Ok(chain),
    }
}

/// Whether `b` is exactly one DER `SEQUENCE` (tag `0x30`, a definite length, nothing after it):
/// the outer shape every X.509 certificate has.
fn is_der_sequence(b: &[u8]) -> bool {
    let [0x30, first, rest @ ..] = b else {
        return false;
    };
    let (len, header) = if first & 0x80 == 0 {
        (usize::from(*first), 0)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 4 || rest.len() < n {
            return false;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |a, &x| (a << 8) | usize::from(x));
        (len, n)
    };
    rest.len() == header + len
}

/// Parse `CLIENT_KEY_FILE`'s bytes — exactly one PEM private key (PKCS#8, PKCS#1 or SEC1) — and
/// [`wipe`] them, on every path, before returning.
fn take_key(bytes: &mut [u8]) -> Result<PrivateKeyDer<'static>, &'static str> {
    use rustls::pki_types::pem::PemObject;
    let mut keys = PrivateKeyDer::pem_slice_iter(bytes);
    let key = match (keys.next(), keys.next()) {
        (Some(Ok(k)), None) => Ok(k),
        (None, _) => Err("holds no PEM private key"),
        (Some(Err(_)), _) | (Some(Ok(_)), Some(Err(_))) => Err("holds a malformed PEM block"),
        (Some(Ok(_)), Some(Ok(_))) => Err("holds more than one private key"),
    };
    drop(keys);
    wipe(bytes);
    key
}

fn build_upstream(
    up: &Upstream,
    roots: Arc<RootCertStore>,
    client: Option<ClientAuth>,
) -> Result<UpstreamTls, TlsSetupError> {
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
    let builder = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(versions)
        .map_err(|_| err("MIN_TLS", "selects no protocol version this build supports"))?
        .with_root_certificates(roots);
    // mTLS: the certificate goes into the BASE configuration, which every pool's configuration
    // clones (`config_for`), so no pool, partition or fresh dial can present less than the operator
    // configured.
    let mut base =
        match client {
            None => builder.with_no_client_auth(),
            Some(ClientAuth { chain, key }) => builder.with_client_auth_cert(chain, key).map_err(
                |e| match e {
                    rustls::Error::InconsistentKeys(_) => err(
                        "CLIENT_KEY_FILE",
                        "does not match CLIENT_CERT_FILE's first (end-entity) certificate",
                    ),
                    _ => err(
                        "CLIENT_KEY_FILE",
                        "is not a private key this build can use (RSA, ECDSA P-256/P-384, Ed25519)",
                    ),
                },
            )?,
        };
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

/// The production file reader for TLS material (`CA_FILE`, `CLIENT_CERT_FILE`, `CLIENT_KEY_FILE`),
/// with a size cap.
///
/// - **Regular files only** ([`open_regular`]): a FIFO would block startup forever (review F6).
/// - **Pre-sized from the file's length** (review F3), so a file that does not change while it is
///   read is read with no reallocation, and `read_to_end`'s growth leaves no freed copy of key
///   material behind. A file that grows meanwhile can still reallocate — best effort.
/// - **Wiped on every error path**, including the over-cap refusal, before the buffer is freed.
pub fn read_capped(path: &Path) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let (file, len) = open_regular(path)?;
    if len > MAX_CA_FILE_BYTES {
        return Err(io::ErrorKind::FileTooLarge.into());
    }
    let mut buf = Vec::with_capacity(usize::try_from(len).unwrap_or(0).saturating_add(1));
    let r = file.take(MAX_CA_FILE_BYTES + 1).read_to_end(&mut buf);
    match r {
        Ok(_) if buf.len() as u64 <= MAX_CA_FILE_BYTES => Ok(buf),
        Ok(_) => {
            wipe(&mut buf);
            Err(io::ErrorKind::FileTooLarge.into())
        }
        Err(e) => {
            wipe(&mut buf);
            Err(e)
        }
    }
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
        contexts_with(ca, extra, Vec::new())
            .get("api")
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
    }

    /// The contexts for `https://api.test` trusting `ca`, with `extra` keys and `files` (path →
    /// contents) served through the read seam beside `/ca.pem`.
    fn contexts_with(
        ca: &Issued,
        extra: &[(&str, &str)],
        files: Vec<(&'static str, Vec<u8>)>,
    ) -> TlsContexts {
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
                return Ok(pem.clone());
            }
            files
                .iter()
                .find(|(f, _)| p == Path::new(f))
                .map(|(_, b)| b.clone())
                .ok_or_else(|| io::ErrorKind::NotFound.into())
        };
        let cfg = HttpConfig::load(vars, &read);
        assert!(cfg.errors().is_empty(), "{:?}", cfg.errors());
        TlsContexts::build(&cfg, &OsRoots::Fixed(Vec::new()), &read)
    }

    /// A client context presenting `client` (cert at `/client.pem`, key at `/client.key`).
    fn mtls_client_for(ca: &Issued, client: &Issued) -> Arc<UpstreamTls> {
        contexts_with(
            ca,
            &[
                ("CLIENT_CERT_FILE", "/client.pem"),
                ("CLIENT_KEY_FILE", "/client.key"),
            ],
            vec![
                ("/client.pem", client.cert_pem().into_bytes()),
                ("/client.key", client.key_pem().into_bytes()),
            ],
        )
        .get("api")
        .unwrap()
        .as_ref()
        .unwrap_or_else(|e| panic!("{e}"))
        .clone()
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

    /// **Premise P-M (the mTLS split; raised as O-F5c, decided as SPEC D23).** Under TLS 1.3 the
    /// client's handshake is complete once it has SENT its Finished, so a server's refusal of the
    /// client certificate — or of its absence — is an alert the client reads only afterwards:
    /// `connect` RESOLVES, and the refusal surfaces on the first read, after request bytes are
    /// already on the socket. Under TLS 1.2 the server's Finished follows its verdict, so the same
    /// refusal fails `connect` (the control). This is why mTLS waited for D23 and is M6-F5c.
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
        assert_eq!(
            client_auth_alert(&e),
            Some(AlertDescription::CertificateRequired),
            "the engine can name it"
        );

        // Control: TLS 1.2 — the refusal fails `connect` itself.
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(mtls_server(&leaf, &client_ca, true), s);
        let e = connect(&ctx, c).await.unwrap_err();
        assert_eq!(classify_handshake_error(&e), TlsCause::Verify, "{e}");
    }

    /// One mTLS handshake from `ctx`'s pool `partition` against `server`, then a request: the
    /// server's answer, or the error that refused it.
    async fn mtls_exchange(
        ctx: &UpstreamTls,
        partition: Option<u32>,
        server: Arc<ServerConfig>,
    ) -> io::Result<Vec<u8>> {
        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(server, s);
        let (tap, _state) = CipherTap::new(c);
        let mut t = tokio_rustls::TlsConnector::from(ctx.config_for(partition))
            .connect(ctx.server_name(), tap)
            .await?;
        t.write_all(b"GET / HTTP/1.1\r\nhost: api.test\r\n\r\n")
            .await?;
        t.flush().await?;
        let mut b = [0u8; 64];
        let n = t.read(&mut b).await?;
        Ok(b[..n].to_vec())
    }

    /// M6-F5c: a configured client certificate is presented and ACCEPTED by a server that requires
    /// one, under TLS 1.3 and TLS 1.2, from EVERY pool's configuration (each a clone of the base:
    /// the unpartitioned pool and two `PARTITION=uid` pools) — the positive control for every
    /// refusal below. Under TLS 1.3 `connect` resolving proves nothing (P-M), so acceptance is the
    /// server's ANSWER to a request.
    #[tokio::test]
    async fn a_configured_client_certificate_is_presented_from_every_pool() {
        let ca = testcert::ca("Ferro Test CA");
        let client_ca = testcert::ca("Client CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let client = client_ca.sign(&Spec::new("ferro-client", Usage::Client));
        let ctx = mtls_client_for(&ca, &client);
        for tls12_only in [false, true] {
            for partition in [None, Some(1000), Some(1001)] {
                let answer =
                    mtls_exchange(&ctx, partition, mtls_server(&leaf, &client_ca, tls12_only))
                        .await
                        .unwrap_or_else(|e| {
                            panic!("tls12_only={tls12_only} partition={partition:?}: refused: {e}")
                        });
                assert!(answer.starts_with(b"HTTP/1.1 200"), "{answer:?}");
            }
        }
    }

    /// D23's second refusal shape: a configured certificate the server REJECTS (issued by a CA it
    /// does not trust). Under TLS 1.3 it, too, arrives after `connect` resolves, and
    /// [`client_auth_alert`] names it; under TLS 1.2 it fails `connect` as `tls_verify`.
    #[tokio::test]
    async fn a_rejected_client_certificate_is_read_after_a_tls13_connect_and_is_tls_verify_under_tls12()
     {
        let ca = testcert::ca("Ferro Test CA");
        let client_ca = testcert::ca("Client CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let stranger = testcert::ca("Stranger CA");
        let rejected = stranger.sign(&Spec::new("ferro-client", Usage::Client));
        let ctx = mtls_client_for(&ca, &rejected);

        let e = mtls_exchange(&ctx, None, mtls_server(&leaf, &client_ca, false))
            .await
            .unwrap_err();
        let alert = client_auth_alert(&e).unwrap_or_else(|| panic!("a named refusal, got {e}"));
        assert!(is_client_auth_alert(alert), "{alert:?}");

        let (c, s) = tokio::io::duplex(64 * 1024);
        let _srv = spawn_server(mtls_server(&leaf, &client_ca, true), s);
        let (tap, _state) = CipherTap::new(c);
        let e = tokio_rustls::TlsConnector::from(ctx.config_for(None))
            .connect(ctx.server_name(), tap)
            .await
            .unwrap_err();
        assert_eq!(classify_handshake_error(&e), TlsCause::Verify, "{e}");
    }

    /// [`client_auth_alert`] reads through an error chain (as `hyper` wraps the I/O error) and names
    /// only an alert about OUR certificate — never another alert, never a plain I/O failure.
    #[test]
    fn client_auth_alert_reads_through_a_chain_and_names_only_certificate_alerts() {
        #[derive(Debug)]
        struct Wrap(io::Error);
        impl std::fmt::Display for Wrap {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "wrapped")
            }
        }
        impl std::error::Error for Wrap {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let alert = |a| io::Error::new(io::ErrorKind::InvalidData, rustls::Error::AlertReceived(a));
        use AlertDescription as A;
        assert_eq!(
            client_auth_alert(&Wrap(alert(A::CertificateRequired))),
            Some(A::CertificateRequired)
        );
        assert_eq!(client_auth_alert(&alert(A::UnknownCA)), Some(A::UnknownCA));
        assert_eq!(client_auth_alert(&Wrap(alert(A::HandshakeFailure))), None);
        assert_eq!(client_auth_alert(&Wrap(alert(A::DecodeError))), None);
        assert_eq!(
            client_auth_alert(&Wrap(io::ErrorKind::ConnectionReset.into())),
            None
        );
    }

    /// Review F2: only what `rustls` needs is checked. A chain whose second certificate is an X.509
    /// **v1** root — `openssl x509 -req -signkey`'s shape, which `webpki`'s strict parser refuses
    /// and real servers accept — LOADS, is presented, and a server that trusts that root serves
    /// the request. (The garbage-DER control is in the unloadable-material test.)
    #[tokio::test]
    async fn a_v1_certificate_in_the_client_chain_is_sent_not_refused() {
        let ca = testcert::ca("Ferro Test CA");
        let leaf = ca.sign(&Spec::new("api.test", Usage::Server).dns("api.test"));
        let v1_root = testcert::ca_v1("V1 Root");
        assert!(
            rustls::server::ParsedCertificate::try_from(&v1_root.cert()).is_err(),
            "premise: webpki's strict parser refuses the v1 root"
        );
        let client = v1_root.sign(&Spec::new("ferro-client", Usage::Client));
        let chain = [client.cert_pem(), v1_root.cert_pem()].concat();
        let ctxs = contexts_with(
            &ca,
            &[
                ("CLIENT_CERT_FILE", "/chain.pem"),
                ("CLIENT_KEY_FILE", "/client.key"),
            ],
            vec![
                ("/chain.pem", chain.into_bytes()),
                ("/client.key", client.key_pem().into_bytes()),
            ],
        );
        assert!(ctxs.errors().is_empty(), "{:?}", ctxs.errors());
        let ctx = ctxs.get("api").unwrap().as_ref().unwrap().clone();
        let answer = mtls_exchange(&ctx, None, mtls_server(&leaf, &v1_root, false))
            .await
            .expect("a server that trusts the v1 root accepts the chain");
        assert!(answer.starts_with(b"HTTP/1.1 200"), "{answer:?}");
    }

    /// Review F3: the CERTIFICATE file's buffer is wiped too — a combined file (certificate and key
    /// in one, common for curl's `cert` and HAProxy) loads as `CLIENT_CERT_FILE`, and its key must
    /// not linger — on success and on a refusal.
    #[test]
    fn the_cert_file_buffer_is_wiped_and_a_combined_file_loads() {
        let ca = testcert::ca("Ferro Test CA");
        let client = ca.sign(&Spec::new("ferro-client", Usage::Client));
        let mut combined = [client.cert_pem(), client.key_pem()].concat().into_bytes();
        assert_eq!(take_chain(&mut combined).map(|c| c.len()), Ok(1));
        assert!(combined.iter().all(|b| *b == 0), "wiped on success");
        let mut bad = [
            client.key_pem().as_bytes(),
            b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n",
        ]
        .concat();
        assert!(take_chain(&mut bad).is_err());
        assert!(bad.iter().all(|b| *b == 0), "wiped on a refusal");
    }

    /// Review F6: a FIFO (or any non-regular file) configured as `CA_FILE`, `CLIENT_CERT_FILE` or
    /// `CLIENT_KEY_FILE` is refused WITHOUT being opened — opening a FIFO for reading blocks until a
    /// writer appears, i.e. forever at start — and disables the upstream naming the key. The
    /// attached-header reader refuses it the same way. Each call is bounded by a 5 s watchdog, so a
    /// regression fails instead of hanging the suite.
    #[test]
    fn a_fifo_tls_file_is_refused_without_blocking() {
        let dir = std::env::temp_dir().join(format!("ferro-f5c-fifo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let fifo = dir.join("material.pem");
        let ok = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo runs");
        assert!(ok.success());
        let within = |f: Box<dyn FnOnce() -> String + Send>| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(f());
            });
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .expect("returned instead of blocking on the FIFO")
        };
        let p = fifo.clone();
        let e = within(Box::new(move || {
            format!("{:?}", read_capped(&p).map(|_| ()).unwrap_err())
        }));
        assert!(e.contains("NotRegularFile"), "{e}");
        let p = fifo.clone();
        let e = within(Box::new(move || {
            format!(
                "{:?}",
                crate::config::read_capped(&p).map(|_| ()).unwrap_err()
            )
        }));
        assert!(e.contains("NotRegularFile"), "{e}");
        // Through the production reader: each TLS key disables the upstream, naming the key.
        let ca = testcert::ca("Ferro Test CA");
        let client = ca.sign(&Spec::new("ferro-client", Usage::Client));
        let ca_pem = dir.join("ca.pem");
        let cert_pem = dir.join("client.pem");
        let key_pem = dir.join("client.key");
        std::fs::write(&ca_pem, ca.cert_pem()).unwrap();
        std::fs::write(&cert_pem, client.cert_pem()).unwrap();
        std::fs::write(&key_pem, client.key_pem()).unwrap();
        for bad in ["CA_FILE", "CLIENT_CERT_FILE", "CLIENT_KEY_FILE"] {
            let path = |k: &str, good: &Path| {
                if k == bad {
                    fifo.display().to_string()
                } else {
                    good.display().to_string()
                }
            };
            let vars: Vec<(OsString, OsString)> = vec![
                ("FERRO_UPSTREAMS".into(), "api".into()),
                (
                    "FERRO_UPSTREAM_API_ORIGIN".into(),
                    "https://api.test".into(),
                ),
                (
                    "FERRO_UPSTREAM_API_CA_FILE".into(),
                    path("CA_FILE", &ca_pem).into(),
                ),
                (
                    "FERRO_UPSTREAM_API_CLIENT_CERT_FILE".into(),
                    path("CLIENT_CERT_FILE", &cert_pem).into(),
                ),
                (
                    "FERRO_UPSTREAM_API_CLIENT_KEY_FILE".into(),
                    path("CLIENT_KEY_FILE", &key_pem).into(),
                ),
            ];
            let s = within(Box::new(move || {
                let cfg = HttpConfig::load(vars, &read_capped);
                let ctxs = TlsContexts::build(&cfg, &OsRoots::Fixed(Vec::new()), &read_capped);
                ctxs.errors()
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            }));
            assert!(
                s.contains(&format!("FERRO_UPSTREAM_API_{bad} is not a regular file")),
                "{bad}: {s}"
            );
        }
        // The control: the same three regular files load.
        let vars: Vec<(OsString, OsString)> = vec![
            ("FERRO_UPSTREAMS".into(), "api".into()),
            (
                "FERRO_UPSTREAM_API_ORIGIN".into(),
                "https://api.test".into(),
            ),
            ("FERRO_UPSTREAM_API_CA_FILE".into(), ca_pem.clone().into()),
            (
                "FERRO_UPSTREAM_API_CLIENT_CERT_FILE".into(),
                cert_pem.clone().into(),
            ),
            (
                "FERRO_UPSTREAM_API_CLIENT_KEY_FILE".into(),
                key_pem.clone().into(),
            ),
        ];
        let cfg = HttpConfig::load(vars, &read_capped);
        let ctxs = TlsContexts::build(&cfg, &OsRoots::Fixed(Vec::new()), &read_capped);
        assert!(ctxs.errors().is_empty(), "{:?}", ctxs.errors());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Key custody (§23.3.2): the key file's buffer is overwritten once it is parsed — on success
    /// and on every refusal — so the PEM does not linger in the buffer this crate owns.
    #[test]
    fn the_key_file_buffer_is_wiped_on_every_path() {
        let ca = testcert::ca("Ferro Test CA");
        let a = ca.sign(&Spec::new("a", Usage::Client)).key_pem();
        let b = ca.sign(&Spec::new("b", Usage::Client)).key_pem();
        let cases: [(Vec<u8>, bool); 4] = [
            (a.clone().into_bytes(), true),
            ([a.as_bytes(), b.as_bytes()].concat(), false),
            (b"no key here".to_vec(), false),
            (
                b"-----BEGIN PRIVATE KEY-----\n!!!!\n-----END PRIVATE KEY-----\n".to_vec(),
                false,
            ),
        ];
        for (mut buf, ok) in cases {
            assert_eq!(take_key(&mut buf).is_ok(), ok);
            assert!(!buf.is_empty() && buf.iter().all(|b| *b == 0), "wiped");
        }
    }

    /// M6-F5c: client certificate or key material that cannot be loaded DISABLES the upstream at
    /// start — the `CA_FILE` rule — naming the upstream and the key, never the path or the contents.
    /// Every case is paired with the good pair as its control.
    #[test]
    fn unloadable_client_material_disables_the_upstream_naming_the_key_never_the_value() {
        const CERT: &str = "CLIENT_CERT_FILE";
        const KEY: &str = "CLIENT_KEY_FILE";
        let ca = testcert::ca("Ferro Test CA");
        let client = ca.sign(&Spec::new("ferro-client", Usage::Client));
        let other = ca.sign(&Spec::new("someone-else", Usage::Client));
        let cert = client.cert_pem().into_bytes();
        let key = client.key_pem().into_bytes();
        let garbage_cert =
            b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".to_vec();
        let garbage_key =
            b"-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n".to_vec();
        let bad_b64_cert: &[u8] = b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n";
        let bad_b64_key: &[u8] = b"-----BEGIN PRIVATE KEY-----\n!!!!\n-----END PRIVATE KEY-----\n";
        let v1_leaf = ca.sign(&Spec::new("ferro-client", Usage::Client).v1());
        let cat = |a: &[u8], b: &[u8]| [a, b].concat();
        type Case = (
            &'static str,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            &'static str,
            &'static str,
        );
        let cases: Vec<Case> = vec![
            (
                "cert missing",
                None,
                Some(key.clone()),
                CERT,
                "cannot be read",
            ),
            (
                "key missing",
                Some(cert.clone()),
                None,
                KEY,
                "cannot be read",
            ),
            (
                "cert not PEM",
                Some(b"not pem at all".to_vec()),
                Some(key.clone()),
                CERT,
                "holds no PEM certificate",
            ),
            (
                "cert block not DER",
                Some(garbage_cert.clone()),
                Some(key.clone()),
                CERT,
                "certificate 1 is not DER-encoded",
            ),
            (
                "good cert, then a block not DER (the control for the v1 chain test)",
                Some(cat(&cert, &garbage_cert)),
                Some(key.clone()),
                CERT,
                "certificate 2 is not DER-encoded",
            ),
            (
                "cert, then a malformed PEM block",
                Some(cat(&cert, bad_b64_cert)),
                Some(key.clone()),
                CERT,
                "PEM block 2 is malformed",
            ),
            (
                "a v1 end-entity (rustls cannot match it to the key)",
                Some(v1_leaf.cert_pem().into_bytes()),
                Some(v1_leaf.key_pem().into_bytes()),
                CERT,
                "certificate 1 (the end-entity) is not an X.509 v3 certificate",
            ),
            (
                "good key, then a malformed PEM block (the twin of the cert-side case)",
                Some(cert.clone()),
                Some(cat(&key, bad_b64_key)),
                KEY,
                "holds a malformed PEM block",
            ),
            (
                "key file holds only a certificate",
                Some(cert.clone()),
                Some(cert.clone()),
                KEY,
                "holds no PEM private key",
            ),
            (
                "two keys",
                Some(cert.clone()),
                Some(cat(&key, other.key_pem().as_bytes())),
                KEY,
                "more than one private key",
            ),
            (
                "key unusable",
                Some(cert.clone()),
                Some(garbage_key),
                KEY,
                "this build can use",
            ),
            (
                "key does not match the certificate",
                Some(cert.clone()),
                Some(other.key_pem().into_bytes()),
                KEY,
                "does not match",
            ),
        ];
        let paths = [(CERT, "/secret/client.pem"), (KEY, "/secret/client.key")];
        for (what, c, k, bad_key, reason) in cases {
            let mut files = Vec::new();
            if let Some(c) = c {
                files.push(("/secret/client.pem", c));
            }
            if let Some(k) = k {
                files.push(("/secret/client.key", k));
            }
            let ctxs = contexts_with(&ca, &paths, files);
            let errs = ctxs.errors();
            assert_eq!(errs.len(), 1, "{what}");
            assert_eq!(errs[0].key, bad_key, "{what}: {}", errs[0]);
            let s = errs[0].to_string();
            assert!(
                s.contains(&format!("FERRO_UPSTREAM_API_{bad_key}")),
                "{what}: {s}"
            );
            assert!(s.contains(reason), "{what}: {s}");
            assert!(!s.contains("/secret"), "{what}: never the path: {s}");
            assert!(!s.contains("BEGIN"), "{what}: never the contents: {s}");
            assert!(s.ends_with("the upstream is disabled"), "{what}: {s}");
            assert!(matches!(ctxs.get("api"), Some(Err(_))), "{what}");
        }
        // The control: the good pair loads.
        let ctxs = contexts_with(
            &ca,
            &paths,
            vec![("/secret/client.pem", cert), ("/secret/client.key", key)],
        );
        assert!(ctxs.errors().is_empty());
        assert!(matches!(ctxs.get("api"), Some(Ok(_))));
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
        // A reused connection's next exchange starts from zero (review F-4's rule, ciphertext half).
        state.arm();
        assert_eq!(state.cipher_armed(), 0);
        assert!(!state.sent());
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
