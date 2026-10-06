//! DNS, the address guard and pinning (SPEC §23.8.5), and the TCP dial (§23.6 step 5).
//!
//! **Pinning.** Every resolved address is checked with the upstream's [`AddressPolicy`] (F3's
//! classification), and the TCP connect goes to EXACTLY that checked `SocketAddr` — nothing
//! re-resolves between the check and the connect, and a connection keeps its address for life
//! (`HttpConn::peer`). Refused addresses in a mixed answer are skipped; if none remain the dial is a
//! `forbidden_address` refusal, made before any connect.
//!
//! **Resolution** is a seam ([`Resolve`]): production uses `getaddrinfo` through
//! `tokio::net::lookup_host`, so `/etc/hosts` and nsswitch behave as they do for curl; tests inject
//! a fixed answer (the only way to make a hostname resolve to `fd00:ec2::254` on a test host).
//! Positive answers are cached per upstream for `DNS_TTL_MS`; negative answers are not cached, and a
//! failed dial drops the cached answer, so the next dial re-resolves (§23.8.5: "new resolution
//! happens only on a new dial … after a failure").
//!
//! **Not done here (stated):** Happy Eyeballs (deferred by §23.8.5 — addresses are tried in resolver
//! order); the connect budget is shared across the admitted addresses, so a dead address costs one
//! share of `CONNECT_TIMEOUT_MS`, never the whole of it, unless it is the last.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::address::{AddressRefusal, Nat64Prefixes};
use crate::config::Upstream;
use crate::origin::Host;

/// A boxed, `Send` future (the resolver seam's return type).
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The resolver seam. `resolve` returns the answer in resolver order.
pub trait Resolve: Send + Sync + 'static {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFut<'a, io::Result<Vec<IpAddr>>>;
}

/// A connection's byte stream, as the engine sees it.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Io for T {}

/// A boxed connection.
pub type BoxIo = Box<dyn Io>;

/// The connector seam: open a stream to EXACTLY `peer`, the address the guard checked (pinning).
/// Production is TCP ([`TcpConnect`]). The seam exists for fault injection BELOW `hyper`: on real
/// TCP, `hyper`'s idle connection task notices a closed socket before a request can be written to
/// it, so the "dispatched, not sent" cell (§23.7.1) is reachable only through a socket that refuses
/// the write — which a test can build, and a loopback upstream cannot.
pub trait Connect: Send + Sync + 'static {
    fn connect<'a>(&'a self, peer: SocketAddr) -> BoxFut<'a, io::Result<BoxIo>>;
}

/// TCP, with `TCP_NODELAY` (§23.6 step 5).
#[derive(Debug, Default)]
pub struct TcpConnect;

impl Connect for TcpConnect {
    fn connect<'a>(&'a self, peer: SocketAddr) -> BoxFut<'a, io::Result<BoxIo>> {
        Box::pin(async move {
            let s = TcpStream::connect(peer).await?;
            let _ = s.set_nodelay(true);
            Ok(Box::new(s) as BoxIo)
        })
    }
}

/// `getaddrinfo`, through `tokio::net::lookup_host` (§23.8.5).
#[derive(Debug, Default)]
pub struct SystemResolver;

impl Resolve for SystemResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFut<'a, io::Result<Vec<IpAddr>>> {
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host, port)).await?;
            Ok(addrs.map(|a| a.ip()).collect())
        })
    }
}

/// Why a dial did not produce a connection. Each maps to one §23.7.1 "before dispatch" cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialFailure {
    /// Resolution failed, answered nothing, or did not answer within the connect bound.
    Dns,
    /// Every resolved address is outside the upstream's admitted classes or always refused. The
    /// first refusal is kept for diagnostics (never for the wire: the cause is `forbidden_address`).
    Address(AddressRefusal),
    ConnectRefused,
    ConnectUnreachable,
    ConnectTimeout,
}

/// A connected socket, pinned to the address that was checked.
pub struct Dialled {
    pub stream: BoxIo,
    pub peer: SocketAddr,
    /// DNS + TCP time (`HttpStats.connect_us`).
    pub elapsed: Duration,
}

/// Per-upstream positive DNS cache.
#[derive(Default)]
pub struct DnsCache {
    entries: Mutex<HashMap<String, (Vec<IpAddr>, Instant)>>,
}

impl DnsCache {
    fn get(&self, upstream: &str) -> Option<Vec<IpAddr>> {
        let map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        map.get(upstream)
            .filter(|(_, expires)| Instant::now() < *expires)
            .map(|(a, _)| a.clone())
    }

    fn put(&self, upstream: &str, addrs: Vec<IpAddr>, ttl: Duration) {
        let mut map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        map.insert(upstream.to_string(), (addrs, Instant::now() + ttl));
    }

    /// Forget an upstream's answer (after a failed dial).
    pub fn forget(&self, upstream: &str) {
        let mut map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(upstream);
    }
}

/// Map a connect error to its §23.5.6 dial cause.
fn connect_cause(e: &io::Error) -> DialFailure {
    match e.kind() {
        io::ErrorKind::ConnectionRefused => DialFailure::ConnectRefused,
        io::ErrorKind::TimedOut => DialFailure::ConnectTimeout,
        _ => DialFailure::ConnectUnreachable,
    }
}

/// The addresses to try for `up`, after the address guard: `(admitted, first refusal)`.
pub fn admitted(
    up: &Upstream,
    addrs: &[IpAddr],
    nat64: &Nat64Prefixes,
) -> (Vec<IpAddr>, Option<AddressRefusal>) {
    let mut ok = Vec::new();
    let mut first_refusal = None;
    for &ip in addrs {
        match up.address.check(ip, nat64) {
            Ok(()) => ok.push(ip),
            Err(r) => {
                // Skipped and counted (§23.8.5); the counter is slice F7's metric. The address is
                // the operator's DNS answer, never request data, so it may be logged.
                tracing::debug!(upstream = %up.name, %ip, range = r.label(), "http: address refused by the guard");
                first_refusal.get_or_insert(r);
            }
        }
    }
    (ok, first_refusal)
}

/// Resolve (or reuse the cached answer), apply the guard, and connect to the first admitted address
/// that answers, all within `bound`.
pub async fn dial(
    up: &Upstream,
    resolver: &dyn Resolve,
    connector: &dyn Connect,
    cache: &DnsCache,
    nat64: &Nat64Prefixes,
    bound: Duration,
) -> Result<Dialled, DialFailure> {
    let start = Instant::now();
    let until = tokio::time::Instant::now() + bound;
    let port = up.origin.port();
    let addrs: Vec<IpAddr> = match up.origin.host() {
        Host::V4(a) => vec![IpAddr::V4(*a)],
        Host::V6(a) => vec![IpAddr::V6(*a)],
        Host::Name(name) => match cache.get(&up.name) {
            Some(a) => a,
            None => match tokio::time::timeout_at(until, resolver.resolve(name, port)).await {
                Ok(Ok(a)) if !a.is_empty() => {
                    cache.put(
                        &up.name,
                        a.clone(),
                        Duration::from_millis(u64::from(up.limits.dns_ttl_ms)),
                    );
                    a
                }
                Ok(Ok(_)) | Ok(Err(_)) | Err(_) => return Err(DialFailure::Dns),
            },
        },
    };
    let (ok, refusal) = admitted(up, &addrs, nat64);
    if ok.is_empty() {
        // An answer the guard refuses outright is not a transient DNS failure; keep it cached for
        // its TTL like any positive answer (re-resolving a rebinding attack's answer helps nobody).
        return Err(DialFailure::Address(refusal.unwrap_or(
            AddressRefusal::Range(crate::address::RefusedRange::Unspecified),
        )));
    }
    let mut last = DialFailure::ConnectUnreachable;
    let n = ok.len();
    for (i, ip) in ok.into_iter().enumerate() {
        let peer = SocketAddr::new(ip, port);
        let now = tokio::time::Instant::now();
        if now >= until {
            last = DialFailure::ConnectTimeout;
            break;
        }
        // One share of what is left per remaining address (Happy Eyeballs is deferred, §23.8.5).
        let share = (until - now) / u32::try_from(n - i).unwrap_or(1);
        match tokio::time::timeout(share, connector.connect(peer)).await {
            Ok(Ok(stream)) => {
                return Ok(Dialled {
                    stream,
                    peer,
                    elapsed: start.elapsed(),
                });
            }
            Ok(Err(e)) => last = connect_cause(&e),
            Err(_) => last = DialFailure::ConnectTimeout,
        }
    }
    cache.forget(&up.name);
    Err(last)
}

/// A resolver with fixed answers, for tests and for the e2e suite in `ferrod`: a name maps to a
/// list of answers, consumed one per resolution (the last repeats), so "the answer changes between
/// dials" is expressible.
#[derive(Default)]
pub struct StaticResolver {
    answers: Mutex<HashMap<String, Vec<Vec<IpAddr>>>>,
    calls: std::sync::atomic::AtomicU64,
}

impl StaticResolver {
    pub fn new() -> Arc<Self> {
        Arc::new(StaticResolver::default())
    }

    /// Answer `name` with each of `answers` in turn.
    pub fn set(&self, name: &str, answers: Vec<Vec<IpAddr>>) {
        self.answers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name.to_string(), answers);
    }

    /// How many resolutions were asked for.
    pub fn calls(&self) -> u64 {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Resolve for StaticResolver {
    fn resolve<'a>(&'a self, host: &'a str, _port: u16) -> BoxFut<'a, io::Result<Vec<IpAddr>>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut map = self.answers.lock().unwrap_or_else(|p| p.into_inner());
        let r = match map.get_mut(host) {
            Some(list) if list.len() > 1 => Ok(list.remove(0)),
            Some(list) if list.len() == 1 => Ok(list[0].clone()),
            _ => Err(io::Error::new(io::ErrorKind::NotFound, "no such name")),
        };
        Box::pin(async move { r })
    }
}
