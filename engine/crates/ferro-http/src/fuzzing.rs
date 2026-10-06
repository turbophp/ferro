//! The bodies of the `cargo-fuzz` targets (`fuzz/fuzz_targets/*.rs`), kept in the library so the
//! same properties also run deterministically under `cargo test` (`tests/property_it.rs`) — the
//! fuzz smoke job needs nightly, and a property that only nightly CI can run is a property a
//! stable `cargo test` never checks.
//!
//! Each function must never panic on any input; a panic is a finding. Each asserts:
//!
//! - [`request`]: validation is deterministic; anything accepted satisfies an INDEPENDENT oracle
//!   of §23.4's invariants (written separately from `validate.rs`, so one bug cannot hide in
//!   both); and an accepted request **re-validates identically after a round trip** — the request
//!   the engine would send (same method and target, the kept headers, `Host` = the authority,
//!   `Content-Length` = the body, `origin` = the normalised origin) is itself accepted, with the
//!   same target, the same kept headers and the same effective idempotency.
//! - [`origin`]: an accepted `ORIGIN` re-parses from its normalised form to the same value, and
//!   that form is lowercase ASCII with no userinfo, path, `%` or numeric non-canonical host.
//! - [`address`]: classification of an IPv4 address equals that of every embedding of it.

use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::Path;
use std::sync::OnceLock;

use crate::address::{AddressPolicy, ClassSet, Nat64Prefixes, classify};
use crate::config::HttpConfig;
use crate::origin::{Host, Origin};
use crate::validate::{Request, Validated, validate};

/// The upstreams the request target validates against. Every policy knob §23.4 reads is set to
/// a non-default value on at least one of them.
pub const FIXTURE_UPSTREAMS: [&str; 7] = [
    "root", "api", "params", "utf8", "attach", "override", "uidonly",
];

/// The peer uid the request target validates as.
pub const FIXTURE_UID: u32 = 1000;

/// The environment of the fixture configuration (also used by the corpus tests).
pub fn fixture_env() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "FERRO_UPSTREAMS",
            "root,api,params,utf8,attach,override,uidonly",
        ),
        ("FERRO_UPSTREAM_ROOT_ORIGIN", "https://api.example.com"),
        ("FERRO_UPSTREAM_API_ORIGIN", "http://127.0.0.1:9200"),
        ("FERRO_UPSTREAM_API_ALLOW_PATHS", "/api/,/v1"),
        ("FERRO_UPSTREAM_API_ADDRESS_CLASSES", "loopback"),
        ("FERRO_UPSTREAM_API_ALLOW_METHODS", "GET,POST,patch"),
        ("FERRO_UPSTREAM_API_IDEMPOTENT_METHODS", "GET"),
        (
            "FERRO_UPSTREAM_API_IDEMPOTENCY_KEY_HEADER",
            "Idempotency-Key",
        ),
        ("FERRO_UPSTREAM_API_PASS_HEADERS", "X-Forwarded-For"),
        ("FERRO_UPSTREAM_PARAMS_ORIGIN", "https://[2001:db8::1]:8443"),
        ("FERRO_UPSTREAM_PARAMS_PATH_PARAMS", "allow"),
        ("FERRO_UPSTREAM_PARAMS_ALLOW_PATHS", "/a;b/,/x"),
        ("FERRO_UPSTREAM_UTF8_ORIGIN", "https://wiki.example.org"),
        ("FERRO_UPSTREAM_UTF8_PATH_ENCODING", "utf8"),
        ("FERRO_UPSTREAM_UTF8_ALLOW_PATHS", "/w/"),
        ("FERRO_UPSTREAM_ATTACH_ORIGIN", "https://pay.example.com"),
        (
            "FERRO_UPSTREAM_ATTACH_ATTACH_HEADERS_FILE",
            "/fixture/attach",
        ),
        (
            "FERRO_UPSTREAM_OVERRIDE_ORIGIN",
            "https://pay.example.com:8443",
        ),
        (
            "FERRO_UPSTREAM_OVERRIDE_ATTACH_HEADERS_FILE",
            "/fixture/attach",
        ),
        ("FERRO_UPSTREAM_OVERRIDE_ATTACH_POLICY", "override"),
        ("FERRO_UPSTREAM_OVERRIDE_MAX_BODY_BYTES", "100"),
        (
            "FERRO_UPSTREAM_UIDONLY_ORIGIN",
            "https://internal.example.com",
        ),
        ("FERRO_UPSTREAM_UIDONLY_ALLOW_UIDS", "2000,2001"),
    ]
}

/// The attached-header file the fixture upstreams name.
pub const FIXTURE_ATTACH: &[u8] = b"Authorization: Bearer FIXTURE-CANARY-SECRET\nX-Api-Key: k\n";

pub fn fixture_read(p: &Path) -> std::io::Result<Vec<u8>> {
    if p == Path::new("/fixture/attach") {
        Ok(FIXTURE_ATTACH.to_vec())
    } else {
        Err(std::io::ErrorKind::NotFound.into())
    }
}

pub fn fixture_config() -> &'static HttpConfig {
    static CFG: OnceLock<HttpConfig> = OnceLock::new();
    CFG.get_or_init(|| {
        let cfg = HttpConfig::load(
            fixture_env()
                .into_iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &fixture_read,
        );
        assert!(
            cfg.errors().is_empty(),
            "fixture config must load cleanly: {:?}",
            cfg.errors()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );
        cfg
    })
}

/// A cursor over fuzz bytes. Exhausted input reads as zeros.
struct Bytes<'a>(&'a [u8]);

impl<'a> Bytes<'a> {
    fn u8(&mut self) -> u8 {
        match self.0.split_first() {
            Some((b, rest)) => {
                self.0 = rest;
                *b
            }
            None => 0,
        }
    }

    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }

    fn take(&mut self, n: usize) -> &'a [u8] {
        let n = n.min(self.0.len());
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        a
    }

    fn text(&mut self, n: usize) -> String {
        String::from_utf8_lossy(self.take(n)).into_owned()
    }
}

/// An owned request decoded from fuzz bytes.
pub struct OwnedRequest {
    pub upstream: String,
    pub method: String,
    pub target: String,
    pub origin: Option<String>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Option<Vec<u8>>,
    pub idempotent: Option<bool>,
    pub decode: bool,
}

impl OwnedRequest {
    pub fn as_request(&self) -> Request<'_> {
        Request {
            upstream: &self.upstream,
            method: &self.method,
            target: &self.target,
            origin: self.origin.as_deref(),
            headers: &self.headers,
            body: self.body.as_deref(),
            idempotent: self.idempotent,
            decode: self.decode,
        }
    }

    pub fn decode(data: &[u8]) -> OwnedRequest {
        let mut b = Bytes(data);
        let sel = b.u8();
        let upstream = if sel & 0x80 != 0 {
            let n = usize::from(b.u8() % 16);
            b.text(n)
        } else {
            FIXTURE_UPSTREAMS[usize::from(sel) % FIXTURE_UPSTREAMS.len()].to_string()
        };
        let n = usize::from(b.u8());
        let method = b.text(n % 40);
        let n = usize::from(b.u16());
        let target = b.text(n % 9000);
        let flags = b.u8();
        let origin = (flags & 1 != 0).then(|| {
            let n = usize::from(b.u8());
            b.text(n % 64)
        });
        let count = usize::from(b.u8() % 8);
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let n = usize::from(b.u8());
            let name = b.text(n % 40);
            let n = usize::from(b.u8());
            let value = b.take(n % 80).to_vec();
            headers.push((name, value));
        }
        let body = (flags & 2 != 0).then(|| vec![0u8; usize::from(b.u8())]);
        let idempotent = match (flags >> 2) & 3 {
            1 => Some(true),
            2 => Some(false),
            _ => None,
        };
        let decode = flags & 16 != 0;
        OwnedRequest {
            upstream,
            method,
            target,
            origin,
            headers,
            body,
            idempotent,
            decode,
        }
    }
}

/// The `validate_request` fuzz target.
pub fn request(data: &[u8]) {
    let owned = OwnedRequest::decode(data);
    check_request(&owned.as_request());
}

/// The request properties, for any request (the property tests call this with generated ones).
pub fn check_request(req: &Request<'_>) {
    let cfg = fixture_config();
    let first = validate(cfg, Some(FIXTURE_UID), req);
    let again = validate(cfg, Some(FIXTURE_UID), req);
    assert_eq!(first, again, "validation is not deterministic");
    let Ok(v) = first else { return };
    oracle(&v, req);
    round_trip(&v, req);
}

const ENGINE_OWNED: &[&str] = &[
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-connection",
    "http2-settings",
    "expect",
    "upgrade",
    "proxy-authorization",
];

/// §23.4's invariants, re-derived independently of `validate.rs`.
fn oracle(v: &Validated<'_>, req: &Request<'_>) {
    let up = v.upstream;
    assert_eq!(
        v.host,
        up.origin.authority(),
        "Host is exactly the upstream authority"
    );
    if let Some(o) = req.origin {
        assert_eq!(o, up.origin.normalised());
    }
    // Method.
    let m = req.method;
    assert!(!m.is_empty() && m.len() <= 32);
    let upper = m.to_ascii_uppercase();
    assert!(upper != "CONNECT" && upper != "TRACE" && upper != "TRACK");
    if let Some(allowed) = &up.allow_methods {
        assert!(allowed.iter().any(|a| a == m));
    }
    // Target.
    let t = req.target.as_bytes();
    assert!(!t.is_empty() && t.len() <= 8192);
    assert_eq!(t[0], b'/');
    assert_ne!(t.get(1), Some(&b'/'));
    const ALLOWED: &[u8] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@/?%";
    assert!(
        t.iter().all(|b| ALLOWED.contains(b)),
        "byte outside step 3's set"
    );
    let path_end = t.iter().position(|&b| b == b'?').unwrap_or(t.len());
    let path = &t[..path_end];
    assert!(!t.windows(3).any(|w| w == b"%00"), "%00 accepted");
    assert!(
        !path.windows(3).any(|w| w.eq_ignore_ascii_case(b"%25")),
        "%25 accepted in the path"
    );
    // Decode once (every '%' must have two hex digits after it).
    let mut decoded = Vec::new();
    let mut i = 0;
    while i < path.len() {
        if path[i] == b'%' {
            let h = std::str::from_utf8(&path[i + 1..i + 3]).expect("hex digits");
            let byte = u8::from_str_radix(h, 16).expect("hex digits");
            assert!(
                byte >= 0x20 && byte != 0x7f,
                "encoded control byte accepted"
            );
            if byte >= 0x80 {
                assert_eq!(
                    up.path_encoding,
                    crate::config::PathEncoding::Utf8,
                    "non-ASCII escape accepted under PATH_ENCODING=ascii"
                );
            }
            decoded.push(byte);
            i += 3;
        } else {
            if path[i] == b';' {
                assert_eq!(up.path_params, crate::config::PathParams::Allow);
            }
            decoded.push(path[i]);
            i += 1;
        }
    }
    for seg in decoded.split(|&b| b == b'/' || b == b'\\') {
        let seg = seg
            .split(|&b| b == b';' || b == b'?' || b == b'#')
            .next()
            .unwrap_or_default();
        let dot_only =
            !seg.is_empty() && seg.contains(&b'.') && seg.iter().all(|&b| b == b'.' || b == b' ');
        assert!(!dot_only, "dot segment accepted");
    }
    let prefixed = up.allow_paths.iter().any(|p| {
        let p = p.as_bytes();
        path == p
            || (path.len() > p.len()
                && &path[..p.len()] == p
                && (p.last() == Some(&b'/') || path[p.len()] == b'/'))
    });
    assert!(prefixed, "path outside ALLOW_PATHS accepted");
    // Headers.
    for &i in &v.send_headers {
        let (n, val) = &req.headers[i];
        // Folded independently of `syntax::fold_name`: lowercase, every non-alphanumeric byte '-'.
        let lower: String = n
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        assert!(!n.starts_with(':'));
        for refused in [
            "x-forwarded-for",
            "x-forwarded-host",
            "x-real-ip",
            "x-http-method-override",
            "destination",
            "true-client-ip",
        ] {
            if lower == refused {
                assert!(
                    up.pass_headers.iter().any(|p| p == refused),
                    "{n} kept without PASS_HEADERS"
                );
            }
        }
        assert!(!ENGINE_OWNED.contains(&lower.as_str()), "{lower} kept");
        if lower == "te" {
            assert!(val.eq_ignore_ascii_case(b"trailers"));
        }
        assert!(
            !val.iter().any(|&b| b == b'\r' || b == b'\n' || b == 0),
            "CR/LF/NUL kept"
        );
        assert!(!up.attached.names(&lower), "an attached name was kept");
    }
    let mut kept = v.send_headers.clone();
    kept.dedup();
    assert_eq!(kept, v.send_headers, "a header index kept twice");
    assert!(kept.windows(2).all(|w| w[0] < w[1]), "header order changed");
}

fn round_trip(v: &Validated<'_>, req: &Request<'_>) {
    let mut headers: Vec<(String, Vec<u8>)> = v
        .send_headers
        .iter()
        .map(|&i| req.headers[i].clone())
        .collect();
    let kept = headers.len();
    let size: usize = headers.iter().map(|(n, v)| n.len() + v.len()).sum();
    // The engine-set Host and Content-Length are re-sent as PHP headers whenever they fit under
    // §23.4.3's limits (which count PHP's headers only): each must then pass its own check.
    if kept + 2 <= crate::validate::MAX_HEADER_LINES
        && size + 64 + v.host.len() <= crate::validate::MAX_HEADER_BYTES
    {
        headers.push(("Host".to_string(), v.host.as_bytes().to_vec()));
        headers.push((
            "Content-Length".to_string(),
            v.content_length.to_string().into_bytes(),
        ));
    }
    let replay = Request {
        upstream: req.upstream,
        method: req.method,
        target: req.target,
        origin: Some(v.upstream.origin.normalised()),
        headers: &headers,
        body: req.body,
        idempotent: req.idempotent,
        decode: req.decode,
    };
    let again = validate(super::fuzzing::fixture_config(), Some(FIXTURE_UID), &replay)
        .unwrap_or_else(|r| panic!("the request the engine would send is refused: {r:?}"));
    assert!(std::ptr::eq(again.upstream, v.upstream));
    assert_eq!(again.host, v.host);
    assert_eq!(again.send_headers, (0..kept).collect::<Vec<_>>());
    assert_eq!(again.overridden, 0);
    assert_eq!(again.content_length, v.content_length);
    assert_eq!(again.idempotent, v.idempotent);
    assert_eq!(again.add_accept_encoding, v.add_accept_encoding);
}

/// The `parse_origin` fuzz target.
pub fn origin(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(o) = Origin::parse(s) else { return };
    let n = o.normalised();
    assert_eq!(
        Origin::parse(n).as_ref(),
        Ok(&o),
        "normalised form re-parses"
    );
    assert!(
        n.bytes()
            .all(|b| b.is_ascii_graphic() && !b.is_ascii_uppercase())
    );
    assert!(!n.contains('@') && !n.contains('%') && !n.contains('\\'));
    assert_eq!(n.matches('/').count(), 2, "no path");
    match o.host() {
        Host::Name(name) => {
            assert!(
                name.parse::<IpAddr>().is_err(),
                "a name that is an IP literal"
            );
            let last = name.rsplit('.').next().unwrap_or_default();
            assert!(
                !last.bytes().all(|b| b.is_ascii_digit()),
                "numeric last label"
            );
        }
        Host::V4(a) => assert_eq!(
            o.authority().split(':').next(),
            Some(a.to_string().as_str())
        ),
        Host::V6(a) => assert!(o.authority().starts_with(&format!("[{a}]"))),
    }
}

/// The `classify_address` fuzz target.
pub fn address(data: &[u8]) {
    let none = Nat64Prefixes::default();
    if let Ok(o) = <[u8; 16]>::try_from(data.get(..16).unwrap_or_default()) {
        let a = IpAddr::V6(Ipv6Addr::from(o));
        let c = classify(a, &none);
        assert!((1..=4).contains(&c.len()));
        let p = AddressPolicy {
            classes: ClassSet {
                public: data.get(16).is_some_and(|b| b & 1 != 0),
                private: data.get(16).is_some_and(|b| b & 2 != 0),
                loopback: data.get(16).is_some_and(|b| b & 4 != 0),
            },
            allow_metadata: data.get(16).is_some_and(|b| b & 8 != 0),
            literal: None,
        };
        let _ = p.check(a, &none);
    }
    if let Ok(o) = <[u8; 4]>::try_from(data.get(..4).unwrap_or_default()) {
        check_embeddings(Ipv4Addr::from(o));
    }
}

/// Every textual and structural embedding of `v4` classifies exactly as `v4` does.
pub fn check_embeddings(v4: Ipv4Addr) {
    let none = Nat64Prefixes::default();
    let want = classify(IpAddr::V4(v4), &none);
    assert_eq!(want.len(), 1);
    let o = v4.octets();
    let mapped = v4.to_ipv6_mapped();
    let wkp = Ipv6Addr::from({
        let mut b = [0u8; 16];
        b[..4].copy_from_slice(&[0, 0x64, 0xff, 0x9b]);
        b[12..].copy_from_slice(&o);
        b
    });
    let local96 = Ipv6Addr::from({
        let mut b = [0u8; 16];
        b[..6].copy_from_slice(&[0, 0x64, 0xff, 0x9b, 0, 1]);
        b[12..].copy_from_slice(&o);
        b
    });
    let custom_prefix = [0x20, 0x01, 0x0d, 0xb8, 0, 0x64, 0, 0, 0, 0, 0, 0];
    let custom = Nat64Prefixes::new(vec![custom_prefix]);
    let custom_addr = Ipv6Addr::from({
        let mut b = [0u8; 16];
        b[..12].copy_from_slice(&custom_prefix);
        b[12..].copy_from_slice(&o);
        b
    });
    for e in [mapped, wkp] {
        assert_eq!(classify(IpAddr::V6(e), &none), want, "{e} vs {v4}");
    }
    assert_eq!(
        classify(IpAddr::V6(custom_addr), &custom),
        want,
        "{custom_addr} vs {v4}"
    );
    // The /48 local-use prefix: the /96 candidate is v4 itself; the address is admitted only if
    // every candidate is, so a refused v4 is refused through it.
    let c = classify(IpAddr::V6(local96), &none);
    assert!(c.contains(&want[0]), "{local96} vs {v4}");
    // Admission agrees too, for every policy.
    for bits in 0u8..16 {
        let p = AddressPolicy {
            classes: ClassSet {
                public: bits & 1 != 0,
                private: bits & 2 != 0,
                loopback: bits & 4 != 0,
            },
            allow_metadata: bits & 8 != 0,
            literal: None,
        };
        let direct = p.check(IpAddr::V4(v4), &none);
        assert_eq!(p.check(IpAddr::V6(mapped), &none), direct);
        assert_eq!(p.check(IpAddr::V6(wkp), &none), direct);
        assert_eq!(p.check(IpAddr::V6(custom_addr), &custom), direct);
        if direct.is_err() {
            assert!(p.check(IpAddr::V6(local96), &none).is_err(), "{local96}");
        }
    }
}
