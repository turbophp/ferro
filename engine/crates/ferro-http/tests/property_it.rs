//! The §23.4 property gate (slice M6-F3's offline half), deterministic under `cargo test`.
//!
//! `proptest` is not a workspace dependency, so the generators are hand-rolled over a fixed-seed
//! xorshift: every run checks the same inputs, and a failure names the seed and iteration.
//!
//! - **The request gate.** For every ACCEPTED generated request, an HTTP/1.1 head is rendered the
//!   way the engine will send it and parsed back the way an upstream reads it: the request-target
//!   it carries is exactly the accepted target, `Host` is exactly the upstream authority and occurs
//!   once, and the head has exactly the lines the plan says (no injection). The *wire* half — the
//!   bytes a real test upstream received through `hyper` — is slice F4's (SPEC §22.2 (cw)).
//! - Every accepted request also passes `ferro_http::fuzzing`'s independent oracle and
//!   re-validates identically after a round trip (the same body the fuzz target runs).
//! - **Dot segments in every encoding.** Appending any encoded dot segment to an accepted path is
//!   refused.
//! - **Addresses in every textual form.** Every address sampled from a refused CIDR is refused as
//!   an IPv4 address, through every embedding, and as an IP-literal `ORIGIN` in every textual
//!   form; every non-canonical IPv4 spelling is refused at parse.
//! - **Header injection.** Any control byte anywhere in a value is refused; the same value
//!   without it is kept.
//!
//! Each property counts its accepted and refused cases and asserts both are substantial, so a
//! generator that only ever produced one class cannot pass vacuously.

use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ferro_http::address::{AddressPolicy, ClassSet, Classification, Nat64Prefixes, classify};
use ferro_http::config::{HttpConfig, UpstreamEntry};
use ferro_http::fuzzing::{
    FIXTURE_UID, FIXTURE_UPSTREAMS, OwnedRequest, check_embeddings, check_request, fixture_config,
};
use ferro_http::origin::{Origin, OriginError};
use ferro_http::{Request, Rule, Validated, validate};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

const FRAGMENTS: &[&str] = &[
    "api",
    "v1",
    "w",
    "x",
    "y",
    "a;b",
    "admin",
    ".",
    "..",
    "...",
    "%2e",
    "%2E",
    "%2e%2e",
    ".%2e",
    "%2f",
    "%2F",
    "%5c",
    ";",
    "%3b",
    "%3B",
    "%25",
    "%2525",
    "%00",
    "%0a",
    "%0d",
    "%20",
    "%7f",
    "%c0%ae",
    "%e0%80%ae",
    "%ef%bc%8e",
    "%e2%80%a5",
    "%c3%a9",
    "%ef%bc%8f",
    "%ef%bc%85",
    "%41",
    "%7e",
    "~",
    "-",
    "_",
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
    "?",
    "/",
    "//",
    "\\",
    "#",
    " ",
    "é",
    "[",
    "]",
    "%",
    "%z",
    "q=1",
    "..;",
    "jsessionid=1",
];

fn gen_target(r: &mut Rng) -> String {
    let mut t = String::from("/");
    let n = 1 + r.below(6);
    for i in 0..n {
        if i > 0 && r.below(3) != 0 {
            t.push('/');
        }
        let f: &&str = r.pick(FRAGMENTS);
        t.push_str(f);
    }
    // Sometimes lead with an allowed prefix, so prefix confinement passes often enough.
    match r.below(4) {
        0 => format!("/api{t}"),
        1 => format!("/w{t}"),
        2 => format!("/x{t}"),
        _ => t,
    }
}

const METHODS: &[&str] = &[
    "GET", "POST", "patch", "get", "TRACE", "connect", "PUT", "M-SEARCH", "G T",
];

const HEADER_NAMES: &[&str] = &[
    "Accept",
    "X-A",
    "Host",
    "Content-Length",
    "Transfer-Encoding",
    "TE",
    "Connection",
    "Expect",
    "Authorization",
    "X-Api-Key",
    "X-Forwarded-For",
    "X-Forwarded-Host",
    "Idempotency-Key",
    "Accept-Encoding",
    "Upgrade",
    ":path",
    "X B",
    "Cookie",
];

fn gen_value(r: &mut Rng, name: &str, req_body: usize, authority: &str) -> Vec<u8> {
    match (name, r.below(4)) {
        ("Host", 0) => authority.as_bytes().to_vec(),
        ("Content-Length", 0) => req_body.to_string().into_bytes(),
        ("Transfer-Encoding", 0) => b"chunked".to_vec(),
        ("TE", 0) => b"trailers".to_vec(),
        _ => {
            let pool: &[&[u8]] = &[
                b"v",
                b"a b",
                b"\xff",
                b"",
                b" lead",
                b"trail ",
                b"x\r\ny: z",
                b"k\0",
                b"gzip",
                b"Bearer t",
                b"1.2.3.4",
                b"12",
            ];
            r.pick(pool).to_vec()
        }
    }
}

const SAFE_FRAGMENTS: &[&str] = &[
    "x", "y", "items", "a.b", "..x", ".hidden", "%41", "%20", "%2f", "~", "-", "q", "1", "@", ":",
];

/// A request built only from benign parts, aimed at the upstream's own prefix — so the accepted
/// class is large enough for the gate to mean something.
fn gen_benign(r: &mut Rng, upstream: &str) -> (String, String) {
    let mut t = match upstream {
        "api" => r.pick(&["/api/", "/v1/"]).to_string(),
        "utf8" => "/w/".to_string(),
        "params" => "/x/".to_string(),
        _ => "/".to_string(),
    };
    for i in 0..1 + r.below(4) {
        if i > 0 {
            t.push('/');
        }
        let f: &&str = r.pick(SAFE_FRAGMENTS);
        t.push_str(f);
    }
    if r.below(3) == 0 {
        t.push_str("?a=1&b=..%2f");
    }
    (t, r.pick(&["GET", "POST"]).to_string())
}

fn gen_request(r: &mut Rng) -> OwnedRequest {
    let upstream = r.pick(&FIXTURE_UPSTREAMS).to_string();
    let benign = r.below(2) == 0;
    let authority = match fixture_config().upstream_for(&upstream, None) {
        Some(u) => u.origin.authority().to_string(),
        None => "internal.example.com".to_string(),
    };
    let body = (r.below(2) == 0).then(|| vec![b'b'; r.below(200)]);
    let body_len = body.as_ref().map_or(0, Vec::len);
    let n = r.below(4);
    let headers = (0..n)
        .map(|_| {
            let name = r.pick(HEADER_NAMES).to_string();
            let v = gen_value(r, &name, body_len, &authority);
            (name, v)
        })
        .collect();
    let origin = match r.below(4) {
        0 => Some(format!("https://{authority}")),
        1 => Some(format!("http://{authority}")),
        _ => None,
    };
    let (target, method) = if benign {
        gen_benign(r, &upstream)
    } else {
        (gen_target(r), r.pick(METHODS).to_string())
    };
    let headers = if benign {
        vec![("Accept".to_string(), b"*/*".to_vec())]
    } else {
        headers
    };
    let origin = if benign { None } else { origin };
    OwnedRequest {
        upstream,
        method,
        target,
        origin,
        headers,
        body,
        idempotent: [None, Some(true), Some(false)][r.below(3)],
        decode: r.below(2) == 0,
    }
}

/// Render the HTTP/1.1 head the engine sends for an accepted request (attached values replaced
/// by a placeholder — this checks framing, not custody), then parse it as an upstream would.
fn render_and_reparse(v: &Validated<'_>, req: &Request<'_>) {
    let mut head = Vec::new();
    head.extend_from_slice(req.method.as_bytes());
    head.push(b' ');
    head.extend_from_slice(req.target.as_bytes());
    head.extend_from_slice(b" HTTP/1.1\r\nHost: ");
    head.extend_from_slice(v.host.as_bytes());
    head.extend_from_slice(b"\r\n");
    let mut expected_lines = 1;
    for &i in &v.send_headers {
        let (n, val) = &req.headers[i];
        head.extend_from_slice(n.as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(val);
        head.extend_from_slice(b"\r\n");
        expected_lines += 1;
    }
    for a in v.upstream.attached.iter() {
        head.extend_from_slice(a.name().as_bytes());
        head.extend_from_slice(b": <attached>\r\n");
        expected_lines += 1;
    }
    head.extend_from_slice(format!("Content-Length: {}\r\n", v.content_length).as_bytes());
    expected_lines += 1;
    if v.add_accept_encoding {
        head.extend_from_slice(b"Accept-Encoding: gzip, deflate\r\n");
        expected_lines += 1;
    }
    head.extend_from_slice(b"\r\n");

    // Parse it back.
    let end = head
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a head");
    assert_eq!(
        end + 4,
        head.len(),
        "the head ends exactly once, at its end"
    );
    let lines: Vec<&[u8]> = head[..end].split(|&b| b == b'\n').collect();
    for l in &lines[..lines.len() - 1] {
        assert_eq!(l.last(), Some(&b'\r'), "a bare LF inside the head");
    }
    let lines: Vec<&[u8]> = lines
        .iter()
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l))
        .collect();
    let rl: Vec<&[u8]> = lines[0].split(|&b| b == b' ').collect();
    assert_eq!(rl.len(), 3, "request line splits into exactly three");
    assert_eq!(rl[0], req.method.as_bytes());
    assert_eq!(
        rl[1],
        req.target.as_bytes(),
        "the target received is exactly the accepted one"
    );
    assert_eq!(rl[2], b"HTTP/1.1");
    assert_eq!(
        lines.len() - 1,
        expected_lines,
        "header line count (no injection)"
    );
    let hosts: Vec<&[u8]> = lines[1..]
        .iter()
        .filter(|l| l.len() >= 5 && l[..5].eq_ignore_ascii_case(b"host:"))
        .map(|l| l[5..].strip_prefix(b" ").unwrap_or(&l[5..]))
        .collect();
    assert_eq!(
        hosts,
        vec![v.upstream.origin.authority().as_bytes()],
        "exactly one Host, the authority"
    );
    for l in &lines[1..] {
        let colon = l.iter().position(|&b| b == b':').expect("name: value");
        assert!(
            colon > 0 && !l[..colon].contains(&b' '),
            "a malformed header line"
        );
    }
}

#[test]
fn the_request_gate_over_generated_requests() {
    let cfg = fixture_config();
    let mut r = Rng(0x5eed_f3f3_0001);
    let (mut accepted, mut refused) = (0usize, 0usize);
    for i in 0..60_000 {
        let owned = gen_request(&mut r);
        let req = owned.as_request();
        // The oracle and the round trip (the fuzz target's body).
        check_request(&req);
        match validate(cfg, Some(FIXTURE_UID), &req) {
            Ok(v) => {
                accepted += 1;
                render_and_reparse(&v, &req);
            }
            Err(e) => {
                refused += 1;
                let shown = e.to_string();
                for (_, val) in req.headers.iter() {
                    if val.len() >= 6 {
                        assert!(
                            !shown
                                .as_bytes()
                                .windows(val.len())
                                .any(|w| w == val.as_slice()),
                            "iteration {i}: refusal quotes a header value: {shown}"
                        );
                    }
                }
            }
        }
    }
    assert!(
        accepted > 3_000,
        "too few accepted cases to mean anything: {accepted}"
    );
    assert!(refused > 3_000, "too few refused cases: {refused}");
}

#[test]
fn fuzz_bodies_never_panic_on_pseudorandom_bytes() {
    let mut r = Rng(0x5eed_f3f3_0002);
    let mut buf = Vec::new();
    for _ in 0..100_000 {
        buf.clear();
        let n = r.below(160);
        for _ in 0..n {
            // Bias towards the bytes the validator branches on.
            let b = match r.below(4) {
                0 => *r.pick(&b"/.%;?\\:@ \r\n\x00ef2Ea"[..]),
                _ => r.next() as u8,
            };
            buf.push(b);
        }
        ferro_http::fuzzing::request(&buf);
        ferro_http::fuzzing::origin(&buf);
        ferro_http::fuzzing::address(&buf);
    }
}

/// Every encoding of a dot segment, appended to an accepted path, is refused.
#[test]
fn dot_segments_in_every_encoding_are_refused() {
    const DOTS: &[&str] = &[
        "..",
        ".",
        "...",
        "%2e%2e",
        "%2E%2E",
        "%2e.",
        ".%2e",
        "%2e",
        "..%20",
        ".%20.",
        "%2e%2e%3b",
        "..%3bx",
        "%2e%2e%3Bjsessionid=1",
    ];
    const SEPS: &[&str] = &["/", "%2f", "%2F", "%5c", "%5C"];
    let cfg = fixture_config();
    let mut r = Rng(0x5eed_f3f3_0003);
    let mut bases = Vec::new();
    while bases.len() < 300 {
        let t = gen_target(&mut r);
        let path = t.split('?').next().unwrap_or("/").to_string();
        let ok = validate(
            cfg,
            Some(FIXTURE_UID),
            &Request {
                upstream: "root",
                method: "GET",
                target: &path,
                origin: None,
                headers: &[],
                body: None,
                idempotent: None,
                decode: false,
            },
        );
        if ok.is_ok() && !path.ends_with('/') {
            bases.push(path);
        }
    }
    let mut checked = 0;
    for (up, base_ok) in [("root", true), ("params", false), ("utf8", false)] {
        for base in bases.iter().take(if base_ok { 300 } else { 1 }) {
            let base = if base_ok {
                base.clone()
            } else if up == "params" {
                "/x".to_string()
            } else {
                "/w/x".to_string()
            };
            for d in DOTS {
                for s1 in SEPS {
                    for s2 in SEPS {
                        let t = format!("{base}{s1}{d}{s2}admin");
                        let res = validate(
                            cfg,
                            Some(FIXTURE_UID),
                            &Request {
                                upstream: up,
                                method: "GET",
                                target: &t,
                                origin: None,
                                headers: &[],
                                body: None,
                                idempotent: None,
                                decode: false,
                            },
                        );
                        let rule = res.err().map(|e| e.rule);
                        assert!(
                            matches!(rule, Some(Rule::PathDotSegment)),
                            "{up} {t}: {rule:?}"
                        );
                        checked += 1;
                    }
                }
            }
        }
    }
    assert!(checked > 7_000, "{checked}");
}

/// Header injection: one control byte anywhere refuses; the same value without it is kept.
#[test]
fn any_control_byte_in_a_value_is_refused_and_its_control_is_kept() {
    let cfg = fixture_config();
    let mut r = Rng(0x5eed_f3f3_0004);
    let controls: Vec<u8> = (0u8..0x20).filter(|&b| b != b'\t').chain([0x7f]).collect();
    for _ in 0..20_000 {
        let len = 1 + r.below(30);
        let clean: Vec<u8> = (0..len)
            .map(|_| match r.below(3) {
                0 => 0x21 + (r.next() % 94) as u8,
                1 => 0x80 + (r.next() % 128) as u8,
                _ => b'a',
            })
            .collect();
        let go = |v: Vec<u8>| {
            let headers = vec![("X-Probe".to_string(), v)];
            validate(
                cfg,
                Some(FIXTURE_UID),
                &Request {
                    upstream: "root",
                    method: "GET",
                    target: "/",
                    origin: None,
                    headers: &headers,
                    body: None,
                    idempotent: None,
                    decode: false,
                },
            )
            .map(|v| v.send_headers)
        };
        assert_eq!(go(clean.clone()), Ok(vec![0]), "control value {clean:?}");
        let mut dirty = clean.clone();
        dirty.insert(r.below(clean.len() + 1), *r.pick(&controls));
        assert_eq!(
            go(dirty.clone()).map_err(|e| e.rule),
            Err(Rule::HeaderValue),
            "{dirty:?}"
        );
    }
}

/// The textual forms `inet_aton` reads as `v4`, other than the canonical dotted quad.
fn noncanonical_forms(v4: Ipv4Addr) -> Vec<String> {
    let [a, b, c, d] = v4.octets();
    let n = u32::from(v4);
    vec![
        n.to_string(),
        format!("0x{n:x}"),
        format!("0{n:o}"),
        format!("{a}.{}", n & 0x00ff_ffff),
        format!("{a}.{b}.{}", n & 0xffff),
        format!("0x{a:x}.{b}.{c}.{d}"),
        format!("0{a:o}.{b}.{c}.{d}"),
        format!("{a}.{b}.{c}.0x{d:x}"),
        format!("0{a:o}.0{b:o}.0{c:o}.0{d:o}"),
        format!("{a:03}.{b:03}.{c:03}.{d:03}"),
    ]
    .into_iter()
    .filter(|s| *s != v4.to_string())
    .collect()
}

fn origin_config(origin: &str, meta: bool) -> HttpConfig {
    let mut env = vec![
        ("FERRO_UPSTREAMS", "lit".to_string()),
        ("FERRO_UPSTREAM_LIT_ORIGIN", origin.to_string()),
    ];
    if meta {
        env.push(("FERRO_UPSTREAM_LIT_ALLOW_METADATA", "1".to_string()));
    }
    HttpConfig::load(
        env.into_iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
        &|_| Err(std::io::ErrorKind::NotFound.into()),
    )
}

fn enabled(cfg: &HttpConfig) -> bool {
    matches!(cfg.entries().next(), Some((_, UpstreamEntry::Enabled(_))))
}

/// Any address in a refused CIDR is refused in every form generated.
#[test]
fn addresses_in_refused_ranges_are_refused_in_every_form() {
    let cidrs: &[([u8; 4], u32, bool)] = &[
        // (base, prefix length, is an M row)
        ([0, 0, 0, 0], 8, false),
        ([224, 0, 0, 0], 4, false),
        ([240, 0, 0, 0], 4, false),
        ([169, 254, 0, 0], 16, true),
        ([100, 100, 100, 200], 32, true),
        ([168, 63, 129, 16], 32, true),
        ([192, 0, 0, 0], 24, true),
    ];
    let none = Nat64Prefixes::default();
    let mut r = Rng(0x5eed_f3f3_0005);
    let mut forms = 0;
    for &(base, len, meta) in cidrs {
        for _ in 0..12 {
            let host_bits = if len == 32 {
                0
            } else {
                (r.next() as u32) >> len
            };
            let v4 = Ipv4Addr::from(u32::from(Ipv4Addr::from(base)) | host_bits);
            let c = classify(IpAddr::V4(v4), &none);
            assert!(
                matches!(c.as_slice(), [Classification::Refused(_)]),
                "{v4}: {c:?}"
            );
            check_embeddings(v4);
            // Every IPv6 literal spelling of the mapped and NAT64 embeddings, as an ORIGIN.
            let [a, b, cc, d] = v4.octets();
            let (hi, lo) = (u16::from_be_bytes([a, b]), u16::from_be_bytes([cc, d]));
            let v6_forms = [
                format!("[::ffff:{v4}]"),
                format!("[::ffff:{hi:x}:{lo:x}]"),
                format!("[0:0:0:0:0:ffff:{hi:x}:{lo:x}]"),
                format!("[0000:0000:0000:0000:0000:ffff:{hi:04x}:{lo:04x}]"),
                format!("[64:ff9b::{v4}]"),
                format!("[64:ff9b::{hi:x}:{lo:x}]"),
                format!("[64:ff9b:0:0:0:0:{hi:x}:{lo:x}]"),
                format!("[64:ff9b:1::{hi:x}:{lo:x}]"),
            ];
            for host in std::iter::once(v4.to_string()).chain(v6_forms) {
                let origin = format!("http://{host}");
                let o = Origin::parse(&origin).unwrap_or_else(|e| panic!("{origin}: {e}"));
                assert!(o.ip_literal().is_some());
                assert!(!enabled(&origin_config(&origin, false)), "{origin} enabled");
                if host.starts_with("[64:ff9b:1:") {
                    // Local-use NAT64: every RFC 6052 layout must admit, and the /48 layout reads
                    // 0.0.0.0 here, so this stays refused even under ALLOW_METADATA=1.
                    assert!(!enabled(&origin_config(&origin, true)), "{origin}");
                } else if meta {
                    // The control: ALLOW_METADATA=1 admits exactly the M rows.
                    assert!(
                        enabled(&origin_config(&origin, true)),
                        "{origin} refused under ALLOW_METADATA=1"
                    );
                } else {
                    assert!(
                        !enabled(&origin_config(&origin, true)),
                        "{origin} admitted by ALLOW_METADATA"
                    );
                }
                forms += 1;
            }
            // Non-canonical IPv4 spellings never parse at all.
            for f in noncanonical_forms(v4) {
                assert_eq!(
                    Origin::parse(&format!("http://{f}")),
                    Err(OriginError::NumericHost),
                    "{f} ({v4})"
                );
                forms += 1;
            }
        }
    }
    assert!(forms > 1_000, "{forms}");
}

/// The controls of the textual property: canonical class addresses parse, and a non-canonical
/// spelling of an ADMISSIBLE address is refused just the same (it is the spelling that is refused).
#[test]
fn canonical_literals_parse_and_noncanonical_spellings_never_do() {
    let mut r = Rng(0x5eed_f3f3_0006);
    for _ in 0..2_000 {
        let v4 = Ipv4Addr::from(r.next() as u32);
        let o = Origin::parse(&format!("http://{v4}")).unwrap();
        assert_eq!(o.ip_literal(), Some(IpAddr::V4(v4)));
        for f in noncanonical_forms(v4) {
            assert_eq!(
                Origin::parse(&format!("http://{f}")),
                Err(OriginError::NumericHost),
                "{f}"
            );
        }
        check_embeddings(v4);
        let v6 = Ipv6Addr::from((u128::from(r.next()) << 64) | u128::from(r.next()));
        let o = Origin::parse(&format!("http://[{v6}]")).unwrap();
        assert_eq!(o.ip_literal(), Some(IpAddr::V6(v6)));
        assert_eq!(Origin::parse(o.normalised()).unwrap(), o);
    }
    // Class rows are admitted only when their class is.
    let none = Nat64Prefixes::default();
    let loopback_only = AddressPolicy {
        classes: ClassSet {
            public: false,
            private: false,
            loopback: true,
        },
        allow_metadata: false,
        literal: None,
    };
    assert!(
        loopback_only
            .check("127.9.9.9".parse().unwrap(), &none)
            .is_ok()
    );
    assert!(
        loopback_only
            .check("::ffff:127.9.9.9".parse().unwrap(), &none)
            .is_ok()
    );
    assert!(
        loopback_only
            .check("10.0.0.1".parse().unwrap(), &none)
            .is_err()
    );
}

/// The fuzz decoder covers every fixture upstream and both outcomes (the fuzz target is not
/// structurally blind).
#[test]
fn the_fuzz_decoder_reaches_every_upstream() {
    let mut seen = std::collections::BTreeSet::new();
    for sel in 0u8..FIXTURE_UPSTREAMS.len() as u8 {
        let data = [sel, 3, b'G', b'E', b'T', 1, 0, b'/', 0];
        seen.insert(OwnedRequest::decode(&data).upstream);
    }
    assert_eq!(seen.len(), FIXTURE_UPSTREAMS.len());
}
