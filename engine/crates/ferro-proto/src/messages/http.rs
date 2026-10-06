//! HTTP-service wire messages (service `HTTP = 6`, M6-F2; SPEC §23.5) — a **bespoke positional
//! codec**, like `messages::sql`.
//!
//! They are `Value`-free, but they cannot ride the `msg!`/rmp-serde path either: header values and
//! bodies are msgpack **`bin`**, which rmp-serde writes for a `Vec<u8>` only through an extra
//! dependency (`serde_bytes`), and `HttpRequest.traceparent` must decode LOSSILY (`ExecRequest` field
//! 9's rule), which no derive expresses. So each message is written field by field with `rmp`, and
//! decoded with the same discipline as `value.rs`/`sql.rs`: every array and `str`/`bin` length is
//! `bound_len`-checked before it allocates, arity is strict, and trailing bytes are refused.
//!
//! **The codec moves bytes; it does not apply §23.4.** A header name is any UTF-8 `str`, a target any
//! UTF-8 `str`, a status any `u16`, a chunk any `bin` — what is ALLOWED is the validator's (slice F3)
//! and the producer's (F4) business, never the decoder's (the S7 precedent: "the codecs move that
//! text verbatim and validate nothing beyond UTF-8"). What the codec does enforce is the wire's TYPE
//! and WIDTH: a `u16` that does not fit is `Malformed`, a `str` that is not UTF-8 is `Malformed`
//! (except `traceparent`), a `bin` where a `str` belongs is `Malformed`.
//!
//! Layouts are pinned in `/proto/PROTOCOL.md` §12 and locked by the `http_*` golden vectors.

use crate::CodecError;
use crate::messages::sql::{peek_nil, read_opt_str, read_opt_u32, write_opt_str, write_opt_u32};
use crate::value::{bound_len, read_bin, read_bool, read_str, read_str_lossy};
use rmp::decode as dec;
use rmp::encode as enc;

/// `u64` statistics and lengths on this service are contractually bounded below 2^63, so the PHP
/// client reads them as native ints (`/proto/PROTOCOL.md` §2). A debug-only tripwire, as on `Stats`.
const U64_WIRE_BOUND: u64 = 1 << 63;

/// One header line: `[name: str, value: bin]`. A list of these, in wire order, is how every header
/// block on this service travels — duplicates and order are preserved; it is never a map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpHeaderField {
    pub name: String,
    pub value: Vec<u8>,
}

/// `REQUEST` (service `HTTP`, method `REQUEST` = 1) — client → server. A positional fixarray of 13
/// (`/proto/PROTOCOL.md` §12.1, SPEC §23.5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// The operator-declared upstream's NAME — never a URL (§23.3).
    pub upstream: String,
    pub method: String,
    /// Origin-form target (§23.4.2). Strict UTF-8 on the wire: non-UTF-8 is `Protocol`.
    pub target: String,
    /// The origin the caller believes the upstream has — checked against it, never used (§23.4).
    pub origin: Option<String>,
    pub headers: Vec<HttpHeaderField>,
    /// `None` is "no body"; `Some(vec![])` is a present, zero-length body. Distinct on the wire.
    pub body: Option<Vec<u8>>,
    pub timeout_ms: Option<u32>,
    pub connect_timeout_ms: Option<u32>,
    pub read_timeout_ms: Option<u32>,
    /// The CALLER's idempotency declaration (§23.7.2): `Some(true)` declares, `Some(false)`
    /// downgrades, `None` defers to the operator.
    pub idempotent: Option<bool>,
    /// Whether the engine decodes `gzip`/`deflate` (§23.9.2).
    pub decode: bool,
    /// Observability only; never sent upstream.
    pub route: Option<String>,
    /// The caller's W3C `traceparent`, decoded LOSSILY exactly as `ExecRequest::traceparent` is —
    /// an observability field must not fail a request. Never forwarded upstream.
    pub traceparent: Option<String>,
}

impl HttpRequest {
    pub const ARITY: u32 = 13;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_str(&mut out, &self.upstream).unwrap();
        enc::write_str(&mut out, &self.method).unwrap();
        enc::write_str(&mut out, &self.target).unwrap();
        write_opt_str(&mut out, &self.origin);
        write_headers(&mut out, &self.headers);
        write_opt_bin(&mut out, &self.body);
        write_opt_u32(&mut out, &self.timeout_ms);
        write_opt_u32(&mut out, &self.connect_timeout_ms);
        write_opt_u32(&mut out, &self.read_timeout_ms);
        write_opt_bool(&mut out, &self.idempotent);
        enc::write_bool(&mut out, self.decode).unwrap();
        write_opt_str(&mut out, &self.route);
        write_opt_str(&mut out, &self.traceparent);
        out
    }

    pub fn decode(b: &[u8]) -> Result<HttpRequest, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "HttpRequest", Self::ARITY)?;
        let upstream = read_str(&mut rd)?;
        let method = read_str(&mut rd)?;
        let target = read_str(&mut rd)?;
        let origin = read_opt_str(&mut rd)?;
        let headers = read_headers(&mut rd)?;
        let body = read_opt_bin(&mut rd)?;
        let timeout_ms = read_opt_u32(&mut rd)?;
        let connect_timeout_ms = read_opt_u32(&mut rd)?;
        let read_timeout_ms = read_opt_u32(&mut rd)?;
        let idempotent = read_opt_bool(&mut rd)?;
        let decode = read_bool(&mut rd)?;
        let route = read_opt_str(&mut rd)?;
        let traceparent = if peek_nil(&mut rd)? {
            None
        } else {
            Some(read_str_lossy(&mut rd)?)
        };
        expect_end(rd)?;
        Ok(HttpRequest {
            upstream,
            method,
            target,
            origin,
            headers,
            body,
            timeout_ms,
            connect_timeout_ms,
            read_timeout_ms,
            idempotent,
            decode,
            route,
            traceparent,
        })
    }
}

/// `HttpHead.decoded`: the `Content-Encoding` the engine removed while decoding (§23.9.2), and the
/// `Content-Length` it removed with it — `None` when the response had none, or one the engine could
/// not represent below 2^63. `[str, u64 | nil]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpDecoded {
    pub content_encoding: String,
    pub content_length: Option<u64>,
}

/// `HEAD` (service `HTTP`, method `HEAD` = 2) — server → client, sent once per response, before any
/// `BODY`. Not terminal and not flagged `STREAM`. A positional fixarray of 6 (§12.2, SPEC §23.5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpHead {
    pub status: u16,
    /// `10`, `11` or `20` (HTTP/1.0, 1.1, 2) — a producer contract, not a codec check.
    pub version: u8,
    /// The HTTP/1.x reason phrase as received (`bin`: it is not guaranteed UTF-8); `None` on HTTP/2.
    pub reason: Option<Vec<u8>>,
    /// As received, minus hop-by-hop, plus §23.9.2's changes; names lowercase (P6).
    pub headers: Vec<HttpHeaderField>,
    pub decoded: Option<HttpDecoded>,
    /// The engine's EFFECTIVE idempotency for this request (§23.7.2) — the one authority the client
    /// classifies a status and a later failure against.
    pub idempotent: bool,
}

impl HttpHead {
    pub const ARITY: u32 = 6;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_uint(&mut out, u64::from(self.status)).unwrap();
        enc::write_uint(&mut out, u64::from(self.version)).unwrap();
        write_opt_bin(&mut out, &self.reason);
        write_headers(&mut out, &self.headers);
        match &self.decoded {
            None => enc::write_nil(&mut out).unwrap(),
            Some(d) => {
                debug_assert!(
                    d.content_length.is_none_or(|n| n < U64_WIRE_BOUND),
                    "HttpDecoded.content_length is contractually bounded < 2^63; got {d:?}"
                );
                enc::write_array_len(&mut out, 2).unwrap();
                enc::write_str(&mut out, &d.content_encoding).unwrap();
                match d.content_length {
                    None => enc::write_nil(&mut out).unwrap(),
                    Some(n) => {
                        enc::write_uint(&mut out, n).unwrap();
                    }
                }
            }
        }
        enc::write_bool(&mut out, self.idempotent).unwrap();
        out
    }

    pub fn decode(b: &[u8]) -> Result<HttpHead, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "HttpHead", Self::ARITY)?;
        let status: u16 = dec::read_int(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("HttpHead status: {e:?}")))?;
        let version: u8 = dec::read_int(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("HttpHead version: {e:?}")))?;
        let reason = read_opt_bin(&mut rd)?;
        let headers = read_headers(&mut rd)?;
        let decoded = if peek_nil(&mut rd)? {
            None
        } else {
            expect_arity(&mut rd, "HttpHead.decoded", 2)?;
            let content_encoding = read_str(&mut rd)?;
            let content_length = if peek_nil(&mut rd)? {
                None
            } else {
                Some(dec::read_int::<u64, _>(&mut rd).map_err(|e| {
                    CodecError::Malformed(format!("HttpHead content_length: {e:?}"))
                })?)
            };
            Some(HttpDecoded {
                content_encoding,
                content_length,
            })
        };
        let idempotent = read_bool(&mut rd)?;
        expect_end(rd)?;
        Ok(HttpHead {
            status,
            version,
            reason,
            headers,
            decoded,
            idempotent,
        })
    }
}

/// `BODY` (service `HTTP`, method `BODY` = 3) — server → client, carried in a frame with the
/// `STREAM` flag. `[chunk: bin]` (§12.3, SPEC §23.5.3). The 256 KiB per-chunk bound and the
/// non-empty rule are the PRODUCER's contract (F4), not a codec check: no receiver enforces them, so
/// they are not registry constants either — if one ever must, it becomes a `/proto` key first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpBody {
    pub chunk: Vec<u8>,
}

impl HttpBody {
    pub const ARITY: u32 = 1;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.chunk.len() + 6);
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_bin(&mut out, &self.chunk).unwrap();
        out
    }

    pub fn decode(b: &[u8]) -> Result<HttpBody, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "HttpBody", Self::ARITY)?;
        let chunk = read_bin(&mut rd)?;
        expect_end(rd)?;
        Ok(HttpBody { chunk })
    }
}

/// Per-exchange accounting: `[queue_us, connect_us, tls_us, ttfb_us, total_us, bytes_sent,
/// bytes_received, reused]` — seven `u64` bounded < 2^63 and a `bool` (§12.4, SPEC §23.5.4).
/// `bytes_sent`/`bytes_received` are the write tracker's PLAINTEXT counts since this exchange's
/// dispatch on HTTP/1.1 (§22.2 (ct)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpStats {
    pub queue_us: u64,
    pub connect_us: u64,
    pub tls_us: u64,
    pub ttfb_us: u64,
    pub total_us: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub reused: bool,
}

impl HttpStats {
    pub const ARITY: u32 = 8;

    fn write(&self, out: &mut Vec<u8>) {
        let counts = [
            self.queue_us,
            self.connect_us,
            self.tls_us,
            self.ttfb_us,
            self.total_us,
            self.bytes_sent,
            self.bytes_received,
        ];
        debug_assert!(
            counts.iter().all(|&n| n < U64_WIRE_BOUND),
            "HttpStats u64 fields are contractually bounded < 2^63 (PHP int limit); got {self:?}"
        );
        enc::write_array_len(out, Self::ARITY).unwrap();
        for n in counts {
            enc::write_uint(out, n).unwrap();
        }
        enc::write_bool(out, self.reused).unwrap();
    }

    fn read(rd: &mut &[u8]) -> Result<HttpStats, CodecError> {
        expect_arity(rd, "HttpStats", Self::ARITY)?;
        let mut n = [0u64; 7];
        for slot in &mut n {
            *slot = dec::read_int(rd)
                .map_err(|e| CodecError::Malformed(format!("HttpStats: {e:?}")))?;
        }
        let reused = read_bool(rd)?;
        Ok(HttpStats {
            queue_us: n[0],
            connect_us: n[1],
            tls_us: n[2],
            ttfb_us: n[3],
            total_us: n[4],
            bytes_sent: n[5],
            bytes_received: n[6],
            reused,
        })
    }
}

/// The terminal `Outcome::Ok` body of a completed exchange, WHATEVER its status (SPEC §23.7.4):
/// `[trailers, stats]` (§12.4, SPEC §23.5.4). It composes with `Outcome::Ok` because
/// `HttpDone::encode()` is exactly one top-level MessagePack value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpDone {
    pub trailers: Vec<HttpHeaderField>,
    pub stats: HttpStats,
}

impl HttpDone {
    pub const ARITY: u32 = 2;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        write_headers(&mut out, &self.trailers);
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<HttpDone, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "HttpDone", Self::ARITY)?;
        let trailers = read_headers(&mut rd)?;
        let stats = HttpStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(HttpDone { trailers, stats })
    }
}

// --- shared helpers (one rule each, mirrored byte for byte in the PHP codec) ---

fn expect_arity(rd: &mut &[u8], what: &str, want: u32) -> Result<(), CodecError> {
    let n = dec::read_array_len(rd)
        .map_err(|e| CodecError::Malformed(format!("{what} array: {e:?}")))?;
    if n != want {
        return Err(CodecError::Malformed(format!("{what} len {n} != {want}")));
    }
    Ok(())
}

fn expect_end(rd: &[u8]) -> Result<(), CodecError> {
    if rd.is_empty() {
        Ok(())
    } else {
        Err(CodecError::TrailingBytes(rd.len()))
    }
}

fn write_headers(out: &mut Vec<u8>, headers: &[HttpHeaderField]) {
    enc::write_array_len(out, headers.len() as u32).unwrap();
    for h in headers {
        enc::write_array_len(out, 2).unwrap();
        enc::write_str(out, &h.name).unwrap();
        enc::write_bin(out, &h.value).unwrap();
    }
}

fn read_headers(rd: &mut &[u8]) -> Result<Vec<HttpHeaderField>, CodecError> {
    let n = dec::read_array_len(rd)
        .map_err(|e| CodecError::Malformed(format!("headers array: {e:?}")))? as usize;
    bound_len(n, rd.len())?; // bound BEFORE with_capacity (a lying u32 length)
    let mut headers = Vec::with_capacity(n);
    for _ in 0..n {
        expect_arity(rd, "header", 2)?;
        let name = read_str(rd)?;
        let value = read_bin(rd)?;
        headers.push(HttpHeaderField { name, value });
    }
    Ok(headers)
}

fn write_opt_bin(out: &mut Vec<u8>, v: &Option<Vec<u8>>) {
    match v {
        None => enc::write_nil(out).unwrap(),
        Some(b) => {
            enc::write_bin(out, b).unwrap();
        }
    }
}

fn read_opt_bin(rd: &mut &[u8]) -> Result<Option<Vec<u8>>, CodecError> {
    if peek_nil(rd)? {
        return Ok(None);
    }
    Ok(Some(read_bin(rd)?))
}

fn write_opt_bool(out: &mut Vec<u8>, v: &Option<bool>) {
    match v {
        None => enc::write_nil(out).unwrap(),
        Some(b) => enc::write_bool(out, *b).unwrap(),
    }
}

fn read_opt_bool(rd: &mut &[u8]) -> Result<Option<bool>, CodecError> {
    if peek_nil(rd)? {
        return Ok(None);
    }
    Ok(Some(read_bool(rd)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Outcome;

    fn h(name: &str, value: &[u8]) -> HttpHeaderField {
        HttpHeaderField {
            name: name.into(),
            value: value.to_vec(),
        }
    }

    fn full_request() -> HttpRequest {
        HttpRequest {
            upstream: "billing".into(),
            method: "POST".into(),
            target: "/v1/charges?x=1".into(),
            origin: Some("https://api.example.com".into()),
            headers: vec![h("content-type", b"application/json"), h("x-raw", &[0x80])],
            body: Some(vec![0xc0, 0x01]),
            timeout_ms: Some(30_000),
            connect_timeout_ms: Some(2_000),
            read_timeout_ms: Some(10_000),
            idempotent: Some(false),
            decode: true,
            route: Some("/v1/charges".into()),
            traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
        }
    }

    #[test]
    fn request_roundtrips_full_and_minimal_and_is_a_fixarray_of_13() {
        let minimal = HttpRequest {
            upstream: "u".into(),
            method: "GET".into(),
            target: "/".into(),
            origin: None,
            headers: vec![],
            body: None,
            timeout_ms: None,
            connect_timeout_ms: None,
            read_timeout_ms: None,
            idempotent: None,
            decode: false,
            route: None,
            traceparent: None,
        };
        for req in [full_request(), minimal] {
            let b = req.encode();
            assert_eq!(b[0], 0x9d, "HttpRequest is a fixarray(13)");
            assert_eq!(HttpRequest::decode(&b).unwrap(), req);
        }
    }

    #[test]
    fn an_empty_body_is_distinct_from_no_body() {
        let mut req = full_request();
        req.body = Some(vec![]);
        let present = req.encode();
        req.body = None;
        let absent = req.encode();
        assert_ne!(present, absent);
        assert_eq!(
            HttpRequest::decode(&present).unwrap().body,
            Some(vec![]),
            "a zero-length body is bin8(0), not nil"
        );
    }

    #[test]
    fn a_header_value_must_be_bin_and_a_name_must_be_str() {
        // [name: str, value: bin]: a str value or a bin name is a wire fault, not a value to adapt.
        let mut b = Vec::new();
        enc::write_array_len(&mut b, 1).unwrap();
        enc::write_array_len(&mut b, 2).unwrap();
        enc::write_str(&mut b, "a").unwrap();
        enc::write_str(&mut b, "b").unwrap();
        assert!(
            read_headers(&mut &b[..]).is_err(),
            "a str header value is refused"
        );
        let mut b = Vec::new();
        enc::write_array_len(&mut b, 1).unwrap();
        enc::write_array_len(&mut b, 2).unwrap();
        enc::write_bin(&mut b, b"a").unwrap();
        enc::write_bin(&mut b, b"b").unwrap();
        assert!(
            read_headers(&mut &b[..]).is_err(),
            "a bin header name is refused"
        );
    }

    #[test]
    fn a_non_utf8_target_is_refused_but_a_non_utf8_traceparent_is_not() {
        let mut req = full_request();
        req.traceparent = None;
        let mut b = req.encode();
        // Replace the target's first byte ('/') with 0xff: same length, invalid UTF-8.
        let at = b.windows(3).position(|w| w == b"/v1").unwrap();
        b[at] = 0xff;
        assert!(matches!(
            HttpRequest::decode(&b),
            Err(CodecError::Malformed(_))
        ));

        // traceparent: invalid UTF-8 decodes lossily (ExecRequest field 9's rule).
        let mut req = full_request();
        req.traceparent = Some("00-x".into());
        let mut b = req.encode();
        let n = b.len();
        b[n - 1] = 0xff; // the last byte of the traceparent body
        let back = HttpRequest::decode(&b).expect("a lossy traceparent never fails the request");
        assert_eq!(back.traceparent.as_deref(), Some("00-\u{fffd}"));
    }

    #[test]
    fn request_rejects_the_wrong_arity_and_trailing_bytes() {
        let mut b = full_request().encode();
        b.push(0xc0);
        assert!(matches!(
            HttpRequest::decode(&b),
            Err(CodecError::TrailingBytes(1))
        ));
        let mut short = full_request().encode();
        short[0] = 0x9c; // claim 12 fields
        assert!(matches!(
            HttpRequest::decode(&short),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn a_lying_header_count_is_refused_before_allocating() {
        let mut b = Vec::new();
        enc::write_array_len(&mut b, u32::MAX).unwrap();
        assert!(matches!(
            read_headers(&mut &b[..]),
            Err(CodecError::Truncated { .. })
        ));
    }

    #[test]
    fn head_roundtrips_every_decoded_arm() {
        for decoded in [
            None,
            Some(HttpDecoded {
                content_encoding: "gzip".into(),
                content_length: Some(70_000),
            }),
            Some(HttpDecoded {
                content_encoding: "deflate".into(),
                content_length: None,
            }),
        ] {
            let head = HttpHead {
                status: 503,
                version: 11,
                reason: Some(b"Service Unavailable".to_vec()),
                headers: vec![h("retry-after", b"5")],
                decoded,
                idempotent: true,
            };
            let b = head.encode();
            assert_eq!(b[0], 0x96, "HttpHead is a fixarray(6)");
            assert_eq!(HttpHead::decode(&b).unwrap(), head);
        }
    }

    #[test]
    fn a_status_that_does_not_fit_u16_is_refused() {
        let mut b = Vec::new();
        enc::write_array_len(&mut b, 6).unwrap();
        enc::write_uint(&mut b, 70_000).unwrap();
        enc::write_uint(&mut b, 11).unwrap();
        enc::write_nil(&mut b).unwrap();
        enc::write_array_len(&mut b, 0).unwrap();
        enc::write_nil(&mut b).unwrap();
        enc::write_bool(&mut b, false).unwrap();
        assert!(matches!(
            HttpHead::decode(&b),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn body_roundtrips_and_is_bin() {
        let body = HttpBody {
            chunk: vec![0xc0; 300],
        };
        let b = body.encode();
        assert_eq!(
            &b[..4],
            &[0x91, 0xc5, 0x01, 0x2c],
            "fixarray(1) + bin16(300)"
        );
        assert_eq!(HttpBody::decode(&b).unwrap(), body);
    }

    #[test]
    fn done_composes_with_outcome_ok() {
        let done = HttpDone {
            trailers: vec![h("grpc-status", b"0")],
            stats: HttpStats {
                queue_us: 1,
                connect_us: 2,
                tls_us: 3,
                ttfb_us: 4,
                total_us: 5_000_000_000,
                bytes_sent: 6,
                bytes_received: 7,
                reused: true,
            },
        };
        let body = done.encode();
        assert_eq!(body[0], 0x92, "HttpDone is a fixarray(2)");
        match Outcome::decode(&Outcome::Ok(body.clone()).encode()).unwrap() {
            Outcome::Ok(recovered) => assert_eq!(HttpDone::decode(&recovered).unwrap(), done),
            other => panic!("expected Outcome::Ok, got {other:?}"),
        }
        let mut trailing = body;
        trailing.push(0xc0);
        assert!(matches!(
            HttpDone::decode(&trailing),
            Err(CodecError::TrailingBytes(1))
        ));
    }
}
