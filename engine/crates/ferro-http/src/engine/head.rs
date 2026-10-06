//! Response heads and `hyper` errors: the exact head limit (§23.9.1), the `HttpHead` the client
//! receives (§23.5.2), and the cause derivation for "sent, no head" (§23.5.6, premise P14).

use std::io;

use ferro_proto::messages::{HttpDecoded, HttpHead, HttpHeaderField};
use http::{HeaderMap, StatusCode, Version};

use super::track::{Dir, TrackState};
use crate::fate::{DispatchedNotSent, HeadReceived, SentNoHead};

/// The exact response-head limit (§23.9.1, as amended by F1a): `hyper`'s `max_buf_size` is the
/// memory backstop, never the limit, because one read can deliver a head up to ~2× it (P15).
pub const MAX_HEAD_BYTES: usize = 256 * 1024;
/// `hyper`'s `max_buf_size` (the backstop) and `max_headers` (exact, P15).
pub const H1_MAX_BUF_SIZE: usize = 256 * 1024;
pub const H1_MAX_HEADERS: usize = 256;

/// `hyper` 1.11.1's `Display` for `Parse::TooLarge`. With D20's features (`client`, no `server`),
/// `hyper::Error::is_parse_too_large()` is not compiled, so this text is the only public signal
/// separating an oversize head from a malformed one (P14's second caveat). Pinned by
/// `oversize_and_malformed_heads_are_told_apart_by_hyper_s_text` in `ferrod`'s e2e suite: an upgrade
/// that changes the text fails loudly. If it drifted unnoticed, an oversize head would read as
/// `malformed_head` — the same Guzzle class and the same fate.
pub const HYPER_TOO_LARGE: &str = "message head is too large";

/// The status line's reason phrase as received: `hyper` stores a non-canonical one in the
/// response's `ReasonPhrase` extension, and leaves the canonical one implicit.
pub fn reason_phrase(ext: &http::Extensions, status: StatusCode) -> Vec<u8> {
    if let Some(r) = ext.get::<hyper::ext::ReasonPhrase>() {
        return r.as_bytes().to_vec();
    }
    status
        .canonical_reason()
        .map(|r| r.as_bytes().to_vec())
        .unwrap_or_default()
}

/// The engine's exact head measure (§23.9.1, F1a): the status line (`HTTP/1.1 NNN reason` + CRLF),
/// plus each field's name, value and 4 bytes (`": "` and CRLF), plus the final CRLF.
pub fn head_size(reason: &[u8], headers: &HeaderMap) -> usize {
    // `HTTP/1.1 NNN` is 12 bytes; the SP before the reason is counted only when there IS a reason:
    // `HTTP/1.1 200\r\n` (no SP, empty reason) is legal and `httparse` accepts it, and counting a
    // space that was never sent refused a head exactly at the limit (review of M6-F4a, finding 6).
    // `HTTP/1.1 200 \r\n` (SP, empty reason) is therefore under-counted by one byte — the
    // permitted direction (§23.9.1 as amended).
    let sp = usize::from(!reason.is_empty());
    let status_line = "HTTP/1.1 200".len() + sp + reason.len() + 2;
    let fields: usize = headers
        .iter()
        .map(|(n, v)| n.as_str().len() + v.as_bytes().len() + 4)
        .sum();
    status_line + fields + 2
}

/// Hop-by-hop response fields, removed before `HEAD` (§23.5.2): the fixed RFC 9110 §7.6.1 set,
/// plus every name the `Connection` field lists.
fn hop_by_hop(headers: &HeaderMap) -> Vec<String> {
    let mut v: Vec<String> = [
        "connection",
        "keep-alive",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for c in headers.get_all(http::header::CONNECTION) {
        if let Ok(s) = c.to_str() {
            for tok in s.split(',') {
                let t = tok.trim().to_ascii_lowercase();
                if !t.is_empty() {
                    v.push(t);
                }
            }
        }
    }
    v
}

/// Header fields as the wire carries them: lowercase names (they are, in `http`), values as bytes,
/// duplicates kept in order.
pub fn fields(headers: &HeaderMap, drop: &[String]) -> Vec<HttpHeaderField> {
    headers
        .iter()
        .filter(|(n, _)| !drop.iter().any(|d| d == n.as_str()))
        .map(|(n, v)| HttpHeaderField {
            name: n.as_str().to_string(),
            value: v.as_bytes().to_vec(),
        })
        .collect()
}

/// The `HEAD` frame for a response. `decoded_as` is the `Content-Encoding` value the engine decodes
/// (§23.9.2, M6-F4b), or `None` when the body passes through: when set, `content-encoding` and
/// `content-length` are removed from the fields and reported in `decoded` — the length as
/// received, `nil` when absent or not a number the wire can carry (§22.2 (cy) item 3).
pub fn http_head(
    status: StatusCode,
    version: Version,
    reason: Vec<u8>,
    headers: &HeaderMap,
    idempotent: bool,
    decoded_as: Option<String>,
) -> HttpHead {
    let mut drop = hop_by_hop(headers);
    let decoded = decoded_as.map(|content_encoding| {
        drop.push("content-encoding".to_string());
        drop.push("content-length".to_string());
        HttpDecoded {
            content_encoding,
            content_length: headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|&n| n < (1 << 63)),
        }
    });
    HttpHead {
        status: status.as_u16(),
        version: if version == Version::HTTP_10 { 10 } else { 11 },
        reason: Some(reason),
        headers: fields(headers, &drop),
        decoded,
        idempotent,
    }
}

fn io_kind(err: &hyper::Error) -> Option<io::ErrorKind> {
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<io::Error>() {
            return Some(io.kind());
        }
        src = s.source();
    }
    None
}

/// A failed exchange before a head, once `sent` is final: which §23.5.6 cause (P14's derivation —
/// the `hyper` error alone does not separate `eof_empty` from `eof_partial_head`, the tracker's
/// dispatch-relative read count does).
pub fn sent_no_head(err: &hyper::Error, track: &TrackState) -> SentNoHead {
    if err.is_parse() && err.to_string() == HYPER_TOO_LARGE {
        SentNoHead::OversizeHead
    } else if err.is_parse() {
        SentNoHead::MalformedHead
    } else if err.is_incomplete_message() {
        if track.read() > 0 {
            SentNoHead::EofPartialHead
        } else {
            SentNoHead::EofEmpty
        }
    } else if matches!(track.first_error(), Some((Dir::Write, _))) {
        // `write` only when the tracker saw the WRITE side fail first (§23.5.6, P14's caveat).
        SentNoHead::Write
    } else {
        // An I/O failure on the read side, or a connection `hyper` found closed (`is_canceled`):
        // bytes of the request reached the upstream and the link then died.
        SentNoHead::Reset
    }
}

/// A failed exchange before a head with `sent = false` (§23.7.1 "dispatched, not sent").
pub fn not_sent(track: &TrackState) -> DispatchedNotSent {
    match track.first_error() {
        Some((Dir::Write, _)) => DispatchedNotSent::UnsentWrite,
        _ => DispatchedNotSent::UnsentClosed,
    }
}

/// A body failure after the head.
pub fn after_head(err: &hyper::Error) -> HeadReceived {
    if err.is_incomplete_message() {
        return HeadReceived::BodyEof;
    }
    match io_kind(err) {
        Some(io::ErrorKind::UnexpectedEof) => HeadReceived::BodyEof,
        Some(
            io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected,
        ) => HeadReceived::BodyReset,
        Some(io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput) => HeadReceived::BodyFraming,
        Some(_) => HeadReceived::BodyReset,
        None if err.is_parse() => HeadReceived::BodyFraming,
        None => HeadReceived::BodyFraming,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_size_is_the_documented_measure() {
        let mut h = HeaderMap::new();
        h.insert("content-length", "0".parse().unwrap());
        h.append("x-a", "bc".parse().unwrap());
        // "HTTP/1.1 200 OK\r\n" = 17; "content-length: 0\r\n" = 19; "x-a: bc\r\n" = 9; "\r\n" = 2.
        assert_eq!(head_size(b"OK", &h), 17 + 19 + 9 + 2);
        // "HTTP/1.1 200\r\n" = 14: no reason, no SP.
        assert_eq!(head_size(b"", &h), 14 + 19 + 9 + 2);
    }

    #[test]
    fn hop_by_hop_fields_and_connection_listed_names_are_removed() {
        let mut h = HeaderMap::new();
        h.insert("connection", "keep-alive, x-private".parse().unwrap());
        h.insert("keep-alive", "timeout=5".parse().unwrap());
        h.insert("transfer-encoding", "chunked".parse().unwrap());
        h.insert("x-private", "1".parse().unwrap());
        h.append("set-cookie", "a=1".parse().unwrap());
        h.append("set-cookie", "b=2".parse().unwrap());
        let head = http_head(
            StatusCode::OK,
            Version::HTTP_11,
            b"OK".to_vec(),
            &h,
            false,
            None,
        );
        let names: Vec<&str> = head.headers.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["set-cookie", "set-cookie"]);
        assert_eq!(head.version, 11);
        assert_eq!(head.decoded, None);
    }

    /// §23.9.2: a decoded body's head loses `content-encoding` and `content-length`, which are
    /// reported in `decoded` instead; every other field is kept.
    #[test]
    fn a_decoded_head_moves_the_encoding_and_length_into_decoded() {
        let mut h = HeaderMap::new();
        h.insert("content-encoding", "gzip".parse().unwrap());
        h.insert("content-length", "1234".parse().unwrap());
        h.insert("etag", "\"x\"".parse().unwrap());
        let head = http_head(
            StatusCode::OK,
            Version::HTTP_11,
            b"OK".to_vec(),
            &h,
            true,
            Some("gzip".into()),
        );
        let names: Vec<&str> = head.headers.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["etag"]);
        assert_eq!(
            head.decoded,
            Some(HttpDecoded {
                content_encoding: "gzip".into(),
                content_length: Some(1234),
            })
        );
        h.remove("content-length");
        let head = http_head(
            StatusCode::OK,
            Version::HTTP_11,
            b"OK".to_vec(),
            &h,
            true,
            Some("gzip".into()),
        );
        assert_eq!(head.decoded.unwrap().content_length, None);
    }
}
