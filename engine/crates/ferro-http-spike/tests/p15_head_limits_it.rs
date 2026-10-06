//! P15 (SPEC §23.19): "`hyper` 1.x's http1 client builder exposes `max_buf_size` and `max_headers`,
//! and the http2 builder `max_header_list_size`. Oversize heads are reported as errors, not
//! truncated."
//!
//! The HTTP/1.1 half runs over a scripted in-memory I/O (`common::Scripted`) so the chunking hyper
//! sees is decided by the test, not by TCP segmentation: the measurement below shows that chunking
//! is exactly what decides whether `max_buf_size` is an exact limit.

mod common;

use std::time::Duration;

use common::*;
use hyper::client::conn::{http1, http2};

const HYPER_TOO_LARGE: &str = "message head is too large";

/// A response head of exactly `total` bytes: status line, `content-length: 0`, and one padding
/// field sized to make up the total.
fn head_of_size(total: usize) -> Vec<u8> {
    let prefix = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nx-pad: ";
    let suffix = "\r\n\r\n";
    let pad = total - prefix.len() - suffix.len();
    let mut h = Vec::with_capacity(total);
    h.extend_from_slice(prefix.as_bytes());
    h.resize(h.len() + pad, b'a');
    h.extend_from_slice(suffix.as_bytes());
    assert_eq!(h.len(), total);
    h
}

/// A response head with exactly `fields` header fields (the first is `content-length: 0`).
fn head_with_fields(fields: usize) -> Vec<u8> {
    let mut h = String::from("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n");
    for i in 1..fields {
        h.push_str(&format!("x-f{i}: v\r\n"));
    }
    h.push_str("\r\n");
    h.into_bytes()
}

/// Run one exchange over `io` with `builder`; `Ok(field count)` or `Err(error display)`.
async fn exchange(io: Scripted, builder: &http1::Builder) -> Result<usize, String> {
    let (mut send, track, _conn) = h1_over(io, builder).await;
    track.arm();
    match within("exchange", send.send_request(get())).await {
        Ok(resp) => Ok(resp.headers().len()),
        Err(e) => {
            assert!(e.is_parse(), "an oversize head is a parse error, not {e:?}");
            Err(e.to_string())
        }
    }
}

/// **P15, `max_headers`: settable, and EXACT.** 256 fields are delivered whole (all 256 present —
/// not truncated); 257 are refused as a parse error, whatever the read chunking.
///
/// Controls: the same 257-field head is ACCEPTED when `max_headers` is raised to 300 (so the setter
/// is what refuses it), and hyper's own default (100) refuses 101 fields — the default would have
/// silently been the limit had the engine not set one.
#[tokio::test]
async fn p15_h1_max_headers_is_settable_and_exact() {
    let b = h1_builder();
    for chunk in [usize::MAX, 4096, 1] {
        let ok = exchange(Scripted::new(&head_with_fields(H1_MAX_HEADERS), chunk), &b).await;
        assert_eq!(
            ok,
            Ok(H1_MAX_HEADERS),
            "256 fields, chunk {chunk}: delivered whole"
        );
        let over = exchange(
            Scripted::new(&head_with_fields(H1_MAX_HEADERS + 1), chunk),
            &b,
        )
        .await;
        assert_eq!(
            over,
            Err(HYPER_TOO_LARGE.into()),
            "257 fields, chunk {chunk}"
        );
    }

    // Control 1: the setter is what refuses.
    let mut wider = h1_builder();
    wider.max_headers(300);
    assert_eq!(
        exchange(
            Scripted::new(&head_with_fields(H1_MAX_HEADERS + 1), usize::MAX),
            &wider
        )
        .await,
        Ok(H1_MAX_HEADERS + 1)
    );
    // Control 2: hyper's default is 100.
    let default = http1::Builder::new();
    assert_eq!(
        exchange(Scripted::new(&head_with_fields(100), usize::MAX), &default).await,
        Ok(100)
    );
    assert_eq!(
        exchange(Scripted::new(&head_with_fields(101), usize::MAX), &default).await,
        Err(HYPER_TOO_LARGE.into())
    );
}

/// **P15, `max_buf_size`: settable, an error not a truncation — but NOT an exact head limit.**
///
/// * Delivered in reads no larger than 4 KiB, the boundary is exact: a 262 144-byte head is
///   delivered and a 262 145-byte head is refused.
/// * Delivered in ONE read, a head well past `max_buf_size` is delivered: hyper checks the buffer
///   length only after a parse attempt fails, and a single read may fill the buffer's whole spare
///   capacity, which `bytes`' amortised growth has made larger than `max_buf_size`.
/// * The ceiling is hard, though: the buffer only reallocates while its capacity is below
///   `max_buf_size`, and each reallocation at most doubles it, so its capacity — and with it any
///   delivered head — stays strictly below `2 × max_buf_size`. A head of exactly `2 × max_buf_size`
///   is refused under every delivery pattern tried.
#[tokio::test]
async fn p15_h1_max_buf_size_is_a_soft_limit_with_a_hard_ceiling_below_twice() {
    let b = h1_builder();
    let max = H1_MAX_BUF_SIZE;

    // Small reads: exact.
    assert!(
        exchange(Scripted::new(&head_of_size(max), 4096), &b)
            .await
            .is_ok()
    );
    assert_eq!(
        exchange(Scripted::new(&head_of_size(max + 1), 4096), &b).await,
        Err(HYPER_TOO_LARGE.into()),
        "small reads: one byte over is refused"
    );

    // One read: the largest delivered head, found by bisection (deterministic: no TCP involved).
    let mut lo = max;
    let mut hi = 2 * max;
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if exchange(Scripted::new(&head_of_size(mid), usize::MAX), &b)
            .await
            .is_ok()
        {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    eprintln!("P15: one-read delivery: largest delivered head {lo} B with max_buf_size {max} B");
    assert!(
        lo > max,
        "the caveat: a single read delivers a head larger than max_buf_size ({lo} B)"
    );

    // The hard ceiling, under every delivery shape tried.
    for (name, io) in [
        (
            "one read",
            Scripted::new(&head_of_size(2 * max), usize::MAX),
        ),
        ("4 KiB reads", Scripted::new(&head_of_size(2 * max), 4096)),
        (
            "max-1 then rest",
            Scripted::split_at(&head_of_size(2 * max), &[max - 1]),
        ),
        (
            "260000 then rest",
            Scripted::split_at(&head_of_size(2 * max), &[260_000]),
        ),
        (
            "8 KiB then one read",
            Scripted::split_at(&head_of_size(2 * max), &[8192]),
        ),
    ] {
        assert_eq!(
            exchange(io, &b).await,
            Err(HYPER_TOO_LARGE.into()),
            "a 2 × max_buf_size head must be refused ({name})"
        );
    }

    // Control: the setter is what limits — the same oversize head under a larger max_buf_size is
    // delivered whole.
    let mut wider = h1_builder();
    wider.max_buf_size(4 * max);
    assert!(
        exchange(Scripted::new(&head_of_size(2 * max), 4096), &wider)
            .await
            .is_ok()
    );
}

// ---------------------------------------------------------------------------------------------
// HTTP/2: `max_header_list_size`.
// ---------------------------------------------------------------------------------------------

const H2_MAX_HEADER_LIST: u32 = 256 * 1024;

/// One HTTP/2 exchange against an `h2` server that answers with a single `x-pad` header of
/// `pad` bytes. Returns the client's result.
async fn h2_exchange(pad: usize, limit: Option<u32>) -> Result<usize, hyper::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(tcp).await.unwrap();
        if let Some(Ok((_req, mut respond))) = conn.accept().await {
            let resp = http::Response::builder()
                .status(200)
                .header("x-pad", "a".repeat(pad))
                .body(())
                .unwrap();
            let _ = respond.send_response(resp, true);
        }
        // Keep driving the connection so the client can read.
        while let Some(r) = conn.accept().await {
            if r.is_err() {
                break;
            }
        }
    });

    let mut builder = http2::Builder::new(hyper_util::rt::TokioExecutor::new());
    if let Some(l) = limit {
        builder.max_header_list_size(l);
    }
    let tcp = dial(addr).await;
    let (mut send, conn) = builder
        .handshake::<_, OneChunk>(hyper_util::rt::TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let req = http::Request::get(format!("http://{addr}/"))
        .body(OneChunk::empty())
        .unwrap();
    let r = within("h2 exchange", send.send_request(req)).await;
    server.abort();
    r.map(|resp| resp.headers().get("x-pad").map_or(0, |v| v.len()))
}

/// **P15, HTTP/2 `max_header_list_size`: settable, and an oversize head is an error.** A head
/// under the limit is delivered whole; one over it fails the request rather than arriving
/// truncated.
///
/// **Finding for the cause mapping (and F1b's P2):** the refusal surfaces as a LIBRARY-initiated
/// stream reset with reason `PROTOCOL_ERROR` (`h2` 0.4.19 `streams.rs`, the client arm of
/// `RecvHeaderBlockError::Oversize`; the comment in `recv.rs` that promises `REFUSED_STREAM` is
/// stale). That is the same error `h2` raises for a MALFORMED header block (`framed_read.rs`,
/// `MalformedMessage` → `library_reset(PROTOCOL_ERROR)`), so on HTTP/2 `oversize_head` and a
/// malformed head are not separable through the public API: both are `h2_stream_error`. In v1 only
/// effectively-idempotent requests ride HTTP/2, where every one of these is the same fate
/// (Retryable `ConnectionLost`), so nothing in §23.7.1 changes. The server PROCESSED this request,
/// and the reset is `is_remote() == false` — the shape P2's negative controls must refuse.
#[tokio::test]
async fn p15_h2_max_header_list_size_is_settable_and_oversize_is_an_error() {
    // HPACK list size = name + value + 32 per field (RFC 9113 §6.5.2); `:status` adds 42.
    let under = H2_MAX_HEADER_LIST as usize - 42 - (5 + 32) - 1024;
    let over = H2_MAX_HEADER_LIST as usize + 1024;

    assert_eq!(
        h2_exchange(under, Some(H2_MAX_HEADER_LIST)).await.unwrap(),
        under,
        "under the limit: delivered whole"
    );

    let err = h2_exchange(over, Some(H2_MAX_HEADER_LIST))
        .await
        .expect_err("over the limit must be an error, not a truncated head");
    let h2e = std::error::Error::source(&err)
        .and_then(|s| s.downcast_ref::<h2::Error>())
        .unwrap_or_else(|| panic!("hyper's h2 error carries an h2::Error source: {err:?}"));
    assert!(h2e.is_reset(), "{h2e:?}");
    assert_eq!(h2e.reason(), Some(h2::Reason::PROTOCOL_ERROR), "{h2e:?}");
    assert!(
        !h2e.is_remote(),
        "a LIBRARY-initiated reset on a request the server processed: {h2e:?}"
    );

    // Control: the setter is what decides. hyper's OWN default is 16 KiB (`proto/h2/client.rs`
    // `DEFAULT_MAX_HEADER_LIST_SIZE`, not h2's 16 MiB), so without the setter a 20 KiB head is
    // refused, and with §23.9.1's 256 KiB the same head is delivered.
    let modest = 20 * 1024;
    assert!(
        h2_exchange(modest, None).await.is_err(),
        "hyper default 16 KiB"
    );
    assert_eq!(
        h2_exchange(modest, Some(H2_MAX_HEADER_LIST)).await.unwrap(),
        modest
    );

    // Far over the limit the refusal is no longer a stream reset: `h2` caps CONTINUATION frames at
    // `limit / frame size × 1.25` (`calc_max_continuation_frames`) and answers past it with a
    // CONNECTION-level GOAWAY(ENHANCE_YOUR_CALM). Still an error, never a truncated head — but it
    // fails every stream on that connection, which on a shared HTTP/2 connection is the §23.1
    // cross-tenant coupling (a retry for the other idempotent streams, never a fate loss).
    let err = h2_exchange(2 * 1024 * 1024, Some(H2_MAX_HEADER_LIST))
        .await
        .expect_err("far over the limit");
    let h2e = std::error::Error::source(&err)
        .and_then(|s| s.downcast_ref::<h2::Error>())
        .unwrap_or_else(|| panic!("{err:?}"));
    assert!(h2e.is_go_away() && !h2e.is_remote(), "{h2e:?}");
    assert_eq!(h2e.reason(), Some(h2::Reason::ENHANCE_YOUR_CALM), "{h2e:?}");
    tokio::time::sleep(Duration::from_millis(1)).await;
}
