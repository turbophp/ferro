//! **M6-F4b — chaos 6's RSS half (SPEC §23.14): a reader that stops replenishing credit holds the
//! engine's memory to the window, for a plain body and for a decompression bomb.**
//!
//! F4a proved the BACKPRESSURE half (`http_engine_it`'s stall probe: exactly the credit's frames,
//! the upstream stalls). This binary proves the MEMORY half, by reading the process's resident set
//! (`VmRSS`) — which is why it is its own test binary with one test: a resident-set measurement is
//! process-wide, and any other test running in parallel in the same process would be measured too.
//!
//! The calibration (SPEC §22.2 (bj)'s rule — a stall probe proves nothing if slowness alone can
//! produce it): each upstream offers 1 GiB (plain) / an endless bomb, far beyond the 64 MiB bound,
//! and the test first waits for the upstream to SETTLE (nothing written for 300 ms) before it reads
//! RSS. A producer that buffered the response would never let the upstream settle short of the
//! whole gigabyte — the settle wait would time out, or RSS would carry the gigabyte — and one that
//! is merely slow would still be reading when the settle check looks.
#![cfg(feature = "http")]

mod common;
mod http_support;

use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferro_http::engine::TcpConnect;
use ferro_proto::consts::{flags, method_http};
use ferro_proto::messages::{HttpRequest, Outcome};
use http_support::*;

const MIB: u64 = 1024 * 1024;
/// What the engine may add to the resident set while a stalled reader holds a response. The
/// window is 16 MiB (64 frames of ≤ 256 KiB, or `credit_bytes`), plus `hyper`'s ≤ 512 KiB read
/// buffer, the decoder's one 256 KiB step, and allocator slack.
const RSS_BOUND: u64 = 64 * MIB;

fn rss() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    let kb: u64 = s
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .expect("VmRSS");
    kb * 1024
}

/// Wait until the upstream has written nothing for 300 ms; panics after 20 s.
async fn settle(rec: &Rec) -> u64 {
    let start = Instant::now();
    let mut last = rec.written();
    let mut still = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "the upstream never stalled (wrote {} B): the engine is reading without credit",
            rec.written()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = rec.written();
        if now != last {
            last = now;
            still = Instant::now();
        } else if still.elapsed() >= Duration::from_millis(300) {
            return now;
        }
    }
}

fn gzip_zeros(n: usize) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(&vec![0u8; n]).unwrap();
    e.finish().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos6_rss_is_bounded_by_the_window_for_a_plain_body_and_a_bomb() {
    // A plain body of 1 GiB, written as fast as the engine will take it.
    let plain = upstream(|mut s, rec| async move {
        if rec.read_request(&mut s).await.is_none() {
            return;
        }
        let total: u64 = 1024 * MIB;
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n\r\n");
        if rec.write(&mut s, head.as_bytes()).await.is_err() {
            return;
        }
        let piece = vec![b'p'; 256 * 1024];
        let mut sent = 0;
        while sent < total {
            if rec.write(&mut s, &piece).await.is_err() {
                return;
            }
            sent += piece.len() as u64;
        }
    })
    .await;
    // An endless decompression bomb: 64 MiB of zeros per ~64 KiB gzip member, forever.
    let member = Arc::new(gzip_zeros(64 * MIB as usize));
    let bomb = upstream(move |mut s, rec| {
        let member = Arc::clone(&member);
        async move {
            if rec.read_request(&mut s).await.is_none() {
                return;
            }
            let head =
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n";
            if rec.write(&mut s, head).await.is_err() {
                return;
            }
            loop {
                let mut out = format!("{:x}\r\n", member.len()).into_bytes();
                out.extend_from_slice(&member);
                out.extend_from_slice(b"\r\n");
                if rec.write(&mut s, &out).await.is_err() {
                    return;
                }
            }
        }
    })
    .await;
    let d = daemon_with(
        upstreams(&[("plain", plain.addr), ("bomb", bomb.addr)], &[]),
        Arc::new(TcpConnect),
        |_| {},
    );
    let mut c = d.client().await;

    for (rid, name, rec, decode) in [
        (2u32, "plain", &plain.rec, false),
        (3, "bomb", &bomb.rec, true),
    ] {
        let before = rss();
        send(
            &mut c,
            rid,
            &HttpRequest {
                decode,
                timeout_ms: Some(60_000),
                ..request(name, "GET", "/big")
            },
        )
        .await;
        // Read every frame the window allows, never replenishing; drop each as it arrives.
        let mut frames = 0u32;
        let mut delivered = 0u64;
        while let Some(f) = c.recv_or_none(Duration::from_millis(1_000)).await {
            assert_eq!(
                f.header.flags & flags::END,
                0,
                "{name}: no terminal while parked"
            );
            frames += 1;
            if f.header.method == method_http::BODY {
                delivered += f.payload.len() as u64;
            }
        }
        let wrote = settle(rec).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let after = rss();
        let grew = after.saturating_sub(before);
        eprintln!(
            "F4b chaos 6 RSS ({name}): {frames} frames / {delivered} B delivered; upstream wrote \
             {wrote} B and stalled; RSS {before} → {after} (+{grew} B, bound {RSS_BOUND} B)"
        );
        assert!(frames <= 64, "{name}: within the window");
        assert!(
            wrote < 512 * MIB,
            "{name}: the upstream stalled far short of what it offered"
        );
        assert!(
            grew < RSS_BOUND,
            "{name}: resident set grew {grew} B with a stalled reader (bound {RSS_BOUND} B)"
        );
        c.cancel(rid).await;
        loop {
            let f = c.recv().await;
            if f.header.flags & flags::END != 0 {
                assert!(matches!(
                    Outcome::decode(&f.payload).unwrap(),
                    Outcome::Cancelled
                ));
                break;
            }
        }
    }
    wait_for("no live connection", || d.engine.live_connections() == 0).await;
}
