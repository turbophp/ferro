//! **C4b: the §13 Prometheus endpoint, scraped over a real socket.**
//!
//! The unit tests in `metrics.rs` prove the exposition format and the routing against a hand-built
//! registry. This proves the thing they cannot: that a statement a CLIENT sent moves a counter an
//! operator can actually SCRAPE — through a real listener, over a real TCP connection, parsed the
//! way Prometheus would parse it.
//!
//! That is the same split C4a recorded and C3-4 before it: a unit test of the renderer passes with
//! the counter never incremented, and a unit test of the counter passes with no endpoint at all.
//!
//! It runs on a SQLite pool, so it needs no server and runs everywhere including CI.

mod common;

use common::{exec_ok, exec_server_with_metrics, req};
use ferro_proto::messages::sql::ExecRequest;

fn write(sql: &str) -> ExecRequest {
    ExecRequest {
        readonly: false,
        ..req(sql)
    }
}

/// Read one scrape the way a scraper does: connect, `GET /metrics`, read to EOF.
async fn scrape(addr: std::net::SocketAddr) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("send request");
    let mut out = String::new();
    s.read_to_string(&mut out).await.expect("read response");
    out
}

/// Pull one sample's value out of an exposition body.
fn sample(body: &str, needle: &str) -> Option<u64> {
    body.lines()
        .find(|l| l.starts_with(needle))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_real_pin_moves_a_counter_a_real_scrape_can_read() {
    let (server, metrics_addr, _metrics_guard) = exec_server_with_metrics().await;
    let mut c = server.connect().await;
    c.hello(1).await;

    let before = scrape(metrics_addr).await;

    // (1) The response is a well-formed scrape, not merely non-empty.
    assert!(
        before.starts_with("HTTP/1.1 200 OK\r\n"),
        "not a 200:\n{before}",
    );
    assert!(
        before.contains("Content-Type: text/plain; version=0.0.4"),
        "wrong content type:\n{before}",
    );

    // (2) Every cause is present BEFORE anything has happened — the zeroes matter, because an
    // operator cannot alert on a series that does not exist yet.
    for label in [
        "tx",
        "listen",
        "lock",
        "prepare",
        "temp",
        "set",
        "pin_function",
        "unknown",
        "session_tracker",
    ] {
        let needle = format!("ferro_pin_cause_total{{pool=\"default\",cause=\"{label}\"}}");
        assert!(
            before.contains(&needle),
            "cause {label} is missing from a fresh scrape:\n{before}",
        );
    }

    const SET: &str = "ferro_pin_cause_total{pool=\"default\",cause=\"set\"}";
    let set_before = sample(&before, SET).expect("the set series parses");

    // (3) Do something that really pins. On SQLite a `PRAGMA` taints UNCONDITIONALLY — the
    // load-bearing property §22.2 (bm) rests on, since pragmas are connection-scoped and the
    // narrowed `Targeted` hygiene profile is only safe because they always taint — and the
    // classifier labels it `Set`. So this is not a contrived pin: it is the exact statement class
    // that makes SQLite's hygiene correct, now visible to an operator.
    exec_ok(&mut c, 2, &write("pragma foreign_keys = on")).await;

    let after = scrape(metrics_addr).await;
    let set_after = sample(&after, SET).expect("the set series still parses");

    assert!(
        set_after > set_before,
        "a real PRAGMA did not move the set pin counter ({set_before} -> {set_after}):\n{after}",
    );

    // (4) The endpoint is only the endpoint.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(metrics_addr).await.unwrap();
    s.write_all(b"GET /admin HTTP/1.1\r\n\r\n").await.unwrap();
    let mut other = String::new();
    s.read_to_string(&mut other).await.unwrap();
    assert!(other.starts_with("HTTP/1.1 404"), "{other}");
}
