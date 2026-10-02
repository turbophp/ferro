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

use common::{exec_err, exec_ok, exec_server_with_metrics, req};
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

/// One labelled sample's value, or `None` if the series is absent.
fn series(body: &str, name_and_labels: &str) -> Option<u64> {
    sample(body, name_and_labels)
}

/// **C4b-2a: the hygiene, error-taxonomy and pool-size families, moved by real traffic.**
///
/// Each assertion is about something a CLIENT did, read back through a real scrape:
/// - a recycle of a clean SQLite connection is `targeted`, and the recycle after a `PRAGMA` (which
///   taints unconditionally, §22.2 (bm)) is `full`;
/// - errors through BOTH terminal paths move `ferro_errors_total`: the SESSION path (`session::error`
///   builds the per-request diagnostics — `ADMIN` has no route, so `Unsupported`; an unknown flag bit,
///   so `Protocol`), and the HANDLER path (the supervisor's terminal for a real EXEC — a syntax error,
///   and a timed-out autocommit WRITE, which §19.3 sends `Indeterminate`). The adversarial review of
///   this slice found the first version exercised the session path TWICE and the handler path never,
///   so a second END builder on the handler path — where every SQL error and every `Indeterminate`
///   lives — passed every test;
/// - the size gauges describe the pool that exists.
#[tokio::test(flavor = "multi_thread")]
async fn real_traffic_moves_the_hygiene_error_and_pool_series() {
    use bytes::Bytes;
    use ferro_proto::consts::service;
    use ferro_proto::header::Header;
    use ferrod::session::codec::OutFrame;

    const TARGETED: &str = "ferro_hygiene_total{pool=\"default\",profile=\"targeted\"}";
    const FULL: &str = "ferro_hygiene_total{pool=\"default\",profile=\"full\"}";
    const SKIPPED: &str = "ferro_hygiene_total{pool=\"default\",profile=\"skipped_clean\"}";
    const UNSUPPORTED: &str = "ferro_errors_total{code=\"Unsupported\",branch=\"NonRetryable\"}";
    const PROTOCOL: &str = "ferro_errors_total{code=\"Protocol\",branch=\"NonRetryable\"}";
    const SYNTAX: &str = "ferro_errors_total{code=\"Syntax\",branch=\"NonRetryable\"}";
    const UNCONFIRMED: &str =
        "ferro_errors_total{code=\"WriteUnconfirmed\",branch=\"Indeterminate\"}";
    const INDETERMINATE: &str = "ferro_indeterminate_total";

    let (server, metrics_addr, _metrics_guard) = exec_server_with_metrics().await;
    let mut c = server.connect().await;
    c.hello(1).await;

    // A statement first, so the pool has a connection to recycle (HELLO's version probe already
    // parked one — every EXEC through the daemon takes the recycled exit, C3-4).
    exec_ok(&mut c, 2, &write("create table t (id integer primary key)")).await;
    let before = scrape(metrics_addr).await;
    for s in [
        TARGETED,
        FULL,
        SKIPPED,
        UNSUPPORTED,
        PROTOCOL,
        SYNTAX,
        UNCONFIRMED,
        INDETERMINATE,
    ] {
        assert!(
            series(&before, s).is_some(),
            "{s} is missing from a scrape:\n{before}"
        );
    }

    // (1) Hygiene: a clean recycle, then a tainting PRAGMA, then the recycle that must be FULL.
    exec_ok(&mut c, 3, &req("select 1")).await;
    exec_ok(&mut c, 4, &write("pragma foreign_keys = on")).await;
    exec_ok(&mut c, 5, &req("select 1")).await;

    // (2) Errors through both paths.
    c.send(OutFrame {
        header: Header {
            flags: 0,
            service: service::ADMIN,
            method: 1,
            request_id: 6,
            payload_len: 0,
        },
        payload: Bytes::new(),
    })
    .await;
    let _ = c.recv().await;
    c.send(OutFrame {
        header: Header {
            flags: 0x8000,
            service: service::SQL,
            method: 1,
            request_id: 7,
            payload_len: 0,
        },
        payload: Bytes::new(),
    })
    .await;
    let _ = c.recv().await;

    // (3) The HANDLER path: a real EXEC's terminal, built by the supervisor.
    let syntax = exec_err(&mut c, 8, &req("selec 1")).await;
    assert_eq!(syntax.code, ferro_proto::consts::errc::SYNTAX, "{syntax:?}");
    // A long autocommit WRITE cut off by its own deadline: SQLite's interrupt classifies as a
    // statement cancel, and a dispatched write whose fate is unknown is §19.3 `Indeterminate`.
    let long_write = ExecRequest {
        timeout_ms: Some(100),
        ..write(
            "insert into t (id) with recursive c(x) as \
             (select 1 union all select x + 1 from c where x < 100000000) select x + 10 from c",
        )
    };
    let unconfirmed = exec_err(&mut c, 9, &long_write).await;
    assert_eq!(
        unconfirmed.branch,
        ferro_proto::consts::branch::INDETERMINATE,
        "a timed-out autocommit write must be sent Indeterminate: {unconfirmed:?}",
    );

    let after = scrape(metrics_addr).await;
    let moved = |s: &str| series(&after, s).unwrap() - series(&before, s).unwrap();

    assert!(
        moved(TARGETED) >= 2,
        "two clean recycles should be targeted:\n{after}"
    );
    assert!(
        moved(FULL) >= 1,
        "the recycle after a PRAGMA must be FULL:\n{after}"
    );
    assert_eq!(
        moved(SKIPPED),
        0,
        "no backend reports a clean profile of None today, so skipped_clean cannot move",
    );
    assert!(
        moved(UNSUPPORTED) >= 1,
        "the dispatch-path error was not counted:\n{after}"
    );
    assert!(
        moved(PROTOCOL) >= 1,
        "the SESSION-path error was not counted — does session::error still build its own frame?\n{after}",
    );
    assert!(
        moved(SYNTAX) >= 1,
        "the HANDLER-path error was not counted — is there a second END builder?\n{after}",
    );
    assert!(
        moved(UNCONFIRMED) >= 1,
        "the Indeterminate write was not counted by code:\n{after}"
    );
    assert!(
        moved(INDETERMINATE) >= 1,
        "ferro_indeterminate_total did not move for an Indeterminate terminal:\n{after}",
    );
    assert_eq!(
        series(&after, "ferro_errors_unregistered_total").unwrap(),
        0,
        "an error terminal carried a code that is not in /proto",
    );

    // (3) Size gauges describe the pool that exists.
    let max = series(&after, "ferro_pool_max_connections{pool=\"default\"}").unwrap();
    let in_use = series(&after, "ferro_pool_in_use_connections{pool=\"default\"}").unwrap();
    let idle = series(&after, "ferro_pool_idle_connections{pool=\"default\"}").unwrap();
    assert!(max > 0, "{after}");
    assert!(
        idle >= 1,
        "a connection should be parked after the traffic:\n{after}"
    );
    assert!(
        in_use + idle <= max,
        "in_use {in_use} + idle {idle} exceeds max {max}"
    );
}
