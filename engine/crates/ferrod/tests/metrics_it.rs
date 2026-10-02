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
use ferro_pool::histogram::{CHECKOUT_BOUNDS_US, PIN_BOUNDS_US};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::{ExecOk, ExecRequest};

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

/// Every `{name}_bucket{pool="default",le=..}` line, in order, as `(le, count)`.
fn buckets(body: &str, name: &str) -> Vec<(String, u64)> {
    let prefix = format!("{name}_bucket{{pool=\"default\",le=\"");
    body.lines()
        .filter_map(|l| l.strip_prefix(prefix.as_str()))
        .map(|rest| {
            let (le, tail) = rest.split_once("\"}").expect("le label closes");
            (
                le.to_string(),
                tail.trim().parse().expect("bucket count parses"),
            )
        })
        .collect()
}

/// A histogram family is well-formed: its `le` labels are exactly `bounds` rendered in SECONDS then
/// `+Inf`, its buckets are cumulative, and the `+Inf` count equals `_count`.
///
/// The `le` check is what catches a unit error: a render printing raw microseconds still produced
/// cumulative buckets ending in `+Inf` and passed the first version of this helper (review F2).
fn assert_histogram_shape(body: &str, name: &str, bounds: &[u64]) -> u64 {
    let b = buckets(body, name);
    assert!(!b.is_empty(), "{name} has no buckets:\n{body}");
    let expected_le: Vec<String> = bounds
        .iter()
        .map(|&us| ferro_pool::histogram::fmt_seconds(us))
        .chain(std::iter::once("+Inf".to_string()))
        .collect();
    let got_le: Vec<String> = b.iter().map(|(le, _)| le.clone()).collect();
    assert_eq!(
        got_le, expected_le,
        "{name}'s le labels are not its bounds in seconds"
    );
    assert_eq!(
        b.last().map(|(le, _)| le.as_str()),
        Some("+Inf"),
        "{name} must end in +Inf"
    );
    assert!(
        b.windows(2).all(|w| w[0].1 <= w[1].1),
        "{name} buckets are not cumulative: {b:?}",
    );
    let count = series(body, &format!("{name}_count{{pool=\"default\"}}")).expect("_count");
    assert_eq!(
        b.last().unwrap().1,
        count,
        "{name}: +Inf bucket must equal _count"
    );
    assert!(
        body.contains(&format!("{name}_sum{{pool=\"default\"}} ")),
        "{name} has no _sum"
    );
    count
}

/// **C4b-2b: the pinned gauge, the two histograms and queue depth, through a real daemon.**
///
/// Pins come from the TX service — a real BEGIN pins the connection for the life of the
/// transaction — and the checkout histogram from ordinary EXECs. Queue depth is asserted only at
/// rest here (0, and present): a deterministic wait needs the pool owned outright, which
/// `ferro-pool`'s `pin_queue_metrics.rs` does for every way a wait can end.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_transaction_moves_the_pin_and_checkout_series() {
    use ferro_proto::consts::{method_tx, service};
    use ferro_proto::messages::Outcome;
    use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};

    const PINNED: &str = "ferro_pool_pinned_connections{pool=\"default\"}";
    const WAITING: &str = "ferro_pool_waiting_checkouts{pool=\"default\"}";
    const PIN: &str = "ferro_pin_duration_seconds";
    const CHECKOUT: &str = "ferro_checkout_duration_seconds";

    let (server, metrics_addr, _metrics_guard) = exec_server_with_metrics().await;
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, &write("create table t (id integer primary key)")).await;

    let before = scrape(metrics_addr).await;
    let pins_before = assert_histogram_shape(&before, PIN, &PIN_BOUNDS_US);
    let checkouts_before = assert_histogram_shape(&before, CHECKOUT, &CHECKOUT_BOUNDS_US);
    assert_eq!(series(&before, PINNED), Some(0), "{before}");
    assert_eq!(series(&before, WAITING), Some(0), "{before}");

    // A real transaction: BEGIN pins a connection until COMMIT.
    c.send_request(
        3,
        service::TX,
        method_tx::BEGIN,
        BeginRequest {
            pool: "default".to_string(),
            isolation: None,
            readonly: false,
        }
        .encode(),
    )
    .await;
    let tx_id = match Outcome::decode(&c.recv().await.payload).expect("BEGIN outcome") {
        Outcome::Ok(body) => BeginResponse::decode(&body).expect("BeginResponse").tx_id,
        other => panic!("BEGIN failed: {other:?}"),
    };
    let during = scrape(metrics_addr).await;
    assert_eq!(
        series(&during, PINNED),
        Some(1),
        "an open transaction pins one connection:\n{during}"
    );

    c.send_request(
        4,
        service::TX,
        method_tx::COMMIT,
        TxControl { tx_id }.encode(),
    )
    .await;
    assert!(
        matches!(Outcome::decode(&c.recv().await.payload), Ok(Outcome::Ok(_))),
        "COMMIT failed"
    );
    exec_ok(&mut c, 5, &req("select 1")).await;

    let after = scrape(metrics_addr).await;
    assert_eq!(
        series(&after, PINNED),
        Some(0),
        "COMMIT must release the pin:\n{after}"
    );
    let pins_after = assert_histogram_shape(&after, PIN, &PIN_BOUNDS_US);
    let checkouts_after = assert_histogram_shape(&after, CHECKOUT, &CHECKOUT_BOUNDS_US);
    assert_eq!(
        pins_after - pins_before,
        1,
        "one transaction is one pin duration"
    );
    assert!(
        checkouts_after > checkouts_before,
        "the BEGIN and the EXEC are checkouts the histogram must see"
    );
    assert_eq!(series(&after, WAITING), Some(0));

    // The OTHER way a pin ends in production: the client goes away mid-transaction. The session's
    // teardown aborts it — the tx actor runs `rollback_tx()` under `tx_teardown_timeout`, so the pin
    // ends at the ROLLBACK site, not in `Drop`. (A first version of this comment claimed `Drop`;
    // deleting the `Drop` release left this test green, which is how that was found. `Drop` is the
    // backstop for a teardown ROLLBACK that fails or times out — `ferro-pool`'s
    // `a_pin_dropped_without_commit_or_rollback_still_ends_once` proves that path exactly.)
    let mut gone = server.connect().await;
    gone.hello(10).await;
    gone.send_request(
        11,
        service::TX,
        method_tx::BEGIN,
        BeginRequest {
            pool: "default".to_string(),
            isolation: None,
            readonly: false,
        }
        .encode(),
    )
    .await;
    let _ = gone.recv().await;
    assert_eq!(series(&scrape(metrics_addr).await, PINNED), Some(1));
    drop(gone);
    // Polled on the gauge, and the histogram is then asserted from the SAME scrape: the pin is
    // observed BEFORE the gauge falls (`PinSlot::release`, Release/Acquire), so a scrape that
    // reads the gauge at 0 must already see the duration. The first version decremented first,
    // and a 30 ms preemption between the two lines failed this exact assertion (review F5).
    let mut released = None;
    for _ in 0..200 {
        let body = scrape(metrics_addr).await;
        if series(&body, PINNED) == Some(0) {
            released = Some(body);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let released = released.expect("a disconnected client's transaction must release its pin");
    assert_eq!(
        assert_histogram_shape(&released, PIN, &PIN_BOUNDS_US) - pins_after,
        1,
        "the abandoned transaction's pin must be observed exactly once",
    );
}

/// Send one EXEC payload as raw bytes and return its terminal, skipping any HEAD/DATA frames a
/// streamed request emits first. Raw because the reproduction below needs a field-9 byte sequence
/// `ExecRequest::encode` cannot produce (it takes a `String`).
async fn exec_raw(c: &mut common::TestClient, rid: u32, payload: Vec<u8>) -> Outcome {
    use ferro_proto::consts::{flags, method_sql, service};
    c.send_request(rid, service::SQL, method_sql::EXEC, payload)
        .await;
    loop {
        let f = c.recv().await;
        assert_eq!(f.header.request_id, rid, "a frame for another request");
        if f.header.flags & flags::END == flags::END {
            return Outcome::decode(&f.payload).expect("decode the terminal");
        }
    }
}

/// **M2-C4c-1: a malformed W3C `traceparent` is dropped and COUNTED on EVERY EXEC path — it never
/// fails a statement.**
///
/// An observability field must never be why a statement fails, and a provider emitting junk must
/// still be visible to an operator. The claim covers every path an EXEC can take, so this sends one
/// down each: autocommit buffered, autocommit streamed, tx-scoped, and the three request shapes the
/// handler refuses for reasons of their own (an unknown pool, a `query_id`, an unknown `tx_id`) —
/// refused for THOSE reasons, and still counted, because the header is interpreted before any shape
/// check. The C4c-1 review found every earlier test used the autocommit-buffered path alone, so
/// counting there and nowhere else survived.
///
/// The header that is not even valid UTF-8 is the review's MAJOR finding: the codec refused the
/// whole request as `Protocol`, failing the statement over its trace context. A provider that
/// forwards an inbound HTTP header verbatim hands an external caller that byte.
///
/// Only this test in this binary sends a `traceparent`, so the counter's deltas are exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_traceparent_is_counted_on_every_exec_path_and_never_fails_one() {
    use ferro_proto::consts::{flags, method_tx, service};
    use ferro_proto::messages::{BeginRequest, BeginResponse, TxControl};

    const INVALID: &str = "ferro_traceparent_invalid_total";
    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let (server, metrics_addr, _metrics_guard) = exec_server_with_metrics().await;
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(
        &mut c,
        2,
        &write("create table tp (id integer primary key)"),
    )
    .await;
    exec_ok(
        &mut c,
        3,
        &write("insert into tp (id) values (1), (2), (3)"),
    )
    .await;

    let counted =
        |body: &str| series(body, INVALID).unwrap_or_else(|| panic!("{INVALID} missing:\n{body}"));
    let base = counted(&scrape(metrics_addr).await);

    let with = |tp: &str, r: ExecRequest| ExecRequest {
        traceparent: Some(tp.to_string()),
        ..r
    };
    // A field-9 string whose LAST byte is not UTF-8: encode a placeholder, then patch it.
    let non_utf8 = |r: ExecRequest| {
        let mut p = with("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0Z", r).encode();
        assert_eq!(p.last(), Some(&b'Z'), "traceparent is the last field");
        *p.last_mut().unwrap() = 0xff;
        p
    };
    let ok_rows = |o: Outcome| match o {
        Outcome::Ok(body) => ExecOk::decode(&body).expect("decode ExecOk"),
        other => panic!("a traceparent failed its statement: {other:?}"),
    };
    let mut bad = 0u64;

    // (0) A VALID header is not counted.
    assert_eq!(
        exec_ok(&mut c, 10, &with(VALID, req("select 1")))
            .await
            .rows
            .len(),
        1
    );
    assert_eq!(
        counted(&scrape(metrics_addr).await),
        base,
        "a VALID traceparent was counted"
    );

    // (1) Autocommit, buffered.
    for (i, tp) in [
        "garbage",
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
    ]
    .iter()
    .enumerate()
    {
        let ok = exec_ok(&mut c, 20 + i as u32, &with(tp, req("select 1"))).await;
        assert_eq!(ok.rows.len(), 1, "{tp}");
        bad += 1;
    }
    let ok = ok_rows(exec_raw(&mut c, 30, non_utf8(req("select 1"))).await);
    assert_eq!(
        ok.rows.len(),
        1,
        "a non-UTF-8 traceparent failed an autocommit statement"
    );
    bad += 1;

    // (2) Autocommit, streamed: the terminal of a stream reports the rows it streamed.
    let streamed = ExecRequest {
        fetch: 2,
        ..req("select id from tp order by id")
    };
    let end = ok_rows(exec_raw(&mut c, 40, non_utf8(streamed.clone())).await);
    assert_eq!(
        end.stats.rows, 3,
        "a non-UTF-8 traceparent failed a streamed statement"
    );
    bad += 1;
    let end = ok_rows(exec_raw(&mut c, 41, with("garbage", streamed).encode()).await);
    assert_eq!(end.stats.rows, 3);
    bad += 1;

    // (3) Tx-scoped.
    c.send_request(
        50,
        service::TX,
        method_tx::BEGIN,
        BeginRequest {
            pool: "default".into(),
            isolation: None,
            readonly: false,
        }
        .encode(),
    )
    .await;
    let t = c.recv().await;
    let tx_id = match Outcome::decode(&t.payload).expect("BEGIN terminal") {
        Outcome::Ok(body) => BeginResponse::decode(&body).expect("BeginResponse").tx_id,
        other => panic!("BEGIN failed: {other:?}"),
    };
    let in_tx = |sql: &str| ExecRequest {
        tx_id: Some(tx_id),
        ..write(sql)
    };
    let ok = ok_rows(
        exec_raw(
            &mut c,
            51,
            non_utf8(in_tx("insert into tp (id) values (4)")),
        )
        .await,
    );
    assert_eq!(
        ok.affected, 1,
        "a non-UTF-8 traceparent failed a tx-scoped statement"
    );
    bad += 1;
    let ok = exec_ok(
        &mut c,
        52,
        &with("garbage", in_tx("select count(*) from tp")),
    )
    .await;
    assert_eq!(ok.rows.len(), 1);
    bad += 1;
    c.send_request(
        53,
        service::TX,
        method_tx::COMMIT,
        TxControl { tx_id }.encode(),
    )
    .await;
    let t = c.recv().await;
    assert_eq!(t.header.flags & flags::END, flags::END);
    assert!(
        matches!(Outcome::decode(&t.payload), Ok(Outcome::Ok(_))),
        "COMMIT failed"
    );

    // (4) Requests the handler refuses for reasons of their OWN — still counted, because the
    // header is interpreted before any shape check. Each must be refused for that reason, not
    // for its trace context.
    let refused = |o: Outcome, why: &str| match o {
        Outcome::Error(e) => assert!(
            e.message.contains(why),
            "refused for the wrong reason (wanted {why:?}): {e:?}"
        ),
        other => panic!("expected a refusal ({why}), got {other:?}"),
    };
    let unknown_pool = ExecRequest {
        pool: "no-such-pool".into(),
        ..req("select 1")
    };
    refused(
        exec_raw(&mut c, 60, non_utf8(unknown_pool)).await,
        "unknown pool",
    );
    bad += 1;
    let manifest = ExecRequest {
        sql: None,
        query_id: Some("q1".into()),
        ..req("select 1")
    };
    refused(
        exec_raw(&mut c, 61, with("garbage", manifest).encode()).await,
        "query_id",
    );
    bad += 1;
    let dead_tx = ExecRequest {
        tx_id: Some(987_654_321),
        ..req("select 1")
    };
    match exec_raw(&mut c, 62, with("garbage", dead_tx).encode()).await {
        Outcome::Error(e) => assert_eq!(e.code, ferro_proto::consts::errc::TX_NOT_FOUND, "{e:?}"),
        other => panic!("an unknown tx_id must be refused: {other:?}"),
    }
    bad += 1;

    let after = counted(&scrape(metrics_addr).await);
    assert_eq!(
        after - base,
        bad,
        "every malformed traceparent must be counted exactly once, on every path",
    );
}
