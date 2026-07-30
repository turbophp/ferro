//! S5E Task 1 — wire-level end-to-end SCENARIO suite over the now-reviewed S5 EXEC path.
//!
//! Where `sql_exec_it.rs` proves the single-request EXEC shapes (SELECT 1, fetch=none, the §19.3
//! fate matrix), this suite proves the SAME real client→UDS→ferrod→pool→Docker-PG→client path holds
//! under realistic *multi-request* conditions: concurrent multiplexed EXECs on one session, a
//! per-request error that must NOT poison the session, a PING answered while a slow EXEC is still
//! in flight (reader stays responsive), a CANCEL against an in-flight EXEC (M1-S4: enforced —
//! `Cancelled{NonRetryable}` for a read), and a boot-epoch change observed across a daemon
//! "restart".
//!
//! It reuses the ONE shared harness in `common/mod.rs` (`TestServer`/`TestClient`, plus the lifted
//! `pg_url`/`exec_server`/`req`/`exec_ok`/`exec_err`/`assert_session_alive`) — a genuine round trip
//! with the daemon in the test process, exactly as `sql_exec_it.rs`. Every PG-touching scenario
//! SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset, so `cargo test --workspace` stays green
//! offline; the boot-epoch scenario needs no PG and always runs.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro \
//!   cargo test -p ferrod --test sql_e2e_scenarios -- --nocapture
//! ```
//!
//! Charter invariants asserted in EVERY scenario: exactly one `END` per request id (each terminal
//! carries `flags::END`, and `assert_session_alive`'s PING→PONG proves no stray second frame ever
//! arrives), and — where applicable — session survival after a per-request error/cancel.

mod common;

use std::time::Instant;

use common::{TestServer, assert_session_alive, exec_err, exec_ok, exec_server, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_core, method_sql, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecOk;
use ferro_proto::value::Value;
use ferrod::epoch::BootEpoch;

/// A query that takes ~200 ms but returns ONE M0-typed row (`int4` `1` → `I64(1)`). `pg_sleep`
/// itself returns `void` (OID 2278), which `rowmap::oid_to_tag` rejects pre-execute as
/// `Unsupported` — so `SELECT pg_sleep(0.2), 1` would break the PING/CANCEL scenarios. Selecting a
/// constant `FROM pg_sleep(..)` keeps the delay while shipping a supported column (verified live).
const SLOW_ROW_SQL: &str = "SELECT 1 FROM pg_sleep(0.2)";

/// A LONGER-sleeping sibling of [`SLOW_ROW_SQL`], used ONLY by `cancel_in_flight_exec` — that test
/// needs a real margin BEYOND however long `checkout()` (a first-time `connect()` to Postgres)
/// takes, which was empirically measured at ~150 ms in this dev environment (WSL2's Docker
/// port-forwarding path). `SLOW_ROW_SQL`'s 200 ms is too tight a budget for that; every OTHER
/// scenario in this file keeps using `SLOW_ROW_SQL` (their timing math, e.g.
/// `concurrent_multiplexed_execs`'s serialized-floor check, depends on the 200 ms figure).
const CANCELLABLE_SLEEP_SQL: &str = "SELECT 1 FROM pg_sleep(1.5)";

// -------------------------------------------------------------------------------------------------
// concurrent_multiplexed_execs — one session multiplexes N in-flight EXECs (each handler owns its
// own pooled conn), terminals may return in ANY order, every id gets exactly one END.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_multiplexed_execs() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // N=8 < the hardcoded pool max_size (16) and ≪ max_inflight (1024), so all 8 checkouts are
    // admitted at once. Each EXEC is a ~200 ms `SELECT i FROM pg_sleep(0.2)` (slow AND
    // self-identifying: the projected constant `i` is the int4→I64 row). We fire all N without
    // awaiting, then assert the WALL-CLOCK to collect all N is far below the SERIALIZED floor
    // (N×200 ms = 1600 ms) — the only way that holds is genuine overlap, so this proves MULTIPLEXING,
    // not queuing (not merely demux correctness). request_id = 100 + i self-identifies each terminal.
    const N: u32 = 8;
    const SLEEP_S: f64 = 0.2;

    let start = Instant::now();
    // Fire all N without awaiting any terminal.
    for i in 1..=N {
        let rid = 100 + i;
        client
            .send_request(
                rid,
                service::SQL,
                method_sql::EXEC,
                req(&format!("SELECT {i} FROM pg_sleep({SLEEP_S})")).encode(),
            )
            .await;
    }

    // Collect all N terminals; match on the echoed request_id (arrival order NOT assumed).
    let mut seen = [false; (N + 1) as usize];
    for _ in 0..N {
        let t = client.recv().await;
        assert_eq!(
            t.header.flags & flags::END,
            flags::END,
            "each concurrent EXEC terminal carries exactly one END"
        );
        assert_eq!(t.header.service, service::SQL);
        assert_eq!(t.header.method, method_sql::EXEC);

        let rid = t.header.request_id;
        assert!(
            (101..=100 + N).contains(&rid),
            "terminal rid {rid} is not one of the {N} fired ids"
        );
        let i = rid - 100;
        assert!(
            !seen[i as usize],
            "request id {rid} returned TWICE — a request-bearing frame must produce exactly one END"
        );
        seen[i as usize] = true;

        let ok = match Outcome::decode(&t.payload).expect("decode Outcome") {
            Outcome::Ok(body) => ExecOk::decode(&body).expect("decode ExecOk"),
            other => panic!("concurrent EXEC {rid} expected Outcome::Ok, got {other:?}"),
        };
        assert_eq!(
            ok.rows,
            vec![vec![Value::I64(i as i64)]],
            "SELECT {i} must echo its own value back on rid {rid} (self-identifying, order-free)"
        );
    }
    for i in 1..=N {
        assert!(seen[i as usize], "no terminal ever arrived for SELECT {i}");
    }

    // THE concurrency proof: all N ~200 ms queries completed in far less than the serialized floor
    // (N × 200 ms = 1600 ms). A server that ran them one-at-a-time could not finish under 1600 ms;
    // observing < 1000 ms is only possible if the handlers overlapped — i.e. real multiplexing, not
    // queuing. (Generous margin: concurrent actual ≈ 200–500 ms; serial ≥ 1600 ms; RECV_TIMEOUT 2 s.)
    let elapsed = start.elapsed();
    let serial_floor = std::time::Duration::from_secs_f64(N as f64 * SLEEP_S);
    assert!(
        elapsed < std::time::Duration::from_millis(1000),
        "collected {N} concurrent {SLEEP_S}s EXECs in {elapsed:?}; serialized floor is {serial_floor:?} \
         — this exceeds the concurrency budget, suggesting the session serialized (queued) them"
    );

    // Exactly N terminals, each a unique id with one END, and nothing else: the session is alive and
    // never produced a stray extra frame for any of the 8 ids.
    assert_session_alive(&mut client, 0xC0FFEE).await;
}

// -------------------------------------------------------------------------------------------------
// error_then_recover — a per-request error is STATEMENT-level, never session-level: a syntax error
// terminal, then a valid SELECT 1 on the SAME session, then PING→PONG.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn error_then_recover() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // 1. A syntax error → Outcome::Error{Syntax}, one END. Known-fate, NOT Indeterminate.
    let ep = exec_err(&mut client, 30, &req("SELCT 1")).await;
    assert_eq!(ep.code, errc::SYNTAX, "a syntax error maps to Syntax");

    // 2. The SAME session still executes a valid statement (the error did not poison it).
    let ok = exec_ok(&mut client, 31, &req("SELECT 1")).await;
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);

    // 3. And liveness still answers.
    assert_session_alive(&mut client, 3).await;
}

// -------------------------------------------------------------------------------------------------
// ping_during_in_flight_exec — the reader loop stays responsive while a handler runs: a PING sent
// mid-EXEC gets its PONG PROMPTLY (before the slow EXEC's ~200 ms terminal), then the EXEC terminal.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ping_during_in_flight_exec() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Start a slow EXEC (~200 ms) but do NOT await its terminal.
    client
        .send_request(
            500,
            service::SQL,
            method_sql::EXEC,
            req(SLOW_ROW_SQL).encode(),
        )
        .await;

    // Immediately PING on the same session (a distinct rid, NOT 9 which assert_session_alive uses).
    client.ping(501, 0xABCD).await;

    // The PONG must come back FIRST: the reader loop answers liveness concurrently with the in-flight
    // handler; the slow query is still running, so its terminal cannot have overtaken the PONG.
    let pong = client.recv().await;
    assert_eq!(
        pong.header.service,
        service::CORE,
        "reader answered PING while EXEC in flight"
    );
    assert_eq!(pong.header.method, method_core::PONG);
    assert_eq!(pong.header.request_id, 501);

    // THEN the slow EXEC's single terminal arrives.
    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 500);
    assert_eq!(
        terminal.header.flags & flags::END,
        flags::END,
        "the slow EXEC terminal carries exactly one END"
    );
    assert_eq!(terminal.header.service, service::SQL);
    assert_eq!(terminal.header.method, method_sql::EXEC);
    let ok = match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Ok(body) => ExecOk::decode(&body).expect("decode ExecOk"),
        other => panic!("slow EXEC expected Outcome::Ok, got {other:?}"),
    };
    assert_eq!(
        ok.rows,
        vec![vec![Value::I64(1)]],
        "SELECT 1 FROM pg_sleep(0.2) → one I64(1) row"
    );

    // Exactly one END for rid 500 (nothing stray), and the session is still alive.
    assert_session_alive(&mut client, 4).await;
}

// -------------------------------------------------------------------------------------------------
// cancel_in_flight_exec — M1-S4: the autocommit EXEC path now ENFORCES the per-request `CANCEL`
// flag via a biased `tokio::select!` (`sql.rs`'s `run_autocommit_exec`): the query is polled first
// (so `sent` is honest), then a routed CANCEL fires the out-of-band cancel and drains the query to
// its erroring (`57014`) completion. This request is a READ (`req()`'s default `readonly: true`),
// so §19.3 routes it to `Cancelled{NonRetryable}` — never `Indeterminate` (there is no
// `Cancelled/Retryable` wire pairing) and never the old M0 `Outcome::Ok` no-op.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cancel_in_flight_exec() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Interleave explicitly (NOT the atomic `exec()` helper): fire the slow EXEC, then CANCEL its id
    // WHILE it is in flight, then read the single terminal. The reader processes the two frames in
    // FIFO order, so the request is registered before the CANCEL reaches `registry.cancel(rid)`.
    //
    // The delay before the CANCEL is load-bearing, NOT cosmetic: Postgres CLEARS a pending cancel
    // signal that arrives before the backend has actually started reading/executing the targeted
    // command (the well-known "a cancel sent between statements has no effect" behavior —
    // `QueryCancelPending` is reset at the top of the backend's next-command loop). This request's
    // EXEC is the FIRST on a brand-new pool, so the handler's `checkout()` pays a real `connect()` —
    // empirically ~150 ms in this dev environment (WSL2's Docker port-forwarding path) — entirely
    // BEFORE the query is ever dispatched; a CANCEL sent any earlier races that connect+dispatch and
    // is silently discarded by Postgres rather than interrupting the (not-yet-started) statement.
    // 600 ms is a 4x margin over that measured cost, and `CANCELLABLE_SLEEP_SQL`'s 1.5 s leaves ~900
    // ms of remaining budget for the cancel to land — comfortably inside `RECV_TIMEOUT` (2 s).
    let rid = 210;
    client
        .send_request(
            rid,
            service::SQL,
            method_sql::EXEC,
            req(CANCELLABLE_SLEEP_SQL).encode(),
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    client.cancel(rid).await;

    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, rid);
    assert_eq!(
        terminal.header.flags & flags::END,
        flags::END,
        "the cancelled EXEC terminal still carries exactly one END"
    );
    assert_eq!(terminal.header.service, service::SQL);
    assert_eq!(terminal.header.method, method_sql::EXEC);

    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(
                ep.code,
                errc::CANCELLED,
                "a cancelled READ is Cancelled, never Indeterminate/a bare Retryable"
            );
            assert_eq!(
                ep.branch,
                branch::NON_RETRYABLE,
                "there is no Cancelled/Retryable wire pairing — a read cancel rides NonRetryable"
            );
        }
        Outcome::Ok(_) => panic!(
            "CANCEL is enforced as of M1-S4: the query must have been interrupted, not run to \
             completion (if this flakes, the cancel lost a legitimate race — investigate timing, \
             don't just widen the assertion to accept Ok)"
        ),
        other => panic!("expected Outcome::Error{{Cancelled}}, got {other:?}"),
    }

    // Session survives AND exactly one END was produced for `rid` (no stray Ok/second frame).
    assert_session_alive(&mut client, 5).await;
}

// -------------------------------------------------------------------------------------------------
// reconnect_across_boot_epoch_change (needs NO PG) — the wire-level §19.1 epoch-change signal the
// client resilience loop (S7) is built on: a restarted daemon issues a FRESH boot_epoch, so a
// reconnect observes a changed value and voids all engine-side state.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_across_boot_epoch_change() {
    // Server A, boot_epoch 1.
    let server_a = TestServer::spawn(BootEpoch(1));
    let mut client_a = server_a.connect().await;
    let epoch_a = client_a.hello(1).await.ack.boot_epoch;
    assert_eq!(epoch_a, 1, "server A advertises its boot_epoch");

    // "Restart": drop A, bring up server B with a DIFFERENT boot_epoch.
    drop(client_a);
    drop(server_a);

    let server_b = TestServer::spawn(BootEpoch(2));
    let mut client_b = server_b.connect().await;
    let epoch_b = client_b.hello(1).await.ack.boot_epoch;
    assert_eq!(epoch_b, 2, "server B advertises its own boot_epoch");

    // The reconnect observes a CHANGED epoch — the signal §19.1 keys the resilience loop off.
    assert_ne!(
        epoch_b, epoch_a,
        "a restarted daemon issues a fresh boot_epoch (client voids engine-side state on the change)"
    );
}
