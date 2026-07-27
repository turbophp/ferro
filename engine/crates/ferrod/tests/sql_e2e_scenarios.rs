//! S5E Task 1 — wire-level end-to-end SCENARIO suite over the now-reviewed S5 EXEC path.
//!
//! Where `sql_exec_it.rs` proves the single-request EXEC shapes (SELECT 1, fetch=none, the §19.3
//! fate matrix), this suite proves the SAME real client→UDS→ferrod→pool→Docker-PG→client path holds
//! under realistic *multi-request* conditions: concurrent multiplexed EXECs on one session, a
//! per-request error that must NOT poison the session, a PING answered while a slow EXEC is still
//! in flight (reader stays responsive), a CANCEL against an in-flight EXEC (the documented M0
//! no-op), and a boot-epoch change observed across a daemon "restart".
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

use common::{TestServer, assert_session_alive, exec_err, exec_ok, exec_server, pg_url, req};
use ferro_proto::consts::{errc, flags, method_core, method_sql, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecOk;
use ferro_proto::value::Value;
use ferrod::epoch::BootEpoch;

/// A query that takes ~200 ms but returns ONE M0-typed row (`int4` `1` → `I64(1)`). `pg_sleep`
/// itself returns `void` (OID 2278), which `rowmap::oid_to_tag` rejects pre-execute as
/// `Unsupported` — so `SELECT pg_sleep(0.2), 1` would break the PING/CANCEL scenarios. Selecting a
/// constant `FROM pg_sleep(..)` keeps the delay while shipping a supported column (verified live).
const SLOW_ROW_SQL: &str = "SELECT 1 FROM pg_sleep(0.2)";

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

    // N=8 < the hardcoded pool max_size (16) and ≪ max_inflight (1024): all 8 checkouts are admitted
    // concurrently, so this proves MULTIPLEXING (not queuing). request_id = 100 + i, sql = SELECT i,
    // so every terminal is self-identifying by BOTH its echoed rid and its row value.
    const N: u32 = 8;

    // Fire all N without awaiting any terminal.
    for i in 1..=N {
        let rid = 100 + i;
        client
            .send_request(
                rid,
                service::SQL,
                method_sql::EXEC,
                req(&format!("SELECT {i}")).encode(),
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
// cancel_in_flight_exec — the TRUE M0 behavior: the EXEC handler binds its cancel token as `_cancel`
// (sql.rs) and NEVER reads it, so a CANCEL is a NO-OP — it neither aborts the query nor yields
// `Outcome::Cancelled`; `co.query` runs to completion and the terminal is `Outcome::Ok`. A
// cancel-aware handler is post-M0.
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
    let rid = 210;
    client
        .send_request(
            rid,
            service::SQL,
            method_sql::EXEC,
            req(SLOW_ROW_SQL).encode(),
        )
        .await;
    client.cancel(rid).await;

    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, rid);
    assert_eq!(
        terminal.header.flags & flags::END,
        flags::END,
        "the (un-cancelled) EXEC terminal still carries exactly one END"
    );
    assert_eq!(terminal.header.service, service::SQL);
    assert_eq!(terminal.header.method, method_sql::EXEC);

    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Ok(body) => {
            let ok = ExecOk::decode(&body).expect("decode ExecOk");
            assert_eq!(
                ok.rows,
                vec![vec![Value::I64(1)]],
                "M0 CANCEL is a no-op: the slow query ran to completion and returned its row"
            );
        }
        Outcome::Cancelled => {
            panic!("M0 CANCEL does not abort EXEC — the handler binds `_cancel` and never reads it")
        }
        other => panic!("expected Outcome::Ok (CANCEL is a no-op in M0), got {other:?}"),
    }

    // Session survives AND exactly one END was produced for `rid` (no stray Cancelled/second frame).
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
