//! S6 Task 4 — the LIVE TX service end to end: a real client → `ferrod` session → the per-`tx_id`
//! actor (a pinned `ferro-pool` `Checkout`) → live Dockerized Postgres → single terminal `END` →
//! client. Proves the transaction contract on a real backend: commit/rollback persistence,
//! savepoints, isolation, readonly rejection, connection PINNING, the two deadlines (idle + the
//! out-of-band mid-statement cancel), and cross-session / unknown-id rejection.
//!
//! Every DB-touching test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — same discipline
//! as `sql_exec_it.rs` — so `cargo test --workspace` stays green offline.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro \
//!   cargo test -p ferrod --test tx_it -- --nocapture
//! ```
//!
//! Every test asserts the terminal is a single `flags::END` frame with the echoed service/method,
//! and (where the session outlives the request) that the session is still alive afterwards
//! (PING→PONG) — i.e. exactly one END was produced (charter rule 4) and nothing else. No statement
//! is ever re-run on a deadline/loss (charter rule 3): a mid-statement `max_tx` yields a single
//! `TxDeadline{Retryable}` (NOT `Indeterminate` — a rolled-back in-tx statement persisted nothing).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClient, TestServer, assert_session_alive, exec, exec_err, exec_ok, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_tx, service, tag};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{
    BeginRequest, BeginResponse, Isolation, SavepointRequest, TxControl,
};
use ferro_proto::value::Value;
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;

// -------------------------------------------------------------------------------------------------
// Server builders + TX client helpers (mirroring `common::{exec_server, req, exec}`).
// -------------------------------------------------------------------------------------------------

/// Like `common::exec_server`, but with caller-supplied transaction deadlines (`idle_in_tx`,
/// `max_tx`) so the deadline tests can drive SHORT timers. Built exactly as `main` builds it (the
/// real `sql::make_handler` + a shared `Arc<TxRegistry>`) so it is a genuine client→ferrod→actor→PG
/// round trip.
fn exec_server_with_deadlines(url: String, idle_in_tx: Duration, max_tx: Duration) -> TestServer {
    let kind = ferrod::config::infer_pool_kind(&url);
    let config = Config {
        pools: vec![PoolSpec {
            name: "default".to_string(),
            dsn: url,
            kind,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }],
        idle_in_tx,
        max_tx,
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry,
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    TestServer::spawn_with_factory(BootEpoch(1), tx_registry, factory)
}

/// `service=TX, method=BEGIN` — assert the one-END terminal shape and decode the `BeginResponse`,
/// returning the allocated `tx_id`.
async fn begin(
    client: &mut TestClient,
    rid: u32,
    pool: &str,
    isolation: Option<u8>,
    readonly: bool,
) -> u64 {
    let breq = BeginRequest {
        pool: pool.to_string(),
        isolation,
        readonly,
    };
    client
        .send_request(rid, service::TX, method_tx::BEGIN, breq.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "BEGIN terminal echoes the rid");
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "BEGIN terminal carries exactly one END"
    );
    assert_eq!(t.header.service, service::TX);
    assert_eq!(t.header.method, method_tx::BEGIN);
    match Outcome::decode(&t.payload).expect("decode BEGIN Outcome") {
        Outcome::Ok(body) => {
            BeginResponse::decode(&body)
                .expect("decode BeginResponse")
                .tx_id
        }
        other => panic!("BEGIN expected Outcome::Ok(BeginResponse), got {other:?}"),
    }
}

/// A tx-scoped `ExecRequest` (`tx_id: Some(..)`); `pool` is ignored by the handler (the tx is
/// already pinned to its conn), but set for realism.
fn tx_req(tx_id: u64, sql: &str, params: Vec<Value>, fetch: u8, readonly: bool) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params,
        timeout_ms: None,
        readonly,
        fetch,
        tx_id: Some(tx_id),
    }
}

/// A tx-scoped `EXEC` (rides `service=SQL, method=EXEC` with `tx_id` set). Reuses `common::exec`,
/// which asserts the one-END shape + SQL/EXEC echoes, and returns the decoded `Outcome`.
async fn exec_in_tx(
    client: &mut TestClient,
    rid: u32,
    tx_id: u64,
    sql: &str,
    params: Vec<Value>,
    fetch: u8,
    readonly: bool,
) -> Outcome {
    exec(client, rid, &tx_req(tx_id, sql, params, fetch, readonly)).await
}

/// A `service=TX` control frame (`COMMIT`/`ROLLBACK`) carrying a `TxControl{tx_id}`. Asserts the
/// one-END shape + TX/method echoes and returns the decoded `Outcome`.
async fn tx_control(client: &mut TestClient, rid: u32, tx_id: u64, method: u16) -> Outcome {
    client
        .send_request(rid, service::TX, method, TxControl { tx_id }.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(
        t.header.request_id, rid,
        "tx-control terminal echoes the rid"
    );
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "tx-control terminal carries exactly one END"
    );
    assert_eq!(t.header.service, service::TX);
    assert_eq!(t.header.method, method);
    Outcome::decode(&t.payload).expect("decode tx-control Outcome")
}

async fn commit(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    tx_control(client, rid, tx_id, method_tx::COMMIT).await
}

async fn rollback(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    tx_control(client, rid, tx_id, method_tx::ROLLBACK).await
}

/// A `service=TX` savepoint control frame (`SAVEPOINT`/`RELEASE`/`ROLLBACK_TO`) carrying a
/// `SavepointRequest{tx_id, name}`. Asserts the one-END shape + TX/method echoes.
async fn savepoint_ctl(
    client: &mut TestClient,
    rid: u32,
    tx_id: u64,
    name: Option<&str>,
    method: u16,
) -> Outcome {
    let sreq = SavepointRequest {
        tx_id,
        name: name.map(str::to_string),
    };
    client
        .send_request(rid, service::TX, method, sreq.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(
        t.header.request_id, rid,
        "savepoint terminal echoes the rid"
    );
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "savepoint terminal carries exactly one END"
    );
    assert_eq!(t.header.service, service::TX);
    assert_eq!(t.header.method, method);
    Outcome::decode(&t.payload).expect("decode savepoint Outcome")
}

async fn savepoint(client: &mut TestClient, rid: u32, tx_id: u64, name: Option<&str>) -> Outcome {
    savepoint_ctl(client, rid, tx_id, name, method_tx::SAVEPOINT).await
}

#[allow(dead_code)]
async fn release(client: &mut TestClient, rid: u32, tx_id: u64, name: Option<&str>) -> Outcome {
    savepoint_ctl(client, rid, tx_id, name, method_tx::RELEASE).await
}

async fn rollback_to(client: &mut TestClient, rid: u32, tx_id: u64, name: Option<&str>) -> Outcome {
    savepoint_ctl(client, rid, tx_id, name, method_tx::ROLLBACK_TO).await
}

/// An autocommit write EXEC (`readonly=false`, `fetch=none`) against the default pool — for the
/// test-fixture DDL / cross-conn setup that must not ride a transaction.
async fn autocommit_write(client: &mut TestClient, rid: u32, sql: &str) {
    let mut w = req(sql);
    w.readonly = false;
    w.fetch = 1;
    match exec(client, rid, &w).await {
        Outcome::Ok(_) => {}
        other => panic!("autocommit write {sql:?} failed: {other:?}"),
    }
}

/// The `I64` in the first cell of the first row (e.g. a `pg_backend_pid()` / `count(*)` scalar).
fn first_i64(ok: &ferro_proto::messages::sql::ExecOk) -> i64 {
    match ok.rows.first().and_then(|r| r.first()) {
        Some(Value::I64(v)) => *v,
        other => panic!("expected an I64 scalar in row 0 col 0, got {other:?}"),
    }
}

// -------------------------------------------------------------------------------------------------
// Commit / rollback persistence (a FRESH autocommit SELECT on a DIFFERENT pooled conn).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_commit_persists() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    autocommit_write(&mut client, 10, "DROP TABLE IF EXISTS ferro_s6_commit").await;
    autocommit_write(&mut client, 11, "CREATE TABLE ferro_s6_commit (id bigint)").await;

    let tx_id = begin(&mut client, 12, "default", None, false).await;
    // INSERT inside the tx.
    match exec_in_tx(
        &mut client,
        13,
        tx_id,
        "INSERT INTO ferro_s6_commit (id) VALUES (?)",
        vec![Value::I64(42)],
        1,
        false,
    )
    .await
    {
        Outcome::Ok(_) => {}
        other => panic!("in-tx INSERT failed: {other:?}"),
    }
    assert!(matches!(
        commit(&mut client, 14, tx_id).await,
        Outcome::Ok(_)
    ));

    // A fresh autocommit SELECT (a DIFFERENT pooled conn) sees the committed row.
    let ok = exec_ok(&mut client, 15, &req("SELECT id FROM ferro_s6_commit")).await;
    assert_eq!(
        ok.rows,
        vec![vec![Value::I64(42)]],
        "COMMIT persisted the row"
    );
    assert_session_alive(&mut client, 100).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tx_rollback_discards() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    autocommit_write(&mut client, 10, "DROP TABLE IF EXISTS ferro_s6_rollback").await;
    autocommit_write(
        &mut client,
        11,
        "CREATE TABLE ferro_s6_rollback (id bigint)",
    )
    .await;

    let tx_id = begin(&mut client, 12, "default", None, false).await;
    match exec_in_tx(
        &mut client,
        13,
        tx_id,
        "INSERT INTO ferro_s6_rollback (id) VALUES (?)",
        vec![Value::I64(7)],
        1,
        false,
    )
    .await
    {
        Outcome::Ok(_) => {}
        other => panic!("in-tx INSERT failed: {other:?}"),
    }
    assert!(matches!(
        rollback(&mut client, 14, tx_id).await,
        Outcome::Ok(_)
    ));

    // A fresh autocommit SELECT does NOT see the rolled-back row.
    let ok = exec_ok(&mut client, 15, &req("SELECT id FROM ferro_s6_rollback")).await;
    assert!(
        ok.rows.is_empty(),
        "ROLLBACK discarded the row: {:?}",
        ok.rows
    );
    assert_session_alive(&mut client, 101).await;
}

// -------------------------------------------------------------------------------------------------
// Savepoint round-trip: INSERT a; SAVEPOINT; INSERT b; ROLLBACK_TO; COMMIT → a persists, b does not.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_savepoint_roundtrip() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    autocommit_write(&mut client, 10, "DROP TABLE IF EXISTS ferro_s6_sp").await;
    autocommit_write(&mut client, 11, "CREATE TABLE ferro_s6_sp (id bigint)").await;

    let tx_id = begin(&mut client, 12, "default", None, false).await;
    // a (id=1) before the savepoint.
    assert!(matches!(
        exec_in_tx(
            &mut client,
            13,
            tx_id,
            "INSERT INTO ferro_s6_sp (id) VALUES (1)",
            vec![],
            1,
            false,
        )
        .await,
        Outcome::Ok(_)
    ));
    assert!(matches!(
        savepoint(&mut client, 14, tx_id, Some("sp")).await,
        Outcome::Ok(_)
    ));
    // b (id=2) after the savepoint.
    assert!(matches!(
        exec_in_tx(
            &mut client,
            15,
            tx_id,
            "INSERT INTO ferro_s6_sp (id) VALUES (2)",
            vec![],
            1,
            false,
        )
        .await,
        Outcome::Ok(_)
    ));
    // ROLLBACK TO the savepoint discards b but keeps a.
    assert!(matches!(
        rollback_to(&mut client, 16, tx_id, Some("sp")).await,
        Outcome::Ok(_)
    ));
    assert!(matches!(
        commit(&mut client, 17, tx_id).await,
        Outcome::Ok(_)
    ));

    let ok = exec_ok(
        &mut client,
        18,
        &req("SELECT id FROM ferro_s6_sp ORDER BY id"),
    )
    .await;
    assert_eq!(
        ok.rows,
        vec![vec![Value::I64(1)]],
        "ROLLBACK_TO kept a (id=1), discarded b (id=2): {:?}",
        ok.rows
    );
    assert_session_alive(&mut client, 102).await;
}

// -------------------------------------------------------------------------------------------------
// Isolation observed: BEGIN RepeatableRead → current_setting('transaction_isolation') inside the tx.
// current_setting(...) (NOT SHOW) so it prepares through the guarded Checkout::query.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_isolation_observed() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let tx_id = begin(
        &mut client,
        12,
        "default",
        Some(u8::from(Isolation::RepeatableRead)),
        false,
    )
    .await;
    let iso = match exec_in_tx(
        &mut client,
        13,
        tx_id,
        "SELECT current_setting('transaction_isolation')",
        vec![],
        0,
        true,
    )
    .await
    {
        Outcome::Ok(body) => ferro_proto::messages::sql::ExecOk::decode(&body).expect("ExecOk"),
        other => panic!("isolation SELECT failed: {other:?}"),
    };
    assert_eq!(
        iso.rows,
        vec![vec![Value::Text("repeatable read".to_string())]],
        "the composed isolation level is observed inside the tx: {:?}",
        iso.rows
    );
    assert!(matches!(
        commit(&mut client, 14, tx_id).await,
        Outcome::Ok(_)
    ));
    assert_session_alive(&mut client, 103).await;
}

// -------------------------------------------------------------------------------------------------
// Readonly tx rejects a write: reads work; a write → 25006 (mapped NonRetryable, raw SQLSTATE on the
// wire), one END, session survives; PG then aborts the tx block, so ROLLBACK is the recovery and the
// pinned conn is released clean (a fresh autocommit SELECT works).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_readonly_rejects_write() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // The target must EXIST so the read-only check (25006) fires, not undefined-table (42P01).
    autocommit_write(&mut client, 10, "DROP TABLE IF EXISTS ferro_s6_ro").await;
    autocommit_write(&mut client, 11, "CREATE TABLE ferro_s6_ro (id bigint)").await;

    let tx_id = begin(&mut client, 12, "default", None, true).await; // READ ONLY

    // Reads are allowed in a read-only tx.
    let ro_read = exec_in_tx(&mut client, 13, tx_id, "SELECT 1", vec![], 0, true).await;
    assert!(
        matches!(&ro_read, Outcome::Ok(_)),
        "a read is allowed in a read-only tx: {ro_read:?}"
    );

    // A write is rejected: SQLSTATE 25006, NonRetryable, one END, and NEVER the §19.3 Indeterminate
    // branch (a rejected write is known-fate — it did not apply).
    let ep = match exec_in_tx(
        &mut client,
        14,
        tx_id,
        "INSERT INTO ferro_s6_ro (id) VALUES (1)",
        vec![],
        1,
        false,
    )
    .await
    {
        Outcome::Error(ep) => ep,
        other => panic!("a write in a RO tx must be rejected, got {other:?}"),
    };
    assert_eq!(
        ep.sqlstate.as_deref(),
        Some("25006"),
        "the raw read-only-transaction SQLSTATE is preserved on the wire"
    );
    assert_eq!(
        ep.branch,
        branch::NON_RETRYABLE,
        "a rejected write is NonRetryable"
    );
    assert_ne!(ep.code, errc::WRITE_UNCONFIRMED);
    assert_ne!(ep.branch, branch::INDETERMINATE);
    assert_session_alive(&mut client, 104).await;

    // PG has aborted the tx block on the error (25P02); ROLLBACK cleanly ends it, and the pinned
    // conn is released clean — a fresh autocommit SELECT works.
    assert!(
        matches!(rollback(&mut client, 15, tx_id).await, Outcome::Ok(_)),
        "ROLLBACK cleanly ends the aborted tx"
    );
    let ok = exec_ok(&mut client, 16, &req("SELECT 1")).await;
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
    assert_session_alive(&mut client, 105).await;
}

// -------------------------------------------------------------------------------------------------
// M1-S1 Task 5 REGRESSION test: a tx-scoped EXEC that errors (a genuine constraint violation,
// 23505) inside the S6 actor's `TxCommand::Exec` path aborts the tx block server-side (the real RFQ
// flips to `E`); the actor's own ROLLBACK teardown (driven by the client's TX/ROLLBACK, same as
// `tx_readonly_rejects_write` above) then returns a CLEAN, reusable conn: a SUBSEQUENT checkout (a
// fresh autocommit statement on this pool) gets an Idle conn — no inherited aborted-tx 25P02.
//
// NOTE: this proves the actor-driven error→rollback→clean-conn BEHAVIOR, not RFQ authority per se —
// on this Err arm the guarantee comes from `Checkout`'s Rule-A unconditional Err-arm fail-safe
// (`tx_open`/`tainted` forced on ANY `r.is_err()`) plus `rollback_tx`'s Ok-arm manual bookkeeping,
// not from the RFQ byte (which is stale-untrustworthy on an Err arm — SPEC §7.1). This test would
// pass identically under the old M0 stub; it is a valid regression test for the actor's
// error/rollback/reuse contract, not a discriminating proof that RFQ is now the authority. That
// discriminating proof (autocommit never pins; a failed statement holds the pin until an explicit
// ROLLBACK, driven by the real RFQ byte) lives in Task 4's `ferro-backend-pg/tests/pg_pool_it.rs`
// (`pg_rfq_autocommit_never_pins`, `pg_rfq_failed_stmt_holds_pin_until_rollback`).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_exec_constraint_violation_then_rollback_leaves_clean_conn() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    autocommit_write(&mut client, 10, "DROP TABLE IF EXISTS ferro_s6_uniq").await;
    autocommit_write(
        &mut client,
        11,
        "CREATE TABLE ferro_s6_uniq (id bigint PRIMARY KEY)",
    )
    .await;
    autocommit_write(&mut client, 12, "INSERT INTO ferro_s6_uniq (id) VALUES (1)").await;

    let tx_id = begin(&mut client, 13, "default", None, false).await;

    // A duplicate-key INSERT inside the tx errors (23505 unique_violation) — via the actor's own
    // `TxCommand::Exec` -> `co.query` path (actor.rs). PG aborts the tx block on this error (the
    // real RFQ byte flips to `E`); the conn is armed for cleanup by `Checkout`'s Rule-A unconditional
    // Err-arm fail-safe (keyed on `r.is_err()`, not on the RFQ byte itself — see the note above).
    let ep = match exec_in_tx(
        &mut client,
        14,
        tx_id,
        "INSERT INTO ferro_s6_uniq (id) VALUES (1)",
        vec![],
        1,
        false,
    )
    .await
    {
        Outcome::Error(ep) => ep,
        other => panic!("a duplicate-key INSERT must be rejected, got {other:?}"),
    };
    assert_eq!(
        ep.sqlstate.as_deref(),
        Some("23505"),
        "the raw unique-violation SQLSTATE is preserved on the wire"
    );
    assert_ne!(
        ep.branch,
        branch::INDETERMINATE,
        "a rolled-back in-tx statement persisted nothing — never Indeterminate"
    );
    assert_session_alive(&mut client, 130).await;

    // The actor's ROLLBACK/teardown ends the aborted tx block and releases a CLEAN conn to the pool
    // (the actor's own belt-and-braces `set_tainted(true)` in `teardown` is not exercised on this
    // path — that runs only on the Abort/Deadline teardown arms, not an explicit client ROLLBACK).
    assert!(
        matches!(rollback(&mut client, 15, tx_id).await, Outcome::Ok(_)),
        "ROLLBACK cleanly ends the aborted (E) tx"
    );

    // A SUBSEQUENT checkout of the pool (a fresh autocommit statement) gets an Idle, usable conn: the
    // SELECT succeeds outright rather than surfacing an inherited "current transaction is aborted"
    // (25P02) — exec_ok panics on anything but Outcome::Ok, so success here IS that proof.
    let ok = exec_ok(&mut client, 16, &req("SELECT 1")).await;
    assert_eq!(
        ok.rows,
        vec![vec![Value::I64(1)]],
        "a fresh autocommit SELECT succeeds on the recycled conn — no inherited aborted-tx state"
    );
    assert_session_alive(&mut client, 131).await;
}

// -------------------------------------------------------------------------------------------------
// Pinning: two in-tx pg_backend_pid() reads return the SAME backend pid (one pinned conn); after
// COMMIT a fresh autocommit pid MAY differ (the conn was released back to the pool).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_pins_one_backend_pid() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let tx_id = begin(&mut client, 12, "default", None, false).await;

    let pid1 = match exec_in_tx(
        &mut client,
        13,
        tx_id,
        "SELECT pg_backend_pid()",
        vec![],
        0,
        true,
    )
    .await
    {
        Outcome::Ok(body) => {
            first_i64(&ferro_proto::messages::sql::ExecOk::decode(&body).expect("ExecOk"))
        }
        other => panic!("pid read 1 failed: {other:?}"),
    };
    let pid2 = match exec_in_tx(
        &mut client,
        14,
        tx_id,
        "SELECT pg_backend_pid()",
        vec![],
        0,
        true,
    )
    .await
    {
        Outcome::Ok(body) => {
            first_i64(&ferro_proto::messages::sql::ExecOk::decode(&body).expect("ExecOk"))
        }
        other => panic!("pid read 2 failed: {other:?}"),
    };
    assert_eq!(
        pid1, pid2,
        "both in-tx statements ran on the SAME pinned backend (pid {pid1} vs {pid2})"
    );
    assert!(matches!(
        commit(&mut client, 15, tx_id).await,
        Outcome::Ok(_)
    ));

    // After release, a fresh autocommit statement may land on any pooled backend (MAY differ).
    let ok = exec_ok(&mut client, 16, &req("SELECT pg_backend_pid()")).await;
    let pid3 = first_i64(&ok);
    eprintln!(
        "TX PINNING >>> in-tx pid1={pid1} pid2={pid2} (equal ⇒ pinned); post-commit autocommit pid3={pid3}"
    );
    assert_eq!(ok.cols[0].tag, tag::I64, "pg_backend_pid() int4 → I64");
    assert_session_alive(&mut client, 106).await;
}

// -------------------------------------------------------------------------------------------------
// idle_in_transaction deadline: BEGIN, sit idle past the deadline, then a tx-scoped EXEC finds the
// tx tombstoned → TxDeadline{Retryable} (the pin was already rolled back + released).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_idle_deadline() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server_with_deadlines(
        url,
        Duration::from_millis(200), // idle_in_tx: short
        Duration::from_secs(60),    // max_tx: generous
    );
    let mut client = server.connect().await;
    client.hello(1).await;

    let tx_id = begin(&mut client, 12, "default", None, false).await;
    // Sit idle well past idle_in_tx: the actor's idle timer fires → rollback + tombstone.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let ep = match exec_in_tx(&mut client, 13, tx_id, "SELECT 1", vec![], 0, true).await {
        Outcome::Error(ep) => ep,
        other => panic!("a command after the idle deadline must be TxDeadline, got {other:?}"),
    };
    assert_eq!(ep.code, errc::TX_DEADLINE, "idle deadline → TxDeadline");
    assert_eq!(
        ep.branch,
        branch::RETRYABLE,
        "TxDeadline is Retryable (client policy)"
    );
    assert_session_alive(&mut client, 107).await;
}

// -------------------------------------------------------------------------------------------------
// max_tx deadline via the OUT-OF-BAND cancel: a long statement is cancelled server-side (57014) when
// the absolute deadline fires; the terminal is a SINGLE TxDeadline{Retryable} (NOT Indeterminate —
// the in-tx statement was rolled back, so nothing persisted), the pin is released, and a subsequent
// checkout gets a clean conn PROMPTLY (bounded). The statement is NEVER re-dispatched (rule 3).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_max_deadline() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server_with_deadlines(
        url,
        Duration::from_secs(60),    // idle_in_tx: generous
        Duration::from_millis(500), // max_tx: short — fires mid pg_sleep
    );
    let mut client = server.connect().await;
    client.hello(1).await;

    let tx_id = begin(&mut client, 12, "default", None, false).await;

    // pg_sleep(10) would take 10s if it ran to completion (>> the 2s recv timeout): a fast
    // TxDeadline terminal PROVES the out-of-band cancel fired mid-statement, not a natural finish.
    let ep = match exec_in_tx(
        &mut client,
        13,
        tx_id,
        "SELECT 1 FROM pg_sleep(10)",
        vec![],
        0,
        true,
    )
    .await
    {
        Outcome::Error(ep) => ep,
        other => panic!("a mid-statement max deadline must be TxDeadline, got {other:?}"),
    };
    eprintln!(
        "TX MAX-DEADLINE (via out-of-band cancel) >>> terminal code={:#06x} branch={} sqlstate={:?}",
        ep.code, ep.branch, ep.sqlstate
    );
    assert_eq!(
        ep.code,
        errc::TX_DEADLINE,
        "the mid-statement cancel → a single TxDeadline terminal"
    );
    assert_eq!(
        ep.branch,
        branch::RETRYABLE,
        "TxDeadline is Retryable, NOT Indeterminate"
    );
    assert_ne!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "a rolled-back in-tx statement persisted nothing — never Indeterminate"
    );
    assert_ne!(ep.branch, branch::INDETERMINATE);

    // Exactly one END (the session is alive), and the pin was released: a subsequent autocommit
    // checkout gets a clean, working conn PROMPTLY (bounded by the recv timeout). Re-touching the
    // dead tx_id yields TxDeadline again (tombstoned) — the statement is never re-run.
    assert_session_alive(&mut client, 108).await;
    let ok = exec_ok(&mut client, 14, &req("SELECT 1")).await;
    assert_eq!(
        ok.rows,
        vec![vec![Value::I64(1)]],
        "the pin was released — a fresh autocommit checkout works promptly"
    );
    match exec_in_tx(&mut client, 15, tx_id, "SELECT 1", vec![], 0, true).await {
        Outcome::Error(ep) => assert_eq!(
            ep.code,
            errc::TX_DEADLINE,
            "the timed-out tx stays tombstoned (never re-dispatched)"
        ),
        other => panic!("a re-touch of the dead tx must be TxDeadline, got {other:?}"),
    }
    assert_session_alive(&mut client, 109).await;
}

// -------------------------------------------------------------------------------------------------
// Cross-session: session A opens a tx; session B using A's tx_id → Protocol (indistinguishable from
// unknown); A's tx is undisturbed and commits.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_cross_session_rejected() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client_a = server.connect().await;
    client_a.hello(1).await;
    let mut client_b = server.connect().await;
    client_b.hello(1).await;

    let tx_id = begin(&mut client_a, 12, "default", None, false).await;

    // B forwards a tx-scoped EXEC with A's tx_id → Protocol (owner mismatch, indistinguishable from
    // an unknown id).
    match exec_in_tx(&mut client_b, 20, tx_id, "SELECT 1", vec![], 0, true).await {
        Outcome::Error(ep) => assert_eq!(
            ep.code,
            errc::PROTOCOL,
            "a cross-session tx-scoped EXEC is Protocol"
        ),
        other => panic!("expected Protocol, got {other:?}"),
    }
    // B's COMMIT of A's tx_id is likewise Protocol.
    match commit(&mut client_b, 21, tx_id).await {
        Outcome::Error(ep) => assert_eq!(
            ep.code,
            errc::PROTOCOL,
            "a cross-session COMMIT is Protocol"
        ),
        other => panic!("expected Protocol, got {other:?}"),
    }
    assert_session_alive(&mut client_b, 110).await;

    // A's tx is undisturbed: a read works and COMMIT succeeds.
    assert!(matches!(
        exec_in_tx(&mut client_a, 13, tx_id, "SELECT 1", vec![], 0, true).await,
        Outcome::Ok(_)
    ));
    assert!(matches!(
        commit(&mut client_a, 14, tx_id).await,
        Outcome::Ok(_)
    ));
    assert_session_alive(&mut client_a, 111).await;
}

// -------------------------------------------------------------------------------------------------
// Unknown tx_id: a COMMIT for a never-issued tx_id → Protocol; the session survives.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_unknown_id_protocol() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    match commit(&mut client, 12, 999_999).await {
        Outcome::Error(ep) => assert_eq!(
            ep.code,
            errc::PROTOCOL,
            "a COMMIT for a never-issued tx_id is Protocol"
        ),
        other => panic!("expected Protocol, got {other:?}"),
    }
    assert_session_alive(&mut client, 112).await;
}

// -------------------------------------------------------------------------------------------------
// M1-S8a Task 7 — savepoint SQL passthrough (SPEC §22.2 (r)). Doctrine's nested-transaction
// emulation is PLAIN SQL through `exec()`, never a driver API, so these are the literal strings
// `Doctrine\DBAL\Connection` emits.
// -------------------------------------------------------------------------------------------------

/// A statement that returns no rows (DDL / INSERT): the base `req` with `readonly = false` and
/// `fetch = FETCH_NONE`.
fn ddl(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.fetch = sql::FETCH_NONE;
    r
}

/// Doctrine's nested-transaction emulation, verbatim: it emits `SAVEPOINT DOCTRINE_1` /
/// `ROLLBACK TO SAVEPOINT DOCTRINE_1` / `RELEASE SAVEPOINT DOCTRINE_1` as PLAIN SQL through
/// `exec()`, never a driver API. The read-back is what proves the savepoint actually took — an
/// accepted statement that did nothing would pass a "no error" assertion.
#[tokio::test(flavor = "multi_thread")]
async fn savepoint_sql_passes_through_inside_a_transaction() {
    let Some(url) = pg_url() else {
        return; // prints `skip: FERRO_TEST_PG_URL unset`
    };
    let server = common::exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
    exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_sp (v int)")).await;

    let tx = begin(&mut c, 3, "default", None, false).await;
    for (rid, sql) in [
        (4, "INSERT INTO s8a_sp (v) VALUES (1)"),
        (5, "SAVEPOINT DOCTRINE_1"),
        (6, "INSERT INTO s8a_sp (v) VALUES (2)"),
        (7, "ROLLBACK TO SAVEPOINT DOCTRINE_1"),
        (8, "INSERT INTO s8a_sp (v) VALUES (3)"),
        (9, "RELEASE SAVEPOINT DOCTRINE_1"),
    ] {
        match exec_in_tx(&mut c, rid, tx, sql, Vec::new(), sql::FETCH_NONE, false).await {
            Outcome::Ok(_) => {}
            other => panic!("{sql:?} must pass through inside a transaction, got {other:?}"),
        }
    }
    match commit(&mut c, 10, tx).await {
        Outcome::Ok(_) => {}
        other => panic!("COMMIT: {other:?}"),
    }

    let rows = exec_ok(&mut c, 11, &req("SELECT v FROM s8a_sp ORDER BY v")).await;
    let got: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::I64(n) => *n,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(
        got,
        vec![1, 3],
        "the savepoint must have rolled 2 back and kept 1 and 3"
    );

    exec_ok(&mut c, 12, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
    assert_session_alive(&mut c, 0xC0FFEE).await;
}

/// A transaction-BOUNDARY verb stays refused INSIDE a transaction too — the half of the split that
/// protects the pin authority. Each refusal is a clean terminal that leaves the transaction usable,
/// which the trailing savepoint + COMMIT + read-back proves.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_sql_stays_refused_inside_a_transaction() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_boundary")).await;
    exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_boundary (v int)")).await;

    let tx = begin(&mut c, 3, "default", None, false).await;
    match exec_in_tx(
        &mut c,
        4,
        tx,
        "INSERT INTO s8a_boundary (v) VALUES (1)",
        Vec::new(),
        sql::FETCH_NONE,
        false,
    )
    .await
    {
        Outcome::Ok(_) => {}
        other => panic!("seed insert: {other:?}"),
    }

    // A DISGUISED boundary verb (mixed case, leading comment/whitespace) is still a boundary verb.
    let mut rid = 5;
    for boundary in [
        "COMMIT",
        "commit;",
        "  RollBack  ",
        "BEGIN",
        "START TRANSACTION",
        "END",
        "ABORT",
        "/* nested */ COMMIT",
        "-- nested\nROLLBACK",
        // A savepoint verb with a boundary verb riding behind it: refused as a COMPOUND savepoint,
        // never delegated (the text protocol would run BOTH statements).
        "SAVEPOINT X1; COMMIT",
    ] {
        match exec_in_tx(
            &mut c,
            rid,
            tx,
            boundary,
            Vec::new(),
            sql::FETCH_NONE,
            false,
        )
        .await
        {
            Outcome::Error(ep) => {
                assert_eq!(
                    ep.code,
                    errc::UNSUPPORTED,
                    "{boundary:?} must be UNSUPPORTED, got {ep:?}"
                );
            }
            other => panic!("{boundary:?} must be refused inside a transaction, got {other:?}"),
        }
        rid += 1;
    }

    // The transaction survived every refusal (nothing reached the backend), so a savepoint still
    // works and the COMMIT still commits.
    match exec_in_tx(
        &mut c,
        rid,
        tx,
        "SAVEPOINT AFTER_REFUSALS",
        Vec::new(),
        sql::FETCH_NONE,
        false,
    )
    .await
    {
        Outcome::Ok(_) => {}
        other => panic!("the tx must still be usable after the refusals, got {other:?}"),
    }
    rid += 1;
    match commit(&mut c, rid, tx).await {
        Outcome::Ok(_) => {}
        other => panic!("COMMIT after the refusals: {other:?}"),
    }
    rid += 1;

    let rows = exec_ok(&mut c, rid, &req("SELECT v FROM s8a_boundary ORDER BY v")).await;
    let got: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::I64(n) => *n,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(
        got,
        vec![1],
        "the refused boundary verbs neither committed nor rolled back the transaction"
    );
    rid += 1;

    exec_ok(&mut c, rid, &ddl("DROP TABLE IF EXISTS s8a_boundary")).await;
    assert_session_alive(&mut c, 0xC0FFEF).await;
}

/// Outside a transaction it stays refused — deliberately, because MySQL would silently ignore a
/// bare `SAVEPOINT` under autocommit (hazard 35 as refined).
#[tokio::test(flavor = "multi_thread")]
async fn savepoint_sql_outside_a_transaction_is_refused() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = common::exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    let mut rid = 1;
    for sp in [
        "SAVEPOINT DOCTRINE_1",
        "ROLLBACK TO SAVEPOINT DOCTRINE_1",
        "RELEASE SAVEPOINT DOCTRINE_1",
    ] {
        let e = exec_err(&mut c, rid, &ddl(sp)).await;
        assert_eq!(e.code, errc::UNSUPPORTED, "{sp:?} -> {e:?}");
        assert!(
            e.message.contains("outside a transaction"),
            "{sp:?} -> {}",
            e.message
        );
        rid += 1;
    }

    // ...and a boundary verb is refused whether or not a transaction is open.
    let e2 = exec_err(&mut c, rid, &ddl("COMMIT")).await;
    assert_eq!(e2.code, errc::UNSUPPORTED);
    assert!(e2.message.contains("use the TX service"), "{}", e2.message);

    // The session survives all of them — exactly one END each (charter rule 4).
    assert_session_alive(&mut c, 0xC0FFEE).await;
}

// -------------------------------------------------------------------------------------------------
// M1-S8a Task 8 — the dialect split did NOT move PostgreSQL (SPEC §22.2 (s)).
// -------------------------------------------------------------------------------------------------

/// PG is untouched by the dialect split — the composed strings are byte-identical to M1-S6's. This
/// mirrors the existing live isolation assertion in `tx_isolation_observed`, but with `readonly` ON,
/// so the `BEGIN ISOLATION LEVEL … READ ONLY` cell is exercised end to end and not only in the
/// table test.
///
/// The direct `current_setting('transaction_isolation')` read is valid HERE and not on MySQL:
/// PG's `BEGIN ISOLATION LEVEL …` sets the CURRENT transaction's level and reports it, while
/// MySQL's `SET TRANSACTION …` prefix applies to the NEXT transaction and is invisible in
/// `@@transaction_isolation` (see `mysql_it.rs`'s lock-conflict proof).
#[tokio::test(flavor = "multi_thread")]
async fn pg_begin_isolation_and_readonly_are_unchanged() {
    let Some(url) = pg_url() else {
        return; // prints `skip: FERRO_TEST_PG_URL unset`
    };
    let server = common::exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_pg_ro")).await;
    exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_pg_ro (v int)")).await;

    let tx = begin(
        &mut c,
        3,
        "default",
        Some(u8::from(Isolation::Serializable)),
        true,
    )
    .await;

    let iso = exec_ok(
        &mut c,
        4,
        &tx_req(
            tx,
            "SELECT current_setting('transaction_isolation')",
            Vec::new(),
            sql::FETCH_ROWS,
            true,
        ),
    )
    .await;
    assert_eq!(
        iso.rows,
        vec![vec![Value::Text("serializable".to_string())]],
        "PG still reports the composed isolation level inside the tx: {:?}",
        iso.rows
    );

    let e = match exec_in_tx(
        &mut c,
        5,
        tx,
        "INSERT INTO s8a_pg_ro (v) VALUES (1)",
        Vec::new(),
        sql::FETCH_NONE,
        false,
    )
    .await
    {
        Outcome::Error(ep) => ep,
        other => panic!("a write in a READ ONLY tx must be refused, got {other:?}"),
    };
    assert_eq!(
        e.sqlstate.as_deref(),
        Some("25006"),
        "PG enforces READ ONLY with the same SQLSTATE: {e:?}"
    );

    assert!(matches!(rollback(&mut c, 6, tx).await, Outcome::Ok(_)));

    // NOTE — there is deliberately NO "the next transaction did not inherit SERIALIZABLE"
    // assertion here. It was written, then measured to be a guard that CANNOT FAIL: with the PG arm
    // mutated to ALSO emit `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …` (a genuine
    // cross-tenant leak), it stayed GREEN — the S3 targeted hygiene profile's `RESET ALL` wipes the
    // session default at the next checkout before anything can observe it. The falsifiable leak
    // guard lives on a RAW connection, with no hygiene in the way: `ferro-backend-mysql`'s
    // `begin_dialect_it::the_batched_isolation_never_survives_the_transaction`.

    exec_ok(&mut c, 10, &ddl("DROP TABLE IF EXISTS s8a_pg_ro")).await;
    assert_session_alive(&mut c, 0xC0FFF0).await;
}
