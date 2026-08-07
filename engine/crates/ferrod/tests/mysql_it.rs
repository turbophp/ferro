//! M1-S6 Task 5 — the LIVE MySQL/MariaDB daemon path end to end: a real client → `ferrod` session
//! → the heterogeneous pool registry (`AnyPool::Mysql`) → the generic EXEC/TX handler bodies
//! (`run_exec_on_pool` / `begin_on_pool`) → live Dockerized MySQL 8 / MariaDB 11 → single terminal
//! `END` → client. Proves that a `kind = mysql` pool (inferred from the `mysql://` DSN scheme):
//!
//!  * round-trips a BUFFERED `SELECT` (`fetch:rows`) through the SAME generic autocommit body PG
//!    uses (the monomorphic → heterogeneous registry fix);
//!  * runs a `BEGIN .. COMMIT` transaction through the tx path (the actor spawns a
//!    `Checkout<MysqlBackend>`; the routing + terminal are backend-agnostic);
//!  * REJECTS `fetch:stream` EARLY with a clean, documented `Unsupported` terminal (MySQL streaming
//!    is DEFERRED — SPEC §22.2 (n)) — NOT a mid-stream error;
//!  * (M1-S8a) refuses it IDENTICALLY on BOTH EXEC arms — autocommit and tx-scoped — off the ONE
//!    `PoolBackend::supports_row_streaming()` authority, the tx-scoped one BEFORE the actor can
//!    touch (and force-taint) the pinned connection.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL` are unset
//! — same discipline as `sql_exec_it.rs` / `tx_it.rs` — so `cargo test --workspace` stays green
//! offline. Where BOTH are set, every scenario runs against BOTH dialects.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferrod --test mysql_it -- --nocapture
//! ```

mod common;

use std::time::Duration;

use common::{TestClient, exec, exec_err, exec_ok, mariadb_url, mysql_url, req};
use ferro_proto::consts::{errc, flags, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};
use ferro_proto::value::Value;
use ferrod::services::sql::{FETCH_NONE, FETCH_STREAM};

// -------------------------------------------------------------------------------------------------
// Targets: run each scenario against every configured dialect (MySQL 8 + MariaDB 11) that is set.
// -------------------------------------------------------------------------------------------------

/// The set of `(label, dsn)` MySQL-family targets under test — MySQL 8 and/or MariaDB 11, whichever
/// env var is set. Empty → the caller SKIPS (offline). Both set → the scenario runs against both.
fn mysql_targets() -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(u) = mysql_url() {
        out.push(("mysql", u));
    }
    if let Some(u) = mariadb_url() {
        out.push(("mariadb", u));
    }
    out
}

// -------------------------------------------------------------------------------------------------
// Minimal TX client helpers (a self-contained subset of `tx_it.rs`'s, kept local so the MySQL story
// lives in one file). BEGIN uses isolation=None / readonly=false → the composed SQL is the bare
// `BEGIN`, which MySQL accepts as `START TRANSACTION` (the PG-flavored `BEGIN READ ONLY` / `BEGIN
// ISOLATION LEVEL ...` forms are NOT valid MySQL — out of scope for this daemon-plumbing task).
// -------------------------------------------------------------------------------------------------

async fn begin(client: &mut TestClient, rid: u32, pool: &str) -> u64 {
    let breq = BeginRequest {
        pool: pool.to_string(),
        isolation: None,
        readonly: false,
    };
    client
        .send_request(rid, service::TX, method_tx::BEGIN, breq.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.flags & flags::END, flags::END, "BEGIN → one END");
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

fn tx_req(tx_id: u64, sql: &str) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params: Vec::new(),
        timeout_ms: None,
        readonly: true,
        fetch: 0, // rows
        tx_id: Some(tx_id),
    }
}

async fn commit(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    client
        .send_request(
            rid,
            service::TX,
            method_tx::COMMIT,
            TxControl { tx_id }.encode(),
        )
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.flags & flags::END, flags::END, "COMMIT → one END");
    assert_eq!(t.header.service, service::TX);
    assert_eq!(t.header.method, method_tx::COMMIT);
    Outcome::decode(&t.payload).expect("decode COMMIT Outcome")
}

/// The `I64` in the first cell of the first row.
fn first_i64(ok: &ferro_proto::messages::sql::ExecOk) -> i64 {
    match ok.rows.first().and_then(|r| r.first()) {
        Some(Value::I64(v)) => *v,
        other => panic!("expected an I64 scalar in row 0 col 0, got {other:?}"),
    }
}

// -------------------------------------------------------------------------------------------------
// (1) Buffered `SELECT 1` (fetch:rows) round-trips e2e through a kind=mysql pool.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mysql_buffered_select_roundtrips() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return; // offline: both URLs unset (each printed its own skip line)
    }
    for (label, url) in targets {
        let server = common::exec_server(url); // kind inferred from the mysql:// scheme
        let mut client = server.connect().await;
        client.hello(1).await;

        // A bare buffered SELECT: `1` is a LONGLONG literal → Value::I64(1) via the MySQL rowmap.
        let ok = exec_ok(&mut client, 10, &req("SELECT 1")).await;
        assert_eq!(ok.rows.len(), 1, "[{label}] one row");
        assert_eq!(first_i64(&ok), 1, "[{label}] SELECT 1 → I64(1)");

        // A second buffered SELECT on the same session proves the conn recycled cleanly.
        let ok2 = exec_ok(&mut client, 11, &req("SELECT 42")).await;
        assert_eq!(first_i64(&ok2), 42, "[{label}] SELECT 42 → I64(42)");
    }
}

// -------------------------------------------------------------------------------------------------
// (2) A BEGIN .. (in-tx SELECT) .. COMMIT transaction runs through the tx path (Checkout<MysqlBackend>).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mysql_tx_begin_commit_roundtrips() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        // BEGIN → a real tx_id (the actor now owns a pinned Checkout<MysqlBackend>).
        let tx_id = begin(&mut client, 20, "default").await;

        // An in-tx buffered SELECT rides SQL/EXEC with tx_id set → forwarded to the owning actor.
        match exec(&mut client, 21, &tx_req(tx_id, "SELECT 7")).await {
            Outcome::Ok(body) => {
                let ok = ferro_proto::messages::sql::ExecOk::decode(&body).expect("decode ExecOk");
                assert_eq!(first_i64(&ok), 7, "[{label}] in-tx SELECT 7 → I64(7)");
            }
            other => panic!("[{label}] in-tx SELECT expected Ok, got {other:?}"),
        }

        // COMMIT closes the tx cleanly (one END, Ok).
        match commit(&mut client, 22, tx_id).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT expected Ok, got {other:?}"),
        }

        // The session outlives the committed tx: a fresh autocommit SELECT still works.
        let ok = exec_ok(&mut client, 23, &req("SELECT 1")).await;
        assert_eq!(
            first_i64(&ok),
            1,
            "[{label}] post-commit autocommit SELECT works"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// (3) A fetch:stream EXEC to a MySQL pool → the documented Unsupported terminal (NOT a mid-stream
//     error). MySQL row streaming is DEFERRED — SPEC §22.2 (n).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mysql_fetch_stream_rejected_as_unsupported() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let mut r = req("SELECT 1");
        r.fetch = FETCH_STREAM;

        // A SINGLE END-terminal error (exactly-one-END), Unsupported, with the documented message —
        // asserted via `exec_err` (which checks the one-END SQL/EXEC terminal shape).
        let ep = exec_err(&mut client, 30, &r).await;
        assert_eq!(
            ep.code,
            errc::UNSUPPORTED,
            "[{label}] fetch:stream on MySQL is an Unsupported terminal"
        );
        assert!(
            ep.message.contains("MySQL"),
            "[{label}] the reject message names MySQL, got {:?}",
            ep.message
        );
        // M1-S8a: the refusal now cites the SPEC deferral entry, not a slice number that has
        // already shipped (the stale "M1-S7" text was the drift this task removed).
        assert!(
            ep.message.contains("§22.2"),
            "[{label}] the reject message points at the SPEC §22.2 deferral, got {:?}",
            ep.message
        );

        // The buffered path on the SAME session still works — only streaming was rejected, the
        // session is intact (no mid-stream desync).
        let ok = exec_ok(&mut client, 31, &req("SELECT 1")).await;
        assert_eq!(
            first_i64(&ok),
            1,
            "[{label}] buffered SELECT still works after the reject"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// (4) M1-S8a Task 1 — ONE streaming-capability authority: BOTH EXEC arms (autocommit and tx-scoped)
//     refuse `fetch:stream` on a MySQL pool with the SAME terminal, and the tx-scoped one refuses
//     BEFORE the actor ever touches the pinned connection.
// -------------------------------------------------------------------------------------------------

/// Both `fetch:stream` arms on a MySQL pool must refuse with the SAME, precise terminal — and the
/// tx-scoped one must refuse BEFORE the actor touches the pinned connection.
///
/// Falsifiable: before this task the tx-scoped arm reached `MysqlBackend::query_stream` and returned
/// the stale `"MySQL streaming lands in M1-S7"` string (a different message from the autocommit
/// arm's), after force-tainting the pinned conn at `ferro-pool/src/pool.rs:674-677`. The
/// byte-equality assertion below is what goes RED if the two arms ever drift again.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_stream_is_refused_identically_on_both_arms_and_the_tx_survives() {
    for (label, url) in mysql_targets() {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(0).await;

        // (a) autocommit arm — `req` is fetch:rows, so flip it to stream.
        let mut auto_req = req("SELECT 1");
        auto_req.fetch = FETCH_STREAM;
        let auto = exec_err(&mut client, 1, &auto_req).await;

        // (b) tx-scoped arm.
        let tx_id = begin(&mut client, 2, "default").await;
        let mut scoped_req = tx_req(tx_id, "SELECT 1");
        scoped_req.fetch = FETCH_STREAM;
        let scoped = exec_err(&mut client, 3, &scoped_req).await;

        // Charter rule 4 on the NEW refusal path: `exec_err` already consumed exactly one
        // END-flagged terminal for rid 3 (at-least-one); nothing else may follow it (at-most-one).
        // A second frame here — a stray HEAD, a duplicate terminal, or the actor's own late
        // terminal — makes this `Some(..)` and the test RED.
        assert!(
            client
                .recv_or_none(Duration::from_millis(250))
                .await
                .is_none(),
            "[{label}] the tx-scoped stream refusal must emit EXACTLY one frame (one END)"
        );

        assert_eq!(
            auto.message, scoped.message,
            "[{label}] the autocommit and tx-scoped stream refusals must come from ONE constructor"
        );
        assert_eq!(auto.code, scoped.code, "[{label}] same terminal code");
        assert_eq!(
            auto.code,
            errc::UNSUPPORTED,
            "[{label}] a stream refusal is Unsupported"
        );

        // The refusal must be the PRE-DISPATCH one. Asserted against the daemon's OWN constructor,
        // not a literal restated here — which is what makes this falsifiable in BOTH directions:
        // `MysqlBackend::query_stream`'s late refusal carries a DIFFERENT string, so if either arm
        // stops guarding (or `supports_row_streaming()` is wrongly `true`), the request reaches the
        // backend, the message changes, and this goes RED. Message equality alone cannot see that —
        // both arms would degrade to the same late string together.
        let expected = ferrod::services::sql::stream_unsupported().message;
        assert_eq!(
            scoped.message, expected,
            "[{label}] the tx-scoped refusal must be declared BEFORE dispatch (the actor never \
             touches — and never force-taints — the pinned conn)"
        );
        assert_eq!(
            auto.message, expected,
            "[{label}] the autocommit refusal must be declared BEFORE checkout"
        );
        assert!(
            auto.message.contains("§22.2"),
            "[{label}] the refusal must cite the spec deferral, got {:?}",
            auto.message
        );
        assert!(
            !auto.message.contains("M1-S7"),
            "[{label}] the stale slice name must be gone, got {:?}",
            auto.message
        );

        // The tx was never touched: a normal statement still runs and COMMIT succeeds.
        let ok = exec_ok(&mut client, 4, &tx_req(tx_id, "SELECT 7")).await;
        assert_eq!(
            first_i64(&ok),
            7,
            "[{label}] the pinned tx conn must still be usable after a refused stream"
        );
        match commit(&mut client, 5, tx_id).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT after a refused tx-scoped stream: {other:?}"),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// (6) M1-S8a Task 7 — savepoint SQL passthrough on the MySQL family (SPEC §22.2 (r)).
// -------------------------------------------------------------------------------------------------

/// A statement that returns no rows (DDL / INSERT / a savepoint op): `readonly = false`,
/// `fetch = FETCH_NONE`.
fn ddl(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.fetch = FETCH_NONE;
    r
}

/// A tx-scoped WRITE request (`readonly = false`, `fetch = FETCH_NONE`) — `tx_req` above is the
/// read-only/rows-fetching form the earlier scenarios use.
fn tx_write_req(tx_id: u64, sql: &str) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params: Vec::new(),
        timeout_ms: None,
        readonly: false,
        fetch: FETCH_NONE,
        tx_id: Some(tx_id),
    }
}

/// Doctrine's nested-transaction emulation, verbatim, on BOTH MySQL-family engines. The read-back
/// is what proves the savepoint took: an accepted statement that did nothing would pass a "no
/// error" assertion.
///
/// This is also the gate on the ROUTING half of the fix. MySQL 8 cannot run a savepoint verb on the
/// prepared-statement path at all (measured on 8.4.11: `COM_STMT_PREPARE` of `SAVEPOINT` /
/// `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` → errno 1295), while MariaDB 11.8 can — so an
/// implementation that only relaxed the guard and left the passthrough on the prepared path is
/// GREEN on MariaDB and RED here on MySQL.
#[tokio::test(flavor = "multi_thread")]
async fn savepoint_sql_passes_through_inside_a_transaction() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return; // offline: both URLs unset (each printed its own skip line)
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
        exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_sp (v INT)")).await;

        let tx = begin(&mut c, 3, "default").await;
        for (rid, stmt) in [
            (4, "INSERT INTO s8a_sp (v) VALUES (1)"),
            (5, "SAVEPOINT DOCTRINE_1"),
            (6, "INSERT INTO s8a_sp (v) VALUES (2)"),
            (7, "ROLLBACK TO SAVEPOINT DOCTRINE_1"),
            (8, "INSERT INTO s8a_sp (v) VALUES (3)"),
            (9, "RELEASE SAVEPOINT DOCTRINE_1"),
        ] {
            match exec(&mut c, rid, &tx_write_req(tx, stmt)).await {
                Outcome::Ok(_) => {}
                other => {
                    panic!("[{label}] {stmt:?} must pass through inside a transaction: {other:?}")
                }
            }
        }
        match commit(&mut c, 10, tx).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT: {other:?}"),
        }

        let rows = exec_ok(&mut c, 11, &req("SELECT v FROM s8a_sp ORDER BY v")).await;
        let got: Vec<i64> = rows
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("[{label}] unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            got,
            vec![1, 3],
            "[{label}] the savepoint must have rolled 2 back and kept 1 and 3"
        );

        exec_ok(&mut c, 12, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
        common::assert_session_alive(&mut c, 0xC0FFEE).await;
    }
}

/// A transaction-BOUNDARY verb stays refused INSIDE a transaction on both engines — including one
/// disguised by case/comments, and one riding behind a savepoint in a COMPOUND statement (which the
/// text protocol would otherwise happily execute, `CLIENT_MULTI_STATEMENTS` being negotiated).
#[tokio::test(flavor = "multi_thread")]
async fn boundary_sql_stays_refused_inside_a_transaction() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_boundary")).await;
        exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_boundary (v INT)")).await;

        let tx = begin(&mut c, 3, "default").await;
        match exec(
            &mut c,
            4,
            &tx_write_req(tx, "INSERT INTO s8a_boundary (v) VALUES (1)"),
        )
        .await
        {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] seed insert: {other:?}"),
        }

        let mut rid = 5;
        for boundary in [
            "COMMIT",
            "commit;",
            "  RollBack  ",
            "BEGIN",
            "START TRANSACTION",
            "END",
            "/* nested */ COMMIT",
            "-- nested\nROLLBACK",
            "SAVEPOINT X1; COMMIT",
        ] {
            match exec(&mut c, rid, &tx_write_req(tx, boundary)).await {
                Outcome::Error(ep) => assert_eq!(
                    ep.code,
                    errc::UNSUPPORTED,
                    "[{label}] {boundary:?} must be UNSUPPORTED, got {ep:?}"
                ),
                other => {
                    panic!("[{label}] {boundary:?} must be refused inside a transaction: {other:?}")
                }
            }
            rid += 1;
        }

        // The transaction survived every refusal, so a savepoint still works and COMMIT commits.
        match exec(&mut c, rid, &tx_write_req(tx, "SAVEPOINT AFTER_REFUSALS")).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] the tx must still be usable after the refusals: {other:?}"),
        }
        rid += 1;
        match commit(&mut c, rid, tx).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT after the refusals: {other:?}"),
        }
        rid += 1;

        let rows = exec_ok(&mut c, rid, &req("SELECT v FROM s8a_boundary ORDER BY v")).await;
        let got: Vec<i64> = rows
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("[{label}] unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            got,
            vec![1],
            "[{label}] the refused boundary verbs neither committed nor rolled back the tx"
        );
        rid += 1;

        exec_ok(&mut c, rid, &ddl("DROP TABLE IF EXISTS s8a_boundary")).await;
        common::assert_session_alive(&mut c, 0xC0FFEF).await;
    }
}

/// Outside a transaction all three savepoint verbs stay refused — deliberately, because MySQL
/// SILENTLY IGNORES a bare `SAVEPOINT` under autocommit (no transaction is started, the savepoint
/// has no effect), so delegating would hand a driver a rollback point that does not exist.
#[tokio::test(flavor = "multi_thread")]
async fn savepoint_sql_outside_a_transaction_is_refused() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
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
            assert_eq!(e.code, errc::UNSUPPORTED, "[{label}] {sp:?} -> {e:?}");
            assert!(
                e.message.contains("outside a transaction"),
                "[{label}] {sp:?} -> {}",
                e.message
            );
            rid += 1;
        }

        let e2 = exec_err(&mut c, rid, &ddl("COMMIT")).await;
        assert_eq!(e2.code, errc::UNSUPPORTED);
        assert!(
            e2.message.contains("use the TX service"),
            "[{label}] {}",
            e2.message
        );

        common::assert_session_alive(&mut c, 0xC0FFEE).await;
    }
}
