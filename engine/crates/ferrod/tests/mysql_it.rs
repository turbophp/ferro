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
//!    lands in M1-S7) — NOT a mid-stream error.
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

use common::{TestClient, exec, exec_err, exec_ok, mariadb_url, mysql_url, req};
use ferro_proto::consts::{errc, flags, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};
use ferro_proto::value::Value;

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
//     error). Streaming lands in M1-S7 with the MySQL streaming bridge.
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
        r.fetch = 2; // FETCH_STREAM

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
        assert!(
            ep.message.contains("M1-S7"),
            "[{label}] the reject message points at the M1-S7 streaming bridge, got {:?}",
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
