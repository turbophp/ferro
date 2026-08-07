//! M1-S6 Task 3 — LIVE `PoolBackend` behavior for MySQL/MariaDB (the SPLIT pin signal).
//!
//! Proves — against real Dockerized MySQL 8 + MariaDB 11 — the two pin signals and the connection
//! ops at PG parity:
//!   * `tx_status` from `SERVER_STATUS_IN_TRANS` (InTx/Idle) and NEVER `Failed` (a statement error
//!     inside a tx leaves the tx open on the server, never an aborted state);
//!   * the BASELINE (a fresh conn is clean) + the session-mutation taint (`SET SESSION` taints, an
//!     `autocommit` toggle does not, a plain `SELECT` does not) with read-and-clear;
//!   * `reset(Full)` (COM_RESET_CONNECTION) clears a `SET SESSION`;
//!   * the `KILL QUERY` out-of-band cancel interrupts `SELECT SLEEP(3)` promptly.
//!
//! EVERY assertion runs against BOTH engines (two test fns sharing `run_backend_suite`). Each SKIPS
//! cleanly without its env var (`FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL`).

use std::time::{Duration, Instant};

use ferro_pool::backend::{Cancel, PoolBackend, ResetProfile, TxStatus};
use ferro_pool::error::PoolError;
use mysql_async::prelude::Queryable;

use ferro_backend_mysql::MysqlBackend;

/// Read a single `u64` scalar off the raw handle — a verification-only read (bypasses the pin
/// authority, which is fine for asserting server state in a test).
async fn read_u64(conn: &mut ferro_backend_mysql::MysqlConn, sql: &str) -> u64 {
    conn.mysql
        .query_first::<u64, _>(sql)
        .await
        .unwrap_or_else(|e| panic!("read `{sql}` failed: {e:?}"))
        .unwrap_or_else(|| panic!("read `{sql}` returned no row"))
}

async fn run_backend_suite(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);

    // ---- BASELINE: a fresh conn is clean (the connect/handshake SETs did NOT taint) -------------
    let mut conn = backend.connect().await.expect("connect");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::Idle,
        "[{label}] a fresh conn is Idle"
    );
    assert!(
        !backend.take_session_mutated(&mut conn),
        "[{label}] BASELINE: a fresh conn reports no session mutation"
    );

    // ---- tx_status from the status flag: InTx / Idle; never Failed --------------------------------
    backend
        .simple_query(&mut conn, "START TRANSACTION")
        .await
        .expect("START TRANSACTION");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::InTx,
        "[{label}] START TRANSACTION => InTx"
    );

    // A statement error INSIDE the tx: MySQL keeps the tx open (no aborted state). The errored
    // statement clears the driver's last_ok_packet, so the very next tx_status is a stale Idle — but
    // it is NEVER Failed, and a subsequent read refreshes the flag to prove the tx is STILL open.
    let dup = backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_smoke (id, note) VALUES (1, 'dup')",
        )
        .await;
    assert!(
        matches!(dup, Err(PoolError::Sql { .. })),
        "[{label}] a duplicate-key error is a known-fate Sql error, got {dup:?}"
    );
    assert_ne!(
        backend.tx_status(&conn),
        TxStatus::Failed,
        "[{label}] tx_status must NEVER be Failed (MySQL has no aborted-open-tx state)"
    );
    // Refresh the OK packet with a benign read → the tx is provably STILL open (InTx, not aborted).
    backend
        .simple_query(&mut conn, "SELECT 1")
        .await
        .expect("SELECT 1 inside the still-open tx");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::InTx,
        "[{label}] the tx is STILL open after a statement error (InTx, never Failed)"
    );
    backend
        .simple_query(&mut conn, "ROLLBACK")
        .await
        .expect("ROLLBACK");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::Idle,
        "[{label}] ROLLBACK => Idle"
    );

    // A clean COMMIT path too.
    backend
        .simple_query(&mut conn, "START TRANSACTION")
        .await
        .unwrap();
    assert_eq!(backend.tx_status(&conn), TxStatus::InTx);
    backend.simple_query(&mut conn, "COMMIT").await.unwrap();
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::Idle,
        "[{label}] COMMIT => Idle"
    );

    // Drain any taint the tx-control statements may have set, so the mutation checks below start clean.
    let _ = backend.take_session_mutated(&mut conn);

    // ---- the session-mutation taint (baselined + curated) ---------------------------------------

    // A plain SELECT does NOT taint.
    backend.simple_query(&mut conn, "SELECT 1").await.unwrap();
    assert!(
        !backend.take_session_mutated(&mut conn),
        "[{label}] a plain SELECT must not taint"
    );

    // An `autocommit` toggle (the Ferro-managed allowlist) does NOT taint — even though it fires a
    // SystemVariables[autocommit] tracker AND the state-changed flag.
    backend
        .simple_query(&mut conn, "SET autocommit = 0")
        .await
        .unwrap();
    assert!(
        !backend.take_session_mutated(&mut conn),
        "[{label}] an autocommit toggle must NOT taint (allowlisted)"
    );
    // Put autocommit back and drain (its tracker again must not taint).
    backend
        .simple_query(&mut conn, "SET autocommit = 1")
        .await
        .unwrap();
    assert!(!backend.take_session_mutated(&mut conn));

    // A genuine `SET SESSION sort_buffer_size` TAINTS, and the taint is read-and-cleared.
    backend
        .simple_query(&mut conn, "SET SESSION sort_buffer_size = 524288")
        .await
        .unwrap();
    assert!(
        backend.take_session_mutated(&mut conn),
        "[{label}] a user SET SESSION sort_buffer_size must taint"
    );
    assert!(
        !backend.take_session_mutated(&mut conn),
        "[{label}] read-and-clear: the taint is consumed on the first read"
    );

    // The §7.1 raison d'être: a SET SESSION buried in a stored program (invisible to the assist
    // lexer) still taints via the OK-packet tracker.
    backend
        .simple_query(&mut conn, "CALL p_set_session()")
        .await
        .unwrap();
    assert!(
        backend.take_session_mutated(&mut conn),
        "[{label}] a SET SESSION inside a stored program must taint (the OK-packet tracker sees it)"
    );

    // ---- reset(Full) = COM_RESET_CONNECTION clears a SET SESSION ---------------------------------
    // Reset FIRST for a clean baseline (prior taint tests, incl. `CALL p_set_session()`, left
    // sort_buffer_size mutated). `* 2` (not `+ const`) so the bumped value is always a valid
    // multiple regardless of the engine's block-size rounding (MySQL default 262144 vs MariaDB
    // 2097152).
    backend
        .reset(&mut conn, ResetProfile::Full)
        .await
        .expect("reset to a clean baseline");
    let default_sbs = read_u64(&mut conn, "SELECT @@session.sort_buffer_size").await;
    let bumped = default_sbs * 2;
    backend
        .simple_query(
            &mut conn,
            &format!("SET SESSION sort_buffer_size = {bumped}"),
        )
        .await
        .unwrap();
    let after_set = read_u64(&mut conn, "SELECT @@session.sort_buffer_size").await;
    assert_eq!(after_set, bumped, "[{label}] the SET took effect");
    backend
        .reset(&mut conn, ResetProfile::Full)
        .await
        .expect("reset(Full)");
    let after_reset = read_u64(&mut conn, "SELECT @@session.sort_buffer_size").await;
    assert_eq!(
        after_reset, default_sbs,
        "[{label}] COM_RESET_CONNECTION restored sort_buffer_size to the default"
    );
    // After a reset the conn is clean again.
    assert!(!backend.take_session_mutated(&mut conn));
    assert!(
        !backend.is_closed(&conn),
        "[{label}] conn healthy after reset"
    );

    // ---- KILL QUERY cancels SELECT SLEEP(3) promptly ---------------------------------------------
    let handle = backend.cancel_handle(&conn);
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.cancel().await; // opens a side conn, runs KILL QUERY <id>
    });
    let start = Instant::now();
    let _ = backend.simple_query(&mut conn, "SELECT SLEEP(3)").await; // Ok or interrupted-Err, both fine
    let elapsed = start.elapsed();
    cancel_task.await.ok();
    assert!(
        elapsed < Duration::from_secs(3),
        "[{label}] KILL QUERY must interrupt SELECT SLEEP(3) well under 3s, took {elapsed:?}"
    );

    // ---- ping / clean disconnect -----------------------------------------------------------------
    backend.ping(&mut conn).await.expect("ping round trip");
    conn.mysql.disconnect().await.ok();

    println!("[{label}] Task-3 backend suite PASSED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_backend_behavior() {
    let Ok(url) = std::env::var("FERRO_TEST_MYSQL_URL") else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL unset (mysql_backend_behavior)");
        return;
    };
    run_backend_suite(&url, "MYSQL").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_backend_behavior() {
    let Ok(url) = std::env::var("FERRO_TEST_MARIADB_URL") else {
        eprintln!("skip: FERRO_TEST_MARIADB_URL unset (mariadb_backend_behavior)");
        return;
    };
    run_backend_suite(&url, "MARIADB").await;
}
