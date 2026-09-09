//! B2a (dev-loop ledger): the vendored `mysql_async` fork's owned-connection STREAM RECOVERY,
//! proven live against real MySQL and MariaDB — the structural blocker SPEC §22.2 (n) recorded
//! against MySQL-family `fetch:stream`, closed at the FORK level.
//!
//! What (n) measured: every `mysql_async` streaming entry point either borrows the connection or,
//! on the owned-`Conn` route, provides NO way to get the `Conn` back — dropping the stream (or the
//! `QueryResult`, for a statement with no result set) CLOSES the connection, which is unusable for
//! a pool that must stream a result set and then RECYCLE the same server session. The fork now
//! carries two additions (see `UPSTREAM_PR_MYSQL_ASYNC.md`):
//!
//! - `ResultSetStream::into_conn()` — drain whatever remains, hand the owned `Conn` back;
//! - `QueryResult::into_conn()` — the same exit for a statement that produced no result set
//!   (`stream_and_drop()` answers `None` there).
//!
//! Every test asserts SESSION IDENTITY (`CONNECTION_ID()` unchanged, a session user variable
//! surviving) — a recovered connection that silently reconnected would be a cross-tenant
//! session-state leak, the exact hazard the pool's pin engine exists to prevent, so "it still
//! answers queries" is NOT the bar; "it is the SAME server session" is.
//!
//! The row-count rule (n) measured is pinned here too: after the drain, counts are read from the
//! CONNECTION's final packet (`Conn::affected_rows()`), never from the stream's own
//! `affected_rows()` accessor, which reports the ok-packet captured at stream SETUP — the
//! PREVIOUS statement's.
//!
//! Each test SKIPS cleanly without its env var (`FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL`);
//! CI's integration lane sets both. Locally:
//!
//!   FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//!   FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!     cargo test -p ferro-backend-mysql --test stream_recovery_it -- --nocapture

use futures_util::StreamExt;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Row};

/// 900 keeps the recursion under MySQL 8's default `cte_max_recursion_depth` (1000) so the same
/// SQL runs unmodified on MariaDB (which has a different knob), while still spanning multiple
/// network packets — a genuine incremental stream, not a single-packet degenerate case.
const ROWS: i64 = 900;
const SEQ_SQL: &str =
    "WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < ?) SELECT n FROM s";

async fn connect(url: &str) -> Conn {
    Conn::new(Opts::from_url(url).expect("test DSN parses"))
        .await
        .expect("test backend reachable")
}

async fn session_id(conn: &mut Conn) -> u64 {
    "SELECT CONNECTION_ID()"
        .first::<u64, _>(&mut *conn)
        .await
        .expect("CONNECTION_ID() answers")
        .expect("one row")
}

/// The identity gate: same server thread id AND the pre-stream session marker intact.
async fn assert_same_session(conn: &mut Conn, id_before: u64, label: &str, at: &str) {
    let id_after = session_id(conn).await;
    assert_eq!(
        id_before, id_after,
        "[{label}] {at}: CONNECTION_ID() changed — the 'recovered' conn is a NEW session"
    );
    let probe = "SELECT @ferro_probe"
        .first::<Option<i64>, _>(&mut *conn)
        .await
        .expect("probe read")
        .expect("one row");
    assert_eq!(
        Some(42),
        probe,
        "[{label}] {at}: the session user variable is gone — new session (or session reset)"
    );
}

async fn full_drain_then_recover(url: &str, label: &str) {
    let mut conn = connect(url).await;
    conn.query_drop("SET @ferro_probe := 42")
        .await
        .expect("marker set");
    let id_before = session_id(&mut conn).await;

    // The owned route B2b will use: `run(conn)` moves the connection in; `stream_and_drop`
    // yields the fork's recoverable stream.
    let qr = SEQ_SQL.with((ROWS,)).run(conn).await.expect("owned run");
    let mut stream = qr
        .stream_and_drop::<Row>()
        .await
        .expect("stream setup")
        .expect("a SELECT has a result set");

    let mut expected = 1i64;
    while let Some(row) = stream.next().await {
        let mut row = row.expect("streamed row");
        let n: i64 = row.take(0).expect("column 0");
        assert_eq!(expected, n, "[{label}] rows arrive in order");
        expected += 1;
    }
    assert_eq!(ROWS + 1, expected, "[{label}] every row arrived");

    // Pre-fork, the `None` above would already have CLOSED the connection. Recover it instead.
    let mut conn = stream
        .into_conn()
        .await
        .expect("into_conn after full drain");
    assert_same_session(&mut conn, id_before, label, "after full drain").await;
    conn.disconnect().await.expect("clean disconnect");
    println!("[{label}] full-drain recovery PASSED");
}

async fn partial_consume_then_recover(url: &str, label: &str) {
    let mut conn = connect(url).await;
    conn.query_drop("SET @ferro_probe := 42")
        .await
        .expect("marker set");
    let id_before = session_id(&mut conn).await;

    let qr = SEQ_SQL.with((ROWS,)).run(conn).await.expect("owned run");
    let mut stream = qr
        .stream_and_drop::<Row>()
        .await
        .expect("stream setup")
        .expect("result set");

    // Take 5 of 900, then abandon: `into_conn` must drain the remaining 895 itself, or the
    // recovered connection would be protocol-desynced and the next statement would hang/garble.
    for i in 1..=5i64 {
        let mut row = stream.next().await.expect("row present").expect("row ok");
        assert_eq!(
            i,
            row.take::<i64, _>(0).expect("column 0"),
            "[{label}] prefix in order"
        );
    }
    let mut conn = stream
        .into_conn()
        .await
        .expect("into_conn mid-stream drains the rest");
    assert_same_session(&mut conn, id_before, label, "after partial consume").await;

    // The recovered session must be fully usable for ordinary work.
    let sum: i64 = "SELECT 2 + 2"
        .first(&mut conn)
        .await
        .expect("post-recovery query")
        .expect("one row");
    assert_eq!(4, sum, "[{label}] recovered conn does real work");
    conn.disconnect().await.expect("clean disconnect");
    println!("[{label}] partial-consume recovery PASSED");
}

async fn no_result_set_recovers_via_query_result(url: &str, label: &str) {
    let mut conn = connect(url).await;
    conn.query_drop("SET @ferro_probe := 42")
        .await
        .expect("marker set");
    conn.query_drop("CREATE TEMPORARY TABLE ferro_b2a_probe (n INT NOT NULL)")
        .await
        .expect("temp table");
    let id_before = session_id(&mut conn).await;

    // A write through the SAME owned route: no result set, so `stream_and_drop` would answer
    // `None` AND eat the connection — `QueryResult::into_conn` is the exit instead.
    let qr = "INSERT INTO ferro_b2a_probe (n) VALUES (?), (?), (?)"
        .with((1, 2, 3))
        .run(conn)
        .await
        .expect("owned INSERT run");
    assert!(
        qr.columns_ref().is_empty(),
        "[{label}] an INSERT reports no result-set columns — the dispatch signal B2b keys on"
    );
    let mut conn = qr
        .into_conn()
        .await
        .expect("into_conn on a no-result-set statement");

    // The (n) row-count rule: the count comes from the CONNECTION's final packet, post-drain.
    assert_eq!(
        3,
        conn.affected_rows(),
        "[{label}] Conn::affected_rows() carries the INSERT count"
    );
    assert_same_session(&mut conn, id_before, label, "after INSERT recovery").await;

    // At-most-once sanity: the write landed exactly once.
    let count: i64 = "SELECT COUNT(*) FROM ferro_b2a_probe"
        .first(&mut conn)
        .await
        .expect("count read")
        .expect("one row");
    assert_eq!(3, count, "[{label}] exactly the three inserted rows exist");
    conn.disconnect().await.expect("clean disconnect");
    println!("[{label}] no-result-set (QueryResult) recovery PASSED");
}

async fn borrowed_stream_refuses_and_conn_survives(url: &str, label: &str) {
    let mut conn = connect(url).await;
    let id_before = session_id(&mut conn).await;

    // The borrowed route: the caller keeps the connection, so there is nothing to recover and
    // `into_conn` must REFUSE — loudly, not by handing back a phantom.
    let mut qr = "SELECT 1".run(&mut conn).await.expect("borrowed run");
    let stream = qr
        .stream::<Row>()
        .await
        .expect("borrowed stream setup")
        .expect("result set");
    let err = stream
        .into_conn()
        .await
        .expect_err("a borrowed stream must refuse into_conn");
    assert!(
        err.to_string().contains("does not own"),
        "[{label}] the refusal names the cause, got: {err}"
    );
    drop(qr);

    // And the borrowed connection we still hold is untouched by the refusal.
    let id_after = session_id(&mut conn).await;
    assert_eq!(
        id_before, id_after,
        "[{label}] the caller's conn survives the refusal"
    );
    conn.disconnect().await.expect("clean disconnect");
    println!("[{label}] borrowed-route refusal PASSED");
}

async fn run_all(url: &str, label: &str) {
    full_drain_then_recover(url, label).await;
    partial_consume_then_recover(url, label).await;
    no_result_set_recovers_via_query_result(url, label).await;
    borrowed_stream_refuses_and_conn_survives(url, label).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_stream_recovery_gate() {
    let Ok(url) = std::env::var("FERRO_TEST_MYSQL_URL") else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL unset (mysql_stream_recovery_gate)");
        return;
    };
    run_all(&url, "MYSQL").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_stream_recovery_gate() {
    let Ok(url) = std::env::var("FERRO_TEST_MARIADB_URL") else {
        eprintln!("skip: FERRO_TEST_MARIADB_URL unset (mariadb_stream_recovery_gate)");
        return;
    };
    run_all(&url, "MARIADB").await;
}
