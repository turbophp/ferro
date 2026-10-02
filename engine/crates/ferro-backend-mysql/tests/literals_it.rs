//! M2-C1g — LIVE proof that `MysqlBackend::literals_are_standard` reads the SERVER's answer.
//!
//! The bit is what a client uses to decide how to build a SQL string literal (`PDO::quote()`), so
//! the test does not trust the status flag on its own: each state is checked against TWO oracles —
//! the session's own `@@sql_mode` and the SEMANTICS of a literal containing a backslash, which is
//! the property the bit claims. Both directions are flipped on one connection (a stuck `false` or a
//! stuck `true` would each pass a one-state test), and hygiene must restore the server default.
//!
//! Each test SKIPS cleanly without its env var (`FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL`).

use ferro_pool::backend::{PoolBackend, ResetProfile};
use mysql_async::prelude::Queryable;

use ferro_backend_mysql::{MysqlBackend, MysqlConn};

/// Oracle 1: does the session's `sql_mode` contain `NO_BACKSLASH_ESCAPES`?
async fn mode_says_standard(conn: &mut MysqlConn) -> bool {
    let m: String = conn
        .driver_mut()
        .query_first("SELECT @@session.sql_mode")
        .await
        .expect("read sql_mode")
        .expect("one row");
    m.split(',').any(|f| f.eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"))
}

/// Oracle 2: the property itself. `'a\b'` is three characters when a backslash is ordinary and
/// two (`a` + the `\b` escape, a backspace) when it is an escape.
async fn backslash_is_ordinary(conn: &mut MysqlConn) -> bool {
    let n: i64 = conn
        .driver_mut()
        .query_first(r"SELECT CHAR_LENGTH('a\b')")
        .await
        .expect("read literal length")
        .expect("one row");
    assert!(n == 2 || n == 3, "unexpected length {n}");
    n == 3
}

async fn assert_state(backend: &MysqlBackend, conn: &mut MysqlConn, label: &str, step: &str) -> bool {
    // Read the bit FIRST: the oracle queries below produce their own OK packets, and the claim
    // under test is about the packet the connection already had.
    let bit = backend.literals_are_standard(conn);
    let mode = mode_says_standard(conn).await;
    let semantic = backslash_is_ordinary(conn).await;
    assert_eq!(mode, semantic, "[{label}/{step}] the two oracles disagree");
    assert_eq!(bit, Some(mode), "[{label}/{step}] the advertised bit must be the server's answer");
    println!("[{label}/{step}] literals_are_standard = {bit:?}");
    mode
}

async fn literals_bit_tracks_the_session(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");

    // ---- the FRESH session: the server's default, whatever it is -------------------------------
    let default = assert_state(&backend, &mut conn, label, "fresh").await;

    // ---- flip it ON, then OFF, on the same connection ------------------------------------------
    backend
        .simple_query(
            &mut conn,
            "SET SESSION sql_mode = CONCAT_WS(',', @@session.sql_mode, 'NO_BACKSLASH_ESCAPES')",
        )
        .await
        .expect("enable NO_BACKSLASH_ESCAPES");
    assert!(assert_state(&backend, &mut conn, label, "on").await, "[{label}] the SET took effect");
    backend
        .simple_query(
            &mut conn,
            "SET SESSION sql_mode = REPLACE(@@session.sql_mode, 'NO_BACKSLASH_ESCAPES', '')",
        )
        .await
        .expect("disable NO_BACKSLASH_ESCAPES");
    assert!(!assert_state(&backend, &mut conn, label, "off").await, "[{label}] the SET took effect");

    // ---- hygiene restores the server default (the probe reads a reset session) -----------------
    backend
        .simple_query(
            &mut conn,
            "SET SESSION sql_mode = CONCAT_WS(',', @@session.sql_mode, 'NO_BACKSLASH_ESCAPES')",
        )
        .await
        .expect("enable again");
    backend.take_session_mutated(&mut conn);
    backend
        .reset(&mut conn, ResetProfile::Full)
        .await
        .expect("COM_RESET_CONNECTION");
    assert_eq!(
        assert_state(&backend, &mut conn, label, "recycled").await,
        default,
        "[{label}] a recycled session advertises the server default again"
    );

    // ---- an ERR clears the driver's last OK packet: unknown, never a guess ---------------------
    let _ = backend
        .simple_query(&mut conn, "SELECT no_such_column FROM no_such_table")
        .await
        .expect_err("the statement must fail");
    assert_eq!(
        backend.literals_are_standard(&conn),
        None,
        "[{label}] after an ERR there is no OK packet to read"
    );

    conn.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_literals_bit_tracks_the_session() {
    let Ok(url) = std::env::var("FERRO_TEST_MYSQL_URL") else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL unset (mysql_literals_bit_tracks_the_session)");
        return;
    };
    literals_bit_tracks_the_session(&url, "MYSQL").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_literals_bit_tracks_the_session() {
    let Ok(url) = std::env::var("FERRO_TEST_MARIADB_URL") else {
        eprintln!("skip: FERRO_TEST_MARIADB_URL unset (mariadb_literals_bit_tracks_the_session)");
        return;
    };
    literals_bit_tracks_the_session(&url, "MARIADB").await;
}
