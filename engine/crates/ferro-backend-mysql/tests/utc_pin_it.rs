//! M1-S7 Task 5a Step 0 — LIVE proof that every pooled MySQL/MariaDB session is pinned to
//! `time_zone = '+00:00'`.
//!
//! **Why this is a correctness gate, not a preference.** MySQL stores `TIMESTAMP` in UTC but
//! **converts it into the session `time_zone` on retrieval**, and the driver hands back zone-**less**
//! `Value::Date(y, m, d, h, mi, s, us)` components — the wire carries no offset at all. SPEC §9 maps
//! MySQL `timestamp` → `TIMESTAMPTZ`, i.e. a **UTC instant**, so the `Z` suffix `mytext::
//! timestamptz_to_text` stamps on those components is truthful ONLY while the session zone is known
//! to be UTC. Under pooling an unpinned session zone would make the same column read differently
//! depending on which connection served the request.
//!
//! Two conditions are asserted, on BOTH engines:
//!   * a **fresh** connection is pinned (the `OptsBuilder::setup` list ran at connect); and
//!   * a **recycled** connection is re-pinned — `Conn::reset` (`COM_RESET_CONNECTION`) re-runs the
//!     setup list, so a user's `SET SESSION time_zone` cannot survive hygiene.
//!
//! Each test SKIPS cleanly without its env var (`FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL`).

use ferro_pool::backend::{PoolBackend, ResetProfile};
use mysql_async::prelude::Queryable;

use ferro_backend_mysql::MysqlBackend;

/// Read a single text scalar off the raw handle — a verification-only read (bypasses the pin
/// authority, which is fine for asserting server state in a test).
async fn read_text(conn: &mut ferro_backend_mysql::MysqlConn, sql: &str) -> String {
    conn.mysql
        .query_first::<String, _>(sql)
        .await
        .unwrap_or_else(|e| panic!("read `{sql}` failed: {e:?}"))
        .unwrap_or_else(|| panic!("read `{sql}` returned no row"))
}

async fn utc_pin_holds_fresh_and_recycled(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);

    // ---- a FRESH conn is UTC-pinned by the connect-time setup list -------------------------------
    let mut conn = backend.connect().await.expect("connect");
    let z = read_text(&mut conn, "SELECT @@session.time_zone").await;
    assert_eq!(z, "+00:00", "[{label}] fresh conn must be UTC-pinned");
    println!("[{label}] fresh conn @@session.time_zone = {z}");

    // The pin is what makes `NOW()` a UTC instant: cross-check it against the server's own
    // UTC_TIMESTAMP() so a mis-set zone cannot pass by coincidence.
    let now = read_text(&mut conn, "SELECT NOW()").await;
    let utc_now = read_text(&mut conn, "SELECT UTC_TIMESTAMP()").await;
    assert_eq!(
        now, utc_now,
        "[{label}] under the pin, NOW() is UTC_TIMESTAMP()"
    );

    // ---- dirty it, then force the recycle path ---------------------------------------------------
    // `COM_RESET_CONNECTION` re-runs the whole setup list, so the pin is restored by hygiene.
    backend
        .simple_query(&mut conn, "SET SESSION time_zone = '+05:30'")
        .await
        .expect("SET SESSION time_zone");
    let dirty = read_text(&mut conn, "SELECT @@session.time_zone").await;
    assert_eq!(dirty, "+05:30", "[{label}] the dirtying SET took effect");

    // A user `SET time_zone` is a real session mutation: `time_zone` is in the curated tracker list,
    // so the S6 ASSIST signal TAINTS the conn (which is what routes it to hygiene in the first place).
    assert!(
        backend.take_session_mutated(&mut conn),
        "[{label}] a user SET SESSION time_zone must taint (PinCause::SessionTracker)"
    );

    backend
        .reset(&mut conn, ResetProfile::Full)
        .await
        .expect("COM_RESET_CONNECTION");
    let z = read_text(&mut conn, "SELECT @@session.time_zone").await;
    assert_eq!(
        z, "+00:00",
        "[{label}] recycled conn must be re-pinned to UTC"
    );
    println!("[{label}] recycled conn @@session.time_zone = {z}");

    conn.mysql.disconnect().await.ok();
    println!("[{label}] utc_pin fresh+recycled PASSED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_utc_pin_holds_fresh_and_recycled() {
    let Ok(url) = std::env::var("FERRO_TEST_MYSQL_URL") else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL unset (mysql_utc_pin_holds_fresh_and_recycled)");
        return;
    };
    utc_pin_holds_fresh_and_recycled(&url, "MYSQL").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_utc_pin_holds_fresh_and_recycled() {
    let Ok(url) = std::env::var("FERRO_TEST_MARIADB_URL") else {
        eprintln!("skip: FERRO_TEST_MARIADB_URL unset (mariadb_utc_pin_holds_fresh_and_recycled)");
        return;
    };
    utc_pin_holds_fresh_and_recycled(&url, "MARIADB").await;
}
