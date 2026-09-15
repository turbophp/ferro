//! C3-3b: the pin authority, hygiene reset, and the affected count.

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::backend::{ResetProfile, TxStatus};

fn backend_on(dir: &tempfile::TempDir, name: &str) -> SqliteBackend {
    SqliteBackend::new(format!("sqlite://{}", dir.path().join(name).display()))
}

/// `tx_status` follows a transaction that the ENGINE opened and closed.
#[tokio::test(flavor = "multi_thread")]
async fn tx_status_follows_begin_and_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "tx.db");
    let mut conn = backend.connect().await.expect("connect");

    assert_eq!(backend.tx_status(&conn), TxStatus::Idle, "fresh connection");
    backend
        .simple_query(&mut conn, "BEGIN IMMEDIATE")
        .await
        .expect("begin");
    assert_eq!(backend.tx_status(&conn), TxStatus::InTx);
    backend
        .simple_query(&mut conn, "COMMIT")
        .await
        .expect("commit");
    assert_eq!(backend.tx_status(&conn), TxStatus::Idle);
}

/// **THE LOAD-BEARING TEST: `p6`'s hazard, through the real backend.**
///
/// SQLite ends a transaction BY ITSELF when an `ON CONFLICT ROLLBACK` constraint is violated, and
/// nothing in the statement text says so. A `tx_status` that cached the engine's own BEGIN would
/// still report `InTx` here and the pool would hold a pin for a transaction that no longer exists.
///
/// The CONTROL is the second half: the identical duplicate insert against a PLAIN unique constraint
/// (SQLite's default `ON CONFLICT ABORT`) fails only the statement and leaves the transaction open,
/// so the same signal still reports `InTx`. Without it, the first half would be equally consistent
/// with "any error ends a transaction".
#[tokio::test(flavor = "multi_thread")]
async fn tx_status_is_a_live_read_not_a_cached_flag() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "live.db");
    let mut conn = backend.connect().await.expect("connect");

    backend
        .simple_query(
            &mut conn,
            "CREATE TABLE auto(v INTEGER UNIQUE ON CONFLICT ROLLBACK);
             CREATE TABLE abort(v INTEGER UNIQUE);",
        )
        .await
        .expect("seed");

    // (a) SQLite rolls the transaction back underneath the engine.
    backend
        .simple_query(&mut conn, "BEGIN IMMEDIATE")
        .await
        .expect("begin");
    backend
        .simple_query(&mut conn, "INSERT INTO auto VALUES (1)")
        .await
        .expect("first insert");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::InTx,
        "transaction is open"
    );

    backend
        .simple_query(&mut conn, "INSERT INTO auto VALUES (1)")
        .await
        .expect_err("duplicate violates the constraint");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::Idle,
        "THE LOAD-BEARING ASSERTION: SQLite rolled the transaction back by itself and the LIVE \
         signal reports it. A cached flag would still say InTx and the pool would pin a \
         transaction that no longer exists."
    );

    // (b) CONTROL: an ordinary constraint failure leaves the transaction OPEN.
    backend
        .simple_query(&mut conn, "BEGIN IMMEDIATE")
        .await
        .expect("begin again");
    backend
        .simple_query(&mut conn, "INSERT INTO abort VALUES (1)")
        .await
        .expect("first insert");
    backend
        .simple_query(&mut conn, "INSERT INTO abort VALUES (1)")
        .await
        .expect_err("duplicate violates the constraint");
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::InTx,
        "THE CONTROL: a plain constraint failure ends the STATEMENT, not the transaction, so the \
         same signal still reports in-transaction. The difference between (a) and (b) is SQLite's \
         own conflict resolution, which is why the signal must be read rather than inferred."
    );
}

/// A statement ERROR keeps the handle, so the signal stays readable.
///
/// Renamed on review: this was written as "a lost handle reports Failed" and then asserted `Idle`,
/// because a failing statement does not lose the handle — only a panicking blocking task does, and
/// that path is private. The `Failed` contract is tested where it is reachable, in `conn.rs`'s unit
/// tests. A test whose name and body disagree is worse than no test, because the name is what a
/// later reader greps for.
#[tokio::test(flavor = "multi_thread")]
async fn a_statement_error_keeps_the_handle_readable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "lost.db");
    let mut conn = backend.connect().await.expect("connect");
    // A panicking blocking call is the only way to lose the handle (C3-3a).
    let _ = backend
        .simple_query(&mut conn, "NOT VALID SQL AT ALL")
        .await;
    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::Idle,
        "a mere statement ERROR keeps the handle, so the signal is still readable"
    );
}

/// The affected count is right where BOTH of SQLite's counters are wrong on their own.
#[tokio::test(flavor = "multi_thread")]
async fn simple_query_affected_is_neither_stale_nor_inflated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "affected.db");
    let mut conn = backend.connect().await.expect("connect");

    backend
        .simple_query(
            &mut conn,
            "CREATE TABLE t(v INTEGER);
             CREATE TABLE log(v INTEGER);
             CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO log VALUES (1); END;",
        )
        .await
        .expect("seed");

    assert_eq!(
        backend
            .simple_query(&mut conn, "INSERT INTO t VALUES (1),(2),(3)")
            .await
            .expect("insert"),
        3,
        "three rows inserted (the trigger's three log rows are NOT this statement's)"
    );

    assert_eq!(
        backend
            .simple_query(&mut conn, "BEGIN IMMEDIATE")
            .await
            .expect("begin"),
        0,
        "NOT STALE: a BEGIN changes no rows. `changes()` alone would still report 3 here, and this \
         is the exact path the pin hook runs BEGIN/COMMIT/ROLLBACK through."
    );
    assert_eq!(
        backend
            .simple_query(&mut conn, "COMMIT")
            .await
            .expect("commit"),
        0,
        "and a COMMIT likewise"
    );

    assert_eq!(
        backend
            .simple_query(&mut conn, "INSERT INTO t VALUES (9)")
            .await
            .expect("triggered insert"),
        1,
        "NOT INFLATED: one row, though the trigger wrote a second. A total_changes() delta would \
         report 2 — PostgreSQL and MySQL both report the statement's own rows here."
    );

    assert_eq!(
        backend
            .simple_query(&mut conn, "UPDATE t SET v=v WHERE v > 1000")
            .await
            .expect("no-op update"),
        0,
        "a statement that matches nothing affects nothing"
    );
}

/// Reset undoes every kind of state a pooled SQLite connection can carry into the next tenant.
#[tokio::test(flavor = "multi_thread")]
async fn reset_rolls_back_disarms_detaches_and_drops_temp() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "reset.db");
    let mut conn = backend.connect().await.expect("connect");
    let side = dir.path().join("side.db");

    backend
        .simple_query(&mut conn, "CREATE TABLE t(v INTEGER)")
        .await
        .expect("seed");

    // Dirty the connection every way it can be dirtied.
    backend
        .simple_query(&mut conn, "BEGIN IMMEDIATE")
        .await
        .expect("open tx");
    backend
        .simple_query(&mut conn, "CREATE TEMP TABLE leak(v INTEGER)")
        .await
        .expect("temp table");
    backend
        .simple_query(
            &mut conn,
            &format!("ATTACH DATABASE '{}' AS side", side.display()),
        )
        .await
        .expect("attach");
    backend.set_query_only(&mut conn, true).await.expect("arm");

    backend
        .reset(&mut conn, ResetProfile::Full)
        .await
        .expect("reset");

    assert_eq!(
        backend.tx_status(&conn),
        TxStatus::Idle,
        "the open transaction is rolled back"
    );
    assert!(!conn.is_query_only(), "query_only is disarmed");

    let attached: i64 = conn
        .driver()
        .expect("live")
        .query_row(
            "SELECT count(*) FROM pragma_database_list WHERE name NOT IN ('main','temp')",
            [],
            |r| r.get(0),
        )
        .expect("count attached");
    assert_eq!(attached, 0, "the attached database is detached");

    let temp_objects: i64 = conn
        .driver()
        .expect("live")
        .query_row("SELECT count(*) FROM temp.sqlite_master", [], |r| r.get(0))
        .expect("count temp");
    assert_eq!(
        temp_objects, 0,
        "the temp table is gone — it would otherwise outlive the checkout that made it, since \
         SQLite keeps temp objects until the CONNECTION closes"
    );

    // And the connection still works.
    backend
        .simple_query(&mut conn, "INSERT INTO t VALUES (1)")
        .await
        .expect("usable after reset");
}

/// Reset on an already-clean connection is a no-op that succeeds — the recycle path runs it on
/// every reused connection, so it must not error when there is nothing to undo. Asserted on BOTH
/// profiles, since they stopped being the same thing: `Full` closes and reopens the connection
/// (§22.2 (bm)) and `Targeted` runs the explicit list, and a repeated no-op has to hold for each.
#[tokio::test(flavor = "multi_thread")]
async fn reset_on_a_clean_connection_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "clean.db");
    let mut conn = backend.connect().await.expect("connect");

    for profile in [ResetProfile::Full, ResetProfile::Targeted] {
        for _ in 0..3 {
            backend
                .reset(&mut conn, profile)
                .await
                .expect("reset on a clean conn must not error (ROLLBACK with no open tx would)");
        }
        assert_eq!(backend.tx_status(&conn), TxStatus::Idle);
    }

    // C3-3b answered `Some(Full)` here on the argument that the two profiles were interchangeable
    // "because SQLite's reset is in-process and destroys no prepared statements". That stopped
    // being true when `Full` became a close-and-reopen, measured at ~250 µs against §16's
    // p50 < 60 µs boundary target — so the clean path takes the cheap profile, and it is safe to
    // because `PRAGMA`/`ATTACH` (the only statements that leave connection-scoped state) taint
    // unconditionally. See §22.2 (bm).
    assert_eq!(backend.clean_reset_profile(), Some(ResetProfile::Targeted));
}
