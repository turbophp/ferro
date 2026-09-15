//! C3-3d: the fate table, proven against errors a real SQLite produces.

use ferro_backend_sqlite::{SqliteBackend, error_map};
use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use ferro_proto::value::Value;

fn backend_on(dir: &tempfile::TempDir, name: &str) -> SqliteBackend {
    SqliteBackend::new(format!("sqlite://{}", dir.path().join(name).display()))
}

fn sql_parts(e: &PoolError) -> (u16, u8, Option<i32>, String) {
    match e {
        PoolError::Sql {
            code,
            branch,
            errno,
            message,
            sqlstate,
        } => {
            assert!(
                sqlstate.is_none(),
                "SQLite has no SQLSTATE — the mirror image of PG, which has no errno"
            );
            (*code, *branch, *errno, message.clone())
        }
        other => panic!("expected PoolError::Sql, got {other:?}"),
    }
}

/// **The extended-code constants are ASSERTED against real errors, never derived on paper** — and
/// the same test demonstrates why that matters, by showing the enum discriminant is a different
/// number from the code it is named for.
#[tokio::test(flavor = "multi_thread")]
async fn extended_codes_are_what_sqlite_actually_reports() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "codes.db");
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "PRAGMA foreign_keys=ON;
             CREATE TABLE p(id INTEGER PRIMARY KEY);
             CREATE TABLE t(
                 id INTEGER PRIMARY KEY,
                 u INTEGER UNIQUE,
                 nn INTEGER NOT NULL,
                 ck INTEGER CHECK (ck > 0),
                 fk INTEGER REFERENCES p(id));
             INSERT INTO p VALUES (1);
             INSERT INTO t VALUES (1, 1, 1, 1, 1);",
        )
        .await
        .expect("seed");

    // Each violation, with the extended code SQLite really produced and the fate it maps to.
    let cases: Vec<(&str, &str, i32, u16)> = vec![
        (
            "INSERT INTO t VALUES (1, 9, 1, 1, 1)",
            "primary key",
            1555,
            errc::UNIQUE,
        ),
        (
            "INSERT INTO t VALUES (2, 1, 1, 1, 1)",
            "unique",
            2067,
            errc::UNIQUE,
        ),
        (
            "INSERT INTO t VALUES (3, 3, NULL, 1, 1)",
            "not null",
            1299,
            errc::NOT_NULL,
        ),
        (
            "INSERT INTO t VALUES (4, 4, 1, 0, 1)",
            "check",
            275,
            errc::CHECK,
        ),
        (
            "INSERT INTO t VALUES (5, 5, 1, 1, 99)",
            "foreign key",
            787,
            errc::FOREIGN_KEY,
        ),
        (
            "SELECT * FROM no_such_table",
            "unresolvable statement",
            1,
            errc::SYNTAX,
        ),
    ];

    for (sql, label, want_extended, want_code) in cases {
        let err = backend.query(&mut conn, sql, &[]).await.expect_err(label);
        let (code, _branch, errno, _msg) = sql_parts(&err);
        assert_eq!(
            errno,
            Some(want_extended),
            "{label}: the EXTENDED code SQLite reports — asserted, not computed from primary|(N<<8)"
        );
        assert_eq!(code, want_code, "{label}: maps to the right fate cell");
    }
}

/// **The trap the constants exist to avoid**, demonstrated rather than asserted in prose: the enum
/// discriminant is NOT SQLite's code, so `ErrorCode::X as i32` compiles cleanly and lies.
#[test]
fn error_code_discriminants_are_not_sqlite_codes() {
    assert_eq!(
        rusqlite::ErrorCode::DatabaseBusy as i32,
        3,
        "declaration order"
    );
    assert_ne!(
        rusqlite::ErrorCode::DatabaseBusy as i32,
        5,
        "SQLITE_BUSY is 5 — casting the enum would silently compare against 3"
    );
    assert_eq!(rusqlite::ErrorCode::OperationInterrupted as i32, 7);
    assert_ne!(
        rusqlite::ErrorCode::OperationInterrupted as i32,
        9,
        "SQLITE_INTERRUPT is 9"
    );
}

/// `SQLITE_BUSY_SNAPSHOT` (517): Retryable, and the message names the D13 cause.
#[tokio::test(flavor = "multi_thread")]
async fn busy_snapshot_is_retryable_contention_and_says_why() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "snap.db");
    let mut a = backend.connect().await.expect("connect a");
    let mut b = backend.connect().await.expect("connect b");

    backend
        .simple_query(
            &mut b,
            "CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1);",
        )
        .await
        .expect("seed");

    // p4a's setup, through the real backend: A holds a DEFERRED read snapshot, B commits past it.
    backend
        .simple_query(&mut a, "BEGIN DEFERRED")
        .await
        .expect("a begins");
    backend
        .query(&mut a, "SELECT v FROM t", &[])
        .await
        .expect("a takes its snapshot");
    backend
        .simple_query(&mut b, "BEGIN IMMEDIATE; UPDATE t SET v=2; COMMIT;")
        .await
        .expect("b commits past a");

    let err = backend
        .query(&mut a, "UPDATE t SET v=3", &[])
        .await
        .expect_err("a cannot upgrade");
    let (code, branch, errno, msg) = sql_parts(&err);
    assert_eq!(errno, Some(517), "SQLITE_BUSY_SNAPSHOT");
    assert_eq!(
        code,
        errc::SERIALIZATION_FAILURE,
        "a write refused because another transaction committed past this snapshot IS a \
         serialization failure — the same semantic PG's 40001 carries, which this engine classifies \
         Retryable"
    );
    assert_eq!(branch, errc::SERIALIZATION_FAILURE_BRANCH);
    assert!(
        msg.contains("DECLARED itself readonly"),
        "the message names the D13 cause, because under D13 this is reachable only from a lying \
         readonly declaration and that is a client bug the developer must see: {msg}"
    );
    backend
        .simple_query(&mut a, "ROLLBACK")
        .await
        .expect("a rolls back");
}

/// `SQLITE_READONLY` (8) from an armed `query_only` connection: NonRetryable, provably not executed.
#[tokio::test(flavor = "multi_thread")]
async fn readonly_refusal_is_unsupported_and_not_retryable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "ro.db");
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(&mut conn, "CREATE TABLE t(v INTEGER)")
        .await
        .expect("seed");
    backend.set_query_only(&mut conn, true).await.expect("arm");

    let err = backend
        .query(&mut conn, "INSERT INTO t VALUES (1)", &[])
        .await
        .expect_err("a query_only connection refuses the write");
    let (code, branch, errno, _) = sql_parts(&err);
    assert_eq!(errno, Some(8), "SQLITE_READONLY");
    assert_eq!(code, errc::UNSUPPORTED);
    assert_eq!(branch, errc::UNSUPPORTED_BRANCH, "NonRetryable");
}

/// The cancel handle really interrupts, and the interrupt maps to CANCELLED so `fate.rs`'s
/// UNCHANGED `is_57014` override fires.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_interrupts_and_maps_to_cancelled() {
    use ferro_pool::backend::{Cancel, PoolBackend};

    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "cancel.db");
    let mut conn = backend.connect().await.expect("connect");

    // Grabbed BEFORE the statement starts, as the trait requires.
    let handle = PoolBackend::cancel_handle(&backend, &conn);
    let firing = tokio::spawn(async move {
        // `sqlite3_interrupt` only affects a statement ALREADY RUNNING, and `Cancel` is fire-once
        // by construction, so this gets one shot and must not take it too early (the p3 hazard).
        //
        // **The timeout below does NOT rescue a missed cancel, and that is worth knowing.** It was
        // added believing it would; the mutation that never fires the cancel HUNG anyway and had to
        // be killed. `tokio::time::timeout` drops the outer future, but the statement is running on
        // a `spawn_blocking` thread that nothing in the async world can reclaim — tokio waits for
        // blocking tasks at shutdown, so the process never exits. The timeout is kept because it
        // converts SOME failures into a message, but the real guarantee is that the interrupt lands,
        // which it does in ~0.25 s here.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        handle.cancel().await;
    });

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        backend.query(
            &mut conn,
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) \
             SELECT count(*) FROM c LIMIT 1",
            &[],
        ),
    )
    .await
    .expect(
        "THE CANCEL MUST LAND: an unbounded recursive CTE never finishes on its own, so a \
             timeout here means the interrupt did not reach a running statement",
    );
    firing.await.expect("cancel fired");

    // The statement either was interrupted (the interesting case) or finished first; only the
    // former is assertable, and the recursive CTE is unbounded so it is the one that happens.
    let err = err.expect_err("the unbounded recursive CTE must be interrupted, not completed");
    let (code, _branch, errno, _) = sql_parts(&err);
    assert_eq!(errno, Some(9), "SQLITE_INTERRUPT");
    assert_eq!(
        code,
        errc::CANCELLED,
        "CANCELLED is what makes fate.rs's UNCHANGED is_57014 override fire — the §19.3 cell, with \
         no edit to the matrix"
    );
}

/// The backend satisfies `PoolBackend`, and the streaming pair is the only `Unsupported` left.
#[tokio::test(flavor = "multi_thread")]
async fn the_trait_is_satisfied_and_only_streaming_is_unsupported() {
    use ferro_pool::backend::{Dialect, PoolBackend};

    // Generic over the trait, so this only compiles if the impl is complete.
    async fn through_the_trait<B: PoolBackend>(b: &B) -> Dialect {
        b.dialect()
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "trait.db");
    assert_eq!(through_the_trait(&backend).await, Dialect::Sqlite);

    let mut conn = PoolBackend::connect(&backend).await.expect("connect");
    let err = match PoolBackend::query_stream(&backend, &mut conn, "SELECT 1", &[]).await {
        Err(e) => e,
        // `SqliteRowStream` is uninhabited, so this arm is unconstructible — which is the point.
        Ok(_) => unreachable!("query_stream cannot succeed: its RowStream cannot be constructed"),
    };
    assert!(
        matches!(&err, PoolError::Unsupported(m) if m.contains("C3-5")),
        "a clean Unsupported naming the slice, exactly as MySQL shipped at M1-S6: {err:?}"
    );
}

/// A bind-arity mismatch never reaches SQLite, so its fate is KNOWN and it must not be
/// `ConnectionLost` — which would make it eligible for the §19.3 Indeterminate override.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_side_rejection_is_known_fate_not_connection_lost() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "arity.db");
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(&mut conn, "CREATE TABLE t(a, b)")
        .await
        .expect("seed");

    let err = backend
        .query(&mut conn, "INSERT INTO t VALUES (?, ?)", &[Value::I64(1)])
        .await
        .expect_err("one param for two placeholders");
    assert!(
        matches!(err, PoolError::Sql { .. }),
        "known fate — it provably never executed, so NOT ConnectionLost: {err:?}"
    );
}

#[test]
fn handle_fatal_is_a_short_deliberate_list() {
    // In-process, so there is no "sent it and never heard back" class: only these leave the handle
    // or the database unusable.
    for extended in [10, 11, 26, 14, 7] {
        assert!(
            matches!(
                error_map::map_extended_for_test(extended),
                PoolError::ConnectionLost
            ),
            "extended {extended} must be handle-fatal"
        );
    }
    for extended in [1, 5, 8, 9, 19, 517] {
        assert!(
            !matches!(
                error_map::map_extended_for_test(extended),
                PoolError::ConnectionLost
            ),
            "extended {extended} has KNOWN fate and must stay a Sql error"
        );
    }
}
