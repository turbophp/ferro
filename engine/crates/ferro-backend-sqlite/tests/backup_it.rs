//! **C3-7 scoping, pinned as tests: the online-backup MECHANISM already works, and the
//! properties an admin surface has to be designed around.**
//!
//! §7.6's last sentence asks for an "online backup API exposed via admin service for snapshots",
//! and the obvious reading is that the backend needs `sqlite3_backup_*` wired through a new engine
//! method. Probed first (the C3-3 discipline), that turns out to be unnecessary: **`VACUUM INTO` is
//! a plain statement, so the pool already runs it**, it is SQLite's own recommended way to take a
//! consistent snapshot of a live database, and the per-request `timeout_ms` + interrupt handle that
//! C3-3d proved is the only thing able to stop a runaway SQLite statement already bounds it.
//!
//! What C3-7 is actually missing is the ADMIN SERVICE — `ADMIN = 5` is a reserved service id in
//! `/proto` with no `[methods.admin]` table at all, and `dispatch::route(service::ADMIN, _)` answers
//! `Unsupported` with a test asserting exactly that. Adding methods there is a `/proto` change
//! (registry + golden vectors + both codecs, charter rule 2) plus an authorization question, so it
//! is split out as C3-7b rather than half-built here.
//!
//! These tests exist so that slice starts from measured ground rather than from prose. They
//! run at POOL level, not through `ferrod`, for the C3-4 reason: what matters is which connection
//! serves which checkout, and a daemon test hands that to a handshake.

use std::time::Duration;

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pin::TxId;
use ferro_pool::pool::Pool;
use ferro_proto::value::Value;

/// Spelled out rather than taken from `rusqlite`'s `ErrorCode`, whose discriminants are declaration
/// order and not SQLite's codes (the C3-1 trap: `DatabaseBusy` is 3, `SQLITE_BUSY` is 5).
const SQLITE_READONLY: i32 = 8;

fn pool_on(path: &std::path::Path, max_size: usize) -> Pool<SqliteBackend> {
    let config = PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(60),
        reap_interval: None,
        pin_functions: Vec::new(),
        pin_on_unknown: true,
    };
    Pool::new(
        SqliteBackend::new(format!("sqlite://{}", path.display()))
            .with_busy_timeout(config.checkout_timeout),
        config,
    )
}

async fn seed(pool: &Pool<SqliteBackend>, rows: i64) {
    let mut co = pool.checkout().await.expect("checkout");
    co.exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .await
        .expect("fixture");
    for i in 0..rows {
        co.exec(&format!("INSERT INTO t (v) VALUES ('row{i}')"))
            .await
            .expect("fixture insert");
    }
}

/// Read the snapshot with a driver connection of our own, NOT through the pool. A snapshot that
/// only the engine that wrote it can read would be no use to an operator, and that is precisely
/// what a backup is for.
fn rows_in_snapshot(path: &std::path::Path) -> i64 {
    let conn = rusqlite::Connection::open(path).expect("open snapshot");
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("count rows in snapshot")
}

fn errno_of(err: &ferro_pool::error::PoolError) -> Option<i32> {
    match err {
        ferro_pool::error::PoolError::Sql { errno, .. } => *errno,
        _ => None,
    }
}

/// **The mechanism, end to end: no engine code, and the result is a standalone database file.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vacuum_into_takes_a_readable_snapshot_through_the_pool() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 200).await;

    let snap = dir.path().join("snap.db");
    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec(&format!("VACUUM INTO '{}'", snap.display()))
            .await
            .expect("VACUUM INTO is an ordinary statement the pool already runs");
    }

    assert!(snap.exists(), "no snapshot file was produced");
    assert_eq!(rows_in_snapshot(&snap), 200);
}

/// **No pool coordination is needed, and that was MEASURED rather than assumed.**
///
/// The worry worth checking was that a snapshot reads pages while another tenant holds SQLite's
/// writer lock, so an admin API might have to drain or quiesce the pool first. It does not: the
/// snapshot is taken from the reader's own consistent view, so it succeeds under a concurrent open
/// write transaction AND excludes that transaction's uncommitted row.
///
/// `max_size` is 2 deliberately. With 1 the writer's still-held checkout would simply deadlock the
/// snapshot's — reported as `checkout: Timeout`, not as anything about backups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_under_a_concurrent_writer_excludes_uncommitted_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 2);
    seed(&pool, 50).await;

    // Tenant A: an OPEN write transaction, still holding its checkout.
    let mut writer = pool.checkout().await.expect("writer checkout");
    // The pin hook, not `exec` — the guarded entry deliberately refuses bare transaction control
    // ("use the TX service instead"), which is the same reason `ferrod`'s TX service composes its
    // own BEGIN. `BEGIN IMMEDIATE` is what D13 composes for an undeclared transaction.
    writer
        .begin_tx_with(TxId(1), "BEGIN IMMEDIATE")
        .await
        .expect("begin");
    writer
        .exec("INSERT INTO t (v) VALUES ('uncommitted')")
        .await
        .expect("uncommitted write");

    // Tenant B, concurrently.
    let snap = dir.path().join("snap.db");
    {
        let mut co = pool.checkout().await.expect("snapshot checkout");
        co.exec(&format!("VACUUM INTO '{}'", snap.display()))
            .await
            .expect("a snapshot must not need the writer to finish");
    }

    assert_eq!(
        rows_in_snapshot(&snap),
        50,
        "the snapshot contains the concurrent writer's UNCOMMITTED row"
    );

    writer.rollback_tx().await.expect("rollback");
}

/// **SQLite refuses an existing target, and an admin API has to be designed around that.**
///
/// It cannot offer "overwrite the previous snapshot" without deleting the file itself first — which
/// is a destructive filesystem action, so it belongs to whatever authorization C3-7b settles rather
/// than being done implicitly. Recorded here so that arrives as a known constraint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vacuum_into_refuses_an_existing_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 10).await;

    let snap = dir.path().join("snap.db");
    let mut co = pool.checkout().await.expect("checkout");
    co.exec(&format!("VACUUM INTO '{}'", snap.display()))
        .await
        .expect("first snapshot");

    let err = co
        .exec(&format!("VACUUM INTO '{}'", snap.display()))
        .await
        .expect_err("SQLite must refuse to overwrite an existing snapshot");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("already exists"),
        "expected an 'output file already exists' refusal, got {msg}"
    );
}

/// **`PRAGMA query_only` bounds writes to OTHER FILES too — which is not obvious and is load-bearing
/// for C3-7b.**
///
/// C3-4 arms `query_only` on a checkout the client DECLARED `readonly`, to turn a lying declaration
/// into an up-front `SQLITE_READONLY` instead of the unretryable deferred-upgrade class. The natural
/// assumption is that it guards the pool's own database; measured, it also refuses `VACUUM INTO`,
/// whose write goes to a path the statement names.
///
/// That matters twice over. It means an honest `readonly` declaration already prevents a tenant
/// taking a full copy of the database on that checkout — and it means the admin surface C3-7b builds
/// must NOT be served on a declared-readonly checkout, or the snapshot will simply fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_readonly_checkout_cannot_take_a_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 10).await;

    let snap = dir.path().join("snap.db");
    let mut co = pool
        .checkout_declared(true)
        .await
        .expect("declared-readonly checkout");
    let err = co
        .exec(&format!("VACUUM INTO '{}'", snap.display()))
        .await
        .expect_err("a declared-readonly checkout must not write a snapshot");

    assert_eq!(
        errno_of(&err),
        Some(SQLITE_READONLY),
        "expected SQLITE_READONLY, got {err:?}"
    );

    // A REFUSED snapshot still leaves a zero-byte file behind — SQLite creates the target before it
    // discovers the source is read-only. Asserted rather than tidied away, because the obvious next
    // worry is that the litter then blocks the retry (the refusal above is on an EXISTING target).
    // It does not: measured, a subsequent snapshot to the same path succeeds, so SQLite treats a
    // zero-byte file as an empty database it may use rather than as an existing one. Both halves
    // are here so C3-7b inherits the pair rather than one of them.
    assert!(
        snap.exists(),
        "expected the zero-byte leftover this test documents"
    );
    assert_eq!(
        std::fs::metadata(&snap).expect("stat the leftover").len(),
        0,
        "the leftover should be empty, not a partial snapshot"
    );

    drop(co);
    let mut co2 = pool.checkout().await.expect("retry checkout");
    co2.exec(&format!("VACUUM INTO '{}'", snap.display()))
        .await
        .expect("the zero-byte leftover must not block a legitimate retry");
    assert_eq!(rows_in_snapshot(&snap), 10);
}

/// **TRIPWIRE, and it is green on purpose: a tenant can ALREADY write a copy of the database to any
/// path the daemon can write, with no `VACUUM INTO` involved.**
///
/// This test asserts a capability rather than a guarantee, which is unusual and deliberate — the
/// C3-6a `foreign_keys` precedent. While scoping C3-7 it looked as though `VACUUM INTO` introduced
/// a §12/D8 hole: the engine owns the database file precisely so PHP never learns its path, yet the
/// statement lets the CLIENT name a path the engine will write. Refusing it was the obvious fix.
///
/// It would have been security theatre, and only a measurement showed that. `ATTACH` is permitted
/// by design (C3-3b's hygiene list exists to DETACH it afterwards), and inside one transaction —
/// where the pool pins every statement to one connection — `ATTACH` plus an ordinary
/// `CREATE TABLE … AS SELECT` writes exactly the same copy to exactly the same arbitrary path.
/// Outside a transaction it does not: the attachment is gone by the next checkout, because `ATTACH`
/// taints unconditionally (§22.2 (bm)). So the boundary is real but general, it predates C3-7, and
/// closing it means deciding whether a tenant statement may name a filesystem path AT ALL — a §21
/// question raised for the owner, not something to patch one verb at a time.
///
/// If that decision closes the boundary, this test goes RED and must be rewritten deliberately.
/// That is the point of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tripwire_a_tenant_can_already_copy_the_database_to_an_arbitrary_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 30).await;

    // Deliberately OUTSIDE the pool's own directory, which is what makes the point.
    let elsewhere = tempfile::tempdir().expect("second tempdir");
    let side = elsewhere.path().join("copy.db");

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.begin_tx_with(TxId(7), "BEGIN IMMEDIATE")
            .await
            .expect("begin");
        co.exec(&format!("ATTACH DATABASE '{}' AS side", side.display()))
            .await
            .expect("ATTACH is permitted by design");
        co.exec("CREATE TABLE side.copy AS SELECT * FROM t")
            .await
            .expect("an ordinary statement");
        co.commit_tx().await.expect("commit");
    }

    let conn = rusqlite::Connection::open(&side).expect("open the side database");
    let n: i64 = conn
        .query_row("SELECT count(*) FROM copy", [], |r| r.get(0))
        .expect("read the copy outside the engine");
    assert_eq!(
        n, 30,
        "the copy is readable outside the engine — this is the open boundary, not a bug in the test"
    );
}

/// The `ATTACH` half of the tripwire above, WITHOUT a transaction — the control that shows the
/// pooling contract already contains the autocommit shape, and therefore that the transaction in
/// the test above is what does the work rather than incidental setup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_attachment_does_not_survive_to_the_next_statement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 5).await;

    let elsewhere = tempfile::tempdir().expect("second tempdir");
    let side = elsewhere.path().join("side.db");

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec(&format!("ATTACH DATABASE '{}' AS side", side.display()))
            .await
            .expect("attach");
    }
    {
        let mut co = pool.checkout().await.expect("next checkout");
        let err = co
            .exec("CREATE TABLE side.copy AS SELECT * FROM t")
            .await
            .expect_err("the attachment must not survive the recycle");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("unknown database"),
            "expected 'unknown database side', got {msg}"
        );
    }
}

/// Plain `VACUUM` — no `INTO` — must keep working, because Laravel's `dropAllTables()` ends with it
/// (C3-6b) and any future refusal aimed at `VACUUM INTO` has to distinguish the two.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_vacuum_still_works() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 10).await;

    let mut co = pool.checkout().await.expect("checkout");
    co.exec("VACUUM").await.expect("plain VACUUM");

    let r = co
        .query("SELECT count(*) FROM t", &[])
        .await
        .expect("count");
    assert_eq!(
        r.rows.first().and_then(|row| row.first()),
        Some(&Value::I64(10))
    );
}
