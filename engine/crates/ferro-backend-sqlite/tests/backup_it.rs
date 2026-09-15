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
/// `SQLITE_AUTH` — what the D14 path guard's `Deny` produces.
const SQLITE_AUTH: i32 = 23;

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

/// Same pool, with D14's allowed directory WIDENED by the operator.
fn pool_allowing(path: &std::path::Path, allow: &std::path::Path) -> Pool<SqliteBackend> {
    let config = PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(60),
        reap_interval: None,
        pin_functions: Vec::new(),
        pin_on_unknown: true,
    };
    Pool::new(
        SqliteBackend::new(format!("sqlite://{}", path.display()))
            .with_busy_timeout(config.checkout_timeout)
            .with_allow_dir(allow),
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

/// **THE BOUNDARY IS CLOSED — SPEC D14, and this test was a TRIPWIRE until it was.**
///
/// C3-7a found that a tenant could write a copy of the whole database to any path the daemon can
/// write, and deliberately asserted that CAPABILITY, green, so the open decision could not be
/// forgotten. D14 answered it, so the assertion inverts: the same statements are now refused.
///
/// Refusing `VACUUM INTO` by name would have been security theatre — this shape, `ATTACH` plus an
/// ordinary `CREATE TABLE … AS SELECT` inside one transaction, produced the identical copy. Both
/// verbs reach SQLite's authorizer as the SAME `SQLITE_ATTACH` event, which is why one guard closes
/// both without parsing any SQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_cannot_copy_the_database_outside_the_allowed_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 30).await;

    // Deliberately OUTSIDE the pool's own directory, which is what makes the point.
    let elsewhere = tempfile::tempdir().expect("second tempdir");
    let side = elsewhere.path().join("copy.db");

    let mut co = pool.checkout().await.expect("checkout");
    co.begin_tx_with(TxId(7), "BEGIN IMMEDIATE")
        .await
        .expect("begin");
    let err = co
        .exec(&format!("ATTACH DATABASE '{}' AS side", side.display()))
        .await
        .expect_err("D14 must refuse a file outside the allowed directory");

    assert_eq!(
        errno_of(&err),
        Some(SQLITE_AUTH),
        "expected SQLITE_AUTH, got {err:?}"
    );
    let msg = format!("{err:?}");
    assert!(
        msg.contains("allow_dir"),
        "the refusal must say what to do about it, got {msg}"
    );
    assert!(!side.exists(), "a refused ATTACH still created the file");

    co.rollback_tx().await.expect("rollback");
}

/// The same for `VACUUM INTO`, because the whole argument for the authorizer is that ONE guard
/// covers both verbs. A denial here creates NO file at all — unlike the declared-readonly refusal
/// above, which leaves a zero-byte one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_outside_the_allowed_directory_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 10).await;

    let elsewhere = tempfile::tempdir().expect("second tempdir");
    let snap = elsewhere.path().join("snap.db");

    let mut co = pool.checkout().await.expect("checkout");
    let err = co
        .exec(&format!("VACUUM INTO '{}'", snap.display()))
        .await
        .expect_err("D14 must refuse a snapshot outside the allowed directory");

    assert_eq!(
        errno_of(&err),
        Some(SQLITE_AUTH),
        "expected SQLITE_AUTH, got {err:?}"
    );
    assert!(!snap.exists(), "a refused snapshot created a file anyway");
}

/// **The operator knob, and the reason D14 is not merely a refusal.** Pointing snapshots at a
/// backup volume is the legitimate case, and it must work — otherwise the decision would have made
/// the §7.6 feature unreachable rather than safe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operator_configured_directory_is_allowed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backups = tempfile::tempdir().expect("backup volume");
    let pool = pool_allowing(&dir.path().join("main.db"), backups.path());
    seed(&pool, 40).await;

    let snap = backups.path().join("snap.db");
    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec(&format!("VACUUM INTO '{}'", snap.display()))
            .await
            .expect("the operator widened the directory, so this must succeed");
    }
    assert_eq!(rows_in_snapshot(&snap), 40);
}

/// **Widening is not the same as opening.** A pool configured with a backup directory must still
/// refuse everywhere else — otherwise `with_allow_dir` would read as "turn the guard off".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn widening_the_directory_does_not_disable_the_guard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backups = tempfile::tempdir().expect("backup volume");
    let elsewhere = tempfile::tempdir().expect("third place");
    let pool = pool_allowing(&dir.path().join("main.db"), backups.path());
    seed(&pool, 5).await;

    let mut co = pool.checkout().await.expect("checkout");
    let err = co
        .exec(&format!(
            "VACUUM INTO '{}'",
            elsewhere.path().join("snap.db").display()
        ))
        .await
        .expect_err("a widened pool must still refuse a third directory");
    assert_eq!(errno_of(&err), Some(SQLITE_AUTH), "got {err:?}");
}

/// The `ATTACH` half of the tripwire above, WITHOUT a transaction — the control that shows the
/// pooling contract already contains the autocommit shape, and therefore that the transaction in
/// the test above is what does the work rather than incidental setup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_attachment_does_not_survive_to_the_next_statement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 5).await;

    // INSIDE the allowed directory now (D14), so this test still measures what it always did —
    // the pooling contract — rather than silently becoming a second copy of the guard test.
    let side = dir.path().join("side.db");

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

/// **`..` must not walk out, which is why the root is canonicalised once at dial.**
///
/// A guard that compared path STRINGS would pass every other test in this file and fall to this
/// one line. The check resolves the candidate's parent before comparing, so a traversal that lands
/// outside the root is refused however it is spelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dot_dot_traversal_does_not_escape_the_allowed_directory() {
    let outer = tempfile::tempdir().expect("tempdir");
    let inner = outer.path().join("db");
    std::fs::create_dir(&inner).expect("mkdir");
    let pool = pool_on(&inner.join("main.db"), 1);
    seed(&pool, 5).await;

    // Spelled relative to the allowed directory, resolving to its PARENT.
    let escape = inner.join("..").join("escape.db");
    let mut co = pool.checkout().await.expect("checkout");
    let err = co
        .exec(&format!("VACUUM INTO '{}'", escape.display()))
        .await
        .expect_err("a `..` traversal must not escape the allowed directory");

    assert_eq!(errno_of(&err), Some(SQLITE_AUTH), "got {err:?}");
    assert!(
        !outer.path().join("escape.db").exists(),
        "the traversal created a file outside the allowed directory"
    );
}

/// **The guard must survive a RECYCLE, and that is not automatic.**
///
/// §22.2 (bm) made `ResetProfile::Full` a close-and-reopen, so a tainted connection is served by a
/// connection `open_configured` built a second time. If the guard were installed only on the
/// fresh-dial path, enforcement would depend on POOL OCCUPANCY — exactly the C3-4 failure, green in
/// any test that does not recycle first.
///
/// `max_size` is 1 so "the next tenant" is necessarily the same connection recycled, and the taint
/// is a `PRAGMA` (which taints unconditionally) applied on a SUCCEEDING statement — a setup step
/// that ended in a FAILED one would silently convert this into a fresh-dial test (the C3-4 lesson).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_guard_survives_a_recycle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("main.db"), 1);
    seed(&pool, 5).await;

    // Tenant 1 taints the connection, so tenant 2 gets the FULL reset — a reopen.
    {
        let mut co = pool.checkout().await.expect("checkout 1");
        co.exec("PRAGMA cache_size = 1000")
            .await
            .expect("a succeeding statement that taints");
    }

    let elsewhere = tempfile::tempdir().expect("second tempdir");
    let snap = elsewhere.path().join("snap.db");
    let mut co = pool.checkout().await.expect("checkout 2 — recycled");
    let err = co
        .exec(&format!("VACUUM INTO '{}'", snap.display()))
        .await
        .expect_err("the guard must be installed on the reopened connection too");

    assert_eq!(errno_of(&err), Some(SQLITE_AUTH), "got {err:?}");
    assert!(!snap.exists());
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
