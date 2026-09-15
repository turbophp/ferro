//! **C3-4 at pool level: both checkout exits apply the declaration, and the reset disarms it.**
//!
//! `ferrod`'s `sqlite_readonly_it.rs` proves the client's declared flag reaches the backend through
//! the whole daemon path. It cannot prove one thing, and the omission is structural rather than a
//! matter of taste: the HELLO handshake's version probe checks a connection out and returns it, so
//! every request there is served by a RECYCLED connection and the fresh-dial exit of
//! `Pool::checkout_declared` is unreachable from any daemon e2e. That was MEASURED, not assumed — a
//! test asserting it stayed green with the fresh-dial arm deleted.
//!
//! So the two exits are separated here, where the pool is driven directly and which connection
//! serves a checkout is decided by this file rather than by a handshake.

use std::time::Duration;

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;

/// `SQLITE_READONLY`. Spelled out rather than taken from `rusqlite`'s `ErrorCode`, whose
/// discriminants are declaration order and not SQLite's codes at all (the C3-1 trap: `DatabaseBusy`
/// is 3, `SQLITE_BUSY` is 5).
const SQLITE_READONLY: i32 = 8;

fn pool_on(path: &std::path::Path) -> Pool<SqliteBackend> {
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
            .with_busy_timeout(config.checkout_timeout),
        config,
    )
}

/// **The fresh-dial exit — the one no daemon test can reach.**
///
/// `max_size` is 1 and this is the pool's first checkout, so the connection is necessarily dialled
/// rather than popped. If the declaration were applied only on the recycled exit, enforcement would
/// depend on pool occupancy: green in any test that warms the pool first, and absent for exactly the
/// requests a cold or saturated pool serves with a new connection.
///
/// MUTATION PROVEN: deleting the `apply_readonly` call from the fresh-dial arm fails this, and only
/// this — every other readonly test in the tree stays green, which is the point.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_freshly_dialled_connection_gets_the_declaration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("fresh.db"));

    let mut co = pool
        .checkout_declared(true)
        .await
        .expect("first checkout dials");
    let err = co
        .exec("CREATE TABLE t(id INTEGER)")
        .await
        .expect_err("a declared-readonly checkout must refuse a write");
    assert_eq!(
        errno_of(&err),
        Some(SQLITE_READONLY),
        "expected SQLITE_READONLY on a freshly dialled connection, got {err:?}"
    );
}

/// **The recycled exit, and the ordering that makes it correct.**
///
/// The arming must be applied AFTER the hygiene reset, or the reset undoes it silently. So this
/// alternates — writable, readonly, writable — on one connection (`max_size` is 1, so there is only
/// ever one).
///
/// **Step 2b exists because without it this test silently stopped testing recycling**, and that took
/// two rounds to see. Step 2 ends in a REFUSED statement, and a connection whose statement failed is
/// not the one the next checkout gets — so step 3 was served a freshly dialled connection, which is
/// read-write by construction, and passed for a reason that had nothing to do with disarming.
/// Measured: with `PRAGMA query_only=OFF` deleted from `reset`, this test was GREEN until step 2b —
/// a SUCCEEDING readonly checkout, which returns its connection to the pool intact — was added, and
/// then it failed.
///
/// **So the hygiene reset IS what disarms a genuinely recycled connection**, and the seam cannot do
/// it: `reset` clears `apply_readonly`'s tracked flag unconditionally, so after a reset the flag
/// reads "off" whether or not the pragma actually is, and `apply_readonly(conn, false)` then
/// short-circuits and issues nothing. The two are not redundant — the flag and the pragma are only
/// kept in step by that one line.
///
/// MUTATION PROVEN, three ways: deleting `apply_readonly` from the recycled arm fails step 2 (the
/// declared-readonly write succeeds); moving it ABOVE the cleanup block fails step 2 as well (the
/// reset clears an arming applied too early); and deleting `query_only=OFF` from `reset` fails
/// step 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recycled_connection_is_re_declared_each_checkout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("recycle.db"));

    // 1. Writable: creates the table and proves the fixture can write at all.
    {
        let mut co = pool.checkout_declared(false).await.expect("checkout 1");
        co.exec("CREATE TABLE t(id INTEGER)")
            .await
            .expect("an undeclared checkout writes");
    }

    // 2. Readonly on the RECYCLED connection: refused. Then a SUCCEEDING readonly statement, so
    //    the connection goes back to the pool WITHOUT a failed statement behind it — see the
    //    comment on step 3 for why that distinction turned out to be load-bearing.
    {
        let mut co = pool.checkout_declared(true).await.expect("checkout 2");
        let err = co
            .exec("INSERT INTO t VALUES (1)")
            .await
            .expect_err("a declared-readonly recycled checkout must refuse a write");
        assert_eq!(
            errno_of(&err),
            Some(SQLITE_READONLY),
            "expected SQLITE_READONLY on the recycled connection, got {err:?}"
        );
    }
    {
        let mut co = pool.checkout_declared(true).await.expect("checkout 2b");
        co.query("SELECT 1", &[]).await.expect("a readonly read");
    }

    // 3. Writable again on that SAME connection: the arming did not leak across the checkout
    //    boundary. This is the cross-tenant class, and it is the reset that closes it — `reset`
    //    disarms, `apply_readonly` re-arms, and they have to meet in that order.
    {
        let mut co = pool.checkout_declared(false).await.expect("checkout 3");
        let affected = co
            .exec("INSERT INTO t VALUES (1)")
            .await
            .expect("the next tenant writes — the previous arming must not have survived");
        assert_eq!(affected, 1);
    }
}

/// **The default arm: a backend with no connection-scoped readonly mode is unaffected.**
///
/// Not a SQLite property but the trait's, asserted where a SQLite backend is in hand: passing
/// `false` must leave the connection exactly as `checkout()` would. Without this, "additive by
/// construction" is a claim in a doc comment rather than something a test would notice breaking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undeclared_checkout_is_identical_to_a_plain_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("plain.db"));

    {
        let mut co = pool.checkout_declared(false).await.expect("declared false");
        co.exec("CREATE TABLE t(id INTEGER)").await.expect("writes");
    }
    {
        let mut co = pool.checkout().await.expect("plain checkout");
        co.exec("INSERT INTO t VALUES (1)").await.expect("writes");
    }
}

/// Pull the backend errno out of a `PoolError`. SQLite has no SQLSTATE, so the extended result code
/// in `errno` is the whole identity of the error (§22.2 (bg)).
fn errno_of(err: &ferro_pool::error::PoolError) -> Option<i32> {
    match err {
        ferro_pool::error::PoolError::Sql { errno, .. } => *errno,
        _ => None,
    }
}

/// **The case the hygiene reset actually covers: a USER-issued `PRAGMA query_only=ON`.**
///
/// This is the §7.4 blind-spot shape. `apply_readonly` tracks its own arming in a flag and
/// short-circuits when the connection is already in the requested state — so when a TENANT arms the
/// pragma itself, by running the statement rather than by declaring anything, the flag still reads
/// `false` and the next checkout's `apply_readonly(conn, false)` does nothing at all. The connection
/// would then reach the next tenant read-only, with no declaration anywhere in the system explaining
/// why their writes fail.
///
/// `reset`'s unconditional `PRAGMA query_only=OFF` is what closes it, which is why it belongs in the
/// hygiene list (C3-3b) rather than in `apply_readonly`: hygiene's job is exactly the state a tenant
/// left behind by means the engine did not mediate.
///
/// MUTATION PROVEN: deleting `PRAGMA query_only=OFF` from `SqliteBackend::reset` fails this. It also
/// fails the recycle test above, which is the correct outcome and not always what this file said —
/// see that test's note on step 2b.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_user_issued_query_only_pragma_does_not_leak_to_the_next_tenant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("user_pragma.db"));

    {
        let mut co = pool.checkout_declared(false).await.expect("checkout 1");
        co.exec("CREATE TABLE t(id INTEGER)").await.expect("writes");
        // The tenant arms it ITSELF — no declaration, so nothing in the pool knows.
        co.exec("PRAGMA query_only=ON")
            .await
            .expect("a tenant may run whatever statement it likes");
    }

    {
        let mut co = pool.checkout_declared(false).await.expect("checkout 2");
        let affected = co
            .exec("INSERT INTO t VALUES (1)")
            .await
            .expect("the next tenant must be able to write — a user-armed pragma survived hygiene");
        assert_eq!(affected, 1);
    }
}
