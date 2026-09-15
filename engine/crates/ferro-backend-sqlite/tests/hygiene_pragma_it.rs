//! **The cross-tenant PRAGMA leak, reproduced at pool level and then closed.**
//!
//! C3-3b built the SQLite hygiene reset as an explicit four-item list, reasoning from what the
//! ENGINE leaves on a connection: an open transaction, its own `query_only` arming, ATTACHed
//! databases, temp objects. That reasoning is complete for the engine and silent about the tenant.
//!
//! A tenant can issue `PRAGMA anything = value`, and SQLite's pragmas are CONNECTION-scoped, so
//! every one of them outlives the checkout. Probed through a real pool, one pragma per tenant pair,
//! seventeen of twenty-three survived a recycle — among them `foreign_keys=OFF` (which disarms the
//! integrity guarantee `open_configured` declares, for every tenant afterwards),
//! `writable_schema=1` (which lets the next tenant `DELETE FROM sqlite_master`), `trusted_schema=0`,
//! `ignore_check_constraints=1`, `read_uncommitted=1`, `synchronous=0`, and `busy_timeout`, which
//! is the pool's own checkout bound.
//!
//! The fix is not a longer list. SQLite has around sixty pragmas and gains more each release, so a
//! hand-kept list rots silently — the same failure C3-6a found in `foreign_keys` resting on a build
//! flag nobody had decided. `ResetProfile::Full` now closes the connection and opens a fresh one,
//! which is what `COM_RESET_CONNECTION` is for MySQL (M1-S6) and is complete by construction.
//!
//! These run at POOL level rather than through `ferrod`, because what has to be exercised is the
//! taint→profile→reset chain and which connection serves which checkout — both of which a daemon
//! test hands to a handshake instead (the C3-4 lesson).

use std::time::Duration;

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::backend::ResetProfile;
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;
use ferro_proto::value::Value;

const BUSY_TIMEOUT_MS: u64 = 5_000;

/// `max_size: 1` is what makes these tests mean anything: there is exactly one connection, so
/// "the next tenant" is necessarily the same physical connection recycled, and a pass cannot be an
/// artifact of having been handed a different one.
fn pool_on(path: &std::path::Path) -> Pool<SqliteBackend> {
    let config = PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_millis(BUSY_TIMEOUT_MS),
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

async fn scalar_i64(pool: &Pool<SqliteBackend>, sql: &str) -> i64 {
    let mut co = pool.checkout().await.expect("checkout");
    let r = co
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    match r.rows.first().and_then(|row| row.first()) {
        Some(Value::I64(n)) => *n,
        other => panic!("{sql} returned {other:?}, expected an I64"),
    }
}

/// **THE LEAK. Tenant 1 disarms foreign keys; tenant 2 must not inherit it.**
///
/// `foreign_keys` is the sharpest of the seventeen because C3-6a made it an engine GUARANTEE: the
/// backend sets and reads it back at dial precisely so an integrity promise does not rest on
/// something nobody decided. A tenant able to switch it off for everyone afterwards defeats that
/// entirely, and silently — the next tenant's orphan row is simply stored.
///
/// MUTATION PROVEN: restore `reset` to the four-item list for both profiles (i.e. delete the `Full`
/// arm) and this fails, with `foreign_keys` reading 0 and the orphan INSERT succeeding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_cannot_disarm_foreign_keys_for_the_next_tenant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("fk.db"));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .await
            .expect("fixture");
        co.exec("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id))")
            .await
            .expect("fixture");
        // The tenant's own statement, through the GUARDED path so the assist lexer sees it.
        co.exec("PRAGMA foreign_keys = OFF").await.expect("pragma");
    }

    assert_eq!(
        scalar_i64(&pool, "PRAGMA foreign_keys").await,
        1,
        "the previous tenant's `PRAGMA foreign_keys = OFF` reached the next tenant"
    );

    // The consequence, not just the flag: with enforcement off this row is stored silently.
    let mut co = pool.checkout().await.expect("checkout");
    let err = co
        .exec("INSERT INTO child (id, p) VALUES (1, 999)")
        .await
        .expect_err("an orphan row must still be refused after a recycle");
    assert!(
        format!("{err:?}").contains("FOREIGN KEY constraint failed"),
        "expected a foreign-key violation after the recycle, got {err:?}"
    );
}

/// **`writable_schema`, because its consequence is schema destruction rather than a wrong answer.**
///
/// With it left at 1, the next tenant can `DELETE FROM sqlite_master` — dropping every table, index
/// and trigger — without ever having asked for the privilege. SQLite guards that statement behind
/// the pragma for exactly this reason, and a pool that carries the pragma forward removes the guard
/// for a tenant who never touched it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_cannot_leave_sqlite_master_writable_for_the_next_tenant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("ws.db"));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("CREATE TABLE keepme (id INTEGER)")
            .await
            .expect("fixture");
        co.exec("PRAGMA writable_schema = 1").await.expect("pragma");
    }

    // Scoped, because `max_size` is 1: holding this checkout across the `scalar_i64` below would
    // deadlock the pool and report as `checkout: Timeout` rather than as a failed assertion.
    {
        let mut co = pool.checkout().await.expect("checkout");
        let err = co
            .exec("DELETE FROM sqlite_master WHERE type = 'table'")
            .await
            .expect_err("sqlite_master must be read-only for a tenant that did not arm the pragma");
        assert!(
            format!("{err:?}").contains("may not be modified"),
            "expected SQLite's own refusal, got {err:?}"
        );
    }

    // And the table is still there — the refusal happened before anything was removed.
    assert_eq!(
        scalar_i64(
            &pool,
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='keepme'"
        )
        .await,
        1,
    );
}

/// **The `Full` profile REOPENS; it does not scrub.** This is the test that tells the two apart.
///
/// `last_insert_rowid()` is per-connection and sticky — it keeps reporting the last rowid this
/// connection inserted, across any number of other statements (§22.2 (bf)). So it is an identity
/// probe: a connection that was merely pragma-scrubbed still answers 1, while a connection that was
/// closed and reopened answers 0.
///
/// Without this, an implementation that enumerated a few pragmas would pass every other test in the
/// file while leaving the other fifty-odd carried forward. The claim being made is "everything a
/// tenant could set is gone", and only the reopen supports it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_full_profile_opens_a_new_connection_rather_than_scrubbing_the_old_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("reopen.db"));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("fixture");
        co.exec("INSERT INTO t (id) VALUES (7)")
            .await
            .expect("insert");
        // Taint, so this checkout returns on the `Full` arm.
        co.exec("PRAGMA recursive_triggers = ON")
            .await
            .expect("pragma");
    }

    assert_eq!(
        scalar_i64(&pool, "SELECT last_insert_rowid()").await,
        0,
        "the connection still remembers the previous tenant's insert, so it was scrubbed rather \
         than reopened — and a scrub only clears the pragmas someone thought to list"
    );
}

/// **The declared setup is RE-APPLIED, which an enumeration could not do.**
///
/// `busy_timeout` is the pool's own `checkout_timeout` (C3-3e): the two bound the same wait from
/// opposite ends, and a tenant lowering it would make the pool abandon contention it was configured
/// to wait out. Clearing it is not enough — there is no "default" to clear it to that the backend
/// knows without the pool's value — so the fix has to go back through `open_configured`, which is
/// what a reopen does by construction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_reset_re_arms_the_pools_own_busy_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("busy.db"));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("PRAGMA busy_timeout = 17").await.expect("pragma");
    }

    assert_eq!(
        scalar_i64(&pool, "PRAGMA busy_timeout").await,
        BUSY_TIMEOUT_MS as i64,
        "the pool's configured busy timeout must be back in force, not the tenant's"
    );
}

/// **A CLEAN recycle is NOT reopened — the two profiles really differ.**
///
/// If `Targeted` quietly did a reopen too, `clean_reset_profile`'s whole argument (that the cheap
/// profile is safe because the expensive one is reserved for tainted connections) would be untested
/// and the ~250 µs cost would be paid on every checkout. The same sticky `last_insert_rowid()`
/// probe answers it from the other side.
///
/// The second half is the regression guard C3-3b earned: `CREATE` is safe-listed on this dialect,
/// so temp DDL does NOT taint, which makes `Targeted` the only profile that will ever clear a temp
/// table. Narrowing the clean profile must not have dropped that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_recycle_keeps_the_connection_and_still_clears_temp_objects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("clean.db"));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .expect("fixture");
        co.exec("INSERT INTO t (id) VALUES (7)")
            .await
            .expect("insert");
        co.exec("CREATE TEMP TABLE tt (id INTEGER)")
            .await
            .expect("temp");
        // NO pragma and no ATTACH: this checkout is clean, so it returns on the `Targeted` arm.
    }

    assert_eq!(
        scalar_i64(&pool, "SELECT last_insert_rowid()").await,
        7,
        "a clean recycle must keep the connection; reopening every checkout would pay the Full \
         profile's cost on the common path"
    );
    assert_eq!(
        scalar_i64(
            &pool,
            "SELECT count(*) FROM temp.sqlite_master WHERE name = 'tt'"
        )
        .await,
        0,
        "the previous tenant's temp table survived a clean recycle",
    );
}

/// The profile choice itself, asserted because the tests above observe it only through behaviour.
#[test]
fn the_clean_profile_is_targeted() {
    let backend = SqliteBackend::new("sqlite:///tmp/unused-by-this-test.db");
    assert_eq!(
        backend.clean_reset_profile(),
        Some(ResetProfile::Targeted),
        "a non-tainted recycle must not pay for a reopen"
    );
}

/// **The reopen must clear the TRACKED `query_only` flag, and the consequence if it does not is a
/// safety hole rather than an inefficiency.**
///
/// `apply_readonly` short-circuits when its tracked flag already equals the request (C3-4). A
/// reopen replaces the handle with one whose `query_only` is physically OFF, so a flag left reading
/// `true` would make the very next declared-readonly checkout short-circuit and arm NOTHING — and a
/// lying `readonly` declaration is the one class charter rule 3 forbids the engine resolving
/// (§22.2 (bi)). The failure is silent: the write simply succeeds.
///
/// MUTATION PROVEN: delete `conn.query_only = false` from `reopen` and this fails, with the write
/// going through on a checkout that declared itself read-only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reopen_clears_the_tracked_readonly_flag() {
    /// `SQLITE_READONLY`, spelled out — `rusqlite`'s `ErrorCode` discriminants are declaration
    /// order, not SQLite's codes (the C3-1 trap).
    const SQLITE_READONLY: i32 = 8;

    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("roflag.db"));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("CREATE TABLE t (id INTEGER)")
            .await
            .expect("fixture");
    }
    {
        // Tenant 1 DECLARES readonly (so the flag is armed true) and also taints, so its checkout
        // returns on the `Full` arm.
        let mut co = pool.checkout_declared(true).await.expect("checkout");
        co.exec("PRAGMA recursive_triggers = ON")
            .await
            .expect("pragma");
    }

    // Tenant 2 declares readonly on the freshly reopened connection.
    let mut co = pool.checkout_declared(true).await.expect("checkout");
    let err = co
        .exec("INSERT INTO t (id) VALUES (1)")
        .await
        .expect_err("a declared-readonly checkout must still be enforced after a Full reset");
    assert_eq!(
        match &err {
            ferro_pool::error::PoolError::Sql { errno, .. } => *errno,
            _ => None,
        },
        Some(SQLITE_READONLY),
        "expected SQLITE_READONLY; the tracked flag survived the reopen and short-circuited the \
         arming: {err:?}"
    );
}
