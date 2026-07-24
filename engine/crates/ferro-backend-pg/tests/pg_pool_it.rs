//! Live `ferro-backend-pg` integration tests (S4 Task 5) against a real Postgres.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset, so `cargo test --workspace`
//! stays green offline (S2 convention). Point it at the S2 Dockerized Postgres:
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg
//! ```

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::{Checkout, Pool};

/// Returns the test DSN, or `None` (after printing a skip notice) if `FERRO_TEST_PG_URL` is
/// unset. Every test below returns immediately when this is `None` — that early return IS the
/// skip.
fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
    }
}

/// Runs `sql` (a single-row, single-column query) on `co`'s connection and returns the `i32`
/// result. Goes straight at the raw `tokio_postgres::Client` (`PgConn::client` is `pub` for
/// exactly this) since the pool-internal `exec`/`simple_query` surface only reports
/// success/failure, never row data.
async fn query_i32(co: &mut Checkout<PgBackend>, sql: &str) -> i32 {
    let row = co
        .conn_mut()
        .client
        .query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("query {sql:?} failed: {e}"));
    row.get(0)
}

async fn backend_pid(co: &mut Checkout<PgBackend>) -> i32 {
    query_i32(co, "SELECT pg_backend_pid()").await
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_checkout_select1_release() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(2));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        let one = query_i32(&mut co, "SELECT 1").await;
        assert_eq!(one, 1);
        backend_pid(&mut co).await
        // `co` drops at the end of this block -> released back to the pool.
    };

    let pid2 = {
        let mut co = pool.checkout().await.expect("checkout again");
        backend_pid(&mut co).await
    };

    assert_eq!(
        pid1, pid2,
        "expected the released connection to be reused, not a fresh one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_tx_pins_single_backend_pid() {
    let Some(url) = test_url() else {
        return;
    };
    // v2/m1: max_size >= 2 so a concurrent checkout has somewhere else to go, proving pinning
    // actually kept it off the pinned connection (rather than there being nothing else to give).
    let pool = Pool::new(PgBackend::new(url), config(2));

    let mut a = pool.checkout().await.expect("checkout A");
    a.begin_tx(TxId(1)).await.expect("begin tx on A");
    assert_eq!(a.pin_state(), PinState::PinnedTx(TxId(1)));
    assert_eq!(a.last_pin_cause(), Some(PinCause::Tx));

    let pid_a1 = backend_pid(&mut a).await;
    let pid_a2 = backend_pid(&mut a).await;
    assert_eq!(
        pid_a1, pid_a2,
        "both statements inside the tx must stay pinned to the same backend pid"
    );

    // While A is still pinned, a concurrent checkout must land on a DIFFERENT backend pid --
    // proof that pinning kept the load off the pinned connection rather than there being no
    // other connection to hand out.
    let mut b = pool.checkout().await.expect("checkout B while A is pinned");
    let pid_b = backend_pid(&mut b).await;
    assert_ne!(
        pid_a1, pid_b,
        "a concurrent checkout must not be handed the pinned tx connection"
    );
    drop(b);

    a.commit_tx().await.expect("commit A");
    assert_eq!(a.pin_state(), PinState::Unpinned);
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_release_hygiene_leaves_conn_clean() {
    let Some(url) = test_url() else {
        return;
    };
    // max_size = 1: the only connection this pool will ever create MUST be the one reused below,
    // so the defensive-rollback assertion isn't at the mercy of which idle conn gets popped.
    let pool = Pool::new(PgBackend::new(url), config(1));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.begin_tx(TxId(42)).await.expect("begin tx");
        co.exec("CREATE TEMP TABLE ferro_s4_hygiene_probe (id int)")
            .await
            .expect("create temp table inside tx");
        // Dropped here WITHOUT commit/rollback: `tx_open` stays set on release, so the *next*
        // checkout performs the defensive ROLLBACK (v2/B1) before handing this conn out again.
        // DDL is transactional in Postgres, so rolling back also undoes the temp table create.
    }

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (defensive rollback should have run)");
    let result = co2
        .conn_mut()
        .client
        .simple_query("SELECT count(*) FROM ferro_s4_hygiene_probe")
        .await;
    assert!(
        result.is_err(),
        "temp table created inside the uncommitted tx should be gone after the defensive \
         ROLLBACK, got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_killed_backend_evicted_no_retry() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url.clone()), config(2));

    let mut a = pool.checkout().await.expect("checkout A");
    let pid_a = backend_pid(&mut a).await;

    // Deterministic kill from a SEPARATE connection (v2/M4): `pg_terminate_backend(pg_backend_pid())`
    // races its own result on the killing connection, so use a second, independent connection.
    let (killer, killer_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect killer");
    tokio::spawn(async move {
        let _ = killer_conn.await;
    });
    let terminated: bool = killer
        .query_one("SELECT pg_terminate_backend($1)", &[&pid_a])
        .await
        .expect("pg_terminate_backend")
        .get(0);
    assert!(terminated, "pg_terminate_backend should report success");

    // Detect death via a ROUND TRIP on A's next use (v2/M4), not via `is_closed()` timing: the
    // backend needs a moment to actually die once the terminate signal lands, so retry briefly.
    let mut last_err = None;
    let mut detected = false;
    for _ in 0..100 {
        match a.exec("SELECT 1").await {
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(e) => {
                last_err = Some(e);
                detected = true;
                break;
            }
        }
    }
    assert!(
        detected,
        "expected the round trip to eventually detect the killed backend"
    );
    assert_eq!(
        last_err,
        Some(PoolError::ConnectionLost),
        "a killed backend must surface as ConnectionLost (Retryable), never a silent success"
    );

    // Charter rule 3 (no transparent retry): the loop above made exactly one *user* statement
    // attempt per iteration and stopped at the first error -- there is no hidden retry loop
    // inside `exec`/`checkout` that could have silently re-issued "SELECT 1" on a fresh
    // connection in place of the failed one.

    drop(a);
    // Give the connection-driver task a moment to flip its `closed` flag, so the next checkout's
    // eviction check isn't itself racing that async update.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut b = pool
        .checkout()
        .await
        .expect("checkout after the kill should reconnect fresh");
    let pid_b = backend_pid(&mut b).await;
    assert_ne!(
        pid_a, pid_b,
        "the pool must have evicted the dead connection and reconnected to a fresh backend pid"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_max_lifetime_recycles_live() {
    let Some(url) = test_url() else {
        return;
    };
    let mut cfg = config(1);
    cfg.max_lifetime = Duration::from_millis(50);
    let pool = Pool::new(PgBackend::new(url), cfg);

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        backend_pid(&mut co).await
    };

    tokio::time::sleep(Duration::from_millis(150)).await;

    let pid2 = {
        let mut co = pool
            .checkout()
            .await
            .expect("checkout after max_lifetime elapsed");
        backend_pid(&mut co).await
    };

    assert_ne!(
        pid1, pid2,
        "a connection past max_lifetime should be recycled (fresh pid), not reused"
    );
}
