//! **C4b-2b: §13's pinned gauge, pin-duration and checkout-duration histograms, and queue depth —
//! counted EXACTLY through a real `Pool` on the fake backend.**
//!
//! Every way a pin can END gets its own test (commit, rollback, the `Checkout` dropped while still
//! pinned), because a balance kept at "the sites that unpin" drifts the first time one is missed —
//! and every way a WAIT can end gets one too (a permit, a timeout, the caller giving up).

use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::TxId;
use ferro_pool::pool::Pool;

fn pool_of(max_size: usize, checkout_timeout: Duration) -> Pool<FakeBackend> {
    Pool::new(
        FakeBackend::new(),
        PoolConfig {
            max_size,
            checkout_timeout,
            ..Default::default()
        },
    )
}

fn pin_count(pool: &Pool<FakeBackend>) -> u64 {
    pool.pin_time().duration().count()
}

#[tokio::test]
async fn a_committed_pin_ends_once() {
    let pool = pool_of(1, Duration::from_secs(5));
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    assert_eq!(pool.gauges().pinned, 1);
    c.commit_tx().await.expect("commit");
    assert_eq!(pool.gauges().pinned, 0);
    assert_eq!(pin_count(&pool), 1);
    drop(c);
    assert_eq!(
        pin_count(&pool),
        1,
        "dropping an UNPINNED checkout must not observe again"
    );
}

#[tokio::test]
async fn a_rolled_back_pin_ends_once() {
    let pool = pool_of(1, Duration::from_secs(5));
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    c.rollback_tx().await.expect("rollback");
    drop(c);
    assert_eq!(pool.gauges().pinned, 0);
    assert_eq!(pin_count(&pool), 1);
}

/// The path no explicit unpin site covers: the transaction is abandoned and the `Checkout` dropped.
#[tokio::test]
async fn a_pin_dropped_without_commit_or_rollback_still_ends_once() {
    let pool = pool_of(1, Duration::from_secs(5));
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    assert_eq!(pool.gauges().pinned, 1);
    drop(c);
    assert_eq!(
        pool.gauges().pinned,
        0,
        "a dropped pinned checkout must release its pin"
    );
    assert_eq!(pin_count(&pool), 1);
}

/// Re-pinning an already-pinned checkout is still ONE pin of one connection.
#[tokio::test]
async fn a_repin_counts_once() {
    let pool = pool_of(1, Duration::from_secs(5));
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    c.begin_tx(TxId(2)).await.expect("re-begin");
    assert_eq!(pool.gauges().pinned, 1);
    drop(c);
    assert_eq!(pool.gauges().pinned, 0);
    assert_eq!(pin_count(&pool), 1);
}

/// Every checkout the pool hands out is observed once — the fresh dial AND the recycle.
#[tokio::test]
async fn every_checkout_is_observed_once_on_both_exits() {
    let pool = pool_of(1, Duration::from_secs(5));
    let c = pool.checkout().await.expect("fresh dial");
    drop(c);
    let c = pool.checkout().await.expect("recycle");
    drop(c);
    assert_eq!(pool.checkout_duration().count(), 2);
}

async fn until(pool: &Pool<FakeBackend>, waiting: usize) {
    for _ in 0..200 {
        if pool.gauges().waiting == waiting {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!(
        "waiting never reached {waiting} (is {})",
        pool.gauges().waiting
    );
}

/// A wait that ends in a permit.
#[tokio::test]
async fn a_wait_that_gets_a_permit_leaves_the_queue() {
    let pool = pool_of(1, Duration::from_secs(5));
    let holder = pool.checkout().await.expect("holder");
    let p2 = pool.clone();
    let waiter = tokio::spawn(async move { p2.checkout().await.map(drop) });
    until(&pool, 1).await;
    drop(holder);
    waiter
        .await
        .expect("join")
        .expect("the waiter gets the permit");
    assert_eq!(pool.gauges().waiting, 0);
}

/// A wait that ends in `checkout_timeout`.
#[tokio::test]
async fn a_wait_that_times_out_leaves_the_queue() {
    let pool = pool_of(1, Duration::from_millis(100));
    let _holder = pool.checkout().await.expect("holder");
    let r = pool.checkout().await;
    assert!(r.is_err(), "the second checkout must time out");
    assert_eq!(pool.gauges().waiting, 0);
}

/// A wait the CALLER abandons (a CANCEL, a session teardown) — only `Drop` sees this one.
#[tokio::test]
async fn a_wait_the_caller_abandons_leaves_the_queue() {
    let pool = pool_of(1, Duration::from_secs(30));
    let _holder = pool.checkout().await.expect("holder");
    let p2 = pool.clone();
    let waiter = tokio::spawn(async move { p2.checkout().await.map(drop) });
    until(&pool, 1).await;
    waiter.abort();
    let _ = waiter.await;
    until(&pool, 0).await;
}

/// A BEGIN that FAILS starts no pin. This is the D13 `BEGIN IMMEDIATE` → `SQLITE_BUSY` path:
/// counting it would show a phantom pinned connection and record a phantom duration. The first
/// version survived the pin moving outside `if r.is_ok()` (review finding F3).
#[tokio::test]
async fn a_failed_begin_starts_no_pin() {
    let pool = pool_of(1, Duration::from_secs(5));
    let mut c = pool.checkout().await.expect("checkout");
    c.conn_mut().arm_fail_next_simple_query();
    c.begin_tx(TxId(1))
        .await
        .expect_err("the armed BEGIN fails");
    assert_eq!(pool.gauges().pinned, 0, "a failed BEGIN must not pin");
    drop(c);
    assert_eq!(pool.gauges().pinned, 0);
    assert_eq!(
        pin_count(&pool),
        0,
        "a pin that never started has no duration"
    );
}

/// The VALUE observed, not just the count: the checkout histogram's sum is exactly the `queue_us`
/// each checkout reported — the same number the EXEC reply carries. Observing a constant passed
/// every count-only test (review finding F2). One checkout is made to WAIT a known time behind
/// another, so the reported total is far from zero and a constant cannot match it by accident.
#[tokio::test]
async fn checkout_latency_sums_the_queue_us_each_checkout_reported() {
    let pool = std::sync::Arc::new(pool_of(1, Duration::from_secs(5)));
    let held = pool.checkout().await.expect("fresh dial");
    let mut reported = held.stats().queue_us;

    let waiter = {
        let pool = std::sync::Arc::clone(&pool);
        tokio::spawn(async move { pool.checkout().await.expect("recycle after a wait") })
    };
    until(&pool, 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(held);
    let waited = waiter.await.expect("join");
    reported += waited.stats().queue_us;
    drop(waited);

    let snap = pool.checkout_duration().snapshot();
    assert_eq!(snap.count, 2);
    assert!(
        reported >= 20_000,
        "the second checkout waited 20 ms: {reported} µs"
    );
    assert_eq!(snap.sum_us, reported);
}

/// The VALUE observed for a pin: a pin held across a known sleep records at least that long.
#[tokio::test]
async fn pin_duration_records_how_long_the_pin_lasted() {
    let pool = pool_of(1, Duration::from_secs(5));
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    tokio::time::sleep(Duration::from_millis(30)).await;
    c.commit_tx().await.expect("commit");
    let snap = pool.pin_time().duration().snapshot();
    assert_eq!(snap.count, 1);
    assert!(
        snap.sum_us >= 30_000,
        "a pin held for 30 ms recorded {} µs",
        snap.sum_us
    );
}
