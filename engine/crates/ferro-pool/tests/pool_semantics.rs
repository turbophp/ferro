//! S4 Task 2 tests for `Pool<B>`: checkout/release, bounded `max_size`, `queue_us` timing,
//! `checkout_timeout`, and the async-cleanup-at-checkout ("recycle-on-next-checkout") model.
//!
//! No reaper exists yet (Task 3) — `PoolConfig::default().reap_interval` is `None`, which is what
//! keeps the `start_paused` tests below deterministic: nothing but the test itself can advance
//! state when the paused clock is advanced.

use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pool::Pool;

#[tokio::test]
async fn checkout_release_reuse() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 4,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    // checkout -> release -> checkout again reuses the SAME conn id.
    let c0 = pool.checkout().await.expect("checkout should succeed");
    let id0 = c0.conn().id;
    drop(c0);

    let c1 = pool.checkout().await.expect("checkout should succeed");
    assert_eq!(
        c1.conn().id,
        id0,
        "released conn should be reused, not reconnected"
    );
    drop(c1);

    // Fill the pool to max_size concurrently: every id handed out must be < max_size (never
    // more than max_size live connections were ever created).
    let mut checkouts = Vec::new();
    for _ in 0..4 {
        checkouts.push(
            pool.checkout()
                .await
                .expect("checkout under max_size should succeed"),
        );
    }
    let ids: Vec<u64> = checkouts.iter().map(|c| c.conn().id).collect();
    assert!(
        ids.iter().all(|&id| id < 4),
        "connect count exceeded max_size: ids = {ids:?}"
    );
    drop(checkouts);

    // Releasing all 4 and checking out 4 more times must still never exceed max_size distinct
    // ids (all conns are reused from the idle stack, no fresh connects).
    for _ in 0..4 {
        let c = pool
            .checkout()
            .await
            .expect("checkout after release should reuse an idle conn");
        assert!(c.conn().id < 4, "unexpected fresh conn id {}", c.conn().id);
    }
}

#[tokio::test(start_paused = true)]
async fn max_size_blocks_then_times_out() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    // Hold the only permit.
    let held = pool
        .checkout()
        .await
        .expect("first checkout should succeed immediately");

    let pool2 = pool.clone();
    let handle = tokio::spawn(async move { pool2.checkout().await });

    // Let the spawned task run up to its pending await (registering the checkout_timeout sleep
    // against the CURRENT paused time) before we advance the clock past the deadline.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(100)).await;

    let result = handle
        .await
        .expect("spawned checkout task should not panic");
    assert!(
        matches!(result, Err(PoolError::Timeout)),
        "expected Err(PoolError::Timeout) once checkout_timeout elapsed"
    );

    drop(held);
}

#[tokio::test(start_paused = true)]
async fn queue_us_observed_on_queued_success() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    // Hold the only conn (id 0).
    let held = pool
        .checkout()
        .await
        .expect("first checkout should succeed");
    let held_id = held.conn().id;

    let pool2 = pool.clone();
    let handle = tokio::spawn(async move { pool2.checkout().await });

    // Let the second checkout register on the semaphore (capturing its `start` Instant) before
    // we advance the clock while it is still queued.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(20)).await;

    // Now release A: pushes the conn back to idle and releases the permit, waking B.
    drop(held);

    let second = handle
        .await
        .expect("spawned checkout task should not panic")
        .expect("second checkout should succeed once the permit is released");

    assert_eq!(
        second.conn().id,
        held_id,
        "the second checkout should reuse the conn A released"
    );
    assert!(
        second.stats().queue_us > 0,
        "a checkout that had to wait for a permit should observe queue_us > 0, got {}",
        second.stats().queue_us
    );
}

#[tokio::test]
async fn evicts_dead_idle_connection() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 4,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let c0 = pool.checkout().await.expect("checkout should succeed");
    let dead_id = c0.conn().id;
    drop(c0); // conn goes idle, still alive at this point

    // Simulate the idle connection dying asynchronously (e.g. a backend driver discovering EOF
    // while the conn sat idle) without going through Drop's own liveness filter.
    pool.poison_idle_for_test(|conn| conn.closed = true);

    let fresh = pool
        .checkout()
        .await
        .expect("checkout should evict the dead idle conn and connect a fresh one");
    assert_ne!(
        fresh.conn().id,
        dead_id,
        "the dead idle conn must never be handed out"
    );
    assert!(!fresh.conn().closed, "the fresh conn must not be closed");
}

#[tokio::test]
async fn connect_failure_releases_permit() {
    let backend = FakeBackend::new();
    backend.arm_fail_connect(1);
    let config = PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let result = pool.checkout().await;
    assert!(
        matches!(result, Err(PoolError::ConnectionLost)),
        "the armed first connect() should fail with ConnectionLost"
    );

    // If the permit had leaked when connect() failed above, this would time out (max_size=1).
    // It succeeds because the permit was released.
    let c = pool
        .checkout()
        .await
        .expect("second checkout should succeed: the failed connect's permit was released");
    assert_eq!(c.conn().id, 0);
}
