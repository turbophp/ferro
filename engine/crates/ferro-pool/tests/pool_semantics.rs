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
use ferro_pool::health::backoff_delay;
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

// --- S4 Task 3: max_lifetime recycling, the cancel-safe reaper, and the backoff schedule -------
//
// Per v2/M3, the reaper-less tests above (and `max_lifetime_recycles` below) keep
// `reap_interval: None` so `start_paused` + manual `advance` stay fully deterministic — nothing
// but the test itself can move pool state when the clock moves. The reaper-specific tests
// (`reaper_closes_stale_idle`, `reaper_stops_on_pool_drop`) turn the reaper on deliberately and
// are kept separate for exactly that reason.

#[tokio::test(start_paused = true)]
async fn max_lifetime_recycles() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 4,
        max_lifetime: Duration::from_secs(60),
        reap_interval: None, // reaper-less: deterministic under start_paused (v2/M3)
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let c0 = pool.checkout().await.expect("checkout should succeed");
    let id0 = c0.conn().id;
    drop(c0); // conn goes idle

    // Advance well past max_lifetime; no reaper exists to act on this, so the eviction can only
    // come from the checkout-time age check (Task 2's `too_old` branch).
    tokio::time::advance(Duration::from_secs(61)).await;

    let c1 = pool
        .checkout()
        .await
        .expect("checkout after max_lifetime should reconnect, not error");
    assert_ne!(
        c1.conn().id,
        id0,
        "a conn older than max_lifetime must be evicted and replaced with a fresh one"
    );
}

#[tokio::test(start_paused = true)]
async fn reaper_closes_stale_idle() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 4,
        max_lifetime: Duration::from_millis(20),
        reap_interval: Some(Duration::from_millis(50)),
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let c0 = pool.checkout().await.expect("checkout should succeed");
    let stale_id = c0.conn().id;
    drop(c0); // conn goes idle

    // Advance past max_lifetime + reap_interval so the reaper's sleep fires with the conn already
    // past max_lifetime (evicted on the reaper's very first tick — no need for multiple periodic
    // firings to line up), then yield so the woken reaper task actually runs its tick (a sync
    // point, not a real sleep).
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;

    let fresh = pool
        .checkout()
        .await
        .expect("checkout should reconnect after the reaper evicted the stale idle conn");
    assert_ne!(
        fresh.conn().id,
        stale_id,
        "the reaper should have evicted the stale idle conn before this checkout"
    );
}

#[tokio::test(start_paused = true)]
async fn reaper_stops_on_pool_drop() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 4,
        max_lifetime: Duration::from_millis(20),
        reap_interval: Some(Duration::from_millis(50)),
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    // Put a conn idle so there would be something for a (hypothetical, still-alive) reaper tick
    // to touch.
    let c0 = pool.checkout().await.expect("checkout should succeed");
    drop(c0);

    // Drop every strong handle to the pool. The reaper's `Weak` should no longer `upgrade()`.
    drop(pool);

    // Advancing time / yielding past where the reaper would have ticked must not hang or panic:
    // `Weak::upgrade()` returns `None` on the next tick and the reaper task exits. Wrapped in a
    // timeout so a genuine regression (the reaper somehow kept alive/looping) fails fast with a
    // clear message instead of hanging the test run.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::time::advance(Duration::from_millis(200)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    })
    .await;
    assert!(
        result.is_ok(),
        "advancing time after dropping the pool must not hang (reaper should have stopped)"
    );
}

#[test]
fn backoff_delay_schedule() {
    let cases = [
        (0u32, Duration::from_millis(10)),
        (3u32, Duration::from_millis(80)),
        (7u32, Duration::from_secs(1)),
        (20u32, Duration::from_secs(1)),
    ];
    for (attempt, upper_bound) in cases {
        // Sample repeatedly since the jitter is randomized; every sample must respect the bound.
        for _ in 0..50 {
            let d = backoff_delay(attempt);
            assert!(
                d <= upper_bound,
                "backoff_delay({attempt}) = {d:?} exceeds bound {upper_bound:?}"
            );
        }
    }
}
