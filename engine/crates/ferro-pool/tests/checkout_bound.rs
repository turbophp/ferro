//! M1-S9a Task 3 — `checkout_timeout` bounds the WHOLE checkout (finding 4a), and a poisoned
//! idle mutex no longer turns `Checkout::drop` into a process abort (finding 7b).
//!
//! The wedged-dial shape was CONFIRMED by the M0 review: a backend that accepts TCP but never
//! completes the startup handshake parked `checkout()` forever while holding a permit; `max_size`
//! such callers took the pool to zero usable capacity permanently, even after the backend
//! recovered. Every await here is wrapped in an outer 600s bound so a regression fails LOUDLY
//! instead of hanging the suite (under `start_paused`, auto-advance makes the outer bound fire
//! immediately once nothing else can run).

use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pool::Pool;

fn cfg(max_size: usize, checkout_ms: u64) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_millis(checkout_ms),
        // Reaper-less: nothing but the test may advance pool state under `start_paused`.
        reap_interval: None,
        ..PoolConfig::default()
    }
}

async fn bounded<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(600), fut)
        .await
        .expect("BOUND EXCEEDED: the operation under test is unbounded (the finding-4a hang)")
}

/// `Checkout` is deliberately not `Debug` (it owns a live backend conn), so assertion messages
/// render the outcome through this instead of `{:?}` on the whole `Result`.
fn outcome<B: ferro_pool::backend::PoolBackend>(
    r: &Result<ferro_pool::pool::Checkout<B>, PoolError>,
) -> String {
    match r {
        Ok(_) => "Ok(<checkout>)".to_string(),
        Err(e) => format!("Err({e:?})"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_wedged_dial_is_bounded_by_checkout_timeout() {
    let backend = FakeBackend::new();
    backend.block_connect();
    let pool = Pool::new(backend, cfg(2, 50));

    let r = bounded(pool.checkout()).await;
    assert!(
        matches!(r, Err(PoolError::Timeout)),
        "a checkout whose dial wedges must resolve Err(Timeout) within checkout_timeout, got {}",
        outcome(&r)
    );

    // The Timeout must have come from the DIAL, not from some unrelated earlier bound (a permit
    // wait that never happened, say) — otherwise this test would pass without ever exercising the
    // finding. `connect()` parked on the gate and its future was DROPPED there, so the waiting
    // counter never got its decrement: a stuck `1` IS the proof the dial was in flight.
    assert_eq!(
        pool.backend().connects_waiting(),
        1,
        "the checkout must have wedged inside backend.connect(), not before it"
    );
    // And the wedged dial leaked nothing: no connection was ever minted.
    assert_eq!(
        pool.backend().total_connected(),
        0,
        "a dial cut off by the deadline must not hand a half-built conn to anyone"
    );
}

#[tokio::test(start_paused = true)]
async fn capacity_returns_after_wedged_dials_time_out() {
    let backend = FakeBackend::new();
    backend.block_connect();
    let pool = Pool::new(backend, cfg(2, 50));

    // Both permits' worth of checkouts wedge in the dial CONCURRENTLY (the confirmed shape: every
    // permit held by a future that never resolves) and both must time out.
    let (r1, r2) = bounded(async { tokio::join!(pool.checkout(), pool.checkout()) }).await;
    assert!(
        matches!(r1, Err(PoolError::Timeout)),
        "got {}",
        outcome(&r1)
    );
    assert!(
        matches!(r2, Err(PoolError::Timeout)),
        "got {}",
        outcome(&r2)
    );

    // BOTH wedged inside the dial — i.e. both permits were genuinely held by a parked
    // `connect()`, which is the confirmed shape (`max_size` wedged dials => zero capacity). If one
    // had merely timed out waiting for a permit, this would read 1.
    assert_eq!(
        pool.backend().connects_waiting(),
        2,
        "both checkouts must have wedged inside backend.connect(), each holding a permit"
    );

    // The permits were RELEASED on the Err returns (the wedged futures were dropped). Once the
    // backend recovers, the pool serves again — the review's confirmed shape was that it NEVER
    // did, even after recovery: zero usable capacity, permanently.
    pool.backend().release_connect();
    let co = bounded(pool.checkout())
        .await
        .expect("the pool must not be permanently wedged after the backend recovers");
    drop(co);

    // And it is genuinely back to full capacity, not down to the one permit that happened to be
    // free: both permits serve again.
    let (a, b) = bounded(async { tokio::join!(pool.checkout(), pool.checkout()) }).await;
    // `Checkout` is not `Debug`; report the errors, which are.
    assert!(
        a.is_ok() && b.is_ok(),
        "both permits must serve again: {} / {}",
        outcome(&a),
        outcome(&b)
    );
}

/// Finding 7b: a poisoned idle mutex must not abort the daemon. With the poison-recovering lock,
/// `Checkout::drop` (and the next `checkout()`) proceed; with the old `.unwrap()`, the drop
/// panics — and in production that panic during another unwind is `std::process::abort()`,
/// taking every worker's connections down with it.
#[tokio::test]
async fn a_poisoned_idle_mutex_does_not_panic_drop_or_checkout() {
    let pool = Pool::new(FakeBackend::new(), cfg(2, 50));
    let co = pool.checkout().await.expect("checkout before poisoning");

    pool.poison_idle_mutex_for_test();

    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(co)));
    assert!(
        dropped.is_ok(),
        "Checkout::drop must recover a poisoned idle mutex, not panic (abort in production)"
    );

    // And the pool still works end to end: the released conn is back on the idle stack and the
    // next checkout can reach it through the same recovered lock.
    let co2 = pool.checkout().await.expect("checkout after poisoning");
    drop(co2);
}
