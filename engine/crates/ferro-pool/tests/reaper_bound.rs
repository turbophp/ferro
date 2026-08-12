//! M1-S9a Task 4 — the reaper's ping is bounded (finding 4b). The M0 review CONFIRMED the
//! unbounded shape: `block_pings()` + `max_size=1` parked the reaper forever holding the sole
//! permit — two successive checkouts both `Err(Timeout)`, no idle conn ever evicted again.
//!
//! Second test: the reaper's three `idle` locks recover from poisoning instead of panicking
//! (finding 7). A reaper that dies on a poisoned mutex is the same outage as a reaper wedged on a
//! ping — nothing is ever evicted again — so both live here.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pool::Pool;

const CHECKOUT_TIMEOUT: Duration = Duration::from_millis(50);
const REAP_INTERVAL: Duration = Duration::from_millis(5);

/// The gap the test opens between the reaper's ping deadline and the observing checkout's own
/// deadline. See `a_wedged_ping_evicts_the_conn_and_the_reaper_keeps_ticking` for why a test that
/// does not open it is decided by a timer tie-break rather than by the property.
const MARGIN: Duration = Duration::from_millis(10);

async fn bounded<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(600), fut)
        .await
        .expect("BOUND EXCEEDED: the reaper ping is unbounded (the finding-4b hang)")
}

fn wedgeable_pool() -> Pool<FakeBackend> {
    Pool::new(
        FakeBackend::new(),
        PoolConfig {
            max_size: 1,
            checkout_timeout: CHECKOUT_TIMEOUT,
            reap_interval: Some(REAP_INTERVAL),
            ..PoolConfig::default()
        },
    )
}

/// Waits (bounded) until the reaper is PROVABLY parked inside a ping: `pings_waiting()` can only
/// become non-zero after the reaper has popped the idle conn under `idle`'s lock, taken an owned
/// permit for it, called `ping()`, and reached the parked `.await` inside it. Returns the observed
/// count, which later assertions use as a BASELINE — never as an absolute value, because a ping
/// future dropped by a timeout skips `FakeBackend::ping`'s post-await decrement.
async fn wait_until_parked_in_ping(pool: &Pool<FakeBackend>) -> u64 {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.backend().pings_waiting() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await; // paused clock: auto-advances
        }
    })
    .await
    .expect("the reaper never reached its (blocked) ping — it is not ticking at all");
    pool.backend().pings_waiting()
}

#[tokio::test(start_paused = true)]
async fn a_wedged_ping_evicts_the_conn_and_the_reaper_keeps_ticking() {
    let pool = wedgeable_pool();

    // Park one idle conn, then freeze pings so the next reaper tick wedges on it.
    let co = pool.checkout().await.expect("first checkout");
    let wedged_conn_id = co.conn().id;
    drop(co);
    pool.backend().block_pings();
    let parked_once = wait_until_parked_in_ping(&pool).await;
    let wedged_at = tokio::time::Instant::now();

    // MEASURED, and the reason this sleep exists (do not delete it): the reaper's ping bound and
    // `checkout()`'s own budget are THE SAME KNOB (`checkout_timeout`). Started at the same
    // instant they expire at the same instant, and which one fires first is a tokio timer
    // tie-break, not a property of the code under test. Without this sleep the test passed with a
    // margin of exactly 1ms — an accident of the 1ms poll granularity above — and adding 1ms to
    // the production ping bound flipped it RED. Sleeping MARGIN first makes the observing
    // checkout's deadline strictly LATER than the ping's by MARGIN, by construction.
    // This cannot make the test pass against the unbounded implementation: there the permit is
    // never released at all, so no amount of waiting produces a successful checkout.
    tokio::time::sleep(MARGIN).await;

    // THE property: the wedged ping is evicted within the bound, the permit comes back, and a
    // checkout succeeds (it dials fresh — connect is not gated here).
    let co = bounded(pool.checkout())
        .await
        .expect("the reaper must release its permit when the ping times out");

    // A ping that ran out of budget had its future DROPPED mid-round-trip, so that connection is
    // in an unknown protocol state. It must be EVICTED, never pushed back into `idle` — otherwise
    // the bound would trade a wedged reaper for the far worse bug of handing a half-pinged
    // connection to the next tenant (the hazard-15 shape). Proven by identity: this checkout must
    // have dialed a FRESH conn.
    assert_ne!(
        co.conn().id,
        wedged_conn_id,
        "the conn whose ping timed out must be evicted, not recycled — its ping future was \
         dropped mid-flight and the next tenant must never inherit it"
    );
    assert_eq!(
        pool.backend().total_connected(),
        2,
        "exactly one fresh dial: the wedged conn was evicted and replaced"
    );
    drop(co);

    // ...and it came back WITHIN THE BOUND, not merely eventually. The `.expect` above is the
    // load-bearing assertion today (a much larger ping bound makes the checkout time out first);
    // this one pins the bound's MAGNITUDE, so an implementation that keeps the checkout budget and
    // the ping budget in step while inflating both still fails here.
    assert!(
        wedged_at.elapsed() <= CHECKOUT_TIMEOUT + MARGIN,
        "the permit must return within one ping budget of the wedge, got {:?}",
        wedged_at.elapsed()
    );

    // And the reaper LOOP is still alive: the conn we just returned to idle gets picked up and
    // pinged by a LATER tick (the counter moves past its captured baseline — the parked future's
    // drop skips its decrement, so compare against the baseline, never an absolute value).
    // PLAN-VERIFY F3: this condition was `<= parked_once - 1`, which is FALSE at entry
    // (parked_once >= 1, so `1 <= 0`), so the loop never ran and the assert below fired
    // immediately as `1 > 1` — a test that could not pass against a CORRECT implementation.
    // Loop until the counter EXCEEDS the baseline; the inner if/break is then redundant.
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.backend().pings_waiting() <= parked_once {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the reaper stopped ticking after the wedged ping (finding 4b: dead for the pool's lifetime)");
    assert!(
        pool.backend().pings_waiting() > parked_once,
        "the reaper must survive a wedged ping and keep examining idle conns on later ticks"
    );
}

/// Finding 7, the second half: a poisoned `idle` mutex must not kill the reaper.
///
/// `reap_once` locks `idle` three times — `len()`, `pop()`, `push()` — and every one used
/// `.lock().unwrap()`. A single panic anywhere while that guard is held poisons the mutex forever,
/// and the very next tick then panics: the reaper task dies and NOTHING is evicted again for the
/// pool's lifetime, exactly the outage the ping bound above exists to prevent, reached by a
/// different door.
///
/// All three sites are discriminated by this one test, because a panic at ANY of them kills the
/// reaper task: site 1 (`len`) and site 2 (`pop`) run before the ping, site 3 (`push`) runs after a
/// SUCCESSFUL ping — which is why the test lets two unblocked ticks complete before it freezes
/// pings and demands proof the reaper is still alive.
///
/// NOTE: this test deliberately panics once, inside `catch_unwind`. A panic message on stderr from
/// this test is EXPECTED output, not a failure.
#[tokio::test(start_paused = true)]
async fn a_poisoned_idle_mutex_does_not_kill_the_reaper() {
    let pool = wedgeable_pool();

    // One conn idle for the reaper to find — and for the poisoning closure to reach.
    let co = pool.checkout().await.expect("first checkout");
    drop(co);

    // Poison `idle` exactly the way production would: panic while its guard is held.
    // `poison_idle_for_test` runs the closure under `idle.lock()`, so the unwind marks the mutex
    // poisoned. Catching the panic is what lets the test continue and observe the consequences.
    let caught = std::panic::catch_unwind(AssertUnwindSafe(|| {
        pool.poison_idle_for_test(|_| {
            panic!("M1-S9a Task 4: deliberately poisoning the idle mutex")
        });
    }));
    assert!(
        caught.is_err(),
        "the poisoning closure never ran, so the mutex is NOT poisoned and this test proves \
         nothing — `idle` must be non-empty when it is called"
    );

    // Let two unblocked reaper ticks run against the poisoned mutex. Each one must lock `idle`
    // three times: len, pop, and — because an unblocked `FakeBackend::ping` SUCCEEDS — push-back.
    tokio::time::sleep(REAP_INTERVAL * 2 + Duration::from_millis(2)).await;

    // Now prove the reaper is still alive by making it park in a ping. It can only get there by
    // having survived every one of those locks.
    pool.backend().block_pings();
    let parked = wait_until_parked_in_ping(&pool).await;
    assert!(
        parked > 0,
        "the reaper must keep ticking through a poisoned idle mutex — a `.lock().unwrap()` at any \
         of the three sites kills the task and the pool is never reaped again"
    );
}
