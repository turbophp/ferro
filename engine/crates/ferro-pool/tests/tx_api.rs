//! S6 tests for the ferro-pool TX API (`begin_tx_with` / `tx_control` / `cancel_handle`) and the
//! BOUNDED recycle cleanup, all driven by the deterministic `FakeBackend` (no Postgres, so
//! `cargo test --workspace` stays green offline). The live behaviour (composed isolation observed,
//! out-of-band cancel -> 57014, savepoints on a pinned conn, same backend pid) is exercised against
//! a real Postgres in `ferro-backend-pg/tests/pg_pool_it.rs`.

use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::Pool;

#[tokio::test]
async fn begin_tx_with_runs_composed_begin_and_pins() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    // A COMPOSED begin string (isolation + readonly) — not a hardcoded "BEGIN".
    let begin_sql = "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY";
    co.begin_tx_with(TxId(7), begin_sql)
        .await
        .expect("begin_tx_with should run the composed BEGIN");

    assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(7)));
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));
    // The composed begin string reached the backend VERBATIM (proving it is not overridden with a
    // plain "BEGIN").
    assert_eq!(co.conn().recorded, vec![begin_sql.to_string()]);
}

#[tokio::test]
async fn begin_tx_still_uses_plain_begin() {
    // begin_tx is now begin_tx_with(id, "BEGIN"); the plain form must be unchanged.
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx(TxId(1)).await.expect("begin_tx");

    assert_eq!(co.conn().recorded, vec!["BEGIN".to_string()]);
    assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(1)));
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));
}

#[tokio::test]
async fn tx_control_bypasses_the_bare_tx_control_guard() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    // SAVEPOINT/RELEASE/ROLLBACK TO are all bare tx-control that the guarded query()/exec() REJECT.
    // tx_control is the engine-only UNGUARDED path, so these engine-composed savepoint statements
    // go straight to the backend.
    co.tx_control("SAVEPOINT sp_1")
        .await
        .expect("SAVEPOINT via tx_control");
    co.tx_control("RELEASE sp_1")
        .await
        .expect("RELEASE via tx_control");
    co.tx_control("ROLLBACK TO sp_1")
        .await
        .expect("ROLLBACK TO via tx_control");

    assert_eq!(
        co.conn().recorded,
        vec![
            "SAVEPOINT sp_1".to_string(),
            "RELEASE sp_1".to_string(),
            "ROLLBACK TO sp_1".to_string(),
        ],
        "tx_control must pass engine-composed savepoint SQL through the UNGUARDED path"
    );

    // Sanity: the SAME SAVEPOINT via the guarded query() IS still rejected — proving tx_control's
    // bypass is a deliberate, separate door, NOT that the guard has been weakened.
    assert!(
        matches!(
            co.query("SAVEPOINT sp_2", &[]).await,
            Err(PoolError::Unsupported(_))
        ),
        "the user-facing guarded query() must still reject bare tx-control"
    );
}

#[tokio::test]
async fn cancel_handle_returns_a_handle() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let co = pool.checkout().await.expect("checkout");

    // The point: Checkout::cancel_handle exists, is callable on &self (borrows nothing a live query
    // future needs), and returns B::CancelHandle. The fake's handle is a no-op.
    let _handle = co.cancel_handle();
}

#[tokio::test]
async fn bounded_recycle_evicts_a_conn_whose_cleanup_blocks() {
    let cfg = PoolConfig {
        max_size: 4,
        // ONE budget for the whole checkout — permit + recycle cleanup + dial (M1-S9a, finding
        // 4a). Short so the test is fast; the outer timeout below is the real "never hang" guard.
        checkout_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let pool = Pool::new(FakeBackend::new(), cfg);

    // First checkout: mark tx_open so the NEXT checkout must run a defensive ROLLBACK on this conn,
    // then release it back to the idle stack.
    let poisoned_id = {
        let mut co = pool.checkout().await.expect("first checkout");
        co.set_tx_open(true);
        co.conn().id
    };

    // Arm the block hook so that defensive ROLLBACK hangs indefinitely on the next checkout.
    pool.backend().block_simple_query();

    // The next checkout pops the poisoned idle conn and its ROLLBACK blocks. Two properties, and
    // M1-S9a changed which one this checkout ends on:
    //   * BOUNDED — the recycle cleanup is cut off, never a hang (the outer 5s is the hard proof);
    //   * EVICTED — the conn whose cleanup did not complete is DROPPED, never handed out or pushed
    //     back (the cross-tenant-leak rule).
    // Before M1-S9a the cleanup got its own FRESH full `checkout_timeout`, so this checkout could
    // still spend a second budget dialling and return a fresh conn — i.e. total checkout latency
    // could reach `max_size * checkout_timeout` while the knob claimed 1x. Now the cleanup is
    // bounded by the checkout's SHARED deadline, so a cleanup that eats the whole budget ends
    // THIS checkout in `Err(Timeout)` — the honest answer for a caller whose budget is spent — and
    // the eviction is proven by the FOLLOWING checkout getting a different conn.
    let r = tokio::time::timeout(Duration::from_secs(5), pool.checkout())
        .await
        .expect("checkout must be bounded (evict on recycle timeout), never hang");
    assert!(
        matches!(r, Err(PoolError::Timeout)),
        "a recycle cleanup that consumes the whole checkout_timeout must end the checkout in \
         Err(Timeout), not silently spend a second budget: got {}",
        match &r {
            Ok(_) => "Ok(<checkout>)".to_string(),
            Err(e) => format!("Err({e:?})"),
        }
    );

    // The poisoned conn was EVICTED by that expiry, not pushed back: the next checkout finds the
    // idle stack empty and dials fresh (still bounded, still never a hang).
    let co2 = tokio::time::timeout(Duration::from_secs(5), pool.checkout())
        .await
        .expect("the checkout after the eviction must be bounded too")
        .expect("checkout should reconnect fresh after evicting the poisoned conn");
    assert_ne!(
        co2.conn().id,
        poisoned_id,
        "the conn whose recycle cleanup blocked must be evicted, not reused"
    );
}
