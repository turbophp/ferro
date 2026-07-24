//! S4 Task 4 tests for the stubbed pin state machine: `PinCause::Tx` on BEGIN, a pinned conn is
//! never handed to a second checkout, the defensive ROLLBACK fires at the *next* checkout (not in
//! Drop — v2/B1), and the guarded `Checkout::exec` rejects bare tx-control (v2/M1 + M2).

use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::Pool;

#[tokio::test]
async fn pin_stub_tx_cause() {
    let backend = FakeBackend::new();
    let pool = Pool::new(backend, PoolConfig::default());

    let mut c = pool.checkout().await.expect("checkout should succeed");
    c.begin_tx(TxId(1)).await.expect("begin_tx should succeed");

    assert_eq!(c.pin_state(), PinState::PinnedTx(TxId(1)));
    assert_eq!(c.last_pin_cause(), Some(PinCause::Tx));
    assert_eq!(c.conn().recorded, vec!["BEGIN".to_string()]);

    c.commit_tx().await.expect("commit_tx should succeed");
    assert_eq!(c.pin_state(), PinState::Unpinned);
    assert_eq!(
        c.conn().recorded,
        vec!["BEGIN".to_string(), "COMMIT".to_string()]
    );

    // A cleanly committed tx clears the tx_open flag: dropping and checking out again must NOT
    // trigger a defensive rollback (no trailing "ROLLBACK" recorded).
    drop(c);
    let next = pool.checkout().await.expect("checkout should succeed");
    assert_eq!(
        next.conn().recorded,
        vec!["BEGIN".to_string(), "COMMIT".to_string()],
        "no defensive rollback should run once the tx was cleanly committed"
    );
}

#[tokio::test]
async fn pinned_conn_not_reused() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 2,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let mut a = pool.checkout().await.expect("checkout should succeed");
    let a_id = a.conn().id;
    a.begin_tx(TxId(7)).await.expect("begin_tx should succeed");

    // A concurrent checkout must get a DIFFERENT connection: A's pinned conn is held (not idle),
    // so it can never be popped off the idle stack for a second checkout.
    let pool2 = pool.clone();
    let b = tokio::spawn(async move { pool2.checkout().await })
        .await
        .expect("spawned checkout task should not panic")
        .expect("concurrent checkout should succeed while A is pinned");
    let b_id = b.conn().id;
    assert_ne!(
        b_id, a_id,
        "the pinned conn must never be handed to a second checkout"
    );

    // Drop B first, then A (still mid-transaction) — A's conn returns to the idle stack on Drop
    // (synchronously, per v2/B1) even though its transaction was never committed/rolled back.
    drop(b);
    drop(a);

    let reused = pool
        .checkout()
        .await
        .expect("checkout after both releases should succeed");
    assert_eq!(
        reused.conn().id,
        a_id,
        "A's connection should return to the pool once its Checkout is dropped"
    );
}

#[tokio::test]
async fn defensive_rollback_on_next_checkout() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let mut a = pool.checkout().await.expect("checkout should succeed");
    let conn_id = a.conn().id;
    a.begin_tx(TxId(1)).await.expect("begin_tx should succeed");
    // No commit/rollback: drop leaves `tx_open` set on the returned idle conn (v2/B1) — Drop
    // itself stays fully synchronous and never runs the ROLLBACK.
    drop(a);

    // Synchronize on the NEXT checkout completing (not on Drop): with max_size=1 the same
    // connection is guaranteed to be reused, and the async cleanup at the start of checkout()
    // must run the defensive ROLLBACK before handing it out.
    let b = pool
        .checkout()
        .await
        .expect("checkout should succeed and perform the defensive rollback");
    assert_eq!(
        b.conn().id,
        conn_id,
        "max_size=1 guarantees the same conn is reused"
    );
    assert_eq!(
        b.conn().recorded.last().map(String::as_str),
        Some("ROLLBACK"),
        "the next checkout should have run a defensive ROLLBACK, recorded = {:?}",
        b.conn().recorded
    );
    assert_eq!(
        b.pin_state(),
        PinState::Unpinned,
        "the handed-out Checkout must start Unpinned/clean"
    );
}

#[tokio::test]
async fn exec_rejects_bare_tx_control() {
    let backend = FakeBackend::new();
    let pool = Pool::new(backend, PoolConfig::default());
    let mut c = pool.checkout().await.expect("checkout should succeed");

    assert!(
        matches!(c.exec("BEGIN").await, Err(PoolError::Unsupported(_))),
        "bare BEGIN via exec() must be rejected"
    );
    assert!(
        matches!(
            c.exec("start transaction").await,
            Err(PoolError::Unsupported(_))
        ),
        "case-insensitive START TRANSACTION via exec() must be rejected"
    );
    assert!(
        matches!(c.exec("  RollBack  ").await, Err(PoolError::Unsupported(_))),
        "whitespace-padded, mixed-case ROLLBACK via exec() must be rejected"
    );

    let affected = c
        .exec("SELECT 1")
        .await
        .expect("an ordinary statement should be allowed through exec()");
    assert_eq!(affected, 0);
    assert_eq!(
        c.conn().recorded,
        vec!["SELECT 1".to_string()],
        "only the ordinary statement should have reached the backend; rejected calls must never \
         reach simple_query"
    );
}
