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
    // trigger a defensive ROLLBACK (no trailing "ROLLBACK" recorded) — but M1-S3's conditional
    // hygiene DOES run the backend's clean-profile reset on this now-recycled, non-tainted conn
    // (the fake defaults `clean_reset_profile()` to `Some(Targeted)`, mirroring `PgBackend`'s §7.4
    // blind-spot backstop), so a trailing "RESET:Targeted" is expected and correct here.
    drop(c);
    let next = pool.checkout().await.expect("checkout should succeed");
    assert_eq!(
        next.conn().recorded,
        vec![
            "BEGIN".to_string(),
            "COMMIT".to_string(),
            "RESET:Targeted".to_string()
        ],
        "no defensive ROLLBACK should run once the tx was cleanly committed, but the non-tainted \
         recycled conn still gets the targeted profile reset (§7.2 conditional hygiene)"
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
    // The dropped conn was OPEN (`tx_open`) but never tainted (a bare BEGIN taints nothing), so the
    // recycle runs the defensive ROLLBACK FIRST, then — M1-S3's conditional hygiene — the backend's
    // clean-profile reset on this now non-tainted recycled conn (the fake's default
    // `clean_reset_profile() == Some(Targeted)`, the §7.4 blind-spot backstop). Assert the full
    // ROLLBACK-then-RESET:Targeted sequence, not just `.last()`.
    assert_eq!(
        b.conn().recorded,
        vec![
            "BEGIN".to_string(),
            "ROLLBACK".to_string(),
            "RESET:Targeted".to_string()
        ],
        "the next checkout should run ROLLBACK then the targeted reset, in that order, recorded = {:?}",
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
    // MINOR 4 (S4 review): a leading comment must not hide the tx-control keyword from the guard.
    assert!(
        matches!(
            c.exec("/* c */ BEGIN").await,
            Err(PoolError::Unsupported(_))
        ),
        "a bare BEGIN behind a leading block comment via exec() must still be rejected"
    );
    assert!(
        matches!(
            c.exec("-- c\nROLLBACK").await,
            Err(PoolError::Unsupported(_))
        ),
        "a bare ROLLBACK behind a leading line comment via exec() must still be rejected"
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

// --- M1-S3 Task 2: the conditional checkout-recycle decision (§7.2) -----------------------------
//
// `Pool::checkout`'s recycle block now picks a hygiene profile off `idle_conn.tainted`: a tainted
// conn (session mutation observed, or an error/aborted-tx fail-safe) gets `ResetProfile::Full`; a
// non-tainted recycled conn gets the backend's `clean_reset_profile()` (the fake defaults to
// `Some(ResetProfile::Targeted)`, mirroring `PgBackend`'s §7.4 blind-spot backstop). These tests
// drive that decision deterministically via the `FakeBackend`.

#[tokio::test]
async fn tainted_conn_gets_full_reset_on_next_checkout() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let mut c = pool.checkout().await.expect("checkout should succeed");
    // A non-local SET taints via the M1-S2 assist lexer (autocommit — no tx involved, so tx_open
    // stays false and only `tainted` is set).
    c.exec("SET search_path TO app")
        .await
        .expect("SET should succeed");
    assert!(c.tainted(), "a non-local SET must taint");
    assert!(!c.tx_open(), "an autocommit SET never opens a tx");

    drop(c);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded,
        vec![
            "SET search_path TO app".to_string(),
            "RESET:Full".to_string()
        ],
        "a tainted conn must get the FULL reset on the next checkout, recorded = {:?}",
        next.conn().recorded
    );
}

#[tokio::test]
async fn non_tainted_recycled_conn_gets_targeted_reset_on_next_checkout() {
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let mut c = pool.checkout().await.expect("checkout should succeed");
    c.exec("SELECT 1").await.expect("plain SELECT");
    assert!(!c.tainted(), "a plain SELECT never taints");
    assert!(!c.tx_open());

    drop(c);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded,
        vec!["SELECT 1".to_string(), "RESET:Targeted".to_string()],
        "a non-tainted recycled conn must get the TARGETED reset (fake's clean_reset_profile == \
         Some(Targeted), mirroring PgBackend's §7.4 blind-spot backstop), recorded = {:?}",
        next.conn().recorded
    );
}

#[tokio::test]
async fn tx_open_and_tainted_rollback_precedes_full_reset() {
    // Ordering must be preserved regardless of WHICH profile applies: the defensive ROLLBACK (for
    // an open tx) always runs before the reset, since a reset (e.g. real PG's DISCARD ALL) cannot
    // run inside a transaction block.
    let backend = FakeBackend::new();
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let mut c = pool.checkout().await.expect("checkout should succeed");
    c.begin_tx(TxId(1)).await.expect("begin_tx should succeed");
    // Taint it via a session-mutating statement inside the open tx.
    c.exec("SET search_path TO app")
        .await
        .expect("SET inside open tx");
    assert!(c.tx_open(), "the tx is still open (SET is a PRESERVE verb)");
    assert!(c.tainted(), "the SET must taint");

    // Dropped without COMMIT/ROLLBACK: tx_open AND tainted both survive onto the idle conn.
    drop(c);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded,
        vec![
            "BEGIN".to_string(),
            "SET search_path TO app".to_string(),
            "ROLLBACK".to_string(),
            "RESET:Full".to_string()
        ],
        "ROLLBACK must precede the (Full) reset, recorded = {:?}",
        next.conn().recorded
    );
}

#[tokio::test]
async fn clean_reset_profile_none_skips_hygiene_entirely_for_a_non_tainted_conn() {
    // The MySQL-known-clean analog (S6, future work): a backend that reports `None` from
    // `clean_reset_profile()` means "this non-tainted conn needs nothing at all" — the recycle
    // must skip the timeout-wrapped cleanup entirely rather than running a no-op reset.
    let backend = FakeBackend::new();
    backend.set_clean_reset_profile(None);
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(backend, config);

    let mut c = pool.checkout().await.expect("checkout should succeed");
    c.exec("SELECT 1").await.expect("plain SELECT");
    assert!(!c.tainted());
    assert!(!c.tx_open());

    drop(c);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded,
        vec!["SELECT 1".to_string()],
        "a None clean_reset_profile must skip hygiene entirely for a non-tainted conn, recorded = {:?}",
        next.conn().recorded
    );
}
