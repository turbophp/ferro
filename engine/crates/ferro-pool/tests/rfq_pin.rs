//! M1-S1 Task 4 tests for the RFQ-driven pin AUTHORITY in `Checkout` (`apply_tx_status`).
//!
//! Drives the deterministic `FakeBackend` (per-`FakeConn` `TxStatus`, inferred from the recorded
//! SQL's leading keyword or forced via `set_tx_status`) so the pin state machine is exercised
//! without a live Postgres. The live S1 acceptance (real RFQ `I`/`T`/`E` off the wire, same
//! `pg_backend_pid`, failed-tx held until ROLLBACK) lives in `ferro-backend-pg/tests/pg_pool_it.rs`.
//!
//! The two correctness rules under test:
//!   Rule A — tx_status is read on BOTH the Ok and Err arms; on Err the `is_err() && tx_open` guard
//!            forces `tainted=true` (the post-drain RFQ guarantee is success-only, atomic may stale).
//!   Rule B — `apply_tx_status` sets the reuse-safety bits (`tx_open`/`tainted`) unconditionally
//!            from I/T/E, but NEVER clobbers a real `TxId` and NEVER fabricates a sentinel `PinnedTx`.

use ferro_pool::backend::{Cancel, TxStatus};
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::{Checkout, Pool};

/// begin_tx_with(InTx) PRESERVES the real `PinnedTx(id)` + `tx_open` + cause; a following commit
/// (fake infers `Idle` from `COMMIT`) clears `tx_open` — the conn is reusable.
#[tokio::test]
async fn begin_pins_intx_then_commit_idles_and_leaves_reusable() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    // begin_tx_with records BEGIN -> fake infers InTx; apply_tx_status(InTx) CONFIRMS tx_open
    // without clobbering the manually-set real TxId.
    co.begin_tx_with(TxId(7), "BEGIN").await.expect("begin");
    assert!(co.tx_open(), "InTx must set tx_open");
    assert!(!co.tainted(), "a clean BEGIN taints nothing");
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(7)),
        "the real TxId set by begin_tx_with must be PRESERVED by apply_tx_status(InTx)"
    );
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx), "pin-cause DoD");

    // A statement inside the tx keeps InTx (fake preserves across an ordinary stmt).
    co.exec("SELECT 1").await.expect("in-tx stmt");
    assert!(co.tx_open(), "an in-tx statement keeps tx_open");
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(7)),
        "pin still held mid-tx"
    );

    // COMMIT -> fake infers Idle -> apply_tx_status(Idle) clears tx_open.
    co.commit_tx().await.expect("commit");
    assert!(!co.tx_open(), "Idle (commit) must clear tx_open");
    assert!(!co.tainted(), "a clean commit leaves no taint");
    assert_eq!(co.pin_state(), PinState::Unpinned, "commit unpins");

    // Drop + re-checkout: no defensive ROLLBACK/RESET runs on the cleanly-committed conn.
    drop(co);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded,
        vec![
            "BEGIN".to_string(),
            "SELECT 1".to_string(),
            "COMMIT".to_string()
        ],
        "a cleanly-committed conn is reusable with no defensive ROLLBACK/RESET appended"
    );
}

/// A `Failed` (RFQ `E`) status observed after an ordinary `exec` sets BOTH `tx_open` AND `tainted`
/// unconditionally, tags the cause `Tx`, and — for an RFQ-ONLY-detected E with no pool-opened tx —
/// LEAVES `pin` `Unpinned` (never fabricates a `TxId`).
#[tokio::test]
async fn failed_status_taints_and_never_fabricates_a_txid() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    // Force the conn's modeled status to Failed (E) — no SQL keyword can express "the last statement
    // errored", so this is the only way to model an aborted tx block via the fake.
    co.conn_mut().set_tx_status(TxStatus::Failed);
    // An ordinary statement PRESERVES the Failed status through the fake; apply_tx_status(Failed)
    // then sets the reuse-safety bits.
    co.exec("SELECT 1")
        .await
        .expect("stmt while status is Failed");

    assert!(co.tx_open(), "Failed (E) is an OPEN (aborted) tx block");
    assert!(
        co.tainted(),
        "Failed (E) must taint — it needs a ROLLBACK before reuse"
    );
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::Tx),
        "an RFQ-detected tx is a Tx pin"
    );
    assert_eq!(
        co.pin_state(),
        PinState::Unpinned,
        "an RFQ-ONLY E with no pool-opened tx must NOT fabricate a PinnedTx sentinel"
    );
}

/// Inside a pool-opened tx, a `Failed` status must NOT clobber the real `PinnedTx(id)` — the reuse
/// danger is carried by `tx_open`/`tainted`, the identity by the real `TxId`.
#[tokio::test]
async fn failed_status_inside_real_tx_preserves_the_real_txid() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx_with(TxId(9), "BEGIN").await.expect("begin");
    assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(9)));

    // A statement inside the tx errors server-side (model: status flips to Failed), observed on the
    // NEXT exec's status read.
    co.conn_mut().set_tx_status(TxStatus::Failed);
    co.exec("SELECT 1").await.expect("stmt observes Failed");

    assert!(co.tx_open());
    assert!(co.tainted());
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(9)),
        "a real pool-opened TxId must be PRESERVED across a Failed status, never clobbered"
    );
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));
}

/// Rule A / stale-atomic guard: an `Err` from `query` while a tx is open MUST taint, even when the
/// conn's status byte still reads a "clean" `InTx` (the post-drain guarantee is success-only, so on
/// Err the atomic may hold a stale byte). Uses the fake's out-of-band cancel to make `query` return
/// Err while the status stays `InTx`.
#[tokio::test]
async fn err_arm_taints_even_with_clean_intx_status() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx_with(TxId(1), "BEGIN").await.expect("begin");
    assert!(co.tx_open());
    assert!(!co.tainted());
    assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(1)));

    // Arm the query gate so the next query() parks; a side task fires the out-of-band cancel once
    // the query is provably in flight -> query() returns Err WHILE the conn's status is still InTx
    // (a "clean" byte). The stale-atomic guard must still taint.
    pool.backend().block_query();
    let cancel = co.cancel_handle();
    let pool2 = pool.clone();
    let canceller = tokio::spawn(async move {
        while pool2.backend().queries_waiting() == 0 {
            tokio::task::yield_now().await;
        }
        cancel.cancel().await;
    });

    let r = co.query("SELECT 1", &[]).await;
    canceller.await.expect("canceller task");

    assert!(
        matches!(r, Err(PoolError::Sql { .. })),
        "the cancelled query must return an Err, got {r:?}"
    );
    assert!(
        co.tainted(),
        "an Err while a tx is open MUST taint, even on a 'clean' InTx byte (stale-atomic guard)"
    );
    assert!(co.tx_open(), "the tx is still open");
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(1)),
        "the real TxId is never clobbered on the Err arm"
    );
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));
}

/// Failed-then-rolled-back (documented, conservative): an explicit `rollback_tx` after a `Failed`
/// status clears `tx_open` (fake infers `Idle` from `ROLLBACK`) but the `tainted` bit SURVIVES — so
/// the next checkout eats exactly one DISCARD-ALL reset. This is safe/conservative, not a bug.
#[tokio::test]
async fn failed_then_rollback_clears_tx_open_but_taint_survives() {
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(FakeBackend::new(), config);
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx_with(TxId(3), "BEGIN").await.expect("begin");
    // Model a failed in-tx statement.
    co.conn_mut().set_tx_status(TxStatus::Failed);
    co.exec("SELECT 1").await.expect("stmt observes Failed");
    assert!(co.tx_open() && co.tainted(), "Failed => tx_open && tainted");

    // Explicit rollback: fake infers Idle from ROLLBACK -> tx_open cleared, but tainted SURVIVES.
    co.rollback_tx().await.expect("rollback");
    assert!(!co.tx_open(), "ROLLBACK (Idle) clears tx_open");
    assert!(
        co.tainted(),
        "a clean Idle does NOT clear a prior taint — the next checkout eats one reset"
    );
    assert_eq!(co.pin_state(), PinState::Unpinned, "rollback unpins");

    // Next checkout (same conn, max_size=1) runs the deferred DISCARD-ALL reset.
    drop(co);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded.last().map(String::as_str),
        Some("RESET:Full"),
        "the surviving taint makes the next checkout run exactly one reset, recorded = {:?}",
        next.conn().recorded
    );
}

/// REGRESSION (Err-arm cross-tenant-leak fix): an `Err` from `exec` while the conn's `tx_status`
/// reports `Idle` (the stale-`Idle`-on-error condition — e.g. a multi-statement batch that opened a
/// tx mid-batch from autocommit, whose trailing `ReadyForQuery` has not decoded yet) MUST still arm
/// the checkout-time cleanup. The pool forces BOTH `tx_open` and `tainted` on ANY error, since on
/// the Err arm neither the RFQ atomic NOR a pre-captured `tx_open` can be trusted. Without the fix
/// the conn returns `tx_open==false && tainted==false`, the recycle is skipped, and the next tenant
/// inherits a possibly-open/aborted tx (charter rule 6). Proven independent of the fake modelling
/// the batch: the fake just errors with the status left at `Idle`.
#[tokio::test]
async fn err_arm_forces_cleanup_even_when_status_reads_idle() {
    let config = PoolConfig {
        max_size: 1,
        ..Default::default()
    };
    let pool = Pool::new(FakeBackend::new(), config);
    let mut co = pool.checkout().await.expect("checkout");
    // Fresh autocommit conn: not in a tx, not tainted, status Idle.
    assert!(!co.tx_open());
    assert!(!co.tainted());

    // Arm a simple_query failure that leaves tx_status == Idle (the stale-Idle-on-error case).
    co.conn_mut().arm_fail_next_simple_query();
    let r = co.exec("SELECT 1").await;
    assert!(
        matches!(r, Err(PoolError::Backend(_))),
        "the armed failure must surface as an Err, got {r:?}"
    );

    // The fail-safe forces BOTH bits so the checkout-time recycle will run ROLLBACK *then* reset.
    assert!(
        co.tx_open(),
        "ANY Err must force tx_open so the recycle ROLLBACKs a possibly-open tx (stale-Idle guard)"
    );
    assert!(
        co.tainted(),
        "ANY Err must force tainted so the recycle DISCARD ALLs a possibly-poisoned conn"
    );

    // Drop + re-checkout (same conn, max_size=1): the recycle ran ROLLBACK then reset, IN THAT
    // ORDER (DISCARD ALL cannot run inside a tx block).
    drop(co);
    let next = pool.checkout().await.expect("checkout again");
    assert_eq!(
        next.conn().recorded,
        vec![
            "SELECT 1".to_string(),
            "ROLLBACK".to_string(),
            "RESET:Full".to_string()
        ],
        "the forced bits must drive a ROLLBACK-then-reset recycle before reuse, recorded = {:?}",
        next.conn().recorded
    );
}

/// Shared pre-condition for the four per-method Err-arm tests below: a freshly checked-out
/// connection starts unpinned and untainted (autocommit, RFQ `Idle`).
fn assert_fresh(co: &Checkout<FakeBackend>) {
    assert!(
        !co.tx_open(),
        "a fresh checkout must start with tx_open == false"
    );
    assert!(
        !co.tainted(),
        "a fresh checkout must start with tainted == false"
    );
}

/// Shared post-condition for the four per-method Err-arm tests below: after an `Err` from the
/// method under test, BOTH reuse-safety bits must be forced regardless of what the (possibly
/// stale) RFQ status read back. Mirrors `err_arm_forces_cleanup_even_when_status_reads_idle`'s
/// assertions, factored out so the four near-identical method bodies don't repeat them.
fn assert_forced(co: &Checkout<FakeBackend>) {
    assert!(
        co.tx_open(),
        "ANY Err from this method must force tx_open so the recycle ROLLBACKs a possibly-open tx"
    );
    assert!(
        co.tainted(),
        "ANY Err from this method must force tainted so the recycle DISCARD ALLs a possibly-poisoned conn"
    );
}

/// REGRESSION (per-method Err-arm, `begin_tx_with`): `begin_tx_with` shares the byte-identical
/// Err-arm fail-safe (`if r.is_err() { self.tx_open = true; self.tainted = true; }`) with
/// `exec`/`query`/`tx_control`/`commit_tx`/`rollback_tx`, but until now only `exec`/`query` had a
/// direct test of it. `begin_tx_with` drives `PoolBackend::simple_query` under the hood exactly
/// like the others (verified by reading `pool.rs`), so the same `arm_fail_next_simple_query` hook
/// exercises it: the armed failure fires on the `BEGIN` statement itself, which the fake also
/// classifies as an Open verb (so `tx_open` would end up `true` via `apply_tx_status` alone) — the
/// bit this test isolates is `tainted`, which ONLY the force sets here. Removing the force from
/// `begin_tx_with` flips the `tainted` assertion to fail (see the fix report for the RED run).
#[tokio::test]
async fn err_arm_forces_cleanup_on_begin_tx_with() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");
    assert_fresh(&co);

    co.conn_mut().arm_fail_next_simple_query();
    let r = co.begin_tx_with(TxId(1), "BEGIN").await;
    assert!(
        matches!(r, Err(PoolError::Backend(_))),
        "the armed failure must surface as an Err, got {r:?}"
    );
    assert_forced(&co);
}

/// REGRESSION (per-method Err-arm, `tx_control`): same fail-safe as above, exercised through the
/// engine-only savepoint passthrough. `SAVEPOINT sp1` is a PRESERVE verb in the fake's model (a
/// real Postgres RFQ byte doesn't flip on a savepoint op either), so on a fresh (`Idle`) conn the
/// status read back after the armed failure is STILL `Idle` — i.e. `apply_tx_status` alone would
/// leave BOTH `tx_open` and `tainted` `false`. This is the cleanest of the four cases: removing the
/// force flips BOTH assertions in `assert_forced` to fail (full RED; see the fix report).
#[tokio::test]
async fn err_arm_forces_cleanup_on_tx_control() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");
    assert_fresh(&co);

    co.conn_mut().arm_fail_next_simple_query();
    let r = co.tx_control("SAVEPOINT sp1").await;
    assert!(
        matches!(r, Err(PoolError::Backend(_))),
        "the armed failure must surface as an Err, got {r:?}"
    );
    assert_forced(&co);
}

/// REGRESSION (per-method Err-arm, `commit_tx`): same fail-safe, exercised through `commit_tx`'s
/// hard-coded `"COMMIT"` statement. `COMMIT` is a Close verb in the fake's model, so the status
/// read back after the armed failure is `Idle` regardless of what preceded it — again both bits
/// would be `false` from `apply_tx_status` alone. Removing the force flips BOTH assertions (full
/// RED; see the fix report).
#[tokio::test]
async fn err_arm_forces_cleanup_on_commit_tx() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");
    assert_fresh(&co);

    co.conn_mut().arm_fail_next_simple_query();
    let r = co.commit_tx().await;
    assert!(
        matches!(r, Err(PoolError::Backend(_))),
        "the armed failure must surface as an Err, got {r:?}"
    );
    assert_forced(&co);
}

/// REGRESSION (per-method Err-arm, `rollback_tx`): same fail-safe, exercised through
/// `rollback_tx`'s hard-coded `"ROLLBACK"` statement (a bare ROLLBACK, so a Close verb — same
/// stale-`Idle`-after-error shape as `commit_tx` above). Removing the force flips BOTH assertions
/// (full RED; see the fix report).
#[tokio::test]
async fn err_arm_forces_cleanup_on_rollback_tx() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");
    assert_fresh(&co);

    co.conn_mut().arm_fail_next_simple_query();
    let r = co.rollback_tx().await;
    assert!(
        matches!(r, Err(PoolError::Backend(_))),
        "the armed failure must surface as an Err, got {r:?}"
    );
    assert_forced(&co);
}

/// An autocommit `exec`/`query` on a fresh conn (fake stays `Idle`) NEVER pins and NEVER taints.
#[tokio::test]
async fn autocommit_statement_never_pins() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("SELECT 1").await.expect("autocommit exec");
    assert!(!co.tx_open(), "an autocommit exec never opens a tx");
    assert!(!co.tainted());
    assert_eq!(co.pin_state(), PinState::Unpinned);
    assert_eq!(
        co.last_pin_cause(),
        None,
        "no tx observed => no Tx pin-cause"
    );

    co.query("SELECT 2", &[]).await.expect("autocommit query");
    assert!(!co.tx_open(), "an autocommit query never opens a tx");
    assert_eq!(co.pin_state(), PinState::Unpinned);
    assert_eq!(co.last_pin_cause(), None);
}
