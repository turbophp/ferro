//! M1-S2 Task 3 TDD: the assist lexer (`ferro-classify`) wired into `Checkout::{exec,query}` via
//! the private `apply_classify` — deterministic `FakeBackend` coverage (the live PG proof that the
//! taint actually closes the SET search_path/LISTEN/temp/advisory leaks lives in
//! `ferro-backend-pg/tests/pg_pool_it.rs`).
//!
//! Every test here asserts the pin-cause DoD (charter "Definition of done") — `last_pin_cause()` —
//! alongside `tainted()`, and (where relevant) the assist-not-authority invariant: the lexer NEVER
//! touches `pin_state()`/`tx_open()`, only RFQ (`apply_tx_status`) does.

use ferro_pool::backend::{PoolBackend, TxStatus};
use ferro_pool::config::PoolConfig;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::Pool;

/// A non-local `SET` on an autocommit (RFQ `Idle`) conn taints with `PinCause::Set` — and,
/// critically (the assist-not-authority invariant), leaves the RFQ-owned bits exactly as an
/// ordinary autocommit statement would: `Unpinned` and `!tx_open()`.
#[tokio::test]
async fn set_search_path_taints_set_and_never_touches_rfq_authority() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("SET search_path TO app")
        .await
        .expect("SET search_path");

    assert!(co.tainted(), "a non-local SET must taint");
    assert_eq!(co.last_pin_cause(), Some(PinCause::Set), "pin-cause DoD");
    assert_eq!(
        co.pin_state(),
        PinState::Unpinned,
        "assist-not-authority: the lexer must never touch pin state"
    );
    assert!(
        !co.tx_open(),
        "assist-not-authority: the lexer must never touch tx_open (RFQ stayed Idle)"
    );
}

/// `LISTEN` taints with `PinCause::Listen`.
#[tokio::test]
async fn listen_taints_listen() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.query("LISTEN ferro_test_chan", &[])
        .await
        .expect("LISTEN");

    assert!(co.tainted());
    assert_eq!(co.last_pin_cause(), Some(PinCause::Listen), "pin-cause DoD");
    assert_eq!(co.pin_state(), PinState::Unpinned);
    assert!(!co.tx_open());
}

/// A session-scoped advisory lock function taints with `PinCause::AdvisoryLock`.
#[tokio::test]
async fn advisory_lock_taints_advisory_lock() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.query("SELECT pg_advisory_lock(1)", &[])
        .await
        .expect("advisory lock");

    assert!(co.tainted());
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::AdvisoryLock),
        "pin-cause DoD"
    );
}

/// A raw client-side `PREPARE` taints with `PinCause::Prepare`.
#[tokio::test]
async fn prepare_taints_prepare() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("PREPARE s AS SELECT 1").await.expect("PREPARE");

    assert!(co.tainted());
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::Prepare),
        "pin-cause DoD"
    );
}

/// Temp-object DDL taints with `PinCause::Temp`.
#[tokio::test]
async fn create_temp_table_taints_temp() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("CREATE TEMP TABLE t(x int)")
        .await
        .expect("CREATE TEMP TABLE");

    assert!(co.tainted());
    assert_eq!(co.last_pin_cause(), Some(PinCause::Temp), "pin-cause DoD");
}

/// The exec-batch path is covered: a multi-statement `exec` whose SECOND statement is the trigger
/// still taints with the trigger's cause, proving `apply_classify` doesn't just look at the leading
/// keyword of the whole string.
#[tokio::test]
async fn multi_statement_exec_batch_taints_from_second_statement() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("SELECT 1; LISTEN c").await.expect("batch");

    assert!(co.tainted(), "a trigger anywhere in the batch must taint");
    assert_eq!(co.last_pin_cause(), Some(PinCause::Listen), "pin-cause DoD");
}

/// The common path must NOT be over-tainted: a plain `SELECT 1` never taints and leaves
/// `last_pin_cause()` at `None` (no assist cause was ever observed).
#[tokio::test]
async fn plain_select_never_taints() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("SELECT 1").await.expect("plain SELECT");

    assert!(!co.tainted(), "a plain SELECT must never taint");
    assert_eq!(
        co.last_pin_cause(),
        None,
        "no trigger, no tx observed => no pin-cause at all"
    );
}

/// The per-pool `pin_functions` escape hatch: a statement referencing a configured name taints
/// with `PinCause::PinFunction`, even though it isn't in `ferro-classify`'s built-in trigger set.
#[tokio::test]
async fn pin_functions_escape_hatch_taints_pin_function() {
    let config = PoolConfig {
        pin_functions: vec!["app_lock".to_string()],
        ..PoolConfig::default()
    };
    let pool = Pool::new(FakeBackend::new(), config);
    let mut co = pool.checkout().await.expect("checkout");

    co.query("SELECT app_lock(1)", &[])
        .await
        .expect("pin_functions statement");

    assert!(co.tainted());
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::PinFunction),
        "pin-cause DoD"
    );
}

/// `pin_on_unknown = true` (the default): an unrecognized/unclassifiable statement taints with
/// `PinCause::Unknown` — the conservative default (charter rule 5: prefer a false taint to a
/// missed one).
#[tokio::test]
async fn pin_on_unknown_true_taints_unknown() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("FLUFF x").await.expect("unclassifiable statement");

    assert!(
        co.tainted(),
        "pin_on_unknown=true must taint an unknown statement"
    );
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::Unknown),
        "pin-cause DoD"
    );
}

/// `pin_on_unknown = false`: the same unclassifiable statement must NOT taint.
#[tokio::test]
async fn pin_on_unknown_false_does_not_taint() {
    let config = PoolConfig {
        pin_on_unknown: false,
        ..PoolConfig::default()
    };
    let pool = Pool::new(FakeBackend::new(), config);
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("FLUFF x").await.expect("unclassifiable statement");

    assert!(
        !co.tainted(),
        "pin_on_unknown=false must NOT taint an unknown statement"
    );
    assert_eq!(co.last_pin_cause(), None);
}

/// A `SET` observed while the RFQ reports an open transaction (`InTx`) still ends with
/// `tx_open() == true` (RFQ's own authority, unaffected by the lexer) AND the MOST RECENT pin
/// cause is the assist cause (`Set`), not `Tx` — `apply_classify` runs after `apply_tx_status` and
/// `last_pin_cause` is documented as "most recently observed cause", not an exclusive state. Both
/// safety bits (`tainted`/`tx_open`) stay set either way.
#[tokio::test]
async fn set_inside_open_tx_keeps_tx_open_with_the_assist_cause() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx_with(TxId(1), "BEGIN").await.expect("begin");
    assert!(co.tx_open());
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));

    // A non-local SET while the tx is open: the fake's leading-tx-verb scan treats SET as a
    // PRESERVE verb (RFQ doesn't flip on a SET, matching real Postgres), so tx_status stays InTx.
    co.exec("SET search_path TO app")
        .await
        .expect("SET inside open tx");

    assert!(
        co.tx_open(),
        "RFQ authority: the tx is still open regardless of the assist lexer"
    );
    assert!(co.tainted());
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::Set),
        "the assist cause overwrites the RFQ's Tx cause as the MOST RECENTLY observed one"
    );
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(1)),
        "the real TxId is never touched by the assist lexer"
    );
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::InTx,
        "sanity: the fake's modeled RFQ byte really did stay InTx across the SET"
    );
}
