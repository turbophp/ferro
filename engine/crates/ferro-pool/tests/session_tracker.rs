//! M1-S6 Task 2 TDD: the SECOND (protocol-derived) assist signal — `PoolBackend::take_session_mutated`
//! wired into `Checkout` via the private `apply_session_tracker`, labelled
//! [`PinCause::SessionTracker`]. Deterministic `FakeBackend` coverage (the live proof against real
//! MySQL, where the OK-packet trackers actually fire — including inside a stored program — lands in
//! a later M1-S6 task once the `MysqlBackend` can connect).
//!
//! These tests assert the pin-cause DoD (`last_pin_cause()`) alongside `tainted()`, and the
//! assist-not-authority invariant: the tracker NEVER touches `pin_state()`/`tx_open()` (only RFQ —
//! `apply_tx_status` — does), and it never clobbers the authoritative `Tx` cause.
//!
//! ADDITIVITY: the `FakeBackend`'s mutation flag defaults to `false` (unarmed), so a conn that is
//! not explicitly `arm_session_mutated()`'d behaves exactly as it did before this task — the same
//! no-op the Postgres backend gets from the trait's default `take_session_mutated -> false`.

use ferro_pool::backend::PoolBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::Pool;

/// A session mutation the OK-packet tracker reports (armed on the conn) taints with
/// `PinCause::SessionTracker` — even though the SQL itself (`SELECT 1`) is one the assist lexer
/// classifies as benign. This is the whole point: the protocol tracker sees a mutation the lexer is
/// blind to. Assist-not-authority: the RFQ-owned bits stay exactly as a plain autocommit statement
/// would leave them (`Unpinned`, `!tx_open`).
#[tokio::test]
async fn session_tracker_mutation_taints_session_tracker() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    // Arm the OK-packet tracker for the next statement, then run a statement the lexer sees as
    // benign (a plain SELECT never classifies as a trigger).
    co.conn_mut().arm_session_mutated();
    co.exec("SELECT 1").await.expect("plain SELECT");

    assert!(
        co.tainted(),
        "a tracker-reported session mutation must taint for reuse-safety"
    );
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::SessionTracker),
        "pin-cause DoD: the OK-packet tracker labels SessionTracker"
    );
    assert_eq!(
        co.pin_state(),
        PinState::Unpinned,
        "assist-not-authority: the tracker must never touch pin state"
    );
    assert!(
        !co.tx_open(),
        "assist-not-authority: the tracker must never touch tx_open (RFQ stayed Idle)"
    );
}

/// The tracker also taints through the row-returning `query` path (not just `exec`), proving
/// `apply_session_tracker` is wired on every instrumented method.
#[tokio::test]
async fn session_tracker_mutation_taints_via_query_path() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.conn_mut().arm_session_mutated();
    co.query("SELECT 1", &[]).await.expect("query SELECT");

    assert!(co.tainted());
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::SessionTracker),
        "pin-cause DoD"
    );
}

/// ADDITIVITY / no-op proof: an UNARMED conn (the default) never taints via the new path and leaves
/// `last_pin_cause()` at `None` — byte-identical to the plain-SELECT path before this task, and to
/// what the Postgres backend gets from the default `take_session_mutated -> false`.
#[tokio::test]
async fn no_mutation_leaves_untainted_and_causeless() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    // Deliberately NOT armed.
    co.exec("SELECT 1").await.expect("plain SELECT");

    assert!(
        !co.tainted(),
        "no tracker mutation => the session-tracker path must not taint"
    );
    assert_eq!(
        co.last_pin_cause(),
        None,
        "no tracker, no lexer trigger, no tx observed => no pin-cause at all"
    );
}

/// The tracker NEVER clobbers the authoritative `Tx` cause: a session mutation observed INSIDE an
/// open transaction (the real MySQL "SET SESSION inside a proc, inside a tx" case) still ends with
/// `last_pin_cause() == Tx` — RFQ's authority is preserved — while `tainted()` is (additionally)
/// set. This is the ONE deliberate difference from `apply_classify` (which DOES overwrite the label).
#[tokio::test]
async fn session_tracker_never_clobbers_tx_authority() {
    let pool = Pool::new(FakeBackend::new(), PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx_with(TxId(1), "BEGIN").await.expect("begin");
    assert!(co.tx_open());
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));

    // A tracker-reported mutation on a statement that keeps the tx open (SELECT is a PRESERVE verb
    // in the fake's RFQ model, so tx_status stays InTx).
    co.conn_mut().arm_session_mutated();
    co.exec("SELECT 1").await.expect("mutating stmt inside tx");

    assert!(
        co.tx_open(),
        "RFQ authority: the tx is still open regardless of the tracker"
    );
    assert!(co.tainted(), "the tracker still adds reuse-safety taint");
    assert_eq!(
        co.last_pin_cause(),
        Some(PinCause::Tx),
        "the tracker must NOT overwrite the authoritative Tx cause"
    );
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(1)),
        "the real TxId is never touched by the tracker"
    );
}

/// Direct unit check of the trait surface: an unarmed `FakeConn` reports `false` from
/// `take_session_mutated` — the same default every non-tracker backend (Postgres) inherits.
#[tokio::test]
async fn take_session_mutated_defaults_false_for_fake() {
    let backend = FakeBackend::new();
    let mut conn = backend.connect().await.expect("connect");
    assert!(
        !backend.take_session_mutated(&mut conn),
        "an unarmed conn reports no mutation (the PG/default behavior)"
    );
    // Once armed, it reports true exactly once, then reverts to false (consumed).
    conn.arm_session_mutated();
    assert!(
        backend.take_session_mutated(&mut conn),
        "armed => true once"
    );
    assert!(
        !backend.take_session_mutated(&mut conn),
        "consumed on read => false again"
    );
}
