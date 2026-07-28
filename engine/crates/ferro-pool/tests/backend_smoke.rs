//! S4 Task 1 smoke tests for the `PoolBackend` trait + `FakeBackend`.
//!
//! These are deliberately backend-agnostic and fast (no real DB, no real sockets): the
//! fake in-memory backend is what lets `ferro-pool`'s mechanics (Task 2+) be tested without a
//! live Postgres. Task 1 only proves the trait shape + fake wiring.

use ferro_pool::backend::{PoolBackend, TxStatus};
use ferro_pool::error::{Branch, PoolError};
use ferro_pool::fake::FakeBackend;

#[tokio::test]
async fn fake_connect_ping_query() {
    let backend = FakeBackend::new();

    let mut conn = backend.connect().await.expect("connect should succeed");
    assert_eq!(conn.id, 0, "first connect() should yield conn id 0");

    backend.ping(&mut conn).await.expect("ping should succeed");
    assert!(!backend.is_closed(&conn), "fresh conn should not be closed");

    let affected = backend
        .simple_query(&mut conn, "select 1")
        .await
        .expect("simple_query should succeed");
    assert_eq!(affected, 0);
    assert_eq!(conn.recorded, vec!["select 1".to_string()]);
}

#[tokio::test]
async fn fake_arms_ping_failure() {
    let backend = FakeBackend::new();
    let mut conn = backend.connect().await.expect("connect should succeed");

    conn.arm_fail_next_ping();

    let err = backend
        .ping(&mut conn)
        .await
        .expect_err("armed ping should fail");
    assert!(matches!(err, PoolError::ConnectionLost));
    assert_eq!(err.taxonomy_branch(), Branch::Retryable);
}

#[tokio::test]
async fn fake_ids_increment() {
    let backend = FakeBackend::new();
    let conn0 = backend.connect().await.expect("connect should succeed");
    let conn1 = backend.connect().await.expect("connect should succeed");
    assert_eq!(conn0.id, 0);
    assert_eq!(conn1.id, 1);
}

// Task 3 (M1 pin engine): `FakeConn` must MODEL the RFQ status per-connection, not default to
// `Idle` unconditionally -- see `FakeConn::tx_status`/`fake::apply_leading_tx_verb` doc comments
// for why an unconditional `Idle` would clobber Task 4's `apply_tx_status` pin.

#[tokio::test]
async fn fake_conn_defaults_to_idle_on_checkout() {
    let backend = FakeBackend::new();
    let conn = backend.connect().await.expect("connect should succeed");
    assert_eq!(backend.tx_status(&conn), TxStatus::Idle);
    assert_eq!(conn.tx_status(), TxStatus::Idle);
}

#[tokio::test]
async fn fake_conn_reports_in_tx_after_recorded_begin() {
    let backend = FakeBackend::new();
    let mut conn = backend.connect().await.expect("connect should succeed");

    backend
        .simple_query(&mut conn, "BEGIN")
        .await
        .expect("simple_query(BEGIN) should succeed");

    assert_eq!(backend.tx_status(&conn), TxStatus::InTx);
}

#[tokio::test]
async fn fake_conn_reports_idle_after_commit_or_rollback() {
    let backend = FakeBackend::new();

    let mut committed = backend.connect().await.expect("connect should succeed");
    backend
        .simple_query(&mut committed, "BEGIN")
        .await
        .expect("simple_query(BEGIN) should succeed");
    backend
        .simple_query(&mut committed, "COMMIT")
        .await
        .expect("simple_query(COMMIT) should succeed");
    assert_eq!(backend.tx_status(&committed), TxStatus::Idle);

    let mut rolled_back = backend.connect().await.expect("connect should succeed");
    backend
        .simple_query(&mut rolled_back, "BEGIN")
        .await
        .expect("simple_query(BEGIN) should succeed");
    backend
        .simple_query(&mut rolled_back, "ROLLBACK")
        .await
        .expect("simple_query(ROLLBACK) should succeed");
    assert_eq!(backend.tx_status(&rolled_back), TxStatus::Idle);
}

#[tokio::test]
async fn fake_conn_begin_via_query_also_reports_in_tx() {
    // The guarded `Checkout::query` path never reaches here with a bare BEGIN (the pin guard
    // rejects it), but the pin hook itself (Task 4) may drive engine-composed tx-control through
    // either `simple_query` or `query` -- both must update the modeled status identically.
    let backend = FakeBackend::new();
    let mut conn = backend.connect().await.expect("connect should succeed");

    backend
        .query(&mut conn, "BEGIN", &[])
        .await
        .expect("query(BEGIN) should succeed");

    assert_eq!(backend.tx_status(&conn), TxStatus::InTx);
}

#[tokio::test]
async fn fake_conn_set_tx_status_drives_failed() {
    let backend = FakeBackend::new();
    let mut conn = backend.connect().await.expect("connect should succeed");

    conn.set_tx_status(TxStatus::Failed);

    assert_eq!(backend.tx_status(&conn), TxStatus::Failed);
    assert_eq!(conn.tx_status(), TxStatus::Failed);
}

#[tokio::test]
async fn fake_conn_status_per_connection_not_shared() {
    // A per-`FakeBackend` field would leak one connection's status into another's; per-`FakeConn`
    // must not.
    let backend = FakeBackend::new();
    let mut a = backend.connect().await.expect("connect should succeed");
    let b = backend.connect().await.expect("connect should succeed");

    backend
        .simple_query(&mut a, "BEGIN")
        .await
        .expect("simple_query(BEGIN) should succeed");

    assert_eq!(backend.tx_status(&a), TxStatus::InTx);
    assert_eq!(
        backend.tx_status(&b),
        TxStatus::Idle,
        "a second, untouched connection must not inherit the first connection's InTx status"
    );
}

#[test]
fn taxonomy_mapping() {
    assert_eq!(PoolError::Timeout.taxonomy_branch(), Branch::Retryable);
    assert_eq!(
        PoolError::ConnectionLost.taxonomy_branch(),
        Branch::Retryable
    );
    assert_eq!(PoolError::Closed.taxonomy_branch(), Branch::NonRetryable);
    assert_eq!(
        PoolError::Unsupported("bare tx-control".to_string()).taxonomy_branch(),
        Branch::NonRetryable
    );
    assert_eq!(
        PoolError::Backend("driver blew up".to_string()).taxonomy_branch(),
        Branch::NonRetryable
    );
}

#[test]
fn errc_mapping_matches_registry() {
    assert_eq!(
        PoolError::Timeout.errc(),
        ferro_proto::consts::errc::POOL_TIMEOUT
    );
    assert_eq!(
        PoolError::ConnectionLost.errc(),
        ferro_proto::consts::errc::CONNECTION_LOST
    );
    assert_eq!(
        PoolError::Unsupported("x".to_string()).errc(),
        ferro_proto::consts::errc::UNSUPPORTED
    );
    assert_eq!(
        PoolError::Closed.errc(),
        ferro_proto::consts::errc::PROTOCOL
    );
    assert_eq!(
        PoolError::Backend("x".to_string()).errc(),
        ferro_proto::consts::errc::PROTOCOL
    );
}
