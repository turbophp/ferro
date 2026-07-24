//! S4 Task 1 smoke tests for the `PoolBackend` trait + `FakeBackend`.
//!
//! These are deliberately backend-agnostic and fast (no real DB, no real sockets): the
//! fake in-memory backend is what lets `ferro-pool`'s mechanics (Task 2+) be tested without a
//! live Postgres. Task 1 only proves the trait shape + fake wiring.

use ferro_pool::backend::PoolBackend;
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
