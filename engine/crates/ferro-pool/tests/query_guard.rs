//! S5 tests for the guarded, row-returning `Checkout::query` (BLOCKER-2). Mirrors
//! `pin_stub.rs::exec_rejects_bare_tx_control`: the guard runs `pin::is_bare_tx_control` FIRST, so
//! a bare `BEGIN`/`COMMIT`/`ROLLBACK` (leading comment/whitespace tolerant) is rejected with
//! `Unsupported` and NEVER reaches the backend — the mechanism that stops an `EXEC BEGIN` from
//! opening an untracked transaction the next tenant would inherit. A normal statement is delegated
//! and returns the `FakeBackend`'s canned `QueryResult`.

use ferro_pool::backend::QueryResult;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pool::Pool;
use ferro_proto::consts::tag;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

#[tokio::test]
async fn query_rejects_bare_tx_control_before_backend() {
    let backend = FakeBackend::new();
    let pool = Pool::new(backend, PoolConfig::default());
    let mut c = pool.checkout().await.expect("checkout should succeed");

    for sql in [
        "BEGIN",
        "commit;",
        "  RollBack  ",
        "START TRANSACTION",
        "/* c */ BEGIN",
        "-- c\nROLLBACK",
    ] {
        assert!(
            matches!(c.query(sql, &[]).await, Err(PoolError::Unsupported(_))),
            "bare tx-control via query() must be rejected: {sql:?}"
        );
    }

    assert!(
        c.conn().recorded.is_empty(),
        "a guard-rejected statement must NEVER reach the backend; recorded = {:?}",
        c.conn().recorded
    );
}

#[tokio::test]
async fn query_returns_canned_rows_for_normal_statement() {
    let backend = FakeBackend::new();
    // Arm a canned two-column, one-row result so the delegation path is observable.
    backend.set_query_result(QueryResult {
        cols: vec![
            ColMeta {
                name: "id".to_string(),
                tag: tag::I64,
            },
            ColMeta {
                name: "name".to_string(),
                tag: tag::TEXT,
            },
        ],
        rows: vec![vec![Value::I64(7), Value::Text("ferro".to_string())]],
        affected: 0,
    });
    let pool = Pool::new(backend, PoolConfig::default());
    let mut c = pool.checkout().await.expect("checkout should succeed");

    let result = c
        .query("SELECT id, name FROM t WHERE id = ?", &[Value::I64(7)])
        .await
        .expect("a normal statement should be delegated to the backend");

    assert_eq!(result.cols.len(), 2);
    assert_eq!(result.cols[0].tag, tag::I64);
    assert_eq!(
        result.rows,
        vec![vec![Value::I64(7), Value::Text("ferro".to_string())]]
    );
    assert_eq!(
        c.conn().recorded,
        vec!["SELECT id, name FROM t WHERE id = ?".to_string()],
        "the normal statement must have reached the backend exactly once"
    );
}
