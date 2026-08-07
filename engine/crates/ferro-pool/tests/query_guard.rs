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
use ferro_pool::pin::TxId;
use ferro_pool::pool::Pool;
use ferro_proto::consts::tag;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

/// A pool over a fresh `FakeBackend`.
fn fake_pool() -> Pool<FakeBackend> {
    Pool::new(FakeBackend::new(), PoolConfig::default())
}

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

/// The M1-S8a guard matrix, driven over ALL THREE guarded entries so a fix applied to one and not
/// the others is RED. Autocommit: everything tx-control-shaped is refused. In a transaction:
/// boundary verbs are still refused, savepoint verbs pass through on `exec`/`query`.
#[tokio::test]
async fn s8a_tx_control_guard_matrix_across_every_guarded_entry() {
    let boundary = [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "START TRANSACTION",
        "END",
        "ABORT",
        "PREPARE TRANSACTION 'x'",
    ];
    let savepoint = [
        "SAVEPOINT s1",
        "RELEASE SAVEPOINT s1",
        "RELEASE s1",
        "ROLLBACK TO SAVEPOINT s1",
        "ROLLBACK TO s1",
        "/* c */ SAVEPOINT s1",
        "-- c\nSAVEPOINT s1",
        "  SavePoint s1  ",
    ];

    // --- autocommit checkout: BOTH classes refused, on every entry.
    for sql in boundary.iter().chain(savepoint.iter()) {
        let pool = fake_pool();
        let mut co = pool.checkout().await.unwrap();
        assert!(!co.tx_open(), "a fresh checkout is autocommit");
        assert!(
            matches!(co.exec(sql).await, Err(PoolError::Unsupported(_))),
            "exec {sql:?}"
        );
        assert!(
            matches!(co.query(sql, &[]).await, Err(PoolError::Unsupported(_))),
            "query {sql:?}"
        );
        assert!(
            matches!(
                co.query_stream(sql, &[]).await.map(|_| ()),
                Err(PoolError::Unsupported(_))
            ),
            "query_stream {sql:?}"
        );
        assert!(
            co.conn().recorded.is_empty(),
            "a guard-rejected statement must NEVER reach the backend; recorded = {:?}",
            co.conn().recorded
        );
    }

    // --- in a transaction: boundary still refused, on every entry.
    for sql in boundary {
        let pool = fake_pool();
        let mut co = pool.checkout().await.unwrap();
        co.begin_tx_with(TxId(1), "BEGIN").await.unwrap();
        assert!(co.tx_open(), "BEGIN opened the transaction");
        assert!(
            matches!(co.exec(sql).await, Err(PoolError::Unsupported(_))),
            "a boundary verb stays refused inside a tx via exec(): {sql:?}"
        );
        assert!(
            matches!(co.query(sql, &[]).await, Err(PoolError::Unsupported(_))),
            "a boundary verb stays refused inside a tx via query(): {sql:?}"
        );
        assert!(
            matches!(
                co.query_stream(sql, &[]).await.map(|_| ()),
                Err(PoolError::Unsupported(_))
            ),
            "a boundary verb stays refused inside a tx via query_stream(): {sql:?}"
        );
        assert_eq!(
            co.conn().recorded,
            vec!["BEGIN".to_string()],
            "only the pin hook's own BEGIN reached the backend: {sql:?}"
        );
    }

    // --- in a transaction: savepoint verbs pass through on `exec` and `query`, and the
    // transaction is STILL open afterwards (the `FakeBackend` models `TxStatus` from the SQL via
    // `leading_tx_verb`, which classifies every savepoint verb as PRESERVE — so this assertion is a
    // real check of that model, not a tautology).
    for sql in savepoint {
        let pool = fake_pool();
        let mut co = pool.checkout().await.unwrap();
        co.begin_tx_with(TxId(1), "BEGIN").await.unwrap();

        co.exec(sql)
            .await
            .unwrap_or_else(|e| panic!("savepoint passthrough via exec() {sql:?}: {e}"));
        assert!(co.tx_open(), "a savepoint must NOT close the transaction");

        let r = co
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("savepoint passthrough via query() {sql:?}: {e}"));
        assert!(
            r.cols.is_empty() && r.rows.is_empty(),
            "a savepoint returns no columns and no rows: {sql:?} -> {r:?}"
        );
        assert!(co.tx_open(), "a savepoint must NOT close the transaction");

        assert_eq!(
            co.conn().recorded,
            vec!["BEGIN".to_string(), sql.to_string(), sql.to_string()],
            "BOTH passthroughs must have reached the backend verbatim: {sql:?}"
        );

        // ...but NOT via `query_stream`: a savepoint returns no result set, so there is nothing to
        // stream and the passthrough deliberately does not extend there.
        assert!(
            matches!(
                co.query_stream(sql, &[]).await.map(|_| ()),
                Err(PoolError::Unsupported(_))
            ),
            "a savepoint is refused on query_stream() even inside a tx: {sql:?}"
        );
    }
}

/// The two EXTRA conditions a savepoint passthrough must satisfy, both of them refusals. Without
/// them, relaxing the guard would be a way to reach the raw text protocol — which runs EVERY
/// statement in the string on both engines — with client-controlled SQL leading with a savepoint.
#[tokio::test]
async fn s8a_savepoint_passthrough_refuses_compound_statements_and_bound_params() {
    let pool = fake_pool();
    let mut co = pool.checkout().await.unwrap();
    co.begin_tx_with(TxId(1), "BEGIN").await.unwrap();

    // A boundary verb riding behind a savepoint is the vector this closes.
    for sql in [
        "SAVEPOINT s; COMMIT",
        "SAVEPOINT s;COMMIT",
        "ROLLBACK TO SAVEPOINT s; ROLLBACK",
        "RELEASE SAVEPOINT s; START TRANSACTION",
        "SAVEPOINT s; SELECT 1",
    ] {
        let e = co.exec(sql).await;
        assert!(
            matches!(e, Err(PoolError::Unsupported(_))),
            "a compound savepoint statement must be refused: {sql:?} -> {e:?}"
        );
        assert!(
            matches!(co.query(sql, &[]).await, Err(PoolError::Unsupported(_))),
            "a compound savepoint statement must be refused via query(): {sql:?}"
        );
    }

    // Params cannot ride the text protocol; accepting them would silently drop them.
    assert!(
        matches!(
            co.query("SAVEPOINT s1", &[Value::I64(1)]).await,
            Err(PoolError::Unsupported(_))
        ),
        "a savepoint statement with bound parameters must be refused"
    );

    // A trailing `;` is fine — that is one statement, not two.
    co.exec("SAVEPOINT s1;")
        .await
        .expect("a trailing semicolon is still a lone statement");

    assert_eq!(
        co.conn().recorded,
        vec!["BEGIN".to_string(), "SAVEPOINT s1;".to_string()],
        "only the BEGIN and the one accepted savepoint reached the backend"
    );
    assert!(co.tx_open(), "the transaction is still open");
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
        ..Default::default()
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
