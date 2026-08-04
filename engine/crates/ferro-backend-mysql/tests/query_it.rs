//! M1-S6 Task 4 — LIVE buffered data-path (`query` + `rowmap` + `bind` + `error_map`) for
//! MySQL/MariaDB, at PG parity.
//!
//! Proves — against real Dockerized MySQL 8 + MariaDB 11 — that:
//!   * every scoped scalar round-trips `query` → `rowmap` (SIGNED bigint, double, text, blob, bool,
//!     null), with the right `ColMeta` tags built off the prepared statement;
//!   * an out-of-scope column (`BIGINT UNSIGNED`, `DECIMAL`) is a LOUD `Unsupported` — never a silent
//!     miscast — raised before the query runs (conn stays clean);
//!   * a bind-arity mismatch is a KNOWN-FATE `Unsupported`, NEVER `ConnectionLost` (§19.3 safety);
//!   * `last_insert_id` is populated after an INSERT on a SIGNED `AUTO_INCREMENT`;
//!   * a duplicate key (errno 1062) is a generic SQL NonRetryable (`Unique`), NOT `Protocol`;
//!   * a real deadlock (two concurrent txs) → `error_map` → `Retryable` (what `classify_fate` passes
//!     through verbatim);
//!   * a statement error via a real `Checkout` force-taints the conn (`ferro-pool`'s Rule-A fires
//!     because `query` PROPAGATES the `Err`), and the pool recovers on the next checkout.
//!
//! EVERY assertion runs against BOTH engines (two entry-point fns sharing `run_query_suite`). Each
//! SKIPS cleanly without its env var (`FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL`).

use std::time::Duration;

use ferro_pool::backend::PoolBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::{Branch, PoolError};
use ferro_pool::pool::Pool;
use ferro_proto::consts::{branch, errc, tag};
use ferro_proto::value::Value;

use ferro_backend_mysql::MysqlBackend;

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// Every scoped scalar round-trips through `query` → `rowmap`, with correct `ColMeta` tags.
async fn scoped_scalars_round_trip(backend: &MysqlBackend, label: &str) {
    let mut conn = backend.connect().await.expect("connect");

    // DDL through the raw (text-protocol) path; a utf8mb4 VARCHAR so a non-ASCII TEXT round-trips
    // and its charset is NOT the binary collation (→ Text, not Bytes). BOOLEAN == TINYINT(1).
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_rt (a BIGINT, b DOUBLE, \
             c VARCHAR(255) CHARACTER SET utf8mb4, d BLOB, e BOOLEAN, f BIGINT)",
        )
        .await
        .expect("create temp table");

    let params = [
        Value::I64(-42),
        Value::F64(2.5),
        Value::Text("héllo".to_string()),
        Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
        Value::Bool(true),
        Value::Null,
    ];
    let ins = backend
        .query(
            &mut conn,
            "INSERT INTO ferro_rt (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
            &params,
        )
        .await
        .expect("insert scalars");
    assert_eq!(ins.affected, 1, "[{label}] one row inserted");
    assert!(ins.rows.is_empty(), "[{label}] an INSERT yields no rows");

    let r = backend
        .query(&mut conn, "SELECT a, b, c, d, e, f FROM ferro_rt", &[])
        .await
        .expect("select scalars");

    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![
            tag::I64,
            tag::F64,
            tag::TEXT,
            tag::BYTES,
            tag::BOOL,
            tag::I64
        ],
        "[{label}] cols map to the canonical scalar tags (BIGINT→I64, DOUBLE→F64, VARCHAR→TEXT, \
         BLOB→BYTES, BOOLEAN→BOOL, nullable BIGINT→I64)"
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::I64(-42),
            Value::F64(2.5),
            Value::Text("héllo".to_string()),
            Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
            Value::Bool(true),
            Value::Null,
        ]],
        "[{label}] every scoped scalar round-trips exactly (incl. a SIGNED bigint, a bool via \
         TINYINT(1), and a NULL)"
    );

    conn.mysql.disconnect().await.ok();
}

/// An out-of-scope column type is a LOUD `Unsupported`, raised before the query runs (conn stays
/// clean). Both `BIGINT UNSIGNED` (the deferred unsigned-64 policy) and `DECIMAL`.
async fn out_of_scope_column_is_unsupported(backend: &MysqlBackend, label: &str) {
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_oos (u BIGINT UNSIGNED, m DECIMAL(10, 2))",
        )
        .await
        .expect("create temp table");
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_oos (u, m) VALUES (?, ?)",
            &[Value::I64(1), Value::F64(3.5)],
        )
        .await
        .expect("insert (bind is fine; the READ types are what's out of scope)");

    for col in ["u", "m"] {
        let err = backend
            .query(&mut conn, &format!("SELECT {col} FROM ferro_oos"), &[])
            .await
            .unwrap_err();
        assert!(
            matches!(err, PoolError::Unsupported(_)),
            "[{label}] out-of-scope column `{col}` must be Unsupported (loud, never a miscast), got {err:?}"
        );
    }

    // The conn stayed clean (we errored during cols-build, before running the query).
    let ok = backend
        .query(&mut conn, "SELECT 1", &[])
        .await
        .expect("conn still usable after an Unsupported cols-build");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
    conn.mysql.disconnect().await.ok();
}

/// A bind-arity mismatch is a KNOWN-FATE `Unsupported`, NEVER `ConnectionLost` (§19.3). The cols
/// (an in-scope BIGINT) build fine, so the arity check is what fails — proving arity, not a column
/// type, is the isolated cause.
async fn bind_arity_mismatch_is_known_fate(backend: &MysqlBackend, label: &str) {
    let mut conn = backend.connect().await.expect("connect");
    // one placeholder, zero params.
    let err = backend
        .query(&mut conn, "SELECT id FROM ferro_smoke WHERE id = ?", &[])
        .await
        .unwrap_err();
    match err {
        PoolError::Sql {
            code, branch: b, ..
        } => {
            assert_eq!(
                code,
                errc::UNSUPPORTED,
                "[{label}] arity mismatch → Unsupported"
            );
            assert_eq!(b, branch::NON_RETRYABLE);
        }
        PoolError::ConnectionLost => panic!(
            "[{label}] REGRESSION: an arity mismatch classified ConnectionLost (fate-unknown) — \
             the false-Indeterminate defect"
        ),
        other => panic!("[{label}] expected Sql{{Unsupported}}, got {other:?}"),
    }
    // Never sent → conn untouched, still usable.
    let ok = backend
        .query(&mut conn, "SELECT 1", &[])
        .await
        .expect("usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
    conn.mysql.disconnect().await.ok();
}

/// `last_insert_id` is populated after an INSERT on a SIGNED `AUTO_INCREMENT` column.
async fn last_insert_id_after_insert(backend: &MysqlBackend, label: &str) {
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_ai \
             (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, note VARCHAR(50))",
        )
        .await
        .expect("create temp table");

    let r = backend
        .query(
            &mut conn,
            "INSERT INTO ferro_ai (note) VALUES (?)",
            &[Value::Text("first".to_string())],
        )
        .await
        .expect("insert");
    assert_eq!(r.affected, 1, "[{label}] one row inserted");
    assert_eq!(
        conn.last_insert_id(),
        Some(1),
        "[{label}] last_insert_id is the fresh AUTO_INCREMENT id (1)"
    );

    // A second insert bumps it.
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_ai (note) VALUES (?)",
            &[Value::Text("second".to_string())],
        )
        .await
        .expect("insert 2");
    assert_eq!(
        conn.last_insert_id(),
        Some(2),
        "[{label}] AUTO_INCREMENT advanced"
    );
    conn.mysql.disconnect().await.ok();
}

/// A duplicate key (errno 1062) is a generic SQL NonRetryable (`Unique`), NEVER `Protocol` — the T3
/// fallback fix. `ferro_smoke` id=1 is seeded, so re-inserting it collides.
async fn duplicate_key_is_unique_nonretryable(backend: &MysqlBackend, label: &str) {
    let mut conn = backend.connect().await.expect("connect");
    let err = backend
        .query(
            &mut conn,
            "INSERT INTO ferro_smoke (id, note) VALUES (?, ?)",
            &[Value::I64(1), Value::Text("dup".to_string())],
        )
        .await
        .unwrap_err();
    match err {
        PoolError::Sql {
            code,
            branch: b,
            ref sqlstate,
            ..
        } => {
            assert_eq!(code, errc::UNIQUE, "[{label}] a duplicate key → Unique");
            assert_ne!(
                code,
                errc::PROTOCOL,
                "[{label}] must NOT be Protocol (T3 fix)"
            );
            assert_eq!(b, branch::NON_RETRYABLE);
            assert_eq!(
                sqlstate.as_deref(),
                Some("23000"),
                "[{label}] raw SQLSTATE preserved"
            );
        }
        other => panic!("[{label}] expected Sql{{Unique}}, got {other:?}"),
    }
    // A statement error is not a session end — the conn survives.
    let ok = backend
        .query(&mut conn, "SELECT 1", &[])
        .await
        .expect("usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
    conn.mysql.disconnect().await.ok();
}

/// A real deadlock (two concurrent txs each locking a row, then crossing) → `error_map` →
/// `Sql{branch: RETRYABLE}` → `taxonomy_branch() == Retryable`. Since `classify_fate` passes a
/// `Sql`'s branch through verbatim (proven in `fate.rs`), the pool-level `Retryable` is the
/// end-to-end §9.2 proof.
async fn deadlock_two_txs_is_retryable(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);

    // Fresh InnoDB table with two rows to cross-lock.
    let mut setup = backend.connect().await.expect("connect setup");
    backend
        .simple_query(&mut setup, "DROP TABLE IF EXISTS ferro_dl")
        .await
        .expect("drop");
    backend
        .simple_query(
            &mut setup,
            "CREATE TABLE ferro_dl (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB",
        )
        .await
        .expect("create");
    backend
        .simple_query(
            &mut setup,
            "INSERT INTO ferro_dl (id, v) VALUES (1, 0), (2, 0)",
        )
        .await
        .expect("seed");

    // conn A: lock row 1. conn B: lock row 2.
    let mut a = backend.connect().await.expect("connect A");
    let mut b = backend.connect().await.expect("connect B");
    backend
        .simple_query(&mut a, "START TRANSACTION")
        .await
        .expect("A begin");
    backend
        .simple_query(&mut b, "START TRANSACTION")
        .await
        .expect("B begin");
    backend
        .query(&mut a, "UPDATE ferro_dl SET v = v + 1 WHERE id = 1", &[])
        .await
        .expect("A locks row 1");
    backend
        .query(&mut b, "UPDATE ferro_dl SET v = v + 1 WHERE id = 2", &[])
        .await
        .expect("B locks row 2");

    // A now reaches for row 2 (held by B) on a spawned task — it BLOCKS.
    let url_a = url.to_string();
    let a_task = tokio::spawn(async move {
        let backend_a = MysqlBackend::new(url_a);
        let res = backend_a
            .query(&mut a, "UPDATE ferro_dl SET v = v + 1 WHERE id = 2", &[])
            .await;
        (a, res)
    });

    // Let A park on B's lock, then B reaches for row 1 (held by A) → a cycle → InnoDB 1213 to one.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let b_res = backend
        .query(&mut b, "UPDATE ferro_dl SET v = v + 1 WHERE id = 1", &[])
        .await;
    let (mut a, a_res) = a_task.await.expect("A task join");

    // Exactly one side is the deadlock victim (a retryable Sql error); the other succeeded.
    let victims: Vec<&PoolError> = [&a_res, &b_res]
        .into_iter()
        .filter_map(|r| r.as_ref().err())
        .collect();
    assert_eq!(
        victims.len(),
        1,
        "[{label}] exactly one tx is the deadlock victim (a={a_res:?}, b={b_res:?})"
    );
    let victim = victims[0];
    match victim {
        PoolError::Sql { branch: br, .. } => assert_eq!(
            *br,
            branch::RETRYABLE,
            "[{label}] the deadlock victim's Sql error is RETRYABLE (a={a_res:?}, b={b_res:?})"
        ),
        other => panic!("[{label}] deadlock victim must be a Sql error, got {other:?}"),
    }
    assert_eq!(
        victim.taxonomy_branch(),
        Branch::Retryable,
        "[{label}] end-to-end: a deadlock coarsens to Retryable (what classify_fate passes through)"
    );

    // Cleanup: roll back the survivor, drop the table.
    backend.simple_query(&mut a, "ROLLBACK").await.ok();
    backend.simple_query(&mut b, "ROLLBACK").await.ok();
    backend
        .simple_query(&mut setup, "DROP TABLE IF EXISTS ferro_dl")
        .await
        .ok();
    a.mysql.disconnect().await.ok();
    b.mysql.disconnect().await.ok();
    setup.mysql.disconnect().await.ok();
}

/// A statement error via a real `Checkout` force-taints the conn: `query` PROPAGATES the `Err`, so
/// `ferro-pool`'s backend-agnostic Rule-A sets `tx_open` + `tainted`. Then the pool RECOVERS on the
/// next checkout (the recycle rolls back + resets the tainted conn).
async fn checkout_force_taint_on_query_error(url: &str, label: &str) {
    let pool = Pool::new(MysqlBackend::new(url), config(1));

    let mut co = pool.checkout().await.expect("checkout");
    // A duplicate key (ferro_smoke id=1 is seeded) → the backend `query` returns Err.
    let err = co
        .query(
            "INSERT INTO ferro_smoke (id, note) VALUES (?, ?)",
            &[Value::I64(1), Value::Text("dup".to_string())],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, PoolError::Sql { .. }),
        "[{label}] a duplicate key is a known-fate Sql error, got {err:?}"
    );
    // THE POINT: `query`'s Err propagation triggers Rule-A — the conn is left tx_open + tainted.
    assert!(
        co.tainted(),
        "[{label}] Rule-A force-taints the conn on a `query` Err (proves Err propagates)"
    );
    assert!(
        co.tx_open(),
        "[{label}] Rule-A sets tx_open on a `query` Err (conservative reuse-safety)"
    );

    // Release + re-checkout (max_size 1 → the SAME conn, now recycled): the tainted conn was rolled
    // back + reset, so it is usable again.
    drop(co);
    let mut co2 = pool
        .checkout()
        .await
        .expect("re-checkout the recycled conn");
    let ok = co2
        .query("SELECT 1", &[])
        .await
        .expect("recycled conn usable");
    assert_eq!(
        ok.rows,
        vec![vec![Value::I64(1)]],
        "[{label}] pool recovered"
    );
}

async fn run_query_suite(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    scoped_scalars_round_trip(&backend, label).await;
    out_of_scope_column_is_unsupported(&backend, label).await;
    bind_arity_mismatch_is_known_fate(&backend, label).await;
    last_insert_id_after_insert(&backend, label).await;
    duplicate_key_is_unique_nonretryable(&backend, label).await;
    deadlock_two_txs_is_retryable(url, label).await;
    checkout_force_taint_on_query_error(url, label).await;
    println!("[{label}] Task-4 buffered data-path suite PASSED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_query_data_path() {
    let Ok(url) = std::env::var("FERRO_TEST_MYSQL_URL") else {
        eprintln!("SKIP mysql_query_data_path: FERRO_TEST_MYSQL_URL unset");
        return;
    };
    run_query_suite(&url, "MYSQL").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mariadb_query_data_path() {
    let Ok(url) = std::env::var("FERRO_TEST_MARIADB_URL") else {
        eprintln!("SKIP mariadb_query_data_path: FERRO_TEST_MARIADB_URL unset");
        return;
    };
    run_query_suite(&url, "MARIADB").await;
}
