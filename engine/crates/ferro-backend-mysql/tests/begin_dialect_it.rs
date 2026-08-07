//! M1-S8a Task 8 — the measured standalone-vs-batched `SET TRANSACTION` difference, pinned against
//! BOTH live engines. This is the load-bearing reason `ferrod::tx::actor::compose_begin_sql` emits a
//! BATCH for the MySQL isolation forms: a STANDALONE `SET TRANSACTION` returns an OK packet with
//! `SERVER_SESSION_STATE_CHANGED` and NO trackers, which the §7.1 rule
//! (`tracker::is_mutation` — `state_changed_flag && !has_sysvar && !has_txstate`) reads as a real
//! session mutation, so every isolation/readonly transaction would taint into a full
//! `COM_RESET_CONNECTION` at the next recycle. Batched, the FINAL OK packet carries a
//! `TransactionState` tracker, which gates that bare-flag path off.
//!
//! It also pins the third fact the composer depends on: the batched form actually OPENS the
//! transaction (`tx_status == InTx`) and its `READ ONLY` genuinely takes (a write inside is refused
//! — errno 1792 / SQLSTATE 25006 on both engines), and leaves NOTHING behind after the transaction
//! ends (no residual taint, and the session's own isolation default is unchanged — the
//! cross-tenant-leak class this whole slice must never open, charter rule 6).
//!
//! Every case SKIPS (does not fail) when its env var is unset, printing the lowercase `skip:` line
//! `ci/assert-no-skips.sh` greps for, so `cargo test --workspace` stays green offline while a LIVE
//! lane with a backend missing FAILS rather than reporting a green no-op:
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferro-backend-mysql --test begin_dialect_it -- --nocapture
//! ```

use ferro_backend_mysql::MysqlBackend;
use ferro_pool::backend::{PoolBackend, TxStatus};
use ferro_pool::error::PoolError;

/// Run `f` once per configured MySQL-family engine.
///
/// Prints `skip: <VAR> unset` per unconfigured engine — which is what the live lane's
/// `ci/assert-no-skips.sh` looks for, so a live run with a backend missing fails rather than
/// reporting a green no-op. No `ran` bookkeeping: the skip line IS the signal.
async fn each_engine<F, Fut>(f: F)
where
    F: Fn(String, &'static str) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for (var, label) in [
        ("FERRO_TEST_MYSQL_URL", "mysql"),
        ("FERRO_TEST_MARIADB_URL", "mariadb"),
    ] {
        match std::env::var(var) {
            Ok(url) => f(url, label).await,
            Err(_) => println!("skip: {var} unset"),
        }
    }
}

/// `take_session_mutated` is the ASSIST taint signal, read-and-cleared per statement. Asserting on
/// it directly (rather than on a `Checkout`'s pin cause, which the RFQ/`Tx` authority would mask) is
/// what makes this test falsifiable.
#[tokio::test]
async fn a_standalone_set_transaction_taints_but_the_batched_form_does_not() {
    each_engine(|url, label| async move {
        let backend = MysqlBackend::new(url);
        let mut conn = backend.connect().await.expect("connect");

        // Baseline: a plain SELECT taints nothing.
        backend.simple_query(&mut conn, "SELECT 1").await.unwrap();
        assert!(
            !backend.take_session_mutated(&mut conn),
            "[{label}] baseline must be clean"
        );

        // (a) STANDALONE — the form a naive two-statement implementation would emit.
        backend
            .simple_query(&mut conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        assert!(
            backend.take_session_mutated(&mut conn),
            "[{label}] a standalone SET TRANSACTION MUST taint — this is why the batch exists"
        );
        backend.simple_query(&mut conn, "ROLLBACK").await.ok();
        let _ = backend.take_session_mutated(&mut conn);

        // (b) BATCHED — what `compose_begin_sql(Dialect::MySql, Some(Serializable), true)` emits.
        backend
            .simple_query(
                &mut conn,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION READ ONLY",
            )
            .await
            .unwrap();
        assert!(
            !backend.take_session_mutated(&mut conn),
            "[{label}] the batched form must NOT taint (the final OK packet carries a \
             TransactionState tracker, which gates the bare-flag path off)"
        );
        assert_eq!(
            backend.tx_status(&conn),
            TxStatus::InTx,
            "[{label}] and it must actually open the transaction"
        );

        // ...and READ ONLY genuinely took. The SQLSTATE is asserted EXACTLY: a bare `is_some()` here
        // passed for ANY error — a typo in the DDL, a dropped connection, a syntax error — so it did
        // not actually pin the read-only property it claimed to (M1-S8a Task 8/9 review, F3).
        // MySQL/MariaDB report errno 1792 / SQLSTATE 25006 for a write in a read-only transaction.
        match backend
            .simple_query(&mut conn, "CREATE TEMPORARY TABLE s8a_ro (i INT)")
            .await
        {
            Err(PoolError::Sql {
                sqlstate, errno, ..
            }) => assert_eq!(
                sqlstate.as_deref(),
                Some("25006"),
                "[{label}] READ ONLY must be enforced with SQLSTATE 25006 (got errno {errno:?})"
            ),
            other => panic!(
                "[{label}] a write inside a READ ONLY transaction must be refused by the SERVER \
                 (a `PoolError::Sql` carrying SQLSTATE 25006), got {other:?}"
            ),
        }
        backend.simple_query(&mut conn, "ROLLBACK").await.ok();
    })
    .await;
}

/// The cross-tenant half (charter rule 6): the batched, next-transaction-only `SET TRANSACTION`
/// leaves NOTHING on the connection once the transaction ends — not a taint, and not the isolation
/// level itself. This is the guard that goes RED the moment anyone "repairs" the (correctly absent)
/// `@@transaction_isolation` reflection by emitting the `SESSION`-scoped form, which persists past
/// `COMMIT`/`ROLLBACK` and would be inherited by the next tenant of the pooled connection.
///
/// The session default is READ FIRST off this very connection rather than hard-coded, so the
/// assertion is a genuine before/after comparison on both engines (MySQL renders it
/// `REPEATABLE-READ`, MariaDB `REPEATABLE-READ` too, but neither literal is assumed here).
#[tokio::test]
async fn the_batched_isolation_never_survives_the_transaction() {
    each_engine(|url, label| async move {
        let backend = MysqlBackend::new(url);
        let mut conn = backend.connect().await.expect("connect");

        let before = read_isolation(&backend, &mut conn).await;
        let _ = backend.take_session_mutated(&mut conn);

        for (batch, ender) in [
            (
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION",
                "COMMIT",
            ),
            (
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
                "ROLLBACK",
            ),
        ] {
            backend.simple_query(&mut conn, batch).await.unwrap();
            assert_eq!(
                backend.tx_status(&conn),
                TxStatus::InTx,
                "[{label}] {batch:?} must open a transaction"
            );
            let _ = backend.take_session_mutated(&mut conn);

            backend.simple_query(&mut conn, ender).await.unwrap();
            assert_eq!(
                backend.tx_status(&conn),
                TxStatus::Idle,
                "[{label}] {ender} must close it"
            );
            assert!(
                !backend.take_session_mutated(&mut conn),
                "[{label}] no residual taint after {ender} of a batched {batch:?}"
            );

            let after = read_isolation(&backend, &mut conn).await;
            assert_eq!(
                after, before,
                "[{label}] the isolation level MUST NOT survive {ender} — a level that persists on \
                 a pooled connection is inherited by the next tenant (charter rule 6). If this is \
                 RED, someone emitted the SESSION-scoped form."
            );
        }
    })
    .await;
}

/// `SELECT @@session.transaction_isolation` off a raw backend connection.
///
/// Verification-only: the engine itself never reads this (the level is deliberately invisible here —
/// see `compose_begin_sql`'s doc comment). MariaDB 11.8 and MySQL 8.4 both expose
/// `@@transaction_isolation`; MariaDB's older `tx_isolation` alias is not used.
async fn read_isolation(
    backend: &MysqlBackend,
    conn: &mut <MysqlBackend as PoolBackend>::Conn,
) -> ferro_proto::value::Value {
    let r = backend
        .query(conn, "SELECT @@session.transaction_isolation", &[])
        .await
        .expect("read the session isolation level");
    r.rows
        .first()
        .and_then(|row| row.first())
        .cloned()
        .expect("one scalar row")
}
