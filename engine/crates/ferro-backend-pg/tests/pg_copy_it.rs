//! Live `Checkout::copy_in`/`copy_out` (M3-D4) against a real PostgreSQL: the pool-level guard, the
//! pin/taint bookkeeping, the abort and `Drop` paths, and what each leaves for the NEXT checkout.
//! SKIPS without `FERRO_TEST_PG_URL`.
//!
//! Recycle tests read `pg_backend_pid()` on BOTH checkouts: a recycled connection keeps its pid, a
//! fresh dial does not. Without that, a test that "proves the connection is reusable" can be
//! silently served a fresh dial (the C3-4 lesson) and prove nothing.

use std::time::Duration;

use bytes::Bytes;
use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool};

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn pool(url: String) -> Pool<PgBackend> {
    Pool::new(
        PgBackend::new(url),
        PoolConfig {
            max_size: 1,
            checkout_timeout: Duration::from_secs(5),
            max_lifetime: Duration::from_secs(1800),
            reap_interval: None,
            ..PoolConfig::default()
        },
    )
}

async fn pid(co: &mut Checkout<PgBackend>) -> i64 {
    match &co.query("SELECT pg_backend_pid()::int8", &[]).await.unwrap().rows[0][0] {
        ferro_proto::value::Value::I64(n) => *n,
        other => panic!("pid: {other:?}"),
    }
}

async fn count(co: &mut Checkout<PgBackend>, table: &str) -> i64 {
    match &co
        .query(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .unwrap()
        .rows[0][0]
    {
        ferro_proto::value::Value::I64(n) => *n,
        other => panic!("count: {other:?}"),
    }
}

async fn fresh_table(pool: &Pool<PgBackend>, table: &str) {
    let mut co = pool.checkout().await.unwrap();
    co.exec(&format!(
        "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id int PRIMARY KEY, name text)"
    ))
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_in_then_copy_out_round_trips_and_leaves_the_connection_clean() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    fresh_table(&pool, "d4_pool_rt").await;
    let mut co = pool.checkout().await.unwrap();
    let mut data = Vec::new();
    for i in 0..1000 {
        data.extend_from_slice(format!("{i}\tname\\t{i}\n").as_bytes());
    }
    let end = {
        let mut h = co.copy_in("COPY d4_pool_rt (id, name) FROM STDIN").await.unwrap();
        for piece in data.chunks(777) {
            h.send(Bytes::copy_from_slice(piece)).await.unwrap();
        }
        h.finish().await.unwrap()
    };
    assert_eq!(end.affected, 1000);
    assert!(!co.tainted(), "a COPY is safe-listed and clean: no taint");
    assert!(!co.tx_open());

    let (out, end) = {
        let mut h = co
            .copy_out("COPY (SELECT id, name FROM d4_pool_rt ORDER BY id) TO STDOUT")
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(chunk) = h.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        (out, h.finish().await.unwrap())
    };
    assert_eq!(out, data, "byte-identical round trip, escapes included");
    assert_eq!(end.affected, 1000);
    assert!(!co.tainted());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shape_guard_refuses_before_anything_reaches_the_server() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    fresh_table(&pool, "d4_pool_guard").await;
    let mut co = pool.checkout().await.unwrap();
    co.exec("INSERT INTO d4_pool_guard VALUES (1, 'keep')").await.unwrap();

    // A non-COPY on the copy-in path would EXECUTE and COMMIT before the driver noticed.
    match co.copy_in("DELETE FROM d4_pool_guard").await {
        Err(PoolError::Unsupported(m)) => assert!(m.contains("FROM STDIN"), "{m}"),
        Err(e) => panic!("expected Unsupported, got {e:?}"),
        Ok(_) => panic!("a DELETE must not be accepted as a COPY"),
    }
    assert_eq!(count(&mut co, "d4_pool_guard").await, 1, "the DELETE never ran");
    // A COPY FROM on the copy-out path would leave the server waiting for data forever.
    assert!(matches!(
        co.copy_out("COPY d4_pool_guard FROM STDIN").await,
        Err(PoolError::Unsupported(_))
    ));
    assert!(matches!(
        co.copy_in("COPY d4_pool_guard FROM '/etc/passwd'").await,
        Err(PoolError::Unsupported(_))
    ));
    assert!(!co.tainted(), "a refusal touches nothing");
    assert_eq!(count(&mut co, "d4_pool_guard").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_applies_nothing_and_the_same_connection_is_recycled_usable() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    fresh_table(&pool, "d4_pool_abort").await;
    let first_pid = {
        let mut co = pool.checkout().await.unwrap();
        let p = pid(&mut co).await;
        {
            let mut h = co.copy_in("COPY d4_pool_abort (id, name) FROM STDIN").await.unwrap();
            let mut big = Vec::new();
            for i in 0..5000 {
                big.extend_from_slice(format!("{i}\tx\n").as_bytes());
            }
            h.send(Bytes::from(big)).await.unwrap();
            h.abort().await;
        }
        assert!(co.tainted(), "an aborted COPY recycles with the full reset");
        p
    };
    let mut co = pool.checkout().await.unwrap();
    assert_eq!(pid(&mut co).await, first_pid, "RECYCLED, not a fresh dial");
    assert_eq!(count(&mut co, "d4_pool_abort").await, 0, "nothing applied");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_handle_discards_the_connection() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    fresh_table(&pool, "d4_pool_drop").await;
    let first_pid = {
        let mut co = pool.checkout().await.unwrap();
        let p = pid(&mut co).await;
        let mut h = co.copy_in("COPY d4_pool_drop (id, name) FROM STDIN").await.unwrap();
        h.send(Bytes::from_static(b"1\tx\n")).await.unwrap();
        drop(h); // neither finished nor aborted
        p
    };
    let mut co = pool.checkout().await.unwrap();
    assert_ne!(pid(&mut co).await, first_pid, "a mid-COPY connection is never reused");
    assert_eq!(count(&mut co, "d4_pool_drop").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_row_is_the_servers_error_and_the_connection_recycles() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    fresh_table(&pool, "d4_pool_bad").await;
    let first_pid = {
        let mut co = pool.checkout().await.unwrap();
        let p = pid(&mut co).await;
        let mut h = co.copy_in("COPY d4_pool_bad (id, name) FROM STDIN").await.unwrap();
        h.send(Bytes::from_static(b"1\tok\nnot-an-int\tbad\n")).await.unwrap();
        match h.finish().await {
            Err(PoolError::Sql { sqlstate, .. }) => assert_eq!(sqlstate.as_deref(), Some("22P02")),
            other => panic!("expected the server's 22P02, got {other:?}"),
        }
        assert!(co.tainted());
        p
    };
    let mut co = pool.checkout().await.unwrap();
    assert_eq!(pid(&mut co).await, first_pid, "recycled");
    assert_eq!(count(&mut co, "d4_pool_bad").await, 0, "atomic: the good row did not land either");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_time_failure_reaches_the_pool_as_an_error() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    let mut co = pool.checkout().await.unwrap();
    co.exec(
        "DROP TABLE IF EXISTS d4_pool_c; DROP TABLE IF EXISTS d4_pool_p;
         CREATE TABLE d4_pool_p (id int PRIMARY KEY);
         CREATE TABLE d4_pool_c (id int, p int REFERENCES d4_pool_p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .await
    .unwrap();
    let r = {
        let mut h = co.copy_in("COPY d4_pool_c FROM STDIN").await.unwrap();
        h.send(Bytes::from_static(b"1\t999\n")).await.unwrap();
        h.finish().await
    };
    match r {
        Err(PoolError::Sql { sqlstate, .. }) => assert_eq!(sqlstate.as_deref(), Some("23503")),
        other => panic!("expected 23503 from the implicit COMMIT, got {other:?}"),
    }
    assert_eq!(count(&mut co, "d4_pool_c").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_error_is_known_and_recycles() {
    let Some(url) = test_url() else { return };
    let pool = pool(url);
    let first_pid = {
        let mut co = pool.checkout().await.unwrap();
        let p = pid(&mut co).await;
        match co.copy_in("COPY d4_no_such_table FROM STDIN").await {
            Err(PoolError::Sql { sqlstate, .. }) => assert_eq!(sqlstate.as_deref(), Some("42P01")),
            Err(e) => panic!("expected 42P01, got {e:?}"),
            Ok(_) => panic!("expected an error"),
        }
        p
    };
    let mut co = pool.checkout().await.unwrap();
    assert_eq!(pid(&mut co).await, first_pid, "a server error keeps the connection");
}
