//! Live RFQ (`ReadyForQuery`) status-byte test for the M1-S1 tokio-postgres fork (task 1).
//!
//! Stock `tokio-postgres` parses the `ReadyForQuery` status byte (`I`/`T`/`E`) off the wire and
//! discards it — M0's pool pinned transactions via a stub because that byte was unreachable. The
//! vendored fork (`vendor/tokio-postgres`) stores it in an `Arc<AtomicU8>` shared between the
//! `Connection` driver and the `Client` handle, and exposes it synchronously via
//! `Client::transaction_status() -> u8`. This test drives a real connection through all three RFQ
//! states (`I`dle, `T`ransaction, `E`rror) and asserts the exposed byte tracks the server
//! authoritatively at each step — the foundation the pin engine (later M1-S1 tasks) consumes.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — mirrors `pg_pool_it.rs`/
//! `pg_query_it.rs` so `cargo test --workspace` stays green offline.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg
//! ```

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// Drives a real backend connection through idle -> tx -> failed-tx -> idle -> tx -> idle,
/// asserting `Client::transaction_status()` reports the authoritative RFQ byte at every step. Goes
/// straight at the raw `tokio_postgres::Client` (`PgConn::client` is `pub` for exactly this, per
/// `pg_pool_it.rs`'s `query_i32` helper) since this task is only about the fork surface, not the
/// pool's consumption of it (a later M1-S1 task).
#[tokio::test(flavor = "multi_thread")]
async fn transaction_status_tracks_rfq_i_t_e() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // A fresh connection with no open transaction reports idle.
    co.conn_mut()
        .client
        .batch_execute("SELECT 1")
        .await
        .expect("SELECT 1");
    assert_eq!(
        co.conn().client.transaction_status(),
        b'I',
        "no open transaction -> RFQ status 'I' (idle)"
    );

    // BEGIN opens a healthy transaction block.
    co.conn_mut()
        .client
        .batch_execute("BEGIN")
        .await
        .expect("BEGIN");
    assert_eq!(
        co.conn().client.transaction_status(),
        b'T',
        "an open, healthy transaction -> RFQ status 'T'"
    );

    // A failing statement inside the tx aborts it, but the connection/session survives (the RFQ
    // still comes back to us, now reporting the failed-tx state).
    let div_by_zero = co.conn_mut().client.batch_execute("SELECT 1/0").await;
    assert!(
        div_by_zero.is_err(),
        "division by zero must error the statement"
    );
    assert_eq!(
        co.conn().client.transaction_status(),
        b'E',
        "a failed statement inside an open tx -> RFQ status 'E'"
    );

    // ROLLBACK clears the failed transaction back to idle.
    co.conn_mut()
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("ROLLBACK");
    assert_eq!(
        co.conn().client.transaction_status(),
        b'I',
        "ROLLBACK of a failed tx -> RFQ status 'I'"
    );

    // A fresh, healthy transaction that COMMITs also lands back on idle.
    co.conn_mut()
        .client
        .batch_execute("BEGIN")
        .await
        .expect("BEGIN again");
    assert_eq!(co.conn().client.transaction_status(), b'T');

    co.conn_mut()
        .client
        .batch_execute(
            "CREATE TEMP TABLE ferro_m1s1_rfq_probe (id bigint); \
             INSERT INTO ferro_m1s1_rfq_probe (id) VALUES (1)",
        )
        .await
        .expect("DDL + INSERT inside the open tx");
    assert_eq!(
        co.conn().client.transaction_status(),
        b'T',
        "still inside the open tx after a successful INSERT -> RFQ status 'T'"
    );

    co.conn_mut()
        .client
        .batch_execute("COMMIT")
        .await
        .expect("COMMIT");
    assert_eq!(
        co.conn().client.transaction_status(),
        b'I',
        "COMMIT of a healthy tx -> RFQ status 'I'"
    );
}
