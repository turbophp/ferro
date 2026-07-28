//! Live test for `PoolBackend::tx_status` (M1-S1 Task 3) against a real Postgres — proves the
//! trait-level wiring (`TxStatus::from_pg_byte(conn.client.transaction_status())`), complementing
//! `pg_rfq_status_it.rs` (Task 1), which only asserts the raw `u8` the fork exposes tracks RFQ.
//! This test goes through `PoolBackend::tx_status` itself, the seam Task 4 will consume as the pin
//! authority.
//!
//! Skips (does not fail) when `FERRO_TEST_PG_URL` is unset, mirroring every other live test here.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg
//! ```

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::backend::{PoolBackend, TxStatus};
use ferro_pool::config::PoolConfig;
use ferro_pool::pin::TxId;
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
    }
}

/// Drives a real connection through idle -> tx -> failed-tx -> idle via the pool's own
/// `begin_tx`/`tx_control`/`rollback_tx`/`commit_tx` (not the raw client), asserting
/// `PoolBackend::tx_status` reports the mapped `TxStatus` at each step.
#[tokio::test(flavor = "multi_thread")]
async fn pg_backend_tx_status_tracks_idle_in_tx_and_failed() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Idle,
        "a fresh checkout with no open transaction reports Idle"
    );

    co.begin_tx(TxId(1)).await.expect("BEGIN via begin_tx");
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::InTx,
        "an open, healthy transaction reports InTx"
    );

    // A failing statement inside the tx aborts it without ending the session.
    let div_by_zero = co.tx_control("SELECT 1/0").await;
    assert!(
        div_by_zero.is_err(),
        "division by zero must error the statement"
    );
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Failed,
        "a failed statement inside an open tx reports Failed"
    );

    co.rollback_tx().await.expect("ROLLBACK");
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Idle,
        "ROLLBACK of a failed tx reports Idle"
    );

    co.begin_tx(TxId(2)).await.expect("BEGIN again");
    assert_eq!(pool.backend().tx_status(co.conn()), TxStatus::InTx);

    co.commit_tx().await.expect("COMMIT");
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Idle,
        "COMMIT of a healthy tx reports Idle"
    );
}
