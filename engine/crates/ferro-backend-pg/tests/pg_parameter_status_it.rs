//! Live `GUC_REPORT` `ParameterStatus` test for the M1-S1 tokio-postgres fork (task 2).
//!
//! Stock `tokio-postgres` tracks `ParameterStatus` messages only on the spawned `Connection`
//! (unreachable from the `Client` handle Ferro holds). The vendored fork (`vendor/tokio-postgres`)
//! mirrors the latest value of each `GUC_REPORT` parameter into a shared `Arc<Mutex<HashMap<...>>>`
//! and exposes it synchronously via `Client::parameter(name) -> Option<String>`. This is
//! ASSIST-ONLY (SPEC §7.1): the RFQ status byte (task 1) is the pin engine's authority;
//! `ParameterStatus` is input to the assist lexer (a later M1-S1 slice), never pin-state
//! authority by itself. `search_path` is NOT a GUC_REPORT param — this test does not rely on it.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — mirrors
//! `pg_pool_it.rs`/`pg_rfq_status_it.rs` so `cargo test --workspace` stays green offline.
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

/// `server_version` is reported at connection startup (before any query runs) — asserts it's
/// already visible via `Client::parameter()` right after checkout. `TimeZone` IS a GUC_REPORT
/// parameter (unlike most session `SET`s), so `SET TimeZone = 'UTC'` must update the same
/// synchronous accessor with no round trip needed to observe it.
#[tokio::test(flavor = "multi_thread")]
async fn parameter_reports_startup_and_updated_guc_report_values() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // Reported at startup, before any statement runs on this connection.
    assert!(
        co.conn().client.parameter("server_version").is_some(),
        "server_version is a GUC_REPORT param reported at connection startup"
    );

    // A non-GUC_REPORT setting (arbitrary custom GUC) must NOT show up here — this accessor only
    // ever reflects what the server actually reported via ParameterStatus.
    assert!(
        co.conn().client.parameter("ferro.not_a_real_guc").is_none(),
        "an unreported parameter must read back None"
    );

    // TimeZone IS GUC_REPORT: SET must be mirrored into Client::parameter() synchronously.
    co.conn_mut()
        .client
        .batch_execute("SET TimeZone = 'UTC'")
        .await
        .expect("SET TimeZone");
    assert_eq!(
        co.conn().client.parameter("TimeZone"),
        Some("UTC".to_string()),
        "TimeZone is a GUC_REPORT param — SET must update Client::parameter() with no round trip"
    );
}
