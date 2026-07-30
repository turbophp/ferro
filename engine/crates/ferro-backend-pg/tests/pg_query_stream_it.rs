//! Live `ferro-backend-pg` INCREMENTAL `Checkout::query_stream` -> `RowStreamHandle` tests (S5 Task
//! 3) against a real Postgres. Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset —
//! mirrors `pg_query_it.rs` so `cargo test --workspace` stays green offline.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg
//! ```

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;
use ferro_proto::consts::tag;
use ferro_proto::value::Value;

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

/// THE HEADLINE: `SELECT generate_series(1, 1000)` yields all 1000 rows INCREMENTALLY through the
/// pull API — one `next()` per row, never collected into a full `Vec` by ferro (constant memory).
/// After `finish()` the conn is left clean/unpinned (`Idle`, not tainted) and the affected count is
/// the command-tag row count (1000).
#[tokio::test(flavor = "multi_thread")]
async fn query_stream_generate_series_1000_incremental() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let end = {
        let mut handle = co
            .query_stream("SELECT generate_series(1, 1000)", &[])
            .await
            .expect("open stream");

        assert_eq!(handle.cols().len(), 1, "one column");
        assert_eq!(
            handle.cols()[0].tag,
            tag::I64,
            "generate_series is int4 → canonical I64 tag"
        );

        // Pull every row one at a time, asserting each value in order — proof the rows arrive
        // incrementally through the pull API, not as one buffered Vec.
        let mut count = 0i64;
        while let Some(item) = handle.next().await {
            let row = item.expect("row must be Ok");
            count += 1;
            assert_eq!(
                row,
                vec![Value::I64(count)],
                "rows arrive in order, one per next()"
            );
        }
        assert_eq!(count, 1000, "all 1000 rows pulled incrementally");

        handle.finish().await.expect("finish")
    };

    assert_eq!(
        end.affected, 1000,
        "PG's SELECT command tag reports the row count (never a hardcoded 0)"
    );
    assert!(
        !co.tainted(),
        "a clean finished stream leaves the conn NOT tainted"
    );
    assert!(
        !co.tx_open(),
        "a clean finished stream leaves tx_open false (RFQ Idle)"
    );

    // The recycled conn is genuinely usable: check it out again and run a normal query.
    drop(co);
    let mut co2 = pool.checkout().await.expect("re-checkout recycled conn");
    let ok = co2
        .query("SELECT 1", &[])
        .await
        .expect("recycled conn usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

/// A REAL mid-stream error (integer division-by-zero at the 3rd row of a `generate_series`) surfaces
/// through `next()` as a SQLSTATE-preserving `Err` (via `error_map`), and `finish()` leaves the conn
/// TAINTED + tx_open (the Rule-A force-taint) so the checkout-time recycle runs ROLLBACK + DISCARD
/// ALL. The recycled conn is then still usable — proof the taint drove a real cleanup, not a leak.
#[tokio::test(flavor = "multi_thread")]
async fn query_stream_mid_stream_error_taints_and_recycles() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    {
        // 1/(n-3): n=1 → 0, n=2 → -1, n=3 → division by zero (SQLSTATE 22012) mid-stream.
        let mut handle = co
            .query_stream("SELECT 1 / (n - 3) FROM generate_series(1, 5) AS n", &[])
            .await
            .expect("open stream (error is deferred to execution, not open)");

        assert_eq!(handle.next().await, Some(Ok(vec![Value::I64(0)])));
        assert_eq!(handle.next().await, Some(Ok(vec![Value::I64(-1)])));
        match handle.next().await {
            Some(Err(PoolError::Sql { ref sqlstate, .. })) => {
                assert_eq!(
                    sqlstate.as_deref(),
                    Some("22012"),
                    "division-by-zero preserves its raw SQLSTATE"
                );
            }
            other => panic!("expected a mid-stream Sql error, got {other:?}"),
        }

        handle
            .finish()
            .await
            .expect("finish runs the terminal sequence");
    }

    assert!(
        co.tainted(),
        "a real mid-stream error must force-taint (Rule A) so the recycle runs DISCARD ALL"
    );

    // The taint drives a real recycle: the next checkout must still get a usable conn.
    drop(co);
    let mut co2 = pool
        .checkout()
        .await
        .expect("re-checkout after tainted recycle");
    let ok = co2
        .query("SELECT 1", &[])
        .await
        .expect("recycled conn usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

/// THE CROSS-TENANT-LEAK REGRESSION, LIVE: a handle ABANDONED mid-stream (dropped after pulling only
/// a few of 1000 rows, WITHOUT `finish()`) leaves the conn tainted (the `Drop` net), and the
/// checkout-time recycle then cleans the partially-drained conn so the NEXT tenant gets a usable
/// connection — never a mid-protocol one. This validates, against real Postgres, that the driver's
/// background drain of a dropped `RowStream` + the Drop-net taint + the recycle together recover the
/// exact connection (max_size == 1, so the re-checkout MUST reuse the same physical conn).
#[tokio::test(flavor = "multi_thread")]
async fn query_stream_abandoned_mid_stream_recycles_clean() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    {
        let mut handle = co
            .query_stream("SELECT generate_series(1, 1000)", &[])
            .await
            .expect("open stream");
        // Pull only a handful, then ABANDON — the conn is left mid-protocol with ~995 rows unread.
        for expected in 1..=5 {
            assert_eq!(handle.next().await, Some(Ok(vec![Value::I64(expected)])));
        }
        // handle drops here WITHOUT finish() — the Drop net must taint the conn.
    }

    assert!(
        co.tainted(),
        "REGRESSION: an abandoned (un-finished) pg stream MUST taint the conn (the Drop safety net)"
    );

    // The taint drives the recycle: the same physical conn is cleaned and handed out usable.
    drop(co);
    let mut co2 = pool
        .checkout()
        .await
        .expect("re-checkout the recycled abandoned conn");
    let ok = co2
        .query("SELECT 42", &[])
        .await
        .expect("recycled conn usable after an abandoned mid-stream");
    assert_eq!(ok.rows, vec![vec![Value::I64(42)]]);
}
