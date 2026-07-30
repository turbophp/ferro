//! S5 Task 3 tests for the INCREMENTAL, pull-based `Checkout::query_stream` -> `RowStreamHandle`
//! (constant memory, mandatory `finish`, `Drop` safety net). Deterministic — driven by the
//! `FakeBackend`'s scripted stream, no Docker. The load-bearing property under test: a streamed
//! connection returns to the pool CORRECTLY pinned/tainted on EVERY exit path (normal end,
//! mid-stream error, abandonment), so the next tenant never inherits a mid-protocol/aborted conn
//! (the cross-tenant leak, charter rule 6). The live-PG incremental proof lives in
//! `ferro-backend-pg/tests/pg_query_stream_it.rs`.

use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::{FakeBackend, StreamScript};
use ferro_pool::pool::Pool;
use ferro_proto::consts::tag;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

fn row(n: i64) -> Vec<Value> {
    vec![Value::I64(n)]
}

/// LAZINESS: `query_stream` produces NO rows up front — each row is pulled only when `next()` is
/// polled. The `FakeBackend`'s shared `stream_pulls` counter is `0` right after `query_stream` and
/// increments by exactly one per `next()`; that is the direct evidence the path is constant-memory
/// (the whole point of streaming vs the buffered `query`).
#[tokio::test]
async fn query_stream_yields_rows_incrementally_and_lazily() {
    let backend = FakeBackend::new();
    backend.set_stream_script(StreamScript {
        cols: vec![ColMeta {
            name: "n".to_string(),
            tag: tag::I64,
        }],
        rows: vec![row(1), row(2), row(3)],
        affected: 3,
        error_at: None,
    });
    let pool = Pool::new(backend, PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    let mut handle = co.query_stream("SELECT n FROM t", &[]).await.expect("open");
    assert_eq!(
        handle.cols(),
        &[ColMeta {
            name: "n".to_string(),
            tag: tag::I64
        }],
        "cols come from the prepared statement, available before any row is pulled"
    );
    assert_eq!(
        pool.backend().stream_pulls(),
        0,
        "LAZY: query_stream must not have pulled any row before next()"
    );

    assert_eq!(handle.next().await, Some(Ok(row(1))));
    assert_eq!(
        pool.backend().stream_pulls(),
        1,
        "one pull after first next()"
    );
    assert_eq!(handle.next().await, Some(Ok(row(2))));
    assert_eq!(pool.backend().stream_pulls(), 2);
    assert_eq!(handle.next().await, Some(Ok(row(3))));
    assert_eq!(pool.backend().stream_pulls(), 3);
    assert_eq!(handle.next().await, None, "None once exhausted");

    let end = handle.finish().await.expect("finish");
    assert_eq!(
        end.affected, 3,
        "affected is the command-tag count, post-drain"
    );
}

/// The GUARD, mirroring `query_guard.rs`: a bare tx-control statement is rejected with `Unsupported`
/// BEFORE the backend is reached (so it can never open an untracked tx the next tenant inherits —
/// charter rule 6). `recorded` staying empty proves nothing reached the backend.
#[tokio::test]
async fn query_stream_rejects_bare_tx_control_before_backend() {
    let backend = FakeBackend::new();
    let pool = Pool::new(backend, PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    for sql in [
        "BEGIN",
        "commit;",
        "  RollBack  ",
        "START TRANSACTION",
        "/* c */ BEGIN",
        "-- c\nROLLBACK",
    ] {
        assert!(
            matches!(
                co.query_stream(sql, &[]).await.map(|_| ()),
                Err(PoolError::Unsupported(_))
            ),
            "bare tx-control via query_stream() must be rejected: {sql:?}"
        );
    }

    assert!(
        co.conn().recorded.is_empty(),
        "a guard-rejected statement must NEVER reach the backend; recorded = {:?}",
        co.conn().recorded
    );
}

/// A mid-stream error surfaces through `next()` as `Err`, AND `finish()` leaves the conn tainted +
/// tx_open (the Rule-A force-taint) so the checkout-time recycle runs ROLLBACK + DISCARD ALL. Script
/// yields one row, then an error on the second pull.
#[tokio::test]
async fn mid_stream_error_taints_conn_on_finish() {
    let backend = FakeBackend::new();
    backend.set_stream_script(StreamScript {
        cols: vec![],
        rows: vec![row(1), row(2)],
        affected: 0,
        error_at: Some(1), // error after emitting exactly one row
    });
    let pool = Pool::new(backend, PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    {
        let mut handle = co.query_stream("SELECT n FROM t", &[]).await.expect("open");
        assert_eq!(handle.next().await, Some(Ok(row(1))), "first row is fine");
        assert!(
            matches!(handle.next().await, Some(Err(_))),
            "second pull yields the mid-stream error"
        );
        handle
            .finish()
            .await
            .expect("finish runs the terminal sequence");
    }

    assert!(
        co.tainted(),
        "a mid-stream error must force-taint (Rule A) so the recycle runs DISCARD ALL"
    );
    assert!(
        co.tx_open(),
        "a mid-stream error force-sets tx_open so the recycle runs a defensive ROLLBACK first"
    );
}

/// THE CROSS-TENANT-LEAK REGRESSION: a handle DROPPED WITHOUT `finish()` (abandonment — a panic,
/// an early return, a cancel that drops the handle) still leaves the `Checkout` tainted, via the
/// `Drop` safety net. Without the net, this partially-drained connection would recycle UNTAINTED
/// and the next tenant would inherit a mid-protocol conn.
#[tokio::test]
async fn abandoned_handle_without_finish_taints_checkout() {
    let backend = FakeBackend::new();
    backend.set_stream_script(StreamScript {
        cols: vec![],
        rows: vec![row(1), row(2), row(3)],
        affected: 3,
        error_at: None,
    });
    let pool = Pool::new(backend, PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    {
        let mut handle = co.query_stream("SELECT n FROM t", &[]).await.expect("open");
        // Pull ONE row then abandon the handle WITHOUT finishing — the conn is now partially drained.
        assert_eq!(handle.next().await, Some(Ok(row(1))));
        // handle drops here at end of scope, WITHOUT finish()
    }

    assert!(
        co.tainted(),
        "REGRESSION: an abandoned (un-finished) stream MUST taint the conn (the Drop safety net) — \
         a partially-drained conn recycling untainted is the cross-tenant leak (charter rule 6)"
    );
}

/// A clean stream that is fully drained and `finish()`ed leaves the conn NOT tainted and NOT
/// tx_open (RFQ `Idle` + no session mutation), and reports the correct `affected` — so it recycles
/// via the normal targeted profile, not a full DISCARD ALL.
#[tokio::test]
async fn clean_finished_stream_leaves_conn_unpinned() {
    let backend = FakeBackend::new();
    backend.set_stream_script(StreamScript {
        cols: vec![ColMeta {
            name: "n".to_string(),
            tag: tag::I64,
        }],
        rows: vec![row(10), row(20)],
        affected: 2,
        error_at: None,
    });
    let pool = Pool::new(backend, PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    let end = {
        let mut handle = co.query_stream("SELECT n FROM t", &[]).await.expect("open");
        assert_eq!(handle.next().await, Some(Ok(row(10))));
        assert_eq!(handle.next().await, Some(Ok(row(20))));
        assert_eq!(handle.next().await, None);
        handle.finish().await.expect("finish")
    };

    assert_eq!(end.affected, 2);
    assert!(
        !co.tainted(),
        "a clean finished stream must not taint the conn"
    );
    assert!(
        !co.tx_open(),
        "a clean finished stream must leave tx_open false (RFQ Idle)"
    );
}

/// `finish()` on its own (no explicit `next()` calls) DRAINS the remainder so the RFQ read is a
/// valid post-drain read and `affected` is correct — the producer may `finish` early (e.g. the
/// client closed the stream) and the conn must still recycle cleanly.
#[tokio::test]
async fn finish_drains_undrained_remainder() {
    let backend = FakeBackend::new();
    backend.set_stream_script(StreamScript {
        cols: vec![],
        rows: vec![row(1), row(2), row(3), row(4), row(5)],
        affected: 5,
        error_at: None,
    });
    let pool = Pool::new(backend, PoolConfig::default());
    let mut co = pool.checkout().await.expect("checkout");

    let end = {
        // Open, pull nothing, finish immediately: finish must drain all 5 scripted rows internally.
        let handle = co.query_stream("SELECT n FROM t", &[]).await.expect("open");
        handle.finish().await.expect("finish")
    };

    assert_eq!(
        end.affected, 5,
        "finish drained the full remainder before reading affected"
    );
    assert!(!co.tainted());
    assert!(!co.tx_open());
    // The fake pulled every scripted row plus the terminal None (6 polls) — proof finish drained.
    assert_eq!(pool.backend().stream_pulls(), 6);
}
