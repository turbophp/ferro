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

/// B2b: the backend RECLAIM hook (`PoolBackend::reclaim_stream`) runs inside `finish` — BEFORE
/// `finalize_stream` reads `tx_status` — and its two arms are load-bearing.
///
/// **Clean arm (the default, unarmed):** `affected` is the stream's post-drain count and the conn
/// recycles normally. This is the byte-for-byte pre-hook behavior every other test in this file
/// relies on; asserted here against the SAME conn id to make the contrast with the failure arm
/// explicit.
///
/// **Failure arm (armed):** a conn-owning backend (MySQL, B2b-2) that cannot restore the driver
/// connection after the drain returns `Err` from `reclaim_stream` and leaves the conn
/// `is_closed`-dead. `finish` must then treat the stream as errored (force-taint) and the pool must
/// DISCARD the husk rather than recycle it — proven by the next checkout on a `max_size=1` pool
/// getting a FRESH connection id. Without this, a backend that failed to hand its connection back
/// would leave the next tenant a dead or mid-protocol session (the cross-tenant-leak class, charter
/// rule 6). No live MySQL needed — the fake models the conn-owning failure directly.
#[tokio::test]
async fn reclaim_hook_clean_recycles_but_a_failed_reclaim_discards_the_conn() {
    let script = || StreamScript {
        cols: vec![ColMeta {
            name: "n".to_string(),
            tag: tag::I64,
        }],
        rows: vec![row(1), row(2)],
        affected: 2,
        error_at: None,
    };

    // ── clean arm: default hook, conn recycles on the same id ──
    let backend = FakeBackend::new();
    backend.set_stream_script(script());
    let pool = Pool::new(
        backend,
        PoolConfig {
            max_size: 1,
            ..Default::default()
        },
    );
    let first_id = {
        let mut co = pool.checkout().await.expect("checkout");
        let id = co.conn().id;
        let end = co
            .query_stream("SELECT n FROM t", &[])
            .await
            .expect("open")
            .finish()
            .await
            .expect("finish");
        assert_eq!(
            end.affected, 2,
            "clean reclaim answers the post-drain count"
        );
        assert!(!co.tainted(), "a clean stream does not taint");
        id
    };
    let reused = pool.checkout().await.expect("re-checkout");
    assert_eq!(
        reused.conn().id,
        first_id,
        "clean reclaim recycles the same conn (max_size=1)"
    );
    drop(reused);

    // ── failure arm: reclaim Err marks the conn dead → the pool discards it ──
    let backend = FakeBackend::new();
    backend.set_stream_script(script());
    let pool = Pool::new(
        backend,
        PoolConfig {
            max_size: 1,
            ..Default::default()
        },
    );
    let failed_id = {
        let mut co = pool.checkout().await.expect("checkout");
        let id = co.conn().id;
        pool.backend().arm_reclaim_fail();
        let end = co
            .query_stream("SELECT n FROM t", &[])
            .await
            .expect("open")
            .finish()
            .await
            .expect("finish returns Ok even when reclaim fails — the stream itself completed");
        assert_eq!(
            end.affected, 0,
            "a failed reclaim reports 0 affected (the count could not be trusted)"
        );
        assert!(
            co.tainted(),
            "a failed reclaim force-taints (finish's errored arm)"
        );
        id
    };
    let fresh = pool
        .checkout()
        .await
        .expect("re-checkout after a failed reclaim");
    assert_ne!(
        fresh.conn().id,
        failed_id,
        "a failed reclaim DISCARDS the conn: the next checkout connects fresh, never inheriting \
         a dead/mid-protocol session (charter rule 6)"
    );
    assert!(!fresh.conn().closed, "and the fresh conn is live");
}

/// FB-3 (iteration-12 adversarial pass, HIGH): the reclaim step inside `finish()` is BOUNDED, so a
/// backend whose connection-restore round trip hangs can never strand the request's terminal frame.
///
/// Why this is the load-bearing property: `ferrod`'s stream producer awaits `handle.finish()`
/// UNRACED — it is the one backend-touching await there with no cancel/deadline arm (every row
/// pull and every send is raced). B2b-1 put a `reclaim_stream().await` inside `finish`, and a real
/// conn-owning backend restores its connection with a `COM_RESET_CONNECTION`-class round trip that
/// can hang indefinitely on a half-dead socket. Unbounded, that hang means the request NEVER emits
/// its single END frame — a charter-rule-4 violation, and the exact class the whole producer is
/// otherwise written defensively against.
///
/// The fake's armed reclaim parks forever and — deliberately — never marks the conn closed, because
/// a hung backend never gets to signal anything. The pool's bound is the only thing that returns.
/// `start_paused` drives the clock, so this asserts the BOUND, not wall-clock luck: the test would
/// hang forever if the timeout were removed.
#[tokio::test(start_paused = true)]
async fn a_hung_reclaim_cannot_strand_finish() {
    let backend = FakeBackend::new();
    backend.set_stream_script(StreamScript {
        cols: vec![ColMeta {
            name: "n".to_string(),
            tag: tag::I64,
        }],
        rows: vec![row(1), row(2)],
        affected: 2,
        error_at: None,
    });
    let bound = std::time::Duration::from_secs(5);
    let pool = Pool::new(
        backend,
        PoolConfig {
            max_size: 1,
            checkout_timeout: bound,
            ..Default::default()
        },
    );

    let mut co = pool.checkout().await.expect("checkout");
    pool.backend().arm_reclaim_hang();

    let started = tokio::time::Instant::now();
    let end = co
        .query_stream("SELECT n FROM t", &[])
        .await
        .expect("open")
        .finish()
        .await
        .expect("finish MUST return even though the backend's reclaim never completes");

    assert!(
        started.elapsed() >= bound,
        "the reclaim really did park until the bound elapsed"
    );
    assert_eq!(
        end.affected, 0,
        "a timed-out reclaim reports 0 affected — the count could not be trusted"
    );
    assert!(
        co.tainted(),
        "a timed-out reclaim force-taints, exactly like a failed one"
    );
}
