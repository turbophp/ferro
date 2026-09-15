//! **C3-5: the incremental row stream, at pool level.**
//!
//! The claims worth testing here are the two `p2` singled out, plus the pairing C3-3e left behind:
//!
//! 1. Rows arrive incrementally and the producer PARKS — constant memory in the backend, not merely
//!    in the frames above it.
//! 2. The connection comes back usable, because the stream owned it for its whole life.
//! 3. `supports_row_streaming` and `query_stream` agree, which is what §22.2 (bh) asked to be
//!    asserted rather than remembered.
//!
//! SQLite needs no server, so all of this runs everywhere including CI.

use std::time::Duration;

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::backend::{Cancel, PoolBackend};
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;
use ferro_proto::value::Value;

fn pool_on(path: &std::path::Path) -> Pool<SqliteBackend> {
    let config = PoolConfig {
        max_size: 2,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(60),
        reap_interval: None,
        pin_functions: Vec::new(),
        pin_on_unknown: true,
    };
    Pool::new(
        SqliteBackend::new(format!("sqlite://{}", path.display()))
            .with_busy_timeout(config.checkout_timeout),
        config,
    )
}

/// A recursive CTE, so a large result needs no fixture rows on disk. `p2`'s generator.
fn seq_sql(n: usize) -> String {
    format!(
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < {n}) \
         SELECT x FROM seq"
    )
}

/// **The pairing C3-3e asked to be stated out loud, now on its other side.**
///
/// The capability and the implementation flipped in ONE change. A backend whose `query_stream`
/// works but whose capability still reads `false` would have `ferrod` refuse every stream before
/// checkout — the mirror of the defect (bh) caught, and just as invisible without this assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_capability_agrees_with_query_stream() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("cap.db"));

    assert!(
        pool.backend().supports_row_streaming(),
        "the capability must be TRUE now that query_stream is implemented — ferrod reads this one \
         method to refuse a fetch:stream BEFORE any checkout, so a false here refuses every stream \
         the backend can actually serve"
    );

    let mut co = pool.checkout().await.expect("checkout");
    let mut stream = co
        .query_stream(&seq_sql(3), &[])
        .await
        .expect("and query_stream really works, which is the other half of the pairing");
    let mut n = 0;
    while let Some(row) = stream.next().await {
        row.expect("row");
        n += 1;
    }
    stream.finish().await.expect("finish");
    assert_eq!(n, 3);
}

/// **Non-buffering, proven by INTERRUPTING a producer that must still be running.**
///
/// The obvious version of this test — take a few rows, sleep, then assert the next row is the one
/// that follows — is worthless, and writing it first is how that was established. Every assertion
/// in it holds just as well when the producer has buffered the entire result during the sleep, so
/// it passes under the very mutation it exists to catch. That is `p2`'s lesson arriving a second
/// time, on a test written knowing about it.
///
/// What discriminates is an action whose OUTCOME DEPENDS on the producer still being mid-statement:
/// fire the connection's interrupt after the idle period. If the producer is parked on the
/// capacity-1 channel around row 11, the interrupt cuts the statement short and the stream ends
/// early. If it has already run to completion, the interrupt lands on nothing and every one of the
/// 200 000 rows is still delivered.
///
/// This doubles as the cancel proof: `SQLITE_INTERRUPT` is the only mechanism that can stop a
/// statement on this backend (§22.2 (bg)), and here it is stopping a real one.
///
/// **`TOTAL` and the idle period are CALIBRATED, not picked.** A first attempt used 200 000 rows and
/// the mutation still passed — measured, the widened producer only reaches ~157 000 of them in
/// 250 ms, so the interrupt truncated the result either way and the test discriminated nothing. At
/// 50 000 a widened producer finishes the whole result in well under the idle window (measured:
/// complete at 250 ms, with the window here set to 400 ms for margin), so "did the interrupt find
/// anything to stop?" becomes a real question with two different answers.
///
/// MUTATION PROVEN: widening `ROW_CHANNEL_CAPACITY` from 1 to `TOTAL` delivers all 50 000 rows and
/// fails the truncation assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_producer_is_parked_mid_statement_not_running_ahead() {
    const TOTAL: usize = 50_000;
    const TAKE: usize = 10;

    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("stream.db"));
    let mut co = pool.checkout().await.expect("checkout");

    // Captured BEFORE the mutable borrow the stream takes, exactly as the version probe does.
    let cancel = co.cancel_handle();
    let mut stream = co.query_stream(&seq_sql(TOTAL), &[]).await.expect("stream");

    let mut got = Vec::with_capacity(TAKE);
    for _ in 0..TAKE {
        let row = stream.next().await.expect("row").expect("ok");
        got.push(row[0].clone());
    }
    assert_eq!(
        got,
        (1..=TAKE as i64).map(Value::I64).collect::<Vec<_>>(),
        "rows arrive in order"
    );

    // Ample wall-clock for an unconstrained producer to finish all TOTAL rows — see the
    // calibration note above; this is a measured margin, not a guess.
    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel().await;

    let mut seen = TAKE;
    let mut ended_with_error = false;
    while let Some(row) = stream.next().await {
        match row {
            Ok(_) => seen += 1,
            Err(_) => {
                ended_with_error = true;
                break;
            }
        }
    }

    assert!(
        seen < TOTAL,
        "NON-BUFFERING: the interrupt fired 400ms after only {TAKE} of {TOTAL} rows were taken, and \
         the stream still delivered {seen} — so the producer was NOT parked mid-statement, it had \
         already run the whole result into memory and the interrupt had nothing to stop"
    );
    assert!(
        seen >= TAKE,
        "sanity: the rows already taken cannot un-happen"
    );
    assert!(
        ended_with_error,
        "an interrupted statement ends in an error, not a clean end-of-rows"
    );
}

/// **The connection comes back, which is the half the POOL needs.**
///
/// `p2` proved the driver allows it; this proves the backend's `reclaim_stream` actually does it,
/// through a real `Checkout`. A connection that could not be handed back would mean one discarded
/// connection per stream — correct but ruinous, and invisible to any test that only reads rows.
///
/// MUTATION PROVEN: dropping the returned connection instead of unparking it fails this test AND
/// the abandonment one below, and only those two — the `SELECT 2` cannot run at all, because every
/// method on the checkout goes through the handle the reclaim was supposed to restore. (Written
/// first as a guess that it would still pass; running it said otherwise, which is why the guess is
/// not what this comment records.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_connection_is_usable_after_a_stream() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("reclaim.db"));
    let mut co = pool.checkout().await.expect("checkout");

    let mut stream = co.query_stream(&seq_sql(100), &[]).await.expect("stream");
    while let Some(row) = stream.next().await {
        row.expect("row");
    }
    let end = stream
        .finish()
        .await
        .expect("finish restores the connection");
    assert_eq!(end.affected, 0, "a SELECT changed nothing");

    // The SAME checkout is usable immediately afterwards — only possible if the handle really came
    // back, since every other method goes through it.
    let after = co.query("SELECT 2", &[]).await.expect("the conn is usable");
    assert_eq!(after.rows[0][0], Value::I64(2));
}

/// **Abandonment: a partially-read stream still returns its connection.**
///
/// The producer parks on the capacity-1 channel; `finish()` drains the remainder under its bound
/// and then reclaims. What is being checked is that abandoning early costs nothing permanent — the
/// connection is usable afterwards and the pool is not down one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_abandoned_stream_still_returns_its_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("abandon.db"));
    let mut co = pool.checkout().await.expect("checkout");

    let mut stream = co
        .query_stream(&seq_sql(50_000), &[])
        .await
        .expect("stream");
    for _ in 0..5 {
        stream.next().await.expect("row").expect("ok");
    }
    // Abandon: finish without reading the rest.
    stream.finish().await.expect("finish drains and reclaims");

    let after = co.query("SELECT 3", &[]).await.expect("the conn is usable");
    assert_eq!(after.rows[0][0], Value::I64(3));
}

/// **A statement that fails to prepare costs no connection.**
///
/// The error arrives from `query_stream` itself rather than as a first-row error, and the
/// connection is restored on that arm — so an ordinary syntax error does not discard a connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prepare_failure_returns_the_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pool = pool_on(&dir.path().join("badsql.db"));
    let mut co = pool.checkout().await.expect("checkout");

    // `expect_err` needs `Debug` on the Ok side and `RowStreamHandle` has none, so match instead.
    let err = match co.query_stream("SELECT FROM WHERE", &[]).await {
        Err(e) => e,
        Ok(_) => panic!("a syntax error must surface from query_stream, not mid-stream"),
    };
    assert!(
        !matches!(err, ferro_pool::error::PoolError::ConnectionLost),
        "a syntax error is not a lost connection: {err:?}"
    );

    let after = co.query("SELECT 4", &[]).await.expect("the conn is usable");
    assert_eq!(after.rows[0][0], Value::I64(4));
}
