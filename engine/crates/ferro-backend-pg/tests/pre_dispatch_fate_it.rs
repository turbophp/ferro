//! M1-S9a Task 9 — the live proof: a statement on a connection that died BEFORE dispatch is a
//! provable did-not-apply, and the error now says so.
//!
//! The M0 core review reproduced this on PG 17 (`m0_fate_probe_it.rs`, run green and deleted):
//! check out, kill the backend from a side connection, then run an INSERT. `tokio-postgres` is
//! prepare-THEN-dispatch (`query.rs`: `prepare()` at step 2, `query_raw()` at step 5), so the loss
//! surfaces on the PREPARE round trip — Parse/Describe only, the Execute NEVER left the process —
//! and the row is provably absent afterwards. The service's `ctx.sent` is pre-built `true` at that
//! call site (it is honest about the CALL SITE: a checkout succeeded), so before this task §19.3
//! reported `WriteUnconfirmed{Indeterminate}` for a write that could not possibly have applied.
//! §19.3 reads `Retryable` when nothing was sent.
//!
//! `PoolError::ConnectionLost { dispatched }` carries the phase, and this file is the guard that
//! the PREPARE site really is marked pre-dispatch against a real server — not merely that
//! `undispatched()` exists.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — same discipline as
//! `pg_pool_it.rs`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ferro_backend_pg::{PgBackend, Value};
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;

static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// One table per test — these run in parallel, and a shared table would make each test's read-back
/// depend on the other's rows. The per-run `unique_key` already isolates rows; separate tables also
/// keep the setup DDL out of each other's way.
const TABLE_BUFFERED: &str = "ferro_s9a_predispatch_buffered";
const TABLE_STREAMED: &str = "ferro_s9a_predispatch_streamed";

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// A per-run unique row key, so concurrent runs against the shared testkit database can never see
/// each other's rows (the read-back below is the whole proof — a stale row would fake a failure).
fn unique_key(prefix: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

/// A raw side connection — never the session under chaos (global constraint 4: assert from a
/// vantage point where the property is observable).
async fn raw_connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("raw side connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// `CREATE TABLE IF NOT EXISTS`, tolerating the concurrent-create race. `IF NOT EXISTS` is NOT
/// race-free in PostgreSQL: two sessions creating the same table at the same instant leave one with
/// a `23505` on `pg_type_typname_nsp_index` (or a `42P07`). These tests run in parallel against a
/// SHARED testkit database, and this file's own two setup calls collided on the very first live
/// run — so the race is real here, not theoretical. The table's existence is what matters; who won
/// does not.
async fn ensure_table(side: &tokio_postgres::Client, name: &str) {
    match side
        .simple_query(&format!("CREATE TABLE IF NOT EXISTS {name} (k text)"))
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let sqlstate = e.as_db_error().map(|d| d.code().code().to_string());
            assert!(
                matches!(sqlstate.as_deref(), Some("23505") | Some("42P07")),
                "setup ddl failed for a reason other than the concurrent-create race: {e}"
            );
        }
    }
}

/// Kills `pid` from `side` and does not return until `pg_stat_activity` no longer lists it — the
/// server has really torn the session down. Never sleep-and-hope.
async fn terminate_and_await_death(side: &tokio_postgres::Client, pid: i32) {
    side.execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .expect("terminate");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = side
            .query("SELECT 1 FROM pg_stat_activity WHERE pid = $1", &[&pid])
            .await
            .expect("pg_stat_activity");
        if rows.is_empty() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "backend never died");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// THE finding-3 guard, on the BUFFERED path (`query::run`).
#[tokio::test]
async fn a_conn_dead_before_dispatch_is_undispatched_connection_lost_and_the_write_unapplied() {
    let Some(url) = test_url() else { return };
    let side = raw_connect(&url).await;
    ensure_table(&side, TABLE_BUFFERED).await;

    let pool = Pool::new(PgBackend::new(url.clone()), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // Learn the pinned conn's pid THROUGH the checkout, then kill it from the side.
    let pid_res = co.query("SELECT pg_backend_pid()", &[]).await.expect("pid");
    let Value::I64(pid) = pid_res.rows[0][0] else {
        panic!(
            "pg_backend_pid must read back as I64, got {:?}",
            pid_res.rows[0][0]
        );
    };
    terminate_and_await_death(&side, pid as i32).await;

    // The write on the dead conn: the PREPARE round trip fails — the INSERT's Execute was never
    // sent. The error must say `dispatched: false`, and §19.3 then reads Retryable, not
    // Indeterminate.
    let key = unique_key("buffered");
    let err = co
        .query(
            &format!("INSERT INTO {TABLE_BUFFERED} VALUES ('{key}')"),
            &[],
        )
        .await
        .expect_err("a query on a terminated backend must fail");
    assert_eq!(
        err,
        PoolError::ConnectionLost { dispatched: false },
        "a prepare-phase loss is a PROVABLE did-not-apply"
    );

    // And provably unapplied — read back over the SIDE connection, never the dead one.
    let n = side
        .query_one(
            &format!("SELECT count(*) FROM {TABLE_BUFFERED} WHERE k = $1"),
            &[&key],
        )
        .await
        .expect("read-back");
    assert_eq!(n.get::<_, i64>(0), 0, "the write must not have applied");
}

/// The SAME property on the STREAMING open path (`query::stream`) — the one M1-S8b filed as an
/// unreproduced sighting ("a stream OPEN whose terminal arrives before any HEAD mapped to
/// Indeterminate where §19.3 reads Retryable"). Its error path pre-builds `sent: true`, so a
/// checkout failure structurally cannot produce it; a prepare-phase loss on an already-checked-out
/// conn can, and does. Both PG entries are prepare-THEN-dispatch, so both must carry the mark —
/// keeping only the buffered one would leave the sighting's own path unfixed.
#[tokio::test]
async fn the_stream_open_path_also_reports_a_pre_dispatch_loss_as_undispatched() {
    let Some(url) = test_url() else { return };
    let side = raw_connect(&url).await;
    ensure_table(&side, TABLE_STREAMED).await;

    let pool = Pool::new(PgBackend::new(url.clone()), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let pid_res = co.query("SELECT pg_backend_pid()", &[]).await.expect("pid");
    let Value::I64(pid) = pid_res.rows[0][0] else {
        panic!(
            "pg_backend_pid must read back as I64, got {:?}",
            pid_res.rows[0][0]
        );
    };
    terminate_and_await_death(&side, pid as i32).await;

    let key = unique_key("streamed");
    // `RowStreamHandle` is deliberately not `Debug` (it owns a live producer), so unwrap by hand
    // rather than with `expect_err`.
    let err = match co
        .query_stream(
            &format!("INSERT INTO {TABLE_STREAMED} VALUES ('{key}') RETURNING k"),
            &[],
        )
        .await
    {
        Ok(_) => panic!("a stream open on a terminated backend must fail"),
        Err(e) => e,
    };
    assert_eq!(
        err,
        PoolError::ConnectionLost { dispatched: false },
        "the stream OPEN's prepare-phase loss is the same PROVABLE did-not-apply"
    );

    let n = side
        .query_one(
            &format!("SELECT count(*) FROM {TABLE_STREAMED} WHERE k = $1"),
            &[&key],
        )
        .await
        .expect("read-back");
    assert_eq!(n.get::<_, i64>(0), 0, "the write must not have applied");
}

/// The CONTROL that stops the two tests above passing for the wrong reason. If `error_map::map`
/// were changed to answer `dispatched: false` unconditionally — the flat mutation that would make
/// both of them green with no phase attribution at all — this test goes RED: a loss on a statement
/// that IS in flight (`pg_terminate_backend(pg_backend_pid())` kills the session running it, which
/// is a FATAL-severity error → `is_session_fatal` → `ConnectionLost`) must still be `dispatched:
/// true`. That direction is the safety-critical one: a wrong `false` licenses replay of a
/// possibly-applied write.
#[tokio::test]
async fn a_loss_on_an_in_flight_statement_is_still_dispatched() {
    let Some(url) = test_url() else { return };
    let pool = Pool::new(PgBackend::new(url.clone()), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // Self-terminating: the statement is unambiguously past PREPARE and executing when the session
    // dies, because IT is what kills the session. No side connection, no race.
    let err = co
        .query("SELECT pg_terminate_backend(pg_backend_pid())", &[])
        .await
        .expect_err("terminating one's own backend must surface as an error");
    assert_eq!(
        err,
        PoolError::ConnectionLost { dispatched: true },
        "a loss on a statement that WAS dispatched must stay Indeterminate-eligible (§19.3): the \
         conservative default is what keeps a possibly-applied write from being replayed"
    );
}
