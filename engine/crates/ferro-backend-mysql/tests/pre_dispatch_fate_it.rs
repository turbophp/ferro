//! M1-S9a Task 9 — the MySQL/MariaDB half of the pre-dispatch proof.
//!
//! The plan left MySQL phase attribution OPEN ("verify at implementation whether the `prep()` error
//! is a distinct await that can take `.undispatched()`"). It is: `query::run` step 1 is
//! `conn.mysql.prep(sql).await` with its own `match`/early `return`, and `drain()` — the only
//! caller of `exec_iter`, i.e. the only thing that sends `COM_STMT_EXECUTE` — is unreachable unless
//! that returns `Ok`. So a prepare-phase loss provably never dispatched the statement, exactly as
//! PG's `prepare()` never reaches Bind/Execute.
//!
//! Structural reachability is not the claim, though — the claim is what a REAL server does, so this
//! file measures it: kill the pooled session from a side connection, wait until it is gone from
//! `information_schema.processlist`, then run an INSERT and read the row count back over the side
//! connection. Both engines, because MySQL 8.4 and MariaDB 11.8 are separate drivers' worth of
//! behaviour (the M0 review's own MariaDB comment-stripping surprise is the precedent).
//!
//! Direction rule (SPEC §19.3): a wrong `dispatched: false` LICENSES REPLAY of a possibly-applied
//! write. If either engine ever stopped surfacing this loss at the prepare step, the control test
//! below would still pass while the two positive ones went RED — which is the correct failure
//! shape: loud, and on the safe side.
//!
//! Every test SKIPS (does not fail) when its `FERRO_TEST_*_URL` is unset.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ferro_backend_mysql::MysqlBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;
use ferro_proto::value::Value;
use mysql_async::prelude::Queryable;

static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// Distinct per engine: the two entry points run in parallel against DIFFERENT servers, but a
/// shared name would still make each run's DDL race the other's on a re-run.
const TABLE: &str = "ferro_s9a_predispatch_my";

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

fn unique_key(prefix: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

/// A raw side connection — never the session under chaos.
async fn side_conn(url: &str) -> mysql_async::Conn {
    mysql_async::Conn::from_url(url)
        .await
        .expect("raw side connection")
}

/// `KILL <id>` from `side`, returning only once the session is gone from `processlist` — never
/// sleep-and-hope. (`KILL` on an already-dead id errors with 1094; both outcomes are fine, the poll
/// below is the real gate.)
async fn kill_and_await_death(side: &mut mysql_async::Conn, id: u32) {
    let _ = side.query_drop(format!("KILL {id}")).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let alive: Vec<u32> = side
            .query(format!(
                "SELECT id FROM information_schema.processlist WHERE id = {id}"
            ))
            .await
            .expect("processlist");
        if alive.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "session {id} never died"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// THE guard: a conn killed BEFORE the statement is prepared reports `dispatched: false`, and the
/// write is provably absent.
async fn pre_dispatch_loss_is_undispatched(url: &str, label: &str) {
    let mut side = side_conn(url).await;
    side.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {TABLE} (k VARCHAR(190))"
    ))
    .await
    .expect("setup ddl");

    let pool = Pool::new(MysqlBackend::new(url.to_string()), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // Learn the pooled session's id THROUGH the checkout, then kill it from the side.
    let id_res = co
        .query("SELECT CONNECTION_ID()", &[])
        .await
        .expect("connection id");
    let id = match id_res.rows[0][0] {
        Value::I64(v) => v as u32,
        Value::U64(v) => v as u32,
        ref other => panic!("[{label}] CONNECTION_ID() read back as {other:?}"),
    };
    kill_and_await_death(&mut side, id).await;

    let key = unique_key("my");
    let err = co
        .query(&format!("INSERT INTO {TABLE} VALUES ('{key}')"), &[])
        .await
        .expect_err("a query on a killed session must fail");
    assert_eq!(
        err,
        PoolError::ConnectionLost { dispatched: false },
        "[{label}] a COM_STMT_PREPARE-phase loss is a PROVABLE did-not-apply: `drain()` (the only \
         caller of `exec_iter`) is unreachable when `prep` returns Err"
    );

    let n: Vec<i64> = side
        .query(format!("SELECT COUNT(*) FROM {TABLE} WHERE k = '{key}'"))
        .await
        .expect("read-back");
    assert_eq!(n[0], 0, "[{label}] the write must not have applied");
}

/// The CONTROL that stops the guard above passing for the wrong reason: a loss on a statement that
/// really WAS in flight must stay `dispatched: true`. Without it, mapping every MySQL
/// `ConnectionLost` to `false` — the flat mutation — would look green.
///
/// The in-flight event is created with the `processlist` + `INFO LIKE` marker discipline
/// (`mysql_chaos_it.rs`), including the `COMMAND IN ('Execute','Query')` filter: a match on the
/// PREPARING phase is exactly the pre-dispatch case this test must NOT observe, and after this task
/// the two classify differently — so without the filter this control could silently assert the
/// wrong phase.
async fn an_in_flight_loss_is_still_dispatched(url: &str, label: &str) {
    let mut side = side_conn(url).await;
    let pool = Pool::new(MysqlBackend::new(url.to_string()), config(1));
    let mut co = pool.checkout().await.expect("checkout");
    let id_res = co
        .query("SELECT CONNECTION_ID()", &[])
        .await
        .expect("connection id");
    let id = match id_res.rows[0][0] {
        Value::I64(v) => v as u32,
        Value::U64(v) => v as u32,
        ref other => panic!("[{label}] CONNECTION_ID() read back as {other:?}"),
    };

    // A long SLEEP carrying a STRING-LITERAL marker (never a `/* comment */` — MariaDB strips
    // comments from `processlist.INFO`).
    let marker = unique_key("inflight");
    let sql = format!("SELECT SLEEP(10) WHERE '{marker}' <> ''");

    let killer = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let hits: Vec<u32> = side
                .query(format!(
                    "SELECT id FROM information_schema.processlist \
                     WHERE id = {id} AND COMMAND IN ('Execute','Query') \
                     AND INFO LIKE '%{marker}%'"
                ))
                .await
                .expect("processlist");
            if !hits.is_empty() {
                side.query_drop(format!("KILL {id}")).await.expect("kill");
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let err = co
        .query(&sql, &[])
        .await
        .expect_err("a killed in-flight statement must fail");
    assert!(
        killer.await.expect("killer task"),
        "[{label}] the statement was never observed IN FLIGHT — a kill that lands before dispatch \
         proves nothing (global constraint 2)"
    );
    assert_eq!(
        err,
        PoolError::ConnectionLost { dispatched: true },
        "[{label}] a loss on a DISPATCHED statement must stay Indeterminate-eligible (§19.3)"
    );
}

fn mysql_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MYSQL_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("skip: FERRO_TEST_MYSQL_URL unset");
            None
        }
    }
}

fn mariadb_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MARIADB_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("skip: FERRO_TEST_MARIADB_URL unset");
            None
        }
    }
}

#[tokio::test]
async fn mysql_pre_dispatch_fate() {
    let Some(url) = mysql_url() else { return };
    pre_dispatch_loss_is_undispatched(&url, "mysql").await;
    an_in_flight_loss_is_still_dispatched(&url, "mysql").await;
}

#[tokio::test]
async fn mariadb_pre_dispatch_fate() {
    let Some(url) = mariadb_url() else { return };
    pre_dispatch_loss_is_undispatched(&url, "mariadb").await;
    an_in_flight_loss_is_still_dispatched(&url, "mariadb").await;
}
