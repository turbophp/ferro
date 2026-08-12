//! M1-S9a Task 1 — the finding-2 guard: the tx-scoped EXEC's `in_tx: true` (`services/sql.rs:332`)
//! becomes OBSERVABLE. The M0 core review mutated that field to `false` and the ENTIRE suite
//! stayed green, because no test anywhere kills the backend LINK during an in-tx statement over
//! the wire path (the in-tx chaos tests are all timeout/cancel-shaped and exit via
//! `ExecStep::Deadline`/the 57014 override, never via `classify_fate(ConnectionLost, in_tx:true)`).
//!
//! These tests create exactly that event on BOTH engine families and pin the §19.3 answer:
//! an in-tx statement link-loss is `CONNECTION_LOST{Retryable}` — the whole transaction is dead
//! and NOTHING persisted (proven by read-back), so replay is safe — and NEVER
//! `WRITE_UNCONFIRMED{Indeterminate}`.
//!
//! Task 8 appends the implicit-commit acceptance here: the one case where an in-tx failure must
//! STOP being Retryable, because earlier statements in the tx HAVE persisted.
//!
//! Every test SKIPS (does not fail) when its `FERRO_TEST_*_URL` is unset — same discipline as
//! `chaos_fate_it.rs`.
//!
//! ```text
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferrod --test in_tx_fate_it -- --nocapture
//! ```
//!
//! **Two harness rules this file inherits from `mysql_chaos_it.rs`, both live-verified there and
//! both load-bearing here (PLAN-VERIFY F2) — do not "simplify" either away:**
//!
//!  1. **The in-flight marker is a STRING LITERAL PREDICATE (`'<marker>' <> ''`), never a
//!     `/* comment */`.** MariaDB STRIPS comments from `information_schema.processlist.INFO`
//!     (MySQL 8 preserves them); both engines preserve a string literal. A comment marker would
//!     work on MySQL only and would silently foreclose ever aiming this guard at MariaDB — on a
//!     bug (`implicit commit inside an explicit transaction`) that is a MySQL **and MariaDB**
//!     family bug. So the marker form is the portable one, and the MySQL-family test below runs
//!     against BOTH engines.
//!
//!  2. **The processlist poll filters `COMMAND IN ('Execute','Query')`.** `INFO` also carries the
//!     statement text during `COM_STMT_PREPARE`, and ferrod's row path is prepare-THEN-execute, so
//!     a bare marker match can mean "the server is PREPARING this statement" — which is NOT in
//!     flight. Today both phases classify identically, so this test would pass either way; that is
//!     precisely why the filter must go in NOW. **After Task 9 lands `ConnectionLost{dispatched}`,
//!     a prep-phase kill becomes `dispatched: false` → `CONNECTION_LOST{Retryable}` — bit for bit
//!     the value this test asserts — so the `sql.rs:332` mutation would survive GREEN and the guard
//!     on the defining safety property would be dead, with nothing in this file or in Task 9
//!     looking wrong in isolation.** It is a time bomb, not a flake.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{TestClient, exec_err, exec_ok, exec_server, mariadb_url, mysql_url, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_sql, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse};
use ferro_proto::value::Value;

/// The persistent testkit fixture table. Shared with `chaos_fate_it`-style suites only by
/// convention: the row key is run-unique, so concurrent runs never collide on a row.
const TABLE: &str = "ferro_s9a_intx";

/// How long [`wait_for_active_conn`] waits for the marked statement to be observed EXECUTING.
/// Matches `mysql_chaos_it.rs`'s bound: generous on purpose, because the victim statement is a
/// 20-second `SLEEP` — once dispatched the EXECUTING state PERSISTS for far longer than this
/// bound, so only a genuinely stuck dispatch can burn it, and burning it is a loud panic (never a
/// silent pass on an unproven chaos event).
const IN_FLIGHT_GUARD_BOUND: Duration = Duration::from_secs(15);

static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// A per-test-run unique string — the counter-row key AND the processlist marker, so concurrent
/// runs on the shared testkit database can never collide or mis-target each other's statements.
/// Only `[A-Za-z0-9_]`, so it is safe embedded as a bare SQL string literal.
fn unique_key(prefix: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

/// A tx-scoped, write-declared EXEC request. `readonly = false` is load-bearing: under the
/// named mutation (`sql.rs:332` `in_tx: false`) the ConnectionLost classification falls through
/// to the `sent && !readonly && !in_tx` arm and becomes WRITE_UNCONFIRMED — which is exactly what
/// these tests must catch.
fn tx_req(sql: &str, tx_id: u64) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.tx_id = Some(tx_id);
    r
}

/// An autocommit, write-declared EXEC request (setup DDL / seed rows).
fn write_req(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r
}

/// Open a transaction via the TX service and return its `tx_id` (the `chaos_fate_it.rs:328`
/// helper, copied — `tests/*.rs` are separate crates and cannot import each other's helpers).
async fn begin(
    client: &mut TestClient,
    rid: u32,
    pool: &str,
    isolation: Option<u8>,
    readonly: bool,
) -> u64 {
    let breq = BeginRequest {
        pool: pool.to_string(),
        isolation,
        readonly,
    };
    client
        .send_request(rid, service::TX, method_tx::BEGIN, breq.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "BEGIN terminal echoes the rid");
    assert_eq!(t.header.flags & flags::END, flags::END);
    match Outcome::decode(&t.payload).expect("decode BEGIN Outcome") {
        Outcome::Ok(body) => {
            BeginResponse::decode(&body)
                .expect("decode BeginResponse")
                .tx_id
        }
        other => panic!("BEGIN expected Outcome::Ok(BeginResponse), got {other:?}"),
    }
}

/// The set of `(label, dsn)` MySQL-family targets under test — MySQL 8 and/or MariaDB 11,
/// whichever env var is set. Empty → the caller SKIPS (offline). Both set → the scenario runs
/// against both, which is only possible because of the string-literal marker (see the module doc).
fn mysql_targets() -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(u) = mysql_url() {
        out.push(("mysql", u));
    }
    if let Some(u) = mariadb_url() {
        out.push(("mariadb", u));
    }
    out
}

/// A raw side connection to the SAME MySQL/MariaDB, entirely OUTSIDE ferrod's pool — used only to
/// poll `information_schema.processlist` and to `KILL` the pinned connection. Resolves to the same
/// vendored `mysql_async` fork `ferro-backend-mysql` uses (workspace `[patch.crates-io]`).
async fn raw_mysql(url: &str) -> mysql_async::Conn {
    mysql_async::Conn::new(mysql_async::Opts::from_url(url).expect("mysql url"))
        .await
        .expect("in_tx_fate harness: raw side connection to MySQL")
}

/// Poll the processlist until a statement whose text carries `marker` is PROVABLY EXECUTING —
/// `COMMAND IN ('Execute','Query')`, never `Prepare` (module doc rule 2) — excluding this poll's
/// own connection, then return that connection's processlist id.
///
/// Panics loudly after [`IN_FLIGHT_GUARD_BOUND`]: a kill landing before dispatch is silently
/// ineffective, and a test built on it would pass for the wrong reason. The marker is bound as a
/// PARAMETER (never inlined into the poll SQL) so the poll's own row can never self-match;
/// `ID <> CONNECTION_ID()` excludes the poller as belt-and-braces.
async fn wait_for_active_conn(side: &mut mysql_async::Conn, marker: &str) -> u64 {
    use mysql_async::prelude::Queryable;
    let pattern = format!("%{marker}%");
    let deadline = Instant::now() + IN_FLIGHT_GUARD_BOUND;
    loop {
        // An IDLE conn has `INFO = NULL` (never matches `LIKE`), so the marker predicate excludes
        // idle threads on its own; the `COMMAND` predicate is what additionally excludes the
        // PREPARING-but-not-yet-executing phase.
        let id: Option<u64> = side
            .exec_first(
                "SELECT ID FROM information_schema.processlist \
                 WHERE ID <> CONNECTION_ID() AND INFO LIKE ? \
                   AND COMMAND IN ('Execute', 'Query')",
                (pattern.clone(),),
            )
            .await
            .expect("processlist poll");
        if let Some(id) = id {
            return id;
        }
        assert!(
            Instant::now() < deadline,
            "statement carrying marker {marker:?} was never observed EXECUTING in the processlist \
             within {IN_FLIGHT_GUARD_BOUND:?} — a kill fired now would prove nothing"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// PG half of the guard. `SELECT pg_terminate_backend(pg_backend_pid())` kills the session it
/// runs on: the statement is DISPATCHED (it executes), the backend dies mid-answer, tokio-postgres
/// surfaces a FATAL-severity error, `is_session_fatal` maps it to `PoolError::ConnectionLost`, and
/// the tx-scoped Err arm classifies it with `in_tx: true` — the branch this file exists to pin.
#[tokio::test]
async fn pg_in_tx_link_loss_is_connection_lost_retryable_never_indeterminate() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(1).await;

    exec_ok(
        &mut c,
        2,
        &write_req(&format!(
            "CREATE TABLE IF NOT EXISTS {TABLE} (k text PRIMARY KEY, n int NOT NULL)"
        )),
    )
    .await;
    let key = unique_key("pg_intx");

    let tx_id = begin(&mut c, 3, "default", None, false).await;
    exec_ok(
        &mut c,
        4,
        &tx_req(&format!("INSERT INTO {TABLE} VALUES ('{key}', 1)"), tx_id),
    )
    .await;

    let ep = exec_err(
        &mut c,
        5,
        &tx_req("SELECT pg_terminate_backend(pg_backend_pid())", tx_id),
    )
    .await;

    assert_eq!(
        ep.code,
        errc::CONNECTION_LOST,
        "an in-tx statement link-loss is CONNECTION_LOST (the whole tx is dead, §19.3), \
         got {:#06x}: {}",
        ep.code,
        ep.message
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "an in-tx statement loss with NOTHING persisted must never be reported Indeterminate"
    );

    // Retryable is HONEST: the INSERT ran inside the killed transaction, so nothing persisted.
    // Read back over a FRESH autocommit checkout (the dead conn is evicted at that checkout).
    let ok = exec_ok(
        &mut c,
        6,
        &req(&format!(
            "SELECT count(*)::int8 FROM {TABLE} WHERE k = '{key}'"
        )),
    )
    .await;
    assert_eq!(
        ok.rows[0][0],
        Value::I64(0),
        "the in-tx INSERT must have died with its transaction — Retryable licenses replay, so \
         this MUST be 0"
    );
}

/// MySQL-family half: the same event via a side-connection `KILL <processlist id>` fired while the
/// tx-scoped statement is PROVABLY EXECUTING. Runs against MySQL 8 AND MariaDB 11 — the blocker
/// this guard protects is a family bug, and the string-literal marker (module doc rule 1) is what
/// makes the MariaDB target reachable at all.
#[tokio::test]
async fn mysql_in_tx_link_loss_is_connection_lost_retryable_never_indeterminate() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        eprintln!("--- in-tx link loss: {label} ---");
        let mut side = raw_mysql(&url).await;
        let server = exec_server(url);
        let mut c = server.connect().await;
        c.hello(1).await;

        exec_ok(
            &mut c,
            2,
            &write_req(&format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} (k VARCHAR(128) PRIMARY KEY, n INT NOT NULL)"
            )),
        )
        .await;
        let key = unique_key("my_intx");

        let tx_id = begin(&mut c, 3, "default", None, false).await;
        exec_ok(
            &mut c,
            4,
            &tx_req(&format!("INSERT INTO {TABLE} VALUES ('{key}', 1)"), tx_id),
        )
        .await;

        // Dispatch the victim statement WITHOUT awaiting its terminal, prove it EXECUTING, kill it.
        // The marker rides a string-literal PREDICATE, not a comment: MariaDB strips comments from
        // `processlist.INFO` (module doc rule 1). `FROM DUAL` is what makes a `WHERE` legal on a
        // FROM-less SELECT in both engines.
        let marker = unique_key("my_intx_kill");
        let victim = tx_req(
            &format!("SELECT SLEEP(20) FROM DUAL WHERE '{marker}' <> ''"),
            tx_id,
        );
        c.send_request(5, service::SQL, method_sql::EXEC, victim.encode())
            .await;
        let id = wait_for_active_conn(&mut side, &marker).await;
        {
            use mysql_async::prelude::Queryable;
            side.query_drop(format!("KILL {id}")).await.expect("KILL");
        }

        let t = c.recv().await;
        assert_eq!(t.header.request_id, 5);
        assert_eq!(t.header.flags & flags::END, flags::END);
        let ep = match Outcome::decode(&t.payload).expect("decode Outcome") {
            Outcome::Error(ep) => ep,
            other => panic!("[{label}] expected Outcome::Error, got {other:?}"),
        };

        assert_eq!(
            ep.code,
            errc::CONNECTION_LOST,
            "[{label}] an in-tx statement link-loss is CONNECTION_LOST{{Retryable}}, \
             got {:#06x}: {}",
            ep.code,
            ep.message
        );
        assert_eq!(ep.branch, branch::RETRYABLE, "[{label}]");
        assert_ne!(ep.code, errc::WRITE_UNCONFIRMED, "[{label}]");

        let ok = exec_ok(
            &mut c,
            6,
            &req(&format!(
                "SELECT CAST(count(*) AS SIGNED) FROM {TABLE} WHERE k = '{key}'"
            )),
        )
        .await;
        assert_eq!(
            ok.rows[0][0],
            Value::I64(0),
            "[{label}] InnoDB rolls the killed connection's open transaction back — nothing may \
             persist"
        );
    }
}

/// M1-S9a Task 8 — THE measured at-least-once shape, now honestly reported. `BEGIN → INSERT →
/// CREATE TABLE` (the implicit commit: the INSERT persists, with no COMMIT ever sent) `→ a later
/// statement killed mid-flight`. The OLD engine answered `CONNECTION_LOST{Retryable}` — "the
/// transaction was rolled back, replaying it is safe" — licensing a replay that double-applies the
/// INSERT that is already durably there. The terminal must now be `WRITE_UNCONFIRMED{Indeterminate}`
/// naming the implicit commit, and the read-back over a FRESH checkout proves WHY.
///
/// The sibling above is the control that keeps this honest: the SAME kill on a plain-DML
/// transaction still answers `CONNECTION_LOST{Retryable}`, because nothing persisted there. The
/// latch/hazard fire only on implicit-commit shapes; they do not swallow the §19.3 in-tx branch.
///
/// Runs against MySQL 8 AND MariaDB 11 — the implicit commit is a family behaviour, and so is the
/// blocker.
#[tokio::test]
async fn mysql_in_tx_loss_after_an_implicit_commit_is_indeterminate_never_retryable() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        eprintln!("--- in-tx loss after an implicit commit: {label} ---");
        let mut side = raw_mysql(&url).await;
        let server = exec_server(url);
        let mut c = server.connect().await;
        c.hello(1).await;

        exec_ok(
            &mut c,
            2,
            &write_req(&format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} (k VARCHAR(128) PRIMARY KEY, n INT NOT NULL)"
            )),
        )
        .await;
        let key = unique_key("my_ic");
        let ddl_table = unique_key("ferro_s9a_ic");

        let tx_id = begin(&mut c, 3, "default", None, false).await;
        exec_ok(
            &mut c,
            4,
            &tx_req(&format!("INSERT INTO {TABLE} VALUES ('{key}', 1)"), tx_id),
        )
        .await;
        // The implicit commit: a real DDL inside the explicit transaction. MySQL/MariaDB commit the
        // open transaction BEFORE executing it, and the OK packet comes back with
        // SERVER_STATUS_IN_TRANS dropped — the signal the actor's latch reads.
        exec_ok(
            &mut c,
            5,
            &tx_req(&format!("CREATE TABLE {ddl_table} (x INT)"), tx_id),
        )
        .await;

        // Kill the pinned conn while a LATER statement is provably EXECUTING (the Task-1 machinery:
        // string-literal marker + COMMAND filter). This statement is NOT itself hazard-shaped, so
        // the ONLY thing that can mark the tx persisted is the latch the DDL armed.
        let marker = unique_key("my_ic_kill");
        let victim = tx_req(
            &format!("SELECT SLEEP(20) FROM DUAL WHERE '{marker}' <> ''"),
            tx_id,
        );
        c.send_request(6, service::SQL, method_sql::EXEC, victim.encode())
            .await;
        let id = wait_for_active_conn(&mut side, &marker).await;
        {
            use mysql_async::prelude::Queryable;
            side.query_drop(format!("KILL {id}")).await.expect("KILL");
        }

        let t = c.recv().await;
        assert_eq!(t.header.request_id, 6);
        assert_eq!(t.header.flags & flags::END, flags::END);
        let ep = match Outcome::decode(&t.payload).expect("decode Outcome") {
            Outcome::Error(ep) => ep,
            other => panic!("[{label}] expected Outcome::Error, got {other:?}"),
        };
        assert_eq!(
            ep.code,
            errc::WRITE_UNCONFIRMED,
            "[{label}] a loss after an implicit commit must be Indeterminate — Retryable here \
             licenses the measured double-apply. got {:#06x}: {}",
            ep.code,
            ep.message
        );
        assert_eq!(ep.branch, branch::INDETERMINATE, "[{label}]");
        assert!(
            ep.message.contains("implicit commit"),
            "[{label}] the terminal must name the mechanism, got: {}",
            ep.message
        );

        // WHY Retryable would have been a lie: the pre-DDL INSERT persisted, no COMMIT ever sent.
        let ok = exec_ok(
            &mut c,
            7,
            &req(&format!(
                "SELECT CAST(count(*) AS SIGNED) FROM {TABLE} WHERE k = '{key}'"
            )),
        )
        .await;
        assert_eq!(
            ok.rows[0][0],
            Value::I64(1),
            "[{label}] the implicit commit durably applied the pre-DDL INSERT — this is the \
             at-least-once mechanism the terminal above must confess to"
        );

        // Cleanup the per-run DDL table (autocommit).
        exec_ok(
            &mut c,
            8,
            &write_req(&format!("DROP TABLE IF EXISTS {ddl_table}")),
        )
        .await;
    }
}
