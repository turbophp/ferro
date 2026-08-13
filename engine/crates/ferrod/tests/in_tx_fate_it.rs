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
//!  2. **The processlist poll filters `COMMAND IN ('Execute','Query')`** — see
//!     [`ACTIVE_CONN_POLL_SQL`], the one place it is written. `INFO` also carries the statement text
//!     during `COM_STMT_PREPARE`, and ferrod's row path is prepare-THEN-execute, so a bare marker
//!     match can mean "the server is PREPARING this statement" — which is NOT in flight.
//!     **This is load-bearing TODAY, not after some future change:** `ferro-backend-mysql`'s
//!     `query::run` marks the prepare arm `.undispatched()` (`query.rs:59`, M1-S9a Task 9 — landed),
//!     so a prep-phase kill classifies `dispatched: false` → `CONNECTION_LOST{Retryable}`, bit for
//!     bit the value the two KILL tests below assert. A poll that matched a PREPARING connection
//!     would therefore make them pass under the very `services/sql.rs` `in_tx: true → false`
//!     mutation they exist to catch, with nothing in this file looking wrong in isolation.
//!     The predicate has its own guard —
//!     [`the_poll_filter_never_mistakes_a_parked_prepare_for_an_in_flight_statement`] — so deleting
//!     it is RED, not green. (The S9a whole-branch review measured the un-guarded state: deleting
//!     the predicate left this file 3/3 GREEN on all three engines.)

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

/// **The one in-flight predicate this file has** — every poll below goes through it, and
/// [`the_poll_filter_never_mistakes_a_parked_prepare_for_an_in_flight_statement`] pins it, so
/// deleting `AND COMMAND IN ('Execute', 'Query')` here is RED, not green (module doc rule 2).
///
/// An IDLE conn has `INFO = NULL` (never matches `LIKE`), so the marker predicate excludes idle
/// threads on its own; the `COMMAND` predicate is what additionally excludes the
/// PREPARING-but-not-yet-executing phase. The marker is bound as a PARAMETER (never inlined into
/// the poll SQL) so the poll's own row can never self-match; `ID <> CONNECTION_ID()` excludes the
/// poller as belt-and-braces.
const ACTIVE_CONN_POLL_SQL: &str = "SELECT ID FROM information_schema.processlist \
     WHERE ID <> CONNECTION_ID() AND INFO LIKE ? AND COMMAND IN ('Execute', 'Query')";

/// ONE application of [`ACTIVE_CONN_POLL_SQL`]: the processlist id of a connection whose current
/// statement text carries `marker` **and is dispatched**, or `None`.
async fn poll_active_conn(side: &mut mysql_async::Conn, marker: &str) -> Option<u64> {
    use mysql_async::prelude::Queryable;
    side.exec_first(ACTIVE_CONN_POLL_SQL, (format!("%{marker}%"),))
        .await
        .expect("processlist poll")
}

/// Poll the processlist until a statement whose text carries `marker` is PROVABLY EXECUTING —
/// `COMMAND IN ('Execute','Query')`, never `Prepare` (module doc rule 2) — excluding this poll's
/// own connection, then return that connection's processlist id.
///
/// Panics loudly after [`IN_FLIGHT_GUARD_BOUND`]: a kill landing before dispatch is silently
/// ineffective, and a test built on it would pass for the wrong reason.
async fn wait_for_active_conn(side: &mut mysql_async::Conn, marker: &str) -> u64 {
    let deadline = Instant::now() + IN_FLIGHT_GUARD_BOUND;
    loop {
        if let Some(id) = poll_active_conn(side, marker).await {
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

/// The UNFILTERED vantage: `(id, COMMAND, STATE)` of whichever connection currently carries
/// `marker` in its statement text — with NO `COMMAND` predicate at all.
///
/// This is deliberately NOT derived from [`ACTIVE_CONN_POLL_SQL`]: the parked-prepare guard needs
/// an independent observer that can still SEE the row the filtered poll must reject, so that
/// "filtered says None" is provably the `COMMAND` predicate discriminating and not the row having
/// vanished.
async fn poll_marked_conn(
    side: &mut mysql_async::Conn,
    marker: &str,
) -> Option<(u64, String, Option<String>)> {
    use mysql_async::prelude::Queryable;
    side.exec_first(
        "SELECT ID, COMMAND, STATE FROM information_schema.processlist \
         WHERE ID <> CONNECTION_ID() AND INFO LIKE ?",
        (format!("%{marker}%"),),
    )
    .await
    .expect("processlist diagnostic poll")
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

/// How long the parked-prepare guard KEEPS asserting once the shape is established. The reviewer
/// measured 19/19 hits over 2s on both engines; a window this long makes a one-off sample
/// impossible and still costs ~2s.
const PARKED_PREPARE_OBSERVE_WINDOW: Duration = Duration::from_secs(2);
/// Minimum number of poll iterations that must observe the prepare STILL parked inside that
/// window. Without a floor the loop could pass vacuously (zero iterations, or a prepare that
/// escaped after one).
const PARKED_PREPARE_MIN_HITS: usize = 10;

/// **The guard for module-doc rule 2** — the `COMMAND IN ('Execute','Query')` predicate in
/// [`ACTIVE_CONN_POLL_SQL`], which is the ONLY thing keeping the two KILLs above inside the
/// EXECUTE phase.
///
/// Why this must exist: `ferro-backend-mysql`'s `query::run` marks the `COM_STMT_PREPARE` arm
/// `.undispatched()` (`query.rs:59`, M1-S9a Task 9), so a kill landing during the PREPARE phase
/// classifies `CONNECTION_LOST{Retryable}` — **bit for bit the value the two tests above assert**.
/// A prep-phase match therefore does not merely prove nothing; it makes those tests pass under the
/// exact `services/sql.rs` `in_tx: true → false` mutation they exist to catch. Deleting the
/// predicate leaves them 3/3 green on all three engines (measured in the S9a whole-branch review),
/// which is why the predicate needs a guard of its own rather than a comment.
///
/// **The PREPARE phase IS holdable on demand** — the earlier "cannot be pinned by a test" note in
/// `task-1-journal.md` (and the stronger claim in `mysql_chaos_it.rs`'s harness doc) is FALSE, and
/// this test is the refutation. A prepare must acquire a SHARED metadata lock on every table it
/// names, and MDL grants are queued: a **pending EXCLUSIVE request** parks every later shared
/// acquirer behind it. So the shape is
///
/// 1. holder: `BEGIN; SELECT * FROM t` — an open transaction holding `SHARED_READ` on `t`;
/// 2. blocker: `ALTER TABLE t …` — an EXCLUSIVE request that can never be granted while (1) lives,
///    so it parks in `Waiting for table metadata lock` **with its request queued**;
/// 3. victim: `COM_STMT_PREPARE` of a `SELECT … FROM t` carrying a marker — queued behind (2), it
///    parks indefinitely in `COMMAND = 'Prepare'` with the marker fully visible in `INFO`.
///
/// The assertions are the two halves that make the predicate falsifiable:
/// - the FILTERED poll must return `None` for the parked prepare, on every iteration of a 2s window
///   (delete the predicate → it returns the prepare's id → RED);
/// - the UNFILTERED poll must return that same connection with `COMMAND = 'Prepare'` — so the
///   `None` above is the predicate discriminating, not the row having gone away; and the FILTERED
///   poll must still return the blocker's `ALTER` (a genuine `COM_QUERY` in flight), so it is not
///   simply a poll that matches nothing.
#[tokio::test]
async fn the_poll_filter_never_mistakes_a_parked_prepare_for_an_in_flight_statement() {
    use mysql_async::prelude::Queryable;

    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        eprintln!("--- parked-prepare vs the COMMAND filter: {label} ---");
        let mut side = raw_mysql(&url).await;

        // Per-run names: two DISJOINT markers (the blocker's ALTER also carries one, and it is a
        // `COMMAND = 'Query'` row that the filtered poll MUST match — if the markers overlapped the
        // filtered poll would match the ALTER and this guard would fail for the wrong reason).
        let table = unique_key("ferro_s9a_mdl");
        let prep_marker = unique_key("mdlprep");
        let alter_marker = unique_key("mdlalter");

        side.query_drop(format!("CREATE TABLE {table} (x INT)"))
            .await
            .expect("create the MDL fixture table");

        // (1) HOLDER — an open transaction that has touched the table holds SHARED_READ MDL on it
        //     until it ends. This connection stays open for the whole scenario.
        let mut holder = raw_mysql(&url).await;
        holder.query_drop("BEGIN").await.expect("holder BEGIN");
        holder
            .query_drop(format!("SELECT * FROM {table}"))
            .await
            .expect("holder SELECT (takes SHARED_READ MDL)");

        // (2) BLOCKER — an EXCLUSIVE MDL request that can never be granted while (1) is open. It
        //     parks, and its pending request is what stalls every later SHARED acquirer.
        let blocker_url = url.clone();
        let blocker_sql = format!("ALTER TABLE {table} ADD COLUMN y INT COMMENT '{alter_marker}'");
        let blocker = tokio::spawn(async move {
            let mut c = raw_mysql(&blocker_url).await;
            let _ = c.query_drop(blocker_sql).await;
        });
        let blocker_id = wait_for_parked_alter(&mut side, &alter_marker).await;

        // (3) VICTIM — a bare `COM_STMT_PREPARE` (no execute can follow: `prep` returns first).
        //     Its shared MDL acquisition queues behind (2)'s pending exclusive request.
        let victim_url = url.clone();
        let victim_sql = format!("SELECT x FROM {table} WHERE '{prep_marker}' <> ''");
        let victim = tokio::spawn(async move {
            let mut c = raw_mysql(&victim_url).await;
            let _ = c.prep(victim_sql).await;
        });

        // Vantage proof FIRST: the parked prepare is visible to an unfiltered marker poll, and the
        // server really reports it as `COMMAND = 'Prepare'`.
        let (victim_id, victim_state) =
            wait_for_parked_prepare(&mut side, &prep_marker, label).await;
        eprintln!(
            "[{label}] prepare parked: id={victim_id} COMMAND=Prepare STATE={victim_state:?}"
        );

        // THE ASSERTION. Repeated for a window, so a single lucky sample cannot carry it.
        let mut hits = 0usize;
        let mut last_seen: Option<(u64, String, Option<String>)> = None;
        let until = Instant::now() + PARKED_PREPARE_OBSERVE_WINDOW;
        while Instant::now() < until {
            let filtered = poll_active_conn(&mut side, &prep_marker).await;
            assert_eq!(
                filtered, None,
                "[{label}] the processlist poll matched a connection that is still PREPARING \
                 (id {victim_id}, marker {prep_marker:?}). `COMMAND IN ('Execute','Query')` is the \
                 only thing that keeps the in-tx KILL guards in this file aimed at a DISPATCHED \
                 statement: `ferro-backend-mysql/src/query.rs` marks the prepare phase \
                 `.undispatched()`, so a prep-phase kill classifies CONNECTION_LOST{{Retryable}} — \
                 exactly the value those guards assert, which would make them pass under the \
                 `in_tx: true -> false` mutation they exist to catch."
            );
            let row = poll_marked_conn(&mut side, &prep_marker).await;
            if let Some((id, ref cmd, _)) = row
                && id == victim_id
                && cmd == "Prepare"
            {
                hits += 1;
            }
            last_seen = row;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            hits >= PARKED_PREPARE_MIN_HITS,
            "[{label}] the prepare did not STAY parked ({hits} observations, need \
             {PARKED_PREPARE_MIN_HITS}) — the assertion above never had a reachable failing input. \
             Last unfiltered observation: {last_seen:?}"
        );

        // CONTROL: the very same filtered poll DOES match the blocker's ALTER — a real
        // `COMMAND = 'Query'` in flight (parked on the same MDL, so equally "not making progress").
        // Without this, a poll that had simply stopped matching anything would look identical.
        assert_eq!(
            poll_active_conn(&mut side, &alter_marker).await,
            Some(blocker_id),
            "[{label}] the filtered poll must still match a DISPATCHED statement — otherwise the \
             `None` asserted above proves nothing about the COMMAND predicate"
        );

        // Teardown: kill the victim, then the blocker, then release the holder, then drop the
        // fixture. Killing the blocker BEFORE the holder rolls back is what keeps the ALTER from
        // running (and from racing the DROP).
        side.query_drop(format!("KILL {victim_id}"))
            .await
            .expect("KILL the parked prepare");
        side.query_drop(format!("KILL {blocker_id}"))
            .await
            .expect("KILL the parked ALTER");
        let _ = tokio::time::timeout(Duration::from_secs(5), victim).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), blocker).await;
        holder
            .query_drop("ROLLBACK")
            .await
            .expect("holder ROLLBACK");
        drop(holder);
        side.query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .await
            .expect("drop the MDL fixture table");
    }
}

/// Wait until the blocker's `ALTER` is parked on the metadata lock, and return its processlist id.
/// Parked (not merely present) is what matters: only a PENDING exclusive request stalls a later
/// shared acquirer, so starting the victim before this returns would let its prepare sail past.
async fn wait_for_parked_alter(side: &mut mysql_async::Conn, marker: &str) -> u64 {
    let deadline = Instant::now() + IN_FLIGHT_GUARD_BOUND;
    loop {
        if let Some((id, cmd, state)) = poll_marked_conn(side, marker).await
            && cmd == "Query"
            && state
                .as_deref()
                .is_some_and(|s| s.to_ascii_lowercase().contains("metadata lock"))
        {
            return id;
        }
        assert!(
            Instant::now() < deadline,
            "the blocker ALTER never parked on the metadata lock within \
             {IN_FLIGHT_GUARD_BOUND:?} — the MDL queue this guard depends on was never formed"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// Wait until the victim's `COM_STMT_PREPARE` is observed parked, and return `(id, STATE)`.
/// Panics on the bound: if the prepare phase is never observable the guard below would assert
/// `None` against nothing at all (species (a): no reachable failing input).
async fn wait_for_parked_prepare(
    side: &mut mysql_async::Conn,
    marker: &str,
    label: &str,
) -> (u64, Option<String>) {
    let deadline = Instant::now() + IN_FLIGHT_GUARD_BOUND;
    loop {
        match poll_marked_conn(side, marker).await {
            Some((id, cmd, state)) if cmd == "Prepare" => return (id, state),
            other => assert!(
                Instant::now() < deadline,
                "[{label}] the prepare carrying marker {marker:?} was never observed with \
                 COMMAND='Prepare' within {IN_FLIGHT_GUARD_BOUND:?}; last row: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}
