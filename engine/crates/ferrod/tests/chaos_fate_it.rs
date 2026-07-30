//! M1-S4 Task 4 — the LIVE CHAOS suite: the §20.3 acceptance test for G7 (the never-silent-unknown
//! / never-transparent-retry guarantee). Kills/times-out/cancels a statement mid-flight against
//! REAL Dockerized Postgres and asserts (a) the terminal carries the exact §19.3 fate branch/code
//! T1–T3 wired up, and (b) the statement was applied AT MOST ONCE — never silently re-dispatched.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset, so `cargo test --workspace`
//! stays green offline — same discipline as `sql_exec_it.rs`/`tx_it.rs`.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro \
//!   cargo test -p ferrod --test chaos_fate_it -- --nocapture
//! ```
//!
//! **The no-re-dispatch proof (ONE uniform mechanism for every write case).** A per-test counter
//! row (`ferro_s4_ctr`, `key` a run-unique string, `n` seeded to 0). The write under chaos is
//! `UPDATE ferro_s4_ctr SET n = n + 1 WHERE key = ? RETURNING n`. After the chaos event, `n` is
//! read back via a FRESH autocommit EXEC: it must be `0` (never applied) or `1` (applied exactly
//! once) — **never ≥2**, which would prove a silent re-dispatch. This works against a REAL backend
//! (unlike the `FakeBackend` unit tests' `recorded` log, which doesn't exist live) because the
//! write under test is itself autocommit: if it ever applied, Postgres already durably committed
//! it server-side, independent of which physical pooled connection answers the read-back and
//! independent of whether the ORIGINAL client ever saw a response.
//!
//! **Provably in-flight, never a fixed-delay guess.** T2's review found a real false-green race: a
//! CANCEL (or, here, a kill) that lands BEFORE the targeted statement is dispatched is silently
//! cleared/ineffective by Postgres, so a test that merely sleeps-then-acts can "pass" without ever
//! proving what it claims. Every external chaos event in this file (`pg_terminate_backend`, the
//! `CANCEL` frame) is preceded by [`wait_for_active_pid`] polling `pg_stat_activity` — over a RAW
//! side connection, entirely outside ferrod's own pool — until the targeted statement is observed
//! `state = 'active'` server-side. The two engine-internal-timer cases (an `ExecRequest.timeout_ms`
//! shorter than the statement's own `pg_sleep`) need no such proof: the timer provably cannot fire
//! before `timeout_ms` has elapsed, by which point the query has already been dispatched and is
//! still sleeping (a >10x margin in both cases here).
//!
//! `pg_sleep(..)` is used only in a `WHERE`/`FROM` predicate (`IS NOT NULL`, or bare `FROM
//! pg_sleep(..)`), NEVER in a projected/`RETURNING` column — `rowmap::oid_to_tag` rejects a
//! `void`-typed OUTPUT column pre-execute (the same constraint `sql_e2e_scenarios.rs`'s
//! `SLOW_ROW_SQL` documents on the read side).

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{TestClient, assert_session_alive, exec, exec_err, exec_ok, exec_server, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_sql, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, Isolation, TxControl};
use ferro_proto::value::Value;

const CTR_TABLE: &str = "ferro_s4_ctr";
const DLK_TABLE: &str = "ferro_s4_dlk";
const SER_TABLE: &str = "ferro_s4_ser";
const WSKEW_TABLE: &str = "ferro_s4_wskew";

// -------------------------------------------------------------------------------------------------
// Shared chaos-harness helpers (raw side connection, in-flight proof, the counter, TX plumbing).
// This file is its own separate test binary/crate (like `tx_it.rs`), so it cannot import another
// `tests/*.rs`'s private helpers — these mirror `tx_it.rs`'s of the same names where applicable.
// -------------------------------------------------------------------------------------------------

static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// A per-test-run unique string (`<prefix>_<pid>_<nanos>_<counter>`) — used both as the counter
/// table's primary key (so concurrently-running chaos tests, or reruns, never collide on the same
/// row) and, embedded in a chaos statement's SQL text as an inert comment, as the
/// `pg_stat_activity.query` marker [`wait_for_active_pid`] filters on (so it can never mistake a
/// DIFFERENT concurrently in-flight chaos statement — on this same shared testkit database — for
/// THIS test's own).
fn unique_key(prefix: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

/// A raw side connection to the SAME Postgres, entirely OUTSIDE ferrod's own pool — used only to
/// `pg_terminate_backend` a targeted backend and to poll `pg_stat_activity`. Resolves to the SAME
/// vendored `tokio-postgres` fork `ferro-backend-pg` uses (the workspace `[patch.crates-io]`).
async fn raw_connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("chaos harness: raw side connection to Postgres");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("chaos harness: raw side connection driver ended: {e}");
        }
    });
    client
}

/// Poll `pg_stat_activity` (over the RAW side connection) every 15ms, up to a 5s bound, until a
/// backend is observed `state = 'active'` running a statement whose text contains `marker` — i.e.
/// PROVABLY in flight, not merely dispatched-but-not-yet-running (or, worse, not yet dispatched at
/// all — the false-green race T2's review found: a kill/CANCEL landing before dispatch is silently
/// cleared/ineffective by Postgres). Returns that backend's pid. Panics — a loud test failure, not
/// a silent skip — if the bound elapses first: better that than a chaos event that proves nothing.
async fn wait_for_active_pid(raw: &tokio_postgres::Client, marker: &str) -> i32 {
    let pattern = format!("%{marker}%");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let rows = raw
            .query(
                "SELECT pid FROM pg_stat_activity WHERE state = 'active' AND query LIKE $1",
                &[&pattern],
            )
            .await
            .expect("chaos harness: pg_stat_activity poll");
        if let Some(row) = rows.first() {
            return row.get::<_, i32>(0);
        }
        if Instant::now() >= deadline {
            panic!(
                "chaos harness: no backend became active running a statement matching {marker:?} \
                 within 5s -- the chaos event would be a false-green race (dispatch never observed)"
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// Poll `pg_stat_activity` (over the RAW side connection) every 15ms, up to a 5s bound, until
/// TWO DISTINCT backends are observed genuinely blocked on a heavyweight lock (`wait_event_type =
/// 'Lock'`, `wait_event = 'transactionid'` for a row-lock wait — confirmed live) running a
/// statement whose text contains `marker`. This is the deadlock test's own "provably in flight"
/// proof: firing two cross-locking requests and merely assuming both have reached Postgres by the
/// time its (shortened) `deadlock_timeout` elapses is itself a false-green race under concurrent
/// test-suite load — tokio/OS scheduling jitter can delay one side's dispatch long enough that the
/// other resolves normally because no cycle ever actually formed (observed live while writing this
/// suite). Panics if the bound elapses first.
async fn wait_for_two_lock_waiters(raw: &tokio_postgres::Client, marker: &str) {
    let pattern = format!("%{marker}%");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let rows = raw
            .query(
                "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE $1",
                &[&pattern],
            )
            .await
            .expect("chaos harness: pg_stat_activity poll");
        if rows.len() >= 2 {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "chaos harness: never observed two backends blocked on a lock matching {marker:?} \
                 within 5s -- the cross-lock setup never became genuinely mutually contended"
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// Fire `pg_terminate_backend(pid)` over the raw side connection, asserting Postgres itself
/// reports success — the same deterministic-kill idiom `ferro-backend-pg`'s own
/// `pg_killed_backend_evicted_no_retry` (`pg_pool_it.rs`) uses.
async fn kill_backend(raw: &tokio_postgres::Client, pid: i32) {
    let terminated: bool = raw
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .expect("chaos harness: pg_terminate_backend")
        .get(0);
    assert!(
        terminated,
        "pg_terminate_backend({pid}) must report success"
    );
}

/// Receive the next frame for `rid`, asserting the shared one-`END`/service/method shape (charter
/// rule 4) used throughout this suite, and decode its `Outcome`. Used (instead of `common::exec`,
/// which sends AND receives atomically) whenever something must happen BETWEEN send and receive —
/// a chaos event proven in flight, or a `CANCEL`.
async fn recv_terminal(client: &mut TestClient, rid: u32, svc: u16, method: u16) -> Outcome {
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "terminal echoes the request id");
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "exactly one END frame per request (charter rule 4)"
    );
    assert_eq!(t.header.service, svc);
    assert_eq!(t.header.method, method);
    Outcome::decode(&t.payload).expect("decode terminal Outcome")
}

/// Run a `CREATE TABLE IF NOT EXISTS` autocommit, tolerating the well-known Postgres catalog race:
/// `IF NOT EXISTS`'s existence check is NOT atomic with the create, so when several of this suite's
/// tests run concurrently (`cargo test`'s default parallelism) and race to create the SAME
/// not-yet-existing table for the first time, one of them can lose with a `23505` duplicate-key
/// error against the `pg_type`/`pg_class` catalog rather than a clean no-op — the table exists
/// either way (via whichever session's create actually landed), so that specific error is benign,
/// not a real failure.
async fn ensure_table(client: &mut TestClient, rid: u32, ddl: &str) {
    let mut w = req(ddl);
    w.readonly = false;
    w.fetch = 1;
    match exec(client, rid, &w).await {
        Outcome::Ok(_) => {}
        Outcome::Error(ep) if ep.sqlstate.as_deref() == Some("23505") => {}
        other => panic!("DDL {ddl:?} failed: {other:?}"),
    }
}

async fn ensure_ctr_table(client: &mut TestClient, rid: u32) {
    ensure_table(
        client,
        rid,
        &format!("CREATE TABLE IF NOT EXISTS {CTR_TABLE} (key text primary key, n int not null)"),
    )
    .await;
}

/// Seed (or reset) `key`'s counter row to `n = 0` — an upsert, not a plain INSERT, so a rerun with
/// a colliding key (should one ever occur) still starts from a known state.
async fn seed_ctr(client: &mut TestClient, rid: u32, key: &str) {
    let mut w = req(&format!(
        "INSERT INTO {CTR_TABLE} (key, n) VALUES (?, 0) ON CONFLICT (key) DO UPDATE SET n = 0"
    ));
    w.readonly = false;
    w.fetch = 1;
    w.params = vec![Value::Text(key.to_string())];
    match exec(client, rid, &w).await {
        Outcome::Ok(_) => {}
        other => panic!("seed_ctr({key:?}) failed: {other:?}"),
    }
}

/// Read `n` back for `key` via a FRESH autocommit EXEC (a brand-new request — the pool may hand
/// back any pooled connection). This is a valid no-re-dispatch proof because the write under chaos
/// is ITSELF autocommit: if it ever applied at all, Postgres already durably committed it
/// server-side, independent of which physical connection answers this read.
async fn read_ctr(client: &mut TestClient, rid: u32, key: &str) -> i64 {
    let mut r = req(&format!("SELECT n FROM {CTR_TABLE} WHERE key = ?"));
    r.params = vec![Value::Text(key.to_string())];
    let ok = exec_ok(client, rid, &r).await;
    match ok.rows.first().and_then(|row| row.first()) {
        Some(Value::I64(n)) => *n,
        other => panic!(
            "read_ctr({key:?}): expected one I64 row, got {other:?} (rows={:?})",
            ok.rows
        ),
    }
}

/// Best-effort per-test row cleanup (each test's `key` is run-unique regardless, so a failed
/// cleanup never contaminates a later run — this just keeps the persistent testkit table small).
async fn cleanup_ctr(client: &mut TestClient, rid: u32, key: &str) {
    let mut w = req(&format!("DELETE FROM {CTR_TABLE} WHERE key = ?"));
    w.readonly = false;
    w.fetch = 1;
    w.params = vec![Value::Text(key.to_string())];
    let _ = exec(client, rid, &w).await;
}

// ---- TX plumbing (mirrors `tx_it.rs`'s helpers of the same names) -------------------------------

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

/// Receive frames until every rid in `want` has been seen exactly once, matching by
/// `header.request_id` rather than arrival order — for a set of concurrently in-flight requests
/// whose relative completion order is genuinely unconstrained (e.g. two independent tx actors'
/// terminals racing each other), where asserting a specific arrival order would itself be a flaky
/// test bug, not a real invariant.
async fn collect_terminals(client: &mut TestClient, mut want: Vec<u32>) -> HashMap<u32, Outcome> {
    let mut got = HashMap::new();
    while !want.is_empty() {
        let t = client.recv().await;
        assert_eq!(
            t.header.flags & flags::END,
            flags::END,
            "exactly one END frame per request (charter rule 4)"
        );
        let rid = t.header.request_id;
        assert!(
            want.contains(&rid),
            "unexpected terminal rid {rid}, still waiting for {want:?}"
        );
        want.retain(|&r| r != rid);
        got.insert(rid, Outcome::decode(&t.payload).expect("decode Outcome"));
    }
    got
}

async fn tx_control(client: &mut TestClient, rid: u32, tx_id: u64, method: u16) -> Outcome {
    client
        .send_request(rid, service::TX, method, TxControl { tx_id }.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid);
    assert_eq!(t.header.flags & flags::END, flags::END);
    Outcome::decode(&t.payload).expect("decode tx-control Outcome")
}

async fn commit(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    tx_control(client, rid, tx_id, method_tx::COMMIT).await
}

async fn rollback(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    tx_control(client, rid, tx_id, method_tx::ROLLBACK).await
}

// -------------------------------------------------------------------------------------------------
// 1. kill mid-write -> WriteUnconfirmed{Indeterminate}; counter in {0,1}.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn kill_mid_write_indeterminate_never_redispatched() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url.clone());
    let mut client = server.connect().await;
    client.hello(1).await;
    let raw = raw_connect(&url).await;

    let key = unique_key("kill_write");
    ensure_ctr_table(&mut client, 10).await;
    seed_ctr(&mut client, 11, &key).await;

    let marker = format!("chaos_kill_write_{key}");
    let sql = format!(
        "/* {marker} */ UPDATE {CTR_TABLE} SET n = n + 1 WHERE key = ? AND pg_sleep(3) IS NOT NULL RETURNING n"
    );
    let mut w = req(&sql);
    w.readonly = false;
    w.params = vec![Value::Text(key.clone())];

    let rid = 12;
    client
        .send_request(rid, service::SQL, method_sql::EXEC, w.encode())
        .await;

    // Provably in flight (not a fixed-delay guess): observe the EXACT backend pid actually
    // executing our marked statement before killing it.
    let pid = wait_for_active_pid(&raw, &marker).await;
    kill_backend(&raw, pid).await;

    let ep = match recv_terminal(&mut client, rid, service::SQL, method_sql::EXEC).await {
        Outcome::Error(ep) => ep,
        other => panic!("a killed in-flight write must error, got {other:?}"),
    };
    assert_eq!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "a killed, DISPATCHED autocommit write's non-execution is unconfirmed -> Indeterminate, got {ep:?}"
    );
    assert_eq!(ep.branch, branch::INDETERMINATE);

    // The no-re-dispatch proof: 0 (never applied) or 1 (applied exactly once) are both fine; >=2
    // would prove a silent re-dispatch.
    let n = read_ctr(&mut client, 13, &key).await;
    assert!(
        n == 0 || n == 1,
        "counter must be 0 or 1 (never re-dispatched), got {n}"
    );

    cleanup_ctr(&mut client, 14, &key).await;
    // The session (and pool) survive a killed BACKEND: only that one pooled connection died.
    assert_session_alive(&mut client, 15).await;
}

// -------------------------------------------------------------------------------------------------
// 2. kill mid-read -> ConnectionLost{Retryable}, NEVER Indeterminate.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn kill_mid_read_retryable_never_indeterminate() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url.clone());
    let mut client = server.connect().await;
    client.hello(1).await;
    let raw = raw_connect(&url).await;

    let marker = unique_key("chaos_kill_read");
    // pg_sleep in the FROM clause, never projected (see the module doc's `oid_to_tag` note).
    let r = req(&format!("/* {marker} */ SELECT 1 FROM pg_sleep(3)")); // readonly: true (req()'s default)

    let rid = 20;
    client
        .send_request(rid, service::SQL, method_sql::EXEC, r.encode())
        .await;

    let pid = wait_for_active_pid(&raw, &marker).await;
    kill_backend(&raw, pid).await;

    let ep = match recv_terminal(&mut client, rid, service::SQL, method_sql::EXEC).await {
        Outcome::Error(ep) => ep,
        other => panic!("a killed in-flight read must error, got {other:?}"),
    };
    assert_eq!(
        ep.code,
        errc::CONNECTION_LOST,
        "a killed READ is a known-fate connection loss -> Retryable, got {ep:?}"
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(
        ep.branch,
        branch::INDETERMINATE,
        "a read's non-execution is NEVER Indeterminate (§19.3)"
    );

    assert_session_alive(&mut client, 21).await;
}

// -------------------------------------------------------------------------------------------------
// 3. engine-enforced statement-timeout, autocommit write -> Indeterminate; counter in {0,1}.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn statement_timeout_autocommit_write_is_indeterminate() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let key = unique_key("stmt_timeout_write");
    ensure_ctr_table(&mut client, 30).await;
    seed_ctr(&mut client, 31, &key).await;

    let sql = format!(
        "UPDATE {CTR_TABLE} SET n = n + 1 WHERE key = ? AND pg_sleep(2) IS NOT NULL RETURNING n"
    );
    let mut w = req(&sql);
    w.readonly = false;
    w.timeout_ms = Some(150); // << the 2s server-side sleep: the engine's own timer wins, deterministically
    w.params = vec![Value::Text(key.clone())];

    let ep = exec_err(&mut client, 32, &w).await;
    assert_eq!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "an engine-enforced statement timeout on a DISPATCHED write -> Indeterminate, got {ep:?}"
    );
    assert_eq!(ep.branch, branch::INDETERMINATE);

    let n = read_ctr(&mut client, 33, &key).await;
    assert!(n == 0 || n == 1, "counter must be 0 or 1, got {n}");

    cleanup_ctr(&mut client, 34, &key).await;
    assert_session_alive(&mut client, 35).await;
}

// -------------------------------------------------------------------------------------------------
// 4. engine-enforced statement-timeout, tx-scoped write -> rollback + tombstone -> Retryable
//    (TxDeadline), counter == 0 (rolled back), tx_id unusable afterward.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn statement_timeout_tx_scoped_write_rolls_back_to_retryable() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let key = unique_key("tx_timeout_write");
    ensure_ctr_table(&mut client, 40).await;
    seed_ctr(&mut client, 41, &key).await;

    let tx_id = begin(&mut client, 42, "default", None, false).await;

    let sql = format!(
        "UPDATE {CTR_TABLE} SET n = n + 1 WHERE key = ? AND pg_sleep(2) IS NOT NULL RETURNING n"
    );
    let mut w = req(&sql);
    w.tx_id = Some(tx_id);
    w.readonly = false;
    w.timeout_ms = Some(150);
    w.params = vec![Value::Text(key.clone())];

    let ep = match exec(&mut client, 43, &w).await {
        Outcome::Error(ep) => ep,
        other => panic!("a tx-scoped statement timeout must error, got {other:?}"),
    };
    assert_eq!(
        ep.code,
        errc::TX_DEADLINE,
        "a tx-scoped timeout rolls the tx back -> TxDeadline/Retryable, got {ep:?}"
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(
        ep.branch,
        branch::INDETERMINATE,
        "a rolled-back in-tx statement persisted nothing -- never Indeterminate"
    );

    // The tx_id is unusable afterward (tombstoned): a re-touch yields TxDeadline again, never a
    // silent re-run of the statement (charter rule 3).
    let mut probe = req("SELECT 1");
    probe.tx_id = Some(tx_id);
    match exec(&mut client, 44, &probe).await {
        Outcome::Error(ep2) => assert_eq!(
            ep2.code,
            errc::TX_DEADLINE,
            "the timed-out tx stays tombstoned"
        ),
        other => panic!("expected TxDeadline on the dead tx_id, got {other:?}"),
    }

    let n = read_ctr(&mut client, 45, &key).await;
    assert_eq!(n, 0, "the tx was rolled back -- the write never committed");

    cleanup_ctr(&mut client, 46, &key).await;
    assert_session_alive(&mut client, 47).await;
}

// -------------------------------------------------------------------------------------------------
// 5. CANCEL race mid-write -> Indeterminate (cancel won) OR Ok (cancel lost the race); either way
//    the counter is consistent and never re-dispatched.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cancel_race_mid_write_consistent_with_counter() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url.clone());
    let mut client = server.connect().await;
    client.hello(1).await;
    let raw = raw_connect(&url).await;

    let key = unique_key("cancel_write");
    ensure_ctr_table(&mut client, 50).await;
    seed_ctr(&mut client, 51, &key).await;

    let marker = format!("chaos_cancel_write_{key}");
    let sql = format!(
        "/* {marker} */ UPDATE {CTR_TABLE} SET n = n + 1 WHERE key = ? AND pg_sleep(3) IS NOT NULL RETURNING n"
    );
    let mut w = req(&sql);
    w.readonly = false;
    w.params = vec![Value::Text(key.clone())];

    let rid = 52;
    client
        .send_request(rid, service::SQL, method_sql::EXEC, w.encode())
        .await;

    // Provably in flight before firing CANCEL -- the exact false-green race T2's review found (a
    // CANCEL landing before dispatch is silently cleared by Postgres, proving nothing).
    wait_for_active_pid(&raw, &marker).await;
    client.cancel(rid).await;

    let outcome = recv_terminal(&mut client, rid, service::SQL, method_sql::EXEC).await;
    let n = read_ctr(&mut client, 53, &key).await;

    match outcome {
        Outcome::Error(ep) => {
            assert_eq!(
                ep.code,
                errc::WRITE_UNCONFIRMED,
                "a cancelled DISPATCHED autocommit write -> Indeterminate, got {ep:?}"
            );
            assert_eq!(ep.branch, branch::INDETERMINATE);
            assert!(
                n == 0 || n == 1,
                "Indeterminate must be consistent with a counter in {{0,1}}, got {n}"
            );
        }
        Outcome::Ok(_) => {
            // The cancel lost the race to a write that genuinely completed (§5.2/§19.3: never
            // fabricate a cancel/error for a statement that actually finished) -- the only outcome
            // consistent with an Ok terminal is that it applied exactly once.
            assert_eq!(
                n, 1,
                "the cancel lost the race (Ok) -- the write genuinely completed, so n must be exactly 1"
            );
        }
        Outcome::Cancelled => panic!(
            "Outcome::Cancelled is retired from the SQL EXEC path (M1-S4 T2) -- a fated cancel \
             always rides a branch-carrying Outcome::Error"
        ),
    }
    assert!(
        n < 2,
        "no re-dispatch: the counter must never reach 2, got {n}"
    );

    cleanup_ctr(&mut client, 54, &key).await;
    assert_session_alive(&mut client, 55).await;
}

// -------------------------------------------------------------------------------------------------
// 6. Deadlock (40P01) between two concurrent tx-scoped statements -> Retryable, live through ferrod.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn deadlock_40p01_is_retryable_live() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url.clone());
    let mut client = server.connect().await;
    client.hello(1).await;
    let raw = raw_connect(&url).await;

    let run = unique_key("dlk");
    let key_a = format!("{run}_a");
    let key_b = format!("{run}_b");

    ensure_table(
        &mut client,
        60,
        &format!("CREATE TABLE IF NOT EXISTS {DLK_TABLE} (key text primary key, n int not null)"),
    )
    .await;
    for (rid, key) in [(61, &key_a), (62, &key_b)] {
        let mut w = req(&format!("INSERT INTO {DLK_TABLE} (key, n) VALUES (?, 0)"));
        w.readonly = false;
        w.fetch = 1;
        w.params = vec![Value::Text(key.clone())];
        match exec(&mut client, rid, &w).await {
            Outcome::Ok(_) => {}
            other => panic!("seed dlk row {key:?} failed: {other:?}"),
        }
    }

    let tx_a = begin(&mut client, 63, "default", None, false).await;
    let tx_b = begin(&mut client, 64, "default", None, false).await;

    // Shrink Postgres's own deadlock-detector wait (default 1s) so the live proof stays
    // comfortably inside the harness's 2s per-frame recv timeout.
    for (rid, tx) in [(65, tx_a), (66, tx_b)] {
        let mut s = req("SET deadlock_timeout = '200ms'");
        s.tx_id = Some(tx);
        match exec(&mut client, rid, &s).await {
            Outcome::Ok(_) => {}
            other => panic!("SET deadlock_timeout failed: {other:?}"),
        }
    }

    // Each tx locks its OWN row first -- uncontended, both succeed immediately.
    for (rid, tx, key) in [(67, tx_a, &key_a), (68, tx_b, &key_b)] {
        let mut w = req(&format!("UPDATE {DLK_TABLE} SET n = n + 1 WHERE key = ?"));
        w.tx_id = Some(tx);
        w.readonly = false;
        w.fetch = 1;
        w.params = vec![Value::Text(key.clone())];
        match exec(&mut client, rid, &w).await {
            Outcome::Ok(_) => {}
            other => panic!("lock {key:?} failed: {other:?}"),
        }
    }

    // Now cross WITHOUT sequentially awaiting: A wants B's row, B wants A's row -- the classic
    // AB-BA cycle. A marker (an inert SQL comment) on BOTH statements lets the raw side connection
    // confirm genuine mutual blocking below.
    let marker = format!("chaos_dlk_{run}");
    let mut cross_a = req(&format!(
        "/* {marker} */ UPDATE {DLK_TABLE} SET n = n + 1 WHERE key = ?"
    ));
    cross_a.tx_id = Some(tx_a);
    cross_a.readonly = false;
    cross_a.fetch = 1;
    cross_a.params = vec![Value::Text(key_b.clone())];
    let mut cross_b = req(&format!(
        "/* {marker} */ UPDATE {DLK_TABLE} SET n = n + 1 WHERE key = ?"
    ));
    cross_b.tx_id = Some(tx_b);
    cross_b.readonly = false;
    cross_b.fetch = 1;
    cross_b.params = vec![Value::Text(key_a.clone())];

    let rid_a = 69;
    let rid_b = 70;
    client
        .send_request(rid_a, service::SQL, method_sql::EXEC, cross_a.encode())
        .await;
    client
        .send_request(rid_b, service::SQL, method_sql::EXEC, cross_b.encode())
        .await;

    // Provably (not presumed) mutually contended: under concurrent test-suite load, tokio/OS
    // scheduling jitter can delay ONE of the two sends reaching its actor/Postgres, so relying on
    // a fixed wait after firing both is itself a false-green race (observed live while writing this
    // test: one side occasionally finished normally because the other's conflicting request had
    // not yet reached Postgres, so no cycle ever formed) -- wait until BOTH backends are actually
    // observed blocked on a lock (`wait_event_type = 'Lock'`) before trusting Postgres's own
    // deadlock timer to resolve the cycle.
    wait_for_two_lock_waiters(&raw, &marker).await;

    // Postgres's own deadlock detector picks exactly one victim (whichever side's OWN periodic
    // check discovers the cycle first, self-aborting) and errors ITS blocked statement with 40P01
    // ("deadlock detected") -- `error_map` classifies that as `Deadlock{Retryable}`. The OTHER side
    // (the survivor) then completes NORMALLY (`Ok`) -- confirmed LIVE (a dedicated timing probe,
    // run repeatedly against this exact harness) to resolve essentially IMMEDIATELY after the
    // victim's error (sub-millisecond), well BEFORE this test ever issues an explicit ROLLBACK on
    // either tx below.
    //
    // (M4a correction: an earlier version of this comment claimed the survivor "stays blocked until
    // the victim's transaction is explicitly rolled back" -- that mental model was never itself
    // verified live and, taken literally, would make this test hang, since the explicit rollback
    // below runs only AFTER both terminals are collected. The "RARE, CONFIRMED-LIVE edge case" this
    // used to carry -- an independent connection loss on the non-victim side, tolerated as an
    // alternative to `Deadlock` in the assertion below -- was reasoned from that same inaccurate
    // "long open-ended block" model; with the real (sub-millisecond) resolution window there is no
    // remaining basis for it, so the tolerance is removed: verified live, repeatedly, to still pass.)
    //
    // The two terminals are NOT assumed to arrive in a fixed order (they are two independent tasks
    // racing to complete and both write to the same multiplexed output stream) -- collect BOTH,
    // matching by rid, and assert the required invariant regardless of which side Postgres picked
    // as the victim: EXACTLY one `Deadlock{Retryable}` and one `Ok`, NEVER `Indeterminate`.
    let mut terminals = collect_terminals(&mut client, vec![rid_a, rid_b]).await;
    let outcome_a = terminals.remove(&rid_a).expect("rid_a terminal");
    let outcome_b = terminals.remove(&rid_b).expect("rid_b terminal");

    let mut saw_deadlock = false;
    let mut saw_ok = false;
    for (label, outcome) in [("cross_a", &outcome_a), ("cross_b", &outcome_b)] {
        match outcome {
            Outcome::Ok(_) => saw_ok = true,
            Outcome::Error(ep) => {
                assert_ne!(
                    ep.branch,
                    branch::INDETERMINATE,
                    "{label}: an in-tx statement's fate must never be Indeterminate (§19.3), got {ep:?}"
                );
                assert_eq!(
                    ep.code,
                    errc::DEADLOCK,
                    "{label}: the only error tolerated here is the deadlock victim's, got {ep:?}"
                );
                assert_eq!(
                    ep.branch,
                    branch::RETRYABLE,
                    "{label}: a deadlock must be Retryable, got {ep:?}"
                );
                saw_deadlock = true;
            }
            Outcome::Cancelled => panic!("{label}: Outcome::Cancelled is retired (M1-S4 T3)"),
        }
    }
    assert!(
        saw_deadlock,
        "neither cross-update ever surfaced Deadlock/Retryable: cross_a={outcome_a:?} cross_b={outcome_b:?}"
    );
    assert!(
        saw_ok,
        "the survivor's cross-update must complete Ok: cross_a={outcome_a:?} cross_b={outcome_b:?}"
    );

    // Best-effort cleanup: both txs are still alive at this point (the victim's plain statement
    // error does not auto-rollback its actor -- see `non_cancel_statement_error_reported_without_
    // auto_rollback` -- and the survivor never errored at all), so both ROLLBACKs below run for
    // real; `let _ =` only guards against an unrelated failure making cleanup itself fail the test.
    let _ = rollback(&mut client, 71, tx_a).await;
    let _ = rollback(&mut client, 72, tx_b).await;

    let mut cleanup = req(&format!("DELETE FROM {DLK_TABLE} WHERE key IN (?, ?)"));
    cleanup.readonly = false;
    cleanup.fetch = 1;
    cleanup.params = vec![Value::Text(key_a), Value::Text(key_b)];
    let _ = exec(&mut client, 73, &cleanup).await;

    assert_session_alive(&mut client, 74).await;
}

// -------------------------------------------------------------------------------------------------
// 7. Serialization failure (40001) between two concurrent SERIALIZABLE tx-scoped statements ->
//    Retryable, live through ferrod. A variant of the textbook PostgreSQL SSI anomaly repro, tuned
//    so the 40001 lands on tx_b's own INSERT STATEMENT rather than at its COMMIT.
//
// NOTE (a real gap this test surfaced, out of this task's scope to fix): `Checkout::commit_tx`/
// `rollback_tx`/`tx_control` (`ferro-pool/src/pool.rs`) all run through the coarse
// `PoolBackend::simple_query`, whose error mapping (`ferro-backend-pg::conn::PgBackend::
// simple_query`) discards the SQLSTATE on any non-session-fatal error, collapsing it to a generic
// `PoolError::Backend(msg)` -> `Protocol{NonRetryable}` -- UNLIKE the SQLSTATE-preserving
// `Checkout::query` path (`error_map::map`) the autocommit/tx-scoped-statement routes use. The
// textbook demo (both txs INSERT, then COMMIT sequentially) fails tx_b's COMMIT, not a statement --
// and that failure is misclassified Protocol/NonRetryable instead of SerializationFailure/Retryable
// (confirmed live while writing this test). This is a pre-existing pool-layer gap (COMMIT/ROLLBACK
// never carried a SQLSTATE-preserving path even before M1-S4; T1-T3 did not touch
// `commit_tx`/`tx_control`), not a T1-T3 regression -- flagged for a follow-up slice, not fixed here
// (out of scope: `ferro-pool`/`ferro-backend-pg`, not `services::fate`/the EXEC paths).
//
// So this test instead has tx_a fully INSERT-and-COMMIT before tx_b (already BEGUN + having read
// beforehand) attempts its own conflicting INSERT: Postgres's SSI detector then aborts THAT
// STATEMENT immediately ("Reason code: Canceled on identification as a pivot, during write" --
// confirmed live), which DOES go through the SQLSTATE-preserving `Checkout::query` path.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn serialization_40001_is_retryable_live() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    ensure_table(
        &mut client,
        80,
        // `v bigint` (not `int4`): the M0 client binds `Value::I64` directly to `int8` (COMMIT-1) —
        // an `int4` column would reject the bind as a known-fate `Unsupported`, not exercise the
        // SSI anomaly this test is actually after.
        &format!("CREATE TABLE IF NOT EXISTS {SER_TABLE} (id bigserial primary key, v bigint)"),
    )
    .await;

    // Both SERIALIZABLE txs read the whole table first (establishing the rw-dependency), then
    // tx_a inserts a disjoint new row (`bigserial`, so it never blocks on tx_b) and fully commits.
    let iso = Some(u8::from(Isolation::Serializable));
    let tx_a = begin(&mut client, 81, "default", iso, false).await;
    let tx_b = begin(&mut client, 82, "default", iso, false).await;

    for (rid, tx) in [(83, tx_a), (84, tx_b)] {
        let mut r = req(&format!("SELECT count(*) FROM {SER_TABLE}"));
        r.tx_id = Some(tx);
        match exec(&mut client, rid, &r).await {
            Outcome::Ok(_) => {}
            other => panic!("read failed: {other:?}"),
        }
    }

    let mut insert_a = req(&format!("INSERT INTO {SER_TABLE} (v) VALUES (?)"));
    insert_a.tx_id = Some(tx_a);
    insert_a.readonly = false;
    insert_a.fetch = 1;
    insert_a.params = vec![Value::I64(1)];
    match exec(&mut client, 85, &insert_a).await {
        Outcome::Ok(_) => {}
        other => panic!("tx_a's insert failed: {other:?}"),
    }
    match commit(&mut client, 86, tx_a).await {
        Outcome::Ok(_) => {}
        other => panic!("tx_a's commit (the first committer) should succeed, got {other:?}"),
    }

    // NOW tx_b (already begun, having read before tx_a committed) attempts its own conflicting
    // INSERT: with tx_a's conflicting write already committed, Postgres's SSI detector identifies
    // tx_b as the pivot and aborts THIS STATEMENT (not a later COMMIT) with 40001.
    let mut insert_b = req(&format!("INSERT INTO {SER_TABLE} (v) VALUES (?)"));
    insert_b.tx_id = Some(tx_b);
    insert_b.readonly = false;
    insert_b.fetch = 1;
    insert_b.params = vec![Value::I64(2)];
    let ep = match exec(&mut client, 87, &insert_b).await {
        Outcome::Error(ep) => ep,
        other => {
            panic!("tx_b's insert must be rejected with a serialization failure, got {other:?}")
        }
    };
    assert_eq!(
        ep.code,
        errc::SERIALIZATION_FAILURE,
        "a genuine SSI anomaly -> SerializationFailure/Retryable, got {ep:?}"
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(ep.branch, branch::INDETERMINATE);

    // tx_b is left open-but-aborted (same S6 "non-cancel statement error, no auto-rollback"
    // behavior the deadlock test exercises) -- clean it up explicitly.
    let _ = rollback(&mut client, 88, tx_b).await;

    assert_session_alive(&mut client, 89).await;
}

// -------------------------------------------------------------------------------------------------
// 8. Serialization failure (40001) caught AT COMMIT (a classic write-skew anomaly), through the TX
//    service's COMMIT path -> Retryable, live through ferrod.
//
// This is the M4b regression test: `Checkout::commit_tx` (`ferro-pool/src/pool.rs`) routes through
// `PoolBackend::simple_query`, and PRE-FIX `PgBackend::simple_query` (`ferro-backend-pg/src/
// conn.rs`) discarded the SQLSTATE on any non-session-fatal error (`PoolError::Backend(msg)`),
// collapsing a genuine 40001/40P01 AT COMMIT into `Protocol{NonRetryable}` -- exactly the gap
// `serialization_40001_is_retryable_live` (test 7, above) flagged but structurally could not cover,
// because that test's own anomaly is caught mid-STATEMENT (`Checkout::query`'s SQLSTATE-preserving
// `error_map::map` path), never at COMMIT. Fixed by routing `simple_query`'s non-fatal error through
// `error_map::map` too (M4b) -- this test is RED against the pre-fix `PoolError::Backend` behavior
// (it observes `Protocol`/`NonRetryable`) and GREEN once `simple_query` preserves the SQLSTATE.
//
// The classic "doctors on call" write-skew anomaly (Cahill et al / the PostgreSQL SSI docs): two
// rows both start "true"; two SERIALIZABLE txs each read BOTH rows (establishing a read dependency
// on both), then each flips ONE row to "false" (a different row per tx, so the writes never
// conflict/block each other) after confirming "the other one is still true". Both UPDATEs succeed
// (no row-lock contention -- confirmed live while writing this test), because neither transaction
// has committed yet, so Postgres cannot yet know whether the cycle is genuinely dangerous. The
// rw-dependency cycle (tx_a reads b's row / tx_b writes it; tx_b reads a's row / tx_a writes it) is
// only detected once the transactions start committing: the FIRST commit succeeds cleanly, and the
// SECOND is the one Postgres aborts with 40001 ("Reason code: Canceled on identification as a
// pivot, during commit attempt" -- confirmed live), never during either UPDATE statement. This is
// SSI deferring the pivot check to COMMIT, the dominant real-world SERIALIZABLE-conflict shape the
// S4 gate ("a serialization/deadlock -> Retryable", unqualified) is about.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn commit_time_serialization_write_skew_is_retryable_live() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let run = unique_key("wskew");
    let key_a = format!("{run}_a");
    let key_b = format!("{run}_b");

    ensure_table(
        &mut client,
        90,
        &format!(
            "CREATE TABLE IF NOT EXISTS {WSKEW_TABLE} (key text primary key, on_call boolean not null)"
        ),
    )
    .await;

    // Seed both rows "on call" (true) -- the invariant both txs will each (wrongly, concurrently)
    // believe is safe to break on their own row alone.
    for (rid, key) in [(91, &key_a), (92, &key_b)] {
        let mut w = req(&format!(
            "INSERT INTO {WSKEW_TABLE} (key, on_call) VALUES (?, true)"
        ));
        w.readonly = false;
        w.fetch = 1;
        w.params = vec![Value::Text(key.clone())];
        match exec(&mut client, rid, &w).await {
            Outcome::Ok(_) => {}
            other => panic!("seed wskew row {key:?} failed: {other:?}"),
        }
    }

    let iso = Some(u8::from(Isolation::Serializable));
    let tx_a = begin(&mut client, 93, "default", iso, false).await;
    let tx_b = begin(&mut client, 94, "default", iso, false).await;

    // Both txs read BOTH rows first (establishing the rw-dependency on both sides) -- sequential
    // awaits are fine here (unlike the deadlock test): SERIALIZABLE reads take SIREAD predicate
    // locks, which never block a concurrent reader/writer, so there is no lock-contention race to
    // prove -- only the eventual COMMIT-time check matters.
    for (rid, tx) in [(95, tx_a), (96, tx_b)] {
        let mut r = req(&format!(
            "SELECT count(*) FROM {WSKEW_TABLE} WHERE key IN (?, ?) AND on_call = true"
        ));
        r.tx_id = Some(tx);
        r.params = vec![Value::Text(key_a.clone()), Value::Text(key_b.clone())];
        match exec(&mut client, rid, &r).await {
            Outcome::Ok(_) => {}
            other => panic!("write-skew read failed: {other:?}"),
        }
    }

    // Each tx flips its OWN row to false -- a different row per tx, so neither UPDATE blocks or
    // conflicts with the other; both succeed (confirmed live: no error surfaces here).
    let mut upd_a = req(&format!(
        "UPDATE {WSKEW_TABLE} SET on_call = false WHERE key = ?"
    ));
    upd_a.tx_id = Some(tx_a);
    upd_a.readonly = false;
    upd_a.fetch = 1;
    upd_a.params = vec![Value::Text(key_a.clone())];
    match exec(&mut client, 97, &upd_a).await {
        Outcome::Ok(_) => {}
        other => panic!("tx_a's update failed: {other:?}"),
    }

    let mut upd_b = req(&format!(
        "UPDATE {WSKEW_TABLE} SET on_call = false WHERE key = ?"
    ));
    upd_b.tx_id = Some(tx_b);
    upd_b.readonly = false;
    upd_b.fetch = 1;
    upd_b.params = vec![Value::Text(key_b.clone())];
    match exec(&mut client, 98, &upd_b).await {
        Outcome::Ok(_) => {}
        other => panic!("tx_b's update failed: {other:?}"),
    }

    // tx_a commits first -- clean, no anomaly detected yet.
    match commit(&mut client, 99, tx_a).await {
        Outcome::Ok(_) => {}
        other => panic!("tx_a's commit (first committer) should succeed, got {other:?}"),
    }

    // tx_b's COMMIT is where Postgres's SSI detector identifies the pivot and aborts with 40001 --
    // THE commit-time case this test exists to prove. This is the SQLSTATE-preserving assertion:
    // pre-fix, `Checkout::commit_tx` -> `PgBackend::simple_query`'s `PoolError::Backend(msg)` would
    // have discarded the SQLSTATE and this would classify `Protocol`/`NonRetryable` instead.
    let ep = match commit(&mut client, 100, tx_b).await {
        Outcome::Error(ep) => ep,
        other => {
            panic!("tx_b's commit must be rejected with a serialization failure, got {other:?}")
        }
    };
    assert_eq!(
        ep.code,
        errc::SERIALIZATION_FAILURE,
        "a commit-time SSI write-skew abort must classify as SerializationFailure, got {ep:?}"
    );
    assert_eq!(
        ep.branch,
        branch::RETRYABLE,
        "a commit-time serialization failure must be Retryable, got {ep:?}"
    );
    assert_ne!(
        ep.branch,
        branch::NON_RETRYABLE,
        "must NOT be misclassified NonRetryable (the pre-fix M4b bug), got {ep:?}"
    );

    // The TX actor tears the tx down unconditionally on ANY Commit reply (Ok or Err) -- tx_b's
    // tx_id is already gone (conn returned to the pool via RAII), no explicit rollback needed.
    let mut cleanup = req(&format!("DELETE FROM {WSKEW_TABLE} WHERE key IN (?, ?)"));
    cleanup.readonly = false;
    cleanup.fetch = 1;
    cleanup.params = vec![Value::Text(key_a), Value::Text(key_b)];
    let _ = exec(&mut client, 101, &cleanup).await;

    assert_session_alive(&mut client, 102).await;
}
