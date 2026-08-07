//! M1-S6 Task 8 — the LIVE MySQL/MariaDB CHAOS suite: the §20.3 acceptance test for G7 (the
//! never-silent-unknown / never-transparent-retry guarantee), the MySQL PARITY of the PG
//! `chaos_fate_it.rs`. It proves the S4 `fate.rs` matrix (reused VERBATIM — NOT edited) holds against
//! real MySQL 8 / MariaDB 11, and that a write under chaos is applied AT MOST ONCE.
//!
//! Every test SKIPS (does not fail) when neither `FERRO_TEST_MYSQL_URL` nor `FERRO_TEST_MARIADB_URL`
//! is set, so `cargo test --workspace` stays green offline — same discipline as `mysql_it.rs`. Where
//! both are set, every scenario runs against BOTH dialects.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d mysql mariadb
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferrod --test mysql_chaos_it -- --nocapture
//! ```
//!
//! **The no-re-dispatch proof (ONE uniform mechanism for every write case).** A per-test counter row
//! (`ferro_s6_ctr`, `k` a run-unique string, `n` a SIGNED `BIGINT` seeded to 0 — there is no
//! `UPDATE … RETURNING` in MySQL, so the at-most-once check is a FRESH `SELECT n` read-back). The
//! write under chaos is `UPDATE ferro_s6_ctr SET n = n + 1 WHERE k = ? …`. After the chaos event, `n`
//! is read back via a FRESH autocommit EXEC (a brand-new pool checkout): it must be `0` (never
//! applied) or `1` (applied exactly once) — **never ≥2**, which would prove a silent re-dispatch.
//! This works against a REAL backend because the write under test is itself autocommit: if it ever
//! applied, MySQL already durably committed it server-side, independent of which physical pooled
//! connection answers the read-back.
//!
//! **Provably EXECUTING, never a fixed-delay guess (the mandatory false-green guard).** A kill/CANCEL
//! that lands BEFORE the statement is executing proves nothing. Every EXTERNAL chaos event (a
//! `KILL`/`KILL QUERY` from the raw side connection, or a client `CANCEL` frame) is preceded by
//! [`wait_for_active_conn`] polling `information_schema.processlist` — over a RAW side connection,
//! entirely outside ferrod's own pool — until the targeted statement is observed in an EXECUTING
//! command state (`COMMAND` `Execute`/`Query`, never `Prepare`) with a statement text containing a
//! run-unique marker. If never observed within the bound, the test SKIPS with a `skip:`-prefixed
//! line, which the CI live lane treats as a lane FAILURE — never a silent green on an unproven chaos
//! event. The two engine-timer cases (an `ExecRequest.timeout_ms` shorter than how long the write is
//! blocked) need no such proof: the timer provably cannot fire before `timeout_ms` has elapsed, by
//! which point the query is dispatched and still blocked on the lock.
//!
//! **Two MySQL-specific chaos-mechanics facts this suite is built around (verified live while
//! writing it):**
//!  1. `KILL QUERY` interrupting a bare `SELECT SLEEP(n)` makes `SLEEP` return `1` WITHOUT erroring
//!     the statement — the opposite of PG's `pg_sleep` (which errors `57014`). So a MySQL chaos write
//!     is NOT blocked with `SLEEP`; it is blocked on a **row lock** held by the raw side connection
//!     (via `SELECT … FOR UPDATE`). `KILL QUERY` on a lock-WAITING `UPDATE` DOES error (`1317`
//!     `ER_QUERY_INTERRUPTED`) — the reliable interruptible-with-error mechanism.
//!  2. **MariaDB STRIPS comments** from `information_schema.processlist.INFO` (MySQL preserves them),
//!     but BOTH preserve a **string literal** — so the in-flight marker is a `'…' <> ''` predicate
//!     embedded in the write SQL (not an inert comment), visible in processlist on both engines.
//!
//! The marker is bound as a PARAMETER in the poll query (never inlined) so the poll's own row can
//! never self-match; `ID <> CONNECTION_ID()` excludes the poller's thread as belt-and-braces. The
//! `ferro` testkit user has no `PROCESS` privilege, but a user always sees/kills its OWN account's
//! threads — the raw side connection and the ferrod pool are both `ferro`, so processlist visibility
//! and `KILL` work without a privilege grant (`innodb_trx`, which WOULD need `PROCESS`, is avoided).

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{
    TestClient, assert_session_alive, exec, exec_err, exec_ok, exec_server, mariadb_url, mysql_url,
    req,
};
use ferro_proto::consts::{branch, errc, flags, method_sql, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};
use ferro_proto::value::Value;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};

const CTR_TABLE: &str = "ferro_s6_ctr";
const DLK_TABLE: &str = "ferro_s6_dlk";

/// How long [`wait_for_active_conn`] waits for the marked statement to be observed EXECUTING before
/// giving up. Generous on purpose: the statement it waits for is blocked on a row lock this harness
/// itself holds, so once dispatched the state PERSISTS — only a genuinely stuck dispatch can burn
/// this bound, and burning it is now a CI lane FAILURE (the `skip:`-prefixed line), not a silent
/// green. A slow, contended runner must not be able to trip it.
const IN_FLIGHT_GUARD_BOUND: Duration = Duration::from_secs(15);

/// Liveness bound for case 6's 1205 terminal — the ONE step gated on a server-side timer (see
/// [`exec_within`]). 10s is ~5x the worst legitimate server-side latency (a 1s
/// `innodb_lock_wait_timeout` floor plus InnoDB's ~1s lock-wait monitor tick), so a genuinely wedged
/// request still fails the test loudly instead of a timing artifact doing it.
const LOCK_WAIT_TERMINAL_BOUND: Duration = Duration::from_secs(10);

// -------------------------------------------------------------------------------------------------
// Targets: run each scenario against every configured dialect (MySQL 8 + MariaDB 11) that is set.
// -------------------------------------------------------------------------------------------------

/// The set of `(label, dsn)` MySQL-family targets under test — MySQL 8 and/or MariaDB 11, whichever
/// env var is set. Empty → the caller SKIPS (offline). Both set → the scenario runs against both.
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

static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// A per-test-run unique token (`<prefix>_<pid>_<nanos>_<counter>`) — used both as the counter row's
/// primary key (so concurrent tests / reruns never collide on a row) and, embedded in a chaos
/// statement's SQL as a `'<marker>' <> ''` predicate, as the `processlist.INFO` marker
/// [`wait_for_active_conn`] filters on (so it never mistakes a DIFFERENT concurrently in-flight chaos
/// statement — on this same shared testkit database — for THIS test's own). Only `[A-Za-z0-9_]`, so
/// it is safe as a bare SQL string literal.
fn unique_key(prefix: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

// -------------------------------------------------------------------------------------------------
// Raw side connection (OUTSIDE ferrod's pool): the chaos-event actuator + in-flight prober + fixture.
// -------------------------------------------------------------------------------------------------

/// A raw side connection to the SAME MySQL/MariaDB, entirely OUTSIDE ferrod's own pool — used to
/// `KILL`/`KILL QUERY` a targeted backend, hold a row lock (so a chaos write is provably
/// interruptible-with-error), poll `information_schema.processlist`, and set up the counter fixture.
/// Resolves to the SAME vendored `mysql_async` fork `ferro-backend-mysql` uses (workspace
/// `[patch.crates-io]`). `mysql_async::Conn` is self-contained (no separate driver task to spawn,
/// unlike `tokio-postgres`).
async fn raw_connect(url: &str) -> Conn {
    let opts = Opts::from_url(url).expect("chaos harness: parse raw side DSN");
    Conn::new(opts)
        .await
        .expect("chaos harness: raw side connection to MySQL/MariaDB")
}

/// Poll `information_schema.processlist` (over the RAW side connection) every 15ms, up to
/// [`IN_FLIGHT_GUARD_BOUND`], until a backend is observed EXECUTING a statement whose text contains
/// `marker` — i.e. PROVABLY in flight (here: dispatched and blocked on the raw side's row lock), not
/// merely dispatched-but-not-yet-running, and certainly not "not yet dispatched at all" (the
/// false-green race). Returns that backend's thread id. Returns `None` (→ the caller SKIPS with a
/// `skip:` line, which the CI live lane fails on) if the bound elapses — better an inconclusive,
/// LOUD skip than a chaos event that proves nothing.
///
/// **`COMMAND` is load-bearing, not decoration — this filter IS the C14 flake fix.** `INFO` also
/// carries the statement text during `COM_STMT_PREPARE`: ferrod's row-returning path is
/// prepare-THEN-execute (two round trips — `Conn::prep` then `exec_iter`, see
/// `ferro-backend-mysql`'s `query::run`), so a marker match alone can mean "the server is PREPARING
/// this statement", which is NOT in flight. Observed live: under parallel load 3 of 240 guard
/// matches were `COMMAND = 'Prepare', STATE = 'Opening tables'`.
///
/// Between the prepare's reply and the execute's request the pooled connection is IDLE, and what a
/// `KILL QUERY` aimed there does is a RACE in the server's own command loop — observed live BOTH
/// ways on these engines: fired at an idle connection it left a following `SELECT SLEEP(4)` running
/// its full 4s (lost), and under load it cut a following `SELECT SLEEP(1)` short (carried over).
/// Either way a CANCEL fired off a `Prepare` match proves nothing about the write's fate; and in the
/// LOST case it also wedges the test, because the write then blocks on the raw side's row lock
/// (released only AFTER the terminal is read) until `innodb_lock_wait_timeout` (50s by default), far
/// past the harness's 2s per-frame bound, so no terminal ever arrives.
///
/// So the guard demands an EXECUTING command state: `Execute` (`COM_STMT_EXECUTE`, the binary
/// protocol ferrod's `query` path uses) or `Query` (`COM_QUERY`, the text protocol the pin hook
/// uses) — never `Prepare`. That is also the STABLE state here: once the marked statement is
/// executing it is blocked on a lock this harness itself holds until after the terminal is read, so
/// it cannot leave `Execute` on its own. The guard therefore waits for a state that PERSISTS instead
/// of sampling for one that is transient — which is what makes it deterministic rather than a race.
///
/// NOTE (why there is no unit-style regression test pinning this filter): the `Prepare` phase cannot
/// be held open on demand. A prepare opens its tables under a plain `MDL_SHARED`
/// (`MYSQL_OPEN_FORCE_SHARED_MDL`), so neither a `LOCK TABLES ... WRITE` holder nor a pending
/// `ALTER TABLE` exclusive request blocks it (both tried live — the ALTER parks, the prepare sails
/// past), and the phase itself costs ≤10ms even for a 61-table join. The protections against this
/// filter being dropped again are therefore this comment and the CI live lane, which FAILS on the
/// `skip:` line the guard emits when it cannot prove execution.
///
/// The `marker` is bound as a PARAMETER (never inlined into the poll SQL), so the poll's own
/// processlist row can never self-match; `ID <> CONNECTION_ID()` excludes the poller as belt-and-
/// braces.
async fn wait_for_active_conn(raw: &mut Conn, marker: &str) -> Option<u64> {
    let pattern = format!("%{marker}%");
    let deadline = Instant::now() + IN_FLIGHT_GUARD_BOUND;
    loop {
        // An IDLE conn has `INFO = NULL` (never matches `LIKE`), so the marker predicate excludes
        // idle threads on its own; the `COMMAND` predicate is what additionally excludes the
        // PREPARING-but-not-yet-executing phase (see the doc above). `ID <> CONNECTION_ID()`
        // excludes the poller's own thread.
        let id: Option<u64> = raw
            .exec_first(
                "SELECT ID FROM information_schema.processlist \
                 WHERE ID <> CONNECTION_ID() AND INFO LIKE ? \
                   AND COMMAND IN ('Execute', 'Query')",
                (pattern.clone(),),
            )
            .await
            .expect("chaos harness: processlist poll");
        if let Some(id) = id {
            return Some(id);
        }
        if Instant::now() >= deadline {
            // Diagnostic on the false-green skip path: dump every non-idle same-account thread so a
            // never-observed marker is debuggable (COMMAND/STATE/INFO), not a silent mystery.
            let rows: Vec<(u64, String, String, Option<String>)> = raw
                .query(
                    "SELECT ID, COMMAND, STATE, INFO FROM information_schema.processlist \
                     WHERE ID <> CONNECTION_ID() AND INFO IS NOT NULL",
                )
                .await
                .unwrap_or_default();
            eprintln!("chaos guard: marker {marker:?} not observed; live threads = {rows:?}");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// `KILL <id>` over the raw side connection — terminates the whole CONNECTION (its in-flight
/// statement's link dies → a transport error → `is_fatal` → `ConnectionLost`). Best-effort: a race
/// where the conn already went away is not a failure.
async fn kill_conn(raw: &mut Conn, id: u64) {
    let _ = raw.query_drop(format!("KILL {id}")).await;
}

/// `CREATE TABLE IF NOT EXISTS` the counter table over the raw side connection (fixture setup,
/// out-of-band from ferrod). Signed `BIGINT`, so the read-back maps to `Value::I64` — this suite
/// asserts §19.3 fates, not type coverage, and a signed counter keeps those assertions on one tag.
/// (`BIGINT UNSIGNED` is no longer out of scope: M1-S7 admits it as `U64`.)
async fn raw_ensure_table(raw: &mut Conn, table: &str) {
    raw.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {table} (k VARCHAR(190) PRIMARY KEY, n BIGINT NOT NULL)"
    ))
    .await
    .expect("chaos harness: create counter table");
}

/// Seed (or reset) `key`'s counter row to `n = 0` over the raw side connection — an upsert, so a
/// rerun with a colliding key (never expected, keys are run-unique) still starts from a known state.
async fn raw_seed(raw: &mut Conn, table: &str, key: &str) {
    raw.exec_drop(
        format!("INSERT INTO {table} (k, n) VALUES (?, 0) ON DUPLICATE KEY UPDATE n = 0"),
        (key.to_string(),),
    )
    .await
    .expect("chaos harness: seed counter row");
}

/// Open an explicit tx on the raw side connection and take an EXCLUSIVE row lock on `key` via
/// `SELECT … FOR UPDATE` (WITHOUT modifying `n`), so a ferrod `UPDATE … WHERE k = ?` on the same row
/// BLOCKS on it — the reliable, interruptible-with-error way to hold a MySQL write in flight (a bare
/// `SLEEP()` self-terminates on `KILL QUERY` without erroring; see the module doc). Released by
/// [`raw_release`].
async fn raw_lock_row(raw: &mut Conn, table: &str, key: &str) {
    raw.query_drop("START TRANSACTION")
        .await
        .expect("chaos harness: raw BEGIN (lock holder)");
    let _: Option<i64> = raw
        .exec_first(
            format!("SELECT n FROM {table} WHERE k = ? FOR UPDATE"),
            (key.to_string(),),
        )
        .await
        .expect("chaos harness: raw SELECT … FOR UPDATE (acquire row lock)");
}

/// Release the raw side connection's held row lock (rolls its lock-holder tx back; `FOR UPDATE` never
/// modified `n`, so nothing to undo).
async fn raw_release(raw: &mut Conn) {
    raw.query_drop("ROLLBACK")
        .await
        .expect("chaos harness: raw ROLLBACK (release row lock)");
}

// -------------------------------------------------------------------------------------------------
// ferrod-side helpers (client → ferrod → pool → DB): the write-under-chaos + the at-most-once read.
// -------------------------------------------------------------------------------------------------

/// A chaos write against `key`: `UPDATE … SET n = n + 1 WHERE k = ? AND '<marker>' <> ''`. The marker
/// is a string literal (visible in processlist INFO on BOTH MySQL and MariaDB — comments are
/// MariaDB-stripped); the row lock held by the raw side connection is what keeps it in flight.
fn chaos_write_sql(table: &str, marker: &str) -> String {
    format!("UPDATE {table} SET n = n + 1 WHERE k = ? AND '{marker}' <> ''")
}

/// Read `n` back for `key` via a FRESH autocommit EXEC (a brand-new request — the pool may hand back
/// any pooled connection). A valid no-re-dispatch proof because the write under chaos is ITSELF
/// autocommit: if it ever applied at all, MySQL already durably committed it server-side, independent
/// of which physical connection answers this read (a plain `SELECT` is a non-blocking MVCC read — it
/// never blocks on the raw side's held X lock).
async fn read_ctr(client: &mut TestClient, rid: u32, table: &str, key: &str) -> i64 {
    let mut r = req(&format!("SELECT n FROM {table} WHERE k = ?"));
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

/// Receive the next frame for `rid`, asserting the shared one-`END`/service/method shape (charter
/// rule 4), and decode its `Outcome`. Used (instead of `common::exec`, which sends AND receives
/// atomically) whenever a chaos event must happen BETWEEN send and receive — an external kill/CANCEL
/// proven in flight.
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

/// `common::exec` (send + read the one terminal, same shape assertions) but with an EXPLICIT
/// liveness deadline instead of the harness-wide 2s `RECV_TIMEOUT`.
///
/// Needed by exactly one step: case 6's 1205 write. Its terminal is gated on a SERVER-side timer the
/// harness cannot make faster — `innodb_lock_wait_timeout` has a 1s FLOOR, and InnoDB's lock-wait
/// monitor only ticks about once a second, so a `= 1` statement legitimately errors 1205 anywhere up
/// to ~2s after it starts waiting. That is already at the default 2s bound with zero margin, and past
/// it under parallel-suite load: reproduced live at 16/25 full-suite runs under half-core contention,
/// every failure the same `client recv: timed out after 2s` on that one step.
///
/// This widens ONLY the harness's liveness bound. Every §19.3 assertion on the terminal it returns is
/// unchanged (still exactly one END, still `SerializationFailure`/`Retryable`, still never
/// `Indeterminate`), and a genuinely wedged request still FAILS the test — just at `dur`, not at 2s.
async fn exec_within(
    client: &mut TestClient,
    rid: u32,
    request: &ExecRequest,
    dur: Duration,
) -> Outcome {
    client
        .send_request(rid, service::SQL, method_sql::EXEC, request.encode())
        .await;
    let t = client
        .recv_or_none(dur)
        .await
        .unwrap_or_else(|| panic!("no EXEC terminal for rid {rid} within {dur:?}"));
    assert_eq!(t.header.request_id, rid, "terminal echoes the request id");
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "exactly one END frame per request (charter rule 4)"
    );
    assert_eq!(t.header.service, service::SQL);
    assert_eq!(t.header.method, method_sql::EXEC);
    Outcome::decode(&t.payload).expect("decode terminal Outcome")
}

// ---- TX plumbing (this suite only ever needs a bare BEGIN) ---------------------------------------

/// BEGIN a bare transaction (isolation=None, readonly=false → the composed SQL is a bare
/// `START TRANSACTION`; the isolation/readonly forms landed in M1-S8a and are gated in
/// `mysql_it.rs`, SPEC §22.2 (s)). Returns its `tx_id`.
async fn begin(client: &mut TestClient, rid: u32, pool: &str) -> u64 {
    let breq = BeginRequest {
        pool: pool.to_string(),
        isolation: None,
        readonly: false,
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

/// Receive frames until every rid in `want` has been seen exactly once, matching by request id
/// rather than arrival order — for concurrently in-flight requests whose completion order is
/// genuinely unconstrained (two independent tx actors' terminals racing).
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

// =================================================================================================
// Case 1. timeout_ms autocommit write -> WriteUnconfirmed{Indeterminate}; counter in {0,1}.
//         (engine-internal timer -> KILL QUERY -> 1317 -> CANCELLED -> is_57014 override; NO
//          processlist guard needed: the 150ms timer provably cannot fire before dispatch, and the
//          write is held in flight by the raw side's row lock the whole time.)
// =================================================================================================

#[tokio::test(flavor = "multi_thread")]
async fn mysql_timeout_autocommit_write_is_indeterminate() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let mut raw = raw_connect(&url).await;
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let key = unique_key("to_write");
        let marker = unique_key("to_mark");
        raw_ensure_table(&mut raw, CTR_TABLE).await;
        raw_seed(&mut raw, CTR_TABLE, &key).await;

        // Hold the row lock so the ferrod write blocks in flight (KILL QUERY on a lock-waiting UPDATE
        // errors 1317; a SLEEP would self-terminate without erroring — see the module doc).
        raw_lock_row(&mut raw, CTR_TABLE, &key).await;

        let mut w = req(&chaos_write_sql(CTR_TABLE, &marker));
        w.readonly = false;
        w.timeout_ms = Some(150); // << far shorter than the (indefinite) lock wait: the engine timer wins
        w.params = vec![Value::Text(key.clone())];

        let ep = exec_err(&mut client, 12, &w).await;
        assert_eq!(
            ep.code,
            errc::WRITE_UNCONFIRMED,
            "[{label}] an engine-enforced timeout on a DISPATCHED autocommit write -> Indeterminate, got {ep:?}"
        );
        assert_eq!(ep.branch, branch::INDETERMINATE);

        raw_release(&mut raw).await;

        let n = read_ctr(&mut client, 13, CTR_TABLE, &key).await;
        assert!(
            n == 0 || n == 1,
            "[{label}] counter must be 0 or 1 (never re-dispatched), got {n}"
        );
        eprintln!(
            "[{label}] case1 timeout autocommit write: fate=WriteUnconfirmed/Indeterminate readback_n={n}"
        );

        assert_session_alive(&mut client, 15).await;
    }
}

// =================================================================================================
// Case 2. Client CANCEL frame mid-write (EXTERNAL, GUARDED) -> Indeterminate (cancel won) OR Ok
//         (cancel lost the race); either way the counter is consistent and never re-dispatched.
// =================================================================================================

#[tokio::test(flavor = "multi_thread")]
async fn mysql_cancel_frame_autocommit_write_consistent() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let mut raw = raw_connect(&url).await;
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let key = unique_key("cancel_write");
        let marker = unique_key("cancel_mark");
        raw_ensure_table(&mut raw, CTR_TABLE).await;
        raw_seed(&mut raw, CTR_TABLE, &key).await;
        raw_lock_row(&mut raw, CTR_TABLE, &key).await;

        let mut w = req(&chaos_write_sql(CTR_TABLE, &marker));
        w.readonly = false;
        w.params = vec![Value::Text(key.clone())];

        let rid = 22;
        client
            .send_request(rid, service::SQL, method_sql::EXEC, w.encode())
            .await;

        // Provably EXECUTING (dispatched AND blocked on the raw side's row lock, never merely
        // being PREPARED) before firing CANCEL: a KILL QUERY that lands while the pooled conn is
        // between COM_STMT_PREPARE and COM_STMT_EXECUTE is silently lost (case 0 proves it), which
        // would make this cancel prove nothing and wedge the request on the row lock.
        let Some(_id) = wait_for_active_conn(&mut raw, &marker).await else {
            eprintln!(
                "skip: [{label}] case2 -- the write was never observed EXECUTING within 5s \
                 (the CANCEL would be a false-green race); the CI live lane fails on this line"
            );
            raw_release(&mut raw).await;
            continue;
        };
        client.cancel(rid).await;

        let outcome = recv_terminal(&mut client, rid, service::SQL, method_sql::EXEC).await;
        raw_release(&mut raw).await;
        let n = read_ctr(&mut client, 23, CTR_TABLE, &key).await;

        match outcome {
            Outcome::Error(ep) => {
                assert_eq!(
                    ep.code,
                    errc::WRITE_UNCONFIRMED,
                    "[{label}] a cancelled DISPATCHED autocommit write -> Indeterminate, got {ep:?}"
                );
                assert_eq!(ep.branch, branch::INDETERMINATE);
                assert!(
                    n == 0 || n == 1,
                    "[{label}] Indeterminate must be consistent with a counter in {{0,1}}, got {n}"
                );
                eprintln!(
                    "[{label}] case2 CANCEL autocommit write: fate=WriteUnconfirmed/Indeterminate readback_n={n}"
                );
            }
            Outcome::Ok(_) => {
                assert_eq!(
                    n, 1,
                    "[{label}] the cancel lost the race (Ok) -- the write genuinely completed, so n must be exactly 1"
                );
                eprintln!(
                    "[{label}] case2 CANCEL autocommit write: cancel lost the race, Ok, readback_n=1"
                );
            }
            Outcome::Cancelled => panic!(
                "[{label}] Outcome::Cancelled is retired from the SQL EXEC path -- a fated cancel \
                 always rides a branch-carrying Outcome::Error"
            ),
        }
        assert!(
            n < 2,
            "[{label}] no re-dispatch: the counter must never reach 2, got {n}"
        );

        assert_session_alive(&mut client, 25).await;
    }
}

// =================================================================================================
// Case 3. Connection KILL mid-write (EXTERNAL `KILL <id>`, GUARDED) -> the conn dies -> a transport
//         error -> is_fatal -> ConnectionLost -> autocommit dispatched write -> Indeterminate;
//         counter in {0,1}. The ferrod SESSION survives (only that one pooled conn died).
// =================================================================================================

#[tokio::test(flavor = "multi_thread")]
async fn mysql_connection_kill_mid_write_is_indeterminate() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let mut raw = raw_connect(&url).await;
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let key = unique_key("kill_write");
        let marker = unique_key("kill_mark");
        raw_ensure_table(&mut raw, CTR_TABLE).await;
        raw_seed(&mut raw, CTR_TABLE, &key).await;
        raw_lock_row(&mut raw, CTR_TABLE, &key).await;

        let mut w = req(&chaos_write_sql(CTR_TABLE, &marker));
        w.readonly = false;
        w.params = vec![Value::Text(key.clone())];

        let rid = 32;
        client
            .send_request(rid, service::SQL, method_sql::EXEC, w.encode())
            .await;

        // Provably EXECUTING (blocked on the raw side's row lock): observe the EXACT pool conn's
        // thread id running our marked statement before killing the whole connection. (`KILL <id>`
        // is phase-INsensitive -- it destroys the connection whatever it is doing -- so this case
        // does not depend on the COMMAND filter the way case 2's KILL QUERY does; it still uses the
        // one shared guard so both prove the same thing.)
        let Some(id) = wait_for_active_conn(&mut raw, &marker).await else {
            eprintln!(
                "skip: [{label}] case3 -- the write was never observed EXECUTING within 5s \
                 (the KILL would be a false-green race); the CI live lane fails on this line"
            );
            raw_release(&mut raw).await;
            continue;
        };
        kill_conn(&mut raw, id).await;

        let ep = match recv_terminal(&mut client, rid, service::SQL, method_sql::EXEC).await {
            Outcome::Error(ep) => ep,
            other => panic!("[{label}] a killed in-flight write must error, got {other:?}"),
        };
        assert_eq!(
            ep.code,
            errc::WRITE_UNCONFIRMED,
            "[{label}] a killed, DISPATCHED autocommit write's non-execution is unconfirmed -> Indeterminate, got {ep:?}"
        );
        assert_eq!(ep.branch, branch::INDETERMINATE);

        raw_release(&mut raw).await;

        let n = read_ctr(&mut client, 33, CTR_TABLE, &key).await;
        assert!(
            n == 0 || n == 1,
            "[{label}] counter must be 0 or 1 (never re-dispatched), got {n}"
        );
        eprintln!(
            "[{label}] case3 KILL-conn autocommit write: fate=WriteUnconfirmed/Indeterminate readback_n={n}"
        );

        // The ferrod session (and pool) survive a killed pooled CONN: only that one conn died.
        assert_session_alive(&mut client, 35).await;
    }
}

// =================================================================================================
// Case 4. in-tx timeout_ms write -> the actor cancels (KILL QUERY) + drains + ROLLBACK + tombstones
//         -> the ONE terminal is TxDeadline{Retryable}; counter == 0 (rolled back); tx_id unusable.
//         (a bare BEGIN — this case does not need an isolation level.)
// =================================================================================================

#[tokio::test(flavor = "multi_thread")]
async fn mysql_in_tx_timeout_rolls_back_to_retryable() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let mut raw = raw_connect(&url).await;
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let key = unique_key("tx_to_write");
        let marker = unique_key("tx_to_mark");
        raw_ensure_table(&mut raw, CTR_TABLE).await;
        raw_seed(&mut raw, CTR_TABLE, &key).await;
        raw_lock_row(&mut raw, CTR_TABLE, &key).await;

        let tx_id = begin(&mut client, 42, "default").await;

        let mut w = req(&chaos_write_sql(CTR_TABLE, &marker));
        w.tx_id = Some(tx_id);
        w.readonly = false;
        w.timeout_ms = Some(150);
        w.params = vec![Value::Text(key.clone())];

        let ep = match exec(&mut client, 43, &w).await {
            Outcome::Error(ep) => ep,
            other => panic!("[{label}] a tx-scoped statement timeout must error, got {other:?}"),
        };
        assert_eq!(
            ep.code,
            errc::TX_DEADLINE,
            "[{label}] a tx-scoped timeout rolls the tx back -> TxDeadline/Retryable, got {ep:?}"
        );
        assert_eq!(ep.branch, branch::RETRYABLE);
        assert_ne!(
            ep.branch,
            branch::INDETERMINATE,
            "[{label}] a rolled-back in-tx statement persisted nothing -- never Indeterminate"
        );

        // The tx_id is unusable afterward (tombstoned): a re-touch yields TxDeadline again, never a
        // silent re-run of the statement (charter rule 3).
        let mut probe = req("SELECT 1");
        probe.tx_id = Some(tx_id);
        match exec(&mut client, 44, &probe).await {
            Outcome::Error(ep2) => assert_eq!(
                ep2.code,
                errc::TX_DEADLINE,
                "[{label}] the timed-out tx stays tombstoned, got {ep2:?}"
            ),
            other => panic!("[{label}] expected TxDeadline on the dead tx_id, got {other:?}"),
        }

        raw_release(&mut raw).await;

        let n = read_ctr(&mut client, 45, CTR_TABLE, &key).await;
        assert_eq!(
            n, 0,
            "[{label}] the tx was rolled back -- the write never committed, got {n}"
        );
        eprintln!(
            "[{label}] case4 in-tx timeout write: fate=TxDeadline/Retryable readback_n={n} (tombstoned)"
        );

        assert_session_alive(&mut client, 47).await;
    }
}

// =================================================================================================
// Case 5. Deadlock (errno 1213) between two concurrent bare-BEGIN txs cross-locking two rows in
//         opposite order -> InnoDB kills exactly one victim -> error_map -> Deadlock{Retryable},
//         never Indeterminate. The surviving write is applied AT MOST ONCE (both rows end == 1).
//         Also the 1213 HALF of the tx-actor re-verification: the victim's tx was auto-rolled-back
//         by InnoDB (tx closed), yet the actor's explicit ROLLBACK is idempotent -> Ok.
// =================================================================================================

#[tokio::test(flavor = "multi_thread")]
async fn mysql_deadlock_1213_is_retryable_live() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let mut raw = raw_connect(&url).await;
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let run = unique_key("dlk");
        let key_a = format!("{run}_a");
        let key_b = format!("{run}_b");
        let marker = format!("{run}_cross");
        raw_ensure_table(&mut raw, DLK_TABLE).await;
        raw_seed(&mut raw, DLK_TABLE, &key_a).await;
        raw_seed(&mut raw, DLK_TABLE, &key_b).await;

        let tx_a = begin(&mut client, 60, "default").await;
        let tx_b = begin(&mut client, 61, "default").await;

        // Each tx locks its OWN row first -- uncontended, both succeed immediately.
        for (rid, tx, key) in [(62, tx_a, &key_a), (63, tx_b, &key_b)] {
            let mut w = req(&format!("UPDATE {DLK_TABLE} SET n = n + 1 WHERE k = ?"));
            w.tx_id = Some(tx);
            w.readonly = false;
            w.params = vec![Value::Text(key.clone())];
            match exec(&mut client, rid, &w).await {
                Outcome::Ok(_) => {}
                other => panic!("[{label}] own-row lock {key:?} failed: {other:?}"),
            }
        }

        // cross_a: tx_a wants tx_b's row. Send WITHOUT awaiting -> it blocks on tx_b's lock.
        let rid_ca = 64;
        let mut cross_a = req(&format!(
            "UPDATE {DLK_TABLE} SET n = n + 1 WHERE k = ? AND '{marker}' <> ''"
        ));
        cross_a.tx_id = Some(tx_a);
        cross_a.readonly = false;
        cross_a.params = vec![Value::Text(key_b.clone())];
        client
            .send_request(rid_ca, service::SQL, method_sql::EXEC, cross_a.encode())
            .await;

        // GUARD: wait until cross_a is provably EXECUTING (blocked-waiting) before closing the cycle -- so the
        // cross_b send below can never "win" against a cross_a that had not yet reached the server
        // (the false-green race the PG suite's `wait_for_two_lock_waiters` addresses; here one
        // confirmed lock-waiter suffices because the second update deterministically CLOSES the cycle
        // InnoDB then detects immediately).
        let Some(_id) = wait_for_active_conn(&mut raw, &marker).await else {
            eprintln!(
                "skip: [{label}] case5 -- cross_a was never observed EXECUTING (lock-waiting) \
                 within 5s (the cross-lock cycle would be a false-green race); the CI live lane \
                 fails on this line"
            );
            // Do NOT try to ROLLBACK here: tx_a's actor is blocked serving the stuck cross_a query,
            // so a ROLLBACK command would queue behind it and hang. Dropping `client` at end of this
            // iteration closes the UDS -> the session aborts both tx actors (session death).
            continue;
        };

        // cross_b: tx_b wants tx_a's row -> the classic AB-BA cycle -> InnoDB kills one with 1213.
        let rid_cb = 65;
        let mut cross_b = req(&format!(
            "UPDATE {DLK_TABLE} SET n = n + 1 WHERE k = ? AND '{marker}' <> ''"
        ));
        cross_b.tx_id = Some(tx_b);
        cross_b.readonly = false;
        cross_b.params = vec![Value::Text(key_a.clone())];
        client
            .send_request(rid_cb, service::SQL, method_sql::EXEC, cross_b.encode())
            .await;

        let mut terminals = collect_terminals(&mut client, vec![rid_ca, rid_cb]).await;
        let outcome_a = terminals.remove(&rid_ca).expect("rid_ca terminal");
        let outcome_b = terminals.remove(&rid_cb).expect("rid_cb terminal");

        // EXACTLY one Deadlock{Retryable} victim + one Ok survivor, NEVER Indeterminate. Identify the
        // survivor (Ok -> COMMIT) and the victim (Error -> ROLLBACK). The victim's ROLLBACK is the
        // 1213 half of the tx-actor re-verification: InnoDB already auto-rolled-back the whole tx, so
        // the actor's explicit `co.rollback_tx()` runs against an already-closed tx and MUST be
        // idempotent (a clean Ok, no error, no double-terminal).
        let mut saw_deadlock = false;
        let mut saw_ok = false;
        let mut commit_rid = 66;
        let mut rollback_rid = 67;
        for (label2, rid, tx, outcome) in [
            ("cross_a", rid_ca, tx_a, &outcome_a),
            ("cross_b", rid_cb, tx_b, &outcome_b),
        ] {
            let _ = rid;
            match outcome {
                Outcome::Ok(_) => {
                    saw_ok = true;
                    match commit(&mut client, commit_rid, tx).await {
                        Outcome::Ok(_) => {}
                        other => panic!(
                            "[{label}] {label2}: survivor COMMIT should succeed, got {other:?}"
                        ),
                    }
                    commit_rid += 100;
                }
                Outcome::Error(ep) => {
                    assert_ne!(
                        ep.branch,
                        branch::INDETERMINATE,
                        "[{label}] {label2}: an in-tx statement's fate must never be Indeterminate (§19.3), got {ep:?}"
                    );
                    assert_eq!(
                        ep.code,
                        errc::DEADLOCK,
                        "[{label}] {label2}: the only error tolerated here is the deadlock victim's, got {ep:?}"
                    );
                    assert_eq!(
                        ep.branch,
                        branch::RETRYABLE,
                        "[{label}] {label2}: a deadlock must be Retryable, got {ep:?}"
                    );
                    saw_deadlock = true;
                    match rollback(&mut client, rollback_rid, tx).await {
                        Outcome::Ok(_) => {}
                        other => panic!(
                            "[{label}] {label2}: the victim's explicit ROLLBACK must be idempotent Ok \
                             against the already-auto-rolled-back (1213) tx, got {other:?}"
                        ),
                    }
                    rollback_rid += 100;
                }
                Outcome::Cancelled => panic!("[{label}] {label2}: Outcome::Cancelled is retired"),
            }
        }
        assert!(
            saw_deadlock,
            "[{label}] neither cross-update surfaced Deadlock/Retryable: a={outcome_a:?} b={outcome_b:?}"
        );
        assert!(
            saw_ok,
            "[{label}] the survivor's cross-update must complete Ok: a={outcome_a:?} b={outcome_b:?}"
        );

        // AT-MOST-ONCE: the survivor committed its own + cross increments; the victim's whole tx was
        // rolled back. Whichever side won, both rows end at EXACTLY 1 -- never 2 (which would prove a
        // statement double-applied / was re-dispatched, charter rule 3).
        let na = read_ctr(&mut client, 80, DLK_TABLE, &key_a).await;
        let nb = read_ctr(&mut client, 81, DLK_TABLE, &key_b).await;
        assert_eq!(na, 1, "[{label}] key_a applied at most once, got {na}");
        assert_eq!(nb, 1, "[{label}] key_b applied at most once, got {nb}");
        eprintln!(
            "[{label}] case5 deadlock: fate=Deadlock(1213)/Retryable (one victim + one Ok, never Indeterminate); \
             victim ROLLBACK idempotent Ok; at-most-once key_a={na} key_b={nb}"
        );

        assert_session_alive(&mut client, 82).await;
    }
}

// =================================================================================================
// Case 6. Lock-wait timeout (errno 1205) -> Retryable, AND the 1205 HALF of the tx-actor
//         re-verification. With `innodb_rollback_on_timeout` OFF (the default), 1205 rolls back only
//         the STATEMENT and leaves the tx `InTx` -- UNLIKE 1213 (whole-tx auto-rollback). Verify the
//         actor's explicit ROLLBACK teardown is correct against a tx LEFT OPEN by 1205: the tx tears
//         down to exactly one Retryable statement terminal, then a clean ROLLBACK Ok, no double
//         terminal, no stuck-open tx.
// =================================================================================================

#[tokio::test(flavor = "multi_thread")]
async fn mysql_lock_wait_timeout_1205_tx_teardown_retryable() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let mut raw = raw_connect(&url).await;
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let key = unique_key("lwt_write");
        let marker = unique_key("lwt_mark");
        raw_ensure_table(&mut raw, CTR_TABLE).await;
        raw_seed(&mut raw, CTR_TABLE, &key).await;

        // The raw side holds the row lock; tx_a will wait on it and time out with 1205.
        raw_lock_row(&mut raw, CTR_TABLE, &key).await;

        let tx_a = begin(&mut client, 90, "default").await;

        // Shrink tx_a's lock-wait timeout to the 1s minimum (the smallest value InnoDB accepts) so
        // the 1205 lands promptly. NOTE the terminal below is read with `exec_within`, not the
        // harness-wide 2s bound: 1s is a FLOOR, and InnoDB's lock-wait monitor ticks about once a
        // second, so 1205 can legitimately surface anywhere up to ~2s later — see `exec_within`.
        let mut s = req("SET SESSION innodb_lock_wait_timeout = 1");
        s.tx_id = Some(tx_a);
        s.readonly = false;
        match exec(&mut client, 91, &s).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] SET innodb_lock_wait_timeout failed: {other:?}"),
        }

        // The contending write: waits on the raw side's lock -> after ~1s -> 1205. This resolves as a
        // server STATEMENT error (ExecStep::Completed), NOT the engine's own timer -> error_map maps
        // 1205 -> SerializationFailure/Retryable (errno-keyed: 1205's SQLSTATE is HY000, which a
        // class-40 heuristic would miss). NO timeout_ms here (that would fire KILL QUERY -> 1317).
        let mut w = req(&chaos_write_sql(CTR_TABLE, &marker));
        w.tx_id = Some(tx_a);
        w.readonly = false;
        w.params = vec![Value::Text(key.clone())];

        let ep = match exec_within(&mut client, 92, &w, LOCK_WAIT_TERMINAL_BOUND).await {
            Outcome::Error(ep) => ep,
            other => {
                panic!("[{label}] the lock-wait-timeout write must error (1205), got {other:?}")
            }
        };
        assert_eq!(
            ep.code,
            errc::SERIALIZATION_FAILURE,
            "[{label}] 1205 lock-wait timeout -> SerializationFailure/Retryable (errno-keyed), got {ep:?}"
        );
        assert_eq!(ep.branch, branch::RETRYABLE);
        assert_ne!(
            ep.branch,
            branch::INDETERMINATE,
            "[{label}] a lock-wait timeout has a KNOWN fate (server rejected the statement) -- never Indeterminate"
        );

        // RE-VERIFICATION (1205 half): 1205 left the tx `InTx` (statement-only rollback). The actor's
        // explicit ROLLBACK teardown -- the SAME `co.rollback_tx()` the deadline/abort teardown runs
        // -- must correctly close the still-open tx: a clean Ok, exactly one terminal, no stuck tx.
        match rollback(&mut client, 93, tx_a).await {
            Outcome::Ok(_) => {}
            other => panic!(
                "[{label}] the actor's explicit ROLLBACK must correctly close a tx LEFT OPEN by 1205, got {other:?}"
            ),
        }

        // No stuck-open tx: the tx_id is gone (deregistered by the clean ROLLBACK) -- a re-touch fails.
        let mut probe = req("SELECT 1");
        probe.tx_id = Some(tx_a);
        match exec(&mut client, 94, &probe).await {
            Outcome::Error(_) => {}
            other => panic!(
                "[{label}] the rolled-back tx_id must be gone (no stuck-open tx), got {other:?}"
            ),
        }

        raw_release(&mut raw).await;

        // Nothing committed (the 1205 statement rolled back, the tx then rolled back).
        let n = read_ctr(&mut client, 95, CTR_TABLE, &key).await;
        assert_eq!(
            n, 0,
            "[{label}] nothing committed after 1205 + ROLLBACK, got {n}"
        );
        eprintln!(
            "[{label}] case6 lock-wait-timeout(1205): fate=SerializationFailure/Retryable; \
             tx LEFT InTx -> explicit ROLLBACK Ok (one terminal, no stuck tx); readback_n={n}"
        );

        assert_session_alive(&mut client, 96).await;
    }
}
