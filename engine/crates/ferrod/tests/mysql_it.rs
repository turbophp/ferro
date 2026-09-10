//! M1-S6 Task 5 — the LIVE MySQL/MariaDB daemon path end to end: a real client → `ferrod` session
//! → the heterogeneous pool registry (`AnyPool::Mysql`) → the generic EXEC/TX handler bodies
//! (`run_exec_on_pool` / `begin_on_pool`) → live Dockerized MySQL 8 / MariaDB 11 → single terminal
//! `END` → client. Proves that a `kind = mysql` pool (inferred from the `mysql://` DSN scheme):
//!
//!  * round-trips a BUFFERED `SELECT` (`fetch:rows`) through the SAME generic autocommit body PG
//!    uses (the monomorphic → heterogeneous registry fix);
//!  * runs a `BEGIN .. COMMIT` transaction through the tx path (the actor spawns a
//!    `Checkout<MysqlBackend>`; the routing + terminal are backend-agnostic);
//!  * STREAMS `fetch:stream` on BOTH EXEC arms — autocommit and tx-scoped — off the ONE
//!    `PoolBackend::supports_row_streaming()` authority (dev-loop B2b-2b closed SPEC §22.2 (n)'s
//!    deferral), handing the PARKED connection back so the session and the transaction survive it;
//!  * (M1-S8a Task 8) opens an ISOLATION-scoped and/or READ ONLY transaction — the dialect-aware
//!    `compose_begin_sql` batch (SPEC §22.2 (s)), which before this slice was ERROR 1064.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL` are unset
//! — same discipline as `sql_exec_it.rs` / `tx_it.rs` — so `cargo test --workspace` stays green
//! offline. Where BOTH are set, every scenario runs against BOTH dialects.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferrod --test mysql_it -- --nocapture
//! ```

mod common;

use std::time::Duration;

use common::{TestClient, exec, exec_err, exec_ok, mariadb_url, mysql_url, req};
use ferro_proto::consts::{branch, errc, flags, method_sql, method_stream, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::sql::StreamData;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, Isolation, TxControl};
use ferro_proto::value::Value;
use ferrod::services::sql::{FETCH_NONE, FETCH_ROWS, FETCH_STREAM};
use ferrod::session::codec::InFrame;

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

// -------------------------------------------------------------------------------------------------
// Minimal TX client helpers (a self-contained subset of `tx_it.rs`'s, kept local so the MySQL story
// lives in one file).
// -------------------------------------------------------------------------------------------------

/// `service=TX, method=BEGIN` — assert the one-END terminal shape and decode the `BeginResponse`.
///
/// `isolation`/`readonly` were hard-coded to `None`/`false` before M1-S8a, because the PG-flavoured
/// `BEGIN READ ONLY` / `BEGIN ISOLATION LEVEL …` forms are ERROR 1064 on MySQL and MariaDB and there
/// was nothing else to send. `compose_begin_sql` is dialect-aware now, so they are real parameters —
/// the same signature `tx_it.rs::begin` has always had.
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
    assert_eq!(t.header.flags & flags::END, flags::END, "BEGIN → one END");
    assert_eq!(t.header.service, service::TX);
    assert_eq!(t.header.method, method_tx::BEGIN);
    match Outcome::decode(&t.payload).expect("decode BEGIN Outcome") {
        Outcome::Ok(body) => {
            BeginResponse::decode(&body)
                .expect("decode BeginResponse")
                .tx_id
        }
        other => panic!("BEGIN expected Outcome::Ok(BeginResponse), got {other:?}"),
    }
}

/// A tx-scoped `ExecRequest`, with the fetch mode and readonly flag the caller needs.
fn tx_req(tx_id: u64, sql: &str, readonly: bool, fetch: u8) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params: Vec::new(),
        timeout_ms: None,
        readonly,
        fetch,
        tx_id: Some(tx_id),
    }
}

/// A tx-scoped READ (`readonly = true`, `fetch:rows`) — the shape the pre-M1-S8a `tx_req` had.
fn tx_read_req(tx_id: u64, sql: &str) -> ExecRequest {
    tx_req(tx_id, sql, true, FETCH_ROWS)
}

/// A `service=TX` control frame (`COMMIT`/`ROLLBACK`) carrying a `TxControl{tx_id}`. Asserts the
/// one-END shape + TX/method echoes and returns the decoded `Outcome`.
async fn tx_control(client: &mut TestClient, rid: u32, tx_id: u64, method: u16) -> Outcome {
    client
        .send_request(rid, service::TX, method, TxControl { tx_id }.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "tx-control → one END"
    );
    assert_eq!(t.header.service, service::TX);
    assert_eq!(t.header.method, method);
    Outcome::decode(&t.payload).expect("decode tx-control Outcome")
}

async fn commit(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    tx_control(client, rid, tx_id, method_tx::COMMIT).await
}

async fn rollback(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    tx_control(client, rid, tx_id, method_tx::ROLLBACK).await
}

/// The `I64` in the first cell of the first row.
fn first_i64(ok: &ferro_proto::messages::sql::ExecOk) -> i64 {
    match ok.rows.first().and_then(|r| r.first()) {
        Some(Value::I64(v)) => *v,
        other => panic!("expected an I64 scalar in row 0 col 0, got {other:?}"),
    }
}

/// The scalar in the first cell of the first row, whatever its tag — for the isolation reads below,
/// which must compare two engine-rendered strings without assuming either literal.
fn first_scalar(ok: &ferro_proto::messages::sql::ExecOk) -> Value {
    ok.rows
        .first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or_else(|| panic!("expected one scalar row, got {:?}", ok.rows))
}

/// The SESSION-scoped isolation level of whatever connection serves this request.
///
/// Deliberately `@@SESSION.` and not the bare `@@transaction_isolation`: this must read the level
/// that OUTLIVES the transaction (the one a pooled connection would hand to the next tenant), never
/// the next-transaction-only modifier `SET TRANSACTION …` installs. See
/// `mysql_begin_honours_isolation_and_readonly`'s in-tx guard.
const SESSION_ISOLATION_SQL: &str = "SELECT @@SESSION.transaction_isolation";

// -------------------------------------------------------------------------------------------------
// (1) Buffered `SELECT 1` (fetch:rows) round-trips e2e through a kind=mysql pool.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mysql_buffered_select_roundtrips() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return; // offline: both URLs unset (each printed its own skip line)
    }
    for (label, url) in targets {
        let server = common::exec_server(url); // kind inferred from the mysql:// scheme
        let mut client = server.connect().await;
        client.hello(1).await;

        // A bare buffered SELECT: `1` is a LONGLONG literal → Value::I64(1) via the MySQL rowmap.
        let ok = exec_ok(&mut client, 10, &req("SELECT 1")).await;
        assert_eq!(ok.rows.len(), 1, "[{label}] one row");
        assert_eq!(first_i64(&ok), 1, "[{label}] SELECT 1 → I64(1)");

        // A second buffered SELECT on the same session proves the conn recycled cleanly.
        let ok2 = exec_ok(&mut client, 11, &req("SELECT 42")).await;
        assert_eq!(first_i64(&ok2), 42, "[{label}] SELECT 42 → I64(42)");
    }
}

// -------------------------------------------------------------------------------------------------
// (2) A BEGIN .. (in-tx SELECT) .. COMMIT transaction runs through the tx path (Checkout<MysqlBackend>).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mysql_tx_begin_commit_roundtrips() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        // BEGIN → a real tx_id (the actor now owns a pinned Checkout<MysqlBackend>).
        let tx_id = begin(&mut client, 20, "default", None, false).await;

        // An in-tx buffered SELECT rides SQL/EXEC with tx_id set → forwarded to the owning actor.
        match exec(&mut client, 21, &tx_read_req(tx_id, "SELECT 7")).await {
            Outcome::Ok(body) => {
                let ok = ferro_proto::messages::sql::ExecOk::decode(&body).expect("decode ExecOk");
                assert_eq!(first_i64(&ok), 7, "[{label}] in-tx SELECT 7 → I64(7)");
            }
            other => panic!("[{label}] in-tx SELECT expected Ok, got {other:?}"),
        }

        // COMMIT closes the tx cleanly (one END, Ok).
        match commit(&mut client, 22, tx_id).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT expected Ok, got {other:?}"),
        }

        // The session outlives the committed tx: a fresh autocommit SELECT still works.
        let ok = exec_ok(&mut client, 23, &req("SELECT 1")).await;
        assert_eq!(
            first_i64(&ok),
            1,
            "[{label}] post-commit autocommit SELECT works"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// (3) A fetch:stream EXEC to a MySQL pool now STREAMS (dev-loop B2b-2b, SPEC §22.2 (n) closed).
//     These two scenarios REPLACE the refusal tests this file carried while streaming was deferred.
// -------------------------------------------------------------------------------------------------

/// One classified streamed frame. Mirrors `stream_it.rs`'s classifier (PG) so the MySQL family is
/// asserted at the same wire level: exactly one HEAD, then DATA, then exactly one END terminal.
enum SFrame {
    Head,
    Data(Vec<Vec<Value>>),
    End(Outcome),
}

fn classify(frame: &InFrame, rid: u32) -> SFrame {
    assert_eq!(
        frame.header.request_id, rid,
        "every frame echoes the request id"
    );
    if frame.header.flags & flags::END == flags::END {
        assert_eq!(
            frame.header.service,
            service::SQL,
            "the terminal rides the SQL/EXEC request, not the STREAM service"
        );
        return SFrame::End(Outcome::decode(&frame.payload).expect("decode terminal Outcome"));
    }
    assert_eq!(
        frame.header.service,
        service::STREAM,
        "a streamed frame is on the STREAM service"
    );
    match frame.header.method {
        method_stream::HEAD => {
            assert_eq!(
                frame.header.flags & flags::STREAM,
                0,
                "HEAD carries no STREAM flag"
            );
            SFrame::Head
        }
        method_stream::DATA => {
            assert_eq!(
                frame.header.flags & flags::STREAM,
                flags::STREAM,
                "a DATA frame carries the STREAM flag"
            );
            SFrame::Data(
                StreamData::decode(&frame.payload)
                    .expect("decode StreamData")
                    .rows,
            )
        }
        other => panic!("unexpected STREAM method {other}"),
    }
}

/// Drain a `fetch:stream` request to its terminal, replenishing credit after every streamed frame,
/// and return the single-int-column rows in arrival ORDER plus the terminal.
async fn drain_stream(client: &mut TestClient, rid: u32, r: &ExecRequest) -> (Vec<i64>, Outcome) {
    client
        .send_request(rid, service::SQL, method_sql::EXEC, r.encode())
        .await;
    let mut rows: Vec<i64> = Vec::new();
    let mut saw_head = false;
    loop {
        let frame = client.recv().await;
        let plen = frame.header.payload_len;
        match classify(&frame, rid) {
            SFrame::Head => {
                assert!(!saw_head, "exactly one HEAD frame per stream");
                saw_head = true;
                client.window_update(rid, 1, plen).await;
            }
            SFrame::Data(batch) => {
                assert!(saw_head, "HEAD must precede any DATA frame");
                for row in batch {
                    assert_eq!(row.len(), 1, "the fixture selects one column per row");
                    match &row[0] {
                        Value::I64(n) => rows.push(*n),
                        other => panic!("expected an I64 cell, got {other:?}"),
                    }
                }
                client.window_update(rid, 1, plen).await;
            }
            SFrame::End(outcome) => {
                assert!(
                    saw_head,
                    "a stream always emits its HEAD before the terminal"
                );
                return (rows, outcome);
            }
        }
    }
}

/// A multi-row fixture that needs no table: a 5-row UNION whose values arrive in a known order.
const STREAM_SQL: &str = "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 \
                          UNION ALL SELECT 4 UNION ALL SELECT 5";

/// The autocommit arm streams — and, the load-bearing part, the SESSION SURVIVES it.
///
/// Streaming on this backend PARKS the driver connection inside the row stream and puts it back
/// through `reclaim_stream` (B2b-1's seam + B2a's `into_conn`). If that hand-back were broken, the
/// pooled connection would be lost or left mid-protocol: the follow-up buffered SELECT below is what
/// proves it came home usable, not merely that rows arrived.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_autocommit_stream_delivers_rows_and_the_session_survives() {
    for (label, url) in mysql_targets() {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        let mut r = req(STREAM_SQL);
        r.fetch = FETCH_STREAM;
        let (rows, outcome) = drain_stream(&mut client, 30, &r).await;

        assert_eq!(rows, vec![1, 2, 3, 4, 5], "[{label}] every row, in order");
        assert!(
            matches!(outcome, Outcome::Ok(_)),
            "[{label}] a fully-drained stream ends in exactly one Ok terminal, got {outcome:?}"
        );
        assert!(
            client
                .recv_or_none(Duration::from_millis(250))
                .await
                .is_none(),
            "[{label}] nothing may follow the terminal (charter rule 4: exactly one END)"
        );

        // THE reclaim proof: the parked conn was handed back and recycled cleanly.
        let ok = exec_ok(&mut client, 31, &req("SELECT 1")).await;
        assert_eq!(
            first_i64(&ok),
            1,
            "[{label}] the streamed connection came back usable (reclaim + unpark worked)"
        );
    }
}

/// **FB-4 (dev-loop ledger) — the end-to-end abandonment proof for a MySQL OWNING stream.**
///
/// B2b-2b gave MySQL a conn-owning `RowStream` and opened this gap in the same change: every link
/// in the abandonment chain was tested (the parked-conn contract live on both engines in
/// `conn_it.rs`; the pool's discard in `ferro-pool`'s `query_stream.rs`) but nothing drove the
/// WHOLE path on MySQL the way `stream_it.rs::abandonment_recovery_after_cancel` does for
/// PostgreSQL. Criterion (c) — "an abandoned owning stream ⇒ the connection is discarded, never
/// recycled" — was therefore proven by construction plus unit coverage, not end to end.
///
/// It matters more after M2/S3 than when it was filed: S3 moved the empty-prepared-list arm onto
/// the owning route too, so MORE statements now park their connection than when FB-4 was written.
///
/// The chain under test: abandon mid-flight → the moved-out `Conn` is dropped with the stream →
/// the `MysqlConn` wrapper is left PARKED → parked reads `is_closed`-dead → the pool DISCARDS the
/// husk instead of recycling a connection whose session is gone. What proves it from outside is
/// the LAST step: a fresh request on the same session must get its own clean reply. If a husk were
/// recycled, that request would land on a dead or mid-protocol connection.
///
/// **Withholding credit after the CANCEL is what makes this self-proving** (the PG original's
/// reasoning, and it holds here): if the CANCEL were silently ignored, the remaining rows could
/// never be sent without further `WINDOW_UPDATE`s, so the drain loop would stall until `recv()`
/// times out — a hard failure, never a false-green `Ok`.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_abandoned_stream_recovers_and_the_session_survives() {
    for (label, url) in mysql_targets() {
        tokio::time::timeout(Duration::from_secs(60), async move {
            // A 2-frame credit window, so a multi-frame result must park and resume repeatedly and
            // the cancel lands genuinely mid-stream rather than after a single batch.
            let server = common::stream_server(url, 2);
            let mut client = server.connect().await;
            client.hello(1).await;

            let rid = 60;
            let mut r = req(ABANDON_SQL);
            r.fetch = FETCH_STREAM;
            client
                .send_request(rid, service::SQL, method_sql::EXEC, r.encode())
                .await;

            let f_head = client.recv().await;
            let hplen = f_head.header.payload_len;
            assert!(
                matches!(classify(&f_head, rid), SFrame::Head),
                "[{label}] the first frame is HEAD"
            );
            client.window_update(rid, 1, hplen).await;

            // Take a few DATA frames, replenishing, so the producer is genuinely mid-stream.
            let mut rows_seen = 0usize;
            let mut data_before_cancel = 0u32;
            while data_before_cancel < 2 {
                let frame = client.recv().await;
                let plen = frame.header.payload_len;
                match classify(&frame, rid) {
                    SFrame::Data(batch) => {
                        rows_seen += batch.len();
                        data_before_cancel += 1;
                        client.window_update(rid, 1, plen).await;
                    }
                    SFrame::End(_) => {
                        panic!("[{label}] the stream ended before it could be abandoned")
                    }
                    SFrame::Head => panic!("[{label}] a second HEAD is a protocol violation"),
                }
            }

            // ABANDON: cancel, then drain to the ONE terminal granting NO further credit.
            client.cancel(rid).await;
            let terminal = loop {
                let frame = client.recv().await;
                match classify(&frame, rid) {
                    SFrame::Data(batch) => rows_seen += batch.len(),
                    SFrame::Head => panic!("[{label}] a second HEAD after cancel"),
                    SFrame::End(o) => break o,
                }
            };

            // TRUNCATION: the cancel cut it far short of the full result.
            assert!(
                rows_seen < ABANDON_ROWS / 5,
                "[{label}] CANCEL must truncate the stream far short of {ABANDON_ROWS} rows when no \
                 further credit is granted; got {rows_seen} — the CANCEL is not gating the producer"
            );

            // Exactly one terminal, and a cancelled READ is never Indeterminate. `Ok` stays legal
            // for the benign race where the drain completed just before the cancel routed — the
            // truncation assertion above already rules out a full drain reaching that arm.
            match &terminal {
                Outcome::Error(ep) => assert_eq!(
                    ep.code,
                    errc::CANCELLED,
                    "[{label}] a cancelled streamed read reports Cancelled (57014)"
                ),
                Outcome::Ok(_) | Outcome::Cancelled => {}
            }
            assert!(
                client
                    .recv_or_none(Duration::from_millis(250))
                    .await
                    .is_none(),
                "[{label}] nothing may follow the terminal (charter rule 4: exactly one END)"
            );

            // THE FB-4 ASSERTION. A fresh request on the SAME session gets its own clean reply:
            // the wire re-framed, and the pool did NOT hand back the abandoned connection's husk.
            let ok = exec_ok(&mut client, 61, &req("SELECT 42")).await;
            assert_eq!(
                first_i64(&ok),
                42,
                "[{label}] the post-abandonment request gets its own reply — no wire desync, and \
                 the discarded husk was replaced rather than recycled"
            );
            common::assert_session_alive(&mut client, 0xFB4).await;
        })
        .await
        .unwrap_or_else(|_| {
            panic!("[{label}] cancel + drain + a fresh request on the same session must not hang")
        });
    }
}

/// The abandonment fixture's full row count: 900 x 30 = 27000, i.e. ~27 DATA frames at the
/// producer's 1024-rows-per-frame default. Large enough that a cancel after two frames is
/// unmistakably a truncation.
const ABANDON_ROWS: usize = 27_000;

/// A multi-frame fixture that needs no table and no session state, and runs UNMODIFIED on both
/// engines.
///
/// Volume comes from a CROSS JOIN rather than from deep recursion, and that is the portable part:
/// MySQL 8 caps a recursive CTE at `cte_max_recursion_depth` (default 1000) while MariaDB uses a
/// different knob entirely (`max_recursive_iterations`) — the constraint `stream_recovery_it.rs`
/// already records, which is why its own fixture stops at 900. Raising either limit would mean a
/// session `SET` (pooled session state, §7.1 — it would taint the connection) or MySQL's `SET_VAR`
/// optimizer hint, which MariaDB does not implement. Two shallow CTEs multiplied together need
/// neither.
const ABANDON_SQL: &str = "WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq \
                           WHERE n < 900), mul AS (SELECT 1 AS m UNION ALL SELECT m + 1 FROM mul \
                           WHERE m < 30) SELECT s.n FROM seq s CROSS JOIN mul";

/// Create a stored procedure over a RAW driver connection, on the TEXT protocol.
///
/// `CREATE PROCEDURE` cannot be PREPARED on MySQL (errno 1295), and every user statement ferrod
/// issues goes through `COM_STMT_PREPARE` — so a procedure fixture cannot be built through the
/// daemon at all. `mysql_async` is already a dev-dependency here for exactly this class of
/// side-channel setup (see `Cargo.toml`) and resolves to the SAME vendored fork the backend uses.
async fn create_procedure(url: &str, name: &str, body: &str) {
    use mysql_async::prelude::Queryable;
    let opts = mysql_async::Opts::from_url(url).expect("a valid mysql:// url");
    let mut c = mysql_async::Conn::new(opts).await.expect("side connection");
    c.query_drop(format!("DROP PROCEDURE IF EXISTS {name}"))
        .await
        .expect("drop any leftover procedure");
    c.query_drop(format!("CREATE PROCEDURE {name}() {body}"))
        .await
        .expect("create the procedure");
    c.disconnect().await.expect("close the side connection");
}

/// Tear down a [`create_procedure`] fixture, over the same raw text-protocol route.
async fn drop_procedure(url: &str, name: &str) {
    use mysql_async::prelude::Queryable;
    let opts = mysql_async::Opts::from_url(url).expect("a valid mysql:// url");
    let mut c = mysql_async::Conn::new(opts).await.expect("side connection");
    c.query_drop(format!("DROP PROCEDURE IF EXISTS {name}"))
        .await
        .expect("drop the procedure");
    c.disconnect().await.expect("close the side connection");
}

/// **M2/S3 — a streamed `CALL` delivers REAL ROWS.** The guard for the one shape whose result
/// columns do not exist until the statement has run.
///
/// Before S3 this path dispatched on the PREPARE-time column list, which a `CALL` leaves empty even
/// when the procedure emits a result set — so a `fetch:stream` `CALL` took the no-result-set arm,
/// ran buffered, and DISCARDED its rows. The module docs recorded that as a known limitation; S3
/// moved the dispatch to the EXECUTED metadata, which is where a `CALL`'s columns actually live.
///
/// MUTATION PROOF: restore the prepare-time dispatch and this goes red on the rows assertion —
/// the stream completes with a clean terminal and ZERO rows, which is exactly the failure mode
/// that made the old behaviour so easy to miss.
///
/// The session assertion is the other half and is not decoration: this arm now PARKS the driver
/// connection (it must, to stream), where before it never did. If the hand-back through
/// `into_conn`/`unpark` were wrong, the rows would still arrive and the pool would be one
/// connection down.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_streamed_call_delivers_rows_and_the_session_survives() {
    for (label, url) in mysql_targets() {
        // The procedure is created over a RAW side connection on the TEXT protocol, NOT through
        // ferrod. That is not a shortcut: MySQL's prepared-statement protocol does not accept
        // `CREATE PROCEDURE` (errno 1295, "not supported in the prepared statement protocol yet"),
        // and every user statement ferrod runs is prepared. The S2 sibling in
        // `ferro-backend-mysql/tests/query_it.rs` meets the same wall and answers it the same way,
        // through `simple_query`. The fixture is not what this test is about — the `CALL` below is,
        // and that still goes through ferrod end to end.
        create_procedure(
            &url,
            "s3_call_rows",
            "BEGIN SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3; END",
        )
        .await;

        let server = common::exec_server(url.clone());
        let mut client = server.connect().await;
        client.hello(1).await;

        let mut r = req("CALL s3_call_rows()");
        r.fetch = FETCH_STREAM;
        let (rows, outcome) = drain_stream(&mut client, 52, &r).await;

        assert_eq!(
            rows,
            vec![1, 2, 3],
            "[{label}] a streamed CALL must deliver the procedure's rows, in order"
        );
        assert!(
            matches!(outcome, Outcome::Ok(_)),
            "[{label}] a fully-drained CALL stream ends in exactly one Ok terminal, got {outcome:?}"
        );
        assert!(
            client
                .recv_or_none(Duration::from_millis(250))
                .await
                .is_none(),
            "[{label}] nothing may follow the terminal (charter rule 4: exactly one END)"
        );

        // The park/reclaim proof for the arm that never parked before S3.
        let ok = exec_ok(&mut client, 53, &req("SELECT 1")).await;
        assert_eq!(
            first_i64(&ok),
            1,
            "[{label}] the connection that streamed a CALL came back usable"
        );

        drop_procedure(&url, "s3_call_rows").await;
    }
}

/// **A streamed INSERT reports its AUTO_INCREMENT key** — the engine-level guard for the B2c
/// regression that CI caught and no offline gate could see.
///
/// `build_stream_terminal_body` has always had a `last_insert_id` slot, but the producer hardcoded
/// `None` into it — correct while PostgreSQL, which has no such protocol field, was the only
/// streaming backend, and silently wrong the moment MySQL/MariaDB started streaming. The visible
/// consequence was that `Doctrine\DBAL\Connection::lastInsertId()` threw `NoIdentityValue` on every
/// streamed INSERT, which is what Doctrine ORM's `IdentityGenerator` calls on every insert.
///
/// Asserted on the TERMINAL rather than through the driver, because that is where the value either
/// exists or does not: an INSERT takes the no-result-set path (`MysqlRowStream::NoRows`), so this
/// also pins that the key survives the arm that never parks the connection.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_streamed_insert_reports_its_generated_key() {
    for (label, url) in mysql_targets() {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;

        exec_ok(&mut client, 40, &ddl("DROP TABLE IF EXISTS b2c_lid")).await;
        exec_ok(
            &mut client,
            41,
            &ddl("CREATE TABLE b2c_lid (id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)"),
        )
        .await;

        // The INSERT rides `fetch:stream` — the shape the DBAL driver now sends for every
        // parameterised statement.
        let mut r = ddl("INSERT INTO b2c_lid (n) VALUES (7)");
        r.fetch = FETCH_STREAM;
        let (rows, outcome) = drain_stream(&mut client, 42, &r).await;
        assert!(rows.is_empty(), "[{label}] an INSERT streams no rows");

        let Outcome::Ok(body) = outcome else {
            panic!("[{label}] a streamed INSERT must end Ok, got {outcome:?}");
        };
        let ok = ferro_proto::messages::sql::ExecOk::decode(&body).expect("decode terminal ExecOk");
        assert_eq!(ok.affected, 1, "[{label}] one row inserted");
        assert!(
            matches!(ok.last_insert_id, Some(Value::U64(n)) if n > 0)
                || matches!(ok.last_insert_id, Some(Value::I64(n)) if n > 0),
            "[{label}] the streamed terminal MUST carry the AUTO_INCREMENT key — this is what \
             lastInsertId() reads; got {:?}",
            ok.last_insert_id
        );

        exec_ok(&mut client, 43, &ddl("DROP TABLE b2c_lid")).await;
    }
}

/// The tx-scoped arm streams off the SAME `supports_row_streaming()` authority, on the PINNED
/// connection — and the transaction is still intact afterwards.
///
/// This is the arm that used to refuse late and force-taint the pinned conn. Now it must stream and
/// give the connection back to the SAME transaction: the in-tx statement and the COMMIT below are
/// what prove the pin survived a park/reclaim round trip.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_tx_scoped_stream_delivers_rows_and_the_tx_survives() {
    for (label, url) in mysql_targets() {
        let server = common::exec_server(url);
        let mut client = server.connect().await;
        client.hello(0).await;

        let tx_id = begin(&mut client, 2, "default", None, false).await;
        let mut r = tx_read_req(tx_id, STREAM_SQL);
        r.fetch = FETCH_STREAM;
        let (rows, outcome) = drain_stream(&mut client, 3, &r).await;

        assert_eq!(
            rows,
            vec![1, 2, 3, 4, 5],
            "[{label}] in-tx stream delivers every row in order"
        );
        assert!(
            matches!(outcome, Outcome::Ok(_)),
            "[{label}] one Ok terminal for the tx-scoped stream, got {outcome:?}"
        );
        assert!(
            client
                .recv_or_none(Duration::from_millis(250))
                .await
                .is_none(),
            "[{label}] exactly one END on the tx-scoped stream too"
        );

        // The pinned conn was reclaimed into the SAME transaction, not lost or tainted away.
        let ok = exec_ok(&mut client, 4, &tx_read_req(tx_id, "SELECT 7")).await;
        assert_eq!(
            first_i64(&ok),
            7,
            "[{label}] the pinned tx connection is still usable after a streamed statement"
        );
        match commit(&mut client, 5, tx_id).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT after a tx-scoped stream: {other:?}"),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// (6) M1-S8a Task 7 — savepoint SQL passthrough on the MySQL family (SPEC §22.2 (r)).
// -------------------------------------------------------------------------------------------------

/// A statement that returns no rows (DDL / INSERT / a savepoint op): `readonly = false`,
/// `fetch = FETCH_NONE`.
fn ddl(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.fetch = FETCH_NONE;
    r
}

/// A tx-scoped WRITE request (`readonly = false`, `fetch = FETCH_NONE`) — `tx_read_req` above is the
/// read-only/rows-fetching form the earlier scenarios use.
fn tx_write_req(tx_id: u64, sql: &str) -> ExecRequest {
    tx_req(tx_id, sql, false, FETCH_NONE)
}

/// Doctrine's nested-transaction emulation, verbatim, on BOTH MySQL-family engines. The read-back
/// is what proves the savepoint took: an accepted statement that did nothing would pass a "no
/// error" assertion.
///
/// This is also the gate on the ROUTING half of the fix. MySQL 8 cannot run a savepoint verb on the
/// prepared-statement path at all (measured on 8.4.11: `COM_STMT_PREPARE` of `SAVEPOINT` /
/// `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` → errno 1295), while MariaDB 11.8 can — so an
/// implementation that only relaxed the guard and left the passthrough on the prepared path is
/// GREEN on MariaDB and RED here on MySQL.
#[tokio::test(flavor = "multi_thread")]
async fn savepoint_sql_passes_through_inside_a_transaction() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return; // offline: both URLs unset (each printed its own skip line)
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
        exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_sp (v INT)")).await;

        let tx = begin(&mut c, 3, "default", None, false).await;
        for (rid, stmt) in [
            (4, "INSERT INTO s8a_sp (v) VALUES (1)"),
            (5, "SAVEPOINT DOCTRINE_1"),
            (6, "INSERT INTO s8a_sp (v) VALUES (2)"),
            (7, "ROLLBACK TO SAVEPOINT DOCTRINE_1"),
            (8, "INSERT INTO s8a_sp (v) VALUES (3)"),
            (9, "RELEASE SAVEPOINT DOCTRINE_1"),
        ] {
            match exec(&mut c, rid, &tx_write_req(tx, stmt)).await {
                Outcome::Ok(_) => {}
                other => {
                    panic!("[{label}] {stmt:?} must pass through inside a transaction: {other:?}")
                }
            }
        }
        match commit(&mut c, 10, tx).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT: {other:?}"),
        }

        let rows = exec_ok(&mut c, 11, &req("SELECT v FROM s8a_sp ORDER BY v")).await;
        let got: Vec<i64> = rows
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("[{label}] unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            got,
            vec![1, 3],
            "[{label}] the savepoint must have rolled 2 back and kept 1 and 3"
        );

        exec_ok(&mut c, 12, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
        common::assert_session_alive(&mut c, 0xC0FFEE).await;
    }
}

/// A transaction-BOUNDARY verb stays refused INSIDE a transaction on both engines — including one
/// disguised by case/comments, and one riding behind a savepoint in a COMPOUND statement (which the
/// text protocol would otherwise happily execute, `CLIENT_MULTI_STATEMENTS` being negotiated).
#[tokio::test(flavor = "multi_thread")]
async fn boundary_sql_stays_refused_inside_a_transaction() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_boundary")).await;
        exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_boundary (v INT)")).await;

        let tx = begin(&mut c, 3, "default", None, false).await;
        match exec(
            &mut c,
            4,
            &tx_write_req(tx, "INSERT INTO s8a_boundary (v) VALUES (1)"),
        )
        .await
        {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] seed insert: {other:?}"),
        }

        let mut rid = 5;
        for boundary in [
            "COMMIT",
            "commit;",
            "  RollBack  ",
            "BEGIN",
            "START TRANSACTION",
            "END",
            "/* nested */ COMMIT",
            "-- nested\nROLLBACK",
            "SAVEPOINT X1; COMMIT",
        ] {
            match exec(&mut c, rid, &tx_write_req(tx, boundary)).await {
                Outcome::Error(ep) => assert_eq!(
                    ep.code,
                    errc::UNSUPPORTED,
                    "[{label}] {boundary:?} must be UNSUPPORTED, got {ep:?}"
                ),
                other => {
                    panic!("[{label}] {boundary:?} must be refused inside a transaction: {other:?}")
                }
            }
            rid += 1;
        }

        // The transaction survived every refusal, so a savepoint still works and COMMIT commits.
        match exec(&mut c, rid, &tx_write_req(tx, "SAVEPOINT AFTER_REFUSALS")).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] the tx must still be usable after the refusals: {other:?}"),
        }
        rid += 1;
        match commit(&mut c, rid, tx).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT after the refusals: {other:?}"),
        }
        rid += 1;

        let rows = exec_ok(&mut c, rid, &req("SELECT v FROM s8a_boundary ORDER BY v")).await;
        let got: Vec<i64> = rows
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("[{label}] unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            got,
            vec![1],
            "[{label}] the refused boundary verbs neither committed nor rolled back the tx"
        );
        rid += 1;

        exec_ok(&mut c, rid, &ddl("DROP TABLE IF EXISTS s8a_boundary")).await;
        common::assert_session_alive(&mut c, 0xC0FFEF).await;
    }
}

/// Outside a transaction all three savepoint verbs stay refused — deliberately, because MySQL
/// SILENTLY IGNORES a bare `SAVEPOINT` under autocommit (no transaction is started, the savepoint
/// has no effect), so delegating would hand a driver a rollback point that does not exist.
#[tokio::test(flavor = "multi_thread")]
async fn savepoint_sql_outside_a_transaction_is_refused() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return;
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        let mut rid = 1;
        for sp in [
            "SAVEPOINT DOCTRINE_1",
            "ROLLBACK TO SAVEPOINT DOCTRINE_1",
            "RELEASE SAVEPOINT DOCTRINE_1",
        ] {
            let e = exec_err(&mut c, rid, &ddl(sp)).await;
            assert_eq!(e.code, errc::UNSUPPORTED, "[{label}] {sp:?} -> {e:?}");
            assert!(
                e.message.contains("outside a transaction"),
                "[{label}] {sp:?} -> {}",
                e.message
            );
            rid += 1;
        }

        let e2 = exec_err(&mut c, rid, &ddl("COMMIT")).await;
        assert_eq!(e2.code, errc::UNSUPPORTED);
        assert!(
            e2.message.contains("use the TX service"),
            "[{label}] {}",
            e2.message
        );

        common::assert_session_alive(&mut c, 0xC0FFEE).await;
    }
}

// -------------------------------------------------------------------------------------------------
// (7) M1-S8a Task 8 — dialect-aware isolation/readonly BEGIN (SPEC §22.2 (s)).
// -------------------------------------------------------------------------------------------------

/// Before M1-S8a, `BEGIN ISOLATION LEVEL …` / `BEGIN READ ONLY` were ERROR 1064 on both engines, so
/// EVERY isolation/readonly BEGIN failed. Nothing pinned that (every MySQL tx test used
/// `isolation: None`), so this is a pure addition.
///
/// READ ONLY is asserted directly (SQLSTATE 25006 on a write). ISOLATION cannot be: a
/// next-transaction-only `SET TRANSACTION` is deliberately NOT reflected in `@@transaction_isolation`
/// — and the SESSION form that WOULD be reflected is forbidden here, because it persists onto the
/// pooled connection for the next tenant (charter rule 6). So isolation is proven by a LOCK
/// CONFLICT, with an `isolation: None` control run that must NOT conflict.
///
/// The contending `UPDATE` is bounded by the request's own `timeout_ms` (the S4 per-request CANCEL
/// path), so a blocked write terminates as a `57014` cancel instead of hanging the suite on InnoDB's
/// 50-second `innodb_lock_wait_timeout`.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_begin_honours_isolation_and_readonly() {
    let targets = mysql_targets();
    if targets.is_empty() {
        return; // offline: both URLs unset (each printed its own skip line)
    }
    for (label, url) in targets {
        let server = common::exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        // The SESSION default, read off the pool BEFORE any isolation-scoped transaction exists.
        // Read, never hard-coded: MySQL 8 and MariaDB 11 both render it `REPEATABLE-READ` today, but
        // the guard below is a genuine before/after comparison, not a literal check.
        let base = first_scalar(&exec_ok(&mut c, 20, &req(SESSION_ISOLATION_SQL)).await);

        // ---- (a) READ ONLY is enforced.
        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_ro")).await;
        exec_ok(
            &mut c,
            2,
            &ddl("CREATE TABLE s8a_ro (id INT PRIMARY KEY, v INT)"),
        )
        .await;
        exec_ok(&mut c, 3, &ddl("INSERT INTO s8a_ro VALUES (1, 1)")).await;

        let tx = begin(
            &mut c,
            4,
            "default",
            Some(u8::from(Isolation::Serializable)),
            true,
        )
        .await;
        let e = match exec(
            &mut c,
            5,
            &tx_write_req(tx, "INSERT INTO s8a_ro VALUES (2, 2)"),
        )
        .await
        {
            Outcome::Error(ep) => ep,
            other => panic!("[{label}] a write in a READ ONLY tx must be refused, got {other:?}"),
        };
        assert_eq!(
            e.sqlstate.as_deref(),
            Some("25006"),
            "[{label}] READ ONLY must be enforced (errno 1792 / SQLSTATE 25006), got {e:?}"
        );
        match rollback(&mut c, 6, tx).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] ROLLBACK of the read-only tx: {other:?}"),
        }

        // ---- (b) SERIALIZABLE is enforced: a read inside the tx LOCKS the row.
        let tx = begin(
            &mut c,
            7,
            "default",
            Some(u8::from(Isolation::Serializable)),
            false,
        )
        .await;

        // ---- THE CROSS-TENANT LEAK GUARD, read INSIDE the pinned transaction (Task 8/9 review, F1).
        // No hygiene can run here — the connection is pinned to this `tx_id` until COMMIT/ROLLBACK —
        // so this is the one place the engine's own SQL is observable end to end through the daemon.
        // The composed BEGIN must not have moved the SESSION-scoped level: a `SET SESSION` /
        // `SET @@SESSION.…` spelling would show up as SERIALIZABLE here and outlive the transaction.
        let in_tx =
            first_scalar(&exec_ok(&mut c, 21, &tx_read_req(tx, SESSION_ISOLATION_SQL)).await);
        assert_eq!(
            in_tx, base,
            "[{label}] the composed BEGIN must leave the SESSION-scoped isolation alone — a level \
             set at SESSION scope survives COMMIT on the pooled connection and is inherited by the \
             next tenant (charter rule 6)"
        );

        match exec(
            &mut c,
            8,
            &tx_read_req(tx, "SELECT v FROM s8a_ro WHERE id = 1"),
        )
        .await
        {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] the in-tx read must succeed: {other:?}"),
        }

        // A SECOND session, so the UPDATE genuinely contends rather than sharing the pinned conn.
        let mut other = server.connect().await;
        other.hello(0).await;
        let mut upd = req("UPDATE s8a_ro SET v = 99 WHERE id = 1");
        upd.readonly = false;
        upd.fetch = FETCH_NONE;
        upd.timeout_ms = Some(1_500); // bounded: a blocked write ends at the deadline, never hangs
        let blocked = exec_err(&mut other, 9, &upd).await;
        // NOTE — the terminal is NOT a raw `57014`, and it cannot be. The contending statement is an
        // AUTOCOMMIT WRITE, and the S4 §19.3 fate matrix deliberately re-labels a cancelled/timed-out
        // dispatched write as `WriteUnconfirmed{Indeterminate}` (`fate.rs`'s `is_57014` override
        // arm), replacing the raw SQLSTATE with the engine's own payload — so `sqlstate` is `None`
        // here by construction. Asserting the §19.3 cell is the truthful form of "the deadline
        // cancelled it", and the message check separates it from the OTHER producer of
        // `WriteUnconfirmed` (a `ConnectionLost` on a sent write).
        assert_eq!(
            blocked.code,
            errc::WRITE_UNCONFIRMED,
            "[{label}] under SERIALIZABLE the in-tx read must LOCK the row, so a concurrent UPDATE \
             blocks until the request deadline cancels it (§19.3 Indeterminate) — got {blocked:?}"
        );
        assert_eq!(
            blocked.branch,
            branch::INDETERMINATE,
            "[{label}] a cancelled autocommit write is the §19.3 Indeterminate branch: {blocked:?}"
        );
        assert!(
            blocked.message.contains("cancelled or timed out"),
            "[{label}] the block must end at the DEADLINE, not at a lost connection: {blocked:?}"
        );
        match rollback(&mut c, 10, tx).await {
            Outcome::Ok(_) => {}
            other_out => panic!("[{label}] ROLLBACK of the serializable tx: {other_out:?}"),
        }

        // ---- (c) THE CONTROL. Same scenario with isolation: None (REPEATABLE READ) — the read is a
        // non-locking consistent read, so the UPDATE goes straight through. Without this run, (b)
        // would pass for any reason the UPDATE happened to be slow.
        let tx = begin(&mut c, 11, "default", None, false).await;
        match exec(
            &mut c,
            12,
            &tx_read_req(tx, "SELECT v FROM s8a_ro WHERE id = 1"),
        )
        .await
        {
            Outcome::Ok(_) => {}
            other_out => panic!("[{label}] the in-tx read must succeed: {other_out:?}"),
        }
        let mut upd2 = req("UPDATE s8a_ro SET v = 42 WHERE id = 1");
        upd2.readonly = false;
        upd2.fetch = FETCH_NONE;
        upd2.timeout_ms = Some(1_500);
        match exec(&mut other, 13, &upd2).await {
            Outcome::Ok(_) => {}
            other_out => panic!(
                "[{label}] under the DEFAULT isolation the concurrent UPDATE must NOT block — if \
                 this fails, (b) proves nothing: {other_out:?}"
            ),
        }
        match rollback(&mut c, 14, tx).await {
            Outcome::Ok(_) => {}
            other_out => panic!("[{label}] ROLLBACK of the control tx: {other_out:?}"),
        }

        // ---- (d) WHY THE LEAK GUARD IS THE *IN-TX* READ ABOVE AND NOT A NEXT-TENANT READ HERE.
        // A "the next tenant did not inherit SERIALIZABLE" assertion at THIS point is a guard that
        // cannot fail, and it was measured as such: with the composer mutated to emit the forbidden
        // SESSION-scoped form, a next-tenant read stayed GREEN — `MysqlBackend::clean_reset_profile()`
        // is `Some(Full)`, so EVERY MySQL recycle runs `COM_RESET_CONNECTION` and wipes the leaked
        // level before the next tenant can observe it. (The PG mirror in `tx_it.rs` was measured the
        // same way and is masked by the targeted profile's `RESET ALL`.) Hygiene masking the leak is
        // defence in depth, NOT the property being asserted — and it is one of the concrete holes
        // any future tracker-clean hygiene skip (§7.2, R2) must close.
        //
        // The falsifiable pool-level guard is therefore the read INSIDE the pinned transaction (step
        // (b)), where hygiene provably cannot have run yet: mutated, it reads `SERIALIZABLE` against
        // a `REPEATABLE-READ` base and goes RED on both engines. A second, independent guard lives
        // one layer further down on a RAW backend connection —
        // `ferro-backend-mysql`'s `begin_dialect_it::the_batched_isolation_never_survives_the_transaction`
        // — which additionally proves the level is gone AFTER the transaction ends.

        exec_ok(&mut c, 15, &ddl("DROP TABLE IF EXISTS s8a_ro")).await;
        common::assert_session_alive(&mut c, 0xC0FFF0).await;
    }
}
