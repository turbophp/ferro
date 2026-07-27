//! S5 Task 3 — the live EXEC path end to end: a real client → `ferrod` session → `ferro-pool`
//! checkout → guarded `Checkout::query` → live Dockerized Postgres → buffered single terminal `END`
//! → client. THE MILESTONE (`exec_select1_shape`).
//!
//! Every DB-touching test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — same discipline
//! as `ferro-backend-pg`'s `pg_query_it.rs`, so `cargo test --workspace` stays green offline.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferrod --test sql_exec_it -- --nocapture
//! ```
//!
//! The session mechanics (registry / supervisor / exactly-one-END) are the SAME S3 path
//! `session_rules.rs` proves; here we extend that coverage to the EXEC handler: every test asserts
//! the terminal is a single `flags::END` frame and then that the session is still alive (PING→PONG),
//! i.e. exactly one END was produced and nothing else.

mod common;

use common::{TestClient, TestServer};
use ferro_proto::consts::{branch, errc, flags, method_core, method_sql, service, tag};
use ferro_proto::messages::sql::{ExecOk, ExecRequest};
use ferro_proto::messages::{ErrorPayload, Outcome};
use ferro_proto::value::Value;
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;

/// The DSN under test, or `None` (→ the test returns early / skips) when unset.
fn pg_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

/// A live `ferrod` session server whose EXEC handler owns a real `Pool<PgBackend>` named "default"
/// pointing at `url`. Uses `TestServer::spawn_with_handler` (no peercred gate) with the real
/// `sql::make_handler`, so this is a genuine client→ferrod→pool→PG round trip.
fn exec_server(url: String) -> TestServer {
    let config = Config {
        pools: vec![PoolSpec {
            name: "default".to_string(),
            dsn: url,
        }],
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let handler = sql::make_handler(registry);
    TestServer::spawn_with_handler(BootEpoch(1), handler)
}

/// A base read-only `EXEC "sql"` against the "default" pool, fetch=rows, no params.
fn req(sql: &str) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params: Vec::new(),
        timeout_ms: None,
        readonly: true,
        fetch: 0,
    }
}

/// Send an EXEC and read back its single terminal, asserting the one-END frame shape (flags::END,
/// service SQL, method EXEC, echoed request_id). Returns the decoded `Outcome`.
async fn exec(client: &mut TestClient, rid: u32, req: &ExecRequest) -> Outcome {
    client
        .send_request(rid, service::SQL, method_sql::EXEC, req.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "terminal echoes the request id");
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "the EXEC terminal carries flags::END (exactly one END)"
    );
    assert_eq!(t.header.service, service::SQL);
    assert_eq!(t.header.method, method_sql::EXEC);
    Outcome::decode(&t.payload).expect("decode terminal Outcome")
}

/// Unwrap an EXEC terminal expected to be `Outcome::Ok(ExecOk)`.
async fn exec_ok(client: &mut TestClient, rid: u32, req: &ExecRequest) -> ExecOk {
    match exec(client, rid, req).await {
        Outcome::Ok(body) => ExecOk::decode(&body).expect("decode ExecOk"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}

/// Unwrap an EXEC terminal expected to be `Outcome::Error(ErrorPayload)`.
async fn exec_err(client: &mut TestClient, rid: u32, req: &ExecRequest) -> ErrorPayload {
    match exec(client, rid, req).await {
        Outcome::Error(ep) => ep,
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
}

/// Prove the session is still alive after a terminal (⇒ exactly one END was produced): PING→PONG.
async fn assert_session_alive(client: &mut TestClient, token: u64) {
    client.ping(9, token).await;
    let pong = client.recv().await;
    assert_eq!(pong.header.service, service::CORE);
    assert_eq!(pong.header.method, method_core::PONG);
    assert_eq!(pong.header.request_id, 9);
}

// -------------------------------------------------------------------------------------------------
// THE MILESTONE: a live SELECT 1 client→ferrod→pool→PG→client.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn exec_select1_shape() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let request = req("SELECT 1");
    let request_bytes = request.encode();
    eprintln!(
        "MILESTONE exec_select1_shape >>> EXEC request frame payload ({} bytes): {}",
        request_bytes.len(),
        hex(&request_bytes)
    );

    client
        .send_request(10, service::SQL, method_sql::EXEC, request_bytes)
        .await;
    let terminal = client.recv().await;
    eprintln!(
        "MILESTONE exec_select1_shape <<< terminal END frame: service={} method={} rid={} flags={:#x} payload({} bytes)={}",
        terminal.header.service,
        terminal.header.method,
        terminal.header.request_id,
        terminal.header.flags,
        terminal.payload.len(),
        hex(&terminal.payload),
    );

    assert_eq!(terminal.header.request_id, 10);
    assert_eq!(terminal.header.flags & flags::END, flags::END);
    assert_eq!(terminal.header.service, service::SQL);
    assert_eq!(terminal.header.method, method_sql::EXEC);

    let ok = match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Ok(body) => ExecOk::decode(&body).expect("decode ExecOk"),
        other => panic!("expected Outcome::Ok(SELECT 1), got {other:?}"),
    };
    eprintln!("MILESTONE exec_select1_shape === decoded ExecOk: {ok:?}");

    // cols = [{name, I64}], rows = [[I64(1)]] — the OID-strict int4→I64 widening, end to end.
    assert_eq!(ok.cols.len(), 1, "SELECT 1 has one column");
    assert_eq!(ok.cols[0].tag, tag::I64, "int4 column maps to the I64 tag");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]], "the row is [I64(1)]");
    // queue_us + exec_us populated: a real fresh-connect checkout and a real DB round trip.
    assert!(ok.stats.exec_us > 0, "exec_us must reflect a real DB query");
    assert!(
        ok.stats.queue_us > 0,
        "queue_us must reflect the (fresh-connect) checkout wait"
    );
    assert_eq!(ok.stats.rows, 1);
    assert!(ok.stats.bytes > 0);

    // Exactly ONE END: the session is alive and never produced a second frame for id 10.
    assert_session_alive(&mut client, 42).await;
}

// -------------------------------------------------------------------------------------------------
// fetch:none → affected, empty rows.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn exec_fetch_none_affected() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // A persistent (non-temp) table: temp tables are per-connection, but the pool may hand a
    // different pooled conn to each EXEC, so the CREATE and the INSERT need a table visible to any
    // connection. `IF NOT EXISTS` keeps it idempotent across a run. bigint column ⇒ the canonical
    // I64→int8 bind matches the inferred param type.
    let mut ddl = req("CREATE TABLE IF NOT EXISTS ferro_s5_none (id bigint)");
    ddl.readonly = false;
    ddl.fetch = 1;
    let _ = exec_ok(&mut client, 20, &ddl).await;

    let mut insert = req("INSERT INTO ferro_s5_none (id) VALUES (?), (?)");
    insert.readonly = false;
    insert.fetch = 1; // none
    insert.params = vec![Value::I64(1), Value::I64(2)];
    let ok = exec_ok(&mut client, 21, &insert).await;

    assert_eq!(ok.affected, 2, "affected comes from the command tag");
    assert!(ok.rows.is_empty(), "fetch=none ships no rows");
    assert_eq!(ok.stats.rows, 0);

    assert_session_alive(&mut client, 7).await;
}

// -------------------------------------------------------------------------------------------------
// A syntax error → terminal Outcome::Error{Syntax} (NOT Indeterminate); session still usable.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn exec_syntax_error() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    let ep = exec_err(&mut client, 30, &req("SELCT 1")).await;
    assert_eq!(ep.code, errc::SYNTAX, "a syntax error maps to Syntax");
    assert_eq!(ep.branch, branch::NON_RETRYABLE);
    assert!(
        ep.sqlstate.as_deref().is_some_and(|s| s.starts_with("42")),
        "the raw SQLSTATE (42xxx) is preserved on the wire, got {:?}",
        ep.sqlstate
    );
    // NOT the Indeterminate branch — a syntax error's fate is known.
    assert_ne!(ep.code, errc::WRITE_UNCONFIRMED);
    assert_ne!(ep.branch, branch::INDETERMINATE);

    // Statement-level, not session-level: the session survives.
    assert_session_alive(&mut client, 3).await;
}

// -------------------------------------------------------------------------------------------------
// COMMIT-1 end-to-end proof: a Value::I64 param against an int4 PK column is known-fate Unsupported,
// NOT WriteUnconfirmed/Indeterminate — EVEN on a non-readonly (write) statement.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn exec_wrong_param_type_not_indeterminate() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // int4 PK ⇒ PG infers the INSERT's parameter as int4; I64→int8 cannot bind it (M0).
    let mut ddl = req("CREATE TABLE IF NOT EXISTS ferro_s5_pk4 (id int4 primary key)");
    ddl.readonly = false;
    ddl.fetch = 1;
    let _ = exec_ok(&mut client, 40, &ddl).await;

    let mut insert = req("INSERT INTO ferro_s5_pk4 (id) VALUES (?)");
    insert.readonly = false; // a WRITE — the readonly override would fire IF this were ConnectionLost
    insert.fetch = 1;
    insert.params = vec![Value::I64(1)];
    let ep = exec_err(&mut client, 41, &insert).await;

    // Known-fate Unsupported (the bind never executed), NOT the fate-unknown Indeterminate.
    assert_eq!(
        ep.code,
        errc::UNSUPPORTED,
        "a bind pre-validation error is known-fate Unsupported"
    );
    assert_ne!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "REGRESSION: a bind error on a write must NOT become WriteUnconfirmed/Indeterminate"
    );
    assert_ne!(ep.branch, branch::INDETERMINATE);

    // Nothing inserted, connection clean: the session (and pool) keep working.
    assert_session_alive(&mut client, 8).await;
    let ok = exec_ok(&mut client, 42, &req("SELECT 1")).await;
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

// -------------------------------------------------------------------------------------------------
// §19.3 checkout-loss fix (T3-review MAJOR), END TO END and OFFLINE: a checkout-time connect failure
// on a NON-READONLY (write) EXEC is a KNOWN-FATE ConnectionLost{Retryable}, NEVER a false
// WriteUnconfirmed{Indeterminate} — the statement was never transmitted. Needs a DSN that FAILS to
// connect (port 1 → connection refused), not a live PG, so it runs without Docker.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn exec_checkout_connect_failure_is_retryable_not_indeterminate() {
    // 127.0.0.1:1 refuses immediately → the pool's fresh-connect at checkout returns
    // PoolError::ConnectionLost (pool.rs: "a connect failure surfaces immediately — no hidden retry").
    let server = exec_server("postgres://ferro:ferro@127.0.0.1:1/ferro".to_string());
    let mut client = server.connect().await;
    client.hello(1).await;

    let mut write = req("INSERT INTO whatever (id) VALUES (1)");
    write.readonly = false; // the readonly→Indeterminate override would fire IF this were sent=true

    let ep = exec_err(&mut client, 70, &write).await;
    assert_eq!(
        ep.code,
        errc::CONNECTION_LOST,
        "a never-transmitted (checkout-time) connect failure is known-fate ConnectionLost"
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "REGRESSION: a write that never left the client must NOT be reported Indeterminate"
    );
    assert_ne!(ep.branch, branch::INDETERMINATE);

    // A statement-level error: the session survives (exactly one END).
    assert_session_alive(&mut client, 4).await;
}

// -------------------------------------------------------------------------------------------------
// query_id / unknown pool / fetch=stream → Unsupported; session survives each.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_query_id_pool_stream() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // query_id set (manifest is M3).
    let mut with_qid = req("SELECT 1");
    with_qid.query_id = Some("q1".to_string());
    assert_eq!(
        exec_err(&mut client, 50, &with_qid).await.code,
        errc::UNSUPPORTED
    );

    // unknown pool name.
    let mut bad_pool = req("SELECT 1");
    bad_pool.pool = "does-not-exist".to_string();
    assert_eq!(
        exec_err(&mut client, 51, &bad_pool).await.code,
        errc::UNSUPPORTED
    );

    // fetch = stream (reserved in M0).
    let mut stream = req("SELECT 1");
    stream.fetch = 2;
    assert_eq!(
        exec_err(&mut client, 52, &stream).await.code,
        errc::UNSUPPORTED
    );

    // All three were per-request errors; the session is unaffected.
    assert_session_alive(&mut client, 5).await;
}

// -------------------------------------------------------------------------------------------------
// A malformed ExecRequest payload → per-request Protocol error, one END, session survives. Needs no
// DB (rejected at decode), but gated for uniformity.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn exec_malformed_payload_is_protocol_error() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;

    // Not a valid ExecRequest fixarray at all.
    client
        .send_request(60, service::SQL, method_sql::EXEC, vec![0xff, 0x00, 0x13])
        .await;
    let terminal = client.recv().await;
    assert_eq!(terminal.header.request_id, 60);
    assert_eq!(terminal.header.flags & flags::END, flags::END);
    match Outcome::decode(&terminal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error(Protocol), got {other:?}"),
    }

    assert_session_alive(&mut client, 6).await;
}

fn hex(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
