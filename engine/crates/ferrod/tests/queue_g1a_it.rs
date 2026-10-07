//! M7-G1a: Ferro Queue's service on the chassis — routing, decoding, store resolution, the
//! per-request refusals, the version gate and shape verification (SPEC §24.3, §24.4), end to end
//! through a real session.
//!
//! Two halves:
//!
//! - **Before any checkout.** The store's pool points at a port nothing listens on, so a request that
//!   reached a checkout or a statement would come back as a connection failure. Each refusal coming
//!   back as ITS OWN terminal is therefore the proof that it was decided before anything was sent.
//! - **Live, against PostgreSQL** (`FERRO_TEST_PG_URL`; skips, and the CI no-skip gate fails, when
//!   unset). Each test owns a fresh schema, so they run in parallel, and each server owns its own
//!   registry, so each first use is a real first use. An absent table's verdict is reused for
//!   `ABSENT_RECHECK` (2 s), so a test that creates the table after a first use waits that out.
//!
//! Every request is answered by exactly one terminal on its own request id and service, and the
//! session survives it (charter rule 4).

mod common;

use std::ffi::OsString;
use std::sync::Arc;

use common::{TestClient, TestServer, assert_session_alive, exec, pg_url, req};
use ferro_proto::consts::{QUEUE_HANDLE_MAX_BYTES, errc, flags, method_queue, service};
use ferro_proto::messages::{
    EnqueueJob, EnqueueRequest, ErrorPayload, FencedRequest, Outcome, QueueCommon,
    QueueScopeRequest, ReleaseRequest, ReserveRequest,
};
use ferro_queue::sql::{JobId, Token};
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;

/// A port nothing listens on: any dial is refused at once.
const DEAD_PG: &str = "postgres://ferro:ferro@127.0.0.1:1/ferro";

/// A session server with one pool, `default`, at `dsn`, and Ferro Queue configured from `vars`
/// (`None` = the queue unconfigured) — loaded by the daemon's own `queue_config::load`.
fn queue_server(dsn: &str, vars: Option<&[(&str, &str)]>) -> (TestServer, Arc<PoolRegistry>) {
    let mut config = Config {
        pools: vec![PoolSpec {
            name: "default".into(),
            dsn: dsn.into(),
            kind: ferrod::config::infer_pool_kind(dsn),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
            allow_dir: None,
        }],
        ..Config::default()
    };
    if let Some(vars) = vars {
        let loaded = ferrod::queue_config::load(
            vars.iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &config,
        );
        config.queue = Some(Arc::new(loaded));
    }
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    let server = TestServer::spawn_with_factory_and_config(
        BootEpoch(1),
        config,
        registry.clone(),
        tx_registry,
        factory,
    );
    (server, registry)
}

/// `jobs` on the `default` pool, plus `extra` store keys.
fn store_vars<'a>(extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut v = vec![
        ("FERRO_QUEUE_STORES", "jobs"),
        ("FERRO_QUEUE_JOBS_POOL", "default"),
    ];
    v.extend_from_slice(extra);
    v
}

async fn connected(server: &TestServer) -> TestClient {
    let mut c = server.connect().await;
    c.hello(1).await;
    c
}

/// Send one QUEUE request and read back its ONE terminal, asserting the frame shape.
async fn queue(client: &mut TestClient, rid: u32, method: u16, payload: Vec<u8>) -> Outcome {
    client
        .send_request(rid, service::QUEUE, method, payload)
        .await;
    let t = client.recv().await;
    assert_eq!(
        t.header.request_id, rid,
        "the terminal echoes the request id"
    );
    assert_eq!(t.header.flags, flags::END, "exactly one END, nothing else");
    assert_eq!(
        (t.header.service, t.header.method),
        (service::QUEUE, method),
        "the terminal rides the request's own service and method"
    );
    Outcome::decode(&t.payload).expect("a terminal Outcome")
}

async fn queue_err(
    client: &mut TestClient,
    rid: u32,
    method: u16,
    payload: Vec<u8>,
) -> ErrorPayload {
    match queue(client, rid, method, payload).await {
        Outcome::Error(ep) => ep,
        other => panic!("expected an error terminal, got {other:?}"),
    }
}

fn size(store: &str) -> Vec<u8> {
    QueueScopeRequest {
        store: store.into(),
        queue: "default".into(),
        common: QueueCommon::default(),
    }
    .encode()
}

fn ack(job_id: &[u8], token: &[u8]) -> Vec<u8> {
    FencedRequest {
        store: "jobs".into(),
        job_id: job_id.to_vec(),
        token: token.to_vec(),
        common: QueueCommon::default(),
    }
    .encode()
}

fn enqueue(payload: &str, queue: &str, dedup: Option<&str>, tx_id: Option<u64>) -> Vec<u8> {
    EnqueueRequest {
        store: "jobs".into(),
        jobs: vec![EnqueueJob {
            queue: queue.into(),
            payload: payload.into(),
            delay_s: 0,
        }],
        dedup_key: dedup.map(str::to_string),
        common: QueueCommon {
            tx_id,
            ..QueueCommon::default()
        },
    }
    .encode()
}

fn reserve(liveness: bool, wait_ms: u32, tx_id: Option<u64>) -> Vec<u8> {
    ReserveRequest {
        store: "jobs".into(),
        queues: vec!["default".into()],
        max_jobs: 1,
        wait_ms,
        liveness,
        common: QueueCommon {
            tx_id,
            ..QueueCommon::default()
        },
    }
    .encode()
}

fn assert_code(ep: &ErrorPayload, code: u16, needle: &str) {
    assert_eq!(ep.code, code, "{ep:?}");
    assert!(
        ep.message.contains(needle),
        "{:?} lacks {needle:?}",
        ep.message
    );
}

// ---------------------------------------------------------------------------------------------
// Before any checkout (no database needed: the pool is unreachable on purpose)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn an_unconfigured_queue_and_an_unknown_store_are_unsupported() {
    let (server, _) = queue_server(DEAD_PG, None);
    let mut c = connected(&server).await;
    let ep = queue_err(&mut c, 2, method_queue::SIZE, size("jobs")).await;
    assert_code(&ep, errc::UNSUPPORTED, "not configured");

    let (server, _) = queue_server(DEAD_PG, Some(&store_vars(&[])));
    let mut c = connected(&server).await;
    let ep = queue_err(&mut c, 3, method_queue::SIZE, size("nope")).await;
    assert_code(&ep, errc::UNSUPPORTED, "unknown queue store");
    // A store refused at configuration answers exactly as an unknown one.
    let (server, _) = queue_server(
        DEAD_PG,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_KIND", "redis")])),
    );
    let mut c = connected(&server).await;
    let refused = queue_err(&mut c, 4, method_queue::SIZE, size("jobs")).await;
    assert_eq!(refused, ep, "refused at configuration == unknown");
    assert_session_alive(&mut c, 44).await;
}

#[tokio::test]
async fn a_malformed_request_is_protocol_and_an_unallocated_method_is_unsupported() {
    let (server, _) = queue_server(DEAD_PG, Some(&store_vars(&[])));
    let mut c = connected(&server).await;
    // A 1 025-byte token: an out-of-bounds field, refused by the codec. Written by hand — the
    // encoder refuses to build it — as `[ "jobs", bin8 "42", bin16 <1025 bytes>, [nil, nil, nil] ]`.
    let over = QUEUE_HANDLE_MAX_BYTES as usize + 1;
    let mut b = vec![0x94, 0xa4];
    b.extend_from_slice(b"jobs");
    b.extend_from_slice(&[0xc4, 0x02, b'4', b'2', 0xc5]);
    b.extend_from_slice(&(over as u16).to_be_bytes());
    b.extend_from_slice(&vec![0; over]);
    b.extend_from_slice(&[0x93, 0xc0, 0xc0, 0xc0]);
    let ep = queue_err(&mut c, 2, method_queue::ACK, b).await;
    assert_code(&ep, errc::PROTOCOL, "token");
    // An ENQUEUE body sent as an ACK: the method decides the shape.
    let ep = queue_err(
        &mut c,
        3,
        method_queue::ACK,
        enqueue("{}", "default", None, None),
    )
    .await;
    assert_eq!(ep.code, errc::PROTOCOL);
    // A QUEUE method id the registry does not allocate never reaches the handler.
    c.send_request(4, service::QUEUE, 8, size("jobs")).await;
    let t = c.recv().await;
    assert_eq!((t.header.request_id, t.header.flags), (4, flags::END));
    let Outcome::Error(ep) = Outcome::decode(&t.payload).unwrap() else {
        panic!("an error terminal");
    };
    assert_eq!(ep.code, errc::UNSUPPORTED);
    assert_session_alive(&mut c, 45).await;
}

/// SPEC §24.3 prerequisite (c): a handle the store cannot decode is `InvalidHandle` — neither
/// `Protocol` (the frame is well-formed) nor `LeaseLost` — and is refused before any statement: the
/// pool is unreachable, so reaching a checkout would have answered a connection failure instead.
#[tokio::test]
async fn an_undecodable_job_id_or_token_is_invalid_handle_before_any_statement() {
    let (server, _) = queue_server(DEAD_PG, Some(&store_vars(&[])));
    let mut c = connected(&server).await;
    let token = Token::from_pg(1_790_000_000, 1).encode();
    for (rid, job_id, tok) in [
        (2u32, &b"042"[..], &token[..]),
        (3, b"+42", &token),
        (4, b" 42", &token),
        (5, b"9223372036854775808", &token),
        (6, b"42", &[0u8; 7][..]),
        (7, b"42", &[0xff; 8][..]),
        (8, &[0xc0; QUEUE_HANDLE_MAX_BYTES as usize][..], &token),
    ] {
        let ep = queue_err(&mut c, rid, method_queue::ACK, ack(job_id, tok)).await;
        assert_eq!(
            (ep.code, ep.branch),
            (errc::INVALID_HANDLE, errc::INVALID_HANDLE_BRANCH),
            "rid {rid}: {ep:?}"
        );
    }
    // RELEASE and EXTEND decode the same way.
    let rel = ReleaseRequest {
        store: "jobs".into(),
        job_id: b"07".to_vec(),
        token: token.to_vec(),
        delay_s: 0,
        common: QueueCommon::default(),
    };
    let ep = queue_err(&mut c, 9, method_queue::RELEASE, rel.encode()).await;
    assert_eq!(ep.code, errc::INVALID_HANDLE);
    let ep = queue_err(&mut c, 10, method_queue::EXTEND, ack(b"1", &[1, 2, 3])).await;
    assert_eq!(ep.code, errc::INVALID_HANDLE);
    // The control: a DECODABLE handle passes the pre-checkout steps and then needs the database —
    // which, here, is unreachable: a known non-execution (`ConnectionLost`, Retryable).
    let ep = queue_err(
        &mut c,
        11,
        method_queue::ACK,
        ack(&JobId(42).encode(), &token),
    )
    .await;
    assert_eq!(
        (ep.code, ep.branch),
        (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
        "{ep:?}"
    );
    assert_session_alive(&mut c, 46).await;
}

/// Every value refusal of SPEC §24.4, before any checkout.
#[tokio::test]
async fn value_refusals_are_unsupported_before_any_checkout() {
    let (server, _) = queue_server(DEAD_PG, Some(&store_vars(&[])));
    let mut c = connected(&server).await;
    let cases: Vec<(u16, Vec<u8>, &str)> = vec![
        (
            method_queue::ENQUEUE,
            enqueue("a\0b", "default", None, None),
            "U+0000",
        ),
        (
            method_queue::ENQUEUE,
            enqueue("{}", "", None, None),
            "queue name",
        ),
        (
            method_queue::ENQUEUE,
            enqueue("{}", &"q".repeat(256), None, None),
            "queue name",
        ),
        (
            method_queue::ENQUEUE,
            enqueue("{}", "default", Some("k"), None),
            "DEDUP_TABLE",
        ),
        (method_queue::RESERVE, reserve(true, 0, None), "liveness"),
        (method_queue::RESERVE, reserve(false, 1, None), "G3"),
        (method_queue::RESERVE, reserve(false, 0, Some(7)), "§24.5"),
        (
            method_queue::CLEAR,
            QueueScopeRequest {
                store: "jobs".into(),
                queue: "\0".into(),
                common: QueueCommon::default(),
            }
            .encode(),
            "queue name",
        ),
    ];
    for (i, (method, payload, needle)) in cases.into_iter().enumerate() {
        let ep = queue_err(&mut c, 20 + i as u32, method, payload).await;
        assert_code(&ep, errc::UNSUPPORTED, needle);
    }
    // M7-G2: a tx-scoped ENQUEUE is no longer refused `Unsupported` — its `tx_id` is RESOLVED, and an
    // unknown one is `TxNotFound`, still before any checkout (the pool is unreachable).
    let ep = queue_err(
        &mut c,
        40,
        method_queue::ENQUEUE,
        enqueue("{}", "default", None, Some(7)),
    )
    .await;
    assert_eq!(
        (ep.code, ep.branch),
        (errc::TX_NOT_FOUND, errc::TX_NOT_FOUND_BRANCH),
        "{ep:?}"
    );
    // MAX_PAYLOAD_BYTES is the store's: one byte over it is refused, by size, not by content.
    let (server, _) = queue_server(
        DEAD_PG,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "4")])),
    );
    let mut c = connected(&server).await;
    let ep = queue_err(
        &mut c,
        2,
        method_queue::ENQUEUE,
        enqueue("12345", "default", None, None),
    )
    .await;
    assert_code(&ep, errc::UNSUPPORTED, "MAX_PAYLOAD_BYTES");
    assert_session_alive(&mut c, 47).await;
}

// ---------------------------------------------------------------------------------------------
// Live: the version gate and shape verification against PostgreSQL
// ---------------------------------------------------------------------------------------------

static SCHEMA_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// A fresh schema name, unique across parallel tests and runs.
fn fresh_schema(tag: &str) -> String {
    let n = SCHEMA_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("g1a_{tag}_{}_{n}", std::process::id())
}

/// Laravel's `jobs.stub` on PostgreSQL, measured against laravel/framework v11's grammar.
fn stock_table(qualified: &str) -> String {
    format!(
        "CREATE TABLE {qualified} (id bigserial PRIMARY KEY, queue varchar(255) NOT NULL, \
         payload text NOT NULL, attempts smallint NOT NULL, reserved_at integer NULL, \
         available_at integer NOT NULL, created_at integer NOT NULL)"
    )
}

async fn sql_ok(c: &mut TestClient, rid: u32, statement: &str) {
    let mut r = req(statement);
    r.readonly = false;
    r.fetch = 1;
    match exec(c, rid, &r).await {
        Outcome::Ok(_) => {}
        other => panic!("{statement}: {other:?}"),
    }
}

/// Past the absent-table negative TTL (SPEC §24.3 amendment, review M2).
async fn wait_absent_recheck() {
    tokio::time::sleep(
        ferrod::services::queue::ABSENT_RECHECK + std::time::Duration::from_millis(100),
    )
    .await;
}

/// A verified store SERVES its verbs (M7-G1b): SIZE answers its counts. (Under G1a a verified store
/// answered `Unsupported` "is verified"; the verbs exist now, so success is the marker.)
async fn assert_verified(c: &mut TestClient, rid: u32) {
    match queue(c, rid, method_queue::SIZE, size("jobs")).await {
        Outcome::Ok(body) => {
            ferro_proto::messages::SizeResponse::decode(&body).expect("a SIZE terminal");
        }
        other => panic!("expected the verified store to serve SIZE, got {other:?}"),
    }
}

#[tokio::test]
async fn a_stock_table_passes_the_gate_and_verification_which_is_then_cached() {
    let Some(url) = pg_url() else { return };
    let schema = fresh_schema("stock");
    let table = format!("{schema}.ferro_jobs");
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_TABLE", &table)])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("CREATE SCHEMA {schema}")).await;
    sql_ok(&mut c, 3, &stock_table(&table)).await;

    // Every verb passes verification and is SERVED (M7-G1b): none answers `Unsupported`.
    let token = Token::from_pg(1, 1).encode();
    for (rid, method, payload) in [
        (10u32, method_queue::SIZE, size("jobs")),
        (11, method_queue::CLEAR, size("jobs")),
        (
            12,
            method_queue::ENQUEUE,
            enqueue("{}", "default", None, None),
        ),
        (13, method_queue::RESERVE, reserve(false, 0, None)),
        (14, method_queue::ACK, ack(b"999", &token)), // no such row: `gone`
    ] {
        match queue(&mut c, rid, method, payload).await {
            Outcome::Ok(_) => {}
            other => panic!("method {method}: {other:?}"),
        }
    }
    // EXTEND of a token naming no row is a known-fate LeaseLost — served, not refused.
    let ep = queue_err(&mut c, 15, method_queue::EXTEND, ack(b"1", &token)).await;
    assert_code(&ep, errc::LEASE_LOST, "did nothing");

    // CACHED for the process: dropping the table does not bring back verification's "does not
    // exist" — the verb is SENT and the statement itself fails (42P01, undefined table).
    sql_ok(&mut c, 20, &format!("DROP SCHEMA {schema} CASCADE")).await;
    let ep = queue_err(&mut c, 21, method_queue::SIZE, size("jobs")).await;
    assert_eq!(ep.sqlstate.as_deref(), Some("42P01"), "{ep:?}");
    assert!(
        !ep.message.contains("on the pool's search_path"),
        "{}",
        ep.message
    );
    assert_session_alive(&mut c, 48).await;
}

#[tokio::test]
async fn an_absent_table_is_named_and_re_checked_at_the_next_use() {
    let Some(url) = pg_url() else { return };
    let schema = fresh_schema("absent");
    let table = format!("{schema}.ferro_jobs");
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_TABLE", &table)])),
    );
    let mut c = connected(&server).await;
    let ep = queue_err(&mut c, 2, method_queue::SIZE, size("jobs")).await;
    assert_code(&ep, errc::UNSUPPORTED, "does not exist");
    assert!(ep.message.contains(&table), "{}", ep.message);
    // Not cached for good: the migration runs after `ferrod` started, and the first use after the
    // short negative TTL (review M2) sees it. Inside the TTL the cached answer stands.
    sql_ok(&mut c, 3, &format!("CREATE SCHEMA {schema}")).await;
    sql_ok(&mut c, 4, &stock_table(&table)).await;
    let ep = queue_err(&mut c, 7, method_queue::SIZE, size("jobs")).await;
    assert_code(&ep, errc::UNSUPPORTED, "does not exist");
    wait_absent_recheck().await;
    assert_verified(&mut c, 5).await;
    sql_ok(&mut c, 6, &format!("DROP SCHEMA {schema} CASCADE")).await;
}

#[tokio::test]
async fn a_wrong_shape_names_the_column_and_is_cached_for_the_process() {
    let Some(url) = pg_url() else { return };
    let schema = fresh_schema("shape");
    let table = format!("{schema}.ferro_jobs");
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_TABLE", &table)])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("CREATE SCHEMA {schema}")).await;
    // created_at as bigint: the token's 32 bits could not carry it.
    sql_ok(
        &mut c,
        3,
        &stock_table(&table).replace("created_at integer", "created_at bigint"),
    )
    .await;
    let ep = queue_err(&mut c, 4, method_queue::SIZE, size("jobs")).await;
    assert_code(&ep, errc::UNSUPPORTED, "created_at");
    assert!(ep.message.contains("bigint"), "{}", ep.message);
    // Definitive, so cached: repairing the table does not change the answer before a restart
    // (SPEC §24.3: `ferrod` has no configuration reload in v1) — not even after the absent-table
    // TTL, which a wrong shape must not be given.
    sql_ok(
        &mut c,
        5,
        &format!("ALTER TABLE {table} ALTER COLUMN created_at TYPE integer"),
    )
    .await;
    wait_absent_recheck().await;
    let again = queue_err(&mut c, 6, method_queue::SIZE, size("jobs")).await;
    assert_eq!(again.message, ep.message);
    sql_ok(&mut c, 7, &format!("DROP SCHEMA {schema} CASCADE")).await;
}

#[tokio::test]
async fn a_missing_column_and_a_non_nullable_reserved_at_are_named() {
    let Some(url) = pg_url() else { return };
    for (tag, edit, column) in [
        ("nocol", ", attempts smallint NOT NULL", "attempts"),
        ("notnull", "reserved_at integer NULL", "reserved_at"),
    ] {
        let schema = fresh_schema(tag);
        let table = format!("{schema}.ferro_jobs");
        let (server, _) = queue_server(
            &url,
            Some(&store_vars(&[("FERRO_QUEUE_JOBS_TABLE", &table)])),
        );
        let mut c = connected(&server).await;
        sql_ok(&mut c, 2, &format!("CREATE SCHEMA {schema}")).await;
        let ddl = if tag == "nocol" {
            stock_table(&table).replace(edit, "")
        } else {
            stock_table(&table).replace(edit, "reserved_at integer NOT NULL")
        };
        sql_ok(&mut c, 3, &ddl).await;
        let ep = queue_err(&mut c, 4, method_queue::SIZE, size("jobs")).await;
        assert_code(&ep, errc::UNSUPPORTED, column);
        sql_ok(&mut c, 5, &format!("DROP SCHEMA {schema} CASCADE")).await;
    }
}

/// `TABLE` is used verbatim and quoted, as Laravel's schema builder quotes the tables it creates:
/// `Jobs` is the case-sensitive table `"Jobs"`, never PostgreSQL's folded `jobs`.
#[tokio::test]
async fn a_mixed_case_table_is_the_quoted_one_not_the_folded_one() {
    let Some(url) = pg_url() else { return };
    let schema = fresh_schema("case");
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[(
            "FERRO_QUEUE_JOBS_TABLE",
            &format!("{schema}.Jobs"),
        )])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("CREATE SCHEMA {schema}")).await;
    // The folded (lowercase) table exists; the quoted one does not.
    sql_ok(&mut c, 3, &stock_table(&format!("{schema}.jobs"))).await;
    let ep = queue_err(&mut c, 4, method_queue::SIZE, size("jobs")).await;
    assert_code(&ep, errc::UNSUPPORTED, "does not exist");
    sql_ok(&mut c, 5, &stock_table(&format!("{schema}.\"Jobs\""))).await;
    wait_absent_recheck().await;
    assert_verified(&mut c, 6).await;
    sql_ok(&mut c, 7, &format!("DROP SCHEMA {schema} CASCADE")).await;
}

/// The default table is `ferro_jobs` resolved through the pool's `search_path` (D22 amendment (a);
/// here the first schema on it), and an unqualified `TABLE` resolves the same way. Uses a role-private schema on the search path through a
/// dedicated table name, so it cannot collide with another test or a real `ferro_jobs`.
#[tokio::test]
async fn an_unqualified_table_is_looked_up_in_current_schema() {
    let Some(url) = pg_url() else { return };
    let name = format!("g1a_unqual_{}", std::process::id());
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_TABLE", &name)])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("DROP TABLE IF EXISTS {name}")).await;
    sql_ok(&mut c, 3, &stock_table(&name)).await;
    assert_verified(&mut c, 4).await;
    sql_ok(&mut c, 5, &format!("DROP TABLE {name}")).await;
}

/// Review M1: verification resolves the table the STATEMENTS will touch. With
/// `search_path = <a>, <b>` and the table only in `<b>`, an unqualified `TABLE` is `<b>`'s table —
/// the first version looked in `current_schema()` (`<a>`) only and reported it absent, uncached, on
/// every request.
#[tokio::test]
async fn an_unqualified_table_follows_the_pools_search_path() {
    let Some(url) = pg_url() else { return };
    let first = fresh_schema("sp_first");
    let second = fresh_schema("sp_second");
    let sep = if url.contains('?') { '&' } else { '?' };
    let dsn = format!("{url}{sep}options=-c%20search_path%3D{first}%2C{second}");
    let (server, _) = queue_server(
        &dsn,
        Some(&store_vars(&[("FERRO_QUEUE_JOBS_TABLE", "ferro_jobs")])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("CREATE SCHEMA {first}")).await;
    sql_ok(&mut c, 3, &format!("CREATE SCHEMA {second}")).await;
    sql_ok(&mut c, 4, &stock_table(&format!("{second}.ferro_jobs"))).await;
    // The control that the pool really runs with that search_path: `current_schema()` is the first.
    match exec(&mut c, 5, &req("SELECT current_schema()::text")).await {
        Outcome::Ok(body) => {
            let ok = ferro_proto::messages::ExecOk::decode(&body).unwrap();
            assert_eq!(
                ok.rows[0][0],
                ferro_proto::value::Value::Text(first.clone()),
                "the pool's search_path is in force"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_verified(&mut c, 6).await;
    sql_ok(&mut c, 7, &format!("DROP SCHEMA {first} CASCADE")).await;
    sql_ok(&mut c, 8, &format!("DROP SCHEMA {second} CASCADE")).await;
}

/// Review M1: a VIEW with exactly the right columns is not a table — refused, naming why, and cached
/// (it exists in the wrong form) — while a PARTITIONED table of the stock layout passes.
#[tokio::test]
async fn a_view_is_refused_and_a_partitioned_table_passes() {
    let Some(url) = pg_url() else { return };
    let schema = fresh_schema("relkind");
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[(
            "FERRO_QUEUE_JOBS_TABLE",
            &format!("{schema}.ferro_jobs"),
        )])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("CREATE SCHEMA {schema}")).await;
    sql_ok(&mut c, 3, &stock_table(&format!("{schema}.real_jobs"))).await;
    sql_ok(
        &mut c,
        4,
        &format!("CREATE VIEW {schema}.ferro_jobs AS SELECT * FROM {schema}.real_jobs"),
    )
    .await;
    let ep = queue_err(&mut c, 5, method_queue::SIZE, size("jobs")).await;
    assert_code(&ep, errc::UNSUPPORTED, "is not a table");
    sql_ok(&mut c, 6, &format!("DROP SCHEMA {schema} CASCADE")).await;

    let schema = fresh_schema("partitioned");
    let (server, _) = queue_server(
        &url,
        Some(&store_vars(&[(
            "FERRO_QUEUE_JOBS_TABLE",
            &format!("{schema}.ferro_jobs"),
        )])),
    );
    let mut c = connected(&server).await;
    sql_ok(&mut c, 2, &format!("CREATE SCHEMA {schema}")).await;
    sql_ok(
        &mut c,
        3,
        &(stock_table(&format!("{schema}.ferro_jobs")) + " PARTITION BY RANGE (id)"),
    )
    .await;
    assert_verified(&mut c, 4).await;
    sql_ok(&mut c, 5, &format!("DROP SCHEMA {schema} CASCADE")).await;
}
