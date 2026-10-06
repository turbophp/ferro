//! M7-G1b: Ferro Queue's seven verbs in autocommit on PostgreSQL, end to end through a real session
//! (SPEC §24.3, §24.4, §24.6).
//!
//! Live against PostgreSQL (`FERRO_TEST_PG_URL`; skips, and the CI no-skip gate fails, when unset),
//! except the two refusals that must be decided before anything is sent, which run against a pool
//! nothing listens on (an answer that is the refusal itself proves no checkout happened).
//!
//! Each test owns a fresh schema with Laravel's stock `jobs.stub` table, and a RAW side connection
//! (outside `ferrod`'s pool) to set rows up and read them back: every claim about what a verb did is
//! checked against the ROWS (§24.2 I1), not only against the terminal.
//!
//! What is proven here, by section:
//!
//! - **the verbs:** ENQUEUE (single and batch, ids in order), RESERVE (attempts, token, deadline;
//!   FIFO; the first non-empty queue; the frame clamp; the attempts ceiling and over-size rows), ACK
//!   (`acked`, `gone`), RELEASE (to the back under a new id, attempts kept, the delay rule; `gone`),
//!   EXTEND, SIZE (stock's three counts and `oldest_pending_at`), CLEAR;
//! - **the fence:** a stale token is `LeaseLost` for ACK, RELEASE and EXTEND and changes no row; a
//!   late ACK after the lease expired is honoured when nobody re-reserved the job;
//! - **the clock and the rounding rules, at second boundaries**, on the database's clock;
//! - **the `MATERIALIZED` premise under real concurrency** (16 sessions on a 16-connection pool):
//!   no reply exceeds its LIMIT and no job is delivered twice;
//! - **the fate of every verb** when its statement is sent and then times out or is cancelled;
//! - **mixed mode** with stock Laravel `DatabaseQueue`'s exact statements (v11.51.0, captured through
//!   pdo_pgsql): a stock push consumed by a Ferro RESERVE, and the reverse;
//! - the two G1a-review carries: a non-unique `id` refused, and an overflowing `delay_s` refused
//!   before sending.

mod common;

use std::collections::HashSet;
use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use common::{TestClient, TestServer, assert_session_alive, pg_url};
use ferro_proto::consts::{ack_outcome, branch, errc, flags, method_queue, service};
use ferro_proto::messages::{
    AckResponse, ClearResponse, EnqueueJob, EnqueueRequest, EnqueueResponse, ErrorPayload,
    ExtendResponse, FencedRequest, Outcome, QueueCommon, QueueScopeRequest, ReleaseRequest,
    ReleaseResponse, ReserveRequest, ReserveResponse, ReservedJob, SizeResponse,
};
use ferro_queue::sql::{JobId, Token};
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;

/// A port nothing listens on: any dial is refused at once.
const DEAD_PG: &str = "postgres://ferro:ferro@127.0.0.1:1/ferro";

const LEASE_S: i64 = 30;

fn queue_server(dsn: &str, vars: &[(&str, &str)]) -> (TestServer, Arc<PoolRegistry>) {
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
    let loaded = ferrod::queue_config::load(
        vars.iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
        &config,
    );
    config.queue = Some(Arc::new(loaded));
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

static SCHEMA_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn fresh_schema(tag: &str) -> String {
    let n = SCHEMA_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("g1b_{tag}_{}_{n}", std::process::id())
}

/// Laravel's `jobs.stub` on PostgreSQL (laravel/framework v11's grammar), with its `(queue)` index.
fn stock_table(qualified: &str) -> String {
    format!(
        "CREATE TABLE {qualified} (id bigserial PRIMARY KEY, queue varchar(255) NOT NULL, \
         payload text NOT NULL, attempts smallint NOT NULL, reserved_at integer NULL, \
         available_at integer NOT NULL, created_at integer NOT NULL); \
         CREATE INDEX ON {qualified} (queue)"
    )
}

async fn raw_connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("raw side connection to Postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One live test's world: a `ferrod` with the `jobs` store on `<schema>.<table>`, a session to it,
/// and a raw side connection for set-up and read-back.
struct World {
    server: TestServer,
    registry: Arc<PoolRegistry>,
    c: Q,
    raw: tokio_postgres::Client,
    url: String,
    schema: String,
    /// `<schema>.<table>`, unquoted (both parts are plain lowercase identifiers).
    table: String,
}

impl World {
    /// `None` (after printing `skip:`) without `FERRO_TEST_PG_URL`.
    async fn new(tag: &str, extra: &[(&str, &str)]) -> Option<World> {
        World::with_table(tag, "ferro_jobs", extra).await
    }

    async fn with_table(tag: &str, name: &str, extra: &[(&str, &str)]) -> Option<World> {
        let url = pg_url()?;
        let raw = raw_connect(&url).await;
        let schema = fresh_schema(tag);
        let table = format!("{schema}.{name}");
        raw.batch_execute(&format!("CREATE SCHEMA {schema}; {}", stock_table(&table)))
            .await
            .unwrap();
        let mut vars = vec![
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "default"),
            ("FERRO_QUEUE_JOBS_TABLE", table.as_str()),
            ("FERRO_QUEUE_JOBS_LEASE_S", "30"),
            ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "65536"),
        ];
        // `extra` overrides a default rather than repeating its key.
        vars.retain(|(k, _)| !extra.iter().any(|(e, _)| e == k));
        vars.extend_from_slice(extra);
        let (server, registry) = queue_server(&url, &vars);
        let c = Q::connect(&server).await;
        Some(World {
            server,
            registry,
            c,
            raw,
            url,
            schema,
            table,
        })
    }

    async fn drop_schema(self) {
        self.raw
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }

    async fn exec(&self, sql: &str) {
        self.raw.batch_execute(sql).await.unwrap();
    }

    /// The database clock, as the verbs compute it.
    async fn db_now(&self) -> i64 {
        self.raw
            .query_one(
                "SELECT floor(extract(epoch FROM statement_timestamp()))::bigint",
                &[],
            )
            .await
            .unwrap()
            .get(0)
    }

    async fn row(&self, id: i64) -> Option<Row> {
        self.raw
            .query_opt(
                &format!(
                    "SELECT queue, payload, attempts, reserved_at, available_at, created_at \
                     FROM {} WHERE id = $1",
                    self.table
                ),
                &[&id],
            )
            .await
            .unwrap()
            .map(|r| Row {
                queue: r.get(0),
                payload: r.get(1),
                attempts: r.get(2),
                reserved_at: r.get(3),
                available_at: r.get(4),
                created_at: r.get(5),
            })
    }

    async fn count(&self) -> i64 {
        self.raw
            .query_one(&format!("SELECT count(*) FROM {}", self.table), &[])
            .await
            .unwrap()
            .get(0)
    }

    /// Insert a row directly; `$1` queue, `$2` payload, and SQL expressions over `s` (the
    /// statement's own `now`) for the three time columns and attempts. Returns `(id, s)`.
    async fn put(
        &self,
        queue: &str,
        payload: &str,
        attempts: &str,
        reserved_at: &str,
        available_at: &str,
    ) -> (i64, i64) {
        let r = self
            .raw
            .query_one(
                &format!(
                    "WITH n AS (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint \
                     AS s) INSERT INTO {} (queue, payload, attempts, reserved_at, available_at, \
                     created_at) SELECT $1, $2, ({attempts})::smallint, ({reserved_at})::integer, \
                     ({available_at})::integer, s FROM n RETURNING id, created_at::bigint",
                    self.table
                ),
                &[&queue, &payload],
            )
            .await
            .unwrap();
        (r.get(0), r.get(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    queue: String,
    payload: String,
    attempts: i16,
    reserved_at: Option<i32>,
    available_at: i32,
    created_at: i32,
}

/// A session plus its next request id.
struct Q {
    c: TestClient,
    rid: u32,
}

impl Q {
    async fn connect(server: &TestServer) -> Q {
        let mut c = server.connect().await;
        c.hello(1).await;
        Q { c, rid: 10 }
    }

    /// Send one QUEUE request and read back its ONE terminal, asserting the frame shape.
    async fn send(&mut self, method: u16, payload: Vec<u8>) -> Outcome {
        self.rid += 1;
        let rid = self.rid;
        self.c
            .send_request(rid, service::QUEUE, method, payload)
            .await;
        self.terminal(rid, method).await
    }

    async fn terminal(&mut self, rid: u32, method: u16) -> Outcome {
        let t = self.c.recv().await;
        assert_eq!(t.header.request_id, rid);
        assert_eq!(t.header.flags, flags::END, "exactly one END, nothing else");
        assert_eq!(
            (t.header.service, t.header.method),
            (service::QUEUE, method)
        );
        Outcome::decode(&t.payload).expect("a terminal Outcome")
    }

    async fn ok(&mut self, method: u16, payload: Vec<u8>) -> Vec<u8> {
        match self.send(method, payload).await {
            Outcome::Ok(body) => body,
            other => panic!("method {method}: expected success, got {other:?}"),
        }
    }

    async fn err(&mut self, method: u16, payload: Vec<u8>) -> ErrorPayload {
        match self.send(method, payload).await {
            Outcome::Error(ep) => ep,
            other => panic!("method {method}: expected an error, got {other:?}"),
        }
    }

    async fn enqueue(&mut self, jobs: &[(&str, &str, u32)]) -> EnqueueResponse {
        let body = self
            .ok(method_queue::ENQUEUE, enqueue_req(jobs, None))
            .await;
        EnqueueResponse::decode(&body).unwrap()
    }

    async fn enqueue_one(&mut self, queue: &str, payload: &str) -> i64 {
        let r = self.enqueue(&[(queue, payload, 0)]).await;
        JobId::decode(&r.job_id.expect("a single job's id"))
            .unwrap()
            .0
    }

    async fn reserve(&mut self, queues: &[&str], max_jobs: u16) -> Vec<ReservedJob> {
        let body = self
            .ok(method_queue::RESERVE, reserve_req(queues, max_jobs, None))
            .await;
        ReserveResponse::decode(&body).unwrap().jobs
    }

    async fn reserve_one(&mut self, queue: &str) -> ReservedJob {
        let mut jobs = self.reserve(&[queue], 1).await;
        assert_eq!(jobs.len(), 1, "expected one job");
        jobs.pop().unwrap()
    }

    async fn ack(&mut self, job_id: &[u8], token: &[u8]) -> Result<u8, ErrorPayload> {
        match self
            .send(method_queue::ACK, fenced_req(job_id, token, None))
            .await
        {
            Outcome::Ok(b) => Ok(AckResponse::decode(&b).unwrap().outcome),
            Outcome::Error(ep) => Err(ep),
            other => panic!("{other:?}"),
        }
    }

    async fn release(
        &mut self,
        job_id: &[u8],
        token: &[u8],
        delay_s: u32,
    ) -> Result<Option<i64>, ErrorPayload> {
        match self
            .send(method_queue::RELEASE, release_req(job_id, token, delay_s))
            .await
        {
            Outcome::Ok(b) => Ok(ReleaseResponse::decode(&b)
                .unwrap()
                .new_job_id
                .map(|id| JobId::decode(&id).unwrap().0)),
            Outcome::Error(ep) => Err(ep),
            other => panic!("{other:?}"),
        }
    }

    async fn extend(&mut self, job_id: &[u8], token: &[u8]) -> Result<i64, ErrorPayload> {
        match self
            .send(method_queue::EXTEND, fenced_req(job_id, token, None))
            .await
        {
            Outcome::Ok(b) => Ok(ExtendResponse::decode(&b).unwrap().lease_deadline),
            Outcome::Error(ep) => Err(ep),
            other => panic!("{other:?}"),
        }
    }

    async fn size(&mut self, queue: &str) -> SizeResponse {
        let body = self.ok(method_queue::SIZE, scope_req(queue, None)).await;
        SizeResponse::decode(&body).unwrap()
    }

    async fn clear(&mut self, queue: &str) -> u64 {
        let body = self.ok(method_queue::CLEAR, scope_req(queue, None)).await;
        ClearResponse::decode(&body).unwrap().deleted
    }
}

fn common(timeout_ms: Option<u32>) -> QueueCommon {
    QueueCommon {
        timeout_ms,
        ..QueueCommon::default()
    }
}

fn enqueue_req(jobs: &[(&str, &str, u32)], timeout_ms: Option<u32>) -> Vec<u8> {
    EnqueueRequest {
        store: "jobs".into(),
        jobs: jobs
            .iter()
            .map(|&(q, p, d)| EnqueueJob {
                queue: q.into(),
                payload: p.into(),
                delay_s: d,
            })
            .collect(),
        dedup_key: None,
        common: common(timeout_ms),
    }
    .encode()
}

fn reserve_req(queues: &[&str], max_jobs: u16, timeout_ms: Option<u32>) -> Vec<u8> {
    ReserveRequest {
        store: "jobs".into(),
        queues: queues.iter().map(|q| q.to_string()).collect(),
        max_jobs,
        wait_ms: 0,
        liveness: false,
        common: common(timeout_ms),
    }
    .encode()
}

fn fenced_req(job_id: &[u8], token: &[u8], timeout_ms: Option<u32>) -> Vec<u8> {
    FencedRequest {
        store: "jobs".into(),
        job_id: job_id.to_vec(),
        token: token.to_vec(),
        common: common(timeout_ms),
    }
    .encode()
}

fn release_req(job_id: &[u8], token: &[u8], delay_s: u32) -> Vec<u8> {
    release_req_t(job_id, token, delay_s, None)
}

fn release_req_t(job_id: &[u8], token: &[u8], delay_s: u32, timeout_ms: Option<u32>) -> Vec<u8> {
    ReleaseRequest {
        store: "jobs".into(),
        job_id: job_id.to_vec(),
        token: token.to_vec(),
        delay_s,
        common: common(timeout_ms),
    }
    .encode()
}

fn scope_req(queue: &str, timeout_ms: Option<u32>) -> Vec<u8> {
    QueueScopeRequest {
        store: "jobs".into(),
        queue: queue.into(),
        common: common(timeout_ms),
    }
    .encode()
}

fn id_of(job: &ReservedJob) -> i64 {
    JobId::decode(&job.job_id).unwrap().0
}

fn assert_lease_lost(r: Result<impl std::fmt::Debug, ErrorPayload>) {
    let ep = r.expect_err("expected LeaseLost");
    assert_eq!(
        (ep.code, ep.branch),
        (errc::LEASE_LOST, errc::LEASE_LOST_BRANCH),
        "{ep:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// Refused before anything is sent (no database: the pool is unreachable on purpose)
// ---------------------------------------------------------------------------------------------

/// Carried from the G1a review: `now + 1 + delay_s` must fit PostgreSQL's `integer`; a `delay_s`
/// that cannot is refused `Unsupported` BEFORE any checkout — against an unreachable pool, the
/// refusal coming back as itself (not a connection failure) is the proof nothing was sent. ENQUEUE
/// (any job of a batch) and RELEASE both.
#[tokio::test]
async fn an_overflowing_delay_is_refused_before_anything_is_sent() {
    let (server, _) = queue_server(
        DEAD_PG,
        &[
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "default"),
        ],
    );
    let mut c = Q::connect(&server).await;
    let ep = c
        .err(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "{}", 0), ("default", "{}", u32::MAX)], None),
        )
        .await;
    assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
    assert!(
        ep.message.contains("delay_s is too large"),
        "{}",
        ep.message
    );
    assert!(ep.message.contains("nothing was sent"), "{}", ep.message);
    let token = Token::from_pg(1, 1).encode();
    let ep = c
        .err(method_queue::RELEASE, release_req(b"1", &token, u32::MAX))
        .await;
    assert!(
        ep.message.contains("delay_s is too large"),
        "{}",
        ep.message
    );
    // The bound is judged on the engine's REAL clock (mutation R7 fed it 0 and survived the
    // `u32::MAX` case above, which no clock admits): one second past today's largest admissible
    // delay is refused, while ten seconds under it reaches the pool.
    let today_max = i64::from(i32::MAX)
        - i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap()
        - 1
        - ferro_queue::checks::DELAY_CLOCK_MARGIN_S;
    let just_over = u32::try_from(today_max + 1).unwrap();
    let ep = c
        .err(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "{}", just_over)], None),
        )
        .await;
    assert!(ep.message.contains("delay_s is too large"), "{ep:?}");
    let under = u32::try_from(today_max - 10).unwrap();
    let ep = c
        .err(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "{}", under)], None),
        )
        .await;
    assert_eq!(ep.code, errc::CONNECTION_LOST, "{ep:?}");
    // The control: an ordinary delay reaches the pool (and its connection failure).
    let ep = c
        .err(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "{}", 3_600)], None),
        )
        .await;
    assert_eq!(
        (ep.code, ep.branch),
        (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
        "{ep:?}"
    );
    assert_session_alive(&mut c.c, 9).await;
}

// ---------------------------------------------------------------------------------------------
// Live: shape verification's uniqueness requirement
// ---------------------------------------------------------------------------------------------

/// Carried from the G1a review: a fence over duplicate ids would match several rows, so a table whose
/// `id` has no unique index is refused, naming `id`, before any verb runs.
#[tokio::test]
async fn a_table_whose_id_is_not_unique_is_refused() {
    let Some(mut w) = World::new("nouniq", &[]).await else {
        return;
    };
    // The stock layout minus its primary key, plus a NON-unique index on id.
    w.exec(&format!(
        "DROP TABLE {t}; {}; ALTER TABLE {t} DROP CONSTRAINT ferro_jobs_pkey; \
         CREATE INDEX ON {t} (id)",
        stock_table(&w.table),
        t = w.table
    ))
    .await;
    let ep =
        w.c.err(method_queue::SIZE, scope_req("default", None))
            .await;
    assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
    assert!(
        ep.message.contains("column id is not unique"),
        "{}",
        ep.message
    );
    // Every verb is refused the same way (the verdict is cached).
    let ep =
        w.c.err(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "{}", 0)], None),
        )
        .await;
    assert!(
        ep.message.contains("column id is not unique"),
        "{}",
        ep.message
    );
    assert_eq!(w.count().await, 0, "nothing was inserted");
    w.drop_schema().await;

    // Each index shape that does NOT make `id` unique on its own is refused; a unique index on
    // `id` alone (with INCLUDE columns) passes. Measured per clause of the catalog predicate.
    for (tag, index, unique) in [
        (
            "partial",
            "CREATE UNIQUE INDEX ON {t} (id) WHERE queue = 'x'",
            false,
        ),
        ("composite", "CREATE UNIQUE INDEX ON {t} (id, queue)", false),
        ("expression", "CREATE UNIQUE INDEX ON {t} ((id + 0))", false),
        (
            "deferrable",
            "ALTER TABLE {t} ADD CONSTRAINT u UNIQUE (id) DEFERRABLE INITIALLY DEFERRED",
            false,
        ),
        (
            "include",
            "CREATE UNIQUE INDEX ON {t} (id) INCLUDE (queue)",
            true,
        ),
    ] {
        let Some(mut w) = World::new(tag, &[]).await else {
            return;
        };
        w.exec(&format!(
            "ALTER TABLE {t} DROP CONSTRAINT ferro_jobs_pkey; {}",
            index.replace("{t}", &w.table),
            t = w.table
        ))
        .await;
        match w
            .c
            .send(method_queue::SIZE, scope_req("default", None))
            .await
        {
            Outcome::Ok(_) => assert!(unique, "{tag}: served"),
            Outcome::Error(ep) => {
                assert!(!unique, "{tag}: {ep:?}");
                assert!(
                    ep.message.contains("column id is not unique"),
                    "{tag}: {ep:?}"
                );
            }
            other => panic!("{other:?}"),
        }
        w.drop_schema().await;
    }

    // An INVALID unique index (review RV3): a `CREATE UNIQUE INDEX CONCURRENTLY` that FAILED on
    // duplicate ids leaves an index in the catalog that enforces nothing — `indisvalid` is what
    // refuses it.
    let Some(mut w) = World::new("invalid", &[]).await else {
        return;
    };
    w.exec(&format!(
        "ALTER TABLE {t} DROP CONSTRAINT ferro_jobs_pkey; \
         INSERT INTO {t} (id, queue, payload, attempts, available_at, created_at) \
         VALUES (7, 'other', 'a', 0, 0, 0), (7, 'other', 'b', 0, 0, 0)",
        t = w.table
    ))
    .await;
    let failed = w
        .raw
        .batch_execute(&format!(
            "CREATE UNIQUE INDEX CONCURRENTLY ferro_jobs_id_cc ON {} (id)",
            w.table
        ))
        .await;
    assert!(
        failed.is_err(),
        "the concurrent build must fail on the duplicates"
    );
    let valid: bool = w
        .raw
        .query_one(
            &format!(
                "SELECT indisvalid FROM pg_index WHERE indexrelid = '{}.ferro_jobs_id_cc'::regclass",
                w.schema
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        !valid,
        "premise: the failed build left an INVALID index behind"
    );
    let ep =
        w.c.err(method_queue::SIZE, scope_req("default", None))
            .await;
    assert!(ep.message.contains("column id is not unique"), "{ep:?}");
    w.drop_schema().await;
}

/// Review F1: a unique index does not reach an INHERITANCE child, and every queue statement (no
/// `ONLY`) reaches the child's rows — measured, one ACK deleted the same `(id, attempts,
/// created_at)` from parent AND child. An ordinary table with inheritance children is refused before
/// any verb runs, naming the reason; the parent's own primary key does not save it. (A partitioned
/// table, whose partitions are `pg_inherits` children too, passes: `queue_g1a_it`'s
/// `a_view_is_refused_and_a_partitioned_table_passes`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_table_with_inheritance_children_is_refused() {
    let Some(mut w) = World::new("inherits", &[]).await else {
        return;
    };
    w.exec(&format!(
        "CREATE TABLE {s}.ferro_jobs_child () INHERITS ({t}); \
         INSERT INTO {s}.ferro_jobs_child (id, queue, payload, attempts, available_at, created_at) \
         VALUES (1, 'default', 'child', 0, 0, 0)",
        s = w.schema,
        t = w.table
    ))
    .await;
    for (method, body) in [
        (method_queue::SIZE, scope_req("default", None)),
        (method_queue::RESERVE, reserve_req(&["default"], 1, None)),
        (method_queue::CLEAR, scope_req("default", None)),
        (
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "{}", 0)], None),
        ),
    ] {
        let ep = w.c.err(method, body).await;
        assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
        assert!(
            ep.message.contains("has inheritance children"),
            "{method}: {}",
            ep.message
        );
    }
    // Nothing ran: the child's row is untouched and the parent is empty.
    let child: i64 = w
        .raw
        .query_one(
            &format!("SELECT count(*) FROM ONLY {}.ferro_jobs_child", w.schema),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(child, 1);
    assert_eq!(
        w.count().await,
        1,
        "the parent scan (with the child) still sees one row"
    );
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Live: the verbs
// ---------------------------------------------------------------------------------------------

/// ENQUEUE inserts `attempts = 0`, `reserved_at = NULL`, `created_at = now` (the database's) and
/// `available_at` per the delay rule; a single job returns its id; a batch returns `inserted` only,
/// with ids ascending in the request's order.
#[tokio::test]
async fn enqueue_writes_the_stock_row_and_a_batch_keeps_its_order() {
    let Some(mut w) = World::new("enq", &[]).await else {
        return;
    };
    let before = w.db_now().await;
    let r = w.c.enqueue(&[("default", "{\"a\":1}", 0)]).await;
    let after = w.db_now().await;
    assert_eq!((r.inserted, r.deduplicated), (1, false));
    let id = JobId::decode(&r.job_id.unwrap()).unwrap().0;
    let row = w.row(id).await.expect("the row exists");
    assert_eq!(row.queue, "default");
    assert_eq!(row.payload, "{\"a\":1}");
    assert_eq!(row.attempts, 0);
    assert_eq!(row.reserved_at, None);
    assert_eq!(row.available_at, row.created_at, "delay 0 → available now");
    assert!(
        (before..=after).contains(&i64::from(row.created_at)),
        "created_at is the database's now"
    );

    let r =
        w.c.enqueue(&[
            ("default", "p0", 0),
            ("emails", "p1", 5),
            ("default", "p2", 0),
        ])
        .await;
    assert_eq!((r.job_id, r.inserted), (None, 3), "a batch returns no id");
    let rows = w
        .raw
        .query(
            &format!(
                "SELECT payload, queue, available_at - created_at FROM {} WHERE id > $1 ORDER BY id",
                w.table
            ),
            &[&id],
        )
        .await
        .unwrap();
    let got: Vec<(String, String, i32)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("p0".into(), "default".into(), 0),
            ("p1".into(), "emails".into(), 6),
            ("p2".into(), "default".into(), 0)
        ],
        "ids ascend in request order; a delay d > 0 is now + 1 + d"
    );
    w.drop_schema().await;
}

/// The whole cycle: RESERVE mints the token `(created_at, attempts)` with `attempts` incremented and
/// `reserved_at = now`, `lease_deadline = reserved_at + lease + 1`; a reserved job is not reserved
/// again; ACK deletes it (`acked`), and a second ACK answers `gone`.
#[tokio::test]
async fn reserve_then_ack_and_a_second_ack_is_gone() {
    let Some(mut w) = World::new("cycle", &[]).await else {
        return;
    };
    let id = w.c.enqueue_one("default", "{}").await;
    let job = w.c.reserve_one("default").await;
    assert_eq!(id_of(&job), id);
    assert_eq!(
        job.job_id,
        JobId(id).encode(),
        "the canonical decimal job_id"
    );
    let row = w.row(id).await.unwrap();
    assert_eq!(row.attempts, 1);
    assert_eq!(job.attempts, 1);
    assert_eq!(job.created_at, i64::from(row.created_at));
    assert_eq!(
        job.token,
        Token::from_pg(row.created_at, 1).encode().to_vec()
    );
    let reserved_at = i64::from(row.reserved_at.expect("reserved"));
    assert_eq!(job.lease_deadline, reserved_at + LEASE_S + 1);
    assert_eq!(
        (job.queue.as_str(), job.payload.as_str()),
        ("default", "{}")
    );
    assert!(w.c.reserve(&["default"], 5).await.is_empty(), "held");

    assert_eq!(
        w.c.ack(&job.job_id, &job.token).await.unwrap(),
        ack_outcome::ACKED
    );
    assert!(w.row(id).await.is_none(), "deleted");
    assert_eq!(
        w.c.ack(&job.job_id, &job.token).await.unwrap(),
        ack_outcome::GONE
    );
    // An id that never existed is `gone` too.
    assert_eq!(
        w.c.ack(&JobId(id + 1000).encode(), &job.token)
            .await
            .unwrap(),
        ack_outcome::GONE
    );
    w.drop_schema().await;
}

/// The fence (§24.3): after the lease expires and ANOTHER reservation takes the job, the first
/// holder's token is stale — ACK, RELEASE and EXTEND each answer `LeaseLost` and change no row — and
/// the current holder's ACK lands.
#[tokio::test]
async fn a_stale_token_is_lease_lost_for_every_fenced_verb_and_changes_nothing() {
    let Some(mut w) = World::new("stale", &[]).await else {
        return;
    };
    let id = w.c.enqueue_one("default", "{}").await;
    let first = w.c.reserve_one("default").await;
    // Expire the lease (the row's state, as if L+1 s had passed).
    w.exec(&format!(
        "UPDATE {} SET reserved_at = reserved_at - {} WHERE id = {id}",
        w.table,
        LEASE_S + 1
    ))
    .await;
    let second = w.c.reserve_one("default").await;
    assert_eq!(id_of(&second), id, "redelivered");
    assert_eq!(second.attempts, 2);
    assert_ne!(second.token, first.token);
    let before = w.row(id).await.unwrap();

    assert_lease_lost(w.c.ack(&first.job_id, &first.token).await);
    assert_lease_lost(w.c.release(&first.job_id, &first.token, 0).await);
    assert_lease_lost(w.c.extend(&first.job_id, &first.token).await);
    // The fence's THIRD column: the current id and attempts with another `created_at` (an id reused
    // after `TRUNCATE … RESTART IDENTITY`, §24.3's precondition) is stale too.
    let row = w.row(id).await.unwrap();
    let forged = Token::from_pg(row.created_at - 1, row.attempts).encode();
    assert_lease_lost(w.c.ack(&second.job_id, &forged).await);
    assert_lease_lost(w.c.release(&second.job_id, &forged, 0).await);
    assert_lease_lost(w.c.extend(&second.job_id, &forged).await);
    assert_eq!(w.row(id).await.unwrap(), before, "no row changed");
    assert_eq!(w.count().await, 1, "RELEASE inserted nothing");

    assert_eq!(
        w.c.ack(&second.job_id, &second.token).await.unwrap(),
        ack_outcome::ACKED
    );
    w.drop_schema().await;
}

/// The widened fence (§24.3): a token is valid while its row exists with that `id`, `attempts` and
/// `created_at` — NOT while its lease runs — so a late ACK, RELEASE or EXTEND after the lease expired
/// is honoured when nobody else reserved the job.
#[tokio::test]
async fn a_late_fenced_verb_after_lease_expiry_is_honoured_when_uncontended() {
    let Some(mut w) = World::new("late", &[]).await else {
        return;
    };
    let expire = |w: &World, id: i64| {
        format!(
            "UPDATE {} SET reserved_at = reserved_at - {} WHERE id = {id}",
            w.table,
            LEASE_S + 100
        )
    };
    let a = w.c.enqueue_one("default", "a").await;
    let job = w.c.reserve_one("default").await;
    w.exec(&expire(&w, a)).await;
    assert_eq!(
        w.c.ack(&job.job_id, &job.token).await.unwrap(),
        ack_outcome::ACKED
    );
    assert!(w.row(a).await.is_none());

    let b = w.c.enqueue_one("default", "b").await;
    let job = w.c.reserve_one("default").await;
    w.exec(&expire(&w, b)).await;
    let deadline = w.c.extend(&job.job_id, &job.token).await.unwrap();
    let row = w.row(b).await.unwrap();
    assert_eq!(
        deadline,
        i64::from(row.reserved_at.unwrap()) + LEASE_S + 1,
        "EXTEND retakes the expired lease: reserved_at = now"
    );
    w.exec(&expire(&w, b)).await;
    let new_id = w.c.release(&job.job_id, &job.token, 0).await.unwrap();
    assert!(new_id.is_some(), "a late RELEASE is honoured too");
    w.drop_schema().await;
}

/// RELEASE re-inserts the job under a NEW id — to the back of the queue, as stock does — with
/// `attempts` kept, `reserved_at = NULL`, `created_at = now` and `available_at` per the delay rule,
/// and deletes the old row; a second RELEASE with the same token answers `gone` (nil).
#[tokio::test]
async fn release_sends_the_job_to_the_back_under_a_new_id() {
    let Some(mut w) = World::new("release", &[]).await else {
        return;
    };
    let a = w.c.enqueue_one("default", "a").await;
    let b = w.c.enqueue_one("default", "b").await;
    let job = w.c.reserve_one("default").await;
    assert_eq!(id_of(&job), a);
    let new_a =
        w.c.release(&job.job_id, &job.token, 0)
            .await
            .unwrap()
            .unwrap();
    assert!(new_a > b, "to the back: {new_a} after {b}");
    assert!(w.row(a).await.is_none(), "the old row is gone");
    let row = w.row(new_a).await.unwrap();
    assert_eq!(
        (row.payload.as_str(), row.attempts, row.reserved_at),
        ("a", 1, None),
        "attempts kept, unreserved"
    );
    assert_eq!(row.available_at, row.created_at, "delay 0");
    assert_eq!(
        w.c.release(&job.job_id, &job.token, 0).await.unwrap(),
        None,
        "gone"
    );
    // FIFO: b first, then the released a.
    let jobs = w.c.reserve(&["default"], 5).await;
    assert_eq!(jobs.iter().map(id_of).collect::<Vec<_>>(), vec![b, new_a]);
    // A delayed release: available_at = now + 1 + d, and not reservable before then.
    let released = &jobs[1];
    let delayed =
        w.c.release(&released.job_id, &released.token, 10)
            .await
            .unwrap()
            .unwrap();
    let row = w.row(delayed).await.unwrap();
    assert_eq!(row.available_at - row.created_at, 11);
    assert_eq!(row.attempts, 2, "the second delivery's count, kept");
    assert!(w.c.reserve(&["default"], 5).await.is_empty(), "not early");
    w.drop_schema().await;
}

/// EXTEND renews by one full lease from the database's now.
#[tokio::test]
async fn extend_renews_the_lease_from_now() {
    let Some(mut w) = World::new("extend", &[]).await else {
        return;
    };
    let id = w.c.enqueue_one("default", "{}").await;
    let job = w.c.reserve_one("default").await;
    w.exec(&format!(
        "UPDATE {} SET reserved_at = reserved_at - 10 WHERE id = {id}",
        w.table
    ))
    .await;
    let before = w.db_now().await;
    let deadline = w.c.extend(&job.job_id, &job.token).await.unwrap();
    let after = w.db_now().await;
    let reserved_at = i64::from(w.row(id).await.unwrap().reserved_at.unwrap());
    assert!((before..=after).contains(&reserved_at), "reserved_at = now");
    assert_eq!(deadline, reserved_at + LEASE_S + 1);
    // EXTEND leaves the token valid: the ACK still lands.
    assert_eq!(
        w.c.ack(&job.job_id, &job.token).await.unwrap(),
        ack_outcome::ACKED
    );
    w.drop_schema().await;
}

/// SIZE: Laravel 12's `pendingSize` / `delayedSize` / `reservedSize` and
/// `creationTimeOfOldestPendingJob` (illuminate/queue v12.69.3), per queue. CLEAR deletes the queue's
/// rows, reserved ones included, and only that queue's.
#[tokio::test]
async fn size_counts_by_state_and_clear_deletes_one_queue() {
    let Some(mut w) = World::new("size", &[]).await else {
        return;
    };
    let (_, s) = w.put("default", "p1", "0", "NULL", "s - 10").await;
    w.put("default", "p2", "0", "NULL", "s - 5").await;
    w.put("default", "d", "0", "NULL", "s + 100").await;
    w.put("default", "r", "1", "s", "s - 1").await;
    w.put("default", "expired", "1", "s - 1000", "s - 2000")
        .await;
    w.put("other", "o", "0", "NULL", "s - 50").await;
    let size = w.c.size("default").await;
    assert_eq!(
        (size.pending, size.delayed, size.reserved),
        (2, 1, 2),
        "an expired lease is still `reserved` in stock's count"
    );
    let oldest = size.oldest_pending_at.unwrap();
    assert!(
        oldest == s - 10,
        "the smallest pending available_at: {oldest} vs {}",
        s - 10
    );
    let empty = w.c.size("nothing-here").await;
    assert_eq!(
        (
            empty.pending,
            empty.delayed,
            empty.reserved,
            empty.oldest_pending_at
        ),
        (0, 0, 0, None)
    );
    assert_eq!(w.c.clear("default").await, 5);
    assert_eq!(w.count().await, 1, "the other queue is untouched");
    assert_eq!(w.c.clear("default").await, 0);
    w.drop_schema().await;
}

/// RESERVE serves its queues in priority order and answers from the FIRST queue that has a job — so a
/// reply's jobs all come from one queue — and FIFO by id within it. Each queue's statement is its own
/// unit with its own `now` (SPEC §24.3 as amended at the G1b review, F3): a reply's jobs all come from
/// ONE statement, so they share one `now` — one `lease_deadline`, one `reserved_at`.
#[tokio::test]
async fn reserve_serves_the_first_non_empty_queue_in_priority_order() {
    let Some(mut w) = World::new("prio", &[]).await else {
        return;
    };
    let d1 = w.c.enqueue_one("default", "d1").await;
    let d2 = w.c.enqueue_one("default", "d2").await;
    let jobs = w.c.reserve(&["high", "default"], 5).await;
    assert_eq!(
        jobs.iter().map(id_of).collect::<Vec<_>>(),
        vec![d1, d2],
        "high is empty, so default answers"
    );
    assert_eq!(
        jobs[0].lease_deadline, jobs[1].lease_deadline,
        "one statement, one now"
    );
    let h = w.c.enqueue_one("high", "h").await;
    let d3 = w.c.enqueue_one("default", "d3").await;
    let jobs = w.c.reserve(&["high", "default"], 5).await;
    assert_eq!(
        jobs.iter().map(id_of).collect::<Vec<_>>(),
        vec![h],
        "only the first non-empty queue, even with room for more"
    );
    let jobs = w.c.reserve(&["high", "default"], 5).await;
    assert_eq!(jobs.iter().map(id_of).collect::<Vec<_>>(), vec![d3]);
    w.drop_schema().await;
}

/// `max_jobs` is clamped so the reply fits one frame (§24.4): 3 at the 4 MiB default payload bound.
/// Rows at the attempts ceiling (smallint) and rows over `MAX_PAYLOAD_BYTES` are never reserved.
#[tokio::test]
async fn the_frame_clamp_the_attempts_ceiling_and_oversize_rows() {
    let Some(mut w) = World::new(
        "clamp",
        &[("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "4194304")],
    )
    .await
    else {
        return;
    };
    for i in 0..5 {
        w.c.enqueue_one("default", &format!("j{i}")).await;
    }
    assert_eq!(w.c.reserve(&["default"], 100).await.len(), 3, "clamped");
    assert_eq!(w.c.reserve(&["default"], 100).await.len(), 2);
    w.drop_schema().await;

    let Some(mut w) = World::new("ceiling", &[("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "8")]).await
    else {
        return;
    };
    let (at_ceiling, _) = w.put("default", "c", "32767", "NULL", "s - 1").await;
    w.put("default", "123456789", "0", "NULL", "s - 1").await; // 9 bytes > 8
    let (below, _) = w.put("default", "ok", "32766", "NULL", "s - 1").await;
    let jobs = w.c.reserve(&["default"], 10).await;
    assert_eq!(jobs.iter().map(id_of).collect::<Vec<_>>(), vec![below]);
    assert_eq!(
        jobs[0].attempts, 32_767,
        "the last reservation the column admits"
    );
    assert_eq!(
        w.row(at_ceiling).await.unwrap().attempts,
        32_767,
        "untouched"
    );
    // An oversize ENQUEUE is refused before sending.
    let ep =
        w.c.err(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "123456789", 0)], None),
        )
        .await;
    assert!(ep.message.contains("MAX_PAYLOAD_BYTES"), "{}", ep.message);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Live: the clock and the rounding rules at second boundaries
// ---------------------------------------------------------------------------------------------

/// Wait until the database's clock is early in a second, so a set-up statement and the RESERVE after
/// it usually share one second. Each boundary test then CHECKS that they did — the RESERVE's `now` is
/// derived from its reply's `lease_deadline` — and retries when they did not, so a boundary is always
/// asserted at one known `now`.
async fn early_in_a_second(raw: &tokio_postgres::Client) {
    let frac: f64 = raw
        .query_one(
            "SELECT (extract(epoch FROM clock_timestamp()) \
              - floor(extract(epoch FROM clock_timestamp())))::float8",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    if frac > 0.4 {
        tokio::time::sleep(Duration::from_secs_f64(1.02 - frac)).await;
    }
}

/// Availability (§24.3): `reserved_at IS NULL AND available_at <= now` — a row available AT `now` is
/// reserved, one available at `now + 1` is not. Lease expiry: `reserved_at < now - lease_s` — a
/// reservation exactly `lease_s` old is NOT expired, one `lease_s + 1` old is.
#[tokio::test]
async fn availability_and_lease_expiry_hold_at_the_second_boundary() {
    let Some(mut w) = World::new("boundary", &[]).await else {
        return;
    };
    let mut verdict = None;
    for _ in 0..10 {
        w.exec(&format!("DELETE FROM {}", w.table)).await;
        early_in_a_second(&w.raw).await;
        // One statement places every row relative to ITS now, `s`.
        let s: i64 = w
            .raw
            .query(
                &format!(
                    "WITH n AS (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint \
                     AS s) INSERT INTO {} (queue, payload, attempts, reserved_at, available_at, \
                     created_at) SELECT 'default', v.p, v.a, v.r::integer, v.av::integer, n.s \
                     FROM n, LATERAL (VALUES \
                     ('control', 0, NULL::bigint, n.s - 100), \
                     ('avail_now', 0, NULL, n.s), \
                     ('avail_next', 0, NULL, n.s + 1), \
                     ('lease_exactly', 1, n.s - {LEASE_S}, n.s - 500), \
                     ('lease_over', 1, n.s - {LEASE_S} - 1, n.s - 500)) AS v(p, a, r, av) \
                     RETURNING created_at::bigint",
                    w.table
                ),
                &[],
            )
            .await
            .unwrap()[0]
            .get(0);
        let jobs = w.c.reserve(&["default"], 10).await;
        let now = jobs[0].lease_deadline - LEASE_S - 1;
        if now != s {
            eprintln!("crossed a second boundary ({s} -> {now}); retrying");
            continue;
        }
        let mut got: Vec<String> = jobs.iter().map(|j| j.payload.clone()).collect();
        got.sort();
        verdict = Some(got);
        break;
    }
    assert_eq!(
        verdict.expect("set-up and RESERVE inside one database second within ten tries"),
        vec!["avail_now", "control", "lease_over"],
        "available AT now: yes; at now + 1: no; a lease exactly L old: held; L + 1 old: expired"
    );
    w.drop_schema().await;
}

/// Delay (§24.3): `available_at = now` for `delay_s = 0`, else `now + 1 + delay_s` — never early: at
/// `created_at + delay_s` (the second stock would release it) the job is still not reservable.
#[tokio::test]
async fn a_delay_is_never_early_at_the_second_boundary() {
    let Some(mut w) = World::new("delay", &[]).await else {
        return;
    };
    let mut verdict = None;
    for _ in 0..10 {
        w.exec(&format!("DELETE FROM {}", w.table)).await;
        early_in_a_second(&w.raw).await;
        let r =
            w.c.enqueue(&[("default", "control", 0), ("default", "d1", 1)])
                .await;
        assert_eq!(r.inserted, 2);
        // Move both rows one second into the past: the d1 job's `created_at + 1` (when stock would
        // release a 1 s delay) is then the RESERVE's now — still one second early under §24.3.
        let s: i64 = w
            .raw
            .query(
                &format!(
                    "UPDATE {} SET created_at = created_at - 1, available_at = available_at - 1 \
                     RETURNING floor(extract(epoch FROM statement_timestamp()))::bigint",
                    w.table
                ),
                &[],
            )
            .await
            .unwrap()[0]
            .get(0);
        let jobs = w.c.reserve(&["default"], 10).await;
        let now = jobs[0].lease_deadline - LEASE_S - 1;
        if now != s {
            eprintln!("crossed a second boundary ({s} -> {now}); retrying");
            continue;
        }
        let rows = w
            .raw
            .query(
                &format!(
                    "SELECT payload, available_at - created_at, created_at::bigint FROM {} \
                     ORDER BY id",
                    w.table
                ),
                &[],
            )
            .await
            .unwrap();
        let created: i64 = rows[1].get(2);
        if created + 1 != now {
            eprintln!("the ENQUEUE ran in an earlier second; retrying");
            continue;
        }
        let stored: Vec<(String, i32)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        verdict = Some((
            jobs.iter().map(|j| j.payload.clone()).collect::<Vec<_>>(),
            stored,
        ));
        break;
    }
    let (reserved, stored) =
        verdict.expect("set-up and RESERVE inside one database second within ten tries");
    assert_eq!(reserved, vec!["control"], "d1 is not early");
    assert_eq!(stored, vec![("control".into(), 0), ("d1".into(), 2)]);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Live: the MATERIALIZED premise under real concurrency
// ---------------------------------------------------------------------------------------------

/// SPEC §24.4's rescan premise, ASSERTED rather than assumed (F12a): the `MATERIALIZED` locking CTE
/// is evaluated once, so under concurrent reservers no statement reserves more than its `LIMIT` and
/// no job is delivered twice. 16 sessions on a 16-connection pool reserve `k = 4` at a time from 800
/// jobs until the queue is empty. Checked three ways: every reply has at most `k` jobs; the union of
/// all replies is every job exactly once; and in the ROWS every job has `attempts = 1` — a statement
/// that updated a row it did not return, or updated one twice, would show there.
///
/// **The concurrency is made real and then MEASURED, not assumed:** a `BEFORE UPDATE` trigger sleeps
/// 5 ms per row, so every reservation statement holds its row locks for about 20 ms, and the test
/// records each RESERVE's start and end and asserts that statements genuinely overlapped (the
/// largest number in flight at once) — a run in which the sessions happened to take turns would
/// prove nothing about `SKIP LOCKED` or the rescan.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reservers_never_exceed_the_limit_or_deliver_a_job_twice() {
    let Some(w) = World::new("concurrent", &[]).await else {
        return;
    };
    const JOBS: i64 = 800;
    const K: u16 = 4;
    const SESSIONS: usize = 16;
    w.exec(&format!(
        "INSERT INTO {} (queue, payload, attempts, reserved_at, available_at, created_at) \
         SELECT 'default', 'job ' || g, 0, NULL, 0, 0 FROM generate_series(1, {JOBS}) AS g; \
         CREATE FUNCTION {s}.slow() RETURNS trigger LANGUAGE plpgsql AS \
           $$ BEGIN PERFORM pg_sleep(0.005); RETURN NEW; END $$; \
         CREATE TRIGGER slow BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {s}.slow()",
        w.table,
        s = w.schema,
        t = w.table,
    ))
    .await;
    let epoch = std::time::Instant::now();
    let start = Arc::new(tokio::sync::Barrier::new(SESSIONS));
    let mut tasks = Vec::new();
    for _ in 0..SESSIONS {
        let mut c = Q::connect(&w.server).await;
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            let mut replies = Vec::new();
            let mut spans = Vec::new();
            let mut empties = 0;
            // Keep going past the first empty reply: rows locked by a concurrent statement are
            // SKIPPED, so one empty answer does not mean the queue is drained.
            // Bounded: 800 jobs need 200 full replies in all, so a session that has made 400
            // RESERVEs is looking at a queue that never drains (a reservation that does not
            // reserve) — stop, and let the assertions below say so instead of hanging.
            while empties < 3 && spans.len() < 400 {
                let started = epoch.elapsed();
                let jobs = c.reserve(&["default"], K).await;
                spans.push((started, epoch.elapsed()));
                if jobs.is_empty() {
                    empties += 1;
                } else {
                    replies.push(jobs.iter().map(id_of).collect::<Vec<_>>());
                }
            }
            (replies, spans)
        }));
    }
    let mut seen = HashSet::new();
    let mut replies = 0;
    let mut partial = 0;
    let mut events = Vec::new();
    for t in tasks {
        let (session_replies, spans) = t.await.unwrap();
        for (a, b) in spans {
            events.push((a, 1i32));
            events.push((b, -1i32));
        }
        for reply in session_replies {
            if reply.len() < usize::from(K) {
                partial += 1;
            }
            replies += 1;
            assert!(
                reply.len() <= usize::from(K),
                "a reply exceeded its LIMIT: {reply:?}"
            );
            for id in reply {
                assert!(seen.insert(id), "job {id} delivered twice");
            }
        }
    }
    assert_eq!(seen.len() as i64, JOBS, "every job delivered");
    let (n, sum, max): (i64, i64, i16) = {
        let r = w
            .raw
            .query_one(
                &format!(
                    "SELECT count(*), sum(attempts)::bigint, max(attempts) FROM {}",
                    w.table
                ),
                &[],
            )
            .await
            .unwrap();
        (r.get(0), r.get(1), r.get(2))
    };
    assert_eq!(
        (n, sum, max),
        (JOBS, JOBS, 1),
        "each row reserved exactly once"
    );
    events.sort();
    let (mut in_flight, mut peak) = (0, 0);
    for (_, d) in events {
        in_flight += d;
        peak = peak.max(in_flight);
    }
    eprintln!(
        "{replies} replies ({partial} short of k) across {SESSIONS} sessions; peak {peak} RESERVEs \
         in flight at once"
    );
    assert!(
        peak >= 8,
        "the reservers must really have run concurrently (peak in flight {peak})"
    );
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Live: fate (§24.6) — a statement sent, then timed out or cancelled
// ---------------------------------------------------------------------------------------------

/// Hold an ACCESS EXCLUSIVE lock on the table from a raw connection, so every verb's statement is
/// SENT and then blocks. Returns the holding connection; dropping out of its transaction releases it.
async fn lock_table(w: &World) -> tokio_postgres::Client {
    let holder = raw_connect(&w.url).await;
    holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
            w.table
        ))
        .await
        .unwrap();
    holder
}

/// Every write verb (ENQUEUE, RESERVE, ACK, RELEASE, EXTEND, CLEAR) whose statement was sent and then
/// timed out is `Indeterminate{WriteUnconfirmed}` — §24.6, with the §19.3 `57014` override — and SIZE,
/// the one read, is `Cancelled`, never `Indeterminate`. The read-back proves the engine re-sent
/// nothing (charter rule 3): after the lock is released, no write applied.
#[tokio::test]
async fn a_sent_then_timed_out_verb_is_indeterminate_for_writes_and_cancelled_for_size() {
    let Some(mut w) = World::new("fate", &[]).await else {
        return;
    };
    // Verify the store and leave one reserved job to fence against.
    let id = w.c.enqueue_one("default", "{}").await;
    let job = w.c.reserve_one("default").await;
    let before = w.row(id).await.unwrap();
    let holder = lock_table(&w).await;
    let t = Some(300);
    let token = &job.token;
    for (method, payload) in [
        (
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "x", 0)], t),
        ),
        (method_queue::RESERVE, reserve_req(&["default"], 1, t)),
        (method_queue::ACK, fenced_req(&job.job_id, token, t)),
        (
            method_queue::RELEASE,
            release_req_t(&job.job_id, token, 0, t),
        ),
        (method_queue::EXTEND, fenced_req(&job.job_id, token, t)),
        (method_queue::CLEAR, scope_req("default", t)),
    ] {
        let ep = w.c.err(method, payload).await;
        assert_eq!(
            (ep.code, ep.branch),
            (errc::WRITE_UNCONFIRMED, branch::INDETERMINATE),
            "method {method}: {ep:?}"
        );
    }
    let ep = w.c.err(method_queue::SIZE, scope_req("default", t)).await;
    assert_eq!(
        (ep.code, ep.branch),
        (errc::CANCELLED, errc::CANCELLED_BRANCH),
        "{ep:?}"
    );
    holder.batch_execute("ROLLBACK").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        w.count().await,
        1,
        "no ENQUEUE applied, CLEAR deleted nothing"
    );
    assert_eq!(w.row(id).await.unwrap(), before, "no fenced verb applied");
    assert_session_alive(&mut w.c.c, 77).await;
    w.drop_schema().await;
}

/// A per-request CANCEL on a sent RESERVE is the same unconfirmed fate (§24.6: "unconfirmed cancel"),
/// and RELEASE likewise; a CANCEL on SIZE is `Cancelled`.
#[tokio::test]
async fn a_cancelled_sent_verb_follows_the_same_fate() {
    let Some(mut w) = World::new("cancel", &[]).await else {
        return;
    };
    let id = w.c.enqueue_one("default", "{}").await;
    let job = w.c.reserve_one("default").await;
    let holder = lock_table(&w).await;
    for (method, payload, indeterminate) in [
        (
            method_queue::RESERVE,
            reserve_req(&["default"], 1, None),
            true,
        ),
        (
            method_queue::RELEASE,
            release_req(&job.job_id, &job.token, 0),
            true,
        ),
        (method_queue::SIZE, scope_req("default", None), false),
    ] {
        w.c.rid += 1;
        let rid = w.c.rid;
        w.c.c
            .send_request(rid, service::QUEUE, method, payload)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        w.c.c.cancel(rid).await;
        let Outcome::Error(ep) = w.c.terminal(rid, method).await else {
            panic!("method {method}: expected an error terminal");
        };
        if indeterminate {
            assert_eq!(ep.branch, branch::INDETERMINATE, "method {method}: {ep:?}");
        } else {
            assert_eq!(ep.code, errc::CANCELLED, "{ep:?}");
        }
    }
    holder.batch_execute("ROLLBACK").await.unwrap();
    let row = w.row(id).await.unwrap();
    assert_eq!(row.attempts, 1, "the cancelled RESERVE re-leased nothing");
    assert_eq!(w.count().await, 1, "the cancelled RELEASE inserted nothing");
    w.drop_schema().await;
}

/// A verb whose checkout cannot complete before the request's deadline is a known non-execution
/// (`PoolTimeout`), never `Indeterminate`: nothing was sent. The store is verified first; then every
/// connection of the pool is held, so the verb's own checkout is what times out.
#[tokio::test]
async fn an_unsent_write_is_never_indeterminate() {
    let Some(mut w) = World::new("unsent", &[]).await else {
        return;
    };
    w.c.size("default").await; // verified
    let ferrod::pools::AnyPool::Pg(pool) = w.registry.get("default").unwrap() else {
        unreachable!()
    };
    let mut held = Vec::new();
    while let Ok(Ok(co)) = tokio::time::timeout(Duration::from_millis(500), pool.checkout()).await {
        held.push(co);
    }
    assert!(!held.is_empty());
    for (method, payload) in [
        (
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "x", 0)], Some(200)),
        ),
        (
            method_queue::RESERVE,
            reserve_req(&["default"], 1, Some(200)),
        ),
        (method_queue::CLEAR, scope_req("default", Some(200))),
    ] {
        let ep = w.c.err(method, payload).await;
        assert_eq!(
            (ep.code, ep.branch),
            (errc::POOL_TIMEOUT, errc::POOL_TIMEOUT_BRANCH),
            "method {method}: {ep:?}"
        );
    }
    // A CANCEL while the verb still waits for its connection is the same known non-execution: a
    // write that was never dispatched is Retryable, never `Indeterminate`.
    w.c.rid += 1;
    let rid = w.c.rid;
    w.c.c
        .send_request(
            rid,
            service::QUEUE,
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "x", 0)], None),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    w.c.c.cancel(rid).await;
    let Outcome::Error(ep) = w.c.terminal(rid, method_queue::ENQUEUE).await else {
        panic!("expected an error terminal");
    };
    assert_eq!(
        (ep.code, ep.branch),
        (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
        "{ep:?}"
    );
    drop(held);
    assert_eq!(w.count().await, 0);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Live: mixed mode with stock Laravel DatabaseQueue (§24.3)
// ---------------------------------------------------------------------------------------------

/// The PHP clock stock stamps with (`InteractsWithTime::currentTime()`, Unix seconds).
fn php_now() -> i32 {
    i32::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

/// Mixed mode (§24.3, the explicit opt-in `TABLE=jobs`): stock `DatabaseQueue` and Ferro on ONE table.
/// The stock side runs laravel/framework v11.51.0's EXACT statements, captured through `pdo_pgsql`
/// (`DatabaseQueue::pushToDatabase`'s `insertGetId`; `pop()`'s transaction with
/// `getNextAvailableJob`'s `FOR UPDATE SKIP LOCKED` and `markJobAsReserved`), on a raw connection as a
/// stock worker on PDO would. A stock push is consumed by a Ferro RESERVE and acked; a Ferro ENQUEUE is
/// popped by stock and is then NOT reserved by Ferro while stock's lease holds.
#[tokio::test]
async fn mixed_mode_stock_push_is_reserved_by_ferro_and_the_reverse() {
    let Some(mut w) =
        World::with_table("mixed", "jobs", &[("FERRO_QUEUE_JOBS_LEASE_S", "90")]).await
    else {
        return;
    };
    let mut stock = raw_connect(&w.url).await;
    stock
        .batch_execute(&format!("SET search_path = {}", w.schema))
        .await
        .unwrap();
    // pushRaw('{"stock":"push"}'): insert into "jobs" (...) values (?, ?, ?, ?, ?, ?) returning "id".
    let now = php_now();
    let stock_id: i64 = stock
        .query_one(
            "insert into \"jobs\" (\"queue\", \"attempts\", \"reserved_at\", \"available_at\", \
             \"created_at\", \"payload\") values ($1, $2, $3, $4, $5, $6) returning \"id\"",
            &[
                &"default",
                &0i16,
                &None::<i32>,
                &now,
                &now,
                &"{\"stock\":\"push\"}",
            ],
        )
        .await
        .unwrap()
        .get(0);
    let job = w.c.reserve_one("default").await;
    assert_eq!(id_of(&job), stock_id, "Ferro reserves stock's job");
    assert_eq!(
        job.job_id,
        stock_id.to_string().into_bytes(),
        "stock's digits"
    );
    assert_eq!(
        (job.attempts, job.payload.as_str()),
        (1, "{\"stock\":\"push\"}")
    );
    assert_eq!(
        w.c.ack(&job.job_id, &job.token).await.unwrap(),
        ack_outcome::ACKED
    );
    assert_eq!(w.count().await, 0);

    // The reverse: Ferro enqueues, stock pops.
    let ferro_id = w.c.enqueue_one("default", "{\"ferro\":1}").await;
    let mut popped = None;
    for _ in 0..3 {
        let now = php_now();
        let tx = stock.transaction().await.unwrap();
        let row = tx
            .query_opt(
                "select * from \"jobs\" where \"queue\" = $1 and ((\"reserved_at\" is null and \
                 \"available_at\" <= $2) or (\"reserved_at\" <= $3)) order by \"id\" asc limit 1 \
                 FOR UPDATE SKIP LOCKED",
                &[&"default", &now, &(now - 90)],
            )
            .await
            .unwrap();
        if let Some(row) = row {
            let id: i64 = row.get("id");
            let attempts: i16 = row.get("attempts");
            tx.execute(
                "update \"jobs\" set \"reserved_at\" = $1, \"attempts\" = $2 where \"id\" = $3",
                &[&now, &(attempts + 1), &id],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            popped = Some((id, attempts + 1, row.get::<_, String>("payload")));
            break;
        }
        tx.rollback().await.unwrap();
        // A stock worker sleeps and polls again (the PHP clock can trail the database's second).
        tokio::time::sleep(Duration::from_millis(1_100)).await;
    }
    assert_eq!(
        popped,
        Some((ferro_id, 1, "{\"ferro\":1}".to_string())),
        "stock pops Ferro's job"
    );
    assert!(
        w.c.reserve(&["default"], 5).await.is_empty(),
        "stock's lease holds against Ferro"
    );
    // Stock's deleteReserved: a plain delete by id.
    stock
        .execute("delete from \"jobs\" where \"id\" = $1", &[&ferro_id])
        .await
        .unwrap();
    assert_eq!(w.count().await, 0);
    w.drop_schema().await;
}

/// The same two directions through REAL stock Laravel — `Illuminate\Queue\DatabaseQueue` on
/// `pdo_pgsql` — rather than its replayed statements. Not run by CI (the Rust lane has no Laravel
/// vendor tree); run it by hand with
/// `FERRO_TEST_LARAVEL_AUTOLOAD=<path to a laravel/framework vendor/autoload.php>` and `--ignored`.
#[tokio::test]
#[ignore = "needs php + a laravel/framework vendor tree (FERRO_TEST_LARAVEL_AUTOLOAD)"]
async fn mixed_mode_with_real_stock_laravel() {
    let autoload = std::env::var("FERRO_TEST_LARAVEL_AUTOLOAD")
        .expect("FERRO_TEST_LARAVEL_AUTOLOAD must point at a laravel/framework autoload.php");
    let url = pg_url().expect("FERRO_TEST_PG_URL");
    let Some(mut w) =
        World::with_table("realstock", "jobs", &[("FERRO_QUEUE_JOBS_LEASE_S", "90")]).await
    else {
        return;
    };
    let parsed: tokio_postgres::Config = url.parse().unwrap();
    let host = match &parsed.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(h) => h.clone(),
        other => panic!("{other:?}"),
    };
    let script = format!(
        r#"<?php
require {autoload:?};
$c = new Illuminate\Database\Capsule\Manager();
$c->addConnection(['driver' => 'pgsql', 'host' => {host:?}, 'port' => {port},
  'database' => {db:?}, 'username' => {user:?}, 'password' => {pass:?}, 'search_path' => {schema:?}]);
$q = new Illuminate\Queue\DatabaseQueue($c->getConnection(), 'jobs', 'default', 90);
$q->setContainer(new Illuminate\Container\Container());
if ($argv[1] === 'push') {{ echo $q->pushRaw('{{"stock":"real"}}'); }}
else {{ $j = $q->pop(); echo $j === null ? 'none' : $j->getJobId() . ' ' . $j->attempts() . ' ' . $j->getRawBody(); if ($j) {{ $j->delete(); }} }}
"#,
        port = parsed.get_ports()[0],
        db = parsed.get_dbname().unwrap(),
        user = parsed.get_user().unwrap(),
        pass = String::from_utf8_lossy(parsed.get_password().unwrap()),
        schema = w.schema,
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stock.php");
    std::fs::write(&path, script).unwrap();
    let php = |arg: &str| {
        let out = std::process::Command::new("php")
            .arg(&path)
            .arg(arg)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    let stock_id: i64 = php("push").trim().parse().unwrap();
    let job = w.c.reserve_one("default").await;
    assert_eq!((id_of(&job), job.attempts), (stock_id, 1));
    assert_eq!(job.payload, "{\"stock\":\"real\"}");
    assert_eq!(
        w.c.ack(&job.job_id, &job.token).await.unwrap(),
        ack_outcome::ACKED
    );

    let ferro_id = w.c.enqueue_one("default", "{\"ferro\":1}").await;
    let mut out = php("pop");
    if out == "none" {
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        out = php("pop");
    }
    assert_eq!(
        out,
        format!("{ferro_id} 1 {{\"ferro\":1}}"),
        "stock pops Ferro's job"
    );
    assert_eq!(w.count().await, 0, "and stock's delete() removed it");
    w.drop_schema().await;
}
