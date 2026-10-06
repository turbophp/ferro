//! M7-G2: Ferro Queue's TRANSACTIONAL path on PostgreSQL, end to end through real sessions (SPEC
//! §24.5, §24.6; chaos rows 2 and 7 of §24.13).
//!
//! Live against PostgreSQL (`FERRO_TEST_PG_URL`; each test skips, and the CI no-skip gate fails, when
//! it is unset). Each test owns a fresh schema holding Laravel's stock `jobs.stub` table and a
//! `business` table standing for the application's own writes, plus a RAW side connection (outside
//! `ferrod`'s pool) that sets rows up, holds locks, watches `pg_stat_activity` and reads back — every
//! claim about what a verb did is checked against the ROWS (§24.2 I1), not only the terminal.
//!
//! `ferrod`'s pool connects with a per-test `application_name`, so a test can prove a statement is IN
//! FLIGHT (a backend of THAT pool waiting on a lock, or sleeping inside a COMMIT) before it acts —
//! never a fixed-delay guess (`chaos_fate_it.rs`'s discipline).
//!
//! What is proven, by section:
//!
//! - **atomicity both ways:** an in-transaction ENQUEUE is invisible until COMMIT and gone after
//!   ROLLBACK; ACK, RELEASE, EXTEND and CLEAR inside a transaction commit or roll back WITH the
//!   business write; the wake hint fires only on COMMIT (§24.5 step 4), a `ROLLBACK_TO` leaves a stale
//!   one, and a failed verb keeps none;
//! - **in-transaction `LeaseLost` (R1):** a stale token, AND an absent row, is `LeaseLost` for every
//!   fenced verb (never `gone`), the transaction stays open; the autocommit controls answer `gone`;
//! - **refusals before anything is sent:** `PoolMismatch` (against a pool nobody listens on, so an
//!   answer that is the refusal itself proves no checkout), a tx-scoped RESERVE, an unknown, foreign or
//!   tombstoned `tx_id` — each leaving an open transaction usable;
//! - **fate (§24.6):** a timed-out or CANCELled in-transaction verb rolls the transaction back and
//!   tombstones it (`TxDeadline{Retryable}`), with a read-back proving nothing applied and nothing was
//!   re-sent; a statement error is the known fate it is; session death rolls back;
//! - **chaos row 2:** the backend link killed under an in-transaction ENQUEUE (`Retryable`, neither
//!   row) and during its COMMIT (`Indeterminate`, both or neither); a REAL `ferrod` process SIGKILLed
//!   before COMMIT (neither row) and during COMMIT (both or neither, every iteration);
//! - **chaos row 7:** transactional ack under induced lease loss — racing holders, an absent row, a
//!   rolled-back second holder, and REPEATABLE READ's `40001` — business rows per job ≤ 1, and the
//!   stale holder never commits.

mod common;

use std::ffi::OsString;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{TestClient, TestServer, assert_session_alive, pg_url};
use ferro_proto::consts::{
    ack_outcome, branch, errc, flags, method_queue, method_sql, method_tx, service,
};
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::{
    AckResponse, BeginRequest, BeginResponse, ClearResponse, EnqueueJob, EnqueueRequest,
    EnqueueResponse, ErrorPayload, ExtendResponse, FencedRequest, Isolation, Outcome, QueueCommon,
    QueueScopeRequest, ReleaseRequest, ReleaseResponse, ReserveRequest, ReserveResponse,
    ReservedJob, SavepointRequest, SizeResponse, TxControl,
};
use ferro_proto::value::Value;
use ferro_queue::sql::JobId;
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;

/// A port nothing listens on: any dial is refused at once.
const DEAD_PG: &str = "postgres://ferro:ferro@127.0.0.1:1/ferro";

static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn next_tag(tag: &str) -> String {
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("g2_{tag}_{}_{n}", std::process::id())
}

/// `url` with `application_name=<app>` appended, so the pool's backends can be found in
/// `pg_stat_activity`.
fn with_app(url: &str, app: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}application_name={app}")
}

fn pool(name: &str, dsn: &str) -> PoolSpec {
    PoolSpec {
        name: name.into(),
        dsn: dsn.into(),
        kind: ferrod::config::infer_pool_kind(dsn),
        pin_functions: Vec::new(),
        pin_on_unknown: true,
        allow_dir: None,
    }
}

fn queue_server(pools: Vec<PoolSpec>, vars: &[(&str, &str)]) -> (TestServer, Arc<PoolRegistry>) {
    let mut config = Config {
        pools,
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

/// Laravel's `jobs.stub` on PostgreSQL, plus the application's `business` table. `business` has NO
/// unique constraint, so "≤ 1 business row per job" is proven by the queue's fence, not a constraint.
fn schema_sql(schema: &str) -> String {
    format!(
        "CREATE SCHEMA {schema}; \
         CREATE TABLE {schema}.ferro_jobs (id bigserial PRIMARY KEY, queue varchar(255) NOT NULL, \
         payload text NOT NULL, attempts smallint NOT NULL, reserved_at integer NULL, \
         available_at integer NOT NULL, created_at integer NOT NULL); \
         CREATE INDEX ON {schema}.ferro_jobs (queue); \
         CREATE TABLE {schema}.business (job_id bigint NOT NULL, worker text NOT NULL)"
    )
}

/// A deferred constraint trigger that sleeps 300 ms INSIDE every COMMIT that inserted a business
/// row: the window a "during COMMIT" chaos event is proven to land in.
fn slow_commit_sql(schema: &str) -> String {
    format!(
        "CREATE FUNCTION {schema}.slow_commit() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_sleep(0.3); RETURN NULL; END $$; \
         CREATE CONSTRAINT TRIGGER slow_commit AFTER INSERT ON {schema}.business \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {schema}.slow_commit()"
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

/// One live test's world.
struct World {
    _server: TestServer,
    registry: Arc<PoolRegistry>,
    server_sock: std::path::PathBuf,
    c: Q,
    raw: tokio_postgres::Client,
    url: String,
    /// The `application_name` of `ferrod`'s `default` pool.
    app: String,
    schema: String,
}

impl World {
    async fn new(tag: &str) -> Option<World> {
        World::with_pools(tag, |_, _| Vec::new(), &[]).await
    }

    /// `more(url, app)` adds pools beside `default`; `vars` add queue keys.
    async fn with_pools(
        tag: &str,
        more: impl FnOnce(&str, &str) -> Vec<PoolSpec>,
        vars: &[(&str, &str)],
    ) -> Option<World> {
        let url = pg_url()?;
        let schema = next_tag(tag);
        let app = schema.clone();
        let raw = raw_connect(&url).await;
        raw.batch_execute(&schema_sql(&schema)).await.unwrap();
        let table = format!("{schema}.ferro_jobs");
        let mut all = vec![
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "default"),
            ("FERRO_QUEUE_JOBS_TABLE", table.as_str()),
            ("FERRO_QUEUE_JOBS_LEASE_S", "30"),
            ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "65536"),
        ];
        all.retain(|(k, _)| !vars.iter().any(|(e, _)| e == k));
        all.extend_from_slice(vars);
        let mut pools = vec![pool("default", &with_app(&url, &app))];
        pools.extend(more(&url, &app));
        let (server, registry) = queue_server(pools, &all);
        let c = Q::connect(&server).await;
        Some(World {
            server_sock: server.socket_path().to_path_buf(),
            _server: server,
            registry,
            c,
            raw,
            url,
            app,
            schema,
        })
    }

    async fn session(&self) -> Q {
        Q::over(common::connect(&self.server_sock).await).await
    }

    async fn drop_schema(self) {
        self.raw
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }

    fn table(&self) -> String {
        format!("{}.ferro_jobs", self.schema)
    }

    fn hints(&self) -> u64 {
        self.registry.queue().unwrap().wake_hints()
    }

    async fn scalar(&self, sql: &str) -> i64 {
        self.raw.query_one(sql, &[]).await.unwrap().get(0)
    }

    async fn jobs(&self) -> i64 {
        self.scalar(&format!("SELECT count(*) FROM {}", self.table()))
            .await
    }

    async fn jobs_with_payload(&self, payload: &str) -> i64 {
        self.raw
            .query_one(
                &format!("SELECT count(*) FROM {} WHERE payload = $1", self.table()),
                &[&payload],
            )
            .await
            .unwrap()
            .get(0)
    }

    async fn business(&self, job_id: i64) -> Vec<String> {
        self.raw
            .query(
                &format!(
                    "SELECT worker FROM {}.business WHERE job_id = $1",
                    self.schema
                ),
                &[&job_id],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// `(attempts, reserved_at)` of job `id`, or `None` when the row is gone.
    async fn row(&self, id: i64) -> Option<(i16, Option<i32>, i32, i32)> {
        self.raw
            .query_opt(
                &format!(
                    "SELECT attempts, reserved_at, available_at, created_at FROM {} WHERE id = $1",
                    self.table()
                ),
                &[&id],
            )
            .await
            .unwrap()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    }

    /// Move job `id`'s lease 60 s into the past: the lease has expired (§24.3), so the next RESERVE
    /// takes the job — the induced lease loss of chaos row 7.
    async fn expire(&self, id: i64) {
        self.raw
            .execute(
                &format!(
                    "UPDATE {} SET reserved_at = reserved_at - 60 WHERE id = $1",
                    self.table()
                ),
                &[&id],
            )
            .await
            .unwrap();
    }

    /// Wait (bounded) for a backend of `ferrod`'s pool matching `cond`, and return its pid.
    async fn wait_for_backend(&self, cond: &str) -> i32 {
        wait_for_backend(&self.raw, &self.app, cond).await
    }

    /// Hold an ACCESS EXCLUSIVE lock on the jobs table, so a verb's statement is SENT and then blocks.
    async fn lock_jobs(&self) -> tokio_postgres::Client {
        let holder = raw_connect(&self.url).await;
        holder
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
                self.table()
            ))
            .await
            .unwrap();
        holder
    }

    /// Wait until no backend of `ferrod`'s pool is inside a transaction any more.
    async fn wait_no_open_tx(&self) {
        wait_no_open_tx(&self.raw, &self.app).await;
    }
}

async fn wait_for_backend(raw: &tokio_postgres::Client, app: &str, cond: &str) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(r) = raw
            .query_opt(
                &format!(
                    "SELECT pid FROM pg_stat_activity WHERE application_name = $1 AND {cond} LIMIT 1"
                ),
                &[&app],
            )
            .await
            .unwrap()
        {
            return r.get(0);
        }
        assert!(Instant::now() < deadline, "no backend matched {cond}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until no backend of the pool `app` is in a transaction (idle in it, or running a statement
/// of one): a rolled-back or killed transaction is really gone from the server, so a read-back that
/// sees no rows proves ROLLBACK, not merely invisibility.
async fn wait_no_open_tx(raw: &tokio_postgres::Client, app: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let n: i64 = raw
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 \
                 AND xact_start IS NOT NULL",
                &[&app],
            )
            .await
            .unwrap()
            .get(0);
        if n == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "{n} transaction(s) still open");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

const LOCK_WAIT: &str = "wait_event_type = 'Lock'";
const IN_COMMIT: &str = "query = 'COMMIT' AND state = 'active' AND wait_event = 'PgSleep'";

/// A session plus its next request id.
struct Q {
    c: TestClient,
    rid: u32,
}

impl Q {
    async fn connect(server: &TestServer) -> Q {
        Q::over(server.connect().await).await
    }

    async fn over(mut c: TestClient) -> Q {
        c.hello(1).await;
        Q { c, rid: 10 }
    }

    fn next(&mut self) -> u32 {
        self.rid += 1;
        self.rid
    }

    /// Send one request and read back its ONE terminal, asserting the frame shape.
    async fn call(&mut self, svc: u16, method: u16, payload: Vec<u8>) -> Outcome {
        let rid = self.next();
        self.c.send_request(rid, svc, method, payload).await;
        self.terminal(rid, svc, method).await
    }

    async fn terminal(&mut self, rid: u32, svc: u16, method: u16) -> Outcome {
        let t = self.c.recv().await;
        assert_eq!(t.header.request_id, rid);
        assert_eq!(t.header.flags, flags::END, "exactly one END, nothing else");
        assert_eq!((t.header.service, t.header.method), (svc, method));
        Outcome::decode(&t.payload).expect("a terminal Outcome")
    }

    async fn queue(&mut self, method: u16, payload: Vec<u8>) -> Result<Vec<u8>, ErrorPayload> {
        match self.call(service::QUEUE, method, payload).await {
            Outcome::Ok(b) => Ok(b),
            Outcome::Error(ep) => Err(ep),
            other => panic!("{other:?}"),
        }
    }

    async fn begin(&mut self, isolation: Option<u8>, readonly: bool) -> u64 {
        self.begin_on("default", isolation, readonly).await
    }

    async fn begin_on(&mut self, pool: &str, isolation: Option<u8>, readonly: bool) -> u64 {
        let req = BeginRequest {
            pool: pool.into(),
            isolation,
            readonly,
        };
        match self.call(service::TX, method_tx::BEGIN, req.encode()).await {
            Outcome::Ok(b) => BeginResponse::decode(&b).unwrap().tx_id,
            other => panic!("BEGIN: {other:?}"),
        }
    }

    async fn ctl(&mut self, tx_id: u64, method: u16) -> Outcome {
        self.call(service::TX, method, TxControl { tx_id }.encode())
            .await
    }

    async fn commit(&mut self, tx_id: u64) {
        let o = self.ctl(tx_id, method_tx::COMMIT).await;
        assert!(matches!(o, Outcome::Ok(_)), "COMMIT: {o:?}");
    }

    async fn rollback(&mut self, tx_id: u64) {
        let o = self.ctl(tx_id, method_tx::ROLLBACK).await;
        assert!(matches!(o, Outcome::Ok(_)), "ROLLBACK: {o:?}");
    }

    async fn savepoint(&mut self, tx_id: u64, method: u16) {
        let req = SavepointRequest {
            tx_id,
            name: Some("s".into()),
        };
        let o = self.call(service::TX, method, req.encode()).await;
        assert!(matches!(o, Outcome::Ok(_)), "savepoint {method}: {o:?}");
    }

    async fn exec_tx(&mut self, tx_id: u64, sql: &str, params: Vec<Value>) -> Outcome {
        let req = ExecRequest {
            pool: "default".into(),
            sql: Some(sql.into()),
            query_id: None,
            params,
            timeout_ms: None,
            readonly: false,
            fetch: 0,
            tx_id: Some(tx_id),
            traceparent: None,
        };
        self.call(service::SQL, method_sql::EXEC, req.encode())
            .await
    }

    /// The application's own write inside the transaction.
    async fn business(&mut self, tx_id: u64, schema: &str, job_id: i64, worker: &str) {
        let o = self
            .exec_tx(
                tx_id,
                &format!("INSERT INTO {schema}.business (job_id, worker) VALUES ($1, $2)"),
                vec![Value::I64(job_id), Value::Text(worker.into())],
            )
            .await;
        assert!(matches!(o, Outcome::Ok(_)), "business write: {o:?}");
    }

    async fn enqueue(
        &mut self,
        jobs: &[(&str, &str, u32)],
        tx: Option<u64>,
    ) -> Result<EnqueueResponse, ErrorPayload> {
        self.queue(method_queue::ENQUEUE, enqueue_req(jobs, tx, None))
            .await
            .map(|b| EnqueueResponse::decode(&b).unwrap())
    }

    async fn enqueue_one(&mut self, queue: &str, payload: &str, tx: Option<u64>) -> i64 {
        let r = self.enqueue(&[(queue, payload, 0)], tx).await.unwrap();
        JobId::decode(&r.job_id.expect("a single job's id"))
            .unwrap()
            .0
    }

    async fn reserve_one(&mut self, queue: &str) -> ReservedJob {
        let body = self
            .queue(method_queue::RESERVE, reserve_req(queue, None))
            .await
            .unwrap();
        let mut jobs = ReserveResponse::decode(&body).unwrap().jobs;
        assert_eq!(jobs.len(), 1, "expected one job");
        jobs.pop().unwrap()
    }

    async fn ack(&mut self, job: &ReservedJob, tx: Option<u64>) -> Result<u8, ErrorPayload> {
        self.queue(method_queue::ACK, fenced_req(job, tx, None))
            .await
            .map(|b| AckResponse::decode(&b).unwrap().outcome)
    }

    async fn release(
        &mut self,
        job: &ReservedJob,
        delay_s: u32,
        tx: Option<u64>,
    ) -> Result<Option<i64>, ErrorPayload> {
        self.queue(method_queue::RELEASE, release_req(job, delay_s, tx))
            .await
            .map(|b| {
                ReleaseResponse::decode(&b)
                    .unwrap()
                    .new_job_id
                    .map(|id| JobId::decode(&id).unwrap().0)
            })
    }

    async fn extend(&mut self, job: &ReservedJob, tx: Option<u64>) -> Result<i64, ErrorPayload> {
        self.queue(method_queue::EXTEND, fenced_req(job, tx, None))
            .await
            .map(|b| ExtendResponse::decode(&b).unwrap().lease_deadline)
    }
}

fn common_of(tx: Option<u64>, timeout_ms: Option<u32>) -> QueueCommon {
    QueueCommon {
        tx_id: tx,
        timeout_ms,
        traceparent: None,
    }
}

fn enqueue_req(jobs: &[(&str, &str, u32)], tx: Option<u64>, timeout_ms: Option<u32>) -> Vec<u8> {
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
        common: common_of(tx, timeout_ms),
    }
    .encode()
}

fn enqueue_req_store(store: &str, tx: Option<u64>) -> Vec<u8> {
    EnqueueRequest {
        store: store.into(),
        jobs: vec![EnqueueJob {
            queue: "default".into(),
            payload: "{}".into(),
            delay_s: 0,
        }],
        dedup_key: None,
        common: common_of(tx, None),
    }
    .encode()
}

fn reserve_req(queue: &str, tx: Option<u64>) -> Vec<u8> {
    ReserveRequest {
        store: "jobs".into(),
        queues: vec![queue.into()],
        max_jobs: 1,
        wait_ms: 0,
        liveness: false,
        common: common_of(tx, None),
    }
    .encode()
}

fn fenced_req(job: &ReservedJob, tx: Option<u64>, timeout_ms: Option<u32>) -> Vec<u8> {
    FencedRequest {
        store: "jobs".into(),
        job_id: job.job_id.clone(),
        token: job.token.clone(),
        common: common_of(tx, timeout_ms),
    }
    .encode()
}

fn release_req(job: &ReservedJob, delay_s: u32, tx: Option<u64>) -> Vec<u8> {
    ReleaseRequest {
        store: "jobs".into(),
        job_id: job.job_id.clone(),
        token: job.token.clone(),
        delay_s,
        common: common_of(tx, None),
    }
    .encode()
}

fn scope_req(queue: &str, tx: Option<u64>) -> Vec<u8> {
    QueueScopeRequest {
        store: "jobs".into(),
        queue: queue.into(),
        common: common_of(tx, None),
    }
    .encode()
}

fn id_of(job: &ReservedJob) -> i64 {
    JobId::decode(&job.job_id).unwrap().0
}

fn assert_code(r: Result<impl std::fmt::Debug, ErrorPayload>, code: u16, br: u8) -> ErrorPayload {
    let ep = r.expect_err("expected an error terminal");
    assert_eq!((ep.code, ep.branch), (code, br), "{ep:?}");
    ep
}

fn assert_lease_lost_in_tx(r: Result<impl std::fmt::Debug, ErrorPayload>) {
    let ep = assert_code(r, errc::LEASE_LOST, errc::LEASE_LOST_BRANCH);
    assert!(ep.message.contains("roll it back"), "{}", ep.message);
}

fn assert_tx_deadline(ep: &ErrorPayload) {
    assert_eq!(
        (ep.code, ep.branch),
        (errc::TX_DEADLINE, errc::TX_DEADLINE_BRANCH),
        "{ep:?}"
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
}

fn outcome_err(o: Outcome) -> ErrorPayload {
    match o {
        Outcome::Error(ep) => ep,
        other => panic!("expected an error terminal, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Atomicity both ways (§24.5)
// ---------------------------------------------------------------------------------------------

/// The outbox, made structural: an in-transaction ENQUEUE (single and batch) is invisible to everyone
/// else until COMMIT, visible with the business write after it, and gone with it after ROLLBACK. The
/// transaction itself sees its own jobs (SIZE in the transaction). The wake hint fires only at
/// COMMIT — one per distinct queue — and never on ROLLBACK.
#[tokio::test]
async fn an_in_tx_enqueue_commits_with_the_business_write_and_vanishes_on_rollback() {
    let Some(mut w) = World::new("enq").await else {
        return;
    };
    let s = w.schema.clone();
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, 1, "app").await;
    let id = w.c.enqueue_one("default", "one", Some(tx)).await;
    let batch =
        w.c.enqueue(&[("a", "x", 0), ("b", "y", 5)], Some(tx))
            .await
            .unwrap();
    assert_eq!((batch.job_id, batch.inserted), (None, 2));
    assert_eq!(w.jobs().await, 0, "invisible outside the transaction");
    assert!(w.business(1).await.is_empty());
    let size = SizeResponse::decode(
        &w.c.queue(method_queue::SIZE, scope_req("default", Some(tx)))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(size.pending, 1, "the transaction sees its own job");
    assert_eq!(w.hints(), 0, "no hint before COMMIT");
    w.c.commit(tx).await;
    assert_eq!(w.jobs().await, 3);
    assert_eq!(w.business(1).await, vec!["app".to_string()]);
    assert!(w.row(id).await.is_some(), "the returned id is the row's");
    assert_eq!(
        w.hints(),
        3,
        "one per distinct queue (default, a, b), at COMMIT"
    );

    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, 2, "app").await;
    w.c.enqueue_one("default", "two", Some(tx)).await;
    w.c.rollback(tx).await;
    w.wait_no_open_tx().await;
    assert_eq!(
        w.jobs_with_payload("two").await,
        0,
        "gone with the rollback"
    );
    assert!(w.business(2).await.is_empty());
    assert_eq!(w.hints(), 3, "a rollback fires nothing");

    // The committed job is a real job: an autocommit RESERVE takes it.
    let job = w.c.reserve_one("default").await;
    assert_eq!(id_of(&job), id);
    assert_session_alive(&mut w.c.c, 77).await;
    w.drop_schema().await;
}

/// Transactional ack (§24.5): ACK, RELEASE, EXTEND and CLEAR inside a transaction commit or roll back
/// TOGETHER with the business write. A rolled-back ACK leaves the job and its token valid; a RELEASE's
/// hint fires at COMMIT only for `delay_s = 0`.
#[tokio::test]
async fn ack_release_extend_and_clear_in_a_tx_are_atomic_with_the_business_write() {
    let Some(mut w) = World::new("atomic").await else {
        return;
    };
    let s = w.schema.clone();

    // ACK.
    let j1 = w.c.enqueue_one("default", "j1", None).await;
    let job = w.c.reserve_one("default").await;
    let before = w.row(j1).await.unwrap();
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, j1, "ack").await;
    assert_eq!(w.c.ack(&job, Some(tx)).await.unwrap(), ack_outcome::ACKED);
    w.c.rollback(tx).await;
    assert_eq!(
        w.row(j1).await,
        Some(before),
        "rolled back: the job is untouched"
    );
    assert!(w.business(j1).await.is_empty());
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, j1, "ack").await;
    assert_eq!(
        w.c.ack(&job, Some(tx)).await.unwrap(),
        ack_outcome::ACKED,
        "the same token is still valid after the rollback"
    );
    w.c.commit(tx).await;
    assert_eq!(w.row(j1).await, None);
    assert_eq!(w.business(j1).await.len(), 1);

    // RELEASE.
    let j2 = w.c.enqueue_one("default", "j2", None).await;
    let job = w.c.reserve_one("default").await;
    let before = w.row(j2).await.unwrap();
    let hints = w.hints();
    let tx = w.c.begin(None, false).await;
    let new = w.c.release(&job, 0, Some(tx)).await.unwrap().unwrap();
    w.c.rollback(tx).await;
    assert_eq!(w.row(j2).await, Some(before), "the old row is intact");
    assert_eq!(w.row(new).await, None, "the new row never existed");
    assert_eq!(w.hints(), hints);
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, j2, "release").await;
    let new = w.c.release(&job, 0, Some(tx)).await.unwrap().unwrap();
    w.c.commit(tx).await;
    assert_eq!(w.row(j2).await, None);
    let (attempts, reserved_at, available_at, created_at) = w.row(new).await.unwrap();
    assert_eq!(
        (attempts, reserved_at),
        (1, None),
        "attempts kept, unreserved"
    );
    assert_eq!(available_at, created_at, "delay 0 is available now");
    assert_eq!(
        w.hints(),
        hints + 1,
        "a delay-0 RELEASE wakes its queue at COMMIT"
    );
    let job = w.c.reserve_one("default").await;
    assert_eq!(id_of(&job), new);
    let tx = w.c.begin(None, false).await;
    let later = w.c.release(&job, 30, Some(tx)).await.unwrap().unwrap();
    w.c.commit(tx).await;
    let (_, _, available_at, created_at) = w.row(later).await.unwrap();
    assert_eq!(
        available_at - created_at,
        31,
        "the delay rule, in a transaction too"
    );
    assert_eq!(w.hints(), hints + 1, "a delayed RELEASE wakes nobody");

    // EXTEND.
    w.raw
        .batch_execute(&format!("UPDATE {} SET available_at = 0", w.table()))
        .await
        .unwrap();
    let job = w.c.reserve_one("default").await;
    let j3 = id_of(&job);
    w.raw
        .execute(
            &format!(
                "UPDATE {} SET reserved_at = reserved_at - 10 WHERE id = $1",
                w.table()
            ),
            &[&j3],
        )
        .await
        .unwrap();
    let old = w.row(j3).await.unwrap().1.unwrap();
    let tx = w.c.begin(None, false).await;
    w.c.extend(&job, Some(tx)).await.unwrap();
    w.c.rollback(tx).await;
    assert_eq!(
        w.row(j3).await.unwrap().1,
        Some(old),
        "rolled back: the lease unchanged"
    );
    let tx = w.c.begin(None, false).await;
    let deadline = w.c.extend(&job, Some(tx)).await.unwrap();
    w.c.commit(tx).await;
    let renewed = w.row(j3).await.unwrap().1.unwrap();
    assert!(renewed >= old + 10, "renewed from now: {old} → {renewed}");
    assert_eq!(deadline, i64::from(renewed) + 30 + 1);

    // CLEAR.
    let n = w.jobs().await;
    assert!(n > 0);
    let tx = w.c.begin(None, false).await;
    let deleted = ClearResponse::decode(
        &w.c.queue(method_queue::CLEAR, scope_req("default", Some(tx)))
            .await
            .unwrap(),
    )
    .unwrap()
    .deleted;
    assert_eq!(i64::try_from(deleted).unwrap(), n);
    w.c.rollback(tx).await;
    assert_eq!(w.jobs().await, n, "a rolled-back CLEAR deleted nothing");
    w.drop_schema().await;
}

/// §24.5 step 4: a `ROLLBACK_TO` that undoes an applied verb leaves its hint — a stale hint, which
/// costs one empty poll — and a verb that FAILED keeps none.
#[tokio::test]
async fn a_rollback_to_leaves_a_stale_hint_and_a_failed_verb_keeps_none() {
    let Some(mut w) = World::new("hint").await else {
        return;
    };
    w.c.queue(method_queue::SIZE, scope_req("default", None))
        .await
        .unwrap(); // verified
    let tx = w.c.begin(None, false).await;
    w.c.savepoint(tx, method_tx::SAVEPOINT).await;
    w.c.enqueue_one("default", "undone", Some(tx)).await;
    w.c.savepoint(tx, method_tx::ROLLBACK_TO).await;
    w.c.commit(tx).await;
    assert_eq!(w.jobs().await, 0, "the savepoint undid the job");
    assert_eq!(w.hints(), 1, "its hint is stale but kept (§24.5)");

    // A statement error: a READ ONLY transaction refuses the INSERT — a known fate, classified
    // in_tx, never Indeterminate, and no hint is kept.
    let tx = w.c.begin(None, true).await;
    let ep =
        w.c.enqueue(&[("default", "ro", 0)], Some(tx))
            .await
            .unwrap_err();
    assert_eq!(ep.sqlstate.as_deref(), Some("25006"), "{ep:?}");
    assert_ne!(ep.branch, branch::INDETERMINATE);
    assert_ne!(
        ep.code,
        errc::TX_DEADLINE,
        "a statement error does not end the transaction"
    );
    w.c.rollback(tx).await;
    assert_eq!(w.hints(), 1);
    assert_eq!(w.jobs().await, 0);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// In-transaction LeaseLost (R1)
// ---------------------------------------------------------------------------------------------

/// SPEC §24.4/§24.6: inside a transaction an unmatched fence is ALWAYS `LeaseLost` — for a stale token
/// AND for an absent row (where autocommit answers `gone`) — on ACK, RELEASE and EXTEND, changes no
/// row, and leaves the transaction OPEN (no SQL error occurred; the caller rolls back).
#[tokio::test]
async fn an_unmatched_fence_in_a_tx_is_lease_lost_never_gone_and_the_tx_stays_open() {
    let Some(mut w) = World::new("lost").await else {
        return;
    };
    let id = w.c.enqueue_one("default", "j", None).await;
    let stale = w.c.reserve_one("default").await;
    w.expire(id).await;
    let current = w.c.reserve_one("default").await;
    assert_eq!(id_of(&current), id);
    let before = w.row(id).await.unwrap();

    let tx = w.c.begin(None, false).await;
    assert_lease_lost_in_tx(w.c.ack(&stale, Some(tx)).await);
    assert_lease_lost_in_tx(w.c.release(&stale, 0, Some(tx)).await);
    assert_lease_lost_in_tx(w.c.extend(&stale, Some(tx)).await);
    let o = w.c.exec_tx(tx, "SELECT 1", vec![]).await;
    assert!(
        matches!(o, Outcome::Ok(_)),
        "the transaction is still open: {o:?}"
    );
    w.c.commit(tx).await;
    assert_eq!(
        w.row(id).await,
        Some(before),
        "no stale verb changed the row"
    );
    assert_eq!(w.jobs().await, 1, "no stale RELEASE inserted a row");

    // The absent row: the current holder ACKs (autocommit), then every fenced verb with its token is
    // LeaseLost in a transaction, while the autocommit controls answer `gone`.
    assert_eq!(w.c.ack(&current, None).await.unwrap(), ack_outcome::ACKED);
    let tx = w.c.begin(None, false).await;
    assert_lease_lost_in_tx(w.c.ack(&current, Some(tx)).await);
    assert_lease_lost_in_tx(w.c.release(&current, 0, Some(tx)).await);
    assert_lease_lost_in_tx(w.c.extend(&current, Some(tx)).await);
    w.c.rollback(tx).await;
    assert_eq!(
        w.c.ack(&current, None).await.unwrap(),
        ack_outcome::GONE,
        "the control: autocommit says gone"
    );
    assert_eq!(
        w.c.release(&current, 0, None).await.unwrap(),
        None,
        "control: nil"
    );
    assert_eq!(w.jobs().await, 0);
    assert_eq!(w.hints(), 0, "a verb that did nothing wakes nobody");
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Refusals before anything is sent
// ---------------------------------------------------------------------------------------------

/// `PoolMismatch` (§24.5 step 2, D22 (b)): a store in another pool than the transaction's is refused
/// before anything is sent — proven against a store on a pool NOBODY LISTENS ON, whose answer is the
/// refusal itself rather than a connection failure — and the transaction stays usable and commits.
/// Both directions: the store's pool dead, and the transaction on a second live pool.
#[tokio::test]
async fn pool_mismatch_is_refused_before_anything_and_the_tx_stays_usable() {
    let Some(mut w) = World::with_pools(
        "mismatch",
        |url, app| vec![pool("other", &with_app(url, app)), pool("dead", DEAD_PG)],
        &[
            ("FERRO_QUEUE_STORES", "jobs,far"),
            ("FERRO_QUEUE_FAR_POOL", "dead"),
        ],
    )
    .await
    else {
        return;
    };
    let s = w.schema.clone();
    let tx = w.c.begin(None, false).await;
    let ep =
        w.c.queue(method_queue::ENQUEUE, enqueue_req_store("far", Some(tx)))
            .await
            .unwrap_err();
    assert_eq!(
        (ep.code, ep.branch),
        (errc::POOL_MISMATCH, errc::POOL_MISMATCH_BRANCH),
        "{ep:?}"
    );
    assert_eq!(ep.branch, branch::NON_RETRYABLE);
    w.c.business(tx, &s, 1, "after").await;
    w.c.commit(tx).await;
    assert_eq!(
        w.business(1).await.len(),
        1,
        "the transaction was untouched"
    );

    let tx = w.c.begin_on("other", None, false).await;
    for (method, payload) in [
        (
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "x", 0)], Some(tx), None),
        ),
        (method_queue::SIZE, scope_req("default", Some(tx))),
        (method_queue::CLEAR, scope_req("default", Some(tx))),
    ] {
        let ep = w.c.queue(method, payload).await.unwrap_err();
        assert_eq!(ep.code, errc::POOL_MISMATCH, "method {method}: {ep:?}");
        assert!(ep.message.contains("pool default") && ep.message.contains("pool other"));
    }
    w.c.business(tx, &s, 2, "other").await;
    w.c.commit(tx).await;
    assert_eq!(w.business(2).await.len(), 1);
    assert_eq!(w.jobs().await, 0, "nothing was enqueued");
    w.drop_schema().await;
}

/// A tx-scoped RESERVE is refused for good (§24.5), before anything — even before the `tx_id` is
/// resolved — and the transaction stays usable; a job is not leased.
#[tokio::test]
async fn a_tx_scoped_reserve_is_refused_and_the_tx_stays_usable() {
    let Some(mut w) = World::new("reserve").await else {
        return;
    };
    let id = w.c.enqueue_one("default", "j", None).await;
    let tx = w.c.begin(None, false).await;
    for tx_id in [tx, 999_999_999] {
        let ep =
            w.c.queue(method_queue::RESERVE, reserve_req("default", Some(tx_id)))
                .await
                .unwrap_err();
        assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
        assert!(ep.message.contains("§24.5"), "{}", ep.message);
    }
    let o = w.c.exec_tx(tx, "SELECT 1", vec![]).await;
    assert!(matches!(o, Outcome::Ok(_)), "{o:?}");
    w.c.commit(tx).await;
    assert_eq!(w.row(id).await.unwrap().0, 0, "no reservation was taken");
    w.drop_schema().await;
}

/// A tx-scoped verb is the store's first use too: it runs the version gate and shape verification
/// (§24.3) — on its own checkout, never inside the application's transaction — so a store whose table
/// is absent, or has the wrong shape, is refused `Unsupported` naming why, nothing is sent, and the
/// transaction stays usable. The control: a good store in the same pool, first used inside a
/// transaction, is verified and served.
#[tokio::test]
async fn a_tx_scoped_verb_verifies_the_store_first_and_leaves_the_tx_usable() {
    let Some(w) = World::with_pools("verify", |_, _| Vec::new(), &[]).await else {
        return;
    };
    let s = w.schema.clone();
    w.raw
        .batch_execute(&format!(
            "CREATE TABLE {s}.wide (id bigserial PRIMARY KEY, queue varchar(255) NOT NULL, \
             payload text NOT NULL, attempts integer NOT NULL, reserved_at integer NULL, \
             available_at integer NOT NULL, created_at integer NOT NULL)"
        ))
        .await
        .unwrap();
    let missing = format!("{s}.missing");
    let wide = format!("{s}.wide");
    let table = w.table();
    let (server, _) = queue_server(
        vec![pool("default", &with_app(&w.url, &w.app))],
        &[
            ("FERRO_QUEUE_STORES", "jobs,gone,wide"),
            ("FERRO_QUEUE_JOBS_POOL", "default"),
            ("FERRO_QUEUE_JOBS_TABLE", table.as_str()),
            ("FERRO_QUEUE_GONE_POOL", "default"),
            ("FERRO_QUEUE_GONE_TABLE", missing.as_str()),
            ("FERRO_QUEUE_WIDE_POOL", "default"),
            ("FERRO_QUEUE_WIDE_TABLE", wide.as_str()),
        ],
    );
    let mut q = Q::connect(&server).await;
    let tx = q.begin(None, false).await;
    for (store, why) in [("gone", "does not exist"), ("wide", "attempts")] {
        let ep = q
            .queue(method_queue::ENQUEUE, enqueue_req_store(store, Some(tx)))
            .await
            .unwrap_err();
        assert_eq!(ep.code, errc::UNSUPPORTED, "{store}: {ep:?}");
        assert!(ep.message.contains(why), "{store}: {}", ep.message);
    }
    q.business(tx, &s, 1, "after").await;
    q.enqueue_one("default", "good", Some(tx)).await;
    q.commit(tx).await;
    assert_eq!(
        w.business(1).await.len(),
        1,
        "the transaction was untouched"
    );
    assert_eq!(w.jobs_with_payload("good").await, 1);
    w.drop_schema().await;
}

/// The `tx_id` resolves exactly as on EXEC: unknown and another session's are the same `TxNotFound`;
/// the owner's tombstoned one is `TxDeadline{Retryable}`.
#[tokio::test]
async fn the_tx_id_resolves_as_on_exec() {
    let Some(mut w) = World::new("resolve").await else {
        return;
    };
    let mut other = w.session().await;
    let tx = w.c.begin(None, false).await;
    for (q, tx_id) in [(&mut other, tx), (&mut w.c, 999_999_999)] {
        let ep = q
            .queue(
                method_queue::ENQUEUE,
                enqueue_req(&[("default", "x", 0)], Some(tx_id), None),
            )
            .await
            .unwrap_err();
        assert_eq!(
            (ep.code, ep.branch),
            (errc::TX_NOT_FOUND, errc::TX_NOT_FOUND_BRANCH),
            "{ep:?}"
        );
    }
    // Tombstone the transaction with a timed-out EXEC, then a QUEUE verb on it is TxDeadline.
    let req = ExecRequest {
        pool: "default".into(),
        sql: Some("SELECT 1 FROM pg_sleep(2)".into()),
        query_id: None,
        params: vec![],
        timeout_ms: Some(50),
        readonly: true,
        fetch: 0,
        tx_id: Some(tx),
        traceparent: None,
    };
    assert_tx_deadline(&outcome_err(
        w.c.call(service::SQL, method_sql::EXEC, req.encode()).await,
    ));
    let ep =
        w.c.queue(method_queue::ACK, {
            FencedRequest {
                store: "jobs".into(),
                job_id: b"1".to_vec(),
                token: ferro_queue::sql::Token::from_pg(1, 1).encode().to_vec(),
                common: common_of(Some(tx), None),
            }
            .encode()
        })
        .await
        .unwrap_err();
    assert_tx_deadline(&ep);
    assert_eq!(w.jobs().await, 0);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Fate (§24.6): timeout, CANCEL, session death
// ---------------------------------------------------------------------------------------------

/// SPEC §24.6's "ENQUEUE in a transaction" row and §24.5 step 3: an in-transaction verb whose
/// statement was SENT and then timed out — or CANCELled, observed blocked on a lock first — rolls the
/// whole transaction back and tombstones it (`TxDeadline{Retryable}`). After the lock is released the
/// read-back proves nothing applied (neither the job nor the business write) and nothing was re-sent
/// (charter rule 3).
#[tokio::test]
async fn a_timed_out_or_cancelled_in_tx_verb_rolls_the_transaction_back() {
    let Some(mut w) = World::new("fate").await else {
        return;
    };
    let s = w.schema.clone();
    let id = w.c.enqueue_one("default", "held", None).await;
    let job = w.c.reserve_one("default").await;
    let before = w.row(id).await.unwrap();
    let holder = w.lock_jobs().await;

    // timeout_ms on an in-tx ENQUEUE.
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, 1, "timeout").await;
    let ep =
        w.c.queue(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "late", 0)], Some(tx), Some(300)),
        )
        .await
        .unwrap_err();
    assert_tx_deadline(&ep);
    let after = w.c.exec_tx(tx, "SELECT 1", vec![]).await;
    assert_tx_deadline(&outcome_err(after));

    // CANCEL on an in-tx ACK, once it is provably blocked.
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, 2, "cancel").await;
    let rid = w.c.next();
    w.c.c
        .send_request(
            rid,
            service::QUEUE,
            method_queue::ACK,
            fenced_req(&job, Some(tx), None),
        )
        .await;
    w.wait_for_backend(LOCK_WAIT).await;
    w.c.c.cancel(rid).await;
    assert_tx_deadline(&outcome_err(
        w.c.terminal(rid, service::QUEUE, method_queue::ACK).await,
    ));
    let after = w.c.exec_tx(tx, "SELECT 1", vec![]).await;
    assert_tx_deadline(&outcome_err(after));

    // A `57014` the statement RETURNS ITSELF — the application's own `SET LOCAL statement_timeout` —
    // takes the same exit as the engine's timer: PostgreSQL aborted the block, so the actor rolls back
    // and tombstones it (the next touch is TxDeadline, not a `25P02` from an aborted block).
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, 3, "statement_timeout").await;
    let o =
        w.c.exec_tx(tx, "SET LOCAL statement_timeout = '200ms'", vec![])
            .await;
    assert!(matches!(o, Outcome::Ok(_)), "{o:?}");
    let ep =
        w.c.queue(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "late2", 0)], Some(tx), None),
        )
        .await
        .unwrap_err();
    assert_tx_deadline(&ep);
    let after = w.c.exec_tx(tx, "SELECT 1", vec![]).await;
    assert_tx_deadline(&outcome_err(after));

    holder.batch_execute("ROLLBACK").await.unwrap();
    w.wait_no_open_tx().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(w.jobs().await, 1, "the timed-out ENQUEUE applied nothing");
    assert_eq!(
        w.row(id).await,
        Some(before),
        "the cancelled ACK applied nothing"
    );
    assert!(w.business(1).await.is_empty() && w.business(2).await.is_empty());
    assert!(w.business(3).await.is_empty());
    assert_eq!(w.jobs_with_payload("late2").await, 0);
    assert_eq!(w.hints(), 0);
    assert_session_alive(&mut w.c.c, 78).await;
    w.drop_schema().await;
}

/// A tx-scoped verb with NO time left once its store is verified (`timeout_ms = 0` on a verified
/// store) is answered unsent — `PoolTimeout`, never dispatched with no time — and the transaction is
/// untouched: it commits its business write afterwards. Dispatching it instead would send the
/// statement, cancel it at once, and roll the application's transaction back (`TxDeadline`).
#[tokio::test]
async fn a_tx_scoped_verb_with_no_time_left_is_answered_unsent_and_the_tx_survives() {
    let Some(mut w) = World::new("notime").await else {
        return;
    };
    let s = w.schema.clone();
    w.c.queue(method_queue::SIZE, scope_req("default", None))
        .await
        .unwrap(); // verified
    let tx = w.c.begin(None, false).await;
    let ep =
        w.c.queue(
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "never", 0)], Some(tx), Some(0)),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (ep.code, ep.branch),
        (errc::POOL_TIMEOUT, errc::POOL_TIMEOUT_BRANCH),
        "{ep:?}"
    );
    w.c.business(tx, &s, 1, "survived").await;
    w.c.commit(tx).await;
    assert_eq!(
        w.business(1).await.len(),
        1,
        "the transaction was untouched"
    );
    assert_eq!(w.jobs().await, 0, "nothing was sent");
    w.drop_schema().await;
}

/// Session death (§24.5 step 3: "session-death abort"): a session that ends holding a transaction
/// with an ENQUEUE and a business write leaves neither.
#[tokio::test]
async fn session_death_rolls_an_in_tx_enqueue_back() {
    let Some(w) = World::new("death").await else {
        return;
    };
    let s = w.schema.clone();
    let mut q = w.session().await;
    let tx = q.begin(None, false).await;
    q.business(tx, &s, 1, "dying").await;
    q.enqueue_one("default", "dying", Some(tx)).await;
    drop(q);
    w.wait_no_open_tx().await;
    assert_eq!(w.jobs().await, 0);
    assert!(w.business(1).await.is_empty());
    assert_eq!(w.hints(), 0);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Chaos row 2 (§24.13)
// ---------------------------------------------------------------------------------------------

/// Chaos row 2's engine-side half: the BACKEND link killed (a) while an in-transaction ENQUEUE is in
/// flight — `Retryable{ConnectionLost}`, the transaction is dead, neither row exists — and (b) while
/// the COMMIT is running (inside a deferred trigger) — `Indeterminate{WriteUnconfirmed}`, and the rows
/// are both or neither.
#[tokio::test]
async fn chaos_row_2_backend_killed_under_an_in_tx_enqueue_and_during_its_commit() {
    let Some(mut w) = World::new("c2b").await else {
        return;
    };
    let s = w.schema.clone();
    w.c.queue(method_queue::SIZE, scope_req("default", None))
        .await
        .unwrap(); // verified before any lock

    // (a) mid-ENQUEUE.
    let holder = w.lock_jobs().await;
    let tx = w.c.begin(None, false).await;
    w.c.business(tx, &s, 1, "a").await;
    let rid = w.c.next();
    w.c.c
        .send_request(
            rid,
            service::QUEUE,
            method_queue::ENQUEUE,
            enqueue_req(&[("default", "a", 0)], Some(tx), None),
        )
        .await;
    let pid = w.wait_for_backend(LOCK_WAIT).await;
    w.raw
        .execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap();
    let ep = outcome_err(
        w.c.terminal(rid, service::QUEUE, method_queue::ENQUEUE)
            .await,
    );
    assert_eq!(
        (ep.code, ep.branch),
        (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
        "an in-tx statement's lost link is Retryable, never Indeterminate: {ep:?}"
    );
    holder.batch_execute("ROLLBACK").await.unwrap();
    w.wait_no_open_tx().await;
    assert_eq!(w.jobs_with_payload("a").await, 0);
    assert!(w.business(1).await.is_empty(), "neither row");

    // (b) during COMMIT.
    w.raw.batch_execute(&slow_commit_sql(&s)).await.unwrap();
    let mut q = w.session().await;
    let tx = q.begin(None, false).await;
    q.business(tx, &s, 2, "b").await;
    q.enqueue_one("default", "b", Some(tx)).await;
    let rid = q.next();
    q.c.send_request(
        rid,
        service::TX,
        method_tx::COMMIT,
        TxControl { tx_id: tx }.encode(),
    )
    .await;
    let pid = w.wait_for_backend(IN_COMMIT).await;
    w.raw
        .execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap();
    let ep = outcome_err(q.terminal(rid, service::TX, method_tx::COMMIT).await);
    assert_eq!(
        (ep.code, ep.branch),
        (errc::WRITE_UNCONFIRMED, errc::WRITE_UNCONFIRMED_BRANCH),
        "a COMMIT whose reply was lost is the one transactional Indeterminate: {ep:?}"
    );
    assert_eq!(ep.branch, branch::INDETERMINATE);
    w.wait_no_open_tx().await;
    let (jobs, business) = (
        w.jobs_with_payload("b").await,
        w.business(2).await.len() as i64,
    );
    assert_eq!(jobs, business, "both or neither, never one");
    assert!(jobs <= 1);
    assert_eq!(
        w.hints(),
        0,
        "a COMMIT that did not answer success fires no hint (§24.5 step 4)"
    );
    w.drop_schema().await;
}

/// Kills its child with SIGKILL on drop, so a failing assertion never leaves a daemon behind.
struct KillOnDrop(std::process::Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A REAL `ferrod` process serving the `jobs` store on `<schema>.ferro_jobs`, its pool connecting
/// as `app`. Returns the child and a session to it.
async fn spawn_ferrod(url: &str, app: &str, schema: &str, n: u32) -> (KillOnDrop, Q) {
    let sock = std::env::temp_dir().join(format!("{app}-{n}.sock"));
    let _ = std::fs::remove_file(&sock);
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_ferrod"))
        .env_clear()
        .env("FERRO_SOCK", &sock)
        .env("FERRO_ALLOW_UIDS", nix::unistd::geteuid().to_string())
        .env("FERRO_POOLS", "default")
        .env("FERRO_POOL_DEFAULT_DSN", with_app(url, app))
        .env("FERRO_QUEUE_STORES", "jobs")
        .env("FERRO_QUEUE_JOBS_POOL", "default")
        .env("FERRO_QUEUE_JOBS_TABLE", format!("{schema}.ferro_jobs"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn ferrod");
    let child = KillOnDrop(child);
    let deadline = Instant::now() + Duration::from_secs(20);
    while tokio::net::UnixStream::connect(&sock).await.is_err() {
        assert!(Instant::now() < deadline, "ferrod never listened");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (child, Q::over(common::connect(&sock).await).await)
}

/// Chaos row 2 with a REAL `ferrod` process SIGKILLed (§24.13): killed while a transaction holds an
/// ENQUEUE and a business write, before COMMIT → neither row, every time (the read-back waits until
/// the server has really ended the transaction, so it proves a rollback, not invisibility); killed
/// DURING COMMIT — half the iterations provably inside it (the deferred trigger is sleeping), half at a
/// varied instant after it was sent — → both or neither, every iteration, never one.
#[tokio::test]
async fn chaos_row_2_sigkill_ferrod_before_and_during_commit() {
    let Some(url) = pg_url() else { return };
    let schema = next_tag("c2k");
    let app = schema.clone();
    let raw = raw_connect(&url).await;
    raw.batch_execute(&schema_sql(&schema)).await.unwrap();
    let count = |sql: String| {
        let raw = &raw;
        async move { raw.query_one(&sql, &[]).await.unwrap().get::<_, i64>(0) }
    };

    // Before COMMIT.
    for i in 0..3u32 {
        let (mut child, mut q) = spawn_ferrod(&url, &app, &schema, i).await;
        let tx = q.begin(None, false).await;
        q.business(tx, &schema, i64::from(i), "before").await;
        q.enqueue_one("default", &format!("before-{i}"), Some(tx))
            .await;
        child.0.kill().unwrap();
        let _ = child.0.wait();
        wait_no_open_tx(&raw, &app).await;
        assert_eq!(
            count(format!(
                "SELECT count(*) FROM {schema}.ferro_jobs WHERE payload = 'before-{i}'"
            ))
            .await,
            0
        );
        assert_eq!(
            count(format!(
                "SELECT count(*) FROM {schema}.business WHERE job_id = {i}"
            ))
            .await,
            0,
            "neither row"
        );
    }

    // During COMMIT.
    raw.batch_execute(&slow_commit_sql(&schema)).await.unwrap();
    let mut outcomes = [0u32; 2];
    for i in 10..22u32 {
        let (mut child, mut q) = spawn_ferrod(&url, &app, &schema, i).await;
        let tx = q.begin(None, false).await;
        q.business(tx, &schema, i64::from(i), "during").await;
        q.enqueue_one("default", &format!("during-{i}"), Some(tx))
            .await;
        let rid = q.next();
        q.c.send_request(
            rid,
            service::TX,
            method_tx::COMMIT,
            TxControl { tx_id: tx }.encode(),
        )
        .await;
        if i % 2 == 0 {
            wait_for_backend(&raw, &app, IN_COMMIT).await;
        } else {
            // 0, 1, 2, 5, 10, 30 ms: from "the COMMIT frame may not have left ferrod" to "it is
            // running".
            let ms = [0u64, 1, 2, 5, 10, 30][usize::try_from((i - 11) / 2).unwrap()];
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        child.0.kill().unwrap();
        let _ = child.0.wait();
        wait_no_open_tx(&raw, &app).await;
        let jobs = count(format!(
            "SELECT count(*) FROM {schema}.ferro_jobs WHERE payload = 'during-{i}'"
        ))
        .await;
        let business = count(format!(
            "SELECT count(*) FROM {schema}.business WHERE job_id = {i}"
        ))
        .await;
        assert_eq!(jobs, business, "iteration {i}: both or neither, never one");
        assert!(jobs <= 1);
        outcomes[usize::try_from(jobs).unwrap()] += 1;
    }
    eprintln!(
        "chaos row 2, killed during COMMIT: neither {} / both {}",
        outcomes[0], outcomes[1]
    );
    raw.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------------------------
// Chaos row 7 (§24.13): transactional ack under induced lease loss
// ---------------------------------------------------------------------------------------------

/// What one worker's transactional ack did.
#[derive(Debug, PartialEq, Eq)]
enum Ended {
    Committed,
    RolledBack(u16),
}

/// The transactional-ack worker of §24.5: all its DB work in ONE transaction, the ACK inside it, and
/// a ROLLBACK on anything but `acked` — `LeaseLost` above all.
async fn transactional_ack(
    q: &mut Q,
    schema: &str,
    job: &ReservedJob,
    worker: &str,
    isolation: Option<u8>,
) -> Ended {
    let tx = q.begin(isolation, false).await;
    q.business(tx, schema, id_of(job), worker).await;
    match q.ack(job, Some(tx)).await {
        Ok(o) => {
            assert_eq!(o, ack_outcome::ACKED, "never `gone` in a transaction");
            q.commit(tx).await;
            Ended::Committed
        }
        Err(ep) => {
            q.rollback(tx).await;
            Ended::RolledBack(ep.code)
        }
    }
}

/// Chaos row 7 over many iterations: the business rows per job are ≤ 1 and the STALE holder never
/// commits, whether it (a) races the new holder concurrently, (b) arrives after the new holder
/// committed (the absent row, R1 — whose autocommit control answers `gone`), (c) arrives while the new
/// holder rolls back, or (d) runs at REPEATABLE READ with a snapshot older than the re-reservation
/// (`40001`, Retryable). The control: a lease that expired with NOBODY re-reserving is honoured.
#[tokio::test]
async fn chaos_row_7_transactional_ack_under_induced_lease_loss() {
    let Some(mut w) = World::new("c7").await else {
        return;
    };
    let s = w.schema.clone();
    let mut b = w.session().await;
    let rr = Some(u8::from(Isolation::RepeatableRead));

    for i in 0..30u32 {
        let queue = format!("q{i}");
        let id = w.c.enqueue_one(&queue, "{}", None).await;
        let stale = w.c.reserve_one(&queue).await;
        match i % 3 {
            // (a) racing.
            0 | 1 => {
                w.expire(id).await;
                let fresh = b.reserve_one(&queue).await;
                let (a_end, b_end) = tokio::join!(
                    transactional_ack(&mut w.c, &s, &stale, "stale", None),
                    transactional_ack(&mut b, &s, &fresh, "fresh", None),
                );
                assert_eq!(a_end, Ended::RolledBack(errc::LEASE_LOST), "iteration {i}");
                assert_eq!(b_end, Ended::Committed, "iteration {i}");
                assert_eq!(
                    w.business(id).await,
                    vec!["fresh".to_string()],
                    "iteration {i}"
                );
            }
            // (b) the absent row (R1).
            _ => {
                w.expire(id).await;
                let fresh = b.reserve_one(&queue).await;
                assert_eq!(
                    transactional_ack(&mut b, &s, &fresh, "fresh", None).await,
                    Ended::Committed
                );
                assert_eq!(
                    transactional_ack(&mut w.c, &s, &stale, "stale", None).await,
                    Ended::RolledBack(errc::LEASE_LOST),
                    "an absent row is LeaseLost in a transaction"
                );
                assert_eq!(
                    w.c.ack(&stale, None).await.unwrap(),
                    ack_outcome::GONE,
                    "control"
                );
                assert_eq!(w.business(id).await, vec!["fresh".to_string()]);
            }
        }
        assert_eq!(
            w.row(id).await,
            None,
            "iteration {i}: the job is done exactly once"
        );
    }

    // (c) the new holder rolls back: the stale holder still cannot commit, and the job survives
    // under the new holder's token.
    for i in 30..35u32 {
        let queue = format!("q{i}");
        let id = w.c.enqueue_one(&queue, "{}", None).await;
        let stale = w.c.reserve_one(&queue).await;
        w.expire(id).await;
        let fresh = b.reserve_one(&queue).await;
        let tx = b.begin(None, false).await;
        b.business(tx, &s, id, "fresh").await;
        assert_eq!(b.ack(&fresh, Some(tx)).await.unwrap(), ack_outcome::ACKED);
        b.rollback(tx).await;
        assert_eq!(
            transactional_ack(&mut w.c, &s, &stale, "stale", None).await,
            Ended::RolledBack(errc::LEASE_LOST)
        );
        assert!(w.business(id).await.is_empty());
        assert_eq!(w.row(id).await.unwrap().0, 2, "still the new holder's");
    }

    // (d) REPEATABLE READ, the stale holder's snapshot older than the re-reservation: 40001.
    for i in 35..40u32 {
        let queue = format!("q{i}");
        let id = w.c.enqueue_one(&queue, "{}", None).await;
        let stale = w.c.reserve_one(&queue).await;
        let tx = w.c.begin(rr, false).await;
        w.c.business(tx, &s, id, "stale").await; // the snapshot is taken here
        w.expire(id).await;
        let fresh = b.reserve_one(&queue).await;
        let ep = w.c.ack(&stale, Some(tx)).await.unwrap_err();
        assert_eq!(ep.sqlstate.as_deref(), Some("40001"), "{ep:?}");
        assert_eq!(ep.branch, branch::RETRYABLE, "{ep:?}");
        w.c.rollback(tx).await;
        assert_eq!(
            transactional_ack(&mut b, &s, &fresh, "fresh", rr).await,
            Ended::Committed
        );
        assert_eq!(w.business(id).await, vec!["fresh".to_string()]);
    }

    // The control: an expired lease NOBODY re-reserved is honoured inside a transaction.
    let id = w.c.enqueue_one("late", "{}", None).await;
    let job = w.c.reserve_one("late").await;
    w.expire(id).await;
    assert_eq!(
        transactional_ack(&mut w.c, &s, &job, "late", None).await,
        Ended::Committed
    );
    assert_eq!(w.business(id).await, vec!["late".to_string()]);

    // Over every iteration: no job has more than one business row, and the stale holder never
    // committed one.
    let worst: i64 = w
        .scalar(&format!(
            "SELECT coalesce(max(n), 0) FROM (SELECT count(*) AS n FROM {s}.business GROUP BY \
             job_id) AS c"
        ))
        .await;
    assert_eq!(worst, 1);
    assert_eq!(
        w.scalar(&format!(
            "SELECT count(*) FROM {s}.business WHERE worker = 'stale'"
        ))
        .await,
        0
    );
    w.drop_schema().await;
}
