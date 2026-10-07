//! M7-G3: Ferro Queue's WAKER — waiting RESERVEs that hold no connection, the wait bound, unreserve,
//! wake hints, coalesced polls and drain (SPEC §24.8), end to end through real sessions on PostgreSQL;
//! chaos rows 11 (the parked-waiter cost bound) and 12 (deliver xor unreserve) of §24.13.
//!
//! Live against PostgreSQL (`FERRO_TEST_PG_URL`; each test skips, and the CI no-skip gate fails, when
//! it is unset). Each test owns a fresh schema with Laravel's stock `jobs.stub` table and a RAW side
//! connection that sets rows up and reads them back: every claim about what the waker did is checked
//! against the ROWS (§24.2 I1), not only against the terminal.
//!
//! **No fixed sleeps decide an outcome.** A test that needs a sweep IN FLIGHT holds it there with a
//! `BEFORE UPDATE` trigger that sleeps only on a RESERVATION (`attempts` rising) and proves it is in
//! flight from `pg_stat_activity` (the pool connects with a per-test `application_name`); a test that
//! needs a waiter PARKED waits for the waker to report it idle. Timing assertions are one-sided upper
//! bounds with generous margins, or lower bounds the engine itself guarantees (a wait never answers
//! before its `wait_ms`).

mod common;

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{TestClient, pg_url};
use ferro_proto::consts::{ack_outcome, branch, errc, flags, method_queue, method_tx, service};
use ferro_proto::messages::{
    AckResponse, BeginRequest, BeginResponse, EnqueueJob, EnqueueRequest, EnqueueResponse,
    ErrorPayload, FencedRequest, Outcome, QueueCommon, QueueScopeRequest, ReserveRequest,
    ReserveResponse, ReservedJob, TxControl,
};
use ferro_queue::sql::JobId;
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::queue_metrics::{HintSource, Trigger, UnreserveCause};
use ferrod::services::queue_waker::Waker;
use ferrod::services::sql;
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;

static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn next_tag(tag: &str) -> String {
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("g3_{tag}_{}_{n}", std::process::id())
}

fn with_app(url: &str, app: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}application_name={app}")
}

fn tmp_socket() -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("{}.sock", next_tag("sock")));
    p
}

/// A real `serve` accept loop (so its `Drain` reaches every session, §23.6.1) with the `jobs` store.
fn queue_server(dsn: &str, vars: &[(&str, &str)], drain: Drain) -> (PathBuf, Arc<PoolRegistry>) {
    let socket = tmp_socket();
    let mut config = Config {
        socket_path: socket.clone(),
        pools: vec![PoolSpec {
            name: "default".into(),
            dsn: dsn.into(),
            kind: ferrod::config::infer_pool_kind(dsn),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
            allow_dir: None,
        }],
        max_inflight: 512,
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
    let listener = ferrod::listener::bind_uds(&config).expect("bind");
    tokio::spawn(ferrod::serve::serve(
        listener,
        config,
        BootEpoch(1),
        drain,
        registry.clone(),
        tx_registry,
        factory,
    ));
    (socket, registry)
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

/// One test's world.
struct World {
    socket: PathBuf,
    registry: Arc<PoolRegistry>,
    raw: tokio_postgres::Client,
    schema: String,
    table: String,
    app: String,
    drain: Drain,
}

impl World {
    async fn new(tag: &str, extra: &[(&str, &str)]) -> Option<World> {
        let url = pg_url()?;
        let raw = raw_connect(&url).await;
        let schema = next_tag(tag);
        let table = format!("{schema}.ferro_jobs");
        raw.batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {table} (id bigserial PRIMARY KEY, queue varchar(255) NOT NULL, \
             payload text NOT NULL, attempts smallint NOT NULL, reserved_at integer NULL, \
             available_at integer NOT NULL, created_at integer NOT NULL); \
             CREATE INDEX ON {table} (queue); \
             CREATE TABLE {schema}.slow (ms integer NOT NULL); \
             INSERT INTO {schema}.slow VALUES (0); \
             CREATE FUNCTION {schema}.slow_reserve() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF NEW.reserved_at IS NOT NULL AND NEW.attempts > OLD.attempts THEN \
                 PERFORM pg_sleep((SELECT ms FROM {schema}.slow) / 1000.0); \
               END IF; \
               RETURN NEW; \
             END $$; \
             CREATE TRIGGER slow BEFORE UPDATE ON {table} FOR EACH ROW \
               EXECUTE FUNCTION {schema}.slow_reserve();"
        ))
        .await
        .unwrap();
        let mut vars = vec![
            ("FERRO_QUEUE_STORES", "jobs"),
            ("FERRO_QUEUE_JOBS_POOL", "default"),
            ("FERRO_QUEUE_JOBS_TABLE", table.as_str()),
            ("FERRO_QUEUE_JOBS_LEASE_S", "30"),
            ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "65536"),
            // Only a hint, an arrival or the test's own poll setting may wake anyone.
            ("FERRO_QUEUE_JOBS_POLL_MS", "600000"),
        ];
        vars.retain(|(k, _)| !extra.iter().any(|(e, _)| e == k));
        vars.extend_from_slice(extra);
        let app = schema.clone();
        let drain = Drain::new();
        let (socket, registry) = queue_server(&with_app(&url, &app), &vars, drain.clone());
        let w = World {
            socket,
            registry,
            raw,
            schema,
            table,
            app,
            drain,
        };
        w.warm().await;
        Some(w)
    }

    /// The store's first use — the version gate and shape verification — done before the test
    /// starts, so a test that holds a sweep in flight is not racing the version probe of a loaded
    /// database (a failed probe refuses the verb `ConnectionLost` and backs off for 5 s, §24.3).
    async fn warm(&self) {
        let mut q = self.session().await;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let req = QueueScopeRequest {
                store: "jobs".into(),
                queue: "warm".into(),
                common: QueueCommon::default(),
            };
            match q
                .call(service::QUEUE, method_queue::SIZE, req.encode())
                .await
            {
                Outcome::Ok(_) => return,
                other => {
                    assert!(
                        Instant::now() < deadline,
                        "the store never became usable: {other:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    async fn session(&self) -> Q {
        let mut c = common::connect(&self.socket).await;
        c.hello(1).await;
        Q { c, rid: 10 }
    }

    fn waker(&self) -> &Arc<Waker> {
        self.registry.queue().unwrap().waker("jobs").unwrap()
    }

    async fn drop_schema(self) {
        self.raw
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }

    async fn slow(&self, ms: i32) {
        self.raw
            .execute(&format!("UPDATE {}.slow SET ms = $1", self.schema), &[&ms])
            .await
            .unwrap();
    }

    /// Insert an available job directly (no hint fires). Returns its id.
    async fn put(&self, queue: &str) -> i64 {
        self.raw
            .query_one(
                &format!(
                    "INSERT INTO {} (queue, payload, attempts, reserved_at, available_at, \
                     created_at) VALUES ($1, 'p', 0, NULL, 0, \
                     floor(extract(epoch FROM now()))::integer) RETURNING id",
                    self.table
                ),
                &[&queue],
            )
            .await
            .unwrap()
            .get(0)
    }

    /// `(attempts, reserved_at)` of a job, `None` when the row is gone.
    async fn row(&self, id: i64) -> Option<(i16, Option<i32>)> {
        self.raw
            .query_opt(
                &format!(
                    "SELECT attempts, reserved_at FROM {} WHERE id = $1",
                    self.table
                ),
                &[&id],
            )
            .await
            .unwrap()
            .map(|r| (r.get(0), r.get(1)))
    }

    async fn all_rows(&self) -> HashMap<i64, (i16, Option<i32>)> {
        self.raw
            .query(
                &format!("SELECT id, attempts, reserved_at FROM {}", self.table),
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.get(0), (r.get(1), r.get(2))))
            .collect()
    }

    /// Wait until a reservation statement of THIS daemon's pool is sleeping in the trigger.
    async fn sweep_in_flight(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let n: i64 = self
                .raw
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 \
                     AND wait_event = 'PgSleep'",
                    &[&self.app],
                )
                .await
                .unwrap()
                .get(0);
            if n > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "no sweep reached the trigger (waiters {}, polls {})",
                self.waker().waiters(),
                self.waker().metrics().polls_total()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Wait until `n` waiters are registered AND parked (no sweep serving them).
    async fn parked(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let w = self.waker();
            if w.waiters() == n && w.idle_waiters() == n {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "waiters {} idle {} (want {n})",
                w.waiters(),
                w.idle_waiters()
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Wait until `cond` holds on the job's row (an unreserve is spawned, so it lands a moment after
    /// the terminal that caused it).
    async fn until_row(&self, id: i64, want: Option<(i16, Option<i32>)>) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let got = self.row(id).await;
            if got == want {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "row {id}: {got:?}, want {want:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Wait (bounded) until `cond` holds: the engine's counters are bumped by the spawned task that sent
/// the statement, a moment after the row it changed is visible.
async fn eventually(cond: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A session plus its next request id.
struct Q {
    c: TestClient,
    rid: u32,
}

impl Q {
    fn next(&mut self) -> u32 {
        self.rid += 1;
        self.rid
    }

    async fn send(&mut self, svc: u16, method: u16, payload: Vec<u8>) -> u32 {
        let rid = self.next();
        self.c.send_request(rid, svc, method, payload).await;
        rid
    }

    /// The ONE terminal of `rid`, within `within`.
    async fn terminal(&mut self, rid: u32, method: u16, within: Duration) -> Outcome {
        let t = self
            .c
            .recv_or_none(within)
            .await
            .unwrap_or_else(|| panic!("no terminal for {rid} within {within:?}"));
        assert_eq!(t.header.request_id, rid);
        assert_eq!(t.header.flags, flags::END, "exactly one END, nothing else");
        assert_eq!(t.header.method, method);
        Outcome::decode(&t.payload).expect("a terminal Outcome")
    }

    async fn call(&mut self, svc: u16, method: u16, payload: Vec<u8>) -> Outcome {
        let rid = self.send(svc, method, payload).await;
        self.terminal(rid, method, Duration::from_secs(10)).await
    }

    async fn reserve_send(&mut self, queues: &[&str], wait_ms: u32) -> u32 {
        self.send(
            service::QUEUE,
            method_queue::RESERVE,
            reserve_req(queues, 1, wait_ms, None),
        )
        .await
    }

    async fn reserve_result(&mut self, rid: u32, within: Duration) -> Outcome {
        self.terminal(rid, method_queue::RESERVE, within).await
    }

    async fn enqueue(&mut self, queue: &str, tx_id: Option<u64>) -> i64 {
        match self
            .call(
                service::QUEUE,
                method_queue::ENQUEUE,
                enqueue_req(queue, 0, tx_id),
            )
            .await
        {
            Outcome::Ok(b) => {
                JobId::decode(&EnqueueResponse::decode(&b).unwrap().job_id.unwrap())
                    .unwrap()
                    .0
            }
            other => panic!("ENQUEUE: {other:?}"),
        }
    }

    async fn begin(&mut self) -> u64 {
        let req = BeginRequest {
            pool: "default".into(),
            isolation: None,
            readonly: false,
        };
        match self.call(service::TX, method_tx::BEGIN, req.encode()).await {
            Outcome::Ok(b) => BeginResponse::decode(&b).unwrap().tx_id,
            other => panic!("BEGIN: {other:?}"),
        }
    }

    async fn end_tx(&mut self, tx_id: u64, method: u16) {
        let o = self
            .call(service::TX, method, TxControl { tx_id }.encode())
            .await;
        assert!(matches!(o, Outcome::Ok(_)), "{o:?}");
    }

    async fn fenced(&mut self, method: u16, job: &ReservedJob) -> Result<Vec<u8>, ErrorPayload> {
        let req = FencedRequest {
            store: "jobs".into(),
            job_id: job.job_id.clone(),
            token: job.token.clone(),
            common: QueueCommon::default(),
        };
        match self.call(service::QUEUE, method, req.encode()).await {
            Outcome::Ok(b) => Ok(b),
            Outcome::Error(ep) => Err(ep),
            other => panic!("{other:?}"),
        }
    }
}

fn reserve_req(queues: &[&str], max_jobs: u16, wait_ms: u32, timeout_ms: Option<u32>) -> Vec<u8> {
    ReserveRequest {
        store: "jobs".into(),
        queues: queues.iter().map(|q| q.to_string()).collect(),
        max_jobs,
        wait_ms,
        liveness: false,
        common: QueueCommon {
            timeout_ms,
            ..QueueCommon::default()
        },
    }
    .encode()
}

fn enqueue_req(queue: &str, delay_s: u32, tx_id: Option<u64>) -> Vec<u8> {
    EnqueueRequest {
        store: "jobs".into(),
        jobs: vec![EnqueueJob {
            queue: queue.into(),
            payload: "{}".into(),
            delay_s,
        }],
        dedup_key: None,
        common: QueueCommon {
            tx_id,
            ..QueueCommon::default()
        },
    }
    .encode()
}

fn jobs_of(o: &Outcome) -> Vec<ReservedJob> {
    match o {
        Outcome::Ok(b) => ReserveResponse::decode(b).unwrap().jobs,
        other => panic!("expected a RESERVE success, got {other:?}"),
    }
}

fn id_of(job: &ReservedJob) -> i64 {
    JobId::decode(&job.job_id).unwrap().0
}

// ---------------------------------------------------------------------------------------------
// Wake-ups
// ---------------------------------------------------------------------------------------------

/// A parked RESERVE is woken by another session's AUTOCOMMIT ENQUEUE through the local hint — the
/// poll is ten minutes away, so nothing else could serve it — and costs exactly one hint sweep.
#[tokio::test]
async fn a_parked_reserve_is_woken_by_an_autocommit_enqueue() {
    let Some(w) = World::new("wake_ac", &[]).await else {
        return;
    };
    let mut worker = w.session().await;
    let mut producer = w.session().await;
    let rid = worker.reserve_send(&["default"], 20_000).await;
    w.parked(1).await;
    let m = w.waker().metrics();
    assert_eq!(
        m.polls(Trigger::Arrival),
        1,
        "its arrival sweep found nothing"
    );
    let sent = Instant::now();
    let id = producer.enqueue("default", None).await;
    let o = worker.reserve_result(rid, Duration::from_secs(5)).await;
    let latency = sent.elapsed();
    let jobs = jobs_of(&o);
    assert_eq!(jobs.len(), 1);
    assert_eq!(id_of(&jobs[0]), id);
    assert_eq!(jobs[0].attempts, 1);
    assert!(
        latency < Duration::from_secs(3),
        "woken by the hint: {latency:?}"
    );
    assert_eq!(m.polls(Trigger::Hint), 1, "one sweep for the wake");
    assert_eq!(m.polls(Trigger::Interval), 0);
    assert_eq!(m.hints(HintSource::Autocommit), 1);
    let (attempts, reserved_at) = w.row(id).await.unwrap();
    assert_eq!(attempts, 1);
    assert!(reserved_at.is_some(), "delivered: reserved");
    assert_eq!(w.waker().waiters(), 0, "deregistered");
    w.drop_schema().await;
}

/// A committed in-transaction ENQUEUE wakes a parked RESERVE at COMMIT (§24.5 step 4); a rolled-back
/// one wakes nobody — the waiter's wait simply expires with `Ok{jobs: []}` (no poll is due).
#[tokio::test]
async fn a_committed_tx_enqueue_wakes_and_a_rolled_back_one_does_not() {
    let Some(w) = World::new("wake_tx", &[]).await else {
        return;
    };
    let mut worker = w.session().await;
    let mut producer = w.session().await;
    let m = Arc::clone(w.waker().metrics());

    let rid = worker.reserve_send(&["default"], 1_500).await;
    w.parked(1).await;
    let tx = producer.begin().await;
    producer.enqueue("default", Some(tx)).await;
    producer.end_tx(tx, method_tx::ROLLBACK).await;
    let o = worker.reserve_result(rid, Duration::from_secs(5)).await;
    assert!(jobs_of(&o).is_empty(), "the rolled-back job never existed");
    assert_eq!(m.polls(Trigger::Hint), 0, "a rollback fires no hint");
    assert_eq!(m.hints(HintSource::AfterCommit), 0);

    let rid = worker.reserve_send(&["default"], 20_000).await;
    w.parked(1).await;
    let tx = producer.begin().await;
    let id = producer.enqueue("default", Some(tx)).await;
    assert_eq!(m.polls(Trigger::Hint), 0, "nothing fires before COMMIT");
    let committed = Instant::now();
    producer.end_tx(tx, method_tx::COMMIT).await;
    let o = worker.reserve_result(rid, Duration::from_secs(5)).await;
    assert_eq!(jobs_of(&o).iter().map(id_of).collect::<Vec<_>>(), vec![id]);
    assert!(committed.elapsed() < Duration::from_secs(3));
    assert_eq!(m.hints(HintSource::AfterCommit), 1);
    assert_eq!(m.polls(Trigger::Hint), 1);
    w.drop_schema().await;
}

/// A `delay_s = 0` autocommit RELEASE wakes the queue its row is in (the statement returns it,
/// M7-G3); the released job reaches the parked waiter under its new id with `attempts` kept.
#[tokio::test]
async fn a_delay_zero_release_wakes_its_queue() {
    let Some(w) = World::new("wake_rel", &[]).await else {
        return;
    };
    let mut a = w.session().await;
    let mut worker = w.session().await;
    let id = a.enqueue("emails", None).await;
    let job = jobs_of(
        &a.call(
            service::QUEUE,
            method_queue::RESERVE,
            reserve_req(&["emails"], 1, 0, None),
        )
        .await,
    )
    .pop()
    .unwrap();
    assert_eq!(id_of(&job), id);
    let rid = worker.reserve_send(&["emails"], 20_000).await;
    w.parked(1).await;
    let req = ferro_proto::messages::ReleaseRequest {
        store: "jobs".into(),
        job_id: job.job_id.clone(),
        token: job.token.clone(),
        delay_s: 0,
        common: QueueCommon::default(),
    };
    let o = a
        .call(service::QUEUE, method_queue::RELEASE, req.encode())
        .await;
    assert!(matches!(o, Outcome::Ok(_)), "{o:?}");
    let got = jobs_of(&worker.reserve_result(rid, Duration::from_secs(5)).await);
    assert_eq!(got.len(), 1);
    assert_ne!(id_of(&got[0]), id, "a new id");
    assert_eq!(got[0].attempts, 2, "attempts kept, plus this delivery");
    assert_eq!(
        w.waker().metrics().hints(HintSource::Autocommit),
        2,
        "ENQUEUE + RELEASE"
    );
    w.drop_schema().await;
}

/// Priority at arrival: a RESERVE on `[high, default]` with both non-empty takes `high`'s job.
#[tokio::test]
async fn a_waiting_reserve_honours_queue_priority_at_arrival() {
    let Some(w) = World::new("prio", &[]).await else {
        return;
    };
    let mut c = w.session().await;
    let low = w.put("default").await;
    let high = w.put("high").await;
    let rid = c.reserve_send(&["high", "default"], 5_000).await;
    let o = c.reserve_result(rid, Duration::from_secs(5)).await;
    assert_eq!(
        jobs_of(&o).iter().map(id_of).collect::<Vec<_>>(),
        vec![high]
    );
    let rid = c.reserve_send(&["high", "default"], 5_000).await;
    let o = c.reserve_result(rid, Duration::from_secs(5)).await;
    assert_eq!(jobs_of(&o).iter().map(id_of).collect::<Vec<_>>(), vec![low]);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// The wait bound, CANCEL and the request deadline
// ---------------------------------------------------------------------------------------------

/// §24.8's normative wait bound: an empty wait answers `Ok{jobs: []}` no earlier than `wait_ms` and no
/// later than `wait_ms + grace` (plus scheduling slack); `MAX_WAIT_MS` clamps it; a request
/// `timeout_ms` below the wait bounds it too. Nothing is reserved by any of them.
#[tokio::test]
async fn the_wait_bound_holds_and_an_expired_wait_reserves_nothing() {
    let Some(w) = World::new("bound", &[("FERRO_QUEUE_JOBS_MAX_WAIT_MS", "3000")]).await else {
        return;
    };
    let mut c = w.session().await;
    let grace = Duration::from_millis(u64::from(ferro_proto::consts::QUEUE_WAIT_GRACE_MS));
    let slack = Duration::from_millis(1_500);
    for (wait_ms, timeout_ms, floor, ceiling) in [
        (
            400u32,
            None,
            400u64,
            Duration::from_millis(400) + grace + slack,
        ),
        // Clamped to MAX_WAIT_MS (60 s unclamped would outlast the read below).
        (
            60_000,
            None,
            3_000,
            Duration::from_millis(3_000) + grace + slack,
        ),
        // The request deadline caps the wait AND its grace: well before MAX_WAIT_MS.
        (60_000, Some(300u32), 300, Duration::from_millis(1_500)),
    ] {
        let started = Instant::now();
        let rid = c
            .send(
                service::QUEUE,
                method_queue::RESERVE,
                reserve_req(&["empty"], 1, wait_ms, timeout_ms),
            )
            .await;
        let o = c.reserve_result(rid, Duration::from_secs(10)).await;
        let took = started.elapsed();
        assert!(jobs_of(&o).is_empty(), "{o:?}");
        assert!(
            took >= Duration::from_millis(floor),
            "{wait_ms}: answered early {took:?}"
        );
        assert!(took < ceiling, "{wait_ms}: past the bound {took:?}");
    }
    let id = w.put("empty").await;
    assert_eq!(w.row(id).await, Some((0, None)));
    w.drop_schema().await;
}

/// A client CANCEL of a parked RESERVE is `Cancelled` — no reservation statement completed for it
/// (§24.4) — the session lives on, and a job enqueued afterwards is still available to anyone.
#[tokio::test]
async fn a_cancelled_parked_reserve_is_cancelled_and_reserves_nothing() {
    let Some(w) = World::new("cancel_parked", &[]).await else {
        return;
    };
    let mut c = w.session().await;
    let rid = c.reserve_send(&["default"], 20_000).await;
    w.parked(1).await;
    c.c.cancel(rid).await;
    let o = c.reserve_result(rid, Duration::from_secs(3)).await;
    assert!(matches!(o, Outcome::Cancelled), "{o:?}");
    let mut p = w.session().await;
    let id = p.enqueue("default", None).await;
    assert_eq!(w.row(id).await, Some((0, None)), "nobody holds it");
    common::assert_session_alive(&mut c.c, 7).await;
    assert_eq!(w.waker().waiters(), 0);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Unreserve (§24.8): every way a reservation can miss its waiter
// ---------------------------------------------------------------------------------------------

/// A sweep is held in flight by the trigger; the waiter it serves is CANCELled meanwhile. The terminal
/// is `Cancelled` at once (not the sweep's jobs, §24.4), and the job the sweep reserved for it is
/// UNRESERVED: back to `attempts = 0, reserved_at = NULL`, counted with cause `cancel`.
#[tokio::test]
async fn a_cancel_during_the_sweep_unreserves_its_job() {
    let Some(w) = World::new("unr_cancel", &[]).await else {
        return;
    };
    w.slow(800).await;
    let id = w.put("default").await;
    let mut c = w.session().await;
    let rid = c.reserve_send(&["default"], 20_000).await;
    w.sweep_in_flight().await;
    let started = Instant::now();
    c.c.cancel(rid).await;
    let o = c.reserve_result(rid, Duration::from_secs(5)).await;
    assert!(matches!(o, Outcome::Cancelled), "{o:?}");
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "not held by the sweep"
    );
    // The counter first: it moves only once the unreserve statement restored a row, which proves
    // the reservation it undid was COMMITTED. (A row read while the sweep still sleeps in the trigger
    // shows the uncommitted pre-reservation state and would prove nothing.)
    let m = w.waker().metrics();
    eventually(
        || m.unreserved_count(UnreserveCause::Cancel) == 1,
        "counted: cancel",
    )
    .await;
    assert_eq!(w.row(id).await, Some((0, None)), "restored exactly");
    assert_eq!(m.unreserve_failed_count(), 0);
    w.drop_schema().await;
}

/// The request's own deadline bounds a sweep in flight too: with `timeout_ms` far below
/// `wait_ms + grace`, the waiter is answered `Ok{jobs: []}` AT its deadline while the sweep that is
/// serving it still sleeps in the trigger, and the job that sweep reserved is unreserved (`deadline`).
#[tokio::test]
async fn the_request_deadline_bounds_a_sweep_in_flight() {
    let Some(w) = World::new("unr_req_deadline", &[]).await else {
        return;
    };
    w.slow(2_000).await;
    let id = w.put("default").await;
    let mut c = w.session().await;
    let started = Instant::now();
    let rid = c
        .send(
            service::QUEUE,
            method_queue::RESERVE,
            reserve_req(&["default"], 1, 20_000, Some(400)),
        )
        .await;
    let o = c.reserve_result(rid, Duration::from_secs(5)).await;
    let took = started.elapsed();
    assert!(jobs_of(&o).is_empty(), "{o:?}");
    assert!(took >= Duration::from_millis(400), "{took:?}");
    assert!(
        took < Duration::from_millis(1_500),
        "at the deadline, not the sweep: {took:?}"
    );
    let m = w.waker().metrics();
    eventually(
        || m.unreserved_count(UnreserveCause::Deadline) == 1,
        "counted: deadline",
    )
    .await;
    assert_eq!(w.row(id).await, Some((0, None)), "restored exactly");
    w.drop_schema().await;
}

/// The grace bound: a sweep still running at `wait_ms + grace` does not hold the answer. The waiter
/// gets `Ok{jobs: []}` on time, and the job the late sweep reserved is unreserved (cause `deadline`).
#[tokio::test]
async fn a_sweep_past_the_grace_bound_is_answered_empty_and_unreserved() {
    let Some(w) = World::new("unr_deadline", &[]).await else {
        return;
    };
    w.slow(3_000).await;
    let id = w.put("default").await;
    let mut c = w.session().await;
    let started = Instant::now();
    let rid = c.reserve_send(&["default"], 100).await;
    let o = c.reserve_result(rid, Duration::from_secs(5)).await;
    let took = started.elapsed();
    assert!(jobs_of(&o).is_empty(), "{o:?}");
    assert!(
        took < Duration::from_millis(2_500),
        "within wait + grace: {took:?}"
    );
    // The counter first: it moves only once the unreserve statement restored a row, which proves
    // the reservation it undid was COMMITTED. (A row read while the sweep still sleeps in the trigger
    // shows the uncommitted pre-reservation state and would prove nothing.)
    let m = w.waker().metrics();
    eventually(
        || m.unreserved_count(UnreserveCause::Deadline) == 1,
        "counted: deadline",
    )
    .await;
    assert_eq!(w.row(id).await, Some((0, None)), "restored exactly");
    w.drop_schema().await;
}

/// Session TEARDOWN at hand-off (§24.4): a sweep is in flight when the worker's connection drops — a
/// SIGKILLed worker. Nobody can receive the job, so it is unreserved (cause `teardown`). Proven for a
/// waiting RESERVE (served by the waker) and a non-waiting one (served on its own checkout, whose
/// statement teardown lets finish rather than interrupt, so its jobs are known and restorable).
#[tokio::test]
async fn a_dropped_session_never_keeps_its_reservation() {
    let Some(w) = World::new("unr_teardown", &[]).await else {
        return;
    };
    w.slow(800).await;
    let m = Arc::clone(w.waker().metrics());
    for (n, (wait_ms, queue)) in [(20_000u32, "waiting"), (0, "immediate")]
        .into_iter()
        .enumerate()
    {
        let id = w.put(queue).await;
        let mut c = w.session().await;
        c.reserve_send(&[queue], wait_ms).await;
        w.sweep_in_flight().await;
        drop(c);
        eventually(
            || m.unreserved_count(UnreserveCause::Teardown) == n as u64 + 1,
            "counted: teardown",
        )
        .await;
        assert_eq!(w.row(id).await, Some((0, None)), "restored exactly");
    }
    assert_eq!(w.waker().metrics().reserve_unconfirmed_count(), 0);
    w.drop_schema().await;
}

/// The same with a GRACEFUL teardown: the worker says GOODBYE while the sweep runs and keeps reading.
/// Its terminal is NOT the job (the session had begun teardown at hand-off) and the job is unreserved:
/// the client and the rows agree that nobody received it.
#[tokio::test]
async fn a_goodbye_during_the_sweep_is_answered_without_the_job() {
    let Some(w) = World::new("unr_goodbye", &[]).await else {
        return;
    };
    w.slow(800).await;
    let id = w.put("default").await;
    let mut c = w.session().await;
    let rid = c.reserve_send(&["default"], 20_000).await;
    w.sweep_in_flight().await;
    c.c.goodbye().await;
    let o = c.reserve_result(rid, Duration::from_secs(5)).await;
    assert!(matches!(o, Outcome::Cancelled), "{o:?}");
    let m = w.waker().metrics();
    eventually(
        || m.unreserved_count(UnreserveCause::Teardown) == 1,
        "counted: teardown",
    )
    .await;
    assert_eq!(w.row(id).await, Some((0, None)), "restored exactly");
    w.drop_schema().await;
}

/// **The G1b carry (§24.8 × EXTEND).** Unreserve restores `attempts − 1`, so the PREVIOUS holder's
/// token matches the row again. Its EXTEND must not re-reserve the now-PENDING job: it is `LeaseLost`
/// and the row stays available. Its late ACK is honoured — the pre-reservation state exactly ("a late
/// ACK is honoured when nobody else took the job"; the undelivered reservation took it from nobody).
/// The control: without an unreserve, the same expired lease IS retaken by EXTEND (§24.4).
#[tokio::test]
async fn after_an_unreserve_the_previous_holders_extend_is_lease_lost() {
    let Some(w) = World::new("extend_hole", &[]).await else {
        return;
    };
    let mut a = w.session().await;
    let reserve0 = reserve_req(&["default"], 1, 0, None);

    // Control: an expired lease nobody re-took is retaken by its holder's EXTEND.
    let ctl = w.put("ctl").await;
    let ctl_job = jobs_of(
        &a.call(
            service::QUEUE,
            method_queue::RESERVE,
            reserve_req(&["ctl"], 1, 0, None),
        )
        .await,
    )
    .pop()
    .unwrap();
    assert_eq!(id_of(&ctl_job), ctl);
    w.raw
        .execute(
            &format!(
                "UPDATE {} SET reserved_at = reserved_at - 100 WHERE id = $1",
                w.table
            ),
            &[&ctl],
        )
        .await
        .unwrap();
    assert!(
        a.fenced(method_queue::EXTEND, &ctl_job).await.is_ok(),
        "the control retakes"
    );

    // The job: A holds attempt 1; its lease expires.
    let id = w.put("default").await;
    let job = jobs_of(
        &a.call(service::QUEUE, method_queue::RESERVE, reserve0.clone())
            .await,
    )
    .pop()
    .unwrap();
    assert_eq!((id_of(&job), job.attempts), (id, 1));
    w.raw
        .execute(
            &format!(
                "UPDATE {} SET reserved_at = reserved_at - 100 WHERE id = $1",
                w.table
            ),
            &[&id],
        )
        .await
        .unwrap();
    // A waiting RESERVE reserves it again (attempt 2) and is CANCELled mid-sweep: unreserved.
    w.slow(800).await;
    let mut b = w.session().await;
    let rid = b.reserve_send(&["default"], 20_000).await;
    w.sweep_in_flight().await;
    b.c.cancel(rid).await;
    assert!(matches!(
        b.reserve_result(rid, Duration::from_secs(5)).await,
        Outcome::Cancelled
    ));
    w.until_row(id, Some((1, None))).await;
    w.slow(0).await;

    // A's token (attempt 1) names the row again. EXTEND: LeaseLost, and the job stays PENDING.
    let ep = a
        .fenced(method_queue::EXTEND, &job)
        .await
        .expect_err("LeaseLost");
    assert_eq!(
        (ep.code, ep.branch),
        (errc::LEASE_LOST, errc::LEASE_LOST_BRANCH),
        "{ep:?}"
    );
    assert_eq!(w.row(id).await, Some((1, None)), "not re-reserved");
    // Its late ACK is honoured: the job ran (A ran it) and nobody else ever received it.
    let b = a.fenced(method_queue::ACK, &job).await.expect("acked");
    assert_eq!(AckResponse::decode(&b).unwrap().outcome, ack_outcome::ACKED);
    assert_eq!(w.row(id).await, None);
    w.drop_schema().await;
}

/// The unreserve statement's fence, against real rows (§24.8): it restores a reservation only when
/// `id`, the minted `attempts`, `created_at` AND the stamped `reserved_at` all match. Run through the
/// engine's own builder on a raw connection, so the SQL is the shipped SQL.
#[tokio::test]
async fn the_unreserve_statement_is_fenced_on_all_four_values() {
    let Some(w) = World::new("unr_fence", &[]).await else {
        return;
    };
    let mut a = w.session().await;
    let id = w.put("default").await;
    let job = jobs_of(
        &a.call(
            service::QUEUE,
            method_queue::RESERVE,
            reserve_req(&["default"], 1, 0, None),
        )
        .await,
    )
    .pop()
    .unwrap();
    let token = ferro_queue::sql::Token::decode(&job.token).unwrap();
    let reserved_at = job.lease_deadline - 30 - 1;
    let table = ferro_queue::ident::TableName::parse(&w.table).unwrap();
    let run = |u: ferro_queue::pg::Unreserve| {
        let stmt = ferro_queue::pg::unreserve(&table, &[u]);
        let (i, a, c, r) = match stmt.params.as_slice() {
            [
                ferro_proto::value::Value::I64(i),
                ferro_proto::value::Value::I64(a),
                ferro_proto::value::Value::I64(c),
                ferro_proto::value::Value::I64(r),
            ] => (*i, *a as i16, *c as i32, *r as i32),
            other => panic!("{other:?}"),
        };
        let raw = &w.raw;
        async move { raw.query(&stmt.sql, &[&i, &a, &c, &r]).await.unwrap().len() }
    };
    let right = ferro_queue::pg::Unreserve {
        id: JobId(id),
        token,
        reserved_at,
    };
    let stale_at = ferro_queue::pg::Unreserve {
        reserved_at: reserved_at - 1,
        ..right
    };
    let stale_attempts = ferro_queue::pg::Unreserve {
        token: ferro_queue::sql::Token::from_pg(token.pg_created_at(), 2),
        ..right
    };
    let other_created = ferro_queue::pg::Unreserve {
        token: ferro_queue::sql::Token::from_pg(token.pg_created_at() - 1, 1),
        ..right
    };
    for wrong in [stale_at, stale_attempts, other_created] {
        assert_eq!(run(wrong).await, 0, "{wrong:?}");
        assert_eq!(w.row(id).await.unwrap().0, 1, "untouched");
    }
    assert_eq!(run(right).await, 1);
    assert_eq!(w.row(id).await, Some((0, None)), "restored");
    assert_eq!(run(right).await, 0, "a repeat is a no-op");
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Coalescing and drain
// ---------------------------------------------------------------------------------------------

/// Single flight per `(store, queue)`: twenty hints that arrive while one sweep is in flight leave ONE
/// follow-up sweep, not twenty.
#[tokio::test]
async fn hints_during_a_sweep_coalesce_into_one_follow_up() {
    let Some(w) = World::new("coalesce", &[]).await else {
        return;
    };
    let mut w1 = w.session().await;
    let mut w2 = w.session().await;
    let mut p = w.session().await;
    let r1 = w1.reserve_send(&["default"], 20_000).await;
    let r2 = w2.reserve_send(&["default"], 20_000).await;
    w.parked(2).await;
    w.slow(800).await;
    p.enqueue("default", None).await; // a hint: one sweep for both waiters, held in the trigger
    w.sweep_in_flight().await;
    let m = Arc::clone(w.waker().metrics());
    assert_eq!(m.polls(Trigger::Hint), 1);
    for _ in 0..20 {
        p.enqueue("default", None).await;
    }
    assert_eq!(m.polls(Trigger::Hint), 1, "still one: the rest are pending");
    let a = jobs_of(&w1.reserve_result(r1, Duration::from_secs(10)).await);
    let b = jobs_of(&w2.reserve_result(r2, Duration::from_secs(10)).await);
    assert_eq!(a.len() + b.len(), 2, "both served");
    assert_eq!(m.hints(HintSource::Autocommit), 21);
    assert!(
        m.polls(Trigger::Hint) <= 2,
        "twenty hints during a sweep coalesced: {}",
        m.polls(Trigger::Hint)
    );
    w.drop_schema().await;
}

/// SIGTERM drain (§24.8): a parked RESERVE gets `Ok{jobs: []}` at once, a new waiting RESERVE is served
/// as a non-waiting one, and NOTHING is released — a delivered job keeps its lease, and its holder's
/// ACK still lands.
#[tokio::test]
async fn drain_answers_parked_waiters_and_releases_nothing() {
    let Some(w) = World::new("drain", &[]).await else {
        return;
    };
    let mut a = w.session().await;
    let id = w.put("default").await;
    let job = jobs_of(
        &a.call(
            service::QUEUE,
            method_queue::RESERVE,
            reserve_req(&["default"], 1, 0, None),
        )
        .await,
    )
    .pop()
    .unwrap();
    let mut c = w.session().await;
    let rid = c.reserve_send(&["default"], 20_000).await;
    w.parked(1).await;
    let started = Instant::now();
    w.drain.trigger();
    let o = c.reserve_result(rid, Duration::from_secs(3)).await;
    assert!(jobs_of(&o).is_empty());
    assert!(started.elapsed() < Duration::from_secs(2));
    let started = Instant::now();
    let rid = c.reserve_send(&["default"], 20_000).await;
    assert!(jobs_of(&c.reserve_result(rid, Duration::from_secs(3)).await).is_empty());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "draining: never parks"
    );
    let (attempts, reserved_at) = w.row(id).await.unwrap();
    assert_eq!(attempts, 1);
    assert!(reserved_at.is_some(), "nothing is released at drain");
    let b = a.fenced(method_queue::ACK, &job).await.expect("acked");
    assert_eq!(AckResponse::decode(&b).unwrap().outcome, ack_outcome::ACKED);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Chaos row 11: the parked-waiter cost bound
// ---------------------------------------------------------------------------------------------

/// **Chaos row 11.** 200 parked RESERVEs hold zero pool connections — asserted on the pool's gauges:
/// nothing pinned, nothing waiting for a permit, and at most one poll's sweep in use (plus the pool's
/// own liveness reaper) —
/// and cost one sweep per `POLL_MS` for their queue, not one per waiter. The measured statement rate
/// is printed beside stock Laravel's for the same 200 idle workers at its default `--sleep 3`.
#[tokio::test]
async fn two_hundred_parked_reserves_hold_no_connection_and_cost_one_poll_per_tick() {
    let Some(w) = World::new("cost", &[("FERRO_QUEUE_JOBS_POLL_MS", "200")]).await else {
        return;
    };
    const N: usize = 200;
    let mut sessions = Vec::with_capacity(N);
    for _ in 0..N {
        let mut s = w.session().await;
        let rid = s.reserve_send(&["idle"], 30_000).await;
        sessions.push((s, rid));
    }
    w.parked(N).await;
    let pool = w.registry.get("default").unwrap();
    let m = Arc::clone(w.waker().metrics());
    let polls0 = m.polls_total();
    let checkouts0 = pool.checkout_histogram().count;
    let started = Instant::now();
    let mut max_in_use = 0;
    while started.elapsed() < Duration::from_secs(3) {
        let g = pool.gauges();
        assert_eq!(g.pinned, 0, "a parked waiter pins nothing");
        assert_eq!(g.waiting, 0, "nor waits for a permit");
        max_in_use = max_in_use.max(g.in_use);
        // At most the one poll's sweep — and the pool's liveness reaper, which may hold an idle
        // connection while it pings it. 200 waiters holding connections would read 200 here.
        assert!(g.in_use <= 2, "at most the one poll's sweep: {}", g.in_use);
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let elapsed = started.elapsed();
    let polls = m.polls_total() - polls0;
    let checkouts = pool.checkout_histogram().count - checkouts0;
    let ticks = elapsed.as_millis() / 200 + 1;
    assert!(
        u128::from(polls) <= ticks,
        "{polls} sweeps in {elapsed:?}: at most one per tick ({ticks})"
    );
    assert!(polls >= 2, "the poll runs: {polls}");
    assert_eq!(checkouts, polls, "every checkout is one poll's sweep");
    eprintln!(
        "chaos row 11 (local): {N} parked waiters, POLL_MS=200: {polls} statements in {elapsed:?} \
         = {:.1}/s (max in_use {max_in_use}); stock `database` driver, {N} idle workers at \
         --sleep 3: {:.1} pinned transactions/s",
        polls as f64 / elapsed.as_secs_f64(),
        N as f64 / 3.0
    );
    assert_eq!(w.waker().waiters(), N);
    for (mut s, rid) in sessions {
        s.c.cancel(rid).await;
        assert!(matches!(
            s.reserve_result(rid, Duration::from_secs(5)).await,
            Outcome::Cancelled
        ));
    }
    assert_eq!(w.waker().waiters(), 0);
    assert_eq!(m.waiters(), 0);
    w.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Chaos row 12: deliver xor unreserve
// ---------------------------------------------------------------------------------------------

/// What one worker saw.
#[derive(Default)]
struct Seen {
    jobs: Vec<i64>,
    waits: usize,
}

/// One worker's loop: waits of four kinds, each racing the producers — a plain wait, a wait CANCELled
/// after a random delay, a wait whose `wait_ms` expires almost at once, and a wait abandoned by a
/// GOODBYE (the session is then replaced). Every job any terminal carries is recorded.
async fn worker_loop(socket: PathBuf, queue: String, iterations: usize, seed: u64) -> Seen {
    let mut seen = Seen::default();
    let mut rng = seed;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let connect = |socket: PathBuf| async move {
        let mut c = common::connect(&socket).await;
        c.hello(1).await;
        Q { c, rid: 10 }
    };
    let mut q = connect(socket.clone()).await;
    for i in 0..iterations {
        seen.waits += 1;
        let kind = (next() % 4) as usize;
        let wait_ms = match kind {
            2 => 1 + (next() % 30) as u32,
            _ => 300 + (next() % 300) as u32,
        };
        let rid = q.reserve_send(&[queue.as_str()], wait_ms).await;
        let delay = Duration::from_millis(next() % 40);
        match kind {
            1 => {
                tokio::time::sleep(delay).await;
                q.c.cancel(rid).await;
            }
            3 => {
                tokio::time::sleep(delay).await;
                q.c.goodbye().await;
            }
            _ => {}
        }
        let o = q.reserve_result(rid, Duration::from_secs(10)).await;
        match o {
            Outcome::Ok(b) => seen
                .jobs
                .extend(ReserveResponse::decode(&b).unwrap().jobs.iter().map(id_of)),
            Outcome::Cancelled => {}
            // A CANCEL or teardown that lands before the request is parked (while it waits for its
            // checkout) is a KNOWN non-execution; never `Indeterminate`.
            Outcome::Error(ep) if kind != 0 && kind != 2 => {
                assert_ne!(ep.branch, branch::INDETERMINATE, "iteration {i}: {ep:?}")
            }
            Outcome::Error(ep) => panic!("iteration {i}: {ep:?}"),
        }
        if kind == 3 {
            q.c.recv_eof().await;
            q = connect(socket.clone()).await;
        }
    }
    seen
}

/// **Chaos row 12 — no engine-made phantoms (F5), deliver xor unreserve (F16).** Thirty workers race
/// ~2 000 waits against CANCELs, near-instant `wait_ms` expiries and GOODBYE teardowns while three
/// producers enqueue 600 jobs, with every reservation slowed by the trigger to widen each window. At
/// the end, against the ROWS: no job is lost; every job a client received was received ONCE and is
/// reserved with `attempts = 1`; every job no client received is AVAILABLE with `attempts = 0` — never
/// both delivered and available, and no attempt counted for a job nobody was handed. No unreserve
/// failed and no reservation was unconfirmed, so nothing is excused.
#[tokio::test]
async fn chaos_row_12_every_job_is_delivered_once_or_still_available() {
    let Some(w) = World::new("row12", &[("FERRO_QUEUE_JOBS_POLL_MS", "100")]).await else {
        return;
    };
    // First use (version gate, shape verification) once, before the race.
    let mut warm = w.session().await;
    jobs_of(
        &warm
            .call(
                service::QUEUE,
                method_queue::RESERVE,
                reserve_req(&["chaos"], 1, 0, None),
            )
            .await,
    );
    // Every reservation sleeps 40 ms per row in the trigger, so sweeps are in flight often enough for
    // cancels and teardowns to land inside them (asserted below: the run is not vacuous).
    w.slow(40).await;
    const WORKERS: usize = 30;
    const ITERATIONS: usize = 70;
    const JOBS: usize = 600;
    let mut tasks = Vec::new();
    for i in 0..WORKERS {
        // Workers 0..20 share a queue in pairs: a job a vanished waiter cannot take goes to its
        // partner when the partner is waiting (§24.8's hand-out). Workers 20..30 are alone, so theirs is UNRESERVED.
        tasks.push(tokio::spawn(worker_loop(
            w.socket.clone(),
            format!("chaos{}", if i < 20 { i / 2 } else { i - 10 }),
            ITERATIONS,
            0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1),
        )));
    }
    let mut producers = Vec::new();
    for n in 0..3 {
        let socket = w.socket.clone();
        producers.push(tokio::spawn(async move {
            let mut p = common::connect(&socket).await;
            p.hello(1).await;
            let mut p = Q { c: p, rid: 10 };
            for j in 0..JOBS / 3 {
                p.enqueue(&format!("chaos{}", (n + j * 3) % 20), None).await;
                tokio::time::sleep(Duration::from_millis(8)).await;
            }
        }));
    }
    for p in producers {
        p.await.unwrap();
    }
    let mut received: HashMap<i64, usize> = HashMap::new();
    let mut waits = 0;
    for t in tasks {
        let seen = t.await.unwrap();
        waits += seen.waits;
        for id in seen.jobs {
            *received.entry(id).or_default() += 1;
        }
    }
    // Every waiter has ended; wait until no sweep and no unreserve is outstanding, so the rows read
    // next are final (a sweep still in its trigger shows uncommitted, pre-reservation rows).
    let m = Arc::clone(w.waker().metrics());
    let waker = Arc::clone(w.waker());
    eventually(|| waker.settled(), "the waker settles").await;
    let rows = w.all_rows().await;
    assert_eq!(rows.len(), JOBS, "no job lost");
    for (id, n) in &received {
        assert_eq!(*n, 1, "job {id} delivered {n} times");
        let (attempts, reserved_at) = rows[id];
        assert_eq!(attempts, 1, "job {id}: one delivery, one attempt");
        assert!(reserved_at.is_some(), "job {id} delivered AND available");
    }
    for (id, (attempts, reserved_at)) in &rows {
        if !received.contains_key(id) {
            assert_eq!(
                (*attempts, *reserved_at),
                (0, None),
                "job {id}: nobody received it, so it must be available with no attempt counted"
            );
        }
    }
    assert_eq!(m.unreserve_failed_count(), 0);
    assert_eq!(m.reserve_unconfirmed_count(), 0);
    let unreserved: u64 = [
        UnreserveCause::Cancel,
        UnreserveCause::Deadline,
        UnreserveCause::Teardown,
    ]
    .iter()
    .map(|c| m.unreserved_count(*c))
    .sum();
    eprintln!(
        "chaos row 12: {waits} waits, {} jobs delivered, {} still available, {unreserved} \
         unreserved (cancel {}, deadline {}, teardown {}), polls {}",
        received.len(),
        JOBS - received.len(),
        m.unreserved_count(UnreserveCause::Cancel),
        m.unreserved_count(UnreserveCause::Deadline),
        m.unreserved_count(UnreserveCause::Teardown),
        m.polls_total()
    );
    assert!(waits >= 2_000);
    assert!(
        m.unreserved_count(UnreserveCause::Cancel) + m.unreserved_count(UnreserveCause::Teardown)
            > 0,
        "the run must have raced cancels or teardowns against in-flight sweeps"
    );
    assert_eq!(w.waker().waiters(), 0);
    w.drop_schema().await;
}

/// Chaos row 12's SIGKILL half: workers that DROP their connection mid-wait. A terminal the engine
/// handed to a live writer an instant before the drop is delivered to a socket nobody reads — §24.7's
/// stock-equivalent residual — so this run asserts what still holds exactly: no job lost, no job
/// delivered twice or carrying more than one attempt, and every job neither received nor reserved by
/// such a race is available.
#[tokio::test]
async fn chaos_row_12_dropped_sessions_lose_nothing() {
    let Some(w) = World::new("row12_drop", &[("FERRO_QUEUE_JOBS_POLL_MS", "100")]).await else {
        return;
    };
    w.slow(20).await;
    const JOBS: usize = 200;
    let mut p = w.session().await;
    for j in 0..JOBS {
        p.enqueue(&format!("drop{}", j % 20), None).await;
    }
    let mut tasks = Vec::new();
    for i in 0..20u64 {
        let socket = w.socket.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            let mut drops = 0usize;
            for j in 0..30u64 {
                let mut c = common::connect(&socket).await;
                c.hello(1).await;
                let mut q = Q { c, rid: 10 };
                let queue = format!("drop{i}");
                let rid = q.reserve_send(&[queue.as_str()], 500).await;
                if (i + j) % 2 == 0 {
                    tokio::time::sleep(Duration::from_millis((i * 7 + j * 3) % 20)).await;
                    drop(q);
                    drops += 1;
                } else {
                    let o = q.reserve_result(rid, Duration::from_secs(10)).await;
                    got.extend(jobs_of(&o).iter().map(id_of));
                }
            }
            (got, drops)
        }));
    }
    let mut received: HashMap<i64, usize> = HashMap::new();
    let mut drops = 0;
    for t in tasks {
        let (got, d) = t.await.unwrap();
        drops += d;
        for id in got {
            *received.entry(id).or_default() += 1;
        }
    }
    // Wait until no sweep and no unreserve is outstanding, so the rows read next are final.
    let waker = Arc::clone(w.waker());
    eventually(|| waker.settled(), "the waker settles").await;
    let rows = w.all_rows().await;
    assert_eq!(rows.len(), JOBS, "no job lost");
    let mut phantoms = 0;
    for (id, (attempts, reserved_at)) in &rows {
        assert!(*attempts <= 1, "job {id}: {attempts} attempts");
        match received.get(id) {
            Some(n) => {
                assert_eq!(*n, 1);
                assert!(reserved_at.is_some());
            }
            None if *attempts == 1 => phantoms += 1,
            None => assert!(reserved_at.is_none()),
        }
    }
    eprintln!(
        "chaos row 12 (drops): {drops} dropped sessions, {} delivered, {phantoms} handed to a \
         writer whose peer had gone (stock-equivalent), teardown unreserves {}",
        received.len(),
        w.waker()
            .metrics()
            .unreserved_count(UnreserveCause::Teardown)
    );
    assert!(phantoms <= drops, "{phantoms} > {drops}");
    w.drop_schema().await;
}
