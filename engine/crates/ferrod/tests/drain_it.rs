//! M1-S9a Task 12 — SIGTERM's graceful drain, live (M0-core-review finding 6). Before this slice
//! the `Drain` token stopped `accept()` and NOTHING else: live sessions kept dispatching new
//! checkout-acquiring work for the whole window and were then `abort_all()`ed mid-flight — no
//! terminals, no engine-side rollback, the writer droppable mid-frame. §18's systemd socket
//! activation assumes the opposite.
//!
//! The contract these two tests pin:
//!   * new checkout-ACQUIRING work (autocommit EXEC, BEGIN) is refused `POOL_TIMEOUT{Retryable}`
//!     with a real terminal — never a silent drop, never a hang;
//!   * a PINNED transaction keeps working for the whole `drain_deadline` window and can COMMIT;
//!   * at `drain_deadline` the session winds ITSELF down through the ordinary cleanup path
//!     (`registry.cancel_all()` → `tx_registry.abort_session()` → `drain_supervisors()` → writer
//!     flush), so a statement still in flight receives its ONE terminal BEFORE the socket closes
//!     (charter rule 4 applies to shutdown), and only then does the client see EOF.
//!
//! Both tests SKIP (do not fail) when `FERRO_TEST_PG_URL` is unset — same discipline as
//! `chaos_fate_it.rs`.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{TestClient, connect, exec_err, exec_ok, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_sql, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};
use ferro_proto::value::Value;
use ferrod::config::{Config, PoolSpec, infer_pool_kind};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;
use tokio::task::JoinHandle;

/// The drain window both tests configure. Short enough to keep the suite fast, long enough that
/// the three post-trigger round trips of test 1 comfortably fit inside it.
const DRAIN_DEADLINE: Duration = Duration::from_secs(2);

/// The session must NOT close before the drain window is actually over: a session that observed
/// the drain and immediately gave up would take a pinned transaction down with it. Comfortably
/// below `DRAIN_DEADLINE`.
const EOF_MIN: Duration = Duration::from_millis(1500);

/// …and it must close by ITSELF, well before `serve`'s hard-abort backstop at
/// `drain_deadline + SESSION_DRAIN_GRACE` (2s + 3s = 5s). A session that only dies to that abort
/// (the pre-S9a behaviour, and named mutation 2 — deleting the session's drain arm) closes at ~5s
/// and fails this bound. The margin is ~1.5s on both sides, which is what makes the guard tight
/// AND non-flaky; see the journal's S4 measurement for why the plan's original 3s-around-`recv_eof`
/// bound could not pass at all.
const EOF_MAX: Duration = Duration::from_millis(3500);

static SOCK_N: AtomicU64 = AtomicU64::new(0);

fn tx_req(sql: &str, tx_id: u64) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.tx_id = Some(tx_id);
    r
}

fn write_req(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r
}

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
        Outcome::Ok(body) => BeginResponse::decode(&body).expect("BeginResponse").tx_id,
        other => panic!("BEGIN expected Ok, got {other:?}"),
    }
}

async fn commit(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    client
        .send_request(
            rid,
            service::TX,
            method_tx::COMMIT,
            TxControl { tx_id }.encode(),
        )
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "COMMIT terminal echoes the rid");
    assert_eq!(t.header.flags & flags::END, flags::END);
    Outcome::decode(&t.payload).expect("decode COMMIT Outcome")
}

/// Hand-assembled `serve` over a REAL pool on `url`, with an injected `Drain` — `common`'s
/// `exec_server` builds its own drain internally and never exposes it, and this task's whole
/// subject is what that handle reaches. Mirrors `main`'s wiring exactly (one registry, one
/// tx registry, `make_handler` fed the SAME drain `serve` gets).
fn spawn_drained_serve(url: String) -> (Drain, PathBuf, JoinHandle<()>) {
    let n = SOCK_N.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let mut socket_path = std::env::temp_dir();
    socket_path.push(format!(
        "ferro-s9a-drain-{}-{nanos}-{n}.sock",
        std::process::id()
    ));

    let config = Config {
        socket_path: socket_path.clone(),
        drain_deadline: DRAIN_DEADLINE,
        pools: vec![PoolSpec {
            name: "default".to_string(),
            kind: infer_pool_kind(&url),
            dsn: url,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }],
        ..Config::default()
    };
    let listener = ferrod::listener::bind_uds(&config).expect("bind_uds");
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let drain = Drain::new();
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
        drain.clone(),
    );
    let served = tokio::spawn(ferrod::serve::serve(
        listener,
        config,
        BootEpoch(1),
        drain.clone(),
        registry,
        tx_registry,
        factory,
    ));

    (drain, socket_path, served)
}

/// The full §18 story on one session: drain fires mid-transaction → new checkout-acquiring work is
/// refused RETRYABLY (with a terminal — charter rule 4), the pinned tx keeps working and COMMITs,
/// and the session closes ITSELF at `drain_deadline` rather than dying to `serve`'s abort.
#[tokio::test]
async fn drain_refuses_new_work_lets_the_pinned_tx_finish_and_closes_cleanly() {
    let Some(url) = pg_url() else {
        eprintln!("skip: FERRO_TEST_PG_URL not set");
        return;
    };

    let (drain, sock, served) = spawn_drained_serve(url);
    let mut c = connect(&sock).await;
    c.hello(1).await;
    let tx_id = begin(&mut c, 2, "default").await;
    exec_ok(&mut c, 3, &tx_req("SELECT 1", tx_id)).await;

    let triggered = Instant::now();
    drain.trigger();

    // (a) NEW checkout-acquiring work is refused, retryably, with a terminal.
    let ep = exec_err(&mut c, 4, &write_req("SELECT 1")).await;
    assert_eq!(
        ep.code,
        errc::POOL_TIMEOUT,
        "an autocommit EXEC during the drain window is refused POOL_TIMEOUT, got {:#06x}: {}",
        ep.code,
        ep.message
    );
    assert_eq!(
        ep.branch,
        branch::RETRYABLE,
        "the refusal must be RETRYABLE — the client's resilience loop reconnects to the successor"
    );

    // (b) …and so is a NEW BEGIN (the other checkout-acquiring entry).
    c.send_request(
        5,
        service::TX,
        method_tx::BEGIN,
        BeginRequest {
            pool: "default".to_string(),
            isolation: None,
            readonly: false,
        }
        .encode(),
    )
    .await;
    let t = c.recv().await;
    assert_eq!(t.header.request_id, 5);
    assert_eq!(t.header.flags & flags::END, flags::END);
    match Outcome::decode(&t.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::POOL_TIMEOUT, "BEGIN refusal: {}", ep.message);
            assert_eq!(ep.branch, branch::RETRYABLE);
        }
        other => panic!("a BEGIN during drain must be refused, got {other:?}"),
    }

    // (c) The PINNED transaction is untouched: statements and COMMIT still work (§18 "let pins
    //     finish"). This is the half that makes the refusal above a policy rather than a blunt
    //     "reject everything".
    exec_ok(&mut c, 6, &tx_req("SELECT 1", tx_id)).await;
    match commit(&mut c, 7, tx_id).await {
        Outcome::Ok(_) => {}
        other => panic!("COMMIT during the drain window must succeed, got {other:?}"),
    }

    // (d) The session winds ITSELF down at drain_deadline: clean EOF, no abort. Both bounds are
    //     load-bearing — the lower one rejects "observed the drain and gave up immediately" (which
    //     would have cut the pinned tx off), the upper one rejects "only died to serve's
    //     grace-abort at drain_deadline + 3s" (the pre-S9a behaviour).
    c.recv_eof_within(EOF_MAX.saturating_sub(triggered.elapsed()))
        .await;
    let closed_at = triggered.elapsed();
    assert!(
        closed_at >= EOF_MIN,
        "the session closed after only {closed_at:?} — it must serve the whole drain window \
         (>= {EOF_MIN:?}) so a pinned transaction can finish"
    );
    assert!(
        closed_at <= EOF_MAX,
        "the session closed after {closed_at:?} — it must wind ITSELF down at drain_deadline \
         ({DRAIN_DEADLINE:?}), not survive to serve's grace-abort backstop"
    );

    tokio::time::timeout(Duration::from_secs(5), served)
        .await
        .expect("serve must return once its sessions have wound down")
        .expect("serve's task must not panic");
}

/// Charter rule 4 at shutdown: a statement STILL IN FLIGHT when the drain window expires receives
/// its ONE terminal — the session exits through its own cleanup path — and the client sees that
/// terminal BEFORE the EOF, never a truncated stream. This is also what `SESSION_DRAIN_GRACE`
/// buys: without it `serve` aborts the session task at the same instant the session starts its
/// cleanup, and the terminal is lost.
#[tokio::test]
async fn an_in_flight_statement_still_gets_its_one_terminal_at_the_drain_deadline() {
    let Some(url) = pg_url() else {
        eprintln!("skip: FERRO_TEST_PG_URL not set");
        return;
    };

    let (drain, sock, served) = spawn_drained_serve(url);
    let mut c = connect(&sock).await;
    c.hello(1).await;
    let tx_id = begin(&mut c, 2, "default").await;

    // A long statement on the PINNED conn, dispatched without awaiting its terminal. The marker is
    // a string LITERAL, not a comment (the in-tree `mysql_chaos_it.rs` lesson — comments are
    // stripped from some engines' process lists; PG keeps them, but the literal form is the one
    // proven portable).
    let marker = format!("s9a_drain_{}_{}", std::process::id(), tx_id);
    let victim = {
        let mut r = tx_req(
            &format!("SELECT pg_sleep(30) WHERE '{marker}' <> ''"),
            tx_id,
        );
        r.readonly = true; // an honest read; the terminal below is a deadline, not a fate branch
        r
    };
    c.send_request(3, service::SQL, method_sql::EXEC, victim.encode())
        .await;

    // PROVE it is in flight before triggering anything (never sleep-and-hope): poll
    // `pg_stat_activity` over a SECOND pooled connection through the same session. This must
    // happen BEFORE the drain — an autocommit EXEC is refused once draining.
    let poll = req(&format!(
        "SELECT count(*)::int8 FROM pg_stat_activity \
         WHERE query LIKE '%{marker}%' AND pid <> pg_backend_pid()"
    ));
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut rid = 10;
    loop {
        let ok = exec_ok(&mut c, rid, &poll).await;
        rid += 1;
        if ok.rows[0][0] != Value::I64(0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the tx-scoped statement never became visible in pg_stat_activity — a drain that \
             lands before dispatch would prove nothing"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let triggered = Instant::now();
    drain.trigger();

    // The window expires with the statement still running; the session's cleanup cancels it, rolls
    // the transaction back, and the supervisor delivers the ONE terminal.
    let t = c.recv_within(EOF_MAX).await;
    let arrived_at = triggered.elapsed();
    assert_eq!(
        t.header.request_id, 3,
        "the in-flight request's terminal, not something else"
    );
    assert_eq!(
        t.header.flags & flags::END,
        flags::END,
        "exactly one frame carrying END for the in-flight request (charter rule 4)"
    );
    match Outcome::decode(&t.payload).expect("decode Outcome") {
        Outcome::Error(ep) => {
            eprintln!(
                "drain-deadline terminal for the in-flight statement: {:#06x} branch {} after \
                 {arrived_at:?} — {}",
                ep.code, ep.branch, ep.message
            );
            // WHICH error is the pre-existing teardown race the session module records as a
            // traced-safe M1-S4 deviation: the cleanup fires `registry.cancel_all()` and then
            // `tx_registry.abort_session()`, and the actor's inner select is `biased` with its
            // `abort` arm ahead of the per-request `cancel` arm — so whichever token the actor
            // observes first decides between `TX_DEADLINE{Retryable}` (cancel arm: tombstone +
            // reply) and `PROTOCOL{NonRetryable}` (abort arm: deregister, reply dropped, the
            // handler mints the prompt terminal itself). MEASURED here: the abort arm wins.
            // Task 12 does not arbitrate that race — it is Wave B's file and a recorded deviation
            // — but it DOES pin the two properties a shutdown may never violate:
            assert!(
                ep.code == errc::PROTOCOL || ep.code == errc::TX_DEADLINE,
                "unexpected drain-deadline terminal {:#06x}: {}",
                ep.code,
                ep.message
            );
            assert_ne!(
                ep.branch,
                branch::INDETERMINATE,
                "the transaction was rolled back at the deadline, so its fate is KNOWN — reporting \
                 an in-flight statement Indeterminate on every restart would cry wolf on the one \
                 branch that must never be routinely retried (§19.3): {:#06x} {}",
                ep.code,
                ep.message
            );
        }
        other => panic!("a statement cut off by the drain deadline must fail, got {other:?}"),
    }
    assert!(
        arrived_at >= EOF_MIN,
        "the terminal arrived after only {arrived_at:?} — the pinned statement must be given the \
         whole drain window (>= {EOF_MIN:?}) before the deadline cuts it off"
    );

    // …and only THEN the socket closes. A truncated stream (EOF instead of the terminal) is what
    // the pre-S9a `abort_all()` produced.
    c.recv_eof_within(EOF_MAX.saturating_sub(triggered.elapsed()))
        .await;

    tokio::time::timeout(Duration::from_secs(5), served)
        .await
        .expect("serve must return once its sessions have wound down")
        .expect("serve's task must not panic");
}
