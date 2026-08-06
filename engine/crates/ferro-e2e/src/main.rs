//! `ferro-e2e` — a one-command, narrated end-to-end demo of the Ferro wire path.
//!
//! It spins up a REAL `ferrod` session server IN-PROCESS (the public `serve` accept loop, a real
//! `PoolRegistry` + the real `sql::make_handler`) bound to a temp UDS socket pointed at a live
//! Postgres, connects the minimal wire client in `client.rs` over that same socket, and runs a
//! scripted sequence — HELLO, a SELECT, a DDL + parametrized INSERT, a row-returning SELECT, a
//! deliberate syntax error (to show the error taxonomy), and a burst of concurrent EXECs (to show
//! multiplexing) — printing each result with its `queue_us`/`exec_us` stats and a final summary.
//!
//! One command:
//! ```text
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo run -p ferro-e2e
//! ```
//! (or `testkit/e2e-demo.sh`, which brings the Postgres up/down around it).
//!
//! Dev/demo tool, NOT shipped runtime — it exists to be watched. When `FERRO_TEST_PG_URL` is unset
//! it prints how to set it and exits 0; it never panics on a missing backend.

mod client;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use client::{BoxErr, DemoClient};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecOk;
use ferro_proto::value::Value;
use std::sync::Arc;

use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::{EpochSource, RandomEpoch};
use ferrod::pools::PoolRegistry;
use ferrod::serve::serve;
use ferrod::services::sql;
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BoxErr> {
    let Some(url) = std::env::var("FERRO_TEST_PG_URL")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        print_unset_help();
        return Ok(());
    };

    // Build the in-process daemon. PoolRegistry::build + serve MUST run inside the tokio runtime
    // (Pool::new spawns a background reaper) — they do, we are inside `#[tokio::main]`.
    let socket_path = temp_socket_path();
    let kind = ferrod::config::infer_pool_kind(&url);
    let config = Config {
        socket_path: socket_path.clone(),
        pools: vec![PoolSpec {
            name: "default".to_string(),
            dsn: url,
            kind,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }],
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let handler = sql::make_handler(
        registry,
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    let listener = ferrod::listener::bind_uds(&config)?;
    let epoch = RandomEpoch.epoch();
    let drain = Drain::new();
    let serve_handle = tokio::spawn(serve(
        listener,
        config,
        epoch,
        drain.clone(),
        tx_registry,
        handler,
    ));

    // Run the narrated sequence against the socket. Capture the result so teardown always runs.
    let result = run_demo(&socket_path).await;

    // Teardown: stop accepting, let `serve` drain (bounded), and unlink the temp socket — no
    // dangling files, no leaked accept loop.
    drain.trigger();
    let _ = tokio::time::timeout(Duration::from_secs(2), serve_handle).await;
    let _ = std::fs::remove_file(&socket_path);

    result
}

/// The scripted, narrated sequence. Each step prints what it did and the stats behind it.
async fn run_demo(socket_path: &Path) -> Result<(), BoxErr> {
    let started = Instant::now();
    println!("=== ferro-e2e: in-process ferrod + live Postgres over a real UDS socket ===");
    println!("    socket: {}", socket_path.display());
    println!();

    let mut client = DemoClient::connect(socket_path).await?;
    let mut ok_count = 0u32;
    let mut err_count = 0u32;

    // [1] HELLO — the handshake, boot_epoch, advertised pools.
    let hs = client.hello(1).await?;
    println!("[1] HELLO -> HELLO_ACK");
    println!("      boot_epoch = {}", hs.boot_epoch);
    println!("      pools      = {:?}", hs.pools);
    println!();

    // [2] SELECT 1 — a single-row read, with queue/exec timings.
    println!("[2] EXEC  SELECT 1");
    let ok = expect_ok(&client.exec(2, "SELECT 1", vec![], 0, true).await?)?;
    println!(
        "      row   = [{}]",
        render_row(ok.rows.first().map_or(&[][..], |r| r))
    );
    print_stats(&ok);
    ok_count += 1;
    println!();

    // [3] CREATE TABLE (fetch:none) — DDL, affected reported.
    let ddl = "CREATE TABLE IF NOT EXISTS ferro_e2e_demo(id bigint, note text)";
    println!("[3] EXEC  {ddl}   [fetch:none]");
    let ok = expect_ok(&client.exec(3, ddl, vec![], 1, false).await?)?;
    println!("      affected = {}", ok.affected);
    ok_count += 1;
    println!();

    // Housekeeping (not a numbered step): start from an empty table so re-runs print exactly the
    // two rows the INSERT adds.
    let _ = expect_ok(
        &client
            .exec(4, "DELETE FROM ferro_e2e_demo", vec![], 1, false)
            .await?,
    )?;

    // [4] parametrized INSERT of two rows (fetch:none) — affected reported.
    let insert = "INSERT INTO ferro_e2e_demo(id, note) VALUES (?, ?), (?, ?)";
    println!("[4] EXEC  {insert}   [params: 1,'alpha', 2,'beta'; fetch:none]");
    let params = vec![
        Value::I64(1),
        Value::Text("alpha".to_string()),
        Value::I64(2),
        Value::Text("beta".to_string()),
    ];
    let ok = expect_ok(&client.exec(5, insert, params, 1, false).await?)?;
    println!("      affected = {}", ok.affected);
    ok_count += 1;
    println!();

    // [5] row-returning SELECT — print each row + stats.
    let select = "SELECT id, note FROM ferro_e2e_demo ORDER BY id";
    println!("[5] EXEC  {select}");
    let ok = expect_ok(&client.exec(6, select, vec![], 0, true).await?)?;
    println!(
        "      cols  = {:?}",
        ok.cols.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    for row in &ok.rows {
        println!("      row   = [{}]", render_row(row));
    }
    print_stats(&ok);
    ok_count += 1;
    println!();

    // [6] a deliberate syntax error — show the classified error taxonomy on the wire.
    println!("[6] EXEC  SELCT 1   (deliberate syntax error)");
    match client.exec(7, "SELCT 1", vec![], 0, true).await? {
        Outcome::Error(ep) => {
            println!(
                "      Outcome::Error  code={} branch={} sqlstate={:?}",
                ep.code, ep.branch, ep.sqlstate
            );
            println!("      message = {}", ep.message);
            err_count += 1;
        }
        other => println!("      UNEXPECTED (wanted an error): {other:?}"),
    }
    println!();

    // [7] concurrency: fire 4 SELECT n without awaiting, then collect (order NOT guaranteed).
    println!("[7] concurrent: fire 4 EXEC (SELECT n) without awaiting, then collect");
    for n in 1..=4u32 {
        client
            .send_exec(30 + n, &format!("SELECT {n}"), vec![], 0, true)
            .await?;
    }
    let mut arrivals = Vec::new();
    for _ in 0..4 {
        let (rid, outcome) = client.recv_terminal().await?;
        let ok = expect_ok(&outcome)?;
        let val = render_row(ok.rows.first().map_or(&[][..], |r| r));
        arrivals.push(format!("rid{rid}=>[{val}]"));
        ok_count += 1;
    }
    println!("      arrival order: {}", arrivals.join("  "));
    println!("      (rids may be reordered vs. send order — proof of multiplexing)");
    println!();

    // [8] summary.
    println!(
        "[8] summary: {ok_count} OK, {err_count} classified error (by design); total wall {:?}",
        started.elapsed()
    );

    Ok(())
}

/// Decode an `Outcome` expected to be `Ok` into its `ExecOk` body, or turn any other outcome into a
/// demo error (so the sequence surfaces an unexpected failure rather than panicking).
fn expect_ok(outcome: &Outcome) -> Result<ExecOk, BoxErr> {
    match outcome {
        Outcome::Ok(body) => Ok(ExecOk::decode(body)?),
        Outcome::Error(ep) => {
            Err(format!("expected Ok, got Error code={} ({})", ep.code, ep.message).into())
        }
        Outcome::Cancelled => Err("expected Ok, got Cancelled".into()),
    }
}

fn print_stats(ok: &ExecOk) {
    println!(
        "      stats: queue_us={} exec_us={} rows={} bytes={}",
        ok.stats.queue_us, ok.stats.exec_us, ok.stats.rows, ok.stats.bytes
    );
}

fn render_row(row: &[Value]) -> String {
    row.iter().map(render_value).collect::<Vec<_>>().join(", ")
}

fn render_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::I64(n) => n.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Text(s) => format!("{s:?}"),
        Value::Bytes(b) => format!("<{} bytes>", b.len()),
        // M1-S7 canonical tags: render the canonical text as-is, prefixed with the tag name so the
        // e2e transcript shows WHICH canonical type a cell arrived as (a TIMESTAMP and a
        // TIMESTAMPTZ are otherwise easy to confuse by eye).
        Value::U64(n) => n.to_string(),
        Value::Decimal(s) => format!("DECIMAL({s})"),
        Value::Date(s) => format!("DATE({s})"),
        Value::Time(s) => format!("TIME({s})"),
        Value::Timestamp(s) => format!("TIMESTAMP({s})"),
        Value::TimestampTz(s) => format!("TIMESTAMPTZ({s})"),
        Value::Uuid(s) => format!("UUID({s})"),
        Value::Json(s) => format!("JSON({s})"),
    }
}

fn temp_socket_path() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!("ferro-e2e-{}-{nanos}.sock", std::process::id()));
    p
}

fn print_unset_help() {
    println!("ferro-e2e: FERRO_TEST_PG_URL is not set — nothing to demo against.");
    println!();
    println!("Bring up the testkit Postgres and point the demo at it, e.g.:");
    println!();
    println!("    docker compose -f testkit/docker-compose.yml up -d");
    println!("    export FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro");
    println!("    cargo run -p ferro-e2e");
    println!();
    println!("(or just run:  testkit/e2e-demo.sh  — it does the up/run/down for you)");
}
