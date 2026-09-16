//! **C4a: the SPEC §13 slow log, end to end through a real `ferrod`.**
//!
//! The unit tests in `slow_log.rs` prove the threshold arithmetic and the redaction matrix against
//! a hand-built record. This file proves the thing they cannot: that a statement a CLIENT sent
//! actually produces a record, with the fingerprint of the SQL the client actually wrote — and
//! that the literal in it does not appear anywhere in the daemon's log output.
//!
//! That distinction is the same one C3-4 recorded: a unit test of the normalizer passes with the
//! emitter wired to nothing, and a unit test of the emitter passes with the call site absent. Only
//! a statement that travels client → session → SQL service → pool → record proves the wiring.
//!
//! **It runs on a SQLite pool, so it needs no server and runs everywhere including CI** — and the
//! threshold is `Some(0)` ("log everything") so the assertion is about WIRING, never about whether
//! a statement happened to take longer than some number of milliseconds on a loaded runner. A
//! timing-dependent slow-log test would be the flaky test this project keeps refusing to write.

mod common;

use std::io;
use std::sync::{Arc, Mutex};

use common::{exec_ok, exec_server_with_slow_log, req};
use ferro_proto::messages::sql::ExecRequest;
use ferrod::config::LogParams;
use tracing_subscriber::fmt::MakeWriter;

/// A `tracing` writer that captures into a shared buffer, so the test can read what the daemon
/// logged. The subscriber is installed GLOBALLY and once, which is why this is its own test binary:
/// `set_global_default` succeeds once per process, and a thread-local scoped subscriber would miss
/// events emitted on tokio's worker threads.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
    }
}

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A WRITE, declared honestly. `common::req` declares `readonly: true` by default — harmless on
/// the wire backends, where `apply_readonly` is a no-op — and on SQLite that declaration is
/// ENFORCED as `PRAGMA query_only` (C3-4/§22.2 (bi)), so a write built from the default is refused
/// with `SQLITE_READONLY` (8) before it runs. Which is the guard doing its job: this test found it
/// by being refused.
fn write(sql: &str) -> ExecRequest {
    ExecRequest {
        readonly: false,
        ..req(sql)
    }
}

/// A SQLite pool in a fresh temp file — no server, no cleanup coupling to other tests.
fn sqlite_url() -> (String, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "ferro-slowlog-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let _ = std::fs::remove_file(&path);
    (format!("sqlite://{}", path.display()), path)
}

/// The whole path, and the property that matters: the record carries the statement's SHAPE and
/// the literal never appears in the log.
#[tokio::test(flavor = "multi_thread")]
async fn a_statement_is_logged_by_fingerprint_and_its_literal_never_appears() {
    const SECRET: &str = "hunter2-swordfish";
    let capture = Capture::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_ansi(false)
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .finish(),
    )
    .expect("this test binary installs the subscriber exactly once");

    let (url, path) = sqlite_url();
    let server = exec_server_with_slow_log(url, Some(0), LogParams::Never);
    let mut c = server.connect().await;
    c.hello(1).await;

    exec_ok(
        &mut c,
        2,
        &write("create table t (id integer primary key, secret text)"),
    )
    .await;
    exec_ok(
        &mut c,
        3,
        &write(&format!("insert into t (secret) values ('{SECRET}')")),
    )
    .await;

    // A statement with BOUND PARAMETERS. Without one, `log_params` is unreachable end to end:
    // the mutation round proved that, by rendering params unconditionally and leaving this test
    // green — because until now every statement here carried its values as LITERALS.
    exec_ok(
        &mut c,
        4,
        &ExecRequest {
            params: vec![ferro_proto::value::Value::Text(SECRET.to_string())],
            ..write("insert into t (secret) values (?)")
        },
    )
    .await;

    let log = capture.contents();

    // (1) The record exists, and it is the INSERT's shape.
    assert!(
        log.contains("slow statement"),
        "no slow-log record was emitted at all:\n{log}",
    );
    assert!(
        log.contains("insert into t (secret) values (?)"),
        "the INSERT's fingerprint is not in the log:\n{log}",
    );

    // (2) **The property.** The literal the client sent is nowhere in the daemon's output — not in
    // the slow-log record, and not in any other line it emitted while handling the statement.
    assert!(
        !log.contains(SECRET),
        "the statement's literal leaked into the log:\n{log}",
    );

    // (3) The §13 split is present and the record is attributable to a pool.
    assert!(
        log.contains("queue_us"),
        "no queue_us in the record:\n{log}"
    );
    assert!(log.contains("exec_us"), "no exec_us in the record:\n{log}");
    assert!(
        log.contains("default"),
        "the record does not name its pool:\n{log}",
    );

    // (4) `log_params: Never` — the default — renders no parameter shapes at all, even though the
    // statement above carried one. The COUNT is still there: it is a measurement, not a value.
    assert!(
        !log.contains("text("),
        "params were rendered under LogParams::Never:\n{log}",
    );
    // A write reports rows AFFECTED, not `rows.len()` — which is 0 for every INSERT.
    assert!(
        log.contains("rows=1"),
        "the INSERT's affected-row count is not in the log:\n{log}",
    );
    assert!(
        log.contains("param_count=1"),
        "the parameter COUNT should be recorded even when the values are not:\n{log}",
    );

    drop(c);
    let _ = std::fs::remove_file(&path);
}
