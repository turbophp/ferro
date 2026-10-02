//! **M2-C4c-2: SPEC §13's OTLP traces, end to end — one span per EXEC, linked to the caller's
//! `traceparent`, on every EXEC path.**
//!
//! Two consumers, deliberately:
//!
//! * A minimal in-test OTLP/HTTP receiver, which records exactly what the daemon SENT. Every test
//!   owns its own receiver and daemon, so counts are exact.
//! * A REAL OpenTelemetry Collector (`the_real_collector_accepts_and_exports_every_field`), because
//!   the in-test receiver can only check what this crate believes the format is. Measured before
//!   any of this was written: the collector drops a MISSPELLED field silently, with HTTP 200 — a
//!   typo'd `parentSpanId` becomes a root span — so that test reads back what the collector
//!   EXPORTED, never the status code.
//!
//! All of it runs on SQLite: no server, so it runs everywhere, CI included.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{exec_err, exec_ok, exec_server_with_otlp, req};
use ferro_proto::consts::{flags, method_tx, service};
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::{BeginRequest, BeginResponse, Outcome, TxControl};
use ferrod::otlp::{Endpoint, OtlpConfig, Sampler};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const SECRET: &str = "hunter2-swordfish";

/// A W3C `traceparent` with a distinct parent span id per request, so each exported span can be
/// matched to the statement that caused it.
fn tp(parent: u64, sampled: bool) -> String {
    format!(
        "00-{TRACE}-{parent:016x}-{}",
        if sampled { "01" } else { "00" }
    )
}

fn traced(parent: u64, r: ExecRequest) -> ExecRequest {
    ExecRequest {
        traceparent: Some(tp(parent, true)),
        ..r
    }
}

fn write(sql: &str) -> ExecRequest {
    ExecRequest {
        readonly: false,
        ..req(sql)
    }
}

fn sqlite_url(tag: &str) -> (String, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "ferro-otlp-{tag}-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    remove_db(&path);
    (format!("sqlite://{}", path.display()), path)
}

/// Remove a test database AND its WAL sidecars — the C4c-2 review counted twelve `-wal`/`-shm`
/// files left in the temp dir by every green run.
fn remove_db(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    for suffix in ["-wal", "-shm"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        let _ = std::fs::remove_file(p);
    }
}

fn otlp_to(addr: std::net::SocketAddr, sampler: Sampler, flush: Duration) -> OtlpConfig {
    OtlpConfig {
        endpoint: Endpoint::parse(&format!("http://{addr}/v1/traces")).unwrap(),
        service_name: "ferrod-test".to_string(),
        sampler,
        flush_interval: flush,
    }
}

/// What a minimal OTLP/HTTP receiver got: every request body, and every span in them.
#[derive(Clone, Default)]
struct Received {
    bodies: Arc<Mutex<Vec<String>>>,
    spans: Arc<Mutex<Vec<Value>>>,
}

impl Received {
    fn spans(&self) -> Vec<Value> {
        self.spans.lock().unwrap().clone()
    }
    fn bodies(&self) -> String {
        self.bodies.lock().unwrap().join("\n")
    }
    /// Wait until `n` spans have arrived, or fail with what did.
    async fn wait_for(&self, n: usize) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let got = self.spans();
            if got.len() >= n {
                return got;
            }
            assert!(
                Instant::now() < deadline,
                "waited for {n} spans, got {}: {got:#?}",
                got.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// An in-test OTLP/HTTP receiver: answers every request 200 and records it.
async fn receiver() -> (std::net::SocketAddr, Received) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let got = Received::default();
    let sink = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                let head_end = loop {
                    let n = s.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "the exporter closed before the request head ended");
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().ok())?
                    })
                    .expect("a Content-Length header");
                while buf.len() < head_end + len {
                    let n = s.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "the exporter closed before the body ended");
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8(buf[head_end..head_end + len].to_vec()).unwrap();
                assert!(head.starts_with("POST /v1/traces HTTP/1.1\r\n"), "{head}");
                let v: Value = serde_json::from_str(&body).expect("the export body is JSON");
                for rs in v["resourceSpans"].as_array().unwrap() {
                    assert_eq!(
                        rs["resource"]["attributes"][0]["value"]["stringValue"],
                        "ferrod-test"
                    );
                    for ss in rs["scopeSpans"].as_array().unwrap() {
                        for sp in ss["spans"].as_array().unwrap() {
                            sink.spans.lock().unwrap().push(sp.clone());
                        }
                    }
                }
                sink.bodies.lock().unwrap().push(body);
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
                    .await;
            });
        }
    });
    (addr, got)
}

/// A span's attributes as `key -> value object` (`{"stringValue": …}` etc.).
fn attrs(span: &Value) -> HashMap<String, Value> {
    span["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| (a["key"].as_str().unwrap().to_string(), a["value"].clone()))
        .collect()
}

fn by_parent(spans: &[Value], parent: u64) -> Value {
    let want = format!("{parent:016x}");
    let found: Vec<&Value> = spans
        .iter()
        .filter(|s| s["parentSpanId"] == want.as_str())
        .collect();
    assert_eq!(
        found.len(),
        1,
        "exactly one span for parent {want}: {spans:#?}"
    );
    found[0].clone()
}

async fn begin(c: &mut common::TestClient, rid: u32) -> u64 {
    c.send_request(
        rid,
        service::TX,
        method_tx::BEGIN,
        BeginRequest {
            pool: "default".into(),
            isolation: None,
            readonly: false,
        }
        .encode(),
    )
    .await;
    let t = c.recv().await;
    match Outcome::decode(&t.payload).unwrap() {
        Outcome::Ok(b) => BeginResponse::decode(&b).unwrap().tx_id,
        other => panic!("BEGIN failed: {other:?}"),
    }
}

async fn commit(c: &mut common::TestClient, rid: u32, tx_id: u64) {
    c.send_request(
        rid,
        service::TX,
        method_tx::COMMIT,
        TxControl { tx_id }.encode(),
    )
    .await;
    let t = c.recv().await;
    assert_eq!(t.header.flags & flags::END, flags::END);
    assert!(matches!(Outcome::decode(&t.payload), Ok(Outcome::Ok(_))));
}

/// Read a streamed EXEC to its terminal.
async fn exec_streamed(c: &mut common::TestClient, rid: u32, r: &ExecRequest) -> Outcome {
    use ferro_proto::consts::method_sql;
    c.send_request(rid, service::SQL, method_sql::EXEC, r.encode())
        .await;
    loop {
        let f = c.recv().await;
        if f.header.flags & flags::END == flags::END {
            return Outcome::decode(&f.payload).unwrap();
        }
    }
}

/// **The claim, on every path.** One span per EXEC — autocommit read, autocommit write, streamed,
/// tx-scoped, failed, and refused for its shape — each the CHILD of the caller's span, each carrying
/// the statement's measurements or its error CODE, and none carrying a literal the statement held.
#[tokio::test(flavor = "multi_thread")]
async fn every_exec_path_exports_one_span_linked_to_the_callers_trace() {
    let (addr, got) = receiver().await;
    let (url, path) = sqlite_url("paths");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(
        &mut c,
        2,
        &write("create table t (id integer primary key, s text)"),
    )
    .await;
    exec_ok(
        &mut c,
        3,
        &write("insert into t (id, s) values (1, 'a'), (2, 'b'), (3, 'c')"),
    )
    .await;

    // (1) autocommit read, carrying a literal that must never reach a span
    let q = format!("select s from t where s <> '{SECRET}' order by id");
    assert_eq!(exec_ok(&mut c, 10, &traced(1, req(&q))).await.rows.len(), 3);
    // (2) autocommit write
    exec_ok(
        &mut c,
        11,
        &traced(2, write("insert into t (id, s) values (4, 'd')")),
    )
    .await;
    // (3) streamed
    let streamed = ExecRequest {
        fetch: 2,
        ..traced(3, req("select id from t order by id"))
    };
    assert!(matches!(
        exec_streamed(&mut c, 12, &streamed).await,
        Outcome::Ok(_)
    ));
    // (4) tx-scoped
    let tx_id = begin(&mut c, 13).await;
    exec_ok(
        &mut c,
        14,
        &ExecRequest {
            tx_id: Some(tx_id),
            ..traced(4, write("insert into t (id, s) values (5, 'e')"))
        },
    )
    .await;
    commit(&mut c, 15, tx_id).await;
    // (5) a statement the backend refuses
    exec_err(&mut c, 16, &traced(5, req("selec 1"))).await;
    // (6) a request refused for its SHAPE, before any backend
    exec_err(
        &mut c,
        17,
        &ExecRequest {
            pool: "no-such-pool".into(),
            ..traced(6, req("select 1"))
        },
    )
    .await;

    let spans = got.wait_for(6).await;
    // Every one of them is in the caller's trace, a CLIENT span named EXEC, the parent's
    // remoteness flagged, its end at or after its start.
    for s in &spans {
        assert_eq!(s["traceId"], TRACE, "{s}");
        assert_eq!(s["name"], "EXEC");
        assert_eq!(s["kind"], 3);
        assert_eq!(s["flags"], 0x301, "sampled, parent remote: {s}");
        let start: u64 = s["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
        let end: u64 = s["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
        assert!(start > 1_700_000_000_000_000_000 && end >= start, "{s}");
    }

    let read = by_parent(&spans, 1);
    for p in 1..=4 {
        assert_full_shape(&by_parent(&spans, p), p, true);
    }
    assert_full_shape(&by_parent(&spans, 5), 5, false);
    let a = attrs(&read);
    assert_eq!(a["db.system.name"]["stringValue"], "sqlite");
    assert_eq!(a["ferro.pool"]["stringValue"], "default");
    assert_eq!(a["ferro.fetch"]["stringValue"], "rows");
    assert_eq!(a["ferro.in_tx"]["boolValue"], false);
    assert_eq!(a["db.response.returned_rows"]["intValue"], "3");
    assert!(a["ferro.exec_us"]["intValue"].is_string());
    assert!(a["ferro.queue_us"]["intValue"].is_string());
    let text = a["db.query.text"]["stringValue"].as_str().unwrap();
    assert!(
        text.contains("s <> ?"),
        "the FINGERPRINT, not the statement: {text}"
    );
    assert!(
        read.get("status").is_none(),
        "success leaves the status unset: {read}"
    );

    let w = attrs(&by_parent(&spans, 2));
    assert_eq!(w["ferro.rows_affected"]["intValue"], "1");
    assert_eq!(w["db.response.returned_rows"]["intValue"], "0");

    let st = attrs(&by_parent(&spans, 3));
    assert_eq!(st["ferro.fetch"]["stringValue"], "stream");
    // Rows 1–3 from the fixture plus row 4 written by (2); row 5 is written later, in (4).
    assert_eq!(st["db.response.returned_rows"]["intValue"], "4");
    let bytes: u64 = st["ferro.response_bytes"]["intValue"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(bytes > 0, "a stream's DATA frames are counted: {st:?}");

    let tx = attrs(&by_parent(&spans, 4));
    assert_eq!(tx["ferro.in_tx"]["boolValue"], true);
    assert_eq!(tx["ferro.rows_affected"]["intValue"], "1");

    let failed = by_parent(&spans, 5);
    assert_eq!(failed["status"]["code"], 2);
    assert_eq!(failed["status"]["message"], "Syntax");
    let f = attrs(&failed);
    assert_eq!(f["error.type"]["stringValue"], "Syntax");
    assert_eq!(f["ferro.error.branch"]["stringValue"], "NonRetryable");
    assert!(
        !f.contains_key("db.response.returned_rows"),
        "no measurements for a failure"
    );

    let refused = by_parent(&spans, 6);
    assert_eq!(refused["status"]["message"], "Unsupported");
    let r = attrs(&refused);
    assert!(
        !r.contains_key("ferro.pool") && !r.contains_key("db.system.name"),
        "a pool name the registry does not know is client-chosen text: {r:?}"
    );

    assert_eq!(
        spans.len(),
        6,
        "one span per EXEC, and BEGIN/COMMIT are not EXECs"
    );
    assert!(
        !got.bodies().contains(SECRET),
        "a statement's literal reached an exported span"
    );
    drop(c);
    remove_db(&path);
}

/// The caller decides: an UNSAMPLED context gets no span, and neither does a statement with no
/// context, or with one that does not parse — under the default sampler, which roots nothing.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_sampled_caller_gets_a_span_by_default() {
    let (addr, got) = receiver().await;
    let (url, path) = sqlite_url("sampling");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;

    let unsampled = ExecRequest {
        traceparent: Some(tp(10, false)),
        ..req("select 1")
    };
    exec_ok(&mut c, 2, &unsampled).await;
    exec_ok(&mut c, 3, &req("select 2")).await;
    let garbage = ExecRequest {
        traceparent: Some("garbage".into()),
        ..req("select 3")
    };
    exec_ok(&mut c, 4, &garbage).await;
    // The marker: the last statement, and the only one entitled to a span.
    exec_ok(&mut c, 5, &traced(11, req("select 4"))).await;

    got.wait_for(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let spans = got.spans();
    assert_eq!(
        spans.len(),
        1,
        "only the sampled caller's statement: {spans:#?}"
    );
    assert_eq!(spans[0]["parentSpanId"], format!("{:016x}", 11));
    drop(c);
    remove_db(&path);
}

/// `OTEL_TRACES_SAMPLER=parentbased_always_on` roots a NEW trace for a statement with no context —
/// and still honours a caller that said not to sample.
#[tokio::test(flavor = "multi_thread")]
async fn parentbased_always_on_roots_an_unparented_statement() {
    let (addr, got) = receiver().await;
    let (url, path) = sqlite_url("root");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOn,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    let unsampled = ExecRequest {
        traceparent: Some(tp(20, false)),
        ..req("select 1")
    };
    exec_ok(&mut c, 2, &unsampled).await;
    exec_ok(&mut c, 3, &req("select 2")).await;

    let spans = got.wait_for(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(got.spans().len(), 1, "the unsampled caller still gets none");
    let root = &spans[0];
    assert!(root.get("parentSpanId").is_none(), "a ROOT span: {root}");
    let trace = root["traceId"].as_str().unwrap();
    assert_eq!(trace.len(), 32);
    assert_ne!(trace, "0".repeat(32));
    assert_ne!(trace, TRACE, "a new trace, not the unsampled caller's");
    assert_eq!(root["flags"], 1, "sampled, no remote parent");
    drop(c);
    remove_db(&path);
}

/// A collector that is DOWN never fails, delays or blocks a statement — the spans are counted as
/// failed and the statements answer normally.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_collector_never_fails_a_statement() {
    let dead = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let (url, path) = sqlite_url("down");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            dead,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    let before = ferrod::otlp::COUNTERS.failed();
    for i in 0..5u32 {
        let ok = exec_ok(&mut c, 10 + i, &traced(30 + u64::from(i), req("select 1"))).await;
        assert_eq!(ok.rows.len(), 1);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while ferrod::otlp::COUNTERS.failed() < before + 5 {
        assert!(
            Instant::now() < deadline,
            "the failed export was never counted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(c);
    remove_db(&path);
}

/// `main` calls `Tracer::shutdown` at exit: what is still queued is exported, not lost. The flush
/// interval here is an hour, so only the shutdown can be what sent it.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_exports_what_is_still_queued() {
    let (addr, got) = receiver().await;
    let (url, path) = sqlite_url("shutdown");
    let (server, registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_secs(3600),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, &traced(40, req("select 1"))).await;
    // `tokio::time::interval` fires its FIRST tick immediately; give it a moment to pass so the
    // span below cannot ride it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    exec_ok(&mut c, 3, &traced(41, req("select 2"))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        got.spans()
            .iter()
            .all(|s| s["parentSpanId"] != format!("{:016x}", 41)),
        "the span went out before shutdown — this test proves nothing"
    );
    registry
        .tracer()
        .unwrap()
        .shutdown(Duration::from_secs(5))
        .await;
    let spans = got.wait_for(1).await;
    by_parent(&spans, 41);
    drop(c);
    remove_db(&path);
}

/// A child process that is killed when the test ends, however it ends.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// **The consumer.** A real OpenTelemetry Collector accepts the export and EXPORTS every field —
/// read back from its file exporter, because a collector silently drops a field it does not know
/// and still answers 200. `FERRO_TEST_OTELCOL` names the `otelcol` binary; CI's integration lane
/// downloads a checksum-pinned release and sets it, and its no-skip gate fails the lane if it is
/// missing.
#[tokio::test(flavor = "multi_thread")]
async fn the_real_collector_accepts_and_exports_every_field() {
    let Ok(bin) = std::env::var("FERRO_TEST_OTELCOL") else {
        eprintln!("skip: FERRO_TEST_OTELCOL unset — the real-collector OTLP e2e did not run");
        return;
    };
    let dir = std::env::temp_dir().join(format!(
        "ferro-otelcol-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let out = dir.join("out.jsonl");
    let cfg = dir.join("collector.yaml");
    std::fs::write(
        &cfg,
        format!(
            "receivers:\n  otlp:\n    protocols:\n      http:\n        endpoint: 127.0.0.1:{port}\n\
             exporters:\n  file:\n    path: {}\n\
             processors:\n  batch:\n    timeout: 100ms\n\
             service:\n  telemetry:\n    metrics:\n      level: none\n  pipelines:\n    traces:\n      receivers: [otlp]\n      processors: [batch]\n      exporters: [file]\n",
            out.display()
        ),
    )
    .unwrap();
    let collector = KillOnDrop(
        std::process::Command::new(&bin)
            .arg("--config")
            .arg(&cfg)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn the collector"),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            Instant::now() < deadline,
            "the collector never started listening"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (url, path) = sqlite_url("collector");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, &write("create table t (id integer primary key)")).await;
    exec_ok(&mut c, 3, &write("insert into t (id) values (1), (2)")).await;
    let q = format!("select id from t where 'x' <> '{SECRET}'");
    exec_ok(&mut c, 4, &traced(0x51, req(&q))).await;
    exec_err(&mut c, 5, &traced(0x52, req("selec 1"))).await;

    let deadline = Instant::now() + Duration::from_secs(20);
    let spans = loop {
        let text = std::fs::read_to_string(&out).unwrap_or_default();
        let spans: Vec<Value> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .flat_map(|l| {
                let v: Value = serde_json::from_str(l).expect("the collector writes JSON lines");
                let mut found = Vec::new();
                for rs in v["resourceSpans"].as_array().cloned().unwrap_or_default() {
                    for ss in rs["scopeSpans"].as_array().cloned().unwrap_or_default() {
                        found.extend(ss["spans"].as_array().cloned().unwrap_or_default());
                    }
                }
                found
            })
            .collect();
        if spans.len() >= 2 {
            assert!(!text.contains(SECRET), "a literal reached the collector");
            assert!(
                text.contains("ferrod-test"),
                "service.name was dropped: {text}"
            );
            break spans;
        }
        assert!(
            Instant::now() < deadline,
            "the collector exported {spans:#?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Every field the in-test receiver checks survived the collector's own parse and re-encode.
    let read = by_parent(&spans, 0x51);
    assert_eq!(read["traceId"], TRACE);
    assert_eq!(read["kind"], 3);
    assert_eq!(read["name"], "EXEC");
    let a = attrs(&read);
    assert_eq!(a["db.system.name"]["stringValue"], "sqlite");
    assert_eq!(a["ferro.pool"]["stringValue"], "default");
    assert_eq!(a["db.response.returned_rows"]["intValue"], "2");
    assert_eq!(a["ferro.in_tx"]["boolValue"], false);
    assert!(
        a["db.query.text"]["stringValue"]
            .as_str()
            .unwrap()
            .contains("<> ?")
    );
    // Every field, in its type, after the collector's own parse and re-encode (review F7).
    assert_full_shape(&read, 0x51, true);
    let failed = by_parent(&spans, 0x52);
    assert_full_shape(&failed, 0x52, false);
    assert_eq!(failed["status"]["code"], 2);
    assert_eq!(failed["status"]["message"], "Syntax");
    assert_eq!(attrs(&failed)["error.type"]["stringValue"], "Syntax");

    drop(c);
    drop(collector);
    remove_db(&path);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Send one EXEC and return its decoded success terminal AND the terminal body's length — the span's
/// `ferro.response_bytes` is pinned against that length, so the test needs the bytes, not just the
/// decoded value. Streamed frames before the terminal are skipped and their payload bytes summed.
async fn exec_measured(
    c: &mut common::TestClient,
    rid: u32,
    r: &ExecRequest,
) -> (ferro_proto::messages::sql::ExecOk, usize) {
    use ferro_proto::consts::method_sql;
    c.send_request(rid, service::SQL, method_sql::EXEC, r.encode())
        .await;
    loop {
        let f = c.recv().await;
        if f.header.flags & flags::END == flags::END {
            match Outcome::decode(&f.payload).unwrap() {
                Outcome::Ok(body) => {
                    let ok = ferro_proto::messages::sql::ExecOk::decode(&body).unwrap();
                    return (ok, body.len());
                }
                other => panic!("expected success: {other:?}"),
            }
        }
    }
}

fn int_attr(a: &HashMap<String, Value>, k: &str) -> u64 {
    a.get(k)
        .and_then(|v| v["intValue"].as_str())
        .unwrap_or_else(|| panic!("{k} missing or not an intValue string: {a:?}"))
        .parse()
        .unwrap()
}

/// Every field a span carries, in its OTLP/JSON type — shared by the in-test receiver and the REAL
/// collector, so a field the collector silently drops fails here (the C4c-2 review renamed `flags`
/// and both timestamps and the collector test stayed green, because it never asked for them).
fn assert_full_shape(s: &Value, parent: u64, success: bool) {
    assert_eq!(s["traceId"], TRACE, "{s}");
    let span_id = s["spanId"].as_str().unwrap();
    assert!(
        span_id.len() == 16
            && span_id != "0000000000000000"
            && span_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "{s}"
    );
    assert_eq!(s["parentSpanId"], format!("{parent:016x}"));
    assert_eq!(s["flags"], 0x301, "sampled + parent remote: {s}");
    assert_eq!(s["name"], "EXEC");
    assert_eq!(s["kind"], 3);
    let start: u64 = s["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
    let end: u64 = s["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
    assert!(start > 1_700_000_000_000_000_000 && end >= start, "{s}");
    let a = attrs(s);
    for k in [
        "db.system.name",
        "ferro.pool",
        "db.query.text",
        "ferro.fetch",
    ] {
        assert!(a[k]["stringValue"].is_string(), "{k}: {s}");
    }
    for k in ["ferro.in_tx", "ferro.readonly"] {
        assert!(a[k]["boolValue"].is_boolean(), "{k}: {s}");
    }
    if success {
        for k in [
            "db.response.returned_rows",
            "ferro.rows_affected",
            "ferro.queue_us",
            "ferro.exec_us",
            "ferro.response_bytes",
        ] {
            int_attr(&a, k);
        }
        // UNSET: absent as this crate sends it, `{}` once a collector has re-encoded it.
        let status = s.get("status");
        assert!(
            status.is_none_or(|st| st.get("code").is_none_or(|c| c == 0)),
            "success leaves the status unset: {s}"
        );
    } else {
        assert_eq!(s["status"]["code"], 2);
        assert!(a["error.type"]["stringValue"].is_string());
        assert!(a["ferro.error.branch"]["stringValue"].is_string());
    }
}

/// **The span's measurements ARE the terminal's** — one read, not two (lesson 26). The review's
/// surviving mutations swapped `queue_us`/`exec_us`, zeroed the span's duration, counted a
/// `fetch:none` SELECT's rows as returned, dropped a stream's DATA bytes from `response_bytes` and
/// pinned `readonly` to a constant; each now fails one of these equalities.
#[tokio::test(flavor = "multi_thread")]
async fn every_measurement_on_a_span_equals_its_terminals() {
    let (addr, got) = receiver().await;
    let (url, path) = sqlite_url("measure");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, &write("create table t (id integer primary key)")).await;
    exec_ok(&mut c, 3, &write("insert into t (id) values (1), (2), (3)")).await;

    // (1) A read with measurable execution time, declared readonly.
    let heavy = "with recursive c(x) as (select 1 union all select x + 1 from c where x < 300000) \
                 select count(*) from c";
    let (read_ok, read_len) = exec_measured(&mut c, 10, &traced(1, req(heavy))).await;
    // (2) A write, NOT readonly.
    let (write_ok, write_len) = exec_measured(
        &mut c,
        11,
        &traced(2, write("insert into t (id) values (4)")),
    )
    .await;
    // (3) A SELECT under fetch:none: the client gets no rows, so neither does the span.
    let none = ExecRequest {
        fetch: 1,
        ..traced(3, req("select id from t"))
    };
    let (none_ok, none_len) = exec_measured(&mut c, 12, &none).await;
    // (4) A stream: HEAD and DATA bytes count, as well as the terminal's.
    let st = ExecRequest {
        fetch: 2,
        ..traced(4, req("select id from t order by id"))
    };
    let (st_ok, st_len) = exec_measured(&mut c, 13, &st).await;

    let spans = got.wait_for(4).await;
    let check = |parent: u64,
                 ok: &ferro_proto::messages::sql::ExecOk,
                 response_bytes: u64,
                 readonly: bool| {
        let s = by_parent(&spans, parent);
        assert_full_shape(&s, parent, true);
        let a = attrs(&s);
        assert_eq!(
            int_attr(&a, "ferro.queue_us"),
            ok.stats.queue_us,
            "queue_us, parent {parent}"
        );
        assert_eq!(
            int_attr(&a, "ferro.exec_us"),
            ok.stats.exec_us,
            "exec_us, parent {parent}"
        );
        assert_eq!(
            int_attr(&a, "ferro.rows_affected"),
            ok.affected,
            "affected, parent {parent}"
        );
        assert_eq!(
            int_attr(&a, "ferro.response_bytes"),
            response_bytes,
            "bytes, parent {parent}"
        );
        assert_eq!(
            a["ferro.readonly"]["boolValue"], readonly,
            "readonly, parent {parent}"
        );
        let start: u64 = s["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
        let end: u64 = s["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
        assert!(
            end - start >= ok.stats.exec_us * 1000,
            "the span must cover the execution: {s}"
        );
        a
    };
    let a = check(1, &read_ok, read_len as u64, true);
    assert!(
        read_ok.stats.exec_us > 1000,
        "the fixture must take measurable time: {:?}",
        read_ok.stats
    );
    assert!(
        read_ok.stats.exec_us > read_ok.stats.queue_us,
        "{:?}",
        read_ok.stats
    );
    assert_eq!(int_attr(&a, "db.response.returned_rows"), 1);
    let a = check(2, &write_ok, write_len as u64, false);
    assert_eq!(int_attr(&a, "ferro.rows_affected"), 1);
    let a = check(3, &none_ok, none_len as u64, true);
    assert_eq!(
        int_attr(&a, "db.response.returned_rows"),
        0,
        "fetch:none returns no rows"
    );
    assert_eq!(a["ferro.fetch"]["stringValue"], "none");
    let a = check(4, &st_ok, st_ok.stats.bytes + st_len as u64, true);
    assert_eq!(int_attr(&a, "db.response.returned_rows"), st_ok.stats.rows);
    assert!(
        st_ok.stats.bytes > 0,
        "HEAD/DATA bytes are what this case exists to count"
    );
    drop(c);
    remove_db(&path);
}

/// A request refused for its SHAPE — before any pool or backend — is a span too. The review's
/// mutation `m` opened the span after the shape checks and survived, because the earlier "refused"
/// case (an unknown pool) is refused AFTER them.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_refused_for_its_shape_is_a_span() {
    let (addr, got) = receiver().await;
    let (url, path) = sqlite_url("shape");
    let (server, _registry) = exec_server_with_otlp(
        url,
        otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        ),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_err(
        &mut c,
        2,
        &ExecRequest {
            query_id: Some("q".into()),
            sql: None,
            ..traced(1, req("x"))
        },
    )
    .await;
    exec_err(
        &mut c,
        3,
        &ExecRequest {
            fetch: 9,
            ..traced(2, req("select 1"))
        },
    )
    .await;
    exec_err(
        &mut c,
        4,
        &ExecRequest {
            sql: None,
            ..traced(3, req("x"))
        },
    )
    .await;
    let spans = got.wait_for(3).await;
    for p in 1..=3 {
        let s = by_parent(&spans, p);
        assert_eq!(s["status"]["message"], "Unsupported", "{s}");
    }
    drop(c);
    remove_db(&path);
}

/// C4c-2 review F5: a tx-scoped EXEC IGNORES its request's `pool`, so its span must name the pool
/// the transaction was pinned to — not the one the client claims.
#[tokio::test(flavor = "multi_thread")]
async fn a_tx_spans_pool_is_the_one_it_was_pinned_to() {
    let (addr, got) = receiver().await;
    let (url_a, path_a) = sqlite_url("pin-a");
    let (url_b, path_b) = sqlite_url("pin-b");
    let (server, _registry) = common::pools_server_with_otlp(
        &[("default", url_a.as_str()), ("other", url_b.as_str())],
        Some(otlp_to(
            addr,
            Sampler::ParentBasedAlwaysOff,
            Duration::from_millis(50),
        )),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    let tx_id = begin(&mut c, 2).await;
    exec_ok(
        &mut c,
        3,
        &ExecRequest {
            tx_id: Some(tx_id),
            pool: "other".into(),
            ..traced(1, req("select 1"))
        },
    )
    .await;
    commit(&mut c, 4, tx_id).await;
    let s = by_parent(&got.wait_for(1).await, 1);
    assert_eq!(attrs(&s)["ferro.pool"]["stringValue"], "default", "{s}");
    drop(c);
    remove_db(&path_a);
    remove_db(&path_b);
}

/// The server backends: each span names its FAMILY, and a literal written in that dialect's own
/// quoting stays out of the span (C4c-2 review F1 — MySQL's `"…"` strings and `\'` escapes reached
/// a collector). Runs where the integration lane provides the servers; prints `skip:` otherwise,
/// which that lane's no-skip gate turns into a failure.
#[tokio::test(flavor = "multi_thread")]
async fn server_backends_name_their_family_and_keep_their_literals_out() {
    let cases: Vec<(String, &str, Vec<String>)> = [
        (
            common::pg_url(),
            "postgresql",
            vec![format!("select 'a\\', '{SECRET}' as x")],
        ),
        (
            common::mysql_url(),
            "mysql",
            vec![
                format!("select 1 from dual where 'x' <> \"{SECRET}\""),
                format!("select 1 from dual where 'O\\'Brien' <> '{SECRET}'"),
                format!("select 1 # {SECRET}\n from dual"),
            ],
        ),
    ]
    .into_iter()
    .filter_map(|(url, fam, stmts)| url.map(|u| (u, fam, stmts)))
    .collect();
    if cases.len() < 2 {
        eprintln!(
            "skip: FERRO_TEST_PG_URL / FERRO_TEST_MYSQL_URL unset — the server-backend OTLP spans did not run"
        );
        if cases.is_empty() {
            return;
        }
    }
    for (url, family, stmts) in cases {
        let (addr, got) = receiver().await;
        let (server, _registry) = exec_server_with_otlp(
            url,
            otlp_to(
                addr,
                Sampler::ParentBasedAlwaysOff,
                Duration::from_millis(50),
            ),
        );
        let mut c = server.connect().await;
        c.hello(1).await;
        for (i, sql) in stmts.iter().enumerate() {
            exec_ok(&mut c, 10 + i as u32, &traced(100 + i as u64, req(sql))).await;
        }
        let spans = got.wait_for(stmts.len()).await;
        for (i, _) in stmts.iter().enumerate() {
            let s = by_parent(&spans, 100 + i as u64);
            assert_eq!(attrs(&s)["db.system.name"]["stringValue"], family, "{s}");
        }
        assert!(
            !got.bodies().contains(SECRET),
            "{family}: a literal reached a span:\n{}",
            got.bodies()
        );
        drop(c);
    }
}
