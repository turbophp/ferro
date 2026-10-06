//! Live end-to-end gate for M3-D4's COPY API: a raw client → `ferrod` → PostgreSQL, exercising the
//! wire contract of `/proto/PROTOCOL.md` §12 directly (no PHP client in the way).
//!
//! What it proves: a COPY_IN round trip under a deliberately SMALL engine-granted window (many
//! re-grants) and a COPY_OUT under a 2-frame credit window, byte-identical; no grant before the COPY
//! has started (a stall probe against a held lock); the engine stops granting when the backend stops
//! consuming; data beyond the grant is session-fatal and discarded data is not; every §19.3 fate
//! branch with a read-back — a CANCEL before COPY_DONE is a KNOWN did-not-apply, one after it is
//! `Indeterminate`; a malformed row is the server's error and the session goes on; COPY inside a
//! transaction (ROLLBACK applies nothing, a CANCEL kills the transaction); the shape guard; COPY on
//! MySQL/SQLite pools is a clean `Unsupported`; and other requests on the same session are served
//! while a COPY is open (D1a). SKIPS without `FERRO_TEST_PG_URL` (and the MySQL case without
//! `FERRO_TEST_MARIADB_URL`/`FERRO_TEST_MYSQL_URL`).

mod common;

use std::time::Duration;

use ferro_proto::consts::{errc, flags, method_core, method_sql, method_stream, method_tx, service};
use ferro_proto::header::Header;
use ferro_proto::messages::sql::{ExecOk, ExecRequest};
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};
use ferro_proto::messages::{CopyData, CopyDone, CopyRequest, ErrorPayload, Outcome, WindowUpdate};
use ferro_proto::value::Value;
use ferrod::session::codec::{InFrame, OutFrame};

use common::{TestClient, assert_session_alive, exec_server_with_session_config, pg_url};

fn write(sql: &str) -> ExecRequest {
    ExecRequest {
        pool: "default".to_string(),
        sql: Some(sql.to_string()),
        query_id: None,
        params: Vec::new(),
        timeout_ms: None,
        readonly: false,
        fetch: 0,
        tx_id: None,
        traceparent: None,
    }
}

async fn exec(client: &mut TestClient, rid: u32, req: &ExecRequest) -> Outcome {
    client
        .send_request(rid, service::SQL, method_sql::EXEC, req.encode())
        .await;
    let t = recv_for(client, rid).await;
    assert_eq!(t.header.flags & flags::END, flags::END);
    Outcome::decode(&t.payload).unwrap()
}

/// Run `sql` — one or more `;`-separated statements (a `$$` body may hold `;`) — one EXEC per
/// statement, returning the last result. EXEC runs exactly one statement.
async fn exec_ok(client: &mut TestClient, rid: u32, sql: &str) -> ExecOk {
    let mut stmts = Vec::new();
    let mut cur = String::new();
    let mut in_dollar = false;
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'$' && b.get(i + 1) == Some(&b'$') {
            in_dollar = !in_dollar;
            cur.push_str("$$");
            i += 2;
            continue;
        }
        if b[i] == b';' && !in_dollar {
            stmts.push(std::mem::take(&mut cur));
        } else {
            cur.push(b[i] as char);
        }
        i += 1;
    }
    stmts.push(cur);
    let mut last = None;
    for st in stmts.iter().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        last = Some(match exec(client, rid, &write(st)).await {
            Outcome::Ok(b) => ExecOk::decode(&b).unwrap(),
            other => panic!("{st}: {other:?}"),
        });
    }
    last.expect("at least one statement")
}

async fn count(client: &mut TestClient, rid: u32, table: &str) -> i64 {
    let ok = exec_ok(client, rid, &format!("SELECT count(*) FROM {table}")).await;
    match ok.rows[0][0] {
        Value::I64(n) => n,
        ref v => panic!("{v:?}"),
    }
}

/// The next frame for `rid` (no other request is in flight in these tests unless they say so).
async fn recv_for(client: &mut TestClient, rid: u32) -> InFrame {
    let f = client.recv().await;
    assert_eq!(f.header.request_id, rid, "unexpected frame {:?}", f.header);
    f
}

fn copy_req(sql: &str, readonly: bool, tx_id: Option<u64>) -> CopyRequest {
    CopyRequest {
        pool: "default".into(),
        sql: sql.into(),
        readonly,
        timeout_ms: None,
        tx_id,
    }
}

/// Send COPY_IN and wait for the engine's first grant (`Ok`) or its terminal (`Err`).
async fn open_in(
    client: &mut TestClient,
    rid: u32,
    req: &CopyRequest,
) -> Result<WindowUpdate, Outcome> {
    client
        .send_request(rid, service::SQL, method_sql::COPY_IN, req.encode())
        .await;
    let f = recv_for(client, rid).await;
    if f.header.flags & flags::END != 0 {
        assert_eq!(f.header.service, service::SQL);
        assert_eq!(f.header.method, method_sql::COPY_IN);
        return Err(Outcome::decode(&f.payload).unwrap());
    }
    assert_eq!(f.header.service, service::CORE, "the grant is a CORE/WINDOW_UPDATE");
    assert_eq!(f.header.method, method_core::WINDOW_UPDATE);
    Ok(WindowUpdate::decode(&f.payload).unwrap())
}

fn frame(svc: u16, method: u16, fl: u16, rid: u32, payload: Vec<u8>) -> OutFrame {
    OutFrame {
        header: Header {
            flags: fl,
            service: svc,
            method,
            request_id: rid,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}

async fn send_data(client: &mut TestClient, rid: u32, data: &[u8]) {
    let p = CopyData {
        data: data.to_vec(),
    }
    .encode();
    client
        .send(frame(service::STREAM, method_stream::COPY_DATA, flags::STREAM, rid, p))
        .await;
}

async fn send_done(client: &mut TestClient, rid: u32) {
    client
        .send(frame(
            service::STREAM,
            method_stream::COPY_DONE,
            0,
            rid,
            CopyDone {}.encode(),
        ))
        .await;
}

/// Read frames for `rid` until its terminal, adding up any grants on the way.
async fn terminal(client: &mut TestClient, rid: u32) -> Outcome {
    loop {
        let f = recv_for(client, rid).await;
        if f.header.flags & flags::END != 0 {
            return Outcome::decode(&f.payload).unwrap();
        }
        assert_eq!(f.header.method, method_core::WINDOW_UPDATE);
    }
}

/// A conforming COPY_IN sender: never more than its credit; chunks split to fit. Returns the
/// terminal and how many grants (after the first) it took.
async fn copy_in_all(client: &mut TestClient, rid: u32, mut grant: WindowUpdate, data: &[u8], chunk: usize) -> (Outcome, u32) {
    let (mut frames, mut bytes) = (grant.frames, grant.bytes as usize);
    let mut regrants = 0;
    let mut off = 0;
    while off < data.len() {
        while frames == 0 || bytes == 0 {
            let f = recv_for(client, rid).await;
            if f.header.flags & flags::END != 0 {
                return (Outcome::decode(&f.payload).unwrap(), regrants);
            }
            grant = WindowUpdate::decode(&f.payload).unwrap();
            frames += grant.frames;
            bytes += grant.bytes as usize;
            regrants += 1;
        }
        let n = chunk.min(bytes).min(data.len() - off);
        send_data(client, rid, &data[off..off + n]).await;
        off += n;
        frames -= 1;
        bytes -= n;
    }
    send_done(client, rid).await;
    (terminal(client, rid).await, regrants)
}

fn ok_body(o: Outcome) -> ExecOk {
    match o {
        Outcome::Ok(b) => ExecOk::decode(&b).unwrap(),
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn err_body(o: Outcome) -> ErrorPayload {
    match o {
        Outcome::Error(e) => e,
        other => panic!("expected Error, got {other:?}"),
    }
}

/// A server whose COPY_IN window is SMALL (8 frames / 64 KiB) and whose stream window is 2 frames,
/// so every round trip below re-grants and replenishes many times.
fn small_windows(url: String) -> common::TestServer {
    exec_server_with_session_config(url, |c| {
        c.copy_in_window_frames = 8;
        c.copy_in_window_bytes = 64 * 1024;
        c.credit_frames = 2;
    })
}

fn rows(n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..n {
        // Text-format specials in every row: an escaped tab and backslash inside the value.
        out.extend_from_slice(format!("{i}\tn\\t{i}\\\\x\n").as_bytes());
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_in_then_copy_out_round_trips_byte_identical_under_small_windows() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_rt; CREATE TABLE d4_e2e_rt (id int PRIMARY KEY, name text)").await;

    let data = rows(100_000);
    let grant = open_in(&mut c, 3, &copy_req("COPY d4_e2e_rt (id, name) FROM STDIN", false, None))
        .await
        .expect("the COPY starts");
    assert_eq!((grant.frames, grant.bytes), (8, 64 * 1024), "the configured window is granted");
    let (t, regrants) = copy_in_all(&mut c, 3, grant, &data, 16 * 1024).await;
    let ok = ok_body(t);
    assert_eq!(ok.affected, 100_000);
    assert_eq!(ok.stats.bytes, data.len() as u64, "stats.bytes = COPY bytes received");
    assert!(regrants > 20, "the window was re-granted many times ({regrants})");

    // COPY_OUT under a 2-frame credit window: replenish as we go.
    let out_req = copy_req("COPY (SELECT id, name FROM d4_e2e_rt ORDER BY id) TO STDOUT", true, None);
    c.send_request(4, service::SQL, method_sql::COPY_OUT, out_req.encode()).await;
    let mut got = Vec::new();
    let mut data_frames = 0;
    let end = loop {
        let f = recv_for(&mut c, 4).await;
        if f.header.flags & flags::END != 0 {
            assert_eq!(f.header.service, service::SQL);
            assert_eq!(f.header.method, method_sql::COPY_OUT);
            break Outcome::decode(&f.payload).unwrap();
        }
        assert_eq!(f.header.service, service::STREAM);
        assert_eq!(f.header.method, method_stream::COPY_DATA);
        assert_eq!(f.header.flags, flags::STREAM);
        got.extend_from_slice(&CopyData::decode(&f.payload).unwrap().data);
        data_frames += 1;
        c.window_update(4, 1, f.payload.len() as u32).await;
    };
    let ok = ok_body(end);
    assert_eq!(ok.affected, 100_000);
    assert_eq!(got, data, "byte-identical");
    assert!(data_frames > 2, "more frames than the window: replenishment was required");
}

/// No byte may be sent into a COPY that has not started: while another session holds a lock the
/// COPY's start waits on, the engine grants NOTHING (a stall probe), and the grant arrives once the
/// lock is released.
#[tokio::test(flavor = "multi_thread")]
async fn nothing_is_granted_before_the_copy_has_started() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut a = server.connect().await;
    a.hello(1).await;
    exec_ok(&mut a, 2, "DROP TABLE IF EXISTS d4_e2e_lock; CREATE TABLE d4_e2e_lock (id int)").await;
    // Session A: a transaction holding the table's ACCESS EXCLUSIVE lock.
    a.send_request(3, service::TX, method_tx::BEGIN, BeginRequest { pool: "default".into(), isolation: None, readonly: false }.encode()).await;
    let tx = match Outcome::decode(&recv_for(&mut a, 3).await.payload).unwrap() {
        Outcome::Ok(b) => BeginResponse::decode(&b).unwrap().tx_id,
        o => panic!("{o:?}"),
    };
    let mut lock = write("LOCK TABLE d4_e2e_lock IN ACCESS EXCLUSIVE MODE");
    lock.tx_id = Some(tx);
    assert!(matches!(exec(&mut a, 4, &lock).await, Outcome::Ok(_)));

    let mut b = server.connect().await;
    b.hello(1).await;
    b.send_request(5, service::SQL, method_sql::COPY_IN, copy_req("COPY d4_e2e_lock FROM STDIN", false, None).encode()).await;
    assert!(
        b.recv_or_none(Duration::from_millis(600)).await.is_none(),
        "no grant while the COPY cannot start"
    );
    a.send_request(6, service::TX, method_tx::ROLLBACK, TxControl { tx_id: tx }.encode()).await;
    let _ = recv_for(&mut a, 6).await;
    let f = recv_for(&mut b, 5).await;
    assert_eq!(f.header.method, method_core::WINDOW_UPDATE, "the grant comes once it starts");
    send_data(&mut b, 5, b"1\n").await;
    send_done(&mut b, 5).await;
    assert_eq!(ok_body(terminal(&mut b, 5).await).affected, 1);
}

/// The engine re-grants only what it has FORWARDED: when the backend stops consuming (a unique
/// check waiting on another session's uncommitted row), the grants stop, and they resume when it
/// does.
#[tokio::test(flavor = "multi_thread")]
async fn grants_stop_while_the_backend_stops_consuming() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut a = server.connect().await;
    a.hello(1).await;
    exec_ok(&mut a, 2, "DROP TABLE IF EXISTS d4_e2e_bp; CREATE TABLE d4_e2e_bp (id int PRIMARY KEY, pad text)").await;
    a.send_request(3, service::TX, method_tx::BEGIN, BeginRequest { pool: "default".into(), isolation: None, readonly: false }.encode()).await;
    let tx = match Outcome::decode(&recv_for(&mut a, 3).await.payload).unwrap() {
        Outcome::Ok(b) => BeginResponse::decode(&b).unwrap().tx_id,
        o => panic!("{o:?}"),
    };
    let mut ins = write("INSERT INTO d4_e2e_bp VALUES (0, 'held')");
    ins.tx_id = Some(tx);
    assert!(matches!(exec(&mut a, 4, &ins).await, Outcome::Ok(_)));

    let mut b = server.connect().await;
    b.hello(1).await;
    let grant = open_in(&mut b, 5, &copy_req("COPY d4_e2e_bp FROM STDIN", false, None)).await.unwrap();
    // Row 0 conflicts with A's uncommitted row: PostgreSQL waits on A and stops reading. Then pad
    // rows, so the data outruns every socket buffer between here and the server.
    let pad = "p".repeat(1000);
    let mut data = b"0\tfirst\n".to_vec();
    for i in 1..20_000 {
        data.extend_from_slice(format!("{i}\t{pad}\n").as_bytes());
    }
    let (mut frames, mut bytes) = (grant.frames, grant.bytes as usize);
    let mut off = 0;
    let mut stalled = false;
    while off < data.len() {
        if frames == 0 || bytes == 0 {
            match b.recv_or_none(Duration::from_millis(800)).await {
                Some(f) => {
                    assert_eq!(f.header.method, method_core::WINDOW_UPDATE);
                    let g = WindowUpdate::decode(&f.payload).unwrap();
                    frames += g.frames;
                    bytes += g.bytes as usize;
                    continue;
                }
                None => {
                    stalled = true;
                    break;
                }
            }
        }
        let n = (8 * 1024).min(bytes).min(data.len() - off);
        send_data(&mut b, 5, &data[off..off + n]).await;
        off += n;
        frames -= 1;
        bytes -= n;
    }
    assert!(stalled, "the grants must stop while the backend is not consuming");
    assert!(off < data.len(), "the client could not send everything ({off} of {})", data.len());
    // A commits its row: the COPY's row 0 now violates the key, the COPY fails — known fate.
    a.send_request(6, service::TX, method_tx::COMMIT, TxControl { tx_id: tx }.encode()).await;
    let _ = recv_for(&mut a, 6).await;
    let t = loop {
        let f = recv_for(&mut b, 5).await;
        if f.header.flags & flags::END != 0 {
            break Outcome::decode(&f.payload).unwrap();
        }
    };
    let e = err_body(t);
    assert_eq!(e.sqlstate.as_deref(), Some("23505"), "{e:?}");
    assert_eq!(e.code, errc::UNIQUE);
    assert_eq!(count(&mut a, 7, "d4_e2e_bp").await, 1, "only A's row");
    assert_session_alive(&mut b, 11).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn data_beyond_the_grant_is_session_fatal_and_data_for_a_finished_id_is_discarded() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_v; CREATE TABLE d4_e2e_v (id int)").await;
    // A chunk for an id that is not in flight is discarded, not fatal.
    send_data(&mut c, 99, b"1\n").await;
    assert_session_alive(&mut c, 7).await;

    let grant = open_in(&mut c, 3, &copy_req("COPY d4_e2e_v FROM STDIN", false, None)).await.unwrap();
    // One byte beyond the grant in a single frame.
    let too_much = vec![b'\n'; grant.bytes as usize + 1];
    send_data(&mut c, 3, &too_much).await;
    // The session-fatal terminal on rid 0, the COPY's own terminal (aborted: nothing applied), EOF.
    let mut saw_fatal = false;
    let mut saw_copy_end = false;
    for _ in 0..4 {
        match c.recv_or_none(Duration::from_secs(5)).await {
            Some(f) if f.header.request_id == 0 => {
                let e = err_body(Outcome::decode(&f.payload).unwrap());
                assert_eq!(e.code, errc::PROTOCOL);
                assert!(e.message.contains("grant"), "{}", e.message);
                saw_fatal = true;
            }
            Some(f) if f.header.request_id == 3 && f.header.flags & flags::END != 0 => {
                let e = err_body(Outcome::decode(&f.payload).unwrap());
                assert_eq!(e.branch, errc::CONNECTION_LOST_BRANCH, "not applied: {e:?}");
                saw_copy_end = true;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(saw_fatal && saw_copy_end);
    let mut d = server.connect().await;
    d.hello(1).await;
    assert_eq!(count(&mut d, 2, "d4_e2e_v").await, 0);

    // COPY data for an in-flight request that is not a COPY_IN is a violation too.
    d.send_request(3, service::SQL, method_sql::EXEC, write("SELECT 1 FROM pg_sleep(0.5)").encode()).await;
    send_data(&mut d, 3, b"x").await;
    let f = d.recv().await;
    assert_eq!(f.header.request_id, 0, "fatal");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_before_copy_done_applies_nothing_and_is_a_known_retryable() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_cx; CREATE TABLE d4_e2e_cx (id int)").await;
    let grant = open_in(&mut c, 3, &copy_req("COPY d4_e2e_cx FROM STDIN", false, None)).await.unwrap();
    let n = (grant.bytes as usize).min(30_000);
    let data: Vec<u8> = rows_ints(5000).into_iter().take(n).collect();
    send_data(&mut c, 3, &data).await;
    c.cancel(3).await;
    let e = err_body(terminal(&mut c, 3).await);
    assert_eq!(e.code, errc::CONNECTION_LOST, "a known did-not-apply: {e:?}");
    assert_eq!(e.branch, errc::CONNECTION_LOST_BRANCH);
    assert!(e.message.contains("end-of-data"), "{}", e.message);
    assert_eq!(count(&mut c, 4, "d4_e2e_cx").await, 0, "nothing applied");
    // The session and the pool go on: a second COPY on the same session works.
    let g = open_in(&mut c, 5, &copy_req("COPY d4_e2e_cx FROM STDIN", false, None)).await.unwrap();
    let (t, _) = copy_in_all(&mut c, 5, g, b"1\n2\n", 1024).await;
    assert_eq!(ok_body(t).affected, 2);
}

fn rows_ints(n: usize) -> Vec<u8> {
    (0..n).flat_map(|i| format!("{i}\n").into_bytes()).collect()
}

/// After COPY_DONE the COPY MAY apply, so a CANCEL that lands while its COMMIT runs (a deferred
/// trigger sleeping at commit time) is `Indeterminate` — the engine cannot confirm non-execution.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_after_copy_done_is_indeterminate() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(
        &mut c,
        2,
        "DROP TABLE IF EXISTS d4_e2e_slow; CREATE TABLE d4_e2e_slow (id int);
         CREATE OR REPLACE FUNCTION d4_e2e_sleep() RETURNS trigger LANGUAGE plpgsql AS
           $$ BEGIN PERFORM pg_sleep(1.5); RETURN NULL; END $$;
         CREATE CONSTRAINT TRIGGER d4_e2e_slow_t AFTER INSERT ON d4_e2e_slow
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION d4_e2e_sleep()",
    )
    .await;
    let _ = open_in(&mut c, 3, &copy_req("COPY d4_e2e_slow FROM STDIN", false, None)).await.unwrap();
    send_data(&mut c, 3, b"1\n").await;
    send_done(&mut c, 3).await;
    tokio::time::sleep(Duration::from_millis(300)).await; // inside the commit-time trigger
    c.cancel(3).await;
    let t = loop {
        match c.recv_or_none(Duration::from_secs(10)).await {
            Some(f) if f.header.flags & flags::END != 0 => break Outcome::decode(&f.payload).unwrap(),
            Some(_) => {}
            None => panic!("no terminal"),
        }
    };
    let e = err_body(t);
    assert_eq!(e.code, errc::WRITE_UNCONFIRMED, "{e:?}");
    assert_eq!(e.branch, errc::WRITE_UNCONFIRMED_BRANCH);
    // (The server did cancel it, so nothing landed — the engine still may not claim that.)
    assert_eq!(count(&mut c, 4, "d4_e2e_slow").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_row_is_the_servers_known_error_and_the_session_goes_on() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_bad; CREATE TABLE d4_e2e_bad (id int)").await;
    let g = open_in(&mut c, 3, &copy_req("COPY d4_e2e_bad FROM STDIN", false, None)).await.unwrap();
    let (t, _) = copy_in_all(&mut c, 3, g, b"1\n2\nnot-an-int\n3\n", 1024).await;
    let e = err_body(t);
    assert_eq!(e.sqlstate.as_deref(), Some("22P02"));
    assert_eq!(e.branch, errc::PROTOCOL_BRANCH, "NonRetryable, known fate: {e:?}");
    assert_eq!(count(&mut c, 4, "d4_e2e_bad").await, 0, "atomic");
    let g = open_in(&mut c, 5, &copy_req("COPY d4_e2e_bad FROM STDIN", false, None)).await.unwrap();
    let (t, _) = copy_in_all(&mut c, 5, g, b"7\n", 1024).await;
    assert_eq!(ok_body(t).affected, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_in_inside_a_transaction_rolls_back_and_a_cancel_kills_the_transaction() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_tx; CREATE TABLE d4_e2e_tx (id int)").await;
    let begin = || BeginRequest { pool: "default".into(), isolation: None, readonly: false }.encode();
    // BEGIN; COPY; ROLLBACK -> nothing.
    c.send_request(3, service::TX, method_tx::BEGIN, begin()).await;
    let tx = BeginResponse::decode(match &Outcome::decode(&recv_for(&mut c, 3).await.payload).unwrap() { Outcome::Ok(b) => b, o => panic!("{o:?}") }).unwrap().tx_id;
    let g = open_in(&mut c, 4, &copy_req("COPY d4_e2e_tx FROM STDIN", false, Some(tx))).await.unwrap();
    let (t, _) = copy_in_all(&mut c, 4, g, &rows_ints(1000), 4096).await;
    assert_eq!(ok_body(t).affected, 1000);
    c.send_request(5, service::TX, method_tx::ROLLBACK, TxControl { tx_id: tx }.encode()).await;
    assert!(matches!(Outcome::decode(&recv_for(&mut c, 5).await.payload).unwrap(), Outcome::Ok(_)));
    assert_eq!(count(&mut c, 6, "d4_e2e_tx").await, 0, "rolled back");

    // BEGIN; COPY (cancelled before DONE) -> TxDeadline; the transaction is gone.
    c.send_request(7, service::TX, method_tx::BEGIN, begin()).await;
    let tx = BeginResponse::decode(match &Outcome::decode(&recv_for(&mut c, 7).await.payload).unwrap() { Outcome::Ok(b) => b, o => panic!("{o:?}") }).unwrap().tx_id;
    let _ = open_in(&mut c, 8, &copy_req("COPY d4_e2e_tx FROM STDIN", false, Some(tx))).await.unwrap();
    send_data(&mut c, 8, b"1\n").await;
    c.cancel(8).await;
    let e = err_body(terminal(&mut c, 8).await);
    assert_eq!(e.code, errc::TX_DEADLINE, "{e:?}");
    c.send_request(9, service::TX, method_tx::COMMIT, TxControl { tx_id: tx }.encode()).await;
    let e = err_body(Outcome::decode(&recv_for(&mut c, 9).await.payload).unwrap());
    assert_eq!(e.code, errc::TX_DEADLINE, "the transaction was rolled back and tombstoned: {e:?}");
    assert_eq!(count(&mut c, 10, "d4_e2e_tx").await, 0);

    // BEGIN; COPY; COMMIT -> applied.
    c.send_request(11, service::TX, method_tx::BEGIN, begin()).await;
    let tx = BeginResponse::decode(match &Outcome::decode(&recv_for(&mut c, 11).await.payload).unwrap() { Outcome::Ok(b) => b, o => panic!("{o:?}") }).unwrap().tx_id;
    let g = open_in(&mut c, 12, &copy_req("COPY d4_e2e_tx FROM STDIN", false, Some(tx))).await.unwrap();
    let (t, _) = copy_in_all(&mut c, 12, g, &rows_ints(10), 4096).await;
    assert_eq!(ok_body(t).affected, 10);
    c.send_request(13, service::TX, method_tx::COMMIT, TxControl { tx_id: tx }.encode()).await;
    assert!(matches!(Outcome::decode(&recv_for(&mut c, 13).await.payload).unwrap(), Outcome::Ok(_)));
    assert_eq!(count(&mut c, 14, "d4_e2e_tx").await, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shape_guard_and_the_readonly_refusal_touch_nothing() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_g; CREATE TABLE d4_e2e_g (id int); INSERT INTO d4_e2e_g VALUES (1)").await;
    for (rid, method, sql, readonly) in [
        (3, method_sql::COPY_IN, "DELETE FROM d4_e2e_g", false),
        (4, method_sql::COPY_OUT, "COPY d4_e2e_g FROM STDIN", true),
        (5, method_sql::COPY_IN, "COPY d4_e2e_g FROM STDIN", true),
        (6, method_sql::COPY_OUT, "COPY d4_e2e_g TO '/tmp/x'", true),
    ] {
        c.send_request(rid, service::SQL, method, copy_req(sql, readonly, None).encode()).await;
        let e = err_body(Outcome::decode(&recv_for(&mut c, rid).await.payload).unwrap());
        assert_eq!(e.code, errc::UNSUPPORTED, "{sql}: {e:?}");
    }
    assert_eq!(count(&mut c, 7, "d4_e2e_g").await, 1, "the DELETE never ran");
}

/// D1a: one session, a COPY_IN open (granted, not yet done) — an EXEC and a PING on the same
/// session are answered meanwhile, and the COPY then completes.
#[tokio::test(flavor = "multi_thread")]
async fn other_requests_are_served_while_a_copy_is_open() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    exec_ok(&mut c, 2, "DROP TABLE IF EXISTS d4_e2e_mx; CREATE TABLE d4_e2e_mx (id int)").await;
    let _ = open_in(&mut c, 3, &copy_req("COPY d4_e2e_mx FROM STDIN", false, None)).await.unwrap();
    send_data(&mut c, 3, b"1\n2\n").await;
    let ok = exec_ok(&mut c, 4, "SELECT 42").await;
    assert_eq!(ok.rows[0][0], Value::I64(42));
    assert_session_alive(&mut c, 5).await;
    send_data(&mut c, 3, b"3\n").await;
    send_done(&mut c, 3).await;
    assert_eq!(ok_body(terminal(&mut c, 3).await).affected, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn abandoning_a_copy_out_with_cancel_leaves_the_session_usable() {
    let Some(url) = pg_url() else { return };
    let server = small_windows(url);
    let mut c = server.connect().await;
    c.hello(1).await;
    let req = copy_req("COPY (SELECT g, repeat('x', 200) FROM generate_series(1, 200000) g) TO STDOUT", true, None);
    c.send_request(3, service::SQL, method_sql::COPY_OUT, req.encode()).await;
    let first = recv_for(&mut c, 3).await;
    assert_eq!(first.header.method, method_stream::COPY_DATA);
    c.cancel(3).await;
    let t = loop {
        let f = recv_for(&mut c, 3).await;
        if f.header.flags & flags::END != 0 {
            break Outcome::decode(&f.payload).unwrap();
        }
        c.window_update(3, 1, f.payload.len() as u32).await;
    };
    let e = err_body(t);
    assert_eq!(e.code, errc::CANCELLED, "a declared-readonly COPY_OUT that was cancelled: {e:?}");
    let ok = exec_ok(&mut c, 4, "SELECT 1").await;
    assert_eq!(ok.rows[0][0], Value::I64(1));
}

/// COPY on a pool whose backend has no COPY sub-protocol is a clean `Unsupported`, before any
/// checkout — SQLite always, and MySQL/MariaDB when a URL is configured.
#[tokio::test(flavor = "multi_thread")]
async fn copy_on_a_non_postgres_pool_is_unsupported() {
    let dir = std::env::temp_dir().join(format!("d4-copy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut pools = vec![("lite".to_string(), format!("sqlite://{}", dir.join("db.sqlite").display()))];
    if let Some(m) = std::env::var("FERRO_TEST_MARIADB_URL").ok().or_else(|| std::env::var("FERRO_TEST_MYSQL_URL").ok()) {
        pools.push(("my".to_string(), m));
    }
    let refs: Vec<(&str, &str)> = pools.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
    let (server, _) = common::pools_server(&refs);
    let mut c = server.connect().await;
    c.hello(1).await;
    let mut rid = 2;
    for (pool, _) in &pools {
        for (method, sql) in [(method_sql::COPY_IN, "COPY t FROM STDIN"), (method_sql::COPY_OUT, "COPY t TO STDOUT")] {
            let mut r = copy_req(sql, method == method_sql::COPY_OUT, None);
            r.pool = pool.clone();
            c.send_request(rid, service::SQL, method, r.encode()).await;
            let e = err_body(Outcome::decode(&recv_for(&mut c, rid).await.payload).unwrap());
            assert_eq!(e.code, errc::UNSUPPORTED, "{pool}: {e:?}");
            assert!(e.message.contains("PostgreSQL only"), "{}", e.message);
            rid += 1;
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
