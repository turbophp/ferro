//! Live end-to-end acceptance gate for the M1-S5 streaming producer (Task 7).
//!
//! Every test here drives a REAL client → `ferrod` → hand-rolled pool → Postgres → client stream of
//! a genuinely large result (`SELECT generate_series(1, 50000)`) under a deliberately SMALL credit
//! window (`credit_frames: 2`, the bytes window left at its `>= MAX_FRAME_PAYLOAD` floor). The
//! window's FRAMES dimension is what forces backpressure: the server ships HEAD + one DATA on the
//! initial 2-frame window, then PARKS until the client replenishes with `WINDOW_UPDATE` — so the
//! harness IS the client and MUST replenish to keep the stream flowing. That replenishment loop is
//! the backpressure proof.
//!
//! What each test proves against real PG:
//!  1. `large_result_under_small_window_gate` — the exec-design gate: all 50000 rows arrive IN
//!     ORDER; exactly ONE terminal END, strictly AFTER the last DATA; the window genuinely gated
//!     (a direct stall probe: with the 2-frame window spent and no WINDOW_UPDATE sent, a short read
//!     times out to `None`), and many WINDOW_UPDATEs were required; no hang (a 60 s timeout bounds
//!     the whole body).
//!  2. `second_stream_on_same_session` — the per-session cap returns to baseline: after the 50k
//!     stream, a SECOND stream on the SAME session drains cleanly (a wedged cap would stall it).
//!  3. `abandonment_recovery_after_cancel` — a stream is abandoned mid-flight with a routed CANCEL;
//!     after draining to its ONE terminal, a fresh request on the SAME session round-trips cleanly
//!     (the wire re-frames, the conn is recycled not leaked, no stale frame).
//!
//! All three SKIP (return early, never fail) when `FERRO_TEST_PG_URL` is unset, so
//! `cargo test --workspace` stays green offline. Run live with:
//!   `FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro \
//!      cargo test -p ferrod --test stream_it -- --nocapture`

mod common;

use std::time::Duration;

use ferro_proto::consts::{errc, flags, method_sql, method_stream, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::{StreamData, StreamHead};
use ferro_proto::value::Value;
use ferrod::session::codec::InFrame;

use common::{TestClient, assert_session_alive, exec_ok, pg_url, req, stream_server};

/// The small per-request window every gate test runs under: 2 FRAMES (bytes stay at the default
/// `>= MAX_FRAME_PAYLOAD` floor). Small enough that a 50000-row / ~49-DATA-frame result must park
/// and resume dozens of times.
const SMALL_WINDOW_FRAMES: u32 = 2;

/// The large result under test: 50000 single-int rows in ascending order — genuinely multi-frame
/// (~49 DATA frames at the producer's 1024-rows-per-frame batch default).
const BIG_SQL: &str = "SELECT generate_series(1, 50000) AS n";
const BIG_ROWS: usize = 50_000;

/// A classified streaming frame. `classify` asserts every frame's shared shape (echoed request id,
/// service/method, flag bits) so each test body reads at the level of "HEAD / DATA / terminal".
#[derive(Debug)]
enum SFrame {
    Head(StreamHead),
    Data(StreamData),
    End(Outcome),
}

/// Classify one received frame, asserting its wire shape. The three frame classes are unambiguous:
/// the terminal carries `flags::END` on `service::SQL`/`method_sql::EXEC`; a HEAD is
/// `service::STREAM`/`method_stream::HEAD` with NO `STREAM` flag; a DATA is
/// `service::STREAM`/`method_stream::DATA` WITH the `STREAM` flag.
fn classify(frame: &InFrame, rid: u32) -> SFrame {
    assert_eq!(
        frame.header.request_id, rid,
        "every stream/terminal frame echoes the request id"
    );
    if frame.header.flags & flags::END == flags::END {
        assert_eq!(
            frame.header.service,
            service::SQL,
            "the terminal rides the SQL/EXEC request, not the STREAM service"
        );
        assert_eq!(frame.header.method, method_sql::EXEC);
        return SFrame::End(Outcome::decode(&frame.payload).expect("decode terminal Outcome"));
    }
    assert_eq!(
        frame.header.service,
        service::STREAM,
        "a non-terminal streamed frame is on the STREAM service"
    );
    match frame.header.method {
        method_stream::HEAD => {
            assert_eq!(
                frame.header.flags & flags::STREAM,
                0,
                "HEAD carries no STREAM flag (only DATA frames do)"
            );
            SFrame::Head(StreamHead::decode(&frame.payload).expect("decode StreamHead"))
        }
        method_stream::DATA => {
            assert_eq!(
                frame.header.flags & flags::STREAM,
                flags::STREAM,
                "a DATA frame carries the STREAM flag"
            );
            SFrame::Data(StreamData::decode(&frame.payload).expect("decode StreamData"))
        }
        other => panic!("unexpected STREAM method {other}"),
    }
}

/// Append a DATA batch's single-int-column rows onto `out`, asserting the one-column `[I64]` shape.
fn push_rows(data: StreamData, out: &mut Vec<i64>) {
    for row in data.rows {
        assert_eq!(row.len(), 1, "generate_series yields one column per row");
        match &row[0] {
            Value::I64(n) => out.push(*n),
            other => panic!("expected an I64 cell, got {other:?}"),
        }
    }
}

/// Send a `fetch:stream` EXEC for `sql` and drain it to its terminal, replenishing the credit window
/// after EVERY streamed frame (HEAD + each DATA) with exactly the frame + bytes it consumed — the
/// natural "return credit as I consume it" client. Asserts the frame ordering (one HEAD, before any
/// DATA; exactly one END, last) and that the terminal is a clean `Ok`. Returns the collected rows.
async fn drain_stream(client: &mut TestClient, rid: u32, sql: &str) -> Vec<i64> {
    let mut r = req(sql);
    r.fetch = 2; // FETCH_STREAM
    client
        .send_request(rid, service::SQL, method_sql::EXEC, r.encode())
        .await;

    let mut rows: Vec<i64> = Vec::new();
    let mut saw_head = false;
    loop {
        let frame = client.recv().await;
        let plen = frame.header.payload_len;
        match classify(&frame, rid) {
            SFrame::Head(_) => {
                assert!(!saw_head, "exactly one HEAD frame per stream");
                saw_head = true;
                client.window_update(rid, 1, plen).await;
            }
            SFrame::Data(data) => {
                assert!(saw_head, "HEAD must precede any DATA frame");
                push_rows(data, &mut rows);
                client.window_update(rid, 1, plen).await;
            }
            SFrame::End(outcome) => {
                assert!(
                    saw_head,
                    "a stream always emits its HEAD before the terminal"
                );
                assert!(
                    matches!(outcome, Outcome::Ok(_)),
                    "a fully-drained stream ends in exactly one Ok terminal, got {outcome:?}"
                );
                break;
            }
        }
    }
    rows
}

// -------------------------------------------------------------------------------------------------
// Test 1 — THE gate: a large result under a small window.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn large_result_under_small_window_gate() {
    let Some(url) = pg_url() else {
        return;
    };
    // (c) NO HANG: the whole body is bounded. A lost wakeup / wedged cap / missing terminal would
    // hang here instead of silently passing.
    tokio::time::timeout(Duration::from_secs(60), gate_body(url))
        .await
        .expect("50k-row stream under a 2-frame window must not hang");
}

async fn gate_body(url: String) {
    let server = stream_server(url, SMALL_WINDOW_FRAMES);
    let mut client = server.connect().await;
    client.hello(1).await;

    let rid = 100;
    let mut r = req(BIG_SQL);
    r.fetch = 2; // FETCH_STREAM
    client
        .send_request(rid, service::SQL, method_sql::EXEC, r.encode())
        .await;

    // Frame 1 (HEAD) and frame 2 (DATA #1) ride the INITIAL 2-frame window — no WINDOW_UPDATE sent
    // yet. Grab both before replenishing anything so the stall probe below is meaningful.
    let f_head = client.recv().await;
    let head_plen = f_head.header.payload_len;
    let head = match classify(&f_head, rid) {
        SFrame::Head(h) => h,
        other => panic!("expected HEAD as the first frame, got {other:?}"),
    };
    assert_eq!(head.cols.len(), 1, "generate_series has one column");
    assert_eq!(head.cols[0].name, "n", "the column is aliased `n`");

    let f_d1 = client.recv().await;
    let d1_plen = f_d1.header.payload_len;
    let mut rows: Vec<i64> = Vec::with_capacity(BIG_ROWS);
    match classify(&f_d1, rid) {
        SFrame::Data(d) => push_rows(d, &mut rows),
        other => panic!("expected DATA #1 after HEAD, got {other:?}"),
    }

    // (d) DIRECT backpressure proof: HEAD + DATA #1 spent BOTH initial frame credits and we have
    // sent NO WINDOW_UPDATE, so the producer is parked on `debit_or_wait`. Nothing more can arrive —
    // a short read MUST time out to `None`. This is what distinguishes a real credit gate from a
    // false green where the window was never enforced.
    assert!(
        client
            .recv_or_none(Duration::from_millis(400))
            .await
            .is_none(),
        "with the 2-frame window exhausted and no WINDOW_UPDATE sent, the stream MUST stall \
         (backpressure genuinely engaged)"
    );

    // Return the two consumed credits, then pump the rest — one WINDOW_UPDATE per consumed frame.
    let mut window_updates = 0u32;
    client.window_update(rid, 1, head_plen).await;
    window_updates += 1;
    client.window_update(rid, 1, d1_plen).await;
    window_updates += 1;

    let mut data_frames = 1u32; // DATA #1 already collected above
    let terminal = loop {
        let frame = client.recv().await;
        let plen = frame.header.payload_len;
        match classify(&frame, rid) {
            SFrame::Data(d) => {
                push_rows(d, &mut rows);
                data_frames += 1;
                // Replenish AFTER consuming — the server was blocked waiting for exactly this.
                client.window_update(rid, 1, plen).await;
                window_updates += 1;
            }
            // (b) exactly-one-END, LAST: we reach this arm exactly once and break immediately. Any
            // terminal that overtook a DATA frame would land here with fewer than 50000 rows
            // collected — caught by the row-count assertion below.
            SFrame::End(o) => break o,
            SFrame::Head(_) => panic!("a second HEAD frame is a protocol violation"),
        }
    };

    // (a) ALL 50000 rows, IN ORDER (n = 1..=50000).
    assert_eq!(rows.len(), BIG_ROWS, "every streamed row was received");
    assert!(
        rows.iter().enumerate().all(|(i, n)| *n == i as i64 + 1),
        "rows arrive in generate_series order 1..=50000 with none dropped or reordered"
    );

    // (d) backpressure genuinely engaged: a 50000-row result under a 2-frame window batches into
    // dozens of DATA frames, each needing a replenishment to unblock the next.
    assert!(
        data_frames >= 40,
        "50000 rows under a 2-frame window span many DATA frames (got {data_frames})"
    );
    assert!(
        window_updates >= 40,
        "many WINDOW_UPDATEs were required to keep the window open (got {window_updates})"
    );

    // (b) the single terminal is a clean Ok (the stream drained to completion).
    assert!(
        matches!(terminal, Outcome::Ok(_)),
        "a fully-drained stream ends in exactly one Ok terminal, got {terminal:?}"
    );

    // one-END-last, proven end to end: the session is immediately responsive with NOTHING stray
    // after the terminal (a leaked DATA frame would surface here as a non-PONG reply).
    assert_session_alive(&mut client, 777).await;

    eprintln!(
        "[gate] rows={} data_frames={} window_updates={} head_bytes={} data1_bytes={}",
        rows.len(),
        data_frames,
        window_updates,
        head_plen,
        d1_plen
    );
}

// -------------------------------------------------------------------------------------------------
// Test 2 — the per-session cap returns to baseline: a second stream on the SAME session drains.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn second_stream_on_same_session() {
    let Some(url) = pg_url() else {
        return;
    };
    tokio::time::timeout(Duration::from_secs(60), async move {
        let server = stream_server(url, SMALL_WINDOW_FRAMES);
        let mut client = server.connect().await;
        client.hello(1).await;

        // First: the big 50k stream drains fully (reserving + releasing cap on every DATA frame).
        let rows1 = drain_stream(&mut client, 200, BIG_SQL).await;
        assert_eq!(rows1.len(), BIG_ROWS, "first stream delivered all rows");

        // Second, on the SAME session: a per-session cap that had NOT returned to baseline (a wedged
        // reservation) would stall this second stream. It streams cleanly to completion instead.
        let rows2 = drain_stream(&mut client, 201, "SELECT generate_series(1, 20000) AS n").await;
        assert_eq!(
            rows2.len(),
            20_000,
            "second stream on the same session delivered all rows"
        );
        assert!(
            rows2.iter().enumerate().all(|(i, n)| *n == i as i64 + 1),
            "second stream's rows are in order 1..=20000"
        );

        assert_session_alive(&mut client, 888).await;
        eprintln!(
            "[cap] first_stream_rows={} second_stream_rows={}",
            rows1.len(),
            rows2.len()
        );
    })
    .await
    .expect("two back-to-back streams on one session must not hang (the cap must not wedge)");
}

// -------------------------------------------------------------------------------------------------
// Test 3 — abandonment recovery: CANCEL mid-stream, drain to the terminal, then a fresh request.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn abandonment_recovery_after_cancel() {
    let Some(url) = pg_url() else {
        return;
    };
    tokio::time::timeout(Duration::from_secs(60), async move {
        let server = stream_server(url, SMALL_WINDOW_FRAMES);
        let mut client = server.connect().await;
        client.hello(1).await;

        let rid = 300;
        let mut r = req(BIG_SQL);
        r.fetch = 2; // FETCH_STREAM
        client
            .send_request(rid, service::SQL, method_sql::EXEC, r.encode())
            .await;

        // Read HEAD + a few DATA frames (replenishing so it keeps producing) — a genuinely
        // mid-stream point, nowhere near the 50000th row.
        let f_head = client.recv().await;
        let hplen = f_head.header.payload_len;
        assert!(
            matches!(classify(&f_head, rid), SFrame::Head(_)),
            "the first frame is HEAD"
        );
        client.window_update(rid, 1, hplen).await;

        let mut data_before_cancel = 0u32;
        while data_before_cancel < 3 {
            let frame = client.recv().await;
            let plen = frame.header.payload_len;
            match classify(&frame, rid) {
                SFrame::Data(_) => {
                    data_before_cancel += 1;
                    client.window_update(rid, 1, plen).await;
                }
                SFrame::End(_) => panic!("the 50k stream ended far too early (before abandonment)"),
                SFrame::Head(_) => panic!("a second HEAD frame is a protocol violation"),
            }
        }

        // ABANDON: route a CANCEL to this request, then DRAIN whatever is already in flight to the
        // ONE terminal. Keep replenishing during the drain so a producer momentarily parked on
        // credit still reaches its abort path — the drain is bounded (at most the remaining result),
        // and the request declares exactly one terminal.
        client.cancel(rid).await;
        let terminal = loop {
            let frame = client.recv().await;
            let plen = frame.header.payload_len;
            match classify(&frame, rid) {
                SFrame::Data(_) => client.window_update(rid, 1, plen).await,
                SFrame::Head(_) => panic!("a second HEAD frame after cancel"),
                SFrame::End(o) => break o,
            }
        };

        // The abandoned read terminates with the ONE END frame. Normally it is `Error(CANCELLED)`
        // (57014 — a read is NEVER `Indeterminate`); on a benign race the stream may have drained
        // just before the cancel routed, giving an `Ok`. Either way: exactly one terminal.
        match &terminal {
            Outcome::Error(ep) => assert_eq!(
                ep.code,
                errc::CANCELLED,
                "a cancelled streamed read reports Cancelled (57014), never Indeterminate"
            ),
            Outcome::Ok(_) => {} // benign race: drained before the cancel took effect
            Outcome::Cancelled => {} // an explicit Cancelled terminal is equally valid
        }

        // RECOVERY: the wire is re-framed and the conn recycled — a FRESH request on the same
        // session round-trips cleanly, getting its OWN reply with no stale streamed frame leaking in.
        let ok = exec_ok(&mut client, 301, &req("SELECT 42")).await;
        assert_eq!(
            ok.rows,
            vec![vec![Value::I64(42)]],
            "the post-abandonment request gets its own reply (no wire desync)"
        );
        assert_session_alive(&mut client, 999).await;
        eprintln!(
            "[abandon] data_before_cancel={} terminal={:?}",
            data_before_cancel, terminal
        );
    })
    .await
    .expect("cancel + drain + a fresh request on the same session must not hang");
}
