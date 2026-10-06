//! Emit deterministic golden vectors: for each case, build the full frame (header+payload),
//! and write {name, header, message(json), frame_hex}. Also emit malformed negative .bin seeds.
use ferro_proto::consts::{
    self, flags, method_admin, method_core, method_http, method_queue, method_sql, method_stream,
    method_tx, service,
};
use ferro_proto::header::Header;
use ferro_proto::messages::*;
use ferro_proto::value::Value;
use std::path::PathBuf;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/vectors")
}

fn frame(flags_: u16, svc: u16, method: u16, req: u32, payload: Vec<u8>) -> Vec<u8> {
    let h = Header {
        flags: flags_,
        service: svc,
        method,
        request_id: req,
        payload_len: payload.len() as u32,
    };
    let mut f = h.encode().to_vec();
    f.extend_from_slice(&payload);
    f
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn write_case(
    name: &str,
    flags_: u16,
    svc: u16,
    method: u16,
    req: u32,
    payload: Vec<u8>,
    msg_json: serde_json::Value,
) {
    let frame = frame(flags_, svc, method, req, payload);
    let v = serde_json::json!({
        "name": name,
        "header": { "flags": flags_, "service": svc, "method": method, "request_id": req },
        "message": msg_json,
        "frame_hex": hex(&frame),
    });
    let out = dir().join(format!("{name}.json"));
    std::fs::write(out, serde_json::to_string_pretty(&v).unwrap() + "\n").unwrap();
}

/// Emit a SQL response vector: payload = the terminal `Outcome::Ok(ExecOk.encode())`, flag END,
/// service SQL, method EXEC. The "message" JSON carries the ExecOk fields (PHP wraps in Outcome::Ok).
fn write_sql_response(name: &str, req: u32, ok: &ExecOk) {
    let payload = Outcome::Ok(ok.encode()).encode();
    write_case(
        name,
        flags::END,
        service::SQL,
        method_sql::EXEC,
        req,
        payload,
        exec_ok_json(ok),
    );
}

/// A single `Value` as `{tag, data}` — `data` mirrors the on-wire payload family (BYTES => array of
/// byte ints, so a non-UTF8 blob survives JSON and re-encodes byte-for-byte in PHP).
fn v_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::json!({ "tag": 0, "data": null }),
        Value::Bool(b) => serde_json::json!({ "tag": 1, "data": b }),
        Value::I64(n) => serde_json::json!({ "tag": 2, "data": n }),
        Value::F64(f) => serde_json::json!({ "tag": 4, "data": f }),
        Value::Text(s) => serde_json::json!({ "tag": 6, "data": s }),
        Value::Bytes(b) => {
            let ints: Vec<u64> = b.iter().map(|x| *x as u64).collect();
            serde_json::json!({ "tag": 7, "data": ints })
        }
        // M1-S7. A `u64` above `0xffffffff` rides the msgpack `uint64` marker, which PHP's pure
        // decoder returns as a DECIMAL STRING (and which a JSON number cannot carry losslessly past
        // 2^53 anyway) — so mirror the decoder exactly: number at or below u32::MAX, string above.
        // Same convention as `HelloAck.boot_epoch`.
        Value::U64(n) => {
            let data = if *n <= u32::MAX as u64 {
                serde_json::json!(n)
            } else {
                serde_json::json!(n.to_string())
            };
            serde_json::json!({ "tag": consts::tag::U64, "data": data })
        }
        // The str-payload S7 tags: the canonical text goes into the vector verbatim.
        Value::Decimal(s) => serde_json::json!({ "tag": consts::tag::DECIMAL, "data": s }),
        Value::Date(s) => serde_json::json!({ "tag": consts::tag::DATE, "data": s }),
        Value::Time(s) => serde_json::json!({ "tag": consts::tag::TIME, "data": s }),
        Value::Timestamp(s) => serde_json::json!({ "tag": consts::tag::TIMESTAMP, "data": s }),
        Value::TimestampTz(s) => serde_json::json!({ "tag": consts::tag::TIMESTAMPTZ, "data": s }),
        Value::Uuid(s) => serde_json::json!({ "tag": consts::tag::UUID, "data": s }),
        Value::Json(s) => serde_json::json!({ "tag": consts::tag::JSON, "data": s }),
    }
}
fn values_json(vs: &[Value]) -> serde_json::Value {
    serde_json::Value::Array(vs.iter().map(v_json).collect())
}
fn exec_request_json(r: &ExecRequest) -> serde_json::Value {
    serde_json::json!({
        "pool": r.pool,
        "sql": r.sql,
        "query_id": r.query_id,
        "params": values_json(&r.params),
        "timeout_ms": r.timeout_ms,
        "readonly": r.readonly,
        "fetch": r.fetch,
        "tx_id": r.tx_id,
        "traceparent": r.traceparent,
    })
}
fn exec_ok_json(ok: &ExecOk) -> serde_json::Value {
    let cols: Vec<serde_json::Value> = ok
        .cols
        .iter()
        .map(|c| serde_json::json!({ "name": c.name, "tag": c.tag }))
        .collect();
    let rows: Vec<serde_json::Value> = ok.rows.iter().map(|r| values_json(r)).collect();
    serde_json::json!({
        "cols": cols,
        "rows": rows,
        "affected": ok.affected,
        "last_insert_id": ok.last_insert_id.as_ref().map(v_json),
        "stats": {
            "queue_us": ok.stats.queue_us,
            "exec_us": ok.stats.exec_us,
            "rows": ok.stats.rows,
            "bytes": ok.stats.bytes,
        },
    })
}

/// A `Vec<ColMeta>` as the JSON `[{name, tag}, ...]` shape shared with `exec_ok_json`'s `cols`.
fn cols_json(cols: &[ColMeta]) -> serde_json::Value {
    serde_json::Value::Array(
        cols.iter()
            .map(|c| serde_json::json!({ "name": c.name, "tag": c.tag }))
            .collect(),
    )
}
fn stream_head_json(head: &StreamHead) -> serde_json::Value {
    serde_json::json!({ "cols": cols_json(&head.cols) })
}
fn stream_data_json(data: &StreamData) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = data.rows.iter().map(|r| values_json(r)).collect();
    serde_json::json!({ "rows": rows })
}

/// A `STREAM`/`HEAD` vector: a plain (non-`END`) message payload, exactly like an `ExecRequest`
/// vector — `HEAD` is not wrapped in the `Outcome` envelope (that's reserved for the terminal
/// `END` frame; §6). No `STREAM` flag on `HEAD` itself (only `DATA` frames carry it, per
/// `/proto/PROTOCOL.md` §10).
fn write_stream_head(name: &str, req: u32, head: &StreamHead) {
    write_case(
        name,
        0,
        service::STREAM,
        method_stream::HEAD,
        req,
        head.encode(),
        stream_head_json(head),
    );
}

/// A `STREAM`/`DATA` vector: a plain (non-`END`) message payload carrying the `STREAM` flag
/// (`flags::STREAM = 0x01`) that marks it as a DATA-channel frame under the per-request credit
/// window (§5.2/§7.2).
fn write_stream_data(name: &str, req: u32, data: &StreamData) {
    write_case(
        name,
        flags::STREAM,
        service::STREAM,
        method_stream::DATA,
        req,
        data.encode(),
        stream_data_json(data),
    );
}

// --- M1-S7 canonical-tag helpers (/proto/PROTOCOL.md §3.2) ---------------------------------
// Every S7 payload is TEXT-canonical (msgpack `str`) except `U64` (msgpack uint), so the whole
// set round-trips through the vector JSON `message` field with no `bin` → list<int> workaround —
// which is exactly why the wire contract is text-canonical. The cols and the row are built by the
// SAME two helpers for both the buffered (`sql_exec_response_types_scalars`) and streamed
// (`stream_data_types`) vectors, so the two paths can never drift apart.

/// The everyday canonical payload for each S7 tag, in `s7_scalar_cols()` order.
///
/// The `U64` here is deliberately SMALL (`5`). **Hard constraint on any golden-vector `U64`:** it
/// must be `<= 0xffffffff` or `> PHP_INT_MAX`, and NEVER inside `(2^32, 2^63]`. rmp emits marker
/// `0xcf` from 2^32 up; PHP `PurePacker::be()` returns a decimal STRING for every `0xcf` uint64
/// while `ext-msgpack` returns an int, and `VectorConformanceTest::hasBigUint` does NOT skip a
/// value in that band (its decimal string is `<= PHP_INT_MAX`), so the ext-vs-pure parity test
/// would fail in CI, which provisions ext-msgpack. `u64::MAX` therefore lives ALONE in
/// `sql_exec_response_types_u64`: a `> PHP_INT_MAX` uint makes `hasBigUint` skip that WHOLE
/// vector's parity assertion, and isolating it keeps that coverage for every other tag.
fn s7_scalar_row() -> Vec<Value> {
    vec![
        // Display scale preserved: "-12345.6700" and "-12345.67" are DISTINCT payloads.
        Value::Decimal("-12345.6700".into()),
        Value::Date("2026-08-05".into()),
        Value::Time("13:45:07".into()),
        // Naive — no zone suffix, ever. Sub-second present => exactly six digits.
        Value::Timestamp("2026-08-05 13:45:07.250000".into()),
        // RFC3339, always normalized to UTC, always the literal `Z`.
        Value::TimestampTz("2026-08-05T13:45:07.250000Z".into()),
        // 36-char canonical lowercase hyphenated — never raw bytes.
        Value::Uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8".into()),
        // Nested object + array + a `null` + a non-ASCII char: proves the raw JSON document text
        // survives UTF-8 intact through both codecs and through the vector JSON itself.
        Value::Json(r#"{"a":[1,2,{"b":null}],"n":"café"}"#.into()),
        Value::U64(5),
    ]
}

/// `ColMeta` for `s7_scalar_row()` — same order, so `cols` and `rows` agree cell for cell.
fn s7_scalar_cols() -> Vec<ColMeta> {
    let names = ["dec", "d", "t", "ts", "tstz", "uu", "js", "u"];
    s7_scalar_row()
        .iter()
        .zip(names)
        .map(|(v, name)| ColMeta {
            name: name.into(),
            tag: v.tag(),
        })
        .collect()
}

fn main() {
    std::fs::create_dir_all(dir().join("negative")).unwrap();

    let hello = Hello {
        client_version: 1,
        type_registry_hash: "deadbeef".into(),
        manifest_hash: None,
        pid: 4242,
        features: 0,
    };
    write_case(
        "hello",
        0,
        service::CORE,
        method_core::HELLO,
        1,
        hello.encode(),
        serde_json::json!({ "client_version":1, "type_registry_hash":"deadbeef",
                            "manifest_hash":null, "pid":4242, "features":0 }),
    );

    let ack = HelloAck {
        engine_version: 1,
        boot_epoch: 0xFFFF_FFFF_FFFF_FFF0,
        features: 0,
        // NON-EMPTY on purpose: an empty list byte-locks no element shape. Every arm of every
        // optional element appears somewhere in the list so the nested fixarray is fully pinned, and
        // the elements carry DIFFERENT `name`/`kind` values so a field-order swap in either codec
        // moves the bytes (a fixture whose fields were interchangeable would not catch one).
        //
        // M2-C2g's `literals_are_standard` has THREE states, not two — `true`, `false` and nil — so
        // it takes a third element to cover them, and the third is not padding: nil is the arm the
        // client's fail-closed refusal hangs off, and a vector that never encoded it would let a
        // codec emitting (say) `false` for nil pass.
        //
        // The two Options are also deliberately NOT covariant across the fixture — element 2 pairs
        // `server_version: None` with `literals_are_standard: Some`, element 3 the reverse. Letting
        // them vary together is the easy mistake, and it would leave a codec that read one where it
        // meant the other producing identical bytes.
        pools: vec![
            PoolInfo {
                name: "main".into(),
                kind: "postgres".into(),
                server_version: Some("PostgreSQL 17.10".into()),
                literals_are_standard: Some(true),
            },
            PoolInfo {
                name: "reporting".into(),
                kind: "mysql".into(),
                server_version: None,
                literals_are_standard: Some(false),
            },
            PoolInfo {
                name: "unprobed".into(),
                kind: "postgres".into(),
                server_version: Some("PostgreSQL 16.4".into()),
                literals_are_standard: None,
            },
        ],
        type_registry_hash: "deadbeef".into(),
    };
    write_case(
        "hello_ack",
        0,
        service::CORE,
        method_core::HELLO_ACK,
        1,
        ack.encode(),
        serde_json::json!({ "engine_version":1, "boot_epoch":"18446744073709551600",
                            "features":0,
                            // KEPT IN STEP BY HAND with the `ack` struct above — `write_case` takes
                            // the encoded BYTES and this human-readable message SEPARATELY, so the
                            // two can drift. They did exactly that when M2-C2g added the fourth
                            // field, and what caught it was the PHP conformance test, which
                            // re-encodes THIS json and compares it to `frame_hex`. That is the
                            // guard; do not weaken it by deriving one side from the other here.
                            "pools":[
                              {"name":"main","kind":"postgres","server_version":"PostgreSQL 17.10",
                               "literals_are_standard":true},
                              {"name":"reporting","kind":"mysql","server_version":null,
                               "literals_are_standard":false},
                              {"name":"unprobed","kind":"postgres","server_version":"PostgreSQL 16.4",
                               "literals_are_standard":null}
                            ],
                            "type_registry_hash":"deadbeef" }),
    );

    write_case(
        "ping",
        0,
        service::CORE,
        method_core::PING,
        9,
        Ping { token: 7 }.encode(),
        serde_json::json!({ "token": 7 }),
    );
    write_case(
        "pong",
        0,
        service::CORE,
        method_core::PONG,
        9,
        Pong { token: 7 }.encode(),
        serde_json::json!({ "token": 7 }),
    );
    write_case(
        "goodbye",
        0,
        service::CORE,
        method_core::GOODBYE,
        0,
        Goodbye {}.encode(),
        serde_json::json!({}),
    );
    write_case(
        "window_update",
        0,
        service::CORE,
        method_core::WINDOW_UPDATE,
        5,
        WindowUpdate {
            frames: 64,
            bytes: 4_194_304,
        }
        .encode(),
        serde_json::json!({ "frames":64, "bytes":4194304 }),
    );

    let err = ErrorPayload {
        code: consts::errc::PROTOCOL,
        branch: consts::errc::PROTOCOL_BRANCH,
        sqlstate: None,
        errno: None,
        message: "reused_request_id".into(),
        detail: None,
        retry_after_ms: None,
    };
    let outcome = Outcome::Error(err);
    write_case(
        "error_protocol",
        flags::END,
        service::CORE,
        0,
        0,
        outcome.encode(),
        serde_json::json!({ "status": consts::outcome::ERROR, "error": {
            "code": consts::errc::PROTOCOL, "branch": consts::errc::PROTOCOL_BRANCH,
            "sqlstate":null, "errno":null, "message":"reused_request_id",
            "detail":null, "retry_after_ms":null } }),
    );

    // B3: the dedicated "that tx_id is not live here" terminal. Its whole reason to exist is to be
    // a DIFFERENT code from `Protocol` on the wire, so the vector's job is to lock that byte — a
    // client swallowing this code on `rollBack()` must not thereby swallow a real protocol fault.
    let err_tx = ErrorPayload {
        code: consts::errc::TX_NOT_FOUND,
        branch: consts::errc::TX_NOT_FOUND_BRANCH,
        sqlstate: None,
        errno: None,
        message: "unknown or forbidden tx_id".into(),
        detail: None,
        retry_after_ms: None,
    };
    write_case(
        "error_tx_not_found",
        flags::END,
        service::TX,
        method_tx::COMMIT,
        22,
        Outcome::Error(err_tx).encode(),
        serde_json::json!({ "status": consts::outcome::ERROR, "error": {
            "code": consts::errc::TX_NOT_FOUND, "branch": consts::errc::TX_NOT_FOUND_BRANCH,
            "sqlstate":null, "errno":null, "message":"unknown or forbidden tx_id",
            "detail":null, "retry_after_ms":null } }),
    );

    // The FIRST vector locking a NON-NULL errno + a real SQLSTATE together. Shape: a MySQL duplicate
    // key — errno 1062, SQLSTATE 23000 — the pair a Doctrine MySQL ExceptionConverter keys on, and
    // the pair that proves the two fields are independent on the wire (23000 alone cannot
    // distinguish a dup key from a NOT NULL violation).
    let err_mysql = ErrorPayload {
        code: consts::errc::UNIQUE,
        branch: consts::errc::UNIQUE_BRANCH,
        sqlstate: Some("23000".into()),
        errno: Some(1062),
        message: "Duplicate entry '1' for key 'PRIMARY'".into(),
        detail: None,
        retry_after_ms: None,
    };
    write_case(
        "error_mysql_errno",
        flags::END,
        service::SQL,
        method_sql::EXEC,
        21,
        Outcome::Error(err_mysql).encode(),
        serde_json::json!({ "status": consts::outcome::ERROR, "error": {
            "code": consts::errc::UNIQUE, "branch": consts::errc::UNIQUE_BRANCH,
            "sqlstate":"23000", "errno":1062, "message":"Duplicate entry '1' for key 'PRIMARY'",
            "detail":null, "retry_after_ms":null } }),
    );

    // --- SQL EXEC vectors (bespoke Value-splicing codec; /proto/PROTOCOL.md §8) ---
    // Request vectors: payload = ExecRequest.encode(), flags 0. Response vectors: the terminal
    // Outcome::Ok(ExecOk.encode()) body, flag END. The "message" JSON carries the ExecRequest fields
    // (requests) or the ExecOk fields (responses); the PHP byte-match re-encodes from it and, for
    // responses, wraps in the Outcome::Ok envelope. request-vs-response is keyed off the name prefix.
    let req_select1 = ExecRequest {
        pool: "main".into(),
        sql: Some("SELECT 1".into()),
        query_id: None,
        params: vec![],
        timeout_ms: None,
        readonly: true,
        fetch: 0,
        tx_id: None,
        traceparent: None,
    };
    write_case(
        "sql_exec_request_select1",
        0,
        service::SQL,
        method_sql::EXEC,
        11,
        req_select1.encode(),
        exec_request_json(&req_select1),
    );

    // The full M0 scalar set, including the divergent-range ints I64(200)=`cc c8` / I64(-200)=`d1 ff 38`.
    let req_params = ExecRequest {
        pool: "main".into(),
        sql: Some("INSERT INTO t (a,b,c,d,e,f,g) VALUES (?,?,?,?,?,?,?)".into()),
        query_id: None,
        params: vec![
            Value::Null,
            Value::Bool(true),
            Value::I64(200),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("hi".into()),
            Value::Bytes(vec![1, 2, 3]),
        ],
        timeout_ms: Some(5000),
        readonly: false,
        fetch: 0,
        tx_id: None,
        traceparent: None,
    };
    write_case(
        "sql_exec_request_params",
        0,
        service::SQL,
        method_sql::EXEC,
        12,
        req_params.encode(),
        exec_request_json(&req_params),
    );

    // A tx-scoped EXEC (S6): the S5 EXEC method carrying an optional `tx_id`. The value is SMALL
    // (7) because `tx_id` is bounded < 2^63 — a > PHP_INT_MAX value would make PurePacker emit a
    // decimal string that `(int)`-casts wrong and redden the PHP byte-match. Locks opt-u64 `Some`.
    let req_intx = ExecRequest {
        pool: "main".into(),
        sql: Some("SELECT 1".into()),
        query_id: None,
        params: vec![],
        timeout_ms: None,
        readonly: false,
        fetch: 0,
        tx_id: Some(7),
        traceparent: None,
    };
    write_case(
        "sql_exec_request_intx",
        0,
        service::SQL,
        method_sql::EXEC,
        19,
        req_intx.encode(),
        exec_request_json(&req_intx),
    );

    // M2-C4c-1: field 9, a W3C `traceparent` (the spec's own example value), on an otherwise plain
    // EXEC. Locks the opt-str `Some` arm of the ninth field in both codecs; every other request
    // vector locks its `nil` arm.
    let req_trace = ExecRequest {
        pool: "main".into(),
        sql: Some("SELECT 1".into()),
        query_id: None,
        params: vec![],
        timeout_ms: None,
        readonly: true,
        fetch: 0,
        tx_id: None,
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
    };
    write_case(
        "sql_exec_request_traceparent",
        0,
        service::SQL,
        method_sql::EXEC,
        43,
        req_trace.encode(),
        exec_request_json(&req_trace),
    );

    let resp_select1 = ExecOk {
        cols: vec![ColMeta {
            name: "?column?".into(),
            tag: consts::tag::I64,
        }],
        rows: vec![vec![Value::I64(1)]],
        affected: 0,
        last_insert_id: None,
        stats: Stats {
            queue_us: 12,
            exec_us: 345,
            rows: 1,
            bytes: 8,
        },
    };
    write_sql_response("sql_exec_response_select1", 13, &resp_select1);

    let resp_none = ExecOk {
        cols: vec![],
        rows: vec![],
        affected: 3,
        last_insert_id: None,
        stats: Stats {
            queue_us: 7,
            exec_us: 120,
            rows: 0,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_none", 14, &resp_none);

    // Some(last_insert_id) in the divergent integer range locks the Option<Value> peek path.
    let resp_lastid = ExecOk {
        cols: vec![],
        rows: vec![],
        affected: 1,
        last_insert_id: Some(Value::I64(200)),
        stats: Stats {
            queue_us: 9,
            exec_us: 88,
            rows: 0,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_lastid", 15, &resp_lastid);

    // 16 cols + a 16-cell row force the array16 marker (0xdc) on both the cols and inner-row lengths.
    let wide_cols: Vec<ColMeta> = (0..16)
        .map(|i| ColMeta {
            name: format!("c{i}"),
            tag: consts::tag::I64,
        })
        .collect();
    let wide_row: Vec<Value> = (0..16).map(|i| Value::I64(i as i64)).collect();
    let resp_wide = ExecOk {
        cols: wide_cols,
        rows: vec![wide_row],
        affected: 0,
        last_insert_id: None,
        stats: Stats {
            queue_us: 3,
            exec_us: 41,
            rows: 1,
            bytes: 16,
        },
    };
    write_sql_response("sql_exec_response_wide", 16, &resp_wide);

    // Some(Value::Null) last_insert_id: the ONE case the Option<Value> peek must disambiguate —
    // `Some(Null)` encodes as the fixarray `[NULL, nil]` (0x92 00 c0), which the peek must read as
    // Some, NOT confuse the inner nil with a bare-nil `None` (0xc0). (T1-review MINOR #1.)
    let resp_nullid = ExecOk {
        cols: vec![],
        rows: vec![],
        affected: 0,
        last_insert_id: Some(Value::Null),
        stats: Stats {
            queue_us: 2,
            exec_us: 7,
            rows: 0,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_nullid", 17, &resp_nullid);

    // S1-deferral shared arbiter: a response row carrying the FULL M0 scalar set incl. the divergent
    // integer ladder (200 = cc c8, -200 = d1 ff 38) and a BYTES whose first byte is 0xc0, locked by
    // BOTH Rust encode==bytes AND PHP re-encode==bytes (not just independently-typed asserts).
    let resp_typedvalue = ExecOk {
        cols: vec![
            ColMeta {
                name: "n".into(),
                tag: consts::tag::NULL,
            },
            ColMeta {
                name: "b".into(),
                tag: consts::tag::BOOL,
            },
            ColMeta {
                name: "pos".into(),
                tag: consts::tag::I64,
            },
            ColMeta {
                name: "neg".into(),
                tag: consts::tag::I64,
            },
            ColMeta {
                name: "f".into(),
                tag: consts::tag::F64,
            },
            ColMeta {
                name: "t".into(),
                tag: consts::tag::TEXT,
            },
            ColMeta {
                name: "by".into(),
                tag: consts::tag::BYTES,
            },
        ],
        rows: vec![vec![
            Value::Null,
            Value::Bool(true),
            Value::I64(200),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("x".into()),
            Value::Bytes(vec![0xc0, 0x01]),
        ]],
        affected: 0,
        last_insert_id: None,
        stats: Stats {
            queue_us: 4,
            exec_us: 12,
            rows: 1,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_typedvalue", 18, &resp_typedvalue);

    // M1-S7 canonical tags, everyday shapes: one cell per S7 tag (§3.2). The `sql_exec_response_`
    // prefix is MANDATORY — PHP's byte-lock provider `VectorConformanceTest::sqlVectors()` keys on
    // it, so a differently-named vector would silently get only the generic header/unpack tests.
    let resp_types_scalars = ExecOk {
        cols: s7_scalar_cols(),
        rows: vec![s7_scalar_row()],
        affected: 0,
        last_insert_id: None,
        stats: Stats {
            queue_us: 5,
            exec_us: 21,
            rows: 1,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_types_scalars", 40, &resp_types_scalars);

    // The sentinels and the fraction-omission rule — the shapes a naive parser silently corrupts.
    // `"infinity"` / `"-infinity"` / `"0000-00-00"` / `"0000-00-00 00:00:00"` are LITERAL payloads
    // carried verbatim and deliberately NOT parseable as a calendar value (§3.2).
    //
    // The bare 30-digit DECIMAL is DELIBERATE — it is the DBAL-realistic big-integer-in-a-`numeric`
    // shape. Its only cost is that `VectorConformanceTest::hasBigUint` sees an all-digit string
    // above PHP_INT_MAX and skips THIS vector's ext-vs-pure comparison; the byte lock never
    // consults `hasBigUint`, so coverage of the bytes themselves is unaffected. Do not "fix" it.
    let resp_types_edge = ExecOk {
        cols: vec![
            ColMeta {
                name: "nan".into(),
                tag: consts::tag::DECIMAL,
            },
            ColMeta {
                name: "big".into(),
                tag: consts::tag::DECIMAL,
            },
            ColMeta {
                name: "inf".into(),
                tag: consts::tag::DATE,
            },
            ColMeta {
                name: "zerod".into(),
                tag: consts::tag::DATE,
            },
            ColMeta {
                name: "t24".into(),
                tag: consts::tag::TIME,
            },
            ColMeta {
                name: "tneg".into(),
                tag: consts::tag::TIME,
            },
            ColMeta {
                name: "whole".into(),
                tag: consts::tag::TIMESTAMP,
            },
            ColMeta {
                name: "zerots".into(),
                tag: consts::tag::TIMESTAMP,
            },
            ColMeta {
                name: "neginf".into(),
                tag: consts::tag::TIMESTAMPTZ,
            },
        ],
        rows: vec![vec![
            // PG NUMERIC allows NaN/Infinity/-Infinity; they are legal DECIMAL payloads.
            Value::Decimal("NaN".into()),
            Value::Decimal("123456789012345678901234567890".into()),
            Value::Date("infinity".into()),
            Value::Date("0000-00-00".into()), // MySQL zero date under a permissive sql_mode
            Value::Time("24:00:00".into()),   // PG-legal, chrono-hostile (chrono wraps it to 00:00)
            Value::Time("-838:59:58.000001".into()), // MySQL TIME spans +/-838h and may be negative
            // Sub-second zero => NO `.ffffff` group at all (never a trimmed variant).
            Value::Timestamp("2026-08-05 13:45:07".into()),
            Value::Timestamp("0000-00-00 00:00:00".into()), // MySQL zero datetime
            Value::TimestampTz("-infinity".into()),
        ]],
        affected: 0,
        last_insert_id: None,
        stats: Stats {
            queue_us: 6,
            exec_us: 33,
            rows: 1,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_types_edge", 41, &resp_types_edge);

    // `u64::MAX` ALONE, on purpose: it rides marker 0xcf, which PHP's pure decoder returns as a
    // decimal STRING while ext-msgpack returns a lossy int/float, so `hasBigUint` skips this
    // vector's ext-vs-pure parity test. Isolating it means the other seven S7 tags (in
    // sql_exec_response_types_scalars) keep that parity coverage. The JSON `data` is the decimal
    // string for the same reason `HelloAck.boot_epoch` is — a JSON number past 2^53 is lossy.
    let resp_types_u64 = ExecOk {
        cols: vec![ColMeta {
            name: "big".into(),
            tag: consts::tag::U64,
        }],
        rows: vec![vec![Value::U64(u64::MAX)]],
        affected: 0,
        last_insert_id: None,
        stats: Stats {
            queue_us: 1,
            exec_us: 2,
            rows: 1,
            bytes: 0,
        },
    };
    write_sql_response("sql_exec_response_types_u64", 42, &resp_types_u64);

    // --- STREAM service vectors (M1-S5 Task 1; /proto/PROTOCOL.md §10). HEAD carries the column
    // metadata (reusing the exact ColMeta shape ExecOk.cols uses); DATA carries a batch of rows
    // (reusing the exact Value [tag,payload] scalar codec ExecOk.rows uses). Neither is wrapped in
    // the Outcome envelope — that's reserved for the terminal END frame, which stays an unchanged
    // ExecOk-shaped Outcome::Ok(affected+stats, no rows) and is not re-vectored here. ---
    let stream_head_cols = StreamHead {
        cols: vec![
            ColMeta {
                name: "id".into(),
                tag: consts::tag::I64,
            },
            ColMeta {
                name: "email".into(),
                tag: consts::tag::TEXT,
            },
            ColMeta {
                name: "avatar".into(),
                tag: consts::tag::BYTES,
            },
        ],
    };
    write_stream_head("stream_head_cols", 30, &stream_head_cols);

    // A DATA batch matching stream_head_cols' 3-col arity: a Null row, the divergent-range negative
    // int (I64(-200) => `d1 ff 38`), and a BYTES cell whose first byte is the 0xc0 nil marker —
    // mirrors sql_exec_response_typedvalue's row shape, riding the SAME Value::encode as ExecOk.rows.
    let stream_data_rows = StreamData {
        rows: vec![
            vec![
                Value::I64(1),
                Value::Text("a@example.com".into()),
                Value::Bytes(vec![0xc0, 0x01]),
            ],
            vec![Value::I64(2), Value::Null, Value::Null],
            vec![
                Value::I64(-200),
                Value::Text("c@example.com".into()),
                Value::Bytes(vec![1, 2, 3]),
            ],
        ],
    };
    write_stream_data("stream_data_rows", 30, &stream_data_rows);

    // The SAME S7 scalar row as sql_exec_response_types_scalars, on the STREAMED path — the client
    // decodes a DATA frame through the same per-cell TypedValue codec (`decodeRow`), so this
    // byte-locks the streamed direction independently rather than assuming the buffered lock
    // covers it. The `stream_data_` prefix is MANDATORY (`VectorConformanceTest::streamVectors()`).
    let stream_data_types = StreamData {
        rows: vec![s7_scalar_row()],
    };
    write_stream_data("stream_data_types", 31, &stream_data_types);

    // --- TX service vectors (S6; /proto/PROTOCOL.md §9). Requests are the positional message
    // payload (flags 0). tx_begin_response is the terminal Outcome::Ok(BeginResponse) envelope
    // (flag END), mirroring how sql_exec_response_* wrap ExecOk. `tx_id` is a small native int. ---
    let begin_req = BeginRequest {
        pool: "main".into(),
        isolation: Some(Isolation::Serializable.into()), // 2
        readonly: false,
    };
    write_case(
        "tx_begin_request",
        0,
        service::TX,
        method_tx::BEGIN,
        20,
        begin_req.encode(),
        serde_json::json!({
            "pool": begin_req.pool,
            "isolation": begin_req.isolation,
            "readonly": begin_req.readonly,
        }),
    );

    let begin_resp = BeginResponse { tx_id: 42 };
    write_case(
        "tx_begin_response",
        flags::END,
        service::TX,
        method_tx::BEGIN,
        20,
        Outcome::Ok(begin_resp.encode()).encode(),
        serde_json::json!({ "status": consts::outcome::OK, "tx_id": begin_resp.tx_id }),
    );

    let commit = TxControl { tx_id: 42 };
    write_case(
        "tx_commit",
        0,
        service::TX,
        method_tx::COMMIT,
        21,
        commit.encode(),
        serde_json::json!({ "tx_id": commit.tx_id }),
    );

    let savepoint = SavepointRequest {
        tx_id: 42,
        name: Some("sp_1".into()),
    };
    write_case(
        "tx_savepoint",
        0,
        service::TX,
        method_tx::SAVEPOINT,
        22,
        savepoint.encode(),
        serde_json::json!({ "tx_id": savepoint.tx_id, "name": savepoint.name }),
    );

    // --- ADMIN service vectors (M2-C3-7b; /proto/PROTOCOL.md §11). The request is the positional
    // message payload (flags 0); the response is the terminal Outcome::Ok(BackupResponse) envelope
    // (flag END), mirroring tx_begin_*. `bytes` is above 2^32 so the u64 width is locked, not just
    // a fixint. error_forbidden is the D15 refusal a non-admin peer receives for that request. ---
    let backup_req = BackupRequest {
        pool: "main".into(),
        file: "nightly.db".into(),
        replace: true,
        timeout_ms: Some(30_000),
    };
    write_case(
        "admin_backup_request",
        0,
        service::ADMIN,
        method_admin::BACKUP,
        40,
        backup_req.encode(),
        serde_json::json!({
            "pool": backup_req.pool,
            "file": backup_req.file,
            "replace": backup_req.replace,
            "timeout_ms": backup_req.timeout_ms,
        }),
    );

    let backup_resp = BackupResponse {
        bytes: 5_000_000_000,
        queue_us: 12,
        exec_us: 734_001,
    };
    write_case(
        "admin_backup_response",
        flags::END,
        service::ADMIN,
        method_admin::BACKUP,
        40,
        Outcome::Ok(backup_resp.encode()).encode(),
        serde_json::json!({ "status": consts::outcome::OK, "bytes": backup_resp.bytes,
            "queue_us": backup_resp.queue_us, "exec_us": backup_resp.exec_us }),
    );

    let err_forbidden = ErrorPayload {
        code: consts::errc::FORBIDDEN,
        branch: consts::errc::FORBIDDEN_BRANCH,
        sqlstate: None,
        errno: None,
        message: "admin verb BACKUP is an OPERATE verb and FERRO_ADMIN_UIDS is empty".into(),
        detail: None,
        retry_after_ms: None,
    };
    // The header is the one the engine ACTUALLY sends for a D15 refusal: the session layer refuses
    // the verb before any handler runs, and a session-built per-request terminal is deliberately
    // generic (`service=CORE, method=0`) — it is identified by its `request_id`, as error_protocol is.
    write_case(
        "error_forbidden",
        flags::END,
        service::CORE,
        0,
        41,
        Outcome::Error(err_forbidden).encode(),
        serde_json::json!({ "status": consts::outcome::ERROR, "error": {
            "code": consts::errc::FORBIDDEN, "branch": consts::errc::FORBIDDEN_BRANCH,
            "sqlstate":null, "errno":null,
            "message":"admin verb BACKUP is an OPERATE verb and FERRO_ADMIN_UIDS is empty",
            "detail":null, "retry_after_ms":null } }),
    );

    // --- The out-of-band terminal (M3-D3; /proto/PROTOCOL.md §1.1). A buffered EXEC result moved
    // into a sealed memfd: the frame is the request's ONE terminal (END) with OOB_FD set, and its
    // payload is the OobRef, not an Outcome — the Outcome is what the memfd holds. `len` is past
    // u16 so the field's width is locked rather than a fixint every width would pass. ---
    let oob = OobRef {
        fd_index: 0,
        len: 1_048_578,
        encoding: consts::oob_encoding::FRAME_PAYLOAD,
    };
    write_case(
        "oob_ref",
        flags::END | flags::OOB_FD,
        service::SQL,
        method_sql::EXEC,
        50,
        oob.encode(),
        serde_json::json!({ "fd_index": oob.fd_index, "len": oob.len, "encoding": oob.encoding }),
    );

    // --- COPY vectors (M3-D4; /proto/PROTOCOL.md §13). Two requests (COPY_IN autocommit with both
    // nullables nil; COPY_OUT declared readonly, tx-scoped, with a timeout — so both arms of each
    // nullable are locked), one CopyData chunk and the empty CopyDone. The chunk is 300 bytes, so the
    // `bin16` width is locked rather than a `bin8` every codec would agree on, and it carries the COPY
    // text format's own specials (tab, newline, backslash) plus a 0xc0 byte — the msgpack nil marker,
    // which a codec that ever read the chunk as anything but opaque bytes would trip on. ---
    let copy_in_req = CopyRequest {
        pool: "main".into(),
        sql: "COPY items (id, name) FROM STDIN".into(),
        readonly: false,
        timeout_ms: None,
        tx_id: None,
    };
    write_case(
        "copy_in_request",
        0,
        service::SQL,
        method_sql::COPY_IN,
        60,
        copy_in_req.encode(),
        serde_json::json!({ "pool": copy_in_req.pool, "sql": copy_in_req.sql,
            "readonly": copy_in_req.readonly, "timeout_ms": null, "tx_id": null }),
    );
    let copy_out_req = CopyRequest {
        pool: "main".into(),
        sql: "COPY (SELECT id, name FROM items) TO STDOUT WITH (FORMAT csv)".into(),
        readonly: true,
        timeout_ms: Some(30_000),
        tx_id: Some(42),
    };
    write_case(
        "copy_out_request",
        0,
        service::SQL,
        method_sql::COPY_OUT,
        61,
        copy_out_req.encode(),
        serde_json::json!({ "pool": copy_out_req.pool, "sql": copy_out_req.sql,
            "readonly": copy_out_req.readonly, "timeout_ms": 30000, "tx_id": 42 }),
    );
    let mut chunk = b"1\tfirst\\name\n2\tsecond\n".to_vec();
    chunk.push(0xc0);
    while chunk.len() < 300 {
        chunk.push(b'x');
    }
    write_case(
        "copy_data",
        flags::STREAM,
        service::STREAM,
        method_stream::COPY_DATA,
        60,
        CopyData {
            data: chunk.clone(),
        }
        .encode(),
        serde_json::json!({ "data_hex": hex(&chunk) }),
    );
    write_case(
        "copy_done",
        0,
        service::STREAM,
        method_stream::COPY_DONE,
        60,
        CopyDone {}.encode(),
        serde_json::json!({}),
    );

    http_vectors();
    queue_vectors();

    // Negative seeds (decoder must reject; also fuzz corpus).
    let mut bad_magic = frame(
        0,
        service::CORE,
        method_core::PING,
        1,
        Ping { token: 1 }.encode(),
    );
    bad_magic[0] = 0x00;
    std::fs::write(dir().join("negative/bad_magic.bin"), &bad_magic).unwrap();

    let mut bad_ver = frame(
        0,
        service::CORE,
        method_core::PING,
        1,
        Ping { token: 1 }.encode(),
    );
    bad_ver[1] = 0x99;
    std::fs::write(dir().join("negative/bad_version.bin"), &bad_ver).unwrap();

    // Oversize payload_len with no payload body present.
    let mut oversize = Header {
        flags: 0,
        service: service::SQL,
        method: 1,
        request_id: 1,
        payload_len: consts::MAX_FRAME_PAYLOAD + 1,
    }
    .encode()
    .to_vec();
    // (intentionally no payload appended)
    oversize.truncate(16);
    std::fs::write(dir().join("negative/oversize_len.bin"), &oversize).unwrap();

    // Reserved flag set.
    let reserved = frame(
        flags::OOB_FD,
        service::CORE,
        method_core::PING,
        1,
        Ping { token: 1 }.encode(),
    );
    std::fs::write(dir().join("negative/reserved_flag.bin"), &reserved).unwrap();

    eprintln!("vectors written to {}", dir().display());
}

// --- HTTP service vectors (M6-F2; SPEC §23.5.5, /proto/PROTOCOL.md §12). `bin` fields ride the
// vector JSON as arrays of byte ints (the `BYTES` Value precedent), so a non-UTF-8 byte survives
// JSON and re-encodes byte for byte in PHP. The `http_` name prefix is MANDATORY: PHP's byte-lock
// provider `VectorConformanceTest::httpVectors()` keys on it. HTTP error terminals are handler-built,
// so — unlike `error_forbidden`'s session-built CORE/0 header — they carry the request's own
// `HTTP`/`REQUEST` header with `END`, and their `detail` is one `[http.causes]` token. ---

fn bytes_json(b: &[u8]) -> serde_json::Value {
    serde_json::Value::Array(b.iter().map(|x| serde_json::json!(*x)).collect())
}
fn opt_bytes_json(b: &Option<Vec<u8>>) -> serde_json::Value {
    b.as_deref().map_or(serde_json::Value::Null, bytes_json)
}
fn headers_json(hs: &[HttpHeaderField]) -> serde_json::Value {
    serde_json::Value::Array(
        hs.iter()
            .map(|h| serde_json::json!([h.name, bytes_json(&h.value)]))
            .collect(),
    )
}
fn hf(name: &str, value: &[u8]) -> HttpHeaderField {
    HttpHeaderField {
        name: name.into(),
        value: value.to_vec(),
    }
}

fn http_request_json(r: &HttpRequest) -> serde_json::Value {
    serde_json::json!({
        "upstream": r.upstream,
        "method": r.method,
        "target": r.target,
        "origin": r.origin,
        "headers": headers_json(&r.headers),
        "body": opt_bytes_json(&r.body),
        "timeout_ms": r.timeout_ms,
        "connect_timeout_ms": r.connect_timeout_ms,
        "read_timeout_ms": r.read_timeout_ms,
        "idempotent": r.idempotent,
        "decode": r.decode,
        "route": r.route,
        "traceparent": r.traceparent,
    })
}

fn http_head_json(h: &HttpHead) -> serde_json::Value {
    serde_json::json!({
        "status": h.status,
        "version": h.version,
        "reason": opt_bytes_json(&h.reason),
        "headers": headers_json(&h.headers),
        "decoded": h.decoded.as_ref().map(|d| serde_json::json!([d.content_encoding, d.content_length])),
        "idempotent": h.idempotent,
    })
}

fn write_http_request(name: &str, req_id: u32, r: &HttpRequest) {
    write_case(
        name,
        0,
        service::HTTP,
        method_http::REQUEST,
        req_id,
        r.encode(),
        http_request_json(r),
    );
}

fn write_http_head(name: &str, req_id: u32, h: &HttpHead) {
    write_case(
        name,
        0,
        service::HTTP,
        method_http::HEAD,
        req_id,
        h.encode(),
        http_head_json(h),
    );
}

fn write_http_error(name: &str, req_id: u32, ep: ErrorPayload) {
    let json = serde_json::json!({ "status": consts::outcome::ERROR, "error": {
        "code": ep.code, "branch": ep.branch, "sqlstate": ep.sqlstate, "errno": ep.errno,
        "message": ep.message, "detail": ep.detail, "retry_after_ms": ep.retry_after_ms } });
    write_case(
        name,
        flags::END,
        service::HTTP,
        method_http::REQUEST,
        req_id,
        Outcome::Error(ep).encode(),
        json,
    );
}

fn http_error(
    code: u16,
    branch: u8,
    message: &str,
    cause: &str,
    retry_after_ms: Option<u32>,
) -> ErrorPayload {
    ErrorPayload {
        code,
        branch,
        sqlstate: None,
        errno: None,
        message: message.into(),
        detail: Some(cause.into()),
        retry_after_ms,
    }
}

fn http_vectors() {
    use consts::{errc, http_cause};

    // The smallest real request: every optional field nil, so each nil arm is locked.
    write_http_request(
        "http_request_get",
        60,
        &HttpRequest {
            upstream: "github".into(),
            method: "GET".into(),
            target: "/repos/turbophp/ferro".into(),
            origin: None,
            headers: vec![hf("accept", b"application/json")],
            body: None,
            timeout_ms: None,
            connect_timeout_ms: None,
            read_timeout_ms: None,
            idempotent: None,
            decode: true,
            route: None,
            traceparent: None,
        },
    );
    // Every field set: a body whose first byte is the 0xc0 nil marker (a `bin` that must not be
    // read as `nil`), a header value carrying 0x80 (not UTF-8, so it must ride `bin`), a read
    // timeout past u16 (uint32 width), and the W3C specification's own example traceparent. Every
    // field of a given type holds a DISTINCT value — the six strings, the three timeouts, and the
    // two bools (`idempotent` true, `decode` false) — so a swap of two same-typed fields moves the
    // decoded message (a review finding: both bools were once false, so a swap of those two was
    // invisible here and caught only by `http_request_get`).
    write_http_request(
        "http_request_post",
        61,
        &HttpRequest {
            upstream: "billing".into(),
            method: "POST".into(),
            target: "/v1/charges?amount=2000".into(),
            origin: Some("https://api.example.com".into()),
            headers: vec![
                hf("content-type", b"application/json"),
                hf("x-raw", &[0x80, 0x41]),
                hf("idempotency-key", b"k-123"),
            ],
            body: Some(vec![0xc0, 0x7b, 0x7d]),
            timeout_ms: Some(30_000),
            connect_timeout_ms: Some(2_000),
            read_timeout_ms: Some(70_000),
            idempotent: Some(true),
            decode: false,
            route: Some("/v1/charges".into()),
            traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
        },
    );
    // HTTP/1.1: a reason phrase, a non-UTF-8 header value, and `decoded` with a Content-Length past
    // u16 (uint32 width).
    write_http_head(
        "http_head",
        61,
        &HttpHead {
            status: 201,
            version: 11,
            reason: Some(b"Created".to_vec()),
            headers: vec![
                hf("content-type", b"application/json"),
                hf("x-raw", &[0x80, 0xff]),
            ],
            decoded: Some(HttpDecoded {
                content_encoding: "gzip".into(),
                content_length: Some(70_000),
            }),
            idempotent: false,
        },
    );
    // HTTP/2: no reason phrase (nil), nothing decoded (nil), effective idempotency true.
    write_http_head(
        "http_head_h2",
        62,
        &HttpHead {
            status: 503,
            version: 20,
            reason: None,
            headers: vec![hf("retry-after", b"5")],
            decoded: None,
            idempotent: true,
        },
    );
    // A chunk of 260 bytes: every byte value once (0x00, 0x80, 0xc0 among them) plus four more, so
    // the bin16 marker is locked rather than bin8.
    let mut chunk: Vec<u8> = (0..=255u8).collect();
    chunk.extend_from_slice(&[0xc0, 0xc0, 0x80, 0x00]);
    let body = HttpBody { chunk };
    write_case(
        "http_body",
        flags::STREAM,
        service::HTTP,
        method_http::BODY,
        61,
        body.encode(),
        serde_json::json!({ "chunk": bytes_json(&body.chunk) }),
    );
    // The completed exchange's terminal: a trailer, and stats in every uint width (fixint, uint8,
    // uint16, uint32, uint64) so no width passes by accident.
    let done = HttpDone {
        trailers: vec![hf("x-checksum", b"abc")],
        stats: HttpStats {
            queue_us: 150,
            connect_us: 1_200,
            tls_us: 7,
            ttfb_us: 70_000,
            total_us: 5_000_000_000,
            bytes_sent: 412,
            bytes_received: 70_123,
            reused: true,
        },
    };
    write_case(
        "http_done",
        flags::END,
        service::HTTP,
        method_http::REQUEST,
        61,
        Outcome::Ok(done.encode()).encode(),
        serde_json::json!({
            "status": consts::outcome::OK,
            "trailers": headers_json(&done.trailers),
            "stats": [
                done.stats.queue_us, done.stats.connect_us, done.stats.tls_us, done.stats.ttfb_us,
                done.stats.total_us, done.stats.bytes_sent, done.stats.bytes_received,
                done.stats.reused,
            ],
        }),
    );

    write_http_error(
        "error_upstream_unavailable",
        63,
        http_error(
            errc::UPSTREAM_UNAVAILABLE,
            errc::UPSTREAM_UNAVAILABLE_BRANCH,
            "upstream billing: circuit breaker open",
            http_cause::BREAKER_OPEN,
            Some(30_000),
        ),
    );
    write_http_error(
        "error_rate_limited",
        64,
        http_error(
            errc::RATE_LIMITED,
            errc::RATE_LIMITED_BRANCH,
            "upstream billing: rate limit reached",
            http_cause::RATE_LIMITED,
            Some(1_500),
        ),
    );
    write_http_error(
        "error_tls_refused",
        65,
        http_error(
            errc::TLS_REFUSED,
            errc::TLS_REFUSED_BRANCH,
            "upstream billing: certificate verification failed",
            http_cause::TLS_VERIFY,
            None,
        ),
    );
    write_http_error(
        "error_response_incomplete",
        66,
        http_error(
            errc::RESPONSE_INCOMPLETE,
            errc::RESPONSE_INCOMPLETE_BRANCH,
            "upstream billing: connection closed before the body ended",
            http_cause::BODY_EOF,
            None,
        ),
    );
    write_http_error(
        "error_forbidden_http",
        67,
        http_error(
            errc::FORBIDDEN,
            errc::FORBIDDEN_BRANCH,
            "request refused by the engine's policy for this upstream",
            http_cause::FORBIDDEN_TARGET,
            None,
        ),
    );
}

// --- QUEUE service vectors (M7-G1a; SPEC §24.4, /proto/PROTOCOL.md §14). `bin` fields (every
// `job_id`, `new_job_id` and `token`) ride the vector JSON as lowercase hex under a `_hex` key, so a
// non-UTF-8 byte survives JSON (the `copy_data` precedent). The `queue_` name prefix is MANDATORY:
// PHP's byte-lock provider `VectorConformanceTest::queueVectors()` keys on it. Each handle appears at
// its `sql`-kind size AND at the registry maximum (QUEUE_HANDLE_MAX_BYTES), per SPEC §24.4; the
// refusal vectors at 0 and max + 1 bytes live in `refusal/` (written by `queue_refusal_vectors`). ---

/// The `sql` kind's 8-byte token for `(created_at, attempts)`: `created_at`'s low 32 bits above
/// `attempts`' 16, big-endian, top two bytes zero (SPEC §24.3). The layout is `ferro-queue`'s
/// (`sql::Token`); its own test asserts it mints exactly the bytes these vectors carry.
fn sql_token(created_at: i64, attempts: u16) -> Vec<u8> {
    (((created_at as u32 as u64) << 16) | u64::from(attempts))
        .to_be_bytes()
        .to_vec()
}

/// A handle of the registry's maximum length whose bytes are not UTF-8 and include `0xc0` (the nil
/// marker), so neither a `str` reading nor a `nil` peek could pass it by accident.
fn max_handle(seed: u8) -> Vec<u8> {
    (0..consts::QUEUE_HANDLE_MAX_BYTES as usize)
        .map(|i| (i as u8).wrapping_mul(7).wrapping_add(seed) | 0x80)
        .collect()
}

fn opt_hex(b: &Option<Vec<u8>>) -> serde_json::Value {
    b.as_deref()
        .map_or(serde_json::Value::Null, |b| serde_json::json!(hex(b)))
}
fn common_json(c: &QueueCommon) -> serde_json::Value {
    serde_json::json!([c.tx_id, c.timeout_ms, c.traceparent])
}
fn qstats_json(s: &QueueStats) -> serde_json::Value {
    serde_json::json!([s.queue_us, s.exec_us])
}

fn write_queue_request(
    name: &str,
    method: u16,
    req_id: u32,
    payload: Vec<u8>,
    json: serde_json::Value,
) {
    write_case(name, 0, service::QUEUE, method, req_id, payload, json);
}

/// A success terminal: `Outcome::Ok(body)` on the request's own `QUEUE`/method header with `END`.
/// The JSON carries the body's fields plus the Outcome `status`.
fn write_queue_ok(
    name: &str,
    method: u16,
    req_id: u32,
    body: Vec<u8>,
    mut json: serde_json::Value,
) {
    json["status"] = serde_json::json!(consts::outcome::OK);
    write_case(
        name,
        flags::END,
        service::QUEUE,
        method,
        req_id,
        Outcome::Ok(body).encode(),
        json,
    );
}

fn write_queue_error(name: &str, method: u16, req_id: u32, code: u16, branch: u8, message: &str) {
    let ep = ErrorPayload {
        code,
        branch,
        sqlstate: None,
        errno: None,
        message: message.into(),
        detail: None,
        retry_after_ms: None,
    };
    let json = serde_json::json!({ "status": consts::outcome::ERROR, "error": {
        "code": ep.code, "branch": ep.branch, "sqlstate": ep.sqlstate, "errno": ep.errno,
        "message": ep.message, "detail": ep.detail, "retry_after_ms": ep.retry_after_ms } });
    write_case(
        name,
        flags::END,
        service::QUEUE,
        method,
        req_id,
        Outcome::Error(ep).encode(),
        json,
    );
}

fn fenced_json(r: &FencedRequest) -> serde_json::Value {
    serde_json::json!({
        "store": r.store, "job_id_hex": hex(&r.job_id), "token_hex": hex(&r.token),
        "common": common_json(&r.common),
    })
}
fn release_json(r: &ReleaseRequest) -> serde_json::Value {
    serde_json::json!({
        "store": r.store, "job_id_hex": hex(&r.job_id), "token_hex": hex(&r.token),
        "delay_s": r.delay_s, "common": common_json(&r.common),
    })
}
fn reserve_response_json(r: &ReserveResponse) -> serde_json::Value {
    let jobs: Vec<serde_json::Value> = r
        .jobs
        .iter()
        .map(|j| {
            serde_json::json!([
                hex(&j.job_id),
                hex(&j.token),
                j.attempts,
                j.queue,
                j.payload,
                j.created_at,
                j.lease_deadline
            ])
        })
        .collect();
    serde_json::json!({ "jobs": jobs, "stats": qstats_json(&r.stats) })
}

fn queue_vectors() {
    use consts::{ack_outcome, errc};
    let traced = QueueCommon {
        tx_id: Some(5_000_000_000),
        timeout_ms: Some(70_000),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
    };
    let plain = QueueCommon::default();
    let stats = QueueStats {
        queue_us: 150,
        exec_us: 70_123,
    };

    // ENQUEUE: one job with a dedup key and every `common` field set (a tx_id past u32), so each
    // non-nil arm of `common` is locked; and a two-job batch with every optional field nil.
    let enq = EnqueueRequest {
        store: "jobs".into(),
        jobs: vec![EnqueueJob {
            queue: "emails".into(),
            payload: "{\"uuid\":\"7e1c\",\"job\":\"SendWelcome\"}".into(),
            delay_s: 0,
        }],
        dedup_key: Some("welcome:42".into()),
        common: traced.clone(),
    };
    let enq_json = |r: &EnqueueRequest| {
        let jobs: Vec<serde_json::Value> = r
            .jobs
            .iter()
            .map(|j| serde_json::json!([j.queue, j.payload, j.delay_s]))
            .collect();
        serde_json::json!({ "store": r.store, "jobs": jobs, "dedup_key": r.dedup_key,
                            "common": common_json(&r.common) })
    };
    write_queue_request(
        "queue_enqueue_request",
        method_queue::ENQUEUE,
        70,
        enq.encode(),
        enq_json(&enq),
    );
    let batch = EnqueueRequest {
        store: "jobs".into(),
        jobs: vec![
            EnqueueJob {
                queue: "default".into(),
                payload: "{}".into(),
                delay_s: 0,
            },
            EnqueueJob {
                queue: "default".into(),
                payload: "é — a multi-byte payload".into(),
                delay_s: 70_000,
            },
        ],
        dedup_key: None,
        common: plain.clone(),
    };
    write_queue_request(
        "queue_enqueue_request_batch",
        method_queue::ENQUEUE,
        71,
        batch.encode(),
        enq_json(&batch),
    );
    // The count bounds INCLUSIVE (review M3: without a positive control at the maximum, a decoder that
    // made the upper bound exclusive, or used another shape's bound, passed every other vector).
    let max_batch = EnqueueRequest {
        store: "jobs".into(),
        jobs: (0..consts::QUEUE_ENQUEUE_MAX_JOBS)
            .map(|i| EnqueueJob {
                queue: "default".into(),
                payload: format!("{{\"n\":{i}}}"),
                delay_s: i,
            })
            .collect(),
        dedup_key: None,
        common: plain.clone(),
    };
    write_queue_request(
        "queue_enqueue_request_max_jobs",
        method_queue::ENQUEUE,
        88,
        max_batch.encode(),
        enq_json(&max_batch),
    );
    let enq_resp_json = |r: &EnqueueResponse| {
        serde_json::json!({ "job_id_hex": opt_hex(&r.job_id), "inserted": r.inserted,
                            "deduplicated": r.deduplicated, "stats": qstats_json(&r.stats) })
    };
    for (name, req_id, r) in [
        (
            "queue_enqueue_response",
            70,
            // A dedup REPLAY (SPEC §24.6): the original `job_id`, nothing inserted. (Review L5: the
            // first version said `inserted: 1` beside `deduplicated: true`, which no engine sends.)
            EnqueueResponse {
                job_id: Some(b"9223372036854775807".to_vec()),
                inserted: 0,
                deduplicated: true,
                stats,
            },
        ),
        (
            "queue_enqueue_response_max",
            72,
            EnqueueResponse {
                job_id: Some(max_handle(1)),
                inserted: 1,
                deduplicated: false,
                stats,
            },
        ),
        (
            "queue_enqueue_response_batch",
            71,
            EnqueueResponse {
                job_id: None,
                inserted: 1000,
                deduplicated: false,
                stats,
            },
        ),
    ] {
        write_queue_ok(
            name,
            method_queue::ENQUEUE,
            req_id,
            r.encode(),
            enq_resp_json(&r),
        );
    }

    // RESERVE: two queues in priority order, a max_jobs past u8 and a wait_ms past u16.
    let res = ReserveRequest {
        store: "jobs".into(),
        queues: vec!["high".into(), "default".into()],
        max_jobs: 300,
        wait_ms: 70_000,
        liveness: false,
        common: plain.clone(),
    };
    write_queue_request(
        "queue_reserve_request",
        method_queue::RESERVE,
        73,
        res.encode(),
        serde_json::json!({ "store": res.store, "queues": res.queues, "max_jobs": res.max_jobs,
                            "wait_ms": res.wait_ms, "liveness": res.liveness,
                            "common": common_json(&res.common) }),
    );
    let max_queues = ReserveRequest {
        store: "jobs".into(),
        queues: (0..consts::QUEUE_RESERVE_MAX_QUEUES)
            .map(|i| format!("q{i}"))
            .collect(),
        max_jobs: 1,
        wait_ms: 0,
        liveness: false,
        common: plain.clone(),
    };
    write_queue_request(
        "queue_reserve_request_max_queues",
        method_queue::RESERVE,
        89,
        max_queues.encode(),
        serde_json::json!({ "store": max_queues.store, "queues": max_queues.queues,
                            "max_jobs": max_queues.max_jobs, "wait_ms": max_queues.wait_ms,
                            "liveness": max_queues.liveness,
                            "common": common_json(&max_queues.common) }),
    );
    // Two jobs: one at the `sql` kind's sizes (the largest canonical id, an 8-byte token), one at the
    // registry maximum for both handles. Times past u32 would not be PG `integer`s, but the WIRE is
    // `i64`, so the second job carries a negative `created_at` to lock the signed ladder.
    let rr = ReserveResponse {
        jobs: vec![
            ReservedJob {
                job_id: b"9223372036854775807".to_vec(),
                token: sql_token(1_790_000_000, 3),
                attempts: 3,
                queue: "high".into(),
                payload: "{\"job\":\"A\"}".into(),
                created_at: 1_790_000_000,
                lease_deadline: 1_790_000_091,
            },
            ReservedJob {
                job_id: max_handle(2),
                token: max_handle(3),
                attempts: 70_000,
                queue: "default".into(),
                payload: "".into(),
                created_at: -1,
                lease_deadline: 5_000_000_000,
            },
        ],
        stats,
    };
    write_queue_ok(
        "queue_reserve_response",
        method_queue::RESERVE,
        73,
        rr.encode(),
        reserve_response_json(&rr),
    );
    let empty = ReserveResponse {
        jobs: vec![],
        stats,
    };
    write_queue_ok(
        "queue_reserve_response_empty",
        method_queue::RESERVE,
        74,
        empty.encode(),
        reserve_response_json(&empty),
    );

    // ACK / EXTEND (one shape) and RELEASE: each at the `sql` sizes and at the maximum.
    let sql_fenced = FencedRequest {
        store: "jobs".into(),
        job_id: b"42".to_vec(),
        token: sql_token(1_790_000_000, 1),
        common: plain.clone(),
    };
    let max_fenced = FencedRequest {
        store: "jobs".into(),
        job_id: max_handle(4),
        token: max_handle(5),
        common: traced.clone(),
    };
    write_queue_request(
        "queue_ack_request",
        method_queue::ACK,
        75,
        sql_fenced.encode(),
        fenced_json(&sql_fenced),
    );
    write_queue_request(
        "queue_ack_request_max",
        method_queue::ACK,
        76,
        max_fenced.encode(),
        fenced_json(&max_fenced),
    );
    write_queue_request(
        "queue_extend_request",
        method_queue::EXTEND,
        77,
        sql_fenced.encode(),
        fenced_json(&sql_fenced),
    );
    write_queue_request(
        "queue_extend_request_max",
        method_queue::EXTEND,
        78,
        max_fenced.encode(),
        fenced_json(&max_fenced),
    );
    for (name, req_id, outcome) in [
        ("queue_ack_response", 75, ack_outcome::ACKED),
        ("queue_ack_response_gone", 76, ack_outcome::GONE),
    ] {
        let a = AckResponse { outcome, stats };
        write_queue_ok(
            name,
            method_queue::ACK,
            req_id,
            a.encode(),
            serde_json::json!({ "outcome": a.outcome, "stats": qstats_json(&a.stats) }),
        );
    }
    let ext = ExtendResponse {
        lease_deadline: 1_790_000_182,
        stats,
    };
    write_queue_ok(
        "queue_extend_response",
        method_queue::EXTEND,
        77,
        ext.encode(),
        serde_json::json!({ "lease_deadline": ext.lease_deadline, "stats": qstats_json(&ext.stats) }),
    );
    let sql_release = ReleaseRequest {
        store: "jobs".into(),
        job_id: b"42".to_vec(),
        token: sql_token(1_790_000_000, 1),
        delay_s: 30,
        common: plain.clone(),
    };
    let max_release = ReleaseRequest {
        store: "jobs".into(),
        job_id: max_handle(6),
        token: max_handle(7),
        delay_s: 70_000,
        common: traced.clone(),
    };
    write_queue_request(
        "queue_release_request",
        method_queue::RELEASE,
        79,
        sql_release.encode(),
        release_json(&sql_release),
    );
    write_queue_request(
        "queue_release_request_max",
        method_queue::RELEASE,
        80,
        max_release.encode(),
        release_json(&max_release),
    );
    for (name, req_id, new_job_id) in [
        ("queue_release_response", 79, Some(b"43".to_vec())),
        ("queue_release_response_max", 80, Some(max_handle(8))),
        ("queue_release_response_gone", 81, None),
    ] {
        let r = ReleaseResponse { new_job_id, stats };
        write_queue_ok(
            name,
            method_queue::RELEASE,
            req_id,
            r.encode(),
            serde_json::json!({ "new_job_id_hex": opt_hex(&r.new_job_id), "stats": qstats_json(&r.stats) }),
        );
    }

    // SIZE / CLEAR (one request shape).
    for (name, method, req_id, common) in [
        ("queue_size_request", method_queue::SIZE, 82, plain.clone()),
        (
            "queue_clear_request",
            method_queue::CLEAR,
            83,
            traced.clone(),
        ),
    ] {
        let r = QueueScopeRequest {
            store: "jobs".into(),
            queue: "default".into(),
            common,
        };
        write_queue_request(
            name,
            method,
            req_id,
            r.encode(),
            serde_json::json!({ "store": r.store, "queue": r.queue, "common": common_json(&r.common) }),
        );
    }
    // `oldest_pending_at` set (a value past u16, so the int ladder is locked) and `nil` (nothing pending).
    for (name, req_id, size) in [
        (
            "queue_size_response",
            82,
            SizeResponse {
                pending: 7,
                delayed: 300,
                reserved: 5_000_000_000,
                oldest_pending_at: Some(1_790_000_000),
                stats,
            },
        ),
        (
            "queue_size_response_empty",
            87,
            SizeResponse {
                pending: 0,
                delayed: 300,
                reserved: 0,
                oldest_pending_at: None,
                stats,
            },
        ),
    ] {
        write_queue_ok(
            name,
            method_queue::SIZE,
            req_id,
            size.encode(),
            serde_json::json!({ "pending": size.pending, "delayed": size.delayed,
                                "reserved": size.reserved,
                                "oldest_pending_at": size.oldest_pending_at,
                                "stats": qstats_json(&size.stats) }),
        );
    }
    let clear = ClearResponse {
        deleted: 70_000,
        stats,
    };
    write_queue_ok(
        "queue_clear_response",
        method_queue::CLEAR,
        83,
        clear.encode(),
        serde_json::json!({ "deleted": clear.deleted, "stats": qstats_json(&clear.stats) }),
    );

    // The three QUEUE codes, each on the request's own QUEUE/method header (handler-built).
    write_queue_error(
        "error_lease_lost",
        method_queue::ACK,
        84,
        errc::LEASE_LOST,
        errc::LEASE_LOST_BRANCH,
        "the token names no current reservation of this job; nothing was done",
    );
    write_queue_error(
        "error_pool_mismatch",
        method_queue::ENQUEUE,
        85,
        errc::POOL_MISMATCH,
        errc::POOL_MISMATCH_BRANCH,
        "store jobs is not on the transaction's pool; nothing was sent",
    );
    write_queue_error(
        "error_invalid_handle",
        method_queue::ACK,
        86,
        errc::INVALID_HANDLE,
        errc::INVALID_HANDLE_BRANCH,
        "store jobs cannot decode this job_id; nothing was sent",
    );

    queue_refusal_vectors(&sql_fenced, &sql_release, &rr);
}

/// One refusal vector: a frame whose header is valid and whose payload is a well-formed message
/// EXCEPT the one field named, which is out of the shape's bounds. Both codecs must refuse it.
fn write_refusal(name: &str, flags_: u16, method: u16, payload: Vec<u8>, field: &str, len: usize) {
    let frame = frame(flags_, service::QUEUE, method, 90, payload);
    let v = serde_json::json!({
        "name": name,
        "header": { "flags": flags_, "service": service::QUEUE, "method": method, "request_id": 90 },
        "field": field,
        "len": len,
        "frame_hex": hex(&frame),
    });
    let out = dir().join("refusal").join(format!("{name}.json"));
    std::fs::write(out, serde_json::to_string_pretty(&v).unwrap() + "\n").unwrap();
}

/// Replace the `bin` written for `marker` in `valid` with a `bin` of `len` bytes.
fn splice_bin(valid: &[u8], marker: &[u8], len: usize) -> Vec<u8> {
    let mut needle = Vec::new();
    rmp::encode::write_bin(&mut needle, marker).unwrap();
    let at = valid
        .windows(needle.len())
        .position(|w| w == needle.as_slice())
        .expect("marker present exactly where it was written");
    assert_eq!(
        valid
            .windows(needle.len())
            .filter(|w| *w == needle.as_slice())
            .count(),
        1,
        "marker must be unique"
    );
    let mut bad = Vec::new();
    rmp::encode::write_bin(&mut bad, &vec![0x5a; len]).unwrap();
    let mut out = valid[..at].to_vec();
    out.extend_from_slice(&bad);
    out.extend_from_slice(&valid[at + needle.len()..]);
    out
}

/// SPEC §24.3's G1 prerequisite (b): every opaque position gets a refusal vector at 0 bytes and at
/// QUEUE_HANDLE_MAX_BYTES + 1 — and so do the two count bounds (§24.4's `1..=1000` jobs and
/// `1..=16` queues), which are receiver-enforced shape bounds too.
fn queue_refusal_vectors(fenced: &FencedRequest, release: &ReleaseRequest, rr: &ReserveResponse) {
    std::fs::create_dir_all(dir().join("refusal")).unwrap();
    let m = b"\x01REFUSE".to_vec();
    let over = consts::QUEUE_HANDLE_MAX_BYTES as usize + 1;
    for len in [0, over] {
        let tag = if len == 0 {
            "0".to_string()
        } else {
            over.to_string()
        };
        let ok = |body: Vec<u8>| Outcome::Ok(body).encode();

        let enq = EnqueueResponse {
            job_id: Some(m.clone()),
            inserted: 1,
            deduplicated: false,
            stats: QueueStats::default(),
        };
        write_refusal(
            &format!("queue_enqueue_response_job_id_{tag}"),
            flags::END,
            method_queue::ENQUEUE,
            splice_bin(&ok(enq.encode()), &m, len),
            "job_id",
            len,
        );

        let mut r = rr.clone();
        r.jobs.truncate(1);
        r.jobs[0].job_id = m.clone();
        write_refusal(
            &format!("queue_reserve_response_job_id_{tag}"),
            flags::END,
            method_queue::RESERVE,
            splice_bin(&ok(r.encode()), &m, len),
            "job_id",
            len,
        );
        let mut r = rr.clone();
        r.jobs.truncate(1);
        r.jobs[0].token = m.clone();
        write_refusal(
            &format!("queue_reserve_response_token_{tag}"),
            flags::END,
            method_queue::RESERVE,
            splice_bin(&ok(r.encode()), &m, len),
            "token",
            len,
        );

        for (verb, method) in [("ack", method_queue::ACK), ("extend", method_queue::EXTEND)] {
            let mut f = fenced.clone();
            f.job_id = m.clone();
            write_refusal(
                &format!("queue_{verb}_request_job_id_{tag}"),
                0,
                method,
                splice_bin(&f.encode(), &m, len),
                "job_id",
                len,
            );
            let mut f = fenced.clone();
            f.token = m.clone();
            write_refusal(
                &format!("queue_{verb}_request_token_{tag}"),
                0,
                method,
                splice_bin(&f.encode(), &m, len),
                "token",
                len,
            );
        }
        let mut rq = release.clone();
        rq.job_id = m.clone();
        write_refusal(
            &format!("queue_release_request_job_id_{tag}"),
            0,
            method_queue::RELEASE,
            splice_bin(&rq.encode(), &m, len),
            "job_id",
            len,
        );
        let mut rq = release.clone();
        rq.token = m.clone();
        write_refusal(
            &format!("queue_release_request_token_{tag}"),
            0,
            method_queue::RELEASE,
            splice_bin(&rq.encode(), &m, len),
            "token",
            len,
        );

        let rel = ReleaseResponse {
            new_job_id: Some(m.clone()),
            stats: QueueStats::default(),
        };
        write_refusal(
            &format!("queue_release_response_new_job_id_{tag}"),
            flags::END,
            method_queue::RELEASE,
            splice_bin(&ok(rel.encode()), &m, len),
            "new_job_id",
            len,
        );
    }

    // The count bounds, written field by field (the encoders' debug asserts refuse to build them).
    let max_jobs = consts::QUEUE_ENQUEUE_MAX_JOBS as usize;
    for n in [0, max_jobs + 1] {
        let mut out = Vec::new();
        rmp::encode::write_array_len(&mut out, EnqueueRequest::ARITY).unwrap();
        rmp::encode::write_str(&mut out, "jobs").unwrap();
        rmp::encode::write_array_len(&mut out, n as u32).unwrap();
        for _ in 0..n {
            rmp::encode::write_array_len(&mut out, 3).unwrap();
            rmp::encode::write_str(&mut out, "q").unwrap();
            rmp::encode::write_str(&mut out, "{}").unwrap();
            rmp::encode::write_uint(&mut out, 0).unwrap();
        }
        rmp::encode::write_nil(&mut out).unwrap();
        out.extend_from_slice(&[0x93, 0xc0, 0xc0, 0xc0]); // common = [nil, nil, nil]
        write_refusal(
            &format!("queue_enqueue_request_jobs_{n}"),
            0,
            method_queue::ENQUEUE,
            out,
            "jobs",
            n,
        );
    }
    let max_queues = consts::QUEUE_RESERVE_MAX_QUEUES as usize;
    for n in [0, max_queues + 1] {
        let mut out = Vec::new();
        rmp::encode::write_array_len(&mut out, ReserveRequest::ARITY).unwrap();
        rmp::encode::write_str(&mut out, "jobs").unwrap();
        rmp::encode::write_array_len(&mut out, n as u32).unwrap();
        for i in 0..n {
            rmp::encode::write_str(&mut out, &format!("q{i}")).unwrap();
        }
        rmp::encode::write_uint(&mut out, 1).unwrap();
        rmp::encode::write_uint(&mut out, 0).unwrap();
        rmp::encode::write_bool(&mut out, false).unwrap();
        out.extend_from_slice(&[0x93, 0xc0, 0xc0, 0xc0]);
        write_refusal(
            &format!("queue_reserve_request_queues_{n}"),
            0,
            method_queue::RESERVE,
            out,
            "queues",
            n,
        );
    }
}
