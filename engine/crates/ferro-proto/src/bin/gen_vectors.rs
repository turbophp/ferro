//! Emit deterministic golden vectors: for each case, build the full frame (header+payload),
//! and write {name, header, message(json), frame_hex}. Also emit malformed negative .bin seeds.
use ferro_proto::consts::{
    self, flags, method_core, method_sql, method_stream, method_tx, service,
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
        pools: vec![],
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
                            "features":0, "pools":[], "type_registry_hash":"deadbeef" }),
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
