//! Bespoke SQL codec: ExecRequest / ExecOk round-trips, divergent-range Value cells, trailing-byte
//! rejection, oversized-array_len refusal (the MAJOR-v2a bound_len fix), Option<Value> peek, and
//! array16. See /proto/PROTOCOL.md §8.
use ferro_proto::messages::{ColMeta, ExecOk, ExecRequest, Outcome, Stats};
use ferro_proto::value::Value;

fn sample_stats() -> Stats {
    Stats {
        queue_us: 12,
        exec_us: 345,
        rows: 1,
        bytes: 64,
    }
}

fn contains_subslice(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn exec_request_roundtrip_full() {
    let r = ExecRequest {
        pool: "main".into(),
        sql: Some("SELECT * FROM t WHERE a = ? AND b = ?".into()),
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
    };
    assert_eq!(ExecRequest::decode(&r.encode()).unwrap(), r);
}

#[test]
fn exec_request_roundtrip_minimal_options_absent() {
    let r = ExecRequest {
        pool: "ro".into(),
        sql: None,
        query_id: Some("q-123".into()),
        params: vec![],
        timeout_ms: None,
        readonly: true,
        fetch: 1,
    };
    assert_eq!(ExecRequest::decode(&r.encode()).unwrap(), r);
}

#[test]
fn exec_request_params_carry_divergent_range_ints() {
    // The load-bearing cross-language integer bytes must appear verbatim inside the spliced params
    // array: I64(200) => uint8 `cc c8`, I64(-200) => int16 `d1 ff 38` (see value.rs).
    let r = ExecRequest {
        pool: "p".into(),
        sql: Some("SELECT ?".into()),
        query_id: None,
        params: vec![Value::I64(200), Value::I64(-200)],
        timeout_ms: None,
        readonly: true,
        fetch: 0,
    };
    let bytes = r.encode();
    assert!(contains_subslice(&bytes, &[0xcc, 0xc8]), "I64(200) uint8");
    assert!(
        contains_subslice(&bytes, &[0xd1, 0xff, 0x38]),
        "I64(-200) int16"
    );
    assert_eq!(ExecRequest::decode(&bytes).unwrap(), r);
}

#[test]
fn exec_ok_roundtrip_cols_rows() {
    let ok = ExecOk {
        cols: vec![
            ColMeta {
                name: "id".into(),
                tag: 2,
            },
            ColMeta {
                name: "name".into(),
                tag: 6,
            },
        ],
        rows: vec![
            vec![Value::I64(200), Value::Text("a".into())],
            vec![Value::I64(-200), Value::Null],
        ],
        affected: 0,
        last_insert_id: None,
        stats: sample_stats(),
    };
    assert_eq!(ExecOk::decode(&ok.encode()).unwrap(), ok);
}

#[test]
fn exec_ok_last_insert_id_some_and_none_roundtrip() {
    let none = ExecOk {
        cols: vec![],
        rows: vec![],
        affected: 1,
        last_insert_id: None,
        stats: sample_stats(),
    };
    assert_eq!(ExecOk::decode(&none.encode()).unwrap(), none);

    // Some(divergent-range Value) locks the Option<Value> peek path (0xc0 => None, else decode).
    let some = ExecOk {
        cols: vec![],
        rows: vec![],
        affected: 1,
        last_insert_id: Some(Value::I64(200)),
        stats: sample_stats(),
    };
    let bytes = some.encode();
    // The Some encoding is a fixarray [tag, payload] (0x92 ...), never a bare nil.
    assert!(
        contains_subslice(&bytes, &[0x92, 0x02, 0xcc, 0xc8]),
        "Some(I64(200)) encodes as [I64, uint8]"
    );
    assert_eq!(ExecOk::decode(&bytes).unwrap(), some);
}

#[test]
fn exec_ok_wide_uses_array16_marker() {
    // 16 cols forces the array16 marker (0xdc) for the cols length — the top-level array is fixarray
    // (0x95) so cols' length prefix is the second byte.
    let cols: Vec<ColMeta> = (0..16)
        .map(|i| ColMeta {
            name: format!("c{i}"),
            tag: 2,
        })
        .collect();
    let rows: Vec<Vec<Value>> = vec![(0..16).map(|i| Value::I64(i as i64)).collect()];
    let ok = ExecOk {
        cols,
        rows,
        affected: 0,
        last_insert_id: None,
        stats: sample_stats(),
    };
    let bytes = ok.encode();
    assert_eq!(bytes[0], 0x95, "top-level fixarray(5)");
    assert_eq!(
        bytes[1], 0xdc,
        "cols length must be array16 (>=16 elements)"
    );
    assert!(
        contains_subslice(&bytes, &[0xdc, 0x00, 0x10]),
        "array16 marker + len 16"
    );
    assert_eq!(ExecOk::decode(&bytes).unwrap(), ok);
}

#[test]
fn exec_request_trailing_bytes_rejected() {
    let r = ExecRequest {
        pool: "p".into(),
        sql: Some("SELECT 1".into()),
        query_id: None,
        params: vec![],
        timeout_ms: None,
        readonly: true,
        fetch: 0,
    };
    let mut b = r.encode();
    b.push(0xff);
    match ExecRequest::decode(&b) {
        Err(ferro_proto::CodecError::TrailingBytes(1)) => {}
        other => panic!("expected TrailingBytes(1), got {other:?}"),
    }
}

#[test]
fn exec_ok_trailing_bytes_rejected() {
    let ok = ExecOk {
        cols: vec![ColMeta {
            name: "id".into(),
            tag: 2,
        }],
        rows: vec![vec![Value::I64(1)]],
        affected: 0,
        last_insert_id: None,
        stats: sample_stats(),
    };
    let mut b = ok.encode();
    b.push(0xff);
    // Rejected via Stats::decode's from_slice trailing-byte check (stats is the final field).
    assert!(ExecOk::decode(&b).is_err());
}

// --- MAJOR-v2a: an oversized/lying array_len must be REFUSED via bound_len, never pre-allocated.
// Each buffer declares an array32 (0xdd) length of u32::MAX with (almost) no body. With the
// bound_len guard these return a clean Err; WITHOUT it, the subsequent Vec::with_capacity(u32::MAX)
// attempts a multi-GB allocation that aborts the process — which is exactly the hole being closed.

#[test]
fn exec_request_oversized_params_len_refused() {
    // fixarray(7), pool "", sql nil, query_id nil, params array32(u32::MAX)
    let b = [0x97u8, 0xa0, 0xc0, 0xc0, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(ExecRequest::decode(&b).is_err());
}

#[test]
fn exec_ok_oversized_cols_len_refused() {
    // fixarray(5), cols array32(u32::MAX)
    let b = [0x95u8, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(ExecOk::decode(&b).is_err());
}

#[test]
fn exec_ok_oversized_rows_len_refused() {
    // fixarray(5), cols [], rows array32(u32::MAX)
    let b = [0x95u8, 0x90, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(ExecOk::decode(&b).is_err());
}

#[test]
fn exec_ok_oversized_inner_row_len_refused() {
    // fixarray(5), cols [], rows [ array32(u32::MAX) ]
    let b = [0x95u8, 0x90, 0x91, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(ExecOk::decode(&b).is_err());
}

#[test]
fn outcome_ok_composes_with_exec_ok_body() {
    let ok = ExecOk {
        cols: vec![ColMeta {
            name: "one".into(),
            tag: 2,
        }],
        rows: vec![vec![Value::I64(1)]],
        affected: 0,
        last_insert_id: None,
        stats: sample_stats(),
    };
    let outcome = Outcome::Ok(ok.encode());
    let round = Outcome::decode(&outcome.encode()).unwrap();
    assert_eq!(round, outcome);
    // The recovered opaque body is exactly one complete value: it decodes back to the same ExecOk.
    match round {
        Outcome::Ok(body) => assert_eq!(ExecOk::decode(&body).unwrap(), ok),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}
