//! Bespoke SQL codec: ExecRequest / ExecOk round-trips, divergent-range Value cells, trailing-byte
//! rejection, oversized-array_len refusal (the MAJOR-v2a bound_len fix), Option<Value> peek, and
//! array16. See /proto/PROTOCOL.md §8.
use ferro_proto::messages::{ColMeta, ExecOk, ExecRequest, Outcome, Stats, StreamData, StreamHead};
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
        tx_id: Some(7),
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
        tx_id: None,
    };
    assert_eq!(ExecRequest::decode(&r.encode()).unwrap(), r);
}

#[test]
fn exec_request_tx_id_opt_u64_roundtrip_no_truncation() {
    // `None` => a bare nil in slot 8; `Some` => a native uint at full u64 width. A value ABOVE
    // u32::MAX proves opt-u64 (not the u32 opt helper) is on the path — a truncating helper would
    // drop the high 32 bits and fail this round trip.
    let none = ExecRequest {
        pool: "p".into(),
        sql: Some("SELECT 1".into()),
        query_id: None,
        params: vec![],
        timeout_ms: None,
        readonly: true,
        fetch: 0,
        tx_id: None,
    };
    assert_eq!(
        none.encode().last(),
        Some(&0xc0),
        "tx_id None is a trailing bare nil"
    );
    assert_eq!(ExecRequest::decode(&none.encode()).unwrap(), none);

    let wide = ExecRequest {
        tx_id: Some(0x1_0000_0000), // > u32::MAX, < 2^63
        ..none.clone()
    };
    assert_eq!(ExecRequest::decode(&wide.encode()).unwrap(), wide);
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
        tx_id: None,
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
        tx_id: None,
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
    // fixarray(8), pool "", sql nil, query_id nil, params array32(u32::MAX). The leading byte MUST
    // be 0x98 (arity 8): with the old 0x97 the decode would fail the `n != 8` arity check BEFORE
    // reaching the params `bound_len` path this test exists to exercise (a vacuous pass).
    let b = [0x98u8, 0xa0, 0xc0, 0xc0, 0xdd, 0xff, 0xff, 0xff, 0xff];
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

// --- STREAM service (HEAD/DATA, M1-S5 Task 1): the hand-rolled StreamHead/StreamData codec.
// Mirrors the ExecOk round-trip/array16/trailing-bytes/oversized-len coverage above, since both
// carry the same ColMeta/Value cells as ExecOk's cols/rows (see /proto/PROTOCOL.md §10). ---

#[test]
fn stream_head_roundtrip_cols() {
    let head = StreamHead {
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
    };
    assert_eq!(StreamHead::decode(&head.encode()).unwrap(), head);
}

#[test]
fn stream_head_roundtrip_empty_cols() {
    let head = StreamHead { cols: vec![] };
    let bytes = head.encode();
    assert_eq!(
        bytes,
        [0x91, 0x90],
        "fixarray(1) wrapping an empty cols fixarray(0)"
    );
    assert_eq!(StreamHead::decode(&bytes).unwrap(), head);
}

#[test]
fn stream_head_wide_uses_array16_marker() {
    // 16 cols forces the array16 marker (0xdc) for the cols length, exactly as ExecOk.cols does.
    let cols: Vec<ColMeta> = (0..16)
        .map(|i| ColMeta {
            name: format!("c{i}"),
            tag: 2,
        })
        .collect();
    let head = StreamHead { cols };
    let bytes = head.encode();
    assert_eq!(bytes[0], 0x91, "top-level fixarray(1)");
    assert_eq!(
        bytes[1], 0xdc,
        "cols length must be array16 (>=16 elements)"
    );
    assert!(
        contains_subslice(&bytes, &[0xdc, 0x00, 0x10]),
        "array16 marker + len 16"
    );
    assert_eq!(StreamHead::decode(&bytes).unwrap(), head);
}

#[test]
fn stream_head_trailing_bytes_rejected() {
    let head = StreamHead {
        cols: vec![ColMeta {
            name: "id".into(),
            tag: 2,
        }],
    };
    let mut b = head.encode();
    b.push(0xff);
    match StreamHead::decode(&b) {
        Err(ferro_proto::CodecError::TrailingBytes(1)) => {}
        other => panic!("expected TrailingBytes(1), got {other:?}"),
    }
}

#[test]
fn stream_head_oversized_cols_len_refused() {
    // fixarray(1), cols array32(u32::MAX) — MAJOR-v2a bound_len must refuse before Vec::with_capacity.
    let b = [0x91u8, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(StreamHead::decode(&b).is_err());
}

#[test]
fn stream_data_roundtrip_rows_incl_null_and_typed_value() {
    let data = StreamData {
        rows: vec![
            vec![Value::I64(200), Value::Text("a".into())],
            vec![Value::Null, Value::Bool(true)],
        ],
    };
    assert_eq!(StreamData::decode(&data.encode()).unwrap(), data);
}

#[test]
fn stream_data_roundtrip_empty_rows() {
    let data = StreamData { rows: vec![] };
    let bytes = data.encode();
    assert_eq!(
        bytes,
        [0x91, 0x90],
        "fixarray(1) wrapping an empty rows fixarray(0)"
    );
    assert_eq!(StreamData::decode(&bytes).unwrap(), data);
}

#[test]
fn stream_data_carries_divergent_range_ints_and_full_scalar_set() {
    // Same cross-language arbiter shape as ExecOk's resp_typedvalue vector: the full M0 scalar set
    // incl. the divergent-range ints (I64(200) => `cc c8`, I64(-200) => `d1 ff 38`) and a BYTES
    // whose first byte is the 0xc0 nil marker, all riding the SAME Value::encode as ExecOk.rows.
    let data = StreamData {
        rows: vec![vec![
            Value::Null,
            Value::Bool(true),
            Value::I64(200),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("x".into()),
            Value::Bytes(vec![0xc0, 0x01]),
        ]],
    };
    let bytes = data.encode();
    assert!(contains_subslice(&bytes, &[0xcc, 0xc8]), "I64(200) uint8");
    assert!(
        contains_subslice(&bytes, &[0xd1, 0xff, 0x38]),
        "I64(-200) int16"
    );
    assert_eq!(StreamData::decode(&bytes).unwrap(), data);
}

#[test]
fn stream_data_trailing_bytes_rejected() {
    let data = StreamData {
        rows: vec![vec![Value::I64(1)]],
    };
    let mut b = data.encode();
    b.push(0xff);
    match StreamData::decode(&b) {
        Err(ferro_proto::CodecError::TrailingBytes(1)) => {}
        other => panic!("expected TrailingBytes(1), got {other:?}"),
    }
}

#[test]
fn stream_data_oversized_rows_len_refused() {
    // fixarray(1), rows array32(u32::MAX)
    let b = [0x91u8, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(StreamData::decode(&b).is_err());
}

#[test]
fn stream_data_oversized_inner_row_len_refused() {
    // fixarray(1), rows [ array32(u32::MAX) ]
    let b = [0x91u8, 0x91, 0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(StreamData::decode(&b).is_err());
}

/// F64 specials byte-identity (T1-review #3 — the S1 F64 gap, first travels in a Value here).
/// JSON can't carry NaN/±Inf/-0.0, so a golden vector can't lock them — this is the code-level
/// lock: the codec MUST emit `[fixarray(2), F64_tag, float64(0xcb), <8 big-endian IEEE-754 bytes>]`
/// equal to `f64::to_be_bytes()`, which the PHP side mirrors via `pack('E', $f)` (see the PHP
/// F64-specials test). Also proves a bit-exact round trip (NaN != NaN, so compare bit patterns).
#[test]
fn f64_specials_byte_identity() {
    use ferro_proto::consts::tag;
    for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0f64] {
        let mut out = Vec::new();
        Value::F64(f).encode(&mut out);
        assert_eq!(out.len(), 11, "2 array + 1 tag + 1 marker + 8 float bytes");
        assert_eq!(out[0], 0x92, "fixarray(2) marker");
        assert_eq!(out[1], tag::F64, "F64 tag as a positive fixint");
        assert_eq!(out[2], 0xcb, "msgpack float64 marker");
        assert_eq!(&out[3..11], &f.to_be_bytes(), "8 big-endian IEEE-754 bytes");
        let mut rd: &[u8] = &out;
        match Value::decode(&mut rd).unwrap() {
            Value::F64(g) => assert_eq!(g.to_bits(), f.to_bits(), "bit-exact f64 round trip"),
            other => panic!("expected F64, got {other:?}"),
        }
        assert!(rd.is_empty(), "decode consumes exactly the value");
    }
}
