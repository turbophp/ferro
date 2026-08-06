#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use ferro_proto::value::Value;

// MUST mirror `ferro_proto::value::Value` variant-for-variant. This package is NOT a workspace
// member, so no `cargo clippy/test --workspace` gate catches a stale list here — a missing variant
// would silently fuzz a subset of the codec forever. Check by hand: `cd .../fuzz && cargo check`.
#[derive(Arbitrary, Debug)]
enum FuzzValue {
    Null, Bool(bool), I64(i64), F64(f64), Text(String), Bytes(Vec<u8>),
    U64(u64), Decimal(String), Date(String), Time(String),
    Timestamp(String), TimestampTz(String), Uuid(String), Json(String),
}

// Any valid Value encodes, decodes to an equal Value, and re-encodes to identical bytes.
fuzz_target!(|fv: FuzzValue| {
    let v = match fv {
        FuzzValue::Null => Value::Null,
        FuzzValue::Bool(b) => Value::Bool(b),
        FuzzValue::I64(n) => Value::I64(n),
        FuzzValue::F64(f) => Value::F64(f),
        FuzzValue::Text(s) => Value::Text(s),
        FuzzValue::Bytes(b) => Value::Bytes(b),
        FuzzValue::U64(n) => Value::U64(n),
        FuzzValue::Decimal(s) => Value::Decimal(s),
        FuzzValue::Date(s) => Value::Date(s),
        FuzzValue::Time(s) => Value::Time(s),
        FuzzValue::Timestamp(s) => Value::Timestamp(s),
        FuzzValue::TimestampTz(s) => Value::TimestampTz(s),
        FuzzValue::Uuid(s) => Value::Uuid(s),
        FuzzValue::Json(s) => Value::Json(s),
    };
    let mut a = Vec::new(); v.encode(&mut a);
    let mut rd: &[u8] = &a;
    let back = Value::decode(&mut rd).expect("valid value decodes");
    // NaN != NaN, so compare bytes not values for floats.
    let mut b = Vec::new(); back.encode(&mut b);
    assert_eq!(a, b, "re-encode not byte-stable");
});
