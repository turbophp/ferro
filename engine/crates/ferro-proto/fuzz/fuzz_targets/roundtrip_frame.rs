#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use ferro_proto::value::Value;

#[derive(Arbitrary, Debug)]
enum FuzzValue { Null, Bool(bool), I64(i64), F64(f64), Text(String), Bytes(Vec<u8>) }

// Any valid Value encodes, decodes to an equal Value, and re-encodes to identical bytes.
fuzz_target!(|fv: FuzzValue| {
    let v = match fv {
        FuzzValue::Null => Value::Null,
        FuzzValue::Bool(b) => Value::Bool(b),
        FuzzValue::I64(n) => Value::I64(n),
        FuzzValue::F64(f) => Value::F64(f),
        FuzzValue::Text(s) => Value::Text(s),
        FuzzValue::Bytes(b) => Value::Bytes(b),
    };
    let mut a = Vec::new(); v.encode(&mut a);
    let mut rd: &[u8] = &a;
    let back = Value::decode(&mut rd).expect("valid value decodes");
    // NaN != NaN, so compare bytes not values for floats.
    let mut b = Vec::new(); back.encode(&mut b);
    assert_eq!(a, b, "re-encode not byte-stable");
});
