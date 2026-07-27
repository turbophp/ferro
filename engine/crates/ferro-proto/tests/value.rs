use ferro_proto::value::Value;

fn enc(v: &Value) -> Vec<u8> {
    let mut o = Vec::new();
    v.encode(&mut o);
    o
}
fn dec(b: &[u8]) -> Value {
    let mut r = b;
    Value::decode(&mut r).unwrap()
}

#[test]
fn null_is_tag0_nil() {
    // fixarray(2) = 0x92 ; tag 0 = positive fixint 0x00 ; nil = 0xc0
    assert_eq!(enc(&Value::Null), vec![0x92, 0x00, 0xc0]);
}

#[test]
fn bool_true() {
    // 0x92, tag 1 (BOOL), 0xc3 (true)
    assert_eq!(enc(&Value::Bool(true)), vec![0x92, 0x01, 0xc3]);
}

#[test]
fn i64_small_positive_is_fixint() {
    // 0x92, tag 2 (I64), positive fixint 1 (0x01)
    assert_eq!(enc(&Value::I64(1)), vec![0x92, 0x02, 0x01]);
}

#[test]
fn i64_200_is_uint8() {
    // Canonical rule = rmp write_sint: non-negative 200 fits uint8 -> 0xcc 0xc8 (NOT int16).
    // This is the load-bearing cross-language byte: PHP PurePacker::packInt MUST match it.
    assert_eq!(enc(&Value::I64(200)), vec![0x92, 0x02, 0xcc, 0xc8]);
}

#[test]
fn i64_negative_uses_signed_marker() {
    // -200 does not fit i8; narrows to int16 0xd1. Negatives keep the signed ladder.
    assert_eq!(enc(&Value::I64(-200)), vec![0x92, 0x02, 0xd1, 0xff, 0x38]);
}

#[test]
fn f64_is_always_float64() {
    let b = enc(&Value::F64(1.5));
    assert_eq!(b[0], 0x92);
    assert_eq!(b[1], 0x04); // tag F64
    assert_eq!(b[2], 0xcb); // float64 marker
}

#[test]
fn text_uses_str_family() {
    // "hi" -> fixstr len2 = 0xa2 'h' 'i'
    assert_eq!(
        enc(&Value::Text("hi".into())),
        vec![0x92, 0x06, 0xa2, b'h', b'i']
    );
}

#[test]
fn bytes_uses_bin_family() {
    // 3 bytes -> bin8 0xc4 0x03 <data>
    assert_eq!(
        enc(&Value::Bytes(vec![1, 2, 3])),
        vec![0x92, 0x07, 0xc4, 0x03, 1, 2, 3]
    );
}

#[test]
fn roundtrip_all_scalars() {
    for v in [
        Value::Null,
        Value::Bool(false),
        Value::I64(-40000),
        Value::F64(-0.0),
        Value::Text(String::new()),
        Value::Bytes(vec![]),
    ] {
        assert_eq!(dec(&enc(&v)), v);
    }
}

#[test]
fn lying_length_prefix_is_rejected_before_allocating() {
    // str32 (0xdb) claiming ~4 GiB with no body must error via the bound check, NOT pre-allocate.
    let s = [0x92u8, 0x06, 0xdb, 0xff, 0xff, 0xff, 0xff];
    let mut r = &s[..];
    assert!(Value::decode(&mut r).is_err());
    // bin32 (0xc6) claiming ~4 GiB with no body: same.
    let b = [0x92u8, 0x07, 0xc6, 0xff, 0xff, 0xff, 0xff];
    let mut r = &b[..];
    assert!(Value::decode(&mut r).is_err());
}
