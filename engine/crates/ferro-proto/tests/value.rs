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
fn s7_text_tags_roundtrip() {
    let cases = vec![
        Value::U64(u64::MAX),
        Value::U64(0),
        Value::Decimal("-12345.6700".into()),
        Value::Decimal("NaN".into()),
        Value::Date("2026-08-05".into()),
        Value::Date("-infinity".into()),
        Value::Time("24:00:00".into()),
        Value::Time("-838:59:58.000001".into()),
        Value::Timestamp("2026-08-05 13:45:07.250000".into()),
        Value::TimestampTz("2026-08-05T13:45:07.250000Z".into()),
        Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
        Value::Json(r#"{"a":[1,2],"b":null}"#.into()),
    ];
    for v in cases {
        let mut buf = Vec::new();
        v.encode(&mut buf);
        let mut rd = &buf[..];
        let got = Value::decode(&mut rd).expect("decodes");
        assert_eq!(got, v, "roundtrip mismatch");
        assert!(rd.is_empty(), "trailing bytes left for {v:?}");
    }
}

/// Hazard 46: U64 uses the CANONICAL NARROWING ladder (write_uint), so a small U64 is a positive
/// fixint on the wire — byte-identical to PHP `PurePacker::packUint`. A marker-strict reader
/// (`dec::read_u64`) would reject exactly this.
#[test]
fn s7_u64_uses_the_canonical_narrowing_ladder() {
    let mut small = Vec::new();
    Value::U64(0).encode(&mut small);
    assert_eq!(
        small,
        vec![0x92, 0x03, 0x00],
        "U64(0) must narrow to a positive fixint"
    );
    let mut big = Vec::new();
    Value::U64(u64::MAX).encode(&mut big);
    assert_eq!(big[2], 0xcf, "U64::MAX must ride the uint64 marker");
}

#[test]
fn s7_tags_report_their_registry_tag() {
    use ferro_proto::consts::tag;
    assert_eq!(Value::U64(1).tag(), tag::U64);
    assert_eq!(Value::Decimal("1".into()).tag(), tag::DECIMAL);
    assert_eq!(Value::Date("2026-01-01".into()).tag(), tag::DATE);
    assert_eq!(Value::Time("00:00:00".into()).tag(), tag::TIME);
    assert_eq!(
        Value::Timestamp("2026-01-01 00:00:00".into()).tag(),
        tag::TIMESTAMP
    );
    assert_eq!(
        Value::TimestampTz("2026-01-01T00:00:00Z".into()).tag(),
        tag::TIMESTAMPTZ
    );
    assert_eq!(
        Value::Uuid("00000000-0000-0000-0000-000000000000".into()).tag(),
        tag::UUID
    );
    assert_eq!(Value::Json("null".into()).tag(), tag::JSON);
}

/// The still-deferred tags MUST stay rejected — this is the §22.2 deferral, enforced.
///
/// The set is DERIVED (`registry.tags` − `registry.implemented`), never hardcoded: a hand-written
/// `[ARRAY, INTERVAL, INET, VECTOR]` stops covering the moment `/proto/types.toml` grows an
/// eighteenth tag, and it silently keeps passing while the new tag goes untested — `/proto` is the
/// single source of truth (charter rule 2), including for what is NOT implemented yet.
#[test]
fn deferred_tags_are_still_rejected() {
    use ferro_proto::registry::Registry;
    let reg = Registry::from_toml_dir(
        &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto"),
    );
    let deferred: Vec<(&String, u8)> = reg
        .tags
        .iter()
        .filter(|(name, _)| !reg.implemented.contains(*name))
        .map(|(name, id)| (name, *id))
        .collect();
    // A vacuous loop is the failure mode this whole review round is about: if `implemented` ever
    // covers every tag, this test must say so out loud rather than pass over an empty set.
    assert!(
        !deferred.is_empty(),
        "no deferred tags left — delete this test deliberately, do not let it pass vacuously"
    );
    for (name, t) in deferred {
        let mut buf = Vec::new();
        rmp::encode::write_array_len(&mut buf, 2).unwrap();
        rmp::encode::write_pfix(&mut buf, t).unwrap();
        rmp::encode::write_nil(&mut buf).unwrap();
        let mut rd = &buf[..];
        assert!(
            Value::decode(&mut rd).is_err(),
            "tag {name} ({t}) is not in /proto `implemented` but the codec decodes it"
        );
    }
}

/// Hazard 2: every new str-payload tag must inherit the bounds discipline.
#[test]
fn s7_str_tags_reject_a_lying_length_prefix() {
    use ferro_proto::consts::tag;
    for t in [
        tag::DECIMAL,
        tag::DATE,
        tag::TIME,
        tag::TIMESTAMP,
        tag::TIMESTAMPTZ,
        tag::UUID,
        tag::JSON,
    ] {
        // str32 claiming 4 GiB with no bytes behind it.
        let buf = [0x92, t, 0xdb, 0xff, 0xff, 0xff, 0xff];
        let mut rd = &buf[..];
        assert!(
            Value::decode(&mut rd).is_err(),
            "tag {t} must reject a lying length"
        );
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
