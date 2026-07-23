use ferro_proto::messages::*;

#[test]
fn hello_roundtrip() {
    let h = Hello {
        client_version: 1,
        type_registry_hash: "abc".into(),
        manifest_hash: None,
        pid: 4242,
        features: 0,
    };
    assert_eq!(Hello::decode(&h.encode()).unwrap(), h);
}

#[test]
fn hello_ack_roundtrip_with_large_epoch() {
    // boot_epoch above i64::MAX exercises unsigned-smallest (uint64) — must round-trip.
    let a = HelloAck {
        engine_version: 1,
        boot_epoch: u64::MAX - 3,
        features: 0,
        pools: vec![],
        type_registry_hash: "abc".into(),
    };
    assert_eq!(HelloAck::decode(&a.encode()).unwrap(), a);
}

#[test]
fn ping_pong_goodbye_roundtrip() {
    assert_eq!(
        Ping::decode(&Ping { token: 7 }.encode()).unwrap(),
        Ping { token: 7 }
    );
    assert_eq!(
        Pong::decode(&Pong { token: 7 }.encode()).unwrap(),
        Pong { token: 7 }
    );
    assert_eq!(Goodbye::decode(&Goodbye {}.encode()).unwrap(), Goodbye {});
}

#[test]
fn window_update_roundtrip() {
    let w = WindowUpdate {
        frames: 64,
        bytes: 4_194_304,
    };
    assert_eq!(WindowUpdate::decode(&w.encode()).unwrap(), w);
}

#[test]
fn error_payload_roundtrip() {
    use ferro_proto::consts::errc;
    let e = ErrorPayload {
        code: errc::PROTOCOL,
        branch: errc::PROTOCOL_BRANCH,
        sqlstate: None,
        errno: None,
        message: "reused_request_id".into(),
        detail: None,
        retry_after_ms: None,
    };
    assert_eq!(ErrorPayload::decode(&e.encode()).unwrap(), e);
}

#[test]
fn outcome_ok_and_error() {
    let ok = Outcome::Ok(vec![0x01]); // opaque body bytes
    assert_eq!(Outcome::decode(&ok.encode()).unwrap(), ok);
    let err = Outcome::Error(ErrorPayload {
        code: 0x3001,
        branch: 3,
        sqlstate: Some("42601".into()),
        errno: None,
        message: "syntax error".into(),
        detail: None,
        retry_after_ms: None,
    });
    assert_eq!(Outcome::decode(&err.encode()).unwrap(), err);
    let c = Outcome::Cancelled;
    assert_eq!(Outcome::decode(&c.encode()).unwrap(), c);
}
