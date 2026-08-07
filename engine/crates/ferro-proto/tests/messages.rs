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

/// `HelloAck.pools` carries STRUCTURED metadata, not bare names. Arity of `HelloAck` itself is
/// unchanged (5) — it is the ELEMENT shape that grew, which is why the version bump (not arity) is
/// what makes a skewed pair fail fast.
#[test]
fn hello_ack_carries_structured_pool_metadata() {
    let ack = HelloAck {
        engine_version: 1,
        boot_epoch: 7,
        features: 0,
        pools: vec![
            PoolInfo {
                name: "main".into(),
                kind: "postgres".into(),
                server_version: Some("PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1)".into()),
            },
            PoolInfo {
                name: "reporting".into(),
                kind: "mysql".into(),
                server_version: None,
            },
        ],
        type_registry_hash: "deadbeef".into(),
    };
    let back = HelloAck::decode(&ack.encode()).expect("round trip");
    assert_eq!(back, ack);
    assert_eq!(back.pools[0].kind, "postgres");
    assert_eq!(back.pools[1].server_version, None);
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
fn error_payload_populated_optionals_roundtrip() {
    // Exercises errno's signed path plus all three Option-Some fields (sqlstate, detail,
    // retry_after_ms), currently uncovered by error_payload_roundtrip's all-None case.
    use ferro_proto::consts::errc;
    let e = ErrorPayload {
        code: errc::DEADLOCK,
        branch: errc::DEADLOCK_BRANCH,
        sqlstate: Some("40P01".into()),
        errno: Some(-5),
        message: "deadlock".into(),
        detail: Some("detail".into()),
        retry_after_ms: Some(100),
    };
    assert_eq!(ErrorPayload::decode(&e.encode()).unwrap(), e);
}

#[test]
fn outcome_ok_and_error() {
    use ferro_proto::consts::errc;
    let ok = Outcome::Ok(vec![0x01]); // opaque body bytes
    assert_eq!(Outcome::decode(&ok.encode()).unwrap(), ok);
    let err = Outcome::Error(ErrorPayload {
        code: errc::SYNTAX,
        branch: errc::SYNTAX_BRANCH,
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
