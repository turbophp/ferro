use ferro_proto::header::Header;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

fn vectors_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/vectors")
}
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn positive_vectors_header_decodes_and_frame_len_is_consistent() {
    let mut count = 0;
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).expect("header decodes");
        assert_eq!(
            h.payload_len as usize,
            frame.len() - 16,
            "vector {p:?} payload_len mismatch"
        );
        assert_eq!(h.service as u64, v["header"]["service"].as_u64().unwrap());
        assert_eq!(h.method as u64, v["header"]["method"].as_u64().unwrap());
        count += 1;
    }
    // Non-vacuity only: this loop asserts a per-vector property, so it must have SEEN vectors.
    // It is deliberately NOT a coverage claim — the old `count >= 7` read like one while being
    // permanently satisfied by every committed vector set since M0, so it locked nothing. The real
    // coverage lock is `every_implemented_tag_has_a_vector` below, whose required set is DERIVED
    // from /proto/types.toml's `implemented` list and so cannot drift from the registry.
    assert!(
        count > 0,
        "no positive vectors found in {:?}",
        vectors_dir()
    );
}

/// Decode every committed positive vector with the REAL codec and collect the union of every
/// TypedValue tag it exercises: both `ColMeta.tag` (what the wire PROMISES a column is) and the
/// `Value::tag()` of every param / row cell / `last_insert_id` (what it actually DELIVERS).
///
/// Deliberately NOT a text scan of the vector JSON — a scan would pass on a vector whose `message`
/// claims a tag its `frame_hex` does not carry, which is precisely the bytes-vs-message drift the
/// byte lock exists to catch.
fn tags_present_in_committed_vectors() -> BTreeSet<u8> {
    use ferro_proto::consts::{flags, method_sql, method_stream, service};
    use ferro_proto::messages::*;

    let mut seen: BTreeSet<u8> = BTreeSet::new();
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).expect("header decodes");
        let payload = &frame[16..];
        match (h.service, h.method) {
            // An OOB_FD frame (M3-D3) carries an OobRef, never a TypedValue: the values are in the
            // memfd, which a vector cannot hold.
            _ if (h.flags & flags::OOB_FD) != 0 => {}
            // A SQL EXEC request (no END flag) carries its bind params.
            (s, m) if s == service::SQL && m == method_sql::EXEC && (h.flags & flags::END) == 0 => {
                let r = ExecRequest::decode(payload).expect("ExecRequest decodes");
                seen.extend(r.params.iter().map(|val| val.tag()));
            }
            // A SQL EXEC response (END flag): the terminal Outcome::Ok(ExecOk) — cols + rows +
            // the optional last_insert_id.
            (s, m) if s == service::SQL && m == method_sql::EXEC => {
                if let Outcome::Ok(body) = Outcome::decode(payload).expect("Outcome decodes") {
                    let ok = ExecOk::decode(&body).expect("ExecOk decodes");
                    seen.extend(ok.cols.iter().map(|c| c.tag));
                    seen.extend(ok.rows.iter().flatten().map(|val| val.tag()));
                    seen.extend(ok.last_insert_id.iter().map(|val| val.tag()));
                }
            }
            (s, m) if s == service::STREAM && m == method_stream::HEAD => {
                let head = StreamHead::decode(payload).expect("StreamHead decodes");
                seen.extend(head.cols.iter().map(|c| c.tag));
            }
            (s, m) if s == service::STREAM && m == method_stream::DATA => {
                let data = StreamData::decode(payload).expect("StreamData decodes");
                seen.extend(data.rows.iter().flatten().map(|val| val.tag()));
            }
            // Core/TX/error vectors carry no TypedValue.
            _ => {}
        }
    }
    seen
}

/// Every tag in the registry's IMPLEMENTED set must have at least one committed golden vector
/// exercising it — and no DEFERRED tag may have one. The required set is derived from
/// /proto/types.toml (the single source of truth that also feeds TYPE_REGISTRY_HASH) so the two
/// cannot drift; a hardcoded parallel list is exactly how the old `m0_scalar` key went dead.
#[test]
fn every_implemented_tag_has_a_vector() {
    use ferro_proto::registry::Registry;

    let proto = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let reg = Registry::from_toml_dir(&proto);
    let seen = tags_present_in_committed_vectors();

    for name in &reg.implemented {
        // `.get()` rather than `reg.tags[name]`: a typo in types.toml's `implemented` (e.g.
        // "TIMESTMAP") is a plausible future edit, and direct indexing panics with rustc's opaque
        // "no entry found for key" instead of naming the offending entry.
        let t = *reg.tags.get(name).unwrap_or_else(|| {
            panic!("implemented tag {name} has no entry in the [tags] table of types.toml")
        });
        assert!(
            seen.contains(&t),
            "no golden vector exercises implemented tag {name} ({t})"
        );
    }
    for (name, t) in &reg.tags {
        if !reg.implemented.contains(name) {
            assert!(
                !seen.contains(t),
                "a golden vector exercises DEFERRED tag {name} ({t}) — the vectors claim coverage \
                 the codec does not have"
            );
        }
    }
}

#[test]
fn message_payloads_are_canonical_and_byte_stable() {
    // For every positive vector, decode the payload into its typed message and re-encode it;
    // the bytes MUST be identical. Since gen-vectors produced each vector via `.encode()`, this
    // proves the on-disk bytes ARE the canonical encoder output (encode==bytes at the message
    // level), and that decode->encode is a fixpoint. This is the Rust half of the cross-language
    // byte lock; the PHP half asserts PurePacker re-encodes to these same bytes (Task 9).
    use ferro_proto::consts::{
        flags, method_admin, method_core as mc, method_http, method_queue, method_sql,
        method_stream, method_tx, service,
    };
    use ferro_proto::messages::*;
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).unwrap();
        let payload = &frame[16..];
        let reencoded: Vec<u8> = match (h.service, h.method) {
            // An OOB_FD frame (M3-D3) carries an OobRef whatever its (service, method): the real
            // payload is in the passed memfd, so it must be checked BEFORE the per-service arms,
            // which would read it as that service's own message.
            _ if (h.flags & flags::OOB_FD) != 0 => OobRef::decode(payload).unwrap().encode(),
            (s, m) if s == service::CORE && m == mc::HELLO => {
                Hello::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::CORE && m == mc::HELLO_ACK => {
                HelloAck::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::CORE && m == mc::PING => {
                Ping::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::CORE && m == mc::PONG => {
                Pong::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::CORE && m == mc::GOODBYE => {
                Goodbye::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::CORE && m == mc::WINDOW_UPDATE => {
                WindowUpdate::decode(payload).unwrap().encode()
            }
            // A SQL EXEC request (no END flag) is an ExecRequest.
            (s, m) if s == service::SQL && m == method_sql::EXEC && (h.flags & flags::END) == 0 => {
                ExecRequest::decode(payload).unwrap().encode()
            }
            // A SQL EXEC response (END flag) is a terminal Outcome::Ok(ExecOk body). CRACK the body
            // so Rust is an independent arbiter for RESPONSES, not just requests (T1-review #7):
            // ExecOk::decode(body) must re-encode to the exact body bytes. Then re-encode the whole
            // Outcome for the outer byte-stability assertion below.
            (s, m) if s == service::SQL && m == method_sql::EXEC => {
                let outcome = Outcome::decode(payload).unwrap();
                if let Outcome::Ok(body) = &outcome {
                    assert_eq!(
                        ExecOk::decode(body).unwrap().encode(),
                        *body,
                        "ExecOk body for {:?} is not canonical / byte-stable",
                        p.file_name().unwrap()
                    );
                }
                outcome.encode()
            }
            // TX request messages (no END flag): positional message payloads.
            (s, m) if s == service::TX && m == method_tx::BEGIN && (h.flags & flags::END) == 0 => {
                BeginRequest::decode(payload).unwrap().encode()
            }
            // END-guarded like the BEGIN arm above, and for the same reason: a TX RESPONSE rides the
            // same (service, method) pair as its request and is an `Outcome`, not a request body. B3's
            // `error_tx_not_found` is exactly that shape — a terminal on TX/COMMIT — and without the
            // guard it would be decoded as a `TxControl` and fail here for the wrong reason.
            (s, m) if s == service::TX && m == method_tx::COMMIT && (h.flags & flags::END) == 0 => {
                TxControl::decode(payload).unwrap().encode()
            }
            (s, m)
                if s == service::TX && m == method_tx::SAVEPOINT && (h.flags & flags::END) == 0 =>
            {
                SavepointRequest::decode(payload).unwrap().encode()
            }
            // A TX BEGIN response (END flag) is a terminal Outcome::Ok(BeginResponse body). CRACK
            // the body so Rust is an independent arbiter for the tx_id response, then re-encode the
            // whole Outcome for the outer byte-stability assertion below.
            (s, m) if s == service::TX && m == method_tx::BEGIN => {
                let outcome = Outcome::decode(payload).unwrap();
                if let Outcome::Ok(body) = &outcome {
                    assert_eq!(
                        BeginResponse::decode(body).unwrap().encode(),
                        *body,
                        "BeginResponse body for {:?} is not canonical / byte-stable",
                        p.file_name().unwrap()
                    );
                }
                outcome.encode()
            }
            // ADMIN BACKUP (M2-C3-7b): the request has no END flag; the response rides the same
            // (service, method) pair as an `Outcome`. CRACK an Ok body so Rust independently
            // arbitrates the BackupResponse layout; an Error body (error_forbidden) is re-encoded
            // whole by the outer assertion like every other error vector.
            (s, m)
                if s == service::ADMIN
                    && m == method_admin::BACKUP
                    && (h.flags & flags::END) == 0 =>
            {
                BackupRequest::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::ADMIN && m == method_admin::BACKUP => {
                let outcome = Outcome::decode(payload).unwrap();
                if let Outcome::Ok(body) = &outcome {
                    assert_eq!(
                        BackupResponse::decode(body).unwrap().encode(),
                        *body,
                        "BackupResponse body for {:?} is not canonical / byte-stable",
                        p.file_name().unwrap()
                    );
                }
                outcome.encode()
            }
            // A STREAM HEAD frame (no END flag, no Outcome envelope — see /proto/PROTOCOL.md §10):
            // a plain StreamHead message payload, exactly like an ExecRequest vector.
            (s, m) if s == service::STREAM && m == method_stream::HEAD => {
                StreamHead::decode(payload).unwrap().encode()
            }
            // A STREAM DATA frame (STREAM flag set, no END flag, no Outcome envelope): a plain
            // StreamData message payload.
            (s, m) if s == service::STREAM && m == method_stream::DATA => {
                StreamData::decode(payload).unwrap().encode()
            }
            // M3-D4 COPY: the two request bodies (no END flag), a COPY_DATA chunk (a strict `bin`,
            // either direction) and the empty COPY_DONE. The terminals are ordinary ExecOk
            // Outcomes and need no vector of their own.
            (s, m)
                if s == service::SQL
                    && (m == method_sql::COPY_IN || m == method_sql::COPY_OUT)
                    && (h.flags & flags::END) == 0 =>
            {
                CopyRequest::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::STREAM && m == method_stream::COPY_DATA => {
                CopyData::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::STREAM && m == method_stream::COPY_DONE => {
                CopyDone::decode(payload).unwrap().encode()
            }
            // HTTP (M6-F2, /proto/PROTOCOL.md §12). A REQUEST without END is the client's request;
            // with END it is the exchange's terminal — CRACK an Ok body so Rust independently
            // arbitrates the HttpDone layout (the error vectors re-encode whole, below).
            (s, m)
                if s == service::HTTP
                    && m == method_http::REQUEST
                    && (h.flags & flags::END) == 0 =>
            {
                HttpRequest::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::HTTP && m == method_http::REQUEST => {
                let outcome = Outcome::decode(payload).unwrap();
                if let Outcome::Ok(body) = &outcome {
                    assert_eq!(
                        HttpDone::decode(body).unwrap().encode(),
                        *body,
                        "HttpDone body for {:?} is not canonical / byte-stable",
                        p.file_name().unwrap()
                    );
                }
                outcome.encode()
            }
            (s, m) if s == service::HTTP && m == method_http::HEAD => {
                HttpHead::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::HTTP && m == method_http::BODY => {
                HttpBody::decode(payload).unwrap().encode()
            }
            // QUEUE (M7-G1a, /proto/PROTOCOL.md §14). Every method is client → engine, so a frame
            // without END is the request and one with END is the terminal; CRACK an Ok body with the
            // method's own response decoder (the three error vectors re-encode whole, below).
            (s, m) if s == service::QUEUE && (h.flags & flags::END) == 0 => match m {
                x if x == method_queue::ENQUEUE => {
                    EnqueueRequest::decode(payload).unwrap().encode()
                }
                x if x == method_queue::RESERVE => {
                    ReserveRequest::decode(payload).unwrap().encode()
                }
                x if x == method_queue::ACK || x == method_queue::EXTEND => {
                    FencedRequest::decode(payload).unwrap().encode()
                }
                x if x == method_queue::RELEASE => {
                    ReleaseRequest::decode(payload).unwrap().encode()
                }
                x if x == method_queue::SIZE || x == method_queue::CLEAR => {
                    QueueScopeRequest::decode(payload).unwrap().encode()
                }
                other => panic!("{p:?}: no QUEUE request decoder for method {other}"),
            },
            (s, m) if s == service::QUEUE => {
                let outcome = Outcome::decode(payload).unwrap();
                if let Outcome::Ok(body) = &outcome {
                    let again = match m {
                        x if x == method_queue::ENQUEUE => {
                            EnqueueResponse::decode(body).unwrap().encode()
                        }
                        x if x == method_queue::RESERVE => {
                            ReserveResponse::decode(body).unwrap().encode()
                        }
                        x if x == method_queue::ACK => AckResponse::decode(body).unwrap().encode(),
                        x if x == method_queue::RELEASE => {
                            ReleaseResponse::decode(body).unwrap().encode()
                        }
                        x if x == method_queue::EXTEND => {
                            ExtendResponse::decode(body).unwrap().encode()
                        }
                        x if x == method_queue::SIZE => {
                            SizeResponse::decode(body).unwrap().encode()
                        }
                        x if x == method_queue::CLEAR => {
                            ClearResponse::decode(body).unwrap().encode()
                        }
                        other => panic!("{p:?}: no QUEUE response decoder for method {other}"),
                    };
                    assert_eq!(
                        &again,
                        body,
                        "QUEUE body for {:?} is not canonical",
                        p.file_name()
                    );
                }
                outcome.encode()
            }
            // error_protocol vectors: an Outcome terminal payload (END flag).
            _ => Outcome::decode(payload).unwrap().encode(),
        };
        assert_eq!(
            reencoded,
            payload.to_vec(),
            "payload for {:?} is not canonical / byte-stable",
            p.file_name().unwrap()
        );
    }
}

/// The version-skew failure MESSAGE that `/proto/PROTOCOL.md` §"Version skew" publishes to operators
/// — `unsupported protocol version: expected 2, got 1` — must be the string the codec actually
/// produces. Built by taking a REAL committed v2 vector and rolling only its version byte to 1, so
/// this is the exact byte sequence an old client would put on the wire.
///
/// SCOPE, stated honestly: this locks the STRING and the fact that the *header* decoder is what
/// rejects an old frame. It does NOT prove the engine DELIVERS that string in an `errc::PROTOCOL`
/// terminal on `request_id=0` — that needs a live `ferrod`, and it is the other half of the same
/// published table. That half was a carry until the M1-S8a review round; it now lives in
/// `ferrod`'s `handshake.rs::a_previous_version_hello_is_answered_with_the_documented_protocol_terminal`
/// (rid, END, code, branch, message, one-frame-then-EOF). Neither test is sufficient alone: this
/// one owns the string, that one owns the delivery.
#[test]
fn a_v1_frame_is_rejected_with_the_documented_skew_message() {
    let v: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(vectors_dir().join("hello.json")).unwrap())
            .unwrap();
    let mut frame = unhex(v["frame_hex"].as_str().unwrap());
    assert_eq!(
        frame[1],
        ferro_proto::consts::PROTOCOL_VERSION,
        "the hello vector must be a CURRENT-version frame before we roll it back"
    );
    frame[1] = 1; // an old (v1) client's HELLO reaching a current engine
    let err = Header::decode(&frame).expect_err("a v1 frame must be rejected");
    // The EXPECTED number is derived, the SHAPE is literal. Hardcoding the number meant editing this
    // test on every bump (it was left at 2 when M2-C2g moved to 3), and an edit-on-bump assertion is
    // one someone eventually "fixes" by pasting whatever the code now says — which pins nothing.
    // Deriving it keeps the real claim: the wording published in /proto/PROTOCOL.md is the wording
    // the codec emits, so a reader who greps for that message finds the code that produces it.
    assert_eq!(
        err.to_string(),
        format!(
            "unsupported protocol version: expected {}, got 1",
            ferro_proto::consts::PROTOCOL_VERSION
        ),
        "the skew message published in /proto/PROTOCOL.md must be the one the codec emits"
    );
}

/// Every negative vector must be rejected FOR ITS OWN REASON.
///
/// A bare `is_err()` here could not tell a right answer from a lucky one: `Header::decode` checks
/// magic, then version, then length, and stops at the first failure — so a `bad_magic.bin` or
/// `oversize_len.bin` whose version byte drifted (e.g. left at 1 across the v1->v2 bump) would be
/// rejected by the VERSION check, never reaching the property it exists to pin, and a reason-blind
/// assertion would stay green. Each fixture therefore names its expected `CodecError` variant, and
/// the variant's fields are derived from the bytes actually on disk (rather than hardcoded) so the
/// error must also REPORT what it saw. A `.bin` with no expectation here is a hard failure.
#[test]
fn negative_vectors_are_rejected_for_their_own_reason() {
    use ferro_proto::CodecError;
    use ferro_proto::consts::{MAGIC, MAX_FRAME_PAYLOAD, PROTOCOL_VERSION};

    let neg = vectors_dir().join("negative");
    let mut seen = std::collections::HashSet::new();
    for entry in fs::read_dir(&neg).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("bin") {
            continue;
        }
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        let bytes = fs::read(&p).unwrap();
        let got = Header::decode(&bytes);
        match name.as_str() {
            "bad_magic.bin" => assert_eq!(
                got,
                Err(CodecError::BadMagic {
                    expected: MAGIC,
                    got: bytes[0]
                }),
                "bad_magic.bin must be rejected BY THE MAGIC CHECK, reporting byte 0"
            ),
            "bad_version.bin" => assert_eq!(
                got,
                Err(CodecError::BadVersion {
                    expected: PROTOCOL_VERSION,
                    got: bytes[1]
                }),
                "bad_version.bin must be rejected BY THE VERSION CHECK, reporting byte 1"
            ),
            "oversize_len.bin" => assert_eq!(
                got,
                Err(CodecError::FrameTooLarge {
                    len: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
                    max: MAX_FRAME_PAYLOAD
                }),
                "oversize_len.bin must be rejected BY THE LENGTH CHECK, reporting payload_len"
            ),
            "reserved_flag.bin" => {
                // This one has a VALID header (good magic/version/len) but sets the reserved OOB_FD
                // flag — it is rejected at the flags layer, not by Header::decode. Assert both facts.
                let h = got.expect("reserved_flag.bin has a structurally valid header");
                assert_eq!(
                    ferro_proto::flags::validate(h.flags),
                    Err(CodecError::UnsupportedFlag),
                    "reserved_flag.bin flags must be rejected by flags::validate"
                );
            }
            other => panic!(
                "negative vector {other} has no expected-reason arm — add one (a reason-blind \
                 assertion is what this test exists to prevent)"
            ),
        }
        seen.insert(name);
    }
    // Completeness guard: every required negative must be present, so a deleted/renamed .bin cannot
    // make this test (especially the reserved_flag branch) pass vacuously.
    for required in [
        "bad_magic.bin",
        "bad_version.bin",
        "oversize_len.bin",
        "reserved_flag.bin",
    ] {
        assert!(
            seen.contains(required),
            "missing required negative vector: {required}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP (M6-F2; SPEC §23.5.5, /proto/PROTOCOL.md §12)
// ---------------------------------------------------------------------------------------------

fn load(name: &str) -> serde_json::Value {
    serde_json::from_str(&fs::read_to_string(vectors_dir().join(format!("{name}.json"))).unwrap())
        .unwrap()
}
fn payload_of(v: &serde_json::Value) -> Vec<u8> {
    unhex(v["frame_hex"].as_str().unwrap())[16..].to_vec()
}
fn json_bytes(v: &serde_json::Value) -> Vec<u8> {
    v.as_array()
        .unwrap_or_else(|| panic!("expected a byte array, got {v}"))
        .iter()
        .map(|b| u8::try_from(b.as_u64().unwrap()).unwrap())
        .collect()
}
fn json_opt_bytes(v: &serde_json::Value) -> Option<Vec<u8>> {
    (!v.is_null()).then(|| json_bytes(v))
}
fn json_headers(v: &serde_json::Value) -> Vec<ferro_proto::messages::HttpHeaderField> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|pair| ferro_proto::messages::HttpHeaderField {
            name: pair[0].as_str().unwrap().to_string(),
            value: json_bytes(&pair[1]),
        })
        .collect()
}
fn json_opt_str(v: &serde_json::Value) -> Option<String> {
    v.as_str().map(str::to_string)
}
fn json_opt_u32(v: &serde_json::Value) -> Option<u32> {
    v.as_u64().map(|n| u32::try_from(n).unwrap())
}

/// The Rust half of the HTTP byte lock compares each DECODED message against its vector's NAMED
/// `message` fields. `message_payloads_are_canonical_and_byte_stable` alone cannot do this: it is a
/// decode→encode fixpoint, which a SYMMETRIC field swap (the same two same-typed fields exchanged in
/// both `encode` and `decode`) passes untouched. The JSON names each field, so a swap fails here.
#[test]
fn http_vectors_decode_to_their_named_message_fields() {
    use ferro_proto::messages::*;

    for name in ["http_request_get", "http_request_post"] {
        let v = load(name);
        let m = &v["message"];
        let got = HttpRequest::decode(&payload_of(&v)).unwrap();
        let want = HttpRequest {
            upstream: m["upstream"].as_str().unwrap().into(),
            method: m["method"].as_str().unwrap().into(),
            target: m["target"].as_str().unwrap().into(),
            origin: json_opt_str(&m["origin"]),
            headers: json_headers(&m["headers"]),
            body: json_opt_bytes(&m["body"]),
            timeout_ms: json_opt_u32(&m["timeout_ms"]),
            connect_timeout_ms: json_opt_u32(&m["connect_timeout_ms"]),
            read_timeout_ms: json_opt_u32(&m["read_timeout_ms"]),
            idempotent: m["idempotent"].as_bool(),
            decode: m["decode"].as_bool().unwrap(),
            route: json_opt_str(&m["route"]),
            traceparent: json_opt_str(&m["traceparent"]),
        };
        assert_eq!(got, want, "{name}");
    }
    // The POST vector sets every field to a DISTINCT value, so no swap of two same-typed fields
    // can leave the decoded struct equal to the message.
    let post = HttpRequest::decode(&payload_of(&load("http_request_post"))).unwrap();
    let timeouts = [
        post.timeout_ms,
        post.connect_timeout_ms,
        post.read_timeout_ms,
    ];
    assert!(timeouts.iter().all(Option::is_some));
    assert_ne!(timeouts[0], timeouts[1]);
    assert_ne!(timeouts[1], timeouts[2]);
    assert_ne!(timeouts[0], timeouts[2]);
    // ...and the two bools differ, so swapping `idempotent` and `decode` moves the message too.
    assert_eq!((post.idempotent, post.decode), (Some(true), false));
    assert_eq!(
        post.body.as_deref().map(|b| b[0]),
        Some(0xc0),
        "a bin that starts with the nil marker"
    );

    for name in ["http_head", "http_head_h2"] {
        let v = load(name);
        let m = &v["message"];
        let got = HttpHead::decode(&payload_of(&v)).unwrap();
        let want = HttpHead {
            status: u16::try_from(m["status"].as_u64().unwrap()).unwrap(),
            version: u8::try_from(m["version"].as_u64().unwrap()).unwrap(),
            reason: json_opt_bytes(&m["reason"]),
            headers: json_headers(&m["headers"]),
            decoded: (!m["decoded"].is_null()).then(|| HttpDecoded {
                content_encoding: m["decoded"][0].as_str().unwrap().into(),
                content_length: m["decoded"][1].as_u64(),
            }),
            idempotent: m["idempotent"].as_bool().unwrap(),
        };
        assert_eq!(got, want, "{name}");
    }

    let v = load("http_body");
    assert_eq!(
        HttpBody::decode(&payload_of(&v)).unwrap().chunk,
        json_bytes(&v["message"]["chunk"])
    );

    let v = load("http_done");
    let m = &v["message"];
    let Outcome::Ok(body) = Outcome::decode(&payload_of(&v)).unwrap() else {
        panic!("http_done is an Outcome::Ok");
    };
    let got = HttpDone::decode(&body).unwrap();
    let s: Vec<u64> = m["stats"].as_array().unwrap()[..7]
        .iter()
        .map(|n| n.as_u64().unwrap())
        .collect();
    let want = HttpDone {
        trailers: json_headers(&m["trailers"]),
        stats: HttpStats {
            queue_us: s[0],
            connect_us: s[1],
            tls_us: s[2],
            ttfb_us: s[3],
            total_us: s[4],
            bytes_sent: s[5],
            bytes_received: s[6],
            reused: m["stats"][7].as_bool().unwrap(),
        },
    };
    assert_eq!(got, want);
    let mut distinct = s.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        7,
        "every stat distinct, so a swap cannot hide"
    );
}

/// SPEC §23.5.5 lists the HTTP golden vectors by name. Parsed out of the spec — each listed name
/// must be committed, and every committed vector on service `HTTP` must be listed, so neither side
/// can grow alone.
#[test]
fn the_http_vectors_are_exactly_the_spec_list() {
    use ferro_proto::consts::service;
    use std::collections::BTreeSet;

    let spec = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../docs/spec/23-http.md"),
    )
    .unwrap();
    let start = spec
        .find("#### 23.5.5 Golden vectors")
        .expect("§23.5.5 heading");
    let end = start + spec[start..].find("#### 23.5.6").expect("§23.5.6 follows");
    let mut listed = BTreeSet::new();
    for line in spec[start..end].lines().filter(|l| l.starts_with("- ")) {
        // A bullet names its vectors BEFORE any parenthesis; what follows describes them (and
        // backticks field names such as `stats` or `retry_after_ms`, which are not vectors).
        let names = line.split('(').next().unwrap();
        for (i, part) in names.split('`').enumerate() {
            if i % 2 == 1 {
                listed.insert(part.to_string());
            }
        }
    }
    assert_eq!(
        listed.len(),
        11,
        "§23.5.5 lists eleven vectors, parsed {listed:?}"
    );

    let mut on_http = BTreeSet::new();
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        if v["header"]["service"].as_u64() == Some(u64::from(service::HTTP)) {
            on_http.insert(v["name"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(
        listed, on_http,
        "SPEC §23.5.5's list and the committed HTTP vectors differ"
    );
}

/// SPEC §23.5.6 / C11: on service `HTTP`, an error terminal's `detail` is EXACTLY one registry cause
/// token, and `sqlstate`/`errno` are nil. Every committed HTTP error vector is held to it, and the
/// two codes §23.5.5 says carry `retry_after_ms` do.
#[test]
fn every_http_error_vector_carries_exactly_one_cause_token() {
    use ferro_proto::consts::{errc, flags, http_cause, service};
    use ferro_proto::messages::Outcome;

    let mut seen = 0;
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).unwrap();
        if h.service != service::HTTP || h.flags & flags::END == 0 {
            continue;
        }
        let Outcome::Error(ep) = Outcome::decode(&frame[16..]).unwrap() else {
            continue;
        };
        seen += 1;
        let detail = ep.detail.as_deref().expect("an HTTP error carries a cause");
        assert!(
            http_cause::ALL.contains(&detail),
            "{p:?}: detail {detail:?} is not an [http.causes] token"
        );
        assert_eq!(
            (ep.sqlstate.as_deref(), ep.errno),
            (None, None),
            "{p:?} (C11)"
        );
        let registered = errc::ALL
            .iter()
            .find(|&&(_, c, _)| c == ep.code)
            .unwrap_or_else(|| panic!("{p:?}: code {:#06x} is not registered", ep.code));
        assert_eq!(ep.branch, registered.2, "{p:?}: branch is the registry's");
        if ep.code == errc::RATE_LIMITED || ep.code == errc::UPSTREAM_UNAVAILABLE {
            assert!(
                ep.retry_after_ms.is_some(),
                "{p:?}: §23.5.5 says it carries retry_after_ms"
            );
        }
    }
    assert_eq!(seen, 5, "five HTTP error vectors (§23.5.5)");
}

// ---------------------------------------------------------------------------------------------
// QUEUE (M7-G1a; SPEC §24.4, /proto/PROTOCOL.md §14)
// ---------------------------------------------------------------------------------------------

fn hex_bytes(v: &serde_json::Value) -> Vec<u8> {
    unhex(
        v.as_str()
            .unwrap_or_else(|| panic!("expected a hex string, got {v}")),
    )
}
fn opt_hex_bytes(v: &serde_json::Value) -> Option<Vec<u8>> {
    (!v.is_null()).then(|| hex_bytes(v))
}
fn json_common(v: &serde_json::Value) -> ferro_proto::messages::QueueCommon {
    ferro_proto::messages::QueueCommon {
        tx_id: v[0].as_u64(),
        timeout_ms: json_opt_u32(&v[1]),
        traceparent: json_opt_str(&v[2]),
    }
}
fn json_qstats(v: &serde_json::Value) -> ferro_proto::messages::QueueStats {
    ferro_proto::messages::QueueStats {
        queue_us: v[0].as_u64().unwrap(),
        exec_us: v[1].as_u64().unwrap(),
    }
}

/// The QUEUE vectors, `(name, header, message, payload)`, read from disk: every vector on service
/// `QUEUE` (positive ones only — refusals live in `refusal/`).
fn queue_vectors_on_disk() -> Vec<(String, Header, serde_json::Value, Vec<u8>)> {
    use ferro_proto::consts::service;
    let mut out = Vec::new();
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).unwrap();
        if h.service == service::QUEUE {
            out.push((
                v["name"].as_str().unwrap().to_string(),
                h,
                v["message"].clone(),
                frame[16..].to_vec(),
            ));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The Rust half of the QUEUE byte lock, by NAMED field (a decode→encode fixpoint alone passes a
/// symmetric field swap). Which decoder applies is read from the HEADER (method and `END`), never
/// from the vector's name.
#[test]
fn queue_vectors_decode_to_their_named_message_fields() {
    use ferro_proto::consts::{flags, method_queue as mq};
    use ferro_proto::messages::*;

    let vectors = queue_vectors_on_disk();
    let mut seen_ok = 0;
    for (name, h, m, payload) in &vectors {
        let end = h.flags & flags::END != 0;
        if !end {
            match h.method {
                x if x == mq::ENQUEUE => {
                    let want = EnqueueRequest {
                        store: m["store"].as_str().unwrap().into(),
                        jobs: m["jobs"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|j| EnqueueJob {
                                queue: j[0].as_str().unwrap().into(),
                                payload: j[1].as_str().unwrap().into(),
                                delay_s: json_opt_u32(&j[2]).unwrap(),
                            })
                            .collect(),
                        dedup_key: json_opt_str(&m["dedup_key"]),
                        common: json_common(&m["common"]),
                    };
                    assert_eq!(EnqueueRequest::decode(payload).unwrap(), want, "{name}");
                }
                x if x == mq::RESERVE => {
                    let want = ReserveRequest {
                        store: m["store"].as_str().unwrap().into(),
                        queues: m["queues"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|q| q.as_str().unwrap().to_string())
                            .collect(),
                        max_jobs: u16::try_from(m["max_jobs"].as_u64().unwrap()).unwrap(),
                        wait_ms: json_opt_u32(&m["wait_ms"]).unwrap(),
                        liveness: m["liveness"].as_bool().unwrap(),
                        common: json_common(&m["common"]),
                    };
                    assert_eq!(ReserveRequest::decode(payload).unwrap(), want, "{name}");
                }
                x if x == mq::ACK || x == mq::EXTEND => {
                    let want = FencedRequest {
                        store: m["store"].as_str().unwrap().into(),
                        job_id: hex_bytes(&m["job_id_hex"]),
                        token: hex_bytes(&m["token_hex"]),
                        common: json_common(&m["common"]),
                    };
                    assert_eq!(FencedRequest::decode(payload).unwrap(), want, "{name}");
                }
                x if x == mq::RELEASE => {
                    let want = ReleaseRequest {
                        store: m["store"].as_str().unwrap().into(),
                        job_id: hex_bytes(&m["job_id_hex"]),
                        token: hex_bytes(&m["token_hex"]),
                        delay_s: json_opt_u32(&m["delay_s"]).unwrap(),
                        common: json_common(&m["common"]),
                    };
                    assert_eq!(ReleaseRequest::decode(payload).unwrap(), want, "{name}");
                }
                x if x == mq::SIZE || x == mq::CLEAR => {
                    let want = QueueScopeRequest {
                        store: m["store"].as_str().unwrap().into(),
                        queue: m["queue"].as_str().unwrap().into(),
                        common: json_common(&m["common"]),
                    };
                    assert_eq!(QueueScopeRequest::decode(payload).unwrap(), want, "{name}");
                }
                other => panic!("{name}: no QUEUE request decoder for method {other}"),
            }
            continue;
        }
        let Outcome::Ok(body) = Outcome::decode(payload).unwrap() else {
            continue; // the three error vectors: `every_queue_error_vector_...` below
        };
        seen_ok += 1;
        let stats = json_qstats(&m["stats"]);
        match h.method {
            x if x == mq::ENQUEUE => assert_eq!(
                EnqueueResponse::decode(&body).unwrap(),
                EnqueueResponse {
                    job_id: opt_hex_bytes(&m["job_id_hex"]),
                    inserted: json_opt_u32(&m["inserted"]).unwrap(),
                    deduplicated: m["deduplicated"].as_bool().unwrap(),
                    stats,
                },
                "{name}"
            ),
            x if x == mq::RESERVE => assert_eq!(
                ReserveResponse::decode(&body).unwrap(),
                ReserveResponse {
                    jobs: m["jobs"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|j| ReservedJob {
                            job_id: hex_bytes(&j[0]),
                            token: hex_bytes(&j[1]),
                            attempts: json_opt_u32(&j[2]).unwrap(),
                            queue: j[3].as_str().unwrap().into(),
                            payload: j[4].as_str().unwrap().into(),
                            created_at: j[5].as_i64().unwrap(),
                            lease_deadline: j[6].as_i64().unwrap(),
                        })
                        .collect(),
                    stats,
                },
                "{name}"
            ),
            x if x == mq::ACK => assert_eq!(
                AckResponse::decode(&body).unwrap(),
                AckResponse {
                    outcome: u8::try_from(m["outcome"].as_u64().unwrap()).unwrap(),
                    stats,
                },
                "{name}"
            ),
            x if x == mq::RELEASE => assert_eq!(
                ReleaseResponse::decode(&body).unwrap(),
                ReleaseResponse {
                    new_job_id: opt_hex_bytes(&m["new_job_id_hex"]),
                    stats,
                },
                "{name}"
            ),
            x if x == mq::EXTEND => assert_eq!(
                ExtendResponse::decode(&body).unwrap(),
                ExtendResponse {
                    lease_deadline: m["lease_deadline"].as_i64().unwrap(),
                    stats,
                },
                "{name}"
            ),
            x if x == mq::SIZE => assert_eq!(
                SizeResponse::decode(&body).unwrap(),
                SizeResponse {
                    pending: m["pending"].as_u64().unwrap(),
                    delayed: m["delayed"].as_u64().unwrap(),
                    reserved: m["reserved"].as_u64().unwrap(),
                    stats,
                },
                "{name}"
            ),
            x if x == mq::CLEAR => assert_eq!(
                ClearResponse::decode(&body).unwrap(),
                ClearResponse {
                    deleted: m["deleted"].as_u64().unwrap(),
                    stats,
                },
                "{name}"
            ),
            other => panic!("{name}: no QUEUE response decoder for method {other}"),
        }
    }
    assert_eq!(seen_ok, 13, "thirteen QUEUE success-terminal vectors");
    // Every registered QUEUE method has at least one request vector AND one success vector, so a
    // method added to `[methods.queue]` without vectors fails here.
    for &(mname, id) in ferro_proto::consts::method_queue::ALL {
        for want_end in [false, true] {
            assert!(
                vectors.iter().any(|(_, h, _, p)| h.method == id
                    && (h.flags & flags::END != 0) == want_end
                    && (!want_end || matches!(Outcome::decode(p), Ok(Outcome::Ok(_))))),
                "QUEUE {mname} has no {} vector",
                if want_end {
                    "success-terminal"
                } else {
                    "request"
                }
            );
        }
    }
}

/// SPEC §24.4: every handle position is locked at its `sql`-kind size AND at the registry maximum.
/// Derived from the decoded vectors, not from their names.
#[test]
fn every_queue_handle_position_is_locked_at_the_sql_size_and_at_the_maximum() {
    use ferro_proto::consts::{QUEUE_HANDLE_MAX_BYTES, flags, method_queue as mq};
    use ferro_proto::messages::*;
    use std::collections::BTreeMap;

    let max = QUEUE_HANDLE_MAX_BYTES as usize;
    let mut lens: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
    for (_, h, _, payload) in queue_vectors_on_disk() {
        let end = h.flags & flags::END != 0;
        let mut add = |pos: &'static str, n: usize| {
            lens.entry(pos).or_default().insert(n);
        };
        if !end {
            match h.method {
                x if x == mq::ACK || x == mq::EXTEND => {
                    let r = FencedRequest::decode(&payload).unwrap();
                    let verb = if x == mq::ACK { "ack" } else { "extend" };
                    add(
                        if verb == "ack" {
                            "ack.job_id"
                        } else {
                            "extend.job_id"
                        },
                        r.job_id.len(),
                    );
                    add(
                        if verb == "ack" {
                            "ack.token"
                        } else {
                            "extend.token"
                        },
                        r.token.len(),
                    );
                }
                x if x == mq::RELEASE => {
                    let r = ReleaseRequest::decode(&payload).unwrap();
                    add("release.job_id", r.job_id.len());
                    add("release.token", r.token.len());
                }
                _ => {}
            }
            continue;
        }
        let Ok(Outcome::Ok(body)) = Outcome::decode(&payload) else {
            continue;
        };
        match h.method {
            x if x == mq::ENQUEUE => {
                if let Some(id) = EnqueueResponse::decode(&body).unwrap().job_id {
                    add("enqueue_response.job_id", id.len());
                }
            }
            x if x == mq::RESERVE => {
                for j in ReserveResponse::decode(&body).unwrap().jobs {
                    add("reserve_response.job_id", j.job_id.len());
                    add("reserve_response.token", j.token.len());
                }
            }
            x if x == mq::RELEASE => {
                if let Some(id) = ReleaseResponse::decode(&body).unwrap().new_job_id {
                    add("release_response.new_job_id", id.len());
                }
            }
            _ => {}
        }
    }
    assert_eq!(lens.len(), 10, "ten handle positions: {lens:?}");
    for (pos, got) in &lens {
        assert!(
            got.contains(&max),
            "{pos} has no {max}-byte vector: {got:?}"
        );
        let sql = if pos.ends_with("token") { 8 } else { 0 };
        if sql == 8 {
            assert!(
                got.contains(&8),
                "{pos} has no 8-byte sql token vector: {got:?}"
            );
        } else {
            assert!(
                got.iter().any(|&n| (1..=20).contains(&n)),
                "{pos} has no sql-kind (1..=20 byte) job_id vector: {got:?}"
            );
        }
    }
}

/// SPEC §24.3 prerequisite (b): every refusal vector is refused by its message's decoder, FOR ITS
/// OWN REASON (the error names the field and the length), and every opaque position and count bound
/// has one at both ends — so a deleted file cannot make this pass vacuously.
#[test]
fn queue_refusal_vectors_are_refused_for_their_own_reason() {
    use ferro_proto::CodecError;
    use ferro_proto::consts::{
        QUEUE_ENQUEUE_MAX_JOBS, QUEUE_HANDLE_MAX_BYTES, QUEUE_RESERVE_MAX_QUEUES, flags,
        method_queue as mq, service,
    };
    use ferro_proto::messages::*;

    let dir = vectors_dir().join("refusal");
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(&dir).unwrap() {
        let p = entry.unwrap().path();
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let name = v["name"].as_str().unwrap().to_string();
        let field = v["field"].as_str().unwrap();
        let len = v["len"].as_u64().unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).expect("a refusal vector's HEADER is valid");
        assert_eq!(h.service, service::QUEUE, "{name}");
        let payload = &frame[16..];
        let body;
        let msg_bytes: &[u8] = if h.flags & flags::END != 0 {
            let Outcome::Ok(b) = Outcome::decode(payload).expect("the envelope is valid") else {
                panic!("{name}: a refusal terminal is an Outcome::Ok");
            };
            body = b;
            &body
        } else {
            payload
        };
        let end = h.flags & flags::END != 0;
        let err = match (h.method, end) {
            (x, false) if x == mq::ENQUEUE => EnqueueRequest::decode(msg_bytes).err(),
            (x, false) if x == mq::RESERVE => ReserveRequest::decode(msg_bytes).err(),
            (x, false) if x == mq::ACK || x == mq::EXTEND => FencedRequest::decode(msg_bytes).err(),
            (x, false) if x == mq::RELEASE => ReleaseRequest::decode(msg_bytes).err(),
            (x, true) if x == mq::ENQUEUE => EnqueueResponse::decode(msg_bytes).err(),
            (x, true) if x == mq::RESERVE => ReserveResponse::decode(msg_bytes).err(),
            (x, true) if x == mq::RELEASE => ReleaseResponse::decode(msg_bytes).err(),
            other => panic!("{name}: no decoder arm for {other:?}"),
        };
        match err {
            Some(CodecError::Malformed(m)) => assert!(
                m.contains(field) && m.contains(&len.to_string()),
                "{name}: refused, but not for its own reason: {m}"
            ),
            other => panic!("{name} must be refused as Malformed naming {field}: {other:?}"),
        }
        seen.insert(name);
    }
    let over = QUEUE_HANDLE_MAX_BYTES + 1;
    let mut required = Vec::new();
    for n in [0, over] {
        for pos in [
            "enqueue_response_job_id",
            "reserve_response_job_id",
            "reserve_response_token",
            "ack_request_job_id",
            "ack_request_token",
            "extend_request_job_id",
            "extend_request_token",
            "release_request_job_id",
            "release_request_token",
            "release_response_new_job_id",
        ] {
            required.push(format!("queue_{pos}_{n}"));
        }
    }
    for n in [0, QUEUE_ENQUEUE_MAX_JOBS + 1] {
        required.push(format!("queue_enqueue_request_jobs_{n}"));
    }
    for n in [0, QUEUE_RESERVE_MAX_QUEUES + 1] {
        required.push(format!("queue_reserve_request_queues_{n}"));
    }
    let required: BTreeSet<String> = required.into_iter().collect();
    assert_eq!(
        seen, required,
        "the refusal set is exactly the required set"
    );
}

/// The three QUEUE codes' vectors: handler-built terminals on the request's own QUEUE/method header,
/// carrying the registered code and branch, with `sqlstate`, `errno` and `detail` nil.
#[test]
fn every_queue_error_vector_carries_its_registered_code() {
    use ferro_proto::consts::{errc, flags};
    use ferro_proto::messages::Outcome;

    let mut codes = BTreeSet::new();
    for (name, h, _, payload) in queue_vectors_on_disk() {
        if h.flags & flags::END == 0 {
            continue; // a request, not a terminal
        }
        let Outcome::Error(ep) = Outcome::decode(&payload).unwrap() else {
            continue;
        };
        assert_eq!(h.flags, flags::END, "{name}");
        let registered = errc::ALL
            .iter()
            .find(|&&(_, c, _)| c == ep.code)
            .unwrap_or_else(|| panic!("{name}: code {:#06x} is not registered", ep.code));
        assert_eq!(ep.branch, registered.2, "{name}: branch is the registry's");
        assert_eq!(
            (ep.sqlstate.as_deref(), ep.errno, ep.detail.as_deref()),
            (None, None, None),
            "{name}"
        );
        codes.insert(ep.code);
    }
    assert_eq!(
        codes,
        BTreeSet::from([errc::LEASE_LOST, errc::POOL_MISMATCH, errc::INVALID_HANDLE])
    );
    for code in [errc::LEASE_LOST, errc::POOL_MISMATCH, errc::INVALID_HANDLE] {
        let (_, _, branch) = errc::ALL.iter().find(|&&(_, c, _)| c == code).unwrap();
        assert_eq!(*branch, ferro_proto::consts::branch::NON_RETRYABLE);
    }
}
