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
        flags, method_core as mc, method_sql, method_stream, method_tx, service,
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
            (s, m) if s == service::TX && m == method_tx::COMMIT => {
                TxControl::decode(payload).unwrap().encode()
            }
            (s, m) if s == service::TX && m == method_tx::SAVEPOINT => {
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
/// rejects an old frame. It does NOT prove the engine delivers that string in an `errc::PROTOCOL`
/// terminal on `request_id=0` — that needs a live `ferrod` and is carried (see the task-11 report's
/// fix-round section). The documented `errc::PROTOCOL` code half remains derived, not asserted here.
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
    frame[1] = 1; // an old (v1) client's HELLO reaching a v2 engine
    let err = Header::decode(&frame).expect_err("a v1 frame must be rejected");
    assert_eq!(
        err.to_string(),
        "unsupported protocol version: expected 2, got 1",
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
