use ferro_proto::header::Header;
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
    assert!(count >= 7, "expected >=7 positive vectors, found {count}");
}

#[test]
fn message_payloads_are_canonical_and_byte_stable() {
    // For every positive vector, decode the payload into its typed message and re-encode it;
    // the bytes MUST be identical. Since gen-vectors produced each vector via `.encode()`, this
    // proves the on-disk bytes ARE the canonical encoder output (encode==bytes at the message
    // level), and that decode->encode is a fixpoint. This is the Rust half of the cross-language
    // byte lock; the PHP half asserts PurePacker re-encodes to these same bytes (Task 9).
    use ferro_proto::consts::{flags, method_core as mc, method_sql, service};
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
            // A SQL EXEC request (no END flag) is an ExecRequest; a SQL EXEC response (END flag) is a
            // terminal Outcome::Ok(ExecOk) and decodes via the Outcome arm below.
            (s, m) if s == service::SQL && m == method_sql::EXEC && (h.flags & flags::END) == 0 => {
                ExecRequest::decode(payload).unwrap().encode()
            }
            // error_protocol + sql_exec_response_* vectors: an Outcome terminal payload (END flag).
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

#[test]
fn negative_vectors_are_rejected() {
    let neg = vectors_dir().join("negative");
    let mut seen = std::collections::HashSet::new();
    for entry in fs::read_dir(&neg).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("bin") {
            continue;
        }
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        let bytes = fs::read(&p).unwrap();
        if name == "reserved_flag.bin" {
            // This one has a VALID header (good magic/version/len) but sets the reserved OOB_FD
            // flag — it is rejected at the flags layer, not by Header::decode. Assert both facts.
            let h =
                Header::decode(&bytes).expect("reserved_flag.bin has a structurally valid header");
            assert_eq!(
                ferro_proto::flags::validate(h.flags),
                Err(ferro_proto::CodecError::UnsupportedFlag),
                "reserved_flag.bin flags must be rejected by flags::validate"
            );
        } else {
            assert!(
                Header::decode(&bytes).is_err(),
                "negative vector {name} was NOT rejected by header decode"
            );
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
