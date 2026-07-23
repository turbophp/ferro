#![no_main]
use libfuzzer_sys::fuzz_target;
use ferro_proto::header::Header;

// Arbitrary bytes in: header decode MUST NOT panic and MUST NOT allocate on an oversize length.
fuzz_target!(|data: &[u8]| {
    if let Ok(h) = Header::decode(data) {
        // If the header decodes, payload_len is already bounded by MAX_FRAME_PAYLOAD.
        // Attempt to slice the claimed payload; never trust it beyond available bytes.
        let body = &data[16.min(data.len())..];
        let take = (h.payload_len as usize).min(body.len());
        let _ = &body[..take];
        // Try message decode on the core methods; must not panic.
        let _ = ferro_proto::messages::Ping::decode(&body[..take]);
        let _ = ferro_proto::messages::Outcome::decode(&body[..take]);
        let mut rd = &body[..take];
        let _ = ferro_proto::value::Value::decode(&mut rd);
    }
});
