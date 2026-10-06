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
        // HelloAck (and Hello) carry a Vec<String> — the length-amplification-interesting
        // shape not otherwise exercised by the fixed-size messages above.
        let _ = ferro_proto::messages::HelloAck::decode(&body[..take]);
        let _ = ferro_proto::messages::Hello::decode(&body[..take]);
        // HTTP (M6-F2): REQUEST is the one HTTP message the ENGINE decodes from an untrusted
        // client, and its header list is a nested length-prefixed array of `[str, bin]` pairs;
        // the engine → client three are included because the codec is shared.
        let _ = ferro_proto::messages::HttpRequest::decode(&body[..take]);
        let _ = ferro_proto::messages::HttpHead::decode(&body[..take]);
        let _ = ferro_proto::messages::HttpBody::decode(&body[..take]);
        let _ = ferro_proto::messages::HttpDone::decode(&body[..take]);
        // QUEUE (M7-G1a): the five request shapes the ENGINE decodes from an untrusted client (with
        // their nested job/queue arrays and bounded `bin` handles), plus the response shapes the
        // shared codec also decodes.
        let _ = ferro_proto::messages::EnqueueRequest::decode(&body[..take]);
        let _ = ferro_proto::messages::ReserveRequest::decode(&body[..take]);
        let _ = ferro_proto::messages::FencedRequest::decode(&body[..take]);
        let _ = ferro_proto::messages::ReleaseRequest::decode(&body[..take]);
        let _ = ferro_proto::messages::QueueScopeRequest::decode(&body[..take]);
        let _ = ferro_proto::messages::ReserveResponse::decode(&body[..take]);
        let _ = ferro_proto::messages::EnqueueResponse::decode(&body[..take]);
        let mut rd = &body[..take];
        let _ = ferro_proto::value::Value::decode(&mut rd);
    }
});
