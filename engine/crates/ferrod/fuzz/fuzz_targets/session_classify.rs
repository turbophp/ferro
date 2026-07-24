#![no_main]
use bytes::BytesMut;
use ferrod::session::classify::{Classification, classify_next};
use ferrod::session::codec::FrameCodec;
use libfuzzer_sys::fuzz_target;

// Arbitrary client bytes fed to the reader's classification step must NEVER panic and must always
// make progress: every call returns a typed Classification; the loop terminates on
// NeedMore/Closed/Fatal — mirroring the real reader loop, where NeedMore means "wait for more
// bytes", Closed means the connection is already gone, and Fatal means the session sends its one
// rid=0 error frame and closes the connection itself (see `session::classify`'s doc comment) — so
// none of the three ever warrants classifying further bytes out of the same buffer.
fuzz_target!(|data: &[u8]| {
    let mut codec = FrameCodec;
    let mut buf = BytesMut::from(data);
    // Bounded iterations as a belt-and-suspenders against any accidental non-advancing case.
    for _ in 0..10_000 {
        match classify_next(&mut codec, &mut buf) {
            Classification::NeedMore | Classification::Closed | Classification::Fatal(_) => break,
            Classification::Frame(_) | Classification::PerRequestErr { .. } => {
                // Consumed one well-formed (or per-request-diagnostic) frame; keep draining
                // whatever bytes remain in this same fuzz input.
            }
        }
    }
});
