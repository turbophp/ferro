//! Pure reader-side classification: given the codec's decode result for the next frame, decide
//! what the session layer should do next — WITHOUT awaiting anything, performing any I/O, or any
//! other side effect. This purity is exactly what lets the Task 8 fuzz target drive `classify`
//! directly against arbitrary bytes and assert "never panics, always terminates in a typed
//! outcome, never a clean close nor an infinite loop" without needing a live connection.
//!
//! Three decode outcomes are handled before flag validation ever runs:
//! - `Ok(None)` (not enough bytes buffered yet for a full frame) -> `NeedMore`. Reached only when
//!   something drives `FrameCodec::decode` directly against a growing buffer (`classify_next`,
//!   the fuzz target) — the reader loop in `session::mod` instead consumes `Framed`'s `Stream`
//!   impl, which already absorbs "wait for more bytes" internally and never surfaces it as an
//!   item, so it maps `Stream::next() == None` straight to `Closed` without ever calling
//!   `classify` with `Ok(None)`.
//! - `Err(FrameError::Io(_))` (the socket itself closed/errored, or `Framed`'s default
//!   `decode_eof` turned a truncated trailing partial frame into an I/O-shaped error) -> `Closed`
//!   — a clean close with no reply frame; there is no peer left to write one to.
//! - `Err(FrameError::Codec(_))` (a header-level fault: bad magic/version, an oversize declared
//!   `payload_len`, or a truncated header — see `ferro_proto::header::Header::decode`'s guard,
//!   which rejects an oversize length before any payload byte is ever read or allocated for) ->
//!   `Fatal`, `errc::PROTOCOL`. These faults carry no usable request id (the header itself didn't
//!   decode), so the resulting frame is `rid=0` by the session-fatal convention (see
//!   `session::error::SessionError`).
//!
//! A successfully decoded frame (`Ok(Some(frame))`) still has to clear
//! `ferro_proto::flags::validate` (see that module's own doc comment for the exact
//! RESERVED-vs-unknown split this mirrors):
//! - a RESERVED bit actually set (`OOB_FD`/`COMPRESSED`) -> `Fatal`, `errc::UNSUPPORTED` — M0
//!   recognizes these bits but implements neither (`OOB_FD` in particular implies ancillary-fd
//!   framing this codec never prepared for), so continuing to read this connection's byte stream
//!   at all is unsafe; the whole session ends.
//! - an unknown, non-reserved bit set -> `PerRequestErr`, `errc::PROTOCOL`, scoped to
//!   `frame.header.request_id` — `payload_len` was already known from the header, so this one
//!   frame is cleanly skippable and the session survives.
//! - otherwise -> `Frame(frame)`, ready for dispatch.

use bytes::BytesMut;
use tokio_util::codec::Decoder;

use ferro_proto::CodecError;
use ferro_proto::consts::errc;
use ferro_proto::flags;
use ferro_proto::messages::ErrorPayload;

use super::codec::{FrameCodec, FrameError, InFrame};
use super::error::error_payload;

/// The reader loop's next move, decided purely from a decode result.
#[derive(Debug)]
pub enum Classification {
    /// A well-formed, flag-valid frame ready for dispatch.
    Frame(InFrame),
    /// `rid`'s single request is over — send this error `END` and skip the frame; the session
    /// survives.
    PerRequestErr { rid: u32, err: ErrorPayload },
    /// Send one `rid=0` error frame, then close the connection.
    Fatal(ErrorPayload),
    /// Not enough bytes buffered yet for a full frame — read more before classifying again.
    NeedMore,
    /// The underlying I/O closed cleanly (or errored) — nothing to reply to.
    Closed,
}

/// Classify a single decode result. Pure: no awaiting, no I/O, no side effects — the entire
/// fatal/per-request/need-more/closed split lives here so it can be exercised directly (the
/// Task 8 fuzz target does exactly this) without a live connection.
pub fn classify(decode_result: Result<Option<InFrame>, &FrameError>) -> Classification {
    match decode_result {
        Ok(None) => Classification::NeedMore,
        Ok(Some(frame)) => classify_flags(frame),
        Err(FrameError::Io(_)) => Classification::Closed,
        Err(FrameError::Codec(codec_err)) => Classification::Fatal(fatal_protocol(codec_err)),
    }
}

/// Convenience for the reader loop's raw-buffer path and the fuzz target: decode the next frame
/// out of `buf` via `codec`, then classify the result in one step.
pub fn classify_next(codec: &mut FrameCodec, buf: &mut BytesMut) -> Classification {
    match codec.decode(buf) {
        Ok(frame_opt) => classify(Ok(frame_opt)),
        Err(err) => classify(Err(&err)),
    }
}

fn classify_flags(frame: InFrame) -> Classification {
    match flags::validate(frame.header.flags) {
        Ok(()) => Classification::Frame(frame),
        Err(CodecError::UnsupportedFlag) => Classification::Fatal(error_payload(
            errc::UNSUPPORTED,
            errc::UNSUPPORTED_BRANCH,
            "frame sets a reserved flag (OOB_FD/COMPRESSED) unsupported in M0",
        )),
        Err(CodecError::UnknownFlags { bits }) => Classification::PerRequestErr {
            rid: frame.header.request_id,
            err: error_payload(
                errc::PROTOCOL,
                errc::PROTOCOL_BRANCH,
                format!("unknown flag bits set: 0x{bits:04X}"),
            ),
        },
        // `flags::validate` only ever returns the two variants above; handled defensively so
        // `classify` stays total (never panics/unwraps) even against a future change to that
        // function's error set.
        Err(other) => Classification::Fatal(fatal_protocol(&other)),
    }
}

fn fatal_protocol(codec_err: &CodecError) -> ErrorPayload {
    error_payload(errc::PROTOCOL, errc::PROTOCOL_BRANCH, codec_err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ferro_proto::consts::{MAX_FRAME_PAYLOAD, flags as flag_bits, service};
    use ferro_proto::header::Header;

    fn frame(flags: u16, svc: u16, method: u16, request_id: u32, payload: &[u8]) -> InFrame {
        InFrame {
            header: Header {
                flags,
                service: svc,
                method,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: Bytes::copy_from_slice(payload),
        }
    }

    #[test]
    fn ok_none_is_need_more() {
        assert!(matches!(classify(Ok(None)), Classification::NeedMore));
    }

    #[test]
    fn io_error_is_closed() {
        let err = FrameError::Io(std::io::Error::other("boom"));
        assert!(matches!(classify(Err(&err)), Classification::Closed));
    }

    #[test]
    fn header_level_codec_error_is_fatal_protocol() {
        let err = FrameError::Codec(CodecError::FrameTooLarge {
            len: MAX_FRAME_PAYLOAD + 1,
            max: MAX_FRAME_PAYLOAD,
        });
        match classify(Err(&err)) {
            Classification::Fatal(ep) => assert_eq!(ep.code, errc::PROTOCOL),
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    #[test]
    fn valid_frame_passes_through() {
        let f = frame(flag_bits::CANCEL, service::SQL, 1, 9, &[]);
        match classify(Ok(Some(f))) {
            Classification::Frame(got) => assert_eq!(got.header.request_id, 9),
            other => panic!("expected Frame, got {other:?}"),
        }
    }

    #[test]
    fn reserved_flag_is_fatal_unsupported() {
        let f = frame(flag_bits::OOB_FD, service::SQL, 1, 9, &[]);
        match classify(Ok(Some(f))) {
            Classification::Fatal(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
            other => panic!("expected Fatal, got {other:?}"),
        }

        let f = frame(flag_bits::COMPRESSED, service::SQL, 1, 9, &[]);
        match classify(Ok(Some(f))) {
            Classification::Fatal(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    #[test]
    fn unknown_flag_bit_is_per_request_protocol() {
        let f = frame(0x8000, service::SQL, 1, 42, &[]);
        match classify(Ok(Some(f))) {
            Classification::PerRequestErr { rid, err } => {
                assert_eq!(rid, 42);
                assert_eq!(err.code, errc::PROTOCOL);
            }
            other => panic!("expected PerRequestErr, got {other:?}"),
        }
    }

    /// Smoke test ahead of the Task 8 fuzz target: a handful of crafted byte buffers driven
    /// through the real codec + `classify_next` must never panic, and must always terminate
    /// (never spin) — each one is drained until `NeedMore`/`Closed`/`Fatal` breaks the loop.
    #[test]
    fn classify_next_never_panics_over_crafted_buffers() {
        let oversize_header = Header {
            flags: 0,
            service: service::SQL,
            method: 1,
            request_id: 1,
            payload_len: MAX_FRAME_PAYLOAD + 1,
        }
        .encode();
        let truncated_body_header = Header {
            flags: 0,
            service: service::SQL,
            method: 1,
            request_id: 2,
            payload_len: 100,
        }
        .encode();

        let crafted: Vec<Vec<u8>> = vec![
            vec![],
            vec![0u8; 4],
            vec![0xFFu8; 16],
            oversize_header.to_vec(),
            truncated_body_header.to_vec(),
        ];

        for bytes_in in crafted {
            let mut codec = FrameCodec;
            let mut buf = BytesMut::from(&bytes_in[..]);
            loop {
                match classify_next(&mut codec, &mut buf) {
                    Classification::NeedMore
                    | Classification::Closed
                    | Classification::Fatal(_) => {
                        break;
                    }
                    Classification::PerRequestErr { .. } | Classification::Frame(_) => continue,
                }
            }
        }
    }
}
