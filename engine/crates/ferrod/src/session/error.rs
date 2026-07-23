//! `SessionError` distinguishes session-fatal failures (close the connection after exactly one
//! error frame) from per-request failures (an error `END` on that request id; the session
//! survives). EVERY terminal/error frame this crate ever writes is encoded as
//! `ferro_proto::messages::Outcome` — never a bare `ErrorPayload` — so there is exactly one
//! encoding path (`into_out_frame` below) for both cases.

use ferro_proto::consts::{errc, flags, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{ErrorPayload, Outcome};

use super::codec::OutFrame;

/// A session-level failure. `Fatal` carries no request id because the session-fatal convention
/// is fixed at `request_id = 0` (see `into_out_frame`); `PerRequest` carries the id of the
/// request whose terminal this becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    Fatal(ErrorPayload),
    PerRequest { rid: u32, err: ErrorPayload },
}

impl SessionError {
    /// HELLO was not the first frame on the connection, or the frame/header itself was faulty
    /// (bad magic/version, oversize length, a reserved flag set, an undecodable HELLO payload) —
    /// session-fatal, `errc::PROTOCOL`.
    pub fn protocol_fatal(detail: impl Into<String>) -> Self {
        SessionError::Fatal(error_payload(errc::PROTOCOL, errc::PROTOCOL_BRANCH, detail))
    }

    /// The client's `type_registry_hash` does not match this build's
    /// `ferro_proto::consts::TYPE_REGISTRY_HASH` — session-fatal, `errc::UNSUPPORTED` (SPEC §5:
    /// this forces a client regen/redeploy rather than silently tolerating drift).
    pub fn type_registry_mismatch(detail: impl Into<String>) -> Self {
        SessionError::Fatal(error_payload(
            errc::UNSUPPORTED,
            errc::UNSUPPORTED_BRANCH,
            detail,
        ))
    }

    /// `SO_PEERCRED` denied the connecting uid — session-fatal, `errc::AUTH`.
    pub fn peercred_denied(detail: impl Into<String>) -> Self {
        SessionError::Fatal(error_payload(errc::AUTH, errc::AUTH_BRANCH, detail))
    }

    /// Encode `self` as the terminal `OutFrame` it becomes on the wire: always
    /// `service=CORE, method=0, flags=END, payload=Outcome::Error(ep).encode()`; `request_id` is
    /// `0` for `Fatal` (the session-fatal convention) or the carried `rid` for `PerRequest`. The
    /// `service`/`method` fields are deliberately generic — a terminal is identified to the
    /// client by `request_id`, not by echoing the original request's service/method.
    pub fn into_out_frame(self) -> OutFrame {
        match self {
            SessionError::Fatal(ep) => terminal_frame(0, ep),
            SessionError::PerRequest { rid, err } => terminal_frame(rid, err),
        }
    }
}

fn error_payload(code: u16, branch: u8, detail: impl Into<String>) -> ErrorPayload {
    ErrorPayload {
        code,
        branch,
        sqlstate: None,
        errno: None,
        message: detail.into(),
        detail: None,
        retry_after_ms: None,
    }
}

fn terminal_frame(request_id: u32, ep: ErrorPayload) -> OutFrame {
    let payload = Outcome::Error(ep).encode();
    OutFrame {
        header: Header {
            flags: flags::END,
            service: service::CORE,
            method: 0,
            request_id,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_frame_is_core_rid0_end_outcome_error() {
        let frame = SessionError::protocol_fatal("boom").into_out_frame();
        assert_eq!(frame.header.service, service::CORE);
        assert_eq!(frame.header.request_id, 0);
        assert_eq!(frame.header.flags, flags::END);
        match Outcome::decode(&frame.payload).unwrap() {
            Outcome::Error(ep) => {
                assert_eq!(ep.code, errc::PROTOCOL);
                assert_eq!(ep.message, "boom");
            }
            other => panic!("expected Outcome::Error, got {other:?}"),
        }
    }

    #[test]
    fn type_registry_mismatch_uses_unsupported() {
        let frame = SessionError::type_registry_mismatch("mismatch").into_out_frame();
        match Outcome::decode(&frame.payload).unwrap() {
            Outcome::Error(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
            other => panic!("expected Outcome::Error, got {other:?}"),
        }
    }

    #[test]
    fn per_request_frame_echoes_the_given_rid() {
        let err = error_payload(errc::PROTOCOL, errc::PROTOCOL_BRANCH, "x");
        let frame = SessionError::PerRequest { rid: 42, err }.into_out_frame();
        assert_eq!(frame.header.request_id, 42);
        assert_eq!(frame.header.flags, flags::END);
    }
}
