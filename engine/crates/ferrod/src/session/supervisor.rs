//! The supervisor: the SOLE terminal-sender for request-bearing requests (SPEC's "Terminal-
//! delivery refinement (v2.1)"). A handler never writes to the wire itself — it declares its
//! outcome via a consuming `Responder` (see `session::responder`), which only stores the outcome
//! into a `cell` shared with the supervisor. Once the handler's spawned task resolves — normally,
//! by early return, or by panic — the supervisor reads that cell exactly once, builds the
//! terminal `Outcome`-encoded `END` frame, and sends it on the control-channel permit reserved
//! back when the request was inserted into the registry: a permit reserved BEFORE the handler
//! ever ran, so the send here is not merely "very likely to succeed" — it is structurally
//! guaranteed capacity regardless of anything else queued on the control channel.
//!
//! Exactly two cases, always exactly one terminal:
//!  - the handler declared a `Terminal` (`Ok`/`Error`/`Cancelled`) → send exactly that.
//!  - the handler panicked (`JoinError::is_panic()`) or returned without declaring (the cell is
//!    still `None`) → synthesize a distinct `Outcome::Error` (`errc::PROTOCOL`,
//!    `detail = NO_TERMINAL_DETAIL`) so this bug path can never be confused with a legitimately
//!    declared error.
//!
//! The registry entry for the request id is removed here, in the supervisor — never from a
//! `Drop` impl, and never on any other code path — so removal always happens exactly once, right
//! after the one-and-only terminal send.

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use ferro_proto::consts::{errc, flags};
use ferro_proto::header::Header;
use ferro_proto::messages::{ErrorPayload, Outcome};

use super::codec::OutFrame;
use super::registry::Registry;
use super::responder::Terminal;

/// The distinct `ErrorPayload.detail` marker used ONLY by the supervisor's synthetic terminal
/// (handler panicked or returned without declaring). Never used for any legitimately declared
/// error, so a client — or a test — can tell "the bug path fired" apart from an ordinary failure.
pub const NO_TERMINAL_DETAIL: &str = "supervisor-synth";

/// Await `handle` (the spawned request-handler task), then send EXACTLY ONE terminal frame on
/// `permit`, then remove `id` from `registry`. `service`/`method` are the ORIGINAL request's, so
/// the terminal frame is identified the same way the request that produced it was.
pub async fn supervise(
    id: u32,
    service: u16,
    method: u16,
    permit: mpsc::OwnedPermit<OutFrame>,
    cell: Arc<Mutex<Option<Terminal>>>,
    handle: JoinHandle<()>,
    registry: Arc<Registry>,
) {
    let terminal = match handle.await {
        Ok(()) => cell
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(no_terminal_declared),
        Err(join_err) => {
            // This design never aborts a handler's `JoinHandle`, so the only way `.await` on it
            // resolves to `Err` is a panic. Asserted, not just assumed: if some future change
            // introduces cancellation, this assertion catches the mismatch instead of silently
            // mis-attributing it to "panic".
            debug_assert!(
                join_err.is_panic(),
                "a handler JoinHandle is never aborted in this design, so a JoinError here \
                 should only ever come from a panic"
            );
            no_terminal_declared()
        }
    };

    let outcome = match terminal {
        Terminal::Ok(body) => Outcome::Ok(body.to_vec()),
        Terminal::Error(ep) => Outcome::Error(ep),
        Terminal::Cancelled => Outcome::Cancelled,
    };
    let frame = build_terminal_frame(service, method, id, outcome);
    // `permit` was reserved at insert time, before the handler ever ran: this send cannot fail
    // for lack of channel capacity. It returns the `Sender` back (tokio's `OwnedPermit::send`
    // API), which we have no further use for.
    let _sender = permit.send(frame);

    registry.remove(id);
}

/// The synthesized terminal for the panic / no-terminal bug path: a distinct `errc::PROTOCOL`
/// error carrying `NO_TERMINAL_DETAIL`.
fn no_terminal_declared() -> Terminal {
    Terminal::Error(ErrorPayload {
        code: errc::PROTOCOL,
        branch: errc::PROTOCOL_BRANCH,
        sqlstate: None,
        errno: None,
        message: "handler produced no terminal".to_string(),
        detail: Some(NO_TERMINAL_DETAIL.to_string()),
        retry_after_ms: None,
    })
}

/// Build the wire `OutFrame` for a terminal: `flags=END`, the given `service`/`method`/
/// `request_id`, payload = `outcome.encode()`. Shared by the supervisor's own terminal send and
/// by `session::mod`'s per-request diagnostic frames (reused id / max_inflight exceeded), which
/// are sent directly on the control channel rather than through a `Responder`/registry entry of
/// their own.
pub(crate) fn build_terminal_frame(
    service: u16,
    method: u16,
    request_id: u32,
    outcome: Outcome,
) -> OutFrame {
    let payload = outcome.encode();
    OutFrame {
        header: Header {
            flags: flags::END,
            service,
            method,
            request_id,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}
