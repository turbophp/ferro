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
use ferro_proto::messages::{ErrorPayload, OobRef, Outcome};

use super::codec::{ControlMsg, OutFrame};
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
    permit: mpsc::OwnedPermit<ControlMsg>,
    cell: Arc<Mutex<Option<Terminal>>>,
    handle: JoinHandle<()>,
    registry: Arc<Registry>,
) {
    supervise_with(id, service, method, permit, cell, handle, registry, None).await;
}

/// [`supervise`], with the session's out-of-band threshold (M3-D3): `Some(t)` when the client
/// advertised `MEMFD_RX` and the engine's OOB path is enabled, in which case a SUCCESS terminal whose
/// inline payload would be at least `t` bytes is sent as an `OOB_FD` terminal instead
/// ([`ok_terminal`]). Error and cancelled terminals are always small and always inline. Either way it
/// is still exactly ONE terminal frame on the one reserved permit.
#[allow(clippy::too_many_arguments)]
pub async fn supervise_with(
    id: u32,
    service: u16,
    method: u16,
    permit: mpsc::OwnedPermit<ControlMsg>,
    cell: Arc<Mutex<Option<Terminal>>>,
    handle: JoinHandle<()>,
    registry: Arc<Registry>,
    oob_threshold: Option<usize>,
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

    let msg = match terminal {
        Terminal::Ok(body) => ok_terminal(service, method, id, &body, oob_threshold),
        Terminal::Error(ep) => ControlMsg::bare(build_terminal_frame(
            service,
            method,
            id,
            Outcome::Error(ep),
        )),
        Terminal::Cancelled => ControlMsg::bare(build_terminal_frame(
            service,
            method,
            id,
            Outcome::Cancelled,
        )),
    };
    // `permit` was reserved at insert time, before the handler ever ran: this send cannot fail
    // for lack of channel capacity. It returns the `Sender` back (tokio's `OwnedPermit::send`
    // API), which we have no further use for. The terminal carries no cap reservation (`cap:
    // None`) — it rides the SAME ordered conduit as any streamed DATA frame this request enqueued
    // during its run, and because those DATA sends happened before this post-`handle.await` send,
    // FIFO on the one channel puts the terminal strictly last (invariant B4).
    let _sender = permit.send(msg);

    registry.remove(id);
}

/// The success terminal for `body`, always built INLINE here. When `oob_threshold` is `Some(t)` and
/// the inline payload (`Outcome::Ok` envelope + `body`) is at least `t` bytes, it is marked `oob`:
/// the writer moves its payload into a sealed memfd just before sending it (`session::oob`), so the
/// engine never holds a memfd for a terminal that is merely queued (review F2).
fn ok_terminal(
    service: u16,
    method: u16,
    id: u32,
    body: &[u8],
    oob_threshold: Option<usize>,
) -> ControlMsg {
    let frame = build_terminal_frame(service, method, id, Outcome::Ok(body.to_vec()));
    let oob = oob_threshold.is_some_and(|threshold| frame.payload.len() >= threshold);
    ControlMsg {
        frame,
        cap: None,
        oob,
    }
}

/// What a terminal frame carries: its `Outcome` inline (every terminal before M3-D3, and every
/// terminal still that is not a large success on a `MEMFD_RX` session), or an [`OobRef`] naming the
/// sealed memfd the `Outcome` was moved into.
pub(crate) enum TerminalPayload {
    Inline(Outcome),
    Oob(OobRef),
}

impl From<Outcome> for TerminalPayload {
    fn from(outcome: Outcome) -> Self {
        TerminalPayload::Inline(outcome)
    }
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
    payload: impl Into<TerminalPayload>,
) -> OutFrame {
    // SPEC §13's error-taxonomy counters (M2-C4b-2a) are recorded HERE because this is the one
    // place a terminal frame is built — `session::error`'s fatal/per-request frames delegate to it
    // too — so every error `END` the daemon sends is counted exactly once, by construction
    // (`metrics::tests::only_one_site_builds_an_end_frame` holds the "one place" claim). An OOB
    // terminal (M3-D3) is built here too, and is always a success.
    let (oob_flag, payload) = match payload.into() {
        TerminalPayload::Inline(outcome) => {
            if let Outcome::Error(ep) = &outcome {
                crate::metrics::ERRORS.record(ep.code, ep.branch);
            }
            (0, outcome.encode())
        }
        TerminalPayload::Oob(oob_ref) => (flags::OOB_FD, oob_ref.encode()),
    };
    OutFrame {
        header: Header {
            flags: flags::END | oob_flag,
            service,
            method,
            request_id,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The threshold is on the INLINE PAYLOAD (envelope + body), inclusive: a body whose inline
    /// payload is exactly `threshold` bytes is marked out of band, one byte shorter is not, and no
    /// threshold at all (a session without `MEMFD_RX`) never is. The frame itself is ALWAYS the
    /// inline terminal — the writer makes the memfd, not the supervisor (review F2).
    #[test]
    fn the_oob_threshold_is_the_inline_payload_size_inclusive() {
        let threshold = 4096;
        let envelope = Outcome::Ok(Vec::new()).encode().len();
        let at = vec![0xc0; threshold - envelope];
        let below = vec![0xc0; threshold - envelope - 1];

        let msg = ok_terminal(2, 1, 9, &at, Some(threshold));
        assert!(msg.oob, "at the threshold: out of band");
        assert_eq!(msg.frame.header.flags, flags::END, "built inline");
        assert_eq!(msg.frame.payload.len(), threshold);
        assert_eq!(msg.frame.payload.to_vec(), Outcome::Ok(at.clone()).encode());

        let msg = ok_terminal(2, 1, 9, &below, Some(threshold));
        assert!(!msg.oob, "one byte below: inline");
        assert_eq!(msg.frame.payload.to_vec(), Outcome::Ok(below).encode());

        let msg = ok_terminal(2, 1, 9, &at, None);
        assert!(!msg.oob, "no MEMFD_RX, never OOB");
    }
}
