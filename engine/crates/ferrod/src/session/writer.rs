//! The writer task: the single point that owns the connection's write half and serializes every
//! outbound frame through it. Everything the connection ever writes — HELLO_ACK, PONG, every
//! terminal/session-fatal frame, AND (from M1-S5) every streamed HEAD/DATA frame — flows through
//! ONE ordered channel of [`ControlMsg`] (see `session::mod` for the full model).
//!
//! **Single ordered conduit (M1-S5 decision, SPEC §22).** DATA and the terminal share this one
//! `control_rx`, so their FIFO order is the channel's send order: a streamed request enqueues its
//! DATA frames DURING the handler run and its terminal only AFTER (via the supervisor's reserved
//! permit), so the terminal can never overtake a DATA frame (invariant B4). The earlier design
//! sketched a SECOND, credit-limited data channel with control prioritized over data; that
//! priority-split is DEFERRED (charter rule 5 — no speculative throughput work before the gate),
//! and the `tokio::select!` loop shape is kept so it can be reintroduced later without a rewrite.
//!
//! **Cap release point (M6).** Each `ControlMsg` may carry a `CapReserve` guard for the per-session
//! byte cap. The writer writes the frame, THEN drops the message — so the reservation is released
//! only after the write has flushed, keeping the reserved bytes an upper bound on the bytes actually
//! buffered toward the socket. The release is the message drop; there is no explicit `release` call.
//!
//! **Out-of-band terminals (M3-D3).** A message marked `oob` is a large success terminal the client
//! can receive by memfd (`session::oob`). Only HERE, immediately before sending, is the memfd made:
//! the writer flushes every frame before it, copies the frame's payload into a sealed memfd, sends
//! an `END | OOB_FD` frame with `sendmsg` so the fd is attached to the frame's FIRST byte — the
//! association rule the receiver keys on — writes any remainder plainly, and closes its copy of the
//! fd. So the engine holds at most one OOB memfd per session, for the duration of one send (review
//! F2). If the memfd cannot be made, or the kernel refuses the fd itself (`ETOOMANYREFS`: nothing
//! was sent), the writer sends the frame it was given — the same terminal, inline.
//!
//! On a send error (the peer went away) or the control channel closing (every `Sender` dropped —
//! the session task is done with this connection), the writer exits. There is nothing further
//! for it to do: the connection is going away either way.

use std::os::fd::AsFd;

use futures::SinkExt;
use tokio::io::AsyncWriteExt;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio_util::codec::FramedWrite;

use super::codec::{ControlMsg, FrameCodec, OutFrame};
use super::oob;

/// Run the writer loop against `sink` (the session socket's write half), draining `control_rx`
/// and writing each frame in order. After each frame is written, its `ControlMsg` (and any
/// `CapReserve` it carried) is dropped — releasing the per-session cap reservation only once the
/// write has flushed (M6).
pub async fn run(
    mut sink: FramedWrite<OwnedWriteHalf, FrameCodec>,
    mut control_rx: mpsc::Receiver<ControlMsg>,
) {
    loop {
        tokio::select! {
            Some(msg) = control_rx.recv() => {
                let ControlMsg { frame, cap, oob } = msg;
                let write_result = if oob {
                    send_oob(&mut sink, frame).await
                } else {
                    sink.send(frame).await.is_ok()
                };
                // Release the cap reservation (if any) AFTER the write flushed, never before —
                // dropping it here, explicitly, makes the M6 release point unmistakable and keeps
                // the reserved bytes an upper bound on the still-buffered bytes.
                drop(cap);
                if !write_result {
                    break;
                }
            }
            else => break,
        }
    }
}

/// Send `frame` — a success terminal marked `oob` — out of band: its payload in a sealed memfd made
/// now, the frame on the wire an `END | OOB_FD` carrying the `OobRef`. Falls back to `frame` itself
/// when the memfd cannot be made or the kernel refuses the fd. `false` means the connection is
/// unusable, exactly as a failed inline write does.
async fn send_oob(sink: &mut FramedWrite<OwnedWriteHalf, FrameCodec>, frame: OutFrame) -> bool {
    // Every earlier frame must be fully on the socket before the fd-bearing `sendmsg`, or the fd
    // would be attached to bytes that are not this frame's first.
    if sink.flush().await.is_err() {
        return false;
    }
    let fd = match oob::seal_payload(&frame.payload) {
        Ok(fd) => fd,
        Err(e) => {
            oob::COUNTERS.record_fallback();
            tracing::warn!(error = %e, "could not build an OOB memfd; sending the terminal inline");
            return sink.send(frame).await.is_ok();
        }
    };
    let len = frame.payload.len() as u64;
    let oob_frame = super::supervisor::build_terminal_frame(
        frame.header.service,
        frame.header.method,
        frame.header.request_id,
        super::supervisor::TerminalPayload::Oob(oob::oob_ref(len)),
    );
    let mut bytes = Vec::with_capacity(16 + oob_frame.payload.len());
    bytes.extend_from_slice(&oob_frame.header.encode());
    bytes.extend_from_slice(&oob_frame.payload);
    let sent = oob::send_with_fd(sink.get_ref().as_ref(), &bytes, fd.as_fd()).await;
    // Sent or not, the engine's copy closes now: on success the kernel holds the in-flight
    // reference for the receiver; on a refusal the payload is still in `frame`.
    drop(fd);
    match sent {
        Ok(n) => {
            oob::COUNTERS.record_sent(len);
            sink.get_mut().write_all(&bytes[n..]).await.is_ok()
        }
        Err(e) if oob::is_fd_refusal(&e) => {
            oob::COUNTERS.record_fallback();
            tracing::warn!(error = %e, "the kernel refused an OOB fd; sending that terminal inline");
            sink.send(frame).await.is_ok()
        }
        Err(_) => false,
    }
}
