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
//! On a send error (the peer went away) or the control channel closing (every `Sender` dropped —
//! the session task is done with this connection), the writer exits. There is nothing further
//! for it to do: the connection is going away either way.

use futures::SinkExt;
use futures::stream::SplitSink;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use super::codec::{ControlMsg, FrameCodec};

/// Run the writer loop against `sink` (the write half of a `Framed<_, FrameCodec>`), draining
/// `control_rx` and writing each frame in order. After each frame is written, its `ControlMsg`
/// (and any `CapReserve` it carried) is dropped — releasing the per-session cap reservation only
/// once the write has flushed (M6).
pub async fn run<W>(
    mut sink: SplitSink<Framed<W, FrameCodec>, super::codec::OutFrame>,
    mut control_rx: mpsc::Receiver<ControlMsg>,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            Some(msg) = control_rx.recv() => {
                let ControlMsg { frame, cap } = msg;
                let write_result = sink.send(frame).await;
                // Release the cap reservation (if any) AFTER the write flushed, never before —
                // dropping it here, explicitly, makes the M6 release point unmistakable and keeps
                // the reserved bytes an upper bound on the still-buffered bytes.
                drop(cap);
                if write_result.is_err() {
                    break;
                }
            }
            else => break,
        }
    }
}
