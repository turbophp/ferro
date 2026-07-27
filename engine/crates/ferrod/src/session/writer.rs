//! The writer task: the single point that owns the connection's write half and serializes every
//! outbound frame through it. S3 wires only the **control channel** — HELLO_ACK, PONG, and every
//! terminal/session-fatal frame all flow through it (see `session::mod` for the full model). The
//! loop is deliberately written with `tokio::select!` from the start (rather than a plain
//! `while let Some(f) = control_rx.recv().await`) so S5 can add the credit-limited **data
//! channel**, with control prioritized over data, without restructuring this task.
//!
//! On a send error (the peer went away) or the control channel closing (every `Sender` dropped —
//! the session task is done with this connection), the writer exits. There is nothing further
//! for it to do: the connection is going away either way.

use futures::SinkExt;
use futures::stream::SplitSink;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use super::codec::{FrameCodec, OutFrame};

/// Run the writer loop against `sink` (the write half of a `Framed<_, FrameCodec>`), draining
/// `control_rx` and writing each frame in order.
pub async fn run<W>(
    mut sink: SplitSink<Framed<W, FrameCodec>, OutFrame>,
    mut control_rx: mpsc::Receiver<OutFrame>,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            Some(frame) = control_rx.recv() => {
                if sink.send(frame).await.is_err() {
                    break;
                }
            }
            else => break,
        }
    }
}
