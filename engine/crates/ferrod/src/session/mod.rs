//! `ferrod` session concurrency model — decided up front (see the S3 plan's "Decided
//! architecture"; SPEC §21 / charter rule 1: do not re-litigate this in code, comments, or
//! refactors).
//!
//! **One tokio task per accepted connection ("the session task").** It owns:
//! - a **reader loop** over the read half of `Framed<UnixStream, FrameCodec>`;
//! - a long-lived **writer task**, spawned once per connection (`writer::run`), fed by a
//!   **control channel** (`mpsc::Sender<OutFrame>` / `Receiver<OutFrame>`) sized to
//!   `max_inflight + slack` so it is effectively never full. S3 routes everything through this
//!   one channel: `HELLO_ACK`, `PONG`, and — once Task 4 adds request handling — every terminal
//!   `END` and every session-fatal error frame. A second, credit-limited **data channel** for
//!   streamed result frames arrives in S5; `writer::run`'s `tokio::select!` loop is written so
//!   that second receiver can be added as another arm without restructuring it.
//! - (from Task 4 on) an **in-flight registry** keyed by `request_id`, populated only by
//!   request-bearing services (SQL/TX/STREAM) — core control/liveness frames (`HELLO_ACK`,
//!   `PONG`, `WINDOW_UPDATE`-ack, `GOODBYE`) are non-terminal, never enter the registry, and are
//!   not subject to the one-`END` rule.
//!
//! **Request handling is spawn-per-request + supervisor** (Task 4): a request-bearing frame
//! spawns a handler task holding a `Responder`, which calls exactly one of
//! `end_ok`/`end_error`/`end_cancelled` to enqueue its single terminal `Outcome`-encoded `END` on
//! the control channel. The session task's supervisor awaits each handler's `JoinHandle` and, if
//! it panicked or resolved without terminating, synthesizes the terminal itself — so **exactly
//! one `END` per request-bearing request** holds even under handler panic. This is why `ferrod`
//! pins `panic = "unwind"` (see `Cargo.toml`): the supervisor's `JoinError::is_panic()` path
//! depends on panics unwinding rather than aborting the process.
//!
//! **This module (S3 Task 3)** lays down the session task itself, the `HELLO`/`HELLO_ACK`
//! handshake (incl. the `TYPE_REGISTRY_HASH` hard check), and the writer task, plus a bare
//! `PING`→`PONG` stub in the reader loop. Task 4 adds the registry/`Responder`/supervisor; Task
//! 5/6 add `GOODBYE` drain, flag-based `CANCEL`/`WINDOW_UPDATE`, and the full dispatch table —
//! until then the reader loop otherwise ignores anything that isn't `HELLO` or `PING`.

pub mod codec;
pub mod error;
pub mod handshake;
pub mod writer;

use futures::StreamExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use ferro_proto::consts::{method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::Ping;

use crate::config::Config;
use crate::epoch::BootEpoch;
use codec::{FrameCodec, OutFrame};
use error::SessionError;

/// Extra control-channel capacity above `max_inflight`, keeping the "control is effectively
/// never full" invariant even with a handful of liveness/ack frames queued alongside in-flight
/// terminals (Task 4's reserved-permit mechanism is the belt-and-suspenders on top of this
/// headroom).
const CONTROL_CHANNEL_SLACK: usize = 8;

/// The session task's entry point, one call per accepted connection.
pub struct Session;

impl Session {
    /// Drive one accepted connection end to end: split the framed stream, spawn the writer task,
    /// perform the `HELLO`/`HELLO_ACK` handshake, then answer liveness (`PING`) until the peer
    /// disconnects.
    ///
    /// `epoch` is the daemon's single boot-time draw, passed in (not redrawn per connection) so
    /// every connection served by this running instance observes the identical `boot_epoch`
    /// (SPEC §19.1) — the caller (`main`, or a test harness) is responsible for drawing it once
    /// via an `EpochSource` and handing the same `BootEpoch` to every `Session::run` call.
    pub async fn run(stream: UnixStream, config: Config, epoch: BootEpoch) {
        let framed = Framed::new(stream, FrameCodec);
        let (sink, mut reader) = framed.split();

        let (control_tx, control_rx) =
            mpsc::channel::<OutFrame>(config.max_inflight + CONTROL_CHANNEL_SLACK);
        let writer_handle = tokio::spawn(writer::run(sink, control_rx));

        // 1. The mandatory first frame must be core/HELLO; anything else is session-fatal.
        let first = match reader.next().await {
            Some(Ok(frame)) => frame,
            Some(Err(_)) | None => {
                // Never got a decodable first frame at all — nothing to reply to; just let the
                // writer task see the control channel close and exit.
                drop(control_tx);
                let _ = writer_handle.await;
                return;
            }
        };

        if !handshake::is_hello(&first) {
            let fatal =
                SessionError::protocol_fatal("first frame was not core/HELLO").into_out_frame();
            let _ = control_tx.send(fatal).await;
            drop(control_tx);
            let _ = writer_handle.await;
            return;
        }

        let _hello = match handshake::validate_hello(&first) {
            Ok(hello) => hello,
            Err(err) => {
                let _ = control_tx.send(err.into_out_frame()).await;
                drop(control_tx);
                let _ = writer_handle.await;
                return;
            }
        };

        let ack = handshake::hello_ack_frame(first.header.request_id, epoch);
        if control_tx.send(ack).await.is_err() {
            // Writer already gone; nothing left to do.
            drop(control_tx);
            let _ = writer_handle.await;
            return;
        }

        // 2. Reader loop. This task answers PING with PONG; everything else is a stub — real
        // dispatch (registry, CANCEL, WINDOW_UPDATE, GOODBYE, SQL/TX routing) lands in Tasks 4-6.
        while let Some(next) = reader.next().await {
            let frame = match next {
                Ok(frame) => frame,
                // A decode error mid-session is a protocol fault; the full fatal/per-request
                // classification (Task 6) isn't wired yet, so for now just stop reading and let
                // the connection close.
                Err(_) => break,
            };

            if frame.header.service == service::CORE && frame.header.method == method_core::PING {
                if let Ok(ping) = Ping::decode(&frame.payload) {
                    let pong = pong_frame(frame.header.request_id, ping.token);
                    if control_tx.send(pong).await.is_err() {
                        break;
                    }
                }
                continue;
            }

            // Stub: every other frame is ignored until the real dispatch table (Task 6).
        }

        drop(control_tx);
        let _ = writer_handle.await;
    }
}

fn pong_frame(request_id: u32, token: u64) -> OutFrame {
    let payload = ferro_proto::messages::Pong { token }.encode();
    OutFrame {
        header: Header {
            flags: 0,
            service: service::CORE,
            method: method_core::PONG,
            request_id,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}
