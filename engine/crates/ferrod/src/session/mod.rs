//! `ferrod` session concurrency model — decided up front (see the S3 plan's "Decided
//! architecture"; SPEC §21 / charter rule 1: do not re-litigate this in code, comments, or
//! refactors).
//!
//! **One tokio task per accepted connection ("the session task").** It owns:
//! - a **reader loop** over the read half of `Framed<UnixStream, FrameCodec>`;
//! - a long-lived **writer task**, spawned once per connection (`writer::run`), fed by a
//!   **control channel** (`mpsc::Sender<OutFrame>` / `Receiver<OutFrame>`) sized to
//!   `max_inflight + slack` so it is effectively never full. Every `HELLO_ACK`, `PONG`, every
//!   terminal `END`, and every session-fatal error frame flows through this one channel. A
//!   second, credit-limited **data channel** for streamed result frames arrives in S5;
//!   `writer::run`'s `tokio::select!` loop is written so that second receiver can be added as
//!   another arm without restructuring it.
//! - an **in-flight registry** (`session::registry::Registry`, a `std::sync::Mutex<HashSet<u32>>`
//!   under the hood) keyed by `request_id`, populated only by request-bearing services (SQL/TX/
//!   STREAM) — core control/liveness frames (`HELLO_ACK`, `PONG`, `WINDOW_UPDATE`-ack, `GOODBYE`)
//!   are non-terminal, never enter the registry, and are not subject to the one-`END` rule.
//!
//! **Request handling is spawn-per-request + supervisor (this module's Task 4 addition).** For
//! every request-bearing frame (`service` is SQL, TX, or STREAM), the reader loop:
//! 1. `registry.insert(id)` — on `Reused`/`Full` it sends a per-request diagnostic error `END`
//!    directly on the control channel (NOT through a `Responder`, NOT via a registry entry of its
//!    own) and does not spawn anything; the original in-flight request (if any) is untouched.
//! 2. Reserves a control-channel permit (`control_tx.clone().reserve_owned().await`) — this
//!    guarantees a delivery slot for the eventual terminal regardless of whatever else is queued
//!    on the control channel, BEFORE the handler ever runs.
//! 3. Builds a `Responder`/`cell` pair (`responder::Responder::new_pair`), spawns the handler
//!    task (`tokio::spawn(handler(frame, responder))`), and spawns a **supervisor task**
//!    (`supervisor::supervise`) that awaits the handler's `JoinHandle` independently of the
//!    reader loop — so the reader loop is never blocked by an in-flight handler and keeps
//!    answering other traffic (PING, further requests, more diagnostics) concurrently.
//!
//! The supervisor is the **sole terminal-sender**: the handler never sends its own terminal, it
//! only *declares* an outcome via the consuming `Responder` (`end_ok`/`end_error`/
//! `end_cancelled`, each taking `self` by value — a second call is a compile error). When the
//! handler's task resolves, the supervisor reads the declared `Terminal` back exactly once and
//! sends it; if the task panicked (`JoinError::is_panic()`) or resolved without ever declaring,
//! the supervisor synthesizes a distinctly-marked `Outcome::Error` instead. Either way, **exactly
//! one `END` per request-bearing request** holds — even under handler panic, early return, or a
//! (compile-time-impossible) double declaration. This is why `ferrod` pins `panic = "unwind"`
//! (see `Cargo.toml`): the supervisor's `JoinError::is_panic()` path depends on panics unwinding
//! rather than aborting the process.
//!
//! **Dispatch is an injectable seam.** `Session::run_with_handler` takes a `HandlerFn` used for
//! every request-bearing frame; `Session::run` is `run_with_handler` with a `default_handler`
//! that declares `end_error(Unsupported)` for anything (real dispatch lands in Task 6). Tests use
//! the seam to script handler behaviour (panic, hang on a `Notify`, declare immediately) without
//! needing a real SQL/TX backend.
//!
//! **This module's Task 3 baseline** (still true) lays down the session task itself, the
//! `HELLO`/`HELLO_ACK` handshake (incl. the `TYPE_REGISTRY_HASH` hard check), and the writer
//! task, plus a `PING`→`PONG` stub in the reader loop. Task 5/6 add `GOODBYE` drain, flag-based
//! `CANCEL`/`WINDOW_UPDATE`, and the full dispatch table — until then any non-CORE, non-request-
//! bearing frame (there are none yet, since SQL/TX/STREAM already cover all request-bearing
//! services) and any CORE frame that isn't `HELLO`/`PING` is simply ignored.

pub mod codec;
pub mod error;
pub mod handshake;
pub mod registry;
pub mod responder;
pub mod supervisor;
pub mod writer;

use std::sync::Arc;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use ferro_proto::consts::{errc, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{ErrorPayload, Outcome, Ping};

use crate::config::Config;
use crate::epoch::BootEpoch;
use codec::{FrameCodec, InFrame, OutFrame};
use error::SessionError;
use registry::{InsertErr, Registry};
use responder::Responder;

/// Extra control-channel capacity above `max_inflight`, keeping the "control is effectively
/// never full" invariant even with a handful of liveness/ack/diagnostic frames queued alongside
/// in-flight terminals (the reserved-permit mechanism in `handle_request_frame` is the
/// belt-and-suspenders on top of this headroom, for the terminals specifically).
const CONTROL_CHANNEL_SLACK: usize = 8;

/// A pluggable handler for request-bearing frames (`service` SQL/TX/STREAM): given the decoded
/// `InFrame` and a `Responder` it owns, the handler declares exactly one terminal outcome (it may
/// also stream DATA frames in S5, via a channel not yet wired). `Session::run` uses
/// `default_handler`; tests and (from Task 6 on) the real dispatch table provide their own.
pub type HandlerFn = Arc<dyn Fn(InFrame, Responder) -> BoxFuture<'static, ()> + Send + Sync>;

/// The session task's entry point, one call per accepted connection.
pub struct Session;

impl Session {
    /// Drive one accepted connection end to end using the default handler (declares
    /// `Unsupported` for every request-bearing frame — real dispatch lands in Task 6).
    pub async fn run(stream: UnixStream, config: Config, epoch: BootEpoch) {
        let handler: HandlerFn = Arc::new(default_handler);
        Self::run_with_handler(stream, config, epoch, handler).await;
    }

    /// Drive one accepted connection end to end: split the framed stream, spawn the writer task,
    /// perform the `HELLO`/`HELLO_ACK` handshake, then answer liveness (`PING`) and route
    /// request-bearing frames (SQL/TX/STREAM) through `registry` + a spawned handler +
    /// supervisor, using `handler` for every such frame, until the peer disconnects.
    ///
    /// `epoch` is the daemon's single boot-time draw, passed in (not redrawn per connection) so
    /// every connection served by this running instance observes the identical `boot_epoch`
    /// (SPEC §19.1) — the caller (`main`, or a test harness) is responsible for drawing it once
    /// via an `EpochSource` and handing the same `BootEpoch` to every `Session::run*` call.
    pub async fn run_with_handler(
        stream: UnixStream,
        config: Config,
        epoch: BootEpoch,
        handler: HandlerFn,
    ) {
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

        // 2. Reader loop. Answers PING with PONG; routes request-bearing frames (SQL/TX/STREAM)
        // through the registry + spawned-handler + supervisor mechanism; everything else is a
        // stub until Tasks 5/6 (GOODBYE, CANCEL, WINDOW_UPDATE, full dispatch classification).
        let registry = Arc::new(Registry::new(config.max_inflight));

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

            if is_request_bearing(frame.header.service) {
                if !handle_request_frame(frame, &registry, &control_tx, &handler).await {
                    break;
                }
                continue;
            }

            // Stub: every other frame (CORE non-PING, ADMIN, unknown services) is ignored until
            // the real dispatch/classification table (Task 6).
        }

        drop(control_tx);
        let _ = writer_handle.await;
    }
}

/// Whether `service` is one of the request-bearing services (SQL/TX/STREAM) that go through the
/// in-flight registry + spawned-handler + supervisor mechanism. CORE (control/liveness) and
/// ADMIN are handled elsewhere (or, for now, ignored — see the reader loop's stub comment).
fn is_request_bearing(svc: u16) -> bool {
    svc == service::SQL || svc == service::TX || svc == service::STREAM
}

/// Route one request-bearing frame: insert into the registry (sending a per-request diagnostic
/// and returning early on `Reused`/`Full`), reserve a control-channel permit, then spawn the
/// handler task and a supervisor task to await it. Returns `false` if the control channel is
/// gone (writer task exited) and the reader loop should stop.
async fn handle_request_frame(
    frame: InFrame,
    registry: &Arc<Registry>,
    control_tx: &mpsc::Sender<OutFrame>,
    handler: &HandlerFn,
) -> bool {
    let id = frame.header.request_id;

    if let Err(err) = registry.insert(id) {
        let message = match err {
            InsertErr::Reused => "reused in-flight request id",
            InsertErr::Full => "max_inflight exceeded",
        };
        let diagnostic = diagnostic_error_frame(&frame, message);
        return control_tx.send(diagnostic).await.is_ok();
    }

    // Reserve the terminal's delivery slot BEFORE the handler ever runs, so the supervisor's
    // eventual send cannot fail for lack of channel capacity no matter what else happens.
    let permit = match control_tx.clone().reserve_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            registry.remove(id);
            return false;
        }
    };

    let (responder, cell) = Responder::new_pair();
    let service = frame.header.service;
    let method = frame.header.method;
    let handler = handler.clone();
    let handle = tokio::spawn(async move { handler(frame, responder).await });

    tokio::spawn(supervisor::supervise(
        id,
        service,
        method,
        permit,
        cell,
        handle,
        registry.clone(),
    ));

    true
}

/// Build a per-request diagnostic error `END` frame (reused id / max_inflight exceeded) directly
/// — no `Responder`, no registry entry of its own; the frame this diagnoses was never inserted
/// (or was already occupying the id), so there is nothing for a supervisor to await here.
fn diagnostic_error_frame(frame: &InFrame, message: &str) -> OutFrame {
    let ep = ErrorPayload {
        code: errc::PROTOCOL,
        branch: errc::PROTOCOL_BRANCH,
        sqlstate: None,
        errno: None,
        message: message.to_string(),
        detail: None,
        retry_after_ms: None,
    };
    supervisor::build_terminal_frame(
        frame.header.service,
        frame.header.method,
        frame.header.request_id,
        Outcome::Error(ep),
    )
}

/// The default handler used by `Session::run`: every request-bearing frame declares
/// `Unsupported` immediately. Real dispatch (SQL/TX handlers) lands in Task 6/S4.
fn default_handler(_frame: InFrame, responder: Responder) -> BoxFuture<'static, ()> {
    async move {
        responder.end_error(unsupported_error_payload());
    }
    .boxed()
}

fn unsupported_error_payload() -> ErrorPayload {
    ErrorPayload {
        code: errc::UNSUPPORTED,
        branch: errc::UNSUPPORTED_BRANCH,
        sqlstate: None,
        errno: None,
        message: "service/method not yet implemented".to_string(),
        detail: None,
        retry_after_ms: None,
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
