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
//! - an **in-flight registry** (`session::registry::Registry`, a
//!   `std::sync::Mutex<HashMap<u32, InFlight>>` under the hood, `InFlight` holding a
//!   `CancellationToken` + a `flow::Credit` — this module's Task 5 addition) keyed by
//!   `request_id`, populated only by request-bearing services (SQL/TX/STREAM) — core
//!   control/liveness frames (`HELLO_ACK`, `PONG`, `WINDOW_UPDATE`-ack, `GOODBYE`) are
//!   non-terminal, never enter the registry, and are not subject to the one-`END` rule.
//!
//! **Request handling is spawn-per-request + supervisor (this module's Task 4 addition).** For
//! every request-bearing frame (`service` is SQL, TX, or STREAM), the reader loop:
//! 1. `registry.insert(id, credit)` — on `Reused`/`Full` it sends a per-request diagnostic error
//!    `END` directly on the control channel (NOT through a `Responder`, NOT via a registry entry
//!    of its own) and does not spawn anything; the original in-flight request (if any) is
//!    untouched.
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
//! that declares `end_error(Unsupported)` for anything (real SQL/TX handlers land in S4/S5).
//! Tests use the seam to script handler behaviour (panic, hang on a `Notify`, declare
//! immediately) without needing a real SQL/TX backend. The handler is also handed a
//! `CancellationToken` (this module's Task 5 addition) alongside its `InFrame`/`Responder` — see
//! below.
//!
//! **Liveness and drain (this module's Task 5 addition).** The reader loop now also routes:
//! - **`CANCEL`** (a flag on ANY frame, `flags::CANCEL`, target = that frame's `request_id`, empty
//!   payload) — checked BEFORE any other dispatch, regardless of the frame's `service`/`method`.
//!   It is advisory and idempotent (SPEC §5.2): `registry.cancel(id)` cancels that id's
//!   `CancellationToken` if it is in-flight, or is a silent no-op if the id is unknown/already
//!   completed. CANCEL itself never produces a reply frame — the in-flight handler observes the
//!   token (`cancel.cancelled().await`, or a race against its own natural completion) and decides
//!   itself whether to call `responder.end_cancelled()`; the supervisor then sends that declared
//!   `Outcome::Cancelled` exactly like any other declared outcome. The supervisor's OWN synthetic
//!   terminal path (panic / no-terminal) is unrelated and still always `Outcome::Error`.
//! - **`core/GOODBYE`** — a graceful-drain announcement. The reader loop simply `break`s: no new
//!   frame (including a new request-bearing one) is read or dispatched after this point. Already
//!   in-flight requests are unaffected — they keep running to completion — because the writer
//!   task and its control channel stay alive until every outstanding supervisor's reserved permit
//!   is consumed (see the concurrency-model paragraph above: "the writer task outlives all
//!   supervisors"). Once the last one finishes, the control channel's last `Sender` drops, the
//!   writer task exits, and `run_with_handler` itself returns — closing the connection. So the
//!   client observes: in-flight terminals still arrive, then EOF.
//! - **`core/WINDOW_UPDATE {request_id, frames, bytes}`** — replenishes that request id's stored
//!   `flow::Credit` via `registry.replenish`. An unknown target id is a silent no-op. No stream
//!   producer consumes credit yet (that arrives with DATA frames in S5), so today this is purely a
//!   plumbing primitive: the credit value is stored and updatable, nothing yet reads it back
//!   except tests.
//!
//! **This module's Task 3 baseline** (still true) lays down the session task itself, the
//! `HELLO`/`HELLO_ACK` handshake (incl. the `TYPE_REGISTRY_HASH` hard check), and the writer
//! task, plus a `PING`→`PONG` stub in the reader loop.
//!
//! **Classification and dispatch (this module's Task 6 addition).** Every decode result from the
//! reader loop is first run through `session::classify::classify` — a PURE function (no
//! awaiting, no I/O; it's what the Task 8 fuzz target drives directly) that applies
//! `ferro_proto::flags::validate` and the session-fatal-vs-per-request split (SPEC's Global
//! Constraints) BEFORE any dispatch happens: a set RESERVED flag (`OOB_FD`/`COMPRESSED`) or a
//! header-level codec fault (bad magic/version, oversize `payload_len`, a truncated header)
//! yields `Classification::Fatal` — one `service=CORE, request_id=0, flags=END,
//! Outcome::Error(ep)` frame, then the connection closes; an unknown non-reserved flag bit yields
//! `Classification::PerRequestErr` — one error `END` on that frame's own `request_id`, and the
//! session survives. Only a `Classification::Frame` ever reaches CANCEL-checking and
//! `dispatch::route`, the `(service, method) -> Route` table: `CoreControl` for
//! `PING`/`GOODBYE`/`WINDOW_UPDATE` (answered as before); `Request` for SQL/TX/STREAM (goes
//! through the registry/handler/supervisor mechanism above, regardless of the specific method id
//! — no method is registered yet, so `default_handler` declares `Unsupported` for all of them);
//! `Unsupported` for anything else (ADMIN, an unrecognized service, or a CORE method this build
//! doesn't recognize) — which, like the reused-id/`max_inflight` diagnostics, sends a per-request
//! `Unsupported` error `END` directly, without ever touching the registry (nothing was spawned
//! for it, so there is no request lifecycle to guard).

pub mod classify;
pub mod codec;
pub mod error;
pub mod flow;
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
use tokio_util::sync::CancellationToken;

use ferro_proto::consts::{errc, flags, method_core, service};
use ferro_proto::flags::has as flag_has;
use ferro_proto::header::Header;
use ferro_proto::messages::{ErrorPayload, Outcome, Ping, WindowUpdate};

use crate::config::Config;
use crate::dispatch::{self, CoreMethod, Route};
use crate::epoch::BootEpoch;
use classify::Classification;
use codec::{FrameCodec, InFrame, OutFrame};
use error::SessionError;
use flow::Credit;
use registry::{InsertErr, Registry};
use responder::Responder;

/// Extra control-channel capacity above `max_inflight`, keeping the "control is effectively
/// never full" invariant even with a handful of liveness/ack/diagnostic frames queued alongside
/// in-flight terminals (the reserved-permit mechanism in `handle_request_frame` is the
/// belt-and-suspenders on top of this headroom, for the terminals specifically).
const CONTROL_CHANNEL_SLACK: usize = 8;

/// A pluggable handler for request-bearing frames (`service` SQL/TX/STREAM): given the decoded
/// `InFrame`, a `Responder` it owns, and a `CancellationToken` it may observe (set by a routed
/// `CANCEL` targeting this request's id — advisory; the handler decides whether/when to honor it
/// by calling `responder.end_cancelled()`), the handler declares exactly one terminal outcome (it
/// may also stream DATA frames in S5, via a channel not yet wired). `Session::run` uses
/// `default_handler` (which ignores the token — an `Unsupported` stub has nothing to cancel);
/// tests and (from Task 6 on) the real dispatch table provide their own.
pub type HandlerFn =
    Arc<dyn Fn(InFrame, Responder, CancellationToken) -> BoxFuture<'static, ()> + Send + Sync>;

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

        // 2. Reader loop. `session::classify` (pure) turns every decode result into a typed
        // `Classification` BEFORE any dispatch happens: `Fatal` sends one `rid=0` error `END`
        // then closes the session; `PerRequestErr` sends one error `END` on that id and the
        // session continues; `NeedMore`/`Closed` are handled directly from `Stream::next()`'s
        // shape (see `classify`'s own doc comment for why `Ok(None)`/`NeedMore` never actually
        // arises on this path — `Framed`'s `Stream` impl already absorbs "wait for more bytes"
        // internally). Only a `Classification::Frame` ever reaches CANCEL-checking and
        // `dispatch::route`, the `(service, method) -> Route` table that decides between core
        // control traffic, the request-bearing registry/handler/supervisor mechanism, and a
        // per-request `Unsupported` for anything else (ADMIN, an unknown service, or a CORE
        // method this build doesn't recognize).
        let registry = Arc::new(Registry::new(config.max_inflight));

        loop {
            let classification = match reader.next().await {
                None => Classification::Closed,
                Some(Ok(frame)) => classify::classify(Ok(Some(frame))),
                Some(Err(err)) => classify::classify(Err(&err)),
            };

            let frame = match classification {
                Classification::NeedMore => continue,
                Classification::Closed => break,
                Classification::Fatal(ep) => {
                    let fatal = SessionError::Fatal(ep).into_out_frame();
                    let _ = control_tx.send(fatal).await;
                    break;
                }
                Classification::PerRequestErr { rid, err } => {
                    let diagnostic = SessionError::PerRequest { rid, err }.into_out_frame();
                    if control_tx.send(diagnostic).await.is_err() {
                        break;
                    }
                    continue;
                }
                Classification::Frame(frame) => frame,
            };

            // CANCEL is flag-based and checked BEFORE any other dispatch, regardless of the
            // frame's service/method: it always targets `header.request_id`, carries an empty
            // payload, and never produces a reply frame of its own (SPEC §5.2).
            if flag_has(frame.header.flags, flags::CANCEL) {
                registry.cancel(frame.header.request_id);
                continue;
            }

            match dispatch::route(frame.header.service, frame.header.method) {
                Route::CoreControl(CoreMethod::Ping) => {
                    if let Ok(ping) = Ping::decode(&frame.payload) {
                        let pong = pong_frame(frame.header.request_id, ping.token);
                        if control_tx.send(pong).await.is_err() {
                            break;
                        }
                    }
                }
                Route::CoreControl(CoreMethod::Goodbye) => {
                    // Drain: stop reading (so no new request-bearing frame is ever dispatched);
                    // already in-flight requests still deliver their one terminal because the
                    // writer/control-channel stay alive until every outstanding supervisor's
                    // reserved permit is consumed (see this module's top doc comment).
                    break;
                }
                Route::CoreControl(CoreMethod::WindowUpdate) => {
                    if let Ok(wu) = WindowUpdate::decode(&frame.payload) {
                        registry.replenish(frame.header.request_id, wu.frames, wu.bytes);
                    }
                }
                Route::Request => {
                    if !handle_request_frame(frame, &registry, &control_tx, &handler, &config).await
                    {
                        break;
                    }
                }
                Route::Unsupported => {
                    // No route in this build: ADMIN, an unknown service, or a CORE method this
                    // build doesn't recognize. Nothing was ever spawned for it, so there is no
                    // registry entry to guard — send the per-request diagnostic directly.
                    let unsupported = SessionError::PerRequest {
                        rid: frame.header.request_id,
                        err: unsupported_error_payload(),
                    }
                    .into_out_frame();
                    if control_tx.send(unsupported).await.is_err() {
                        break;
                    }
                }
            }
        }

        drop(control_tx);
        let _ = writer_handle.await;
    }
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
    config: &Config,
) -> bool {
    let id = frame.header.request_id;
    let credit = Credit::new(config.credit_frames, config.credit_bytes);

    let cancel = match registry.insert(id, credit) {
        Ok(cancel) => cancel,
        Err(err) => {
            let message = match err {
                InsertErr::Reused => "reused in-flight request id",
                InsertErr::Full => "max_inflight exceeded",
            };
            let diagnostic = diagnostic_error_frame(&frame, message);
            return control_tx.send(diagnostic).await.is_ok();
        }
    };

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
    let handle = tokio::spawn(async move { handler(frame, responder, cancel).await });

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
/// `Unsupported` immediately (ignoring the cancel token — there is nothing in-flight to cancel).
/// Real dispatch (SQL/TX handlers) lands in Task 6/S4.
fn default_handler(
    _frame: InFrame,
    responder: Responder,
    _cancel: CancellationToken,
) -> BoxFuture<'static, ()> {
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
