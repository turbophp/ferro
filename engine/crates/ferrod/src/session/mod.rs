//! `ferrod` session concurrency model — decided up front (see the S3 plan's "Decided
//! architecture"; SPEC §21 / charter rule 1: do not re-litigate this in code, comments, or
//! refactors).
//!
//! **One tokio task per accepted connection ("the session task").** It owns:
//! - a **reader loop** over the read half of `Framed<UnixStream, FrameCodec>`;
//! - a long-lived **writer task**, spawned once per connection (`writer::run`), fed by a
//!   **control channel** (`mpsc::Sender<ControlMsg>` / `Receiver<ControlMsg>`) sized to
//!   `max_inflight + slack` so it is effectively never full. Every `HELLO_ACK`, `PONG`, every
//!   terminal `END`, every session-fatal error frame — AND (from M1-S5) every streamed HEAD/DATA
//!   frame — flows through this ONE ordered channel. That single-conduit choice is what makes a
//!   streamed request's terminal never overtake its DATA (invariant B4): DATA is enqueued during
//!   the handler run, the terminal only after (via the supervisor's reserved permit), so FIFO on
//!   the one channel puts the terminal last. A `ControlMsg` is an `OutFrame` plus an optional
//!   `CapReserve` guard (M6) the writer drops after the write; non-streamed sends carry `None`.
//!   The earlier design sketched a SECOND, credit-limited data channel with control prioritized
//!   over data — that priority-split is DEFERRED (charter rule 5, SPEC §22); `writer::run`'s
//!   `tokio::select!` loop shape is kept so it can be reintroduced later without a rewrite.
//! - an **in-flight registry** (`session::registry::Registry`, a
//!   `std::sync::Mutex<HashMap<u32, InFlight>>` under the hood, `InFlight` holding a
//!   `CancellationToken` + a `flow::Credit` — this module's Task 5 addition) keyed by
//!   `request_id`, populated only by request-bearing services (SQL/TX/STREAM) — core
//!   control/liveness frames (`HELLO_ACK`, `PONG`, `GOODBYE`, `WINDOW_UPDATE`) are non-terminal,
//!   never enter the registry, and are not subject to the one-`END` rule. `WINDOW_UPDATE` and
//!   `GOODBYE` in particular are inbound-only control frames — the reader loop applies/acts on
//!   them and never sends a reply of any kind (there is no `WINDOW_UPDATE_ACK` method).
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
//! 3. Builds a stream-capable `Responder`/`cell` pair (`responder::Responder::new_streaming`,
//!    carrying this request's credit window, the per-session cap, a `control_tx` clone, and the
//!    request id so a `fetch:stream` handler can `send_head`/`send_data` — M1-S5), spawns the
//!    handler task (`tokio::spawn(handler(frame, responder))`), and spawns a **supervisor task**
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
//! **Dispatch is an injectable seam.** `Session::run_with_handler` takes a `HandlerFactory` (S6:
//! `Fn(SessionId) -> HandlerFn`) plus the shared `Arc<TxRegistry>`; it draws a `SessionId` once,
//! builds this connection's `HandlerFn` via `factory(session_id)`, and uses that for every
//! request-bearing frame. `Session::run` is `run_with_handler` with a throwaway registry + a
//! trivial factory whose `default_handler` declares `end_error(Unsupported)` for anything (real
//! SQL/TX handlers land in S5/S6). Tests use the seam to script handler behaviour (panic, hang on
//! a `Notify`, declare immediately) without needing a real SQL/TX backend. The handler is also
//! handed a `CancellationToken` (this module's Task 5 addition) alongside its
//! `InFrame`/`Responder` — see below.
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
//!
//! **Handshake hardening + per-session task ownership (S3 fix pass).** Three additions on top of
//! the above, none of them weakening exactly-one-`END`:
//! - The mandatory first frame is read under `tokio::time::timeout(config.handshake_timeout,
//!   ..)`: a peercred-passing peer that connects and then never sends anything no longer pins an
//!   fd + tasks forever (slowloris/fd-exhaustion) — past the deadline, the connection is dropped
//!   silently (there was never a valid HELLO to fail). Whatever the first read's outcome, it is
//!   routed through the SAME `classify::classify` split the reader loop uses for every later
//!   frame, so a header-level codec fault (bad magic/version, oversize length, a truncated header)
//!   produces the same `rid=0` fatal `Outcome::Error(PROTOCOL)` frame here too — never a silent
//!   close — and a reserved flag (`OOB_FD`/`COMPRESSED`) set on the HELLO frame itself is likewise
//!   caught for free (`flags::validate` runs inside `classify`).
//! - `request_id == 0` is reserved for session-context terminals (session-fatal / GOODBYE /
//!   no-request-context) — a request-bearing (SQL/TX/STREAM) frame claiming it is rejected by
//!   `handle_request_frame` directly, the same way a reused id or an over-quota one is: a
//!   per-request diagnostic sent straight on the control channel, the registry never touched.
//! - Every per-request supervisor task is spawned into a **per-session `JoinSet`** (`supervisors`,
//!   local to `run_with_handler`) instead of being detached — reaped opportunistically during the
//!   reader loop itself (a guarded `supervisors.join_next()` arm, mirroring `serve`'s own
//!   accept-loop reap, so this JoinSet never accumulates one dead entry per historical request for
//!   the connection's whole lifetime) and, once the reader loop ends for any reason, drained with
//!   a bound (`drain_supervisors`, `config.drain_deadline`) after `registry.cancel_all()` nudges
//!   still-running handlers to wrap up. This closes the "writer exits early on a `sink.send()`
//!   error, detached handler/supervisor tasks are orphaned" hole: no per-request task outlives its
//!   session. The supervisor remains the sole terminal-sender throughout — this only changes who
//!   *owns* (and, past a bound, hard-aborts) the task that awaits it.

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
use std::time::Duration;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use ferro_proto::consts::{errc, flags, method_core, service};
use ferro_proto::flags::has as flag_has;
use ferro_proto::header::Header;
use ferro_proto::messages::{ErrorPayload, Outcome, Ping, WindowUpdate};

use crate::config::Config;
use crate::dispatch::{self, CoreMethod, Route};
use crate::epoch::BootEpoch;
use crate::pools::PoolRegistry;
use crate::tx::TxRegistry;
use classify::Classification;
use codec::{ControlMsg, FrameCodec, InFrame, OutFrame};
use error::SessionError;
use flow::{Credit, SessionCap};
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
/// by calling `responder.end_cancelled()`), the handler declares exactly one terminal outcome (a
/// streamed `fetch` handler may also emit HEAD/DATA frames first, via `Responder::send_head`/
/// `send_data`, which enqueue on the same ordered control channel — M1-S5). `Session::run` uses
/// `default_handler` (which ignores the token — an `Unsupported` stub has nothing to cancel);
/// tests and (from Task 6 on) the real dispatch table provide their own.
pub type HandlerFn =
    Arc<dyn Fn(InFrame, Responder, CancellationToken) -> BoxFuture<'static, ()> + Send + Sync>;

/// A process-monotonic id for one accepted connection's session, drawn once per connection from
/// [`TxRegistry::next_session_id`] (S6). It is the OWNER key a transaction is registered under, so
/// a tx-scoped request from a different session is indistinguishable from an unknown `tx_id` (see
/// `crate::tx`). Distinct from the wire `request_id` (per-request, client-chosen).
pub type SessionId = u64;

/// Builds the per-connection [`HandlerFn`] given that connection's [`SessionId`] (S6 seam). We
/// inject a FACTORY rather than a bare `HandlerFn` so a handler can capture its own session id (the
/// tx service keys tx ownership off it) without threading a `SessionId` through `HandlerFn`'s own
/// signature — which would churn the ~8 scripted-handler test closures and the mod.rs call site.
/// `Session::run` and the pure-session tests use a trivial factory that ignores the id.
pub type HandlerFactory = Arc<dyn Fn(SessionId) -> HandlerFn + Send + Sync>;

/// The session task's entry point, one call per accepted connection.
pub struct Session;

impl Session {
    /// Drive one accepted connection end to end using the default handler (declares
    /// `Unsupported` for every request-bearing frame — real dispatch lands in Task 6).
    ///
    /// A standalone session with no tx service of its own: mint a throwaway [`TxRegistry`] + a
    /// trivial factory whose default handler creates no transactions (so the `abort_session` at
    /// cleanup is a guaranteed no-op). This keeps every pure-session test path (`common::spawn` /
    /// `Session::run`) untouched by the S6 seam.
    pub async fn run(stream: UnixStream, config: Config, epoch: BootEpoch) {
        let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
        // Its own pool registry, exactly as it already mints its own throwaway `TxRegistry`
        // (M1-S8a Task 12). The `Config`s used on this path carry no pools, so this builds an EMPTY
        // registry and dials nothing — and if one ever does carry pools, `Pool::new` is lazy, so it
        // still dials nothing until a checkout asks.
        let pool_registry = PoolRegistry::build(&config);
        let factory: HandlerFactory = Arc::new(|_session_id| default_handler_fn());
        Self::run_with_handler(stream, config, epoch, pool_registry, tx_registry, factory).await;
    }

    /// Drive one accepted connection end to end: split the framed stream, spawn the writer task,
    /// perform the `HELLO`/`HELLO_ACK` handshake, then answer liveness (`PING`) and route
    /// request-bearing frames (SQL/TX/STREAM) through `registry` + a spawned handler +
    /// supervisor, until the peer disconnects.
    ///
    /// `tx_registry` is the shared, process-global transaction registry (S6 seam): a `SessionId` is
    /// drawn from it once here, the per-connection handler is built via `factory(session_id)`, and
    /// on every session-end route the owned transactions are aborted (see the cleanup path below).
    ///
    /// `pool_registry` is the same shared, process-global [`PoolRegistry`] the handler resolves
    /// pools out of (M1-S8a Task 12). The session needs it for ONE thing: the `HELLO_ACK` pool
    /// metadata, whose `server_version` is learned lazily off the registry's per-pool cache. It is
    /// deliberately the SAME registry the SQL handler uses, so a version probe checks out of the
    /// very pool a subsequent statement will run on.
    ///
    /// `epoch` is the daemon's single boot-time draw, passed in (not redrawn per connection) so
    /// every connection served by this running instance observes the identical `boot_epoch`
    /// (SPEC §19.1) — the caller (`main`, or a test harness) is responsible for drawing it once
    /// via an `EpochSource` and handing the same `BootEpoch` to every `Session::run*` call.
    pub async fn run_with_handler(
        stream: UnixStream,
        config: Config,
        epoch: BootEpoch,
        pool_registry: Arc<PoolRegistry>,
        tx_registry: Arc<TxRegistry>,
        factory: HandlerFactory,
    ) {
        // Draw this connection's SessionId once (S6 seam) and build its handler. `session_id` is
        // used both to key tx ownership (inside the handler) and to abort this session's
        // transactions at cleanup. No transaction can exist before the reader loop below (the
        // handler only runs for request-bearing frames, and none are dispatched during the
        // handshake), so the handshake-phase early returns need not — and do not — abort.
        let session_id = tx_registry.next_session_id();
        let handler = factory(session_id);

        let framed = Framed::new(stream, FrameCodec);
        let (sink, mut reader) = framed.split();

        let (control_tx, control_rx) =
            mpsc::channel::<ControlMsg>(config.max_inflight + CONTROL_CHANNEL_SLACK);
        let writer_handle = tokio::spawn(writer::run(sink, control_rx));

        // One per-session aggregate byte cap, shared (as an `Arc`) with every streamed request's
        // `Responder` (M6). Its first construction lands here — `SessionCap`/`reserve_or_wait`/
        // `CapReserve` were built in Task 2 with no instantiation site until now. Non-streamed
        // requests carry the same handle but never reserve against it.
        let session_cap = Arc::new(SessionCap::new(config.session_cap_bytes as u64));

        // 1. The mandatory first frame must arrive within `config.handshake_timeout` and decode as
        // core/HELLO. Whatever the read's outcome, route it through the SAME `classify` split the
        // reader loop below uses for every later frame — so a header-level codec fault (bad
        // magic/version, oversize length, a truncated header) yields the same rid=0 fatal
        // `Outcome::Error(PROTOCOL)` frame here too, not a silent close; only a genuine I/O
        // error/clean EOF (`Classification::Closed`) or a handshake timeout stay silent (there was
        // never a valid HELLO in hand to reply about). This also runs `flags::validate` against
        // the HELLO frame itself for free: a reserved flag (`OOB_FD`/`COMPRESSED`) set on it is
        // `Classification::Fatal`, exactly like on any later frame.
        let first_classification =
            match tokio::time::timeout(config.handshake_timeout, reader.next()).await {
                Err(_elapsed) => {
                    // No HELLO within the handshake deadline: drop silently, same shape as a clean
                    // EOF — nothing was ever agreed upon, so there is nothing to reply to.
                    drop(control_tx);
                    let _ = writer_handle.await;
                    return;
                }
                Ok(None) => Classification::Closed,
                Ok(Some(Ok(frame))) => classify::classify(Ok(Some(frame))),
                Ok(Some(Err(err))) => classify::classify(Err(&err)),
            };

        let first = match first_classification {
            // `Framed`'s `Stream` impl never actually surfaces `Ok(None)` as an item on this path
            // (see `classify`'s own doc comment) — handled defensively so this match stays total.
            Classification::NeedMore | Classification::Closed => {
                // Never got a decodable first frame at all — nothing to reply to; just let the
                // writer task see the control channel close and exit.
                drop(control_tx);
                let _ = writer_handle.await;
                return;
            }
            Classification::Fatal(ep) => {
                // A header-level codec fault, OR a reserved flag set on the first frame: the SAME
                // rid=0 session-fatal `END` the reader loop sends for any later frame — not a
                // silent close.
                let fatal = SessionError::Fatal(ep).into_out_frame();
                let _ = control_tx.send(ControlMsg::bare(fatal)).await;
                drop(control_tx);
                let _ = writer_handle.await;
                return;
            }
            Classification::PerRequestErr { rid, err } => {
                // An otherwise-decodable first frame with an unknown, non-reserved flag bit set.
                // There is no valid HELLO in hand and thus no session to keep alive past this
                // point — send the diagnostic on the frame's own id, then close.
                let diagnostic = SessionError::PerRequest { rid, err }.into_out_frame();
                let _ = control_tx.send(ControlMsg::bare(diagnostic)).await;
                drop(control_tx);
                let _ = writer_handle.await;
                return;
            }
            Classification::Frame(frame) => frame,
        };

        if !handshake::is_hello(&first) {
            let fatal =
                SessionError::protocol_fatal("first frame was not core/HELLO").into_out_frame();
            let _ = control_tx.send(ControlMsg::bare(fatal)).await;
            drop(control_tx);
            let _ = writer_handle.await;
            return;
        }

        let _hello = match handshake::validate_hello(&first) {
            Ok(hello) => hello,
            Err(err) => {
                let _ = control_tx
                    .send(ControlMsg::bare(err.into_out_frame()))
                    .await;
                drop(control_tx);
                let _ = writer_handle.await;
                return;
            }
        };

        // The per-pool metadata, with `server_version` learned lazily per pool, CONCURRENTLY, and
        // bounded AS A WHOLE by `PoolRegistry::VERSION_PROBE_BUDGET` — never fatal: a pool whose
        // backend is unreachable (or merely slow) advertises `server_version: nil` and the
        // handshake still completes. The budget is deliberately well under the client's default
        // 5 s `ioTimeout`, which covers the HELLO_ACK read (`Ferro.php`/`Transport.php`).
        //
        // The registry is the SINGLE source of the advertised pool list — deliberately not "the
        // registry, or `config.pools` when the registry is empty". Every `PoolRegistry` in the tree
        // is built FROM a `Config`, so a pool-bearing config never yields a pool-less registry and
        // such a fallback branch could not fire; and two derivations of one wire field are exactly
        // how the two drift. `Session::run`'s pool-less path builds its own (empty) registry and so
        // advertises no pools, which is what it has.
        let ack = handshake::hello_ack_frame(
            first.header.request_id,
            epoch,
            pool_registry.pool_info().await,
        );
        if control_tx.send(ControlMsg::bare(ack)).await.is_err() {
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

        // Owns every per-request supervisor task spawned below (S3 fix pass — see this module's
        // top doc comment): reaped opportunistically inside the loop's own `select!` (mirroring
        // `serve`'s accept-loop reap, one level down) so it never accumulates one dead entry per
        // historical request for the connection's whole lifetime, and drained-then-aborted once
        // the loop ends, below, so no per-request task outlives this session.
        let mut supervisors: JoinSet<()> = JoinSet::new();

        loop {
            let classification = tokio::select! {
                biased;

                // Pure bookkeeping: frees a finished supervisor task's slot. Never delays or
                // otherwise affects frame delivery — the terminal it sent already went out on the
                // control channel independently of when this JoinSet gets around to reaping it.
                Some(_res) = supervisors.join_next(), if !supervisors.is_empty() => {
                    continue;
                }

                next = reader.next() => match next {
                    None => Classification::Closed,
                    Some(Ok(frame)) => classify::classify(Ok(Some(frame))),
                    Some(Err(err)) => classify::classify(Err(&err)),
                },
            };

            let frame = match classification {
                Classification::NeedMore => continue,
                Classification::Closed => break,
                Classification::Fatal(ep) => {
                    let fatal = SessionError::Fatal(ep).into_out_frame();
                    let _ = control_tx.send(ControlMsg::bare(fatal)).await;
                    break;
                }
                Classification::PerRequestErr { rid, err } => {
                    let diagnostic = SessionError::PerRequest { rid, err }.into_out_frame();
                    if control_tx.send(ControlMsg::bare(diagnostic)).await.is_err() {
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
                        if control_tx.send(ControlMsg::bare(pong)).await.is_err() {
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
                    if !handle_request_frame(
                        frame,
                        &registry,
                        &control_tx,
                        &session_cap,
                        &handler,
                        &config,
                        &mut supervisors,
                    )
                    .await
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
                    if control_tx
                        .send(ControlMsg::bare(unsupported))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }

        // Shutdown, whatever the reader loop's exit reason (clean EOF, a session-fatal
        // classification, GOODBYE-drain, or the writer having gone away — every route that could
        // have opened a transaction `break`s here): nudge every still-running handler toward
        // finishing quickly (cooperative — a handler that ignores its `CancellationToken` is
        // unaffected), ABORT this session's transactions, then give the per-session supervisor
        // `JoinSet` a bounded chance to drain before hard-aborting whatever is left, so no
        // per-request task outlives this session (S3 fix pass). Exactly-one-`END` is untouched:
        // whichever way a given supervisor task ends up finishing (normally, or hard-aborted past
        // the deadline), it was already — and remains — the sole sender of its request's one
        // terminal frame.
        //
        // ORDER IS LOAD-BEARING (S6 seam): `cancel_all()` only fires each request's
        // `CancellationToken`, which the FORWARDING HANDLER of a tx-scoped request — blocked on a
        // `oneshot` recv from its tx actor — does NOT itself observe — so `abort_session` runs
        // BETWEEN it and the drain: aborting the actors drops their reply senders, the in-flight
        // handler's recv returns `Err`, it declares its one terminal, and the supervisor delivers
        // the `END` inside the drain window.
        //
        // M1-S4 NOTE (SPEC §22.2): the tx actor's OWN in-flight-statement select (`tx/actor.rs`)
        // now ALSO carries a per-request `cancel: CancellationToken` — of structural necessity the
        // SAME token this `cancel_all()` fires (it is the one token a client's own
        // `CANCEL{request_id}` must reach). So on session death, firing `cancel_all()` HERE, before
        // `abort_session` below, can make the actor's `cancel` arm win the teardown race instead of
        // `abort`: the transaction still rolls back and the conn is still released either way, but
        // the actor tombstones + REPLIES `TxDeadline{Retryable}` (a real, declared terminal) rather
        // than dropping the reply for the handler to synthesize its own `Protocol`. This is a
        // recorded, traced-safe deviation (§22.2), not a bug: exactly one teardown, exactly one
        // terminal, no leaked permit, and a losing client sees a safe-or-better fate (the tx is
        // already rolled back, so there is no double-apply risk either way).
        registry.cancel_all();
        tx_registry.abort_session(session_id).await;
        drain_supervisors(&mut supervisors, config.drain_deadline).await;

        drop(control_tx);
        let _ = writer_handle.await;
    }
}

/// Wait for every per-request supervisor task in `supervisors` to finish, up to `deadline`.
/// Mirrors `serve::drain_sessions`'s identical shape one level up: anything still outstanding past
/// the deadline is hard-closed via `abort_all` (and the `JoinSet`'s own `Drop`, belt-and-
/// suspenders) rather than waited on indefinitely. Aborting a supervisor task does NOT forcibly
/// stop the handler task it was awaiting — this design never aborts a handler's `JoinHandle` (see
/// `session::supervisor`'s doc comment) — so this bounds how long THIS session's own shutdown can
/// be blocked by a handler that ignores the cancellation `registry.cancel_all()` already sent it;
/// it does not guarantee a truly non-cooperative handler's task stops running.
async fn drain_supervisors(supervisors: &mut JoinSet<()>, deadline: Duration) {
    let wait_all = async { while supervisors.join_next().await.is_some() {} };

    if tokio::time::timeout(deadline, wait_all).await.is_err() {
        tracing::warn!(
            ?deadline,
            "per-session request drain deadline exceeded: hard-closing remaining request tasks"
        );
        supervisors.abort_all();
    }
}

/// Route one request-bearing frame: insert into the registry (sending a per-request diagnostic
/// and returning early on `Reused`/`Full`/a reserved `request_id == 0`), reserve a control-channel
/// permit, then spawn the handler task and a supervisor task (owned by the caller's per-session
/// `supervisors` `JoinSet`) to await it. Returns `false` if the control channel is gone (writer
/// task exited) and the reader loop should stop.
async fn handle_request_frame(
    frame: InFrame,
    registry: &Arc<Registry>,
    control_tx: &mpsc::Sender<ControlMsg>,
    session_cap: &Arc<SessionCap>,
    handler: &HandlerFn,
    config: &Config,
    supervisors: &mut JoinSet<()>,
) -> bool {
    let id = frame.header.request_id;

    // request_id 0 is reserved for session-context terminals (session-fatal / GOODBYE / no-
    // request-context) — see this module's top doc comment. A request-bearing frame claiming it
    // is rejected the same way a reused id or an over-quota one is: a per-request diagnostic sent
    // directly, the registry never touched.
    if id == 0 {
        let diagnostic =
            diagnostic_error_frame(&frame, "request_id 0 is reserved for session context");
        return control_tx.send(ControlMsg::bare(diagnostic)).await.is_ok();
    }

    let credit = Credit::new(config.credit_frames, config.credit_bytes);

    // `credit_cell` is the producer's handle onto this request's flow-control window (wired into
    // the streamed `Responder` below, M1-S5 Task 4a). The registry keeps its own clone regardless,
    // so a routed `WINDOW_UPDATE` can replenish it purely by id.
    let (cancel, credit_cell) = match registry.insert(id, credit) {
        Ok(pair) => pair,
        Err(err) => {
            let message = match err {
                InsertErr::Reused => "reused in-flight request id",
                InsertErr::Full => "max_inflight exceeded",
            };
            let diagnostic = diagnostic_error_frame(&frame, message);
            return control_tx.send(ControlMsg::bare(diagnostic)).await.is_ok();
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

    let service = frame.header.service;
    let method = frame.header.method;

    // The handler ALWAYS gets a stream-capable `Responder` (M5 — additive, `HandlerFn` unchanged):
    // it carries this request's credit window, the shared session cap, a `control_tx` clone, and
    // the request id so a `fetch:stream` handler can `send_head`/`send_data`. A non-streamed
    // handler simply never calls those and declares its one terminal exactly as before.
    let (responder, cell) =
        Responder::new_streaming(id, credit_cell, Arc::clone(session_cap), control_tx.clone());
    let handler = handler.clone();
    let handle = tokio::spawn(async move { handler(frame, responder, cancel).await });

    // Owned by the per-session `supervisors` JoinSet (S3 fix pass) rather than a detached
    // `tokio::spawn` — see `drain_supervisors` and this module's top doc comment for why: without
    // this, a writer that exits early (a `sink.send()` error, e.g. a mid-request client
    // disconnect with other requests still in flight) left this task orphaned, tracked by nothing.
    supervisors.spawn(supervisor::supervise(
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

/// A `HandlerFn` equivalent to `Session::run`'s built-in stub: declares `Unsupported` for every
/// request-bearing frame. Exposed (unlike the private `default_handler` it wraps) so `serve`'s
/// real, non-test callers (`main`) can hand `serve` a `HandlerFn` the same way a test hands it a
/// scripted one, without duplicating `Session::run`'s default dispatch.
pub fn default_handler_fn() -> HandlerFn {
    Arc::new(default_handler)
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
