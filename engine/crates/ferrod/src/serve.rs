//! The peercred-gated accept loop, extracted from `main` so it is directly testable: a test binds
//! a real `UnixListener`, calls `serve` with an injected `shutdown::Drain` (no real signal
//! needed), connects clients, triggers the drain, and asserts behavior. `main` itself is just:
//! wire config + a real `SIGTERM`/`ctrl_c` watcher driving the SAME `Drain` type + this function.
//!
//! **Accept-time peercred gate.** Every accepted connection is checked (`peercred::peer_uid` +
//! `config.uid_allowed`) BEFORE any part of `Session::run_with_handler` ever runs: a denied or
//! unreadable peer never gets a HELLO_ACK, never gets a writer task, and is never spawned as a
//! session — but the rejection itself is session-fatal per SPEC G-4, so it is not a silent
//! `drop(stream)`. `deny_connection` below wraps the raw stream in a one-shot
//! `Framed<_, FrameCodec>` just long enough to send a single `rid=0, flags=END,
//! Outcome::Error{code: AUTH}` frame (`SessionError::peercred_denied`), then closes it. Only an
//! allowed peer's connection is spawned as a session task.
//!
//! **Connection cap (M1-S9a, M0-core-review finding 5b).** An allowed peer's connection is spawned
//! only while `sessions.len() < config.max_connections`; the overflow gets the same one-frame
//! treatment via `deny_connection`, carrying `POOL_TIMEOUT{Retryable}` (`SessionError::overloaded`)
//! so the client's resilience loop retries rather than treating it as terminal. The check sits
//! INSIDE the peercred-allowed arm on purpose: authentication stays the outermost gate, so a
//! peer we would refuse anyway is answered `AUTH` (SPEC G-4) whatever the daemon's load, and never
//! learns it from a different reply. It costs one `getsockopt` on a connection we are about to
//! close — the fd is already accepted either way, so nothing is reclaimed sooner by reordering.
//!
//! Note what the cap does NOT cover: if the process fd limit binds before `max_connections`,
//! `accept(2)` fails with `EMFILE` and the arm below logs + `continue`s while the connection stays
//! in the backlog, i.e. a hot retry loop. That is why the default cap is chosen to sit under the
//! usual `LimitNOFILE` (see `config::DEFAULT_MAX_CONNECTIONS`); the loop's own EMFILE behaviour is
//! recorded as out of this slice's scope, not fixed here.
//!
//! **Drain.** The accept loop is a `tokio::select!` between `drain.wait()`, an opportunistic
//! **reap** of finished session tasks, and `listener.accept()` — `biased` so that (1) once
//! draining has started, a connection already queued in the kernel's accept backlog is never
//! additionally accepted, even if it happened to be ready in the same poll (SPEC's "stop accepting
//! on drain" is a hard edge, not a race), and (2) a finished session task is reaped before we
//! accept another connection, rather than after. Every spawned session task is tracked in a
//! `JoinSet`; the reap arm (`sessions.join_next()`, guarded by `!sessions.is_empty()` so an empty
//! set never yields a spurious-but-harmless `Ready(None)`) calls `join_next` DURING the accept
//! loop's normal operation, not only at drain time — without it, every accepted connection leaves
//! one dead entry in the `JoinSet` for the rest of the daemon's uptime (an unbounded task/memory
//! leak, weaponizable by nothing more than connect/close churn). Once the accept loop breaks,
//! `serve` waits for that same `JoinSet` to drain up to `config.drain_deadline`, then — if
//! anything is still outstanding — hard-closes by aborting whatever remains (`JoinSet::drop`
//! aborts every task still in the set) rather than waiting indefinitely.

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinSet;
use tokio_util::codec::Framed;

use crate::config::Config;
use crate::epoch::BootEpoch;
use crate::peercred;
use crate::pools::PoolRegistry;
use crate::session::codec::FrameCodec;
use crate::session::error::SessionError;
use crate::session::{HandlerFactory, Session};
use crate::shutdown::Drain;
use crate::tx::TxRegistry;

/// Drive `listener`'s peercred-gated accept loop until `drain` is triggered, then let already-
/// spawned session tasks finish (up to `config.drain_deadline`) before returning. Every accepted
/// connection is driven via
/// `Session::run_with_handler(.., pool_registry.clone(), tx_registry.clone(), factory.clone())`;
/// the one `tx_registry` is shared by every session (S6 seam), and so is the one `pool_registry` —
/// which is what makes the `HELLO_ACK` server-version cache a per-DAEMON cache rather than a
/// per-connection one (M1-S8a Task 12; a per-connection cache would re-probe on every handshake,
/// which is exactly what the cache exists to prevent).
pub async fn serve(
    listener: UnixListener,
    config: Config,
    epoch: BootEpoch,
    drain: Drain,
    pool_registry: Arc<PoolRegistry>,
    tx_registry: Arc<TxRegistry>,
    factory: HandlerFactory,
) {
    let mut sessions: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            biased;

            // Checked first: once draining has started, never accept another connection, even
            // one already sitting in the kernel's backlog.
            _ = drain.wait() => {
                tracing::info!("drain triggered: no longer accepting new connections");
                break;
            }

            // Reap a finished session task's `JoinSet` slot. Checked ahead of `accept` (still
            // `biased`) so cleanup isn't starved by a steady stream of new connections; this is
            // pure bookkeeping — it never refuses/delays a connection, it only frees capacity a
            // completed session task is done using.
            Some(res) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(join_err) = res {
                    tracing::warn!(error = %join_err, "session task ended abnormally");
                }
            }

            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((stream, _addr)) => stream,
                    Err(err) => {
                        tracing::warn!(error = %err, "accept failed");
                        continue;
                    }
                };

                match peercred::peer_uid(&stream) {
                    Ok(uid) if config.uid_allowed(uid) => {
                        // The connection cap (M1-S9a finding 5b), checked on the ALLOWED path
                        // only: a peer we would refuse anyway never learns anything about our
                        // load, and never displaces the AUTH answer SPEC G-4 owes it. `len()` is
                        // the live session count for free (hazard 20) — the reap arm above is
                        // `biased` ahead of this one, so no finished session is still counted here.
                        if sessions.len() >= config.max_connections {
                            tracing::warn!(
                                limit = config.max_connections,
                                "connection limit reached: rejecting new connection"
                            );
                            deny_connection(
                                stream,
                                SessionError::overloaded(format!(
                                    "connection limit ({}) reached; retry shortly",
                                    config.max_connections
                                )),
                            )
                            .await;
                            continue;
                        }

                        let session_config = config.clone();
                        let session_factory = factory.clone();
                        let session_pool_registry = pool_registry.clone();
                        let session_tx_registry = tx_registry.clone();
                        sessions.spawn(async move {
                            Session::run_with_handler(
                                stream,
                                session_config,
                                epoch,
                                session_pool_registry,
                                session_tx_registry,
                                session_factory,
                            )
                            .await;
                        });
                    }
                    Ok(uid) => {
                        tracing::warn!(uid, "peercred denied: rejecting connection");
                        let err = SessionError::peercred_denied(format!(
                            "peer uid {uid} is not permitted to connect"
                        ));
                        deny_connection(stream, err).await;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "peercred lookup failed: rejecting connection");
                        let err =
                            SessionError::peercred_denied(format!("peercred lookup failed: {err}"));
                        deny_connection(stream, err).await;
                    }
                }
            }
        }
    }

    drain_sessions(sessions, config.drain_deadline).await;
}

/// Send one session-fatal Auth frame on a just-accepted, not-yet-a-`Session` stream, then close
/// it: `rid=0, flags=END, Outcome::Error{code: errc::AUTH}` (SPEC G-4 — a peercred denial is
/// session-fatal, never a silent close). `Framed::send` both encodes and flushes the frame; a
/// write/flush failure (the peer already gone) is logged and otherwise ignored — either way the
/// stream is dropped right after, closing the connection.
async fn deny_connection(stream: UnixStream, err: SessionError) {
    let mut framed = Framed::new(stream, FrameCodec::default());
    if let Err(send_err) = framed.send(err.into_out_frame()).await {
        tracing::warn!(error = %send_err, "failed to send peercred-deny frame");
    }
}

/// Wait for every session task in `sessions` to finish, up to `deadline`. Anything still
/// outstanding past the deadline is hard-closed: `abort_all` (and the `JoinSet`'s own `Drop`,
/// belt-and-suspenders) aborts every remaining task rather than waiting on it indefinitely.
async fn drain_sessions(mut sessions: JoinSet<()>, deadline: Duration) {
    let wait_all = async { while sessions.join_next().await.is_some() {} };

    if tokio::time::timeout(deadline, wait_all).await.is_err() {
        tracing::warn!(
            ?deadline,
            "drain deadline exceeded: hard-closing remaining sessions"
        );
        sessions.abort_all();
    }
}
