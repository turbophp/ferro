//! The peercred-gated accept loop, extracted from `main` so it is directly testable: a test binds
//! a real `UnixListener`, calls `serve` with an injected `shutdown::Drain` (no real signal
//! needed), connects clients, triggers the drain, and asserts behavior. `main` itself is just:
//! wire config + a real `SIGTERM`/`ctrl_c` watcher driving the SAME `Drain` type + this function.
//!
//! **Accept-time peercred gate.** Every accepted connection is checked (`peercred::peer_uid` +
//! `config.uid_allowed`) BEFORE any part of `Session::run_with_handler` ever runs: a denied or
//! unreadable peer's stream is dropped immediately — no HELLO_ACK, no writer task, nothing put on
//! the wire at all. Only an allowed peer's connection is spawned as a session task.
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

use std::time::Duration;

use tokio::net::UnixListener;
use tokio::task::JoinSet;

use crate::config::Config;
use crate::epoch::BootEpoch;
use crate::peercred;
use crate::session::{HandlerFn, Session};
use crate::shutdown::Drain;

/// Drive `listener`'s peercred-gated accept loop until `drain` is triggered, then let already-
/// spawned session tasks finish (up to `config.drain_deadline`) before returning. Every accepted
/// connection is driven via `Session::run_with_handler(.., handler.clone())`.
pub async fn serve(
    listener: UnixListener,
    config: Config,
    epoch: BootEpoch,
    drain: Drain,
    handler: HandlerFn,
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
                        let session_config = config.clone();
                        let session_handler = handler.clone();
                        sessions.spawn(async move {
                            Session::run_with_handler(stream, session_config, epoch, session_handler)
                                .await;
                        });
                    }
                    Ok(uid) => {
                        tracing::warn!(uid, "peercred denied: rejecting connection");
                        drop(stream);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "peercred lookup failed: rejecting connection");
                        drop(stream);
                    }
                }
            }
        }
    }

    drain_sessions(sessions, config.drain_deadline).await;
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
