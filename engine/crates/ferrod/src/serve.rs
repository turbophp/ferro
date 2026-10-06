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
//!
//! **The HTTP drain (SPEC §23.6.1, M6-F4b) — both chassis changes live here.** (1) Every session is
//! spawned with the same `Drain` the `SIGTERM` watcher triggers (`Session::run_draining`), so a
//! session's services are TOLD the daemon is draining instead of finding out from the hard abort;
//! Ferro HTTP refuses new requests at once (`draining`) and stops in-flight exchanges at
//! `FERRO_HTTP_DRAIN_MS`. (2) `serve` outlasts that cap: if HTTP exchanges are still in flight when
//! `drain_deadline` expires, the wait extends to `FERRO_HTTP_DRAIN_MS + drain_deadline` from the
//! drain's start — not their maximum — so an exchange stopped AT the cap still has `drain_deadline`
//! to emit its engine-classified `END` before the hard abort (otherwise the client classifies it by
//! §23.7.3, which cannot see an operator's `IDEMPOTENT_METHODS`). The extension ends early,
//! `drain_deadline` after the last exchange's terminal ([`http_drain_end`]). With no HTTP in flight
//! at `drain_deadline`, the drain is exactly what it was. Cost, stated: sessions live — and keep
//! serving SQL — for the extension too, because an HTTP exchange and SQL statements share a session.

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
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
                        // Out of fds or kernel memory, the pending connection stays in the backlog
                        // and `accept` fails again at once: without a pause this loop spins a core
                        // and writes a log line per spin (M3-D3 review F2 measured 717 000 lines in
                        // 13 s). Back off briefly, still answering a drain.
                        if accept_error_is_resource_exhaustion(&err) {
                            tokio::select! {
                                biased;
                                _ = drain.wait() => {}
                                _ = tokio::time::sleep(ACCEPT_BACKOFF) => {}
                            }
                        }
                        continue;
                    }
                };

                match peercred::peer_uid(&stream) {
                    Ok(uid) if config.uid_allowed(uid) => {
                        let session_config = config.clone();
                        let session_factory = factory.clone();
                        let session_pool_registry = pool_registry.clone();
                        let session_tx_registry = tx_registry.clone();
                        let session_drain = drain.clone();
                        sessions.spawn(async move {
                            Session::run_draining(
                                stream,
                                session_config,
                                epoch,
                                session_pool_registry,
                                session_tx_registry,
                                session_factory,
                                session_drain,
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

    let started = drain.started_at().unwrap_or_else(tokio::time::Instant::now);
    drain_sessions(
        sessions,
        config.drain_deadline,
        started,
        pool_registry.http_drain(),
    )
    .await;
}

/// Send one session-fatal Auth frame on a just-accepted, not-yet-a-`Session` stream, then close
/// it: `rid=0, flags=END, Outcome::Error{code: errc::AUTH}` (SPEC G-4 — a peercred denial is
/// session-fatal, never a silent close). `Framed::send` both encodes and flushes the frame; a
/// write/flush failure (the peer already gone) is logged and otherwise ignored — either way the
/// stream is dropped right after, closing the connection.
async fn deny_connection(stream: UnixStream, err: SessionError) {
    let mut framed = Framed::new(stream, FrameCodec);
    if let Err(send_err) = framed.send(err.into_out_frame()).await {
        tracing::warn!(error = %send_err, "failed to send peercred-deny frame");
    }
}

/// When `serve`'s extended HTTP wait ends (SPEC §23.6.1 chassis change 2): `FERRO_HTTP_DRAIN_MS +
/// drain_deadline` after the drain began, or — if every HTTP exchange has already returned its
/// terminal, at `zero_at` — `drain_deadline` after that, whichever is first. Pure, so the formula
/// (a SUM, not the maximum) is pinned by a unit test rather than by a timing race.
pub fn http_drain_end(
    started: tokio::time::Instant,
    drain_deadline: Duration,
    http_cap: Duration,
    zero_at: Option<tokio::time::Instant>,
) -> tokio::time::Instant {
    let hard = started + http_cap + drain_deadline;
    match zero_at {
        Some(z) => hard.min(z + drain_deadline),
        None => hard,
    }
}

/// Wait for every session task in `sessions` to finish, up to `deadline` after the drain began —
/// extended while Ferro HTTP exchanges are in flight (see the module docs and [`http_drain_end`]).
/// Anything still outstanding past the wait is hard-closed: `abort_all` (and the `JoinSet`'s own
/// `Drop`, belt-and-suspenders) aborts every remaining task rather than waiting on it indefinitely.
async fn drain_sessions(
    mut sessions: JoinSet<()>,
    deadline: Duration,
    started: tokio::time::Instant,
    http: Option<(Duration, watch::Receiver<usize>)>,
) {
    let wait_all = async { while sessions.join_next().await.is_some() {} };
    if tokio::time::timeout_at(started + deadline, wait_all)
        .await
        .is_ok()
    {
        return;
    }

    if let Some((cap, mut in_flight)) = http
        && *in_flight.borrow() > 0
    {
        tracing::info!(
            in_flight = *in_flight.borrow(),
            ?cap,
            "drain: HTTP exchanges in flight; waiting up to FERRO_HTTP_DRAIN_MS + drain_deadline"
        );
        let mut end = http_drain_end(started, deadline, cap, None);
        let mut zero_seen = false;
        loop {
            tokio::select! {
                biased;
                r = sessions.join_next() => {
                    if r.is_none() {
                        return;
                    }
                }
                () = tokio::time::sleep_until(end) => break,
                _ = in_flight.wait_for(|n| *n == 0), if !zero_seen => {
                    // Every exchange has returned its terminal (or the engine is gone): give the
                    // last `END`s `drain_deadline` to leave, never past the hard end.
                    zero_seen = true;
                    end = http_drain_end(started, deadline, cap, Some(tokio::time::Instant::now()));
                }
            }
        }
    }

    tracing::warn!(
        ?deadline,
        "drain deadline exceeded: hard-closing remaining sessions"
    );
    sessions.abort_all();
}

/// How long the accept loop pauses after an `accept` that failed for lack of fds or kernel memory.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// `EMFILE`, `ENFILE`, `ENOBUFS` and `ENOMEM`: `accept` failures that will repeat at once, because
/// the connection stays queued until a resource frees up. Every other failure (`ECONNABORTED`, a
/// peer that reset before it was accepted) concerns one connection and retries immediately.
fn accept_error_is_resource_exhaustion(err: &std::io::Error) -> bool {
    use nix::errno::Errno;
    matches!(
        err.raw_os_error().map(Errno::from_raw),
        Some(Errno::EMFILE | Errno::ENFILE | Errno::ENOBUFS | Errno::ENOMEM)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SPEC §23.6.1 chassis change 2: the extended wait is the SUM `FERRO_HTTP_DRAIN_MS +
    /// drain_deadline` from the drain's start (not the maximum, which would hard-abort at the very
    /// instant an exchange is stopped at the cap), cut short to `drain_deadline` after the last HTTP
    /// terminal, and never beyond the sum.
    #[test]
    fn the_http_drain_wait_is_the_sum_cut_short_after_the_last_terminal() {
        let t0 = tokio::time::Instant::now();
        let dd = Duration::from_secs(5);
        let cap = Duration::from_secs(30);
        assert_eq!(
            http_drain_end(t0, dd, cap, None),
            t0 + Duration::from_secs(35)
        );
        assert_eq!(
            http_drain_end(t0, dd, cap, Some(t0 + Duration::from_secs(12))),
            t0 + Duration::from_secs(17)
        );
        assert_eq!(
            http_drain_end(t0, dd, cap, Some(t0 + Duration::from_secs(33))),
            t0 + Duration::from_secs(35),
            "never past the sum"
        );
        assert_eq!(
            http_drain_end(t0, dd, Duration::ZERO, None),
            t0 + dd,
            "a zero cap still leaves drain_deadline for the stopped exchanges' terminals"
        );
    }

    #[test]
    fn only_resource_exhaustion_backs_off() {
        use nix::errno::Errno;
        for e in [Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM] {
            assert!(accept_error_is_resource_exhaustion(
                &std::io::Error::from_raw_os_error(e as i32)
            ));
        }
        for e in [Errno::ECONNABORTED, Errno::EPROTO, Errno::EINTR] {
            assert!(!accept_error_is_resource_exhaustion(
                &std::io::Error::from_raw_os_error(e as i32)
            ));
        }
    }
}
