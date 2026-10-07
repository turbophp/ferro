//! The hand-rolled HTTP/1.1 keep-alive pool (SPEC §23.8.1, §23.8.2; D9 extended).
//!
//! Built directly on `hyper::client::conn::http1` — `hyper-util`'s `legacy::Client` is not used,
//! because its pool sits where `sent` must be observed and it re-dispatches internally. One sub-pool
//! per upstream, or per (upstream, peer uid) under `PARTITION=uid` (§23.8.1). An HTTP/1.1
//! connection carries one exchange at a time, with no pipelining.
//!
//! **Reuse (§23.8.2).** Idle connections are taken LIFO. A candidate must be open, younger than
//! `MAX_LIFETIME_MS`, and idle for less than its idle limit (`IDLE_TIMEOUT_MS`, clamped to the
//! server's `Keep-Alive: timeout` minus 1 s). **A non-idempotent request additionally refuses a
//! connection idle longer than `H1_UNSAFE_REUSE_MAX_IDLE_MS` and dials fresh** — the keep-alive race
//! is the stock cause of a stale-reuse `Indeterminate`, curl hides it with a re-send, and Ferro may
//! not re-send (charter rule 3), so it narrows the race instead. Such a connection is left in the
//! pool for an idempotent request; since the stack is LIFO, every connection below it is idler
//! still, so the check needs only the top.
//!
//! **Discard, never drain (§23.8.2).** After a cancel, a timeout, an abandoned body or any failure,
//! the connection is discarded: [`HttpConn::discard`] aborts `hyper`'s connection task and AWAITS it,
//! which drops the I/O — the point after which `sent` may be read (§23.7.1).
//!
//! **Not here (slice F6, §23.15):** `MAX_CONNECTIONS` / `MAX_REQUESTS` / `MAX_DIALS` as concurrency
//! limits and the `MAX_QUEUED` queue. F4a bounds only what the pool RETAINS: at most
//! `MAX_CONNECTIONS` idle connections per sub-pool; a surplus one is discarded on return.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hyper::client::conn::http1;
use rustls::AlertDescription;
use tokio::task::JoinHandle;

use super::body::OneChunk;
use super::track::TrackState;

/// One HTTP/1.1 upstream connection.
pub struct HttpConn {
    pub sender: http1::SendRequest<OneChunk>,
    /// `hyper`'s connection task. It writes independently of the response future (§23.7.1), so
    /// `sent` is final only once it has been aborted and awaited. Its output is the TLS
    /// client-certificate refusal its connection died of, if any (M6-F5c review F4: when the task
    /// reads the alert before the request is queued, `hyper` hands the request back with an error
    /// that carries no I/O source, so the task is the only place the alert can still be read).
    pub task: JoinHandle<Option<AlertDescription>>,
    pub track: Arc<TrackState>,
    /// The checked address this connection is pinned to for life (§23.8.5).
    pub peer: SocketAddr,
    pub created: Instant,
    pub idle_since: Instant,
    /// `IDLE_TIMEOUT_MS`, clamped by the last response's `Keep-Alive: timeout` (§23.8.2).
    pub idle_limit: Duration,
    /// Counts this connection in the engine's live-connection gauge for exactly its life.
    pub live: LiveConn,
}

impl HttpConn {
    /// Abort `hyper`'s connection task and wait until it is gone, so the I/O is dropped and no
    /// later byte can follow. Only after this is the tracker's `sent` final (§23.7.1).
    pub async fn discard(self) {
        let _ = self.discard_reporting().await;
    }

    /// [`discard`](Self::discard), returning the client-certificate refusal the connection task
    /// died of, if it had already died of one (a task still running is aborted: `None`).
    pub async fn discard_reporting(mut self) -> Option<AlertDescription> {
        self.task.abort();
        (&mut self.task).await.ok().flatten()
    }
}

/// **No connection outlives its `HttpConn` (M6-F4b, chaos 12).** Dropping a `JoinHandle` does NOT
/// stop a tokio task, so before this an `HttpConn` dropped without [`HttpConn::discard`] — the
/// exchange's future dropped mid-flight, a stale or surplus pooled connection — left `hyper`'s
/// connection task, and with it the socket, running. Every drop now aborts it; `discard` remains the
/// path that also WAITS, which is what reading `sent` needs.
impl Drop for HttpConn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One live connection, counted in the engine's gauge from dial to drop.
#[derive(Debug)]
pub struct LiveConn(Arc<AtomicUsize>);

impl LiveConn {
    pub fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        LiveConn(Arc::clone(count))
    }
}

impl Drop for LiveConn {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The sub-pool key: the upstream, and the peer uid under `PARTITION=uid`.
pub type PoolKey = (String, Option<u32>);

/// What a sub-pool may hand out, decided per checkout.
#[derive(Clone, Copy, Debug)]
pub struct ReusePolicy {
    pub max_lifetime: Duration,
    pub idempotent: bool,
    pub unsafe_reuse_max_idle: Duration,
}

/// Every sub-pool's idle stack.
#[derive(Default)]
pub struct Pools {
    idle: Mutex<HashMap<PoolKey, Vec<HttpConn>>>,
}

impl Pools {
    /// Take the most recently returned usable connection, discarding every stale one found on the
    /// way. `None`: dial fresh.
    pub fn checkout(&self, key: &PoolKey, policy: ReusePolicy) -> Option<HttpConn> {
        let mut stale = Vec::new();
        let picked = {
            let mut map = self.idle.lock().unwrap_or_else(|p| p.into_inner());
            let stack = map.get_mut(key)?;
            let now = Instant::now();
            let mut picked = None;
            while let Some(c) = stack.pop() {
                let idle = now.saturating_duration_since(c.idle_since);
                let unusable = c.sender.is_closed()
                    || c.task.is_finished()
                    || now.saturating_duration_since(c.created) >= policy.max_lifetime
                    || idle >= c.idle_limit;
                if unusable {
                    stale.push(c);
                    continue;
                }
                if !policy.idempotent && idle > policy.unsafe_reuse_max_idle {
                    // §23.8.2: too idle for a non-idempotent request. Left for idempotent ones; the
                    // ones below it are idler still (LIFO), so dial fresh.
                    stack.push(c);
                    break;
                }
                picked = Some(c);
                break;
            }
            picked
        };
        drop(stale); // each drop aborts its connection task
        picked
    }

    /// Return a connection after a completed exchange. `max_idle` bounds what the sub-pool retains
    /// (`MAX_CONNECTIONS`); a surplus connection is discarded.
    pub fn checkin(&self, key: PoolKey, mut conn: HttpConn, max_idle: usize) {
        conn.idle_since = Instant::now();
        let mut map = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let stack = map.entry(key).or_default();
        if stack.len() >= max_idle {
            drop(conn); // aborts its connection task
            return;
        }
        stack.push(conn);
    }

    /// Return a connection that was checked out but never used (a `CANCEL` or deadline caught
    /// before dispatch). Unlike [`Pools::checkin`] it keeps `idle_since`, so the reuse rules —
    /// above all `H1_UNSAFE_REUSE_MAX_IDLE_MS` — keep seeing how long it has really been idle.
    pub fn return_unused(&self, key: PoolKey, conn: HttpConn, max_idle: usize) {
        let mut map = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let stack = map.entry(key).or_default();
        if stack.len() >= max_idle {
            drop(conn); // aborts its connection task
            return;
        }
        stack.push(conn);
    }

    /// Idle connections currently held for `key` (diagnostics and tests).
    pub fn idle_count(&self, key: &PoolKey) -> usize {
        let map = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        map.get(key).map_or(0, Vec::len)
    }
}

/// `Keep-Alive: timeout=N` → the idle limit: `min(IDLE_TIMEOUT_MS, N s − 1 s)` (§23.8.2). A header
/// with no parseable `timeout` leaves the configured limit.
pub fn idle_limit(configured: Duration, keep_alive: Option<&[u8]>) -> Duration {
    let Some(v) = keep_alive.and_then(|v| std::str::from_utf8(v).ok()) else {
        return configured;
    };
    for part in v.split(',') {
        let mut kv = part.splitn(2, '=');
        let k = kv.next().unwrap_or("").trim();
        if k.eq_ignore_ascii_case("timeout")
            && let Some(secs) = kv.next().and_then(|s| s.trim().parse::<u64>().ok())
        {
            let server = Duration::from_secs(secs).saturating_sub(Duration::from_secs(1));
            return configured.min(server);
        }
    }
    configured
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::track::Tracker;
    use hyper_util::rt::TokioIo;

    /// A live `HttpConn` over an in-memory pipe whose far end is kept open (returned), idle for
    /// `idle_ago` already.
    async fn conn(idle_ago: Duration) -> (HttpConn, tokio::io::DuplexStream) {
        let (a, b) = tokio::io::duplex(4096);
        let (tracked, track) = Tracker::new(a);
        let (sender, c) = http1::handshake::<_, OneChunk>(TokioIo::new(tracked))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            let _ = c.await;
            None
        });
        let now = Instant::now();
        let conn = HttpConn {
            sender,
            task,
            track,
            peer: "127.0.0.1:1".parse().unwrap(),
            created: now,
            idle_since: now.checked_sub(idle_ago).unwrap(),
            idle_limit: Duration::from_secs(60),
            live: LiveConn::new(&Arc::new(AtomicUsize::new(0))),
        };
        (conn, b)
    }

    fn policy(idempotent: bool) -> ReusePolicy {
        ReusePolicy {
            max_lifetime: Duration::from_secs(60),
            idempotent,
            unsafe_reuse_max_idle: Duration::from_secs(2),
        }
    }

    /// Review minor of M6-F4a: a connection handed back UNUSED (a `CANCEL` or deadline caught
    /// before dispatch) keeps its true idleness, so `H1_UNSAFE_REUSE_MAX_IDLE_MS` still refuses it
    /// to a non-idempotent request; a connection returned after an exchange is fresh again.
    #[tokio::test]
    async fn an_unused_return_keeps_idle_since_and_a_checkin_resets_it() {
        let key: PoolKey = ("u".into(), None);
        let pools = Pools::default();
        let (c, _far) = conn(Duration::from_secs(5)).await;
        pools.return_unused(key.clone(), c, 8);
        assert!(
            pools.checkout(&key, policy(false)).is_none(),
            "5 s idle is past the 2 s unsafe-reuse bound"
        );
        assert!(pools.checkout(&key, policy(true)).is_some());

        let (c, _far2) = conn(Duration::from_secs(5)).await;
        pools.checkin(key.clone(), c, 8);
        assert!(
            pools.checkout(&key, policy(false)).is_some(),
            "checkin resets idleness"
        );
    }

    #[test]
    fn keep_alive_timeout_clamps_the_idle_limit_minus_a_second() {
        let cfg = Duration::from_secs(15);
        assert_eq!(idle_limit(cfg, None), cfg);
        assert_eq!(
            idle_limit(cfg, Some(b"timeout=5, max=100")),
            Duration::from_secs(4)
        );
        assert_eq!(idle_limit(cfg, Some(b"max=100, Timeout=60")), cfg);
        assert_eq!(idle_limit(cfg, Some(b"timeout=1")), Duration::ZERO);
        assert_eq!(idle_limit(cfg, Some(b"timeout=x")), cfg);
    }
}
