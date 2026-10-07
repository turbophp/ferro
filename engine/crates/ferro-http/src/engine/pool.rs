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
//! **Connection limits (slice M6-F6, §23.6 step 5).** Each sub-pool holds at most `MAX_CONNECTIONS`
//! connections — idle, in use and being dialled together — and runs at most `MAX_DIALS` dials at
//! once; both are per SUB-POOL, so under `PARTITION=uid` per (upstream, uid), because that key
//! "partitions *connections*" (§23.8.1; SPEC §22.2 (dk)). A request that finds no usable idle
//! connection and no room to dial WAITS in [`Pools::take`] until a connection is returned, closed
//! or a dial ends; the caller bounds that wait (`QUEUE_TIMEOUT_MS`, the deadline, `CANCEL`). When
//! the sub-pool is full and every idle connection is unusable FOR THIS REQUEST (too idle for a
//! non-idempotent one), the idlest is closed to make room, so such a request never waits behind
//! connections it may not use. Every connection counts from the moment its dial is reserved
//! ([`DialSlot`]) to the moment it is dropped ([`ConnSlot`]), so the count cannot leak.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hyper::client::conn::http1;
use rustls::AlertDescription;
use tokio::sync::Notify;
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
    /// Counts this connection against its sub-pool's `MAX_CONNECTIONS` for exactly its life (F6).
    pub slot: ConnSlot,
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

/// A sub-pool's connection counts, shared by its connections so a drop anywhere frees the slot.
#[derive(Debug, Default)]
pub struct SubShared {
    /// Connections that exist (idle, in use, being torn down) or are being dialled.
    live: AtomicUsize,
    /// Dials in progress.
    dialing: AtomicUsize,
    /// Woken whenever a slot frees or a connection is returned.
    changed: Notify,
}

/// A connection's place under `MAX_CONNECTIONS`; dropped with the connection.
#[derive(Debug)]
pub struct ConnSlot(Arc<SubShared>);

impl Drop for ConnSlot {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

/// A dial in progress under `MAX_DIALS`: counted until the dial ends.
#[derive(Debug)]
struct Dialing(Arc<SubShared>);

impl Drop for Dialing {
    fn drop(&mut self) {
        self.0.dialing.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

/// A reserved dial: one place under `MAX_CONNECTIONS` and one under `MAX_DIALS`. A dial that fails,
/// or is dropped (a `CANCEL`, the deadline), frees both; one that succeeds keeps the connection's
/// place ([`DialSlot::connected`]) and frees the dial's.
#[derive(Debug)]
pub struct DialSlot {
    conn: Option<ConnSlot>,
    _dialing: Dialing,
}

impl DialSlot {
    /// The dial succeeded: the connection keeps its slot, and the dial slot is freed.
    pub fn connected(mut self) -> ConnSlot {
        self.conn
            .take()
            .expect("a DialSlot holds its ConnSlot until connected")
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

/// A sub-pool's connection limits (§23.3.1: `MAX_CONNECTIONS`, `MAX_DIALS`).
#[derive(Clone, Copy, Debug)]
pub struct Caps {
    pub max_connections: usize,
    pub max_dials: usize,
}

/// What [`Pools::take`] hands a request.
#[derive(Debug)]
pub enum Take {
    /// A usable idle connection (reuse).
    Idle(HttpConn),
    /// Room to dial a new one.
    Dial(DialSlot),
}

impl std::fmt::Debug for HttpConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConn")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Sub {
    idle: Vec<HttpConn>,
    shared: Arc<SubShared>,
}

/// Every sub-pool's idle stack and counts.
#[derive(Default)]
pub struct Pools {
    subs: Mutex<HashMap<PoolKey, Sub>>,
}

/// One decision under the lock.
enum Step {
    Got(Take),
    /// Something was closed to make room: decide again.
    Again,
    /// Nothing usable and no room: wait for a change.
    Wait,
}

impl Pools {
    fn shared(&self, key: &PoolKey) -> Arc<SubShared> {
        let mut map = self.subs.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(&map.entry(key.clone()).or_default().shared)
    }

    /// The most recently returned usable connection, or room to dial one — waiting for either if
    /// the sub-pool is at `MAX_CONNECTIONS` or `MAX_DIALS`. Stale connections found on the way are
    /// closed. The caller bounds the wait; dropping the future abandons it with nothing reserved.
    pub async fn take(&self, key: &PoolKey, policy: ReusePolicy, caps: Caps) -> Take {
        let shared = self.shared(key);
        loop {
            // Registered BEFORE the decision, so a slot freed after it wakes this request.
            let changed = shared.changed.notified();
            let mut closed = Vec::new();
            let step = {
                let mut map = self.subs.lock().unwrap_or_else(|p| p.into_inner());
                let sub = map.entry(key.clone()).or_default();
                Self::decide(sub, policy, caps, &mut closed)
            };
            drop(closed); // each drop aborts its connection task and frees its slot
            match step {
                Step::Got(t) => return t,
                Step::Again => {}
                Step::Wait => changed.await,
            }
        }
    }

    fn decide(sub: &mut Sub, policy: ReusePolicy, caps: Caps, closed: &mut Vec<HttpConn>) -> Step {
        let now = Instant::now();
        while let Some(c) = sub.idle.pop() {
            let idle = now.saturating_duration_since(c.idle_since);
            let unusable = c.sender.is_closed()
                || c.task.is_finished()
                || now.saturating_duration_since(c.created) >= policy.max_lifetime
                || idle >= c.idle_limit;
            if unusable {
                closed.push(c);
                continue;
            }
            if !policy.idempotent && idle > policy.unsafe_reuse_max_idle {
                // §23.8.2: too idle for a non-idempotent request. Left for idempotent ones; the
                // ones below it are idler still (LIFO), so dial fresh.
                sub.idle.push(c);
                break;
            }
            return Step::Got(Take::Idle(c));
        }
        if !closed.is_empty() {
            // Their slots free only once they are dropped, outside the lock.
            return Step::Again;
        }
        let s = &sub.shared;
        if s.live.load(Ordering::SeqCst) < caps.max_connections
            && s.dialing.load(Ordering::SeqCst) < caps.max_dials
        {
            // Every increment happens under the pool lock, so the limits are never over-committed.
            s.live.fetch_add(1, Ordering::SeqCst);
            s.dialing.fetch_add(1, Ordering::SeqCst);
            return Step::Got(Take::Dial(DialSlot {
                conn: Some(ConnSlot(Arc::clone(s))),
                _dialing: Dialing(Arc::clone(s)),
            }));
        }
        if s.live.load(Ordering::SeqCst) >= caps.max_connections && !sub.idle.is_empty() {
            // Full, and every idle connection is unusable for THIS request: close the idlest
            // (the bottom of the LIFO stack) to make room.
            closed.push(sub.idle.remove(0));
            return Step::Again;
        }
        Step::Wait
    }

    /// Return a connection after a completed exchange. `max_idle` bounds what the sub-pool retains
    /// (`MAX_CONNECTIONS`); a surplus connection is discarded.
    pub fn checkin(&self, key: PoolKey, mut conn: HttpConn, max_idle: usize) {
        conn.idle_since = Instant::now();
        self.put_back(key, conn, max_idle);
    }

    /// Return a connection that was checked out but never used (a `CANCEL` or deadline caught
    /// before dispatch). Unlike [`Pools::checkin`] it keeps `idle_since`, so the reuse rules —
    /// above all `H1_UNSAFE_REUSE_MAX_IDLE_MS` — keep seeing how long it has really been idle.
    pub fn return_unused(&self, key: PoolKey, conn: HttpConn, max_idle: usize) {
        self.put_back(key, conn, max_idle);
    }

    fn put_back(&self, key: PoolKey, conn: HttpConn, max_idle: usize) {
        let surplus = {
            let mut map = self.subs.lock().unwrap_or_else(|p| p.into_inner());
            let sub = map.entry(key).or_default();
            if sub.idle.len() >= max_idle {
                Some(conn)
            } else {
                sub.idle.push(conn);
                sub.shared.changed.notify_waiters();
                None
            }
        };
        drop(surplus); // aborts its connection task and frees its slot
    }

    /// Idle connections currently held for `key` (diagnostics and tests).
    pub fn idle_count(&self, key: &PoolKey) -> usize {
        let map = self.subs.lock().unwrap_or_else(|p| p.into_inner());
        map.get(key).map_or(0, |s| s.idle.len())
    }

    /// Connections counted against `key`'s `MAX_CONNECTIONS` (idle, in use, dialling).
    pub fn connection_count(&self, key: &PoolKey) -> usize {
        let map = self.subs.lock().unwrap_or_else(|p| p.into_inner());
        map.get(key)
            .map_or(0, |s| s.shared.live.load(Ordering::SeqCst))
    }

    /// Dials in progress for `key` (against `MAX_DIALS`).
    pub fn dial_count(&self, key: &PoolKey) -> usize {
        let map = self.subs.lock().unwrap_or_else(|p| p.into_inner());
        map.get(key)
            .map_or(0, |s| s.shared.dialing.load(Ordering::SeqCst))
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

    const CAPS: Caps = Caps {
        max_connections: 8,
        max_dials: 8,
    };

    /// A live `HttpConn` in `key`'s sub-pool (counted against its `MAX_CONNECTIONS`) over an
    /// in-memory pipe whose far end is kept open (returned), idle for `idle_ago` already.
    async fn conn_in(
        pools: &Pools,
        key: &PoolKey,
        idle_ago: Duration,
    ) -> (HttpConn, tokio::io::DuplexStream) {
        let (a, b) = tokio::io::duplex(4096);
        let (tracked, track) = Tracker::new(a);
        let (sender, c) = http1::handshake::<_, OneChunk>(TokioIo::new(tracked))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            let _ = c.await;
            None
        });
        let shared = pools.shared(key);
        shared.live.fetch_add(1, Ordering::SeqCst);
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
            slot: ConnSlot(shared),
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

    fn key() -> PoolKey {
        ("u".into(), None)
    }

    /// Still waiting after a generous grace: a wait that never ends under correct code.
    async fn pending<F: std::future::Future>(f: F) -> bool {
        tokio::time::timeout(Duration::from_millis(100), f)
            .await
            .is_err()
    }

    /// Review minor of M6-F4a: a connection handed back UNUSED (a `CANCEL` or deadline caught
    /// before dispatch) keeps its true idleness, so `H1_UNSAFE_REUSE_MAX_IDLE_MS` still refuses it
    /// to a non-idempotent request; a connection returned after an exchange is fresh again.
    #[tokio::test]
    async fn an_unused_return_keeps_idle_since_and_a_checkin_resets_it() {
        let key = key();
        let pools = Pools::default();
        let (c, _far) = conn_in(&pools, &key, Duration::from_secs(5)).await;
        pools.return_unused(key.clone(), c, 8);
        assert!(
            matches!(pools.take(&key, policy(false), CAPS).await, Take::Dial(_)),
            "5 s idle is past the 2 s unsafe-reuse bound"
        );
        assert!(matches!(
            pools.take(&key, policy(true), CAPS).await,
            Take::Idle(_)
        ));

        let (c, _far2) = conn_in(&pools, &key, Duration::from_secs(5)).await;
        pools.checkin(key.clone(), c, 8);
        assert!(
            matches!(pools.take(&key, policy(false), CAPS).await, Take::Idle(_)),
            "checkin resets idleness"
        );
    }

    /// `MAX_CONNECTIONS` counts every connection from its dial's reservation to its drop: at the
    /// limit a request waits, and a dropped (or failed) one frees the slot.
    #[tokio::test]
    async fn max_connections_bounds_live_connections_and_a_drop_frees_the_slot() {
        let pools = Pools::default();
        let key = key();
        let caps = Caps {
            max_connections: 2,
            max_dials: 8,
        };
        let Take::Dial(d1) = pools.take(&key, policy(true), caps).await else {
            panic!("dial")
        };
        let (c2, _far) = conn_in(&pools, &key, Duration::ZERO).await;
        assert_eq!(pools.connection_count(&key), 2);
        assert!(pending(pools.take(&key, policy(true), caps)).await);
        drop(d1); // a failed dial frees its slot
        assert_eq!(pools.connection_count(&key), 1);
        let Take::Dial(d3) = pools.take(&key, policy(true), caps).await else {
            panic!("dial")
        };
        let s3 = d3.connected();
        assert_eq!(pools.connection_count(&key), 2, "a connected dial keeps its slot");
        let waiter = pools.take(&key, policy(true), caps);
        tokio::pin!(waiter);
        assert!(pending(&mut waiter).await);
        drop(c2); // a connection dropped anywhere frees its slot and wakes the waiter
        assert!(matches!(waiter.await, Take::Dial(_)));
        drop(s3);
    }

    /// `MAX_DIALS` bounds dials in progress, not connections.
    #[tokio::test]
    async fn max_dials_bounds_dials_in_progress() {
        let pools = Pools::default();
        let key = key();
        let caps = Caps {
            max_connections: 8,
            max_dials: 1,
        };
        let Take::Dial(d1) = pools.take(&key, policy(true), caps).await else {
            panic!("dial")
        };
        assert_eq!(pools.dial_count(&key), 1);
        let waiter = pools.take(&key, policy(true), caps);
        tokio::pin!(waiter);
        assert!(pending(&mut waiter).await);
        let s1 = d1.connected();
        assert_eq!(pools.dial_count(&key), 0);
        let Take::Dial(d2) = waiter.await else {
            panic!("dial")
        };
        assert_eq!(pools.connection_count(&key), 2);
        drop((s1, d2));
    }

    /// A connection returned to a full sub-pool wakes a waiter, which reuses it.
    #[tokio::test]
    async fn a_returned_connection_wakes_a_waiter() {
        let pools = Pools::default();
        let key = key();
        let caps = Caps {
            max_connections: 1,
            max_dials: 1,
        };
        let (c, _far) = conn_in(&pools, &key, Duration::ZERO).await;
        let waiter = pools.take(&key, policy(true), caps);
        tokio::pin!(waiter);
        assert!(pending(&mut waiter).await);
        pools.checkin(key.clone(), c, 1);
        assert!(matches!(waiter.await, Take::Idle(_)));
    }

    /// Full, with only connections too idle for a non-idempotent request: the idlest is closed to
    /// make room, so such a request never waits behind connections it may not use.
    #[tokio::test]
    async fn a_full_pool_of_unusable_idle_connections_makes_room() {
        let pools = Pools::default();
        let key = key();
        let caps = Caps {
            max_connections: 2,
            max_dials: 2,
        };
        let (old, mut far_old) = conn_in(&pools, &key, Duration::from_secs(9)).await;
        let (newer, _far_new) = conn_in(&pools, &key, Duration::from_secs(5)).await;
        pools.return_unused(key.clone(), old, 2);
        pools.return_unused(key.clone(), newer, 2);
        let Take::Dial(d) = pools.take(&key, policy(false), caps).await else {
            panic!("room is made to dial")
        };
        assert_eq!(pools.connection_count(&key), 2);
        assert_eq!(pools.idle_count(&key), 1, "one idle connection was closed");
        // The idlest one (the bottom of the stack) went: its far end sees EOF.
        let mut b = [0u8; 1];
        let n = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::io::AsyncReadExt::read(&mut far_old, &mut b),
        )
        .await
        .expect("closed")
        .unwrap();
        assert_eq!(n, 0);
        // An idempotent request still reuses the one left.
        drop(d);
        assert!(matches!(
            pools.take(&key, policy(true), caps).await,
            Take::Idle(_)
        ));
    }

    /// Stale connections found on the way are closed, and their slots reused.
    #[tokio::test]
    async fn stale_connections_free_their_slots() {
        let pools = Pools::default();
        let key = key();
        let caps = Caps {
            max_connections: 1,
            max_dials: 1,
        };
        let (c, _far) = conn_in(&pools, &key, Duration::ZERO).await;
        let p = ReusePolicy {
            max_lifetime: Duration::ZERO, // every connection is past its lifetime
            ..policy(true)
        };
        pools.checkin(key.clone(), c, 1);
        assert!(matches!(pools.take(&key, p, caps).await, Take::Dial(_)));
        assert_eq!(pools.idle_count(&key), 0);
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
