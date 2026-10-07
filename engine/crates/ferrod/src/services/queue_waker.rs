//! Ferro Queue's **waker** (SPEC §24.8; M7-G3): waiting RESERVEs without holding connections.
//!
//! One [`Waker`] per enabled store owns that store's wait sets — a FIFO per queue — and its poll
//! schedule. A waiting RESERVE (`wait_ms > 0`) is a [`Waiter`]: a slot, a `Notify` and a deadline.
//! It holds **no connection, no pin and no permit** while it waits (§24.8's normative cost bound):
//! every statement is run BY THE WAKER, one per `(store, queue)` at a time (single flight), with a
//! `LIMIT k` covering every waiter it serves, on a checkout it takes for that statement alone.
//!
//! # The life of a waiter
//!
//! 1. **Register, then sweep (§24.8).** The handler registers the waiter in the FIFO of each of its
//!    queues BEFORE its first sweep, so a hint that fires meanwhile is never lost: it finds the
//!    waiter registered (busy or not) and the queue marked for another sweep.
//! 2. **Arrival: priority order.** A new waiter is eligible for ONE queue at a time, in the order it
//!    gave (`--queue=high,default`): its first sweep is of `high`, and only when that brought it
//!    nothing does it become eligible for `default`. A trigger for a queue whose only owed waiters
//!    are busy in another queue's sweep is KEPT and runs when that sweep frees one of them (review
//!    F3), so no hint is lost to a momentarily busy waiter. Each of those is an ordinary per-queue sweep
//!    (trigger `arrival`), coalesced with every other waiter eligible for that queue. After the last,
//!    the waiter is PARKED and eligible for all its queues: whichever of them yields first serves it —
//!    BLPOP's semantics, priority at the call and first-come while blocked (§22.2 (dl)).
//! 3. **Parked.** It is served by a sweep of one of its queues, triggered by a local wake hint
//!    (§24.8 trigger 1), by the coalesced poll every `POLL_MS` (trigger 2), or by a `refill` — a sweep
//!    that came back FULL is followed by one for the waiters it could not cover.
//! 4. **Its terminal.** The waker deposits an [`Offer`] in the slot — jobs, a failed sweep's
//!    classified terminal, or "nothing" — under the slot's lock, and only into a slot nobody has
//!    finished. The handler finishes the slot itself on its wait deadline, its grace bound, a client
//!    CANCEL, session teardown or the daemon's drain, under the same lock. So each waiter is finished
//!    EXACTLY ONCE, by whichever side gets there first, and a job the waker could not deposit is never
//!    lost: it is offered to another waiter of the queue, or UNRESERVED.
//!
//! # Deliver xor unreserve (§24.4, chaos row 12)
//!
//! A reserved job ends in exactly one of three places: a slot the handler then hands to a LIVE
//! session's writer (`Liveness::hand_off`), another waiter's slot, or the fenced unreserve. The slot's
//! lock decides between the first two and the third; the session's liveness lock decides the hand-off;
//! and a hand-off that finds the session tearing down unreserves the jobs itself. No path does both.
//!
//! # The wait bound (§24.8, normative)
//!
//! A waiter's terminal is due by `wait_end + queue_wait_grace_ms` (`hard_end`), and by the request's
//! own `timeout_ms` deadline when it has one. The waker starts no statement for a waiter past its
//! `wait_end` (such a waiter is answered `Ok{jobs: []}` by its handler); a statement still running at a
//! waiter's `hard_end` does not hold the answer — the handler answers `Ok{jobs: []}`, and whatever the
//! statement brings back for it is offered on or unreserved (cause `deadline`).
//!
//! # Never a retry (§24.2 I4, charter rule 3)
//!
//! A sweep after an EMPTY sweep is a poll, not a retry. A FAILED sweep ends every waiter it served
//! with its classified terminal and every other idle waiter of that queue with `Ok{jobs: []}`; nothing
//! is re-sent, and they re-ask. A failed unreserve is counted and left to the job's `lease_deadline`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use ferro_proto::consts::branch;
use ferro_proto::messages::{ErrorPayload, QueueStats};
use ferro_queue::checks;
use ferro_queue::config::StoreConfig;
use ferro_queue::ident::TableName;
use ferro_queue::pg::{self as pgq, Reserved};
use ferro_queue::shape::Statement;

use super::fate::{self, OpContext};
use super::queue_metrics::{HintSource, QueueMetrics, Trigger, UnreserveCause};
use crate::session::Liveness;
use crate::shutdown::Drain;

/// What a [`Runner`] reports: the statement's result with the checkout's `queue_us` and the
/// statement's `exec_us`, or the error with whether the statement was SENT.
pub type RunResult = Result<(QueryResult, u64, u64), (PoolError, bool)>;

/// Run ONE engine-authored statement autocommit on the store's pool: take a checkout, run the
/// statement through the guarded, interruptible path, release the checkout — everything bounded by
/// the `Duration` (the store's `WAKER_STMT_TIMEOUT_MS`). A function rather than a pool so the waker is
/// independent of the backend type, and so its scheduling is unit-testable without a database.
pub type Runner = Arc<dyn Fn(Statement, Duration) -> BoxFuture<'static, RunResult> + Send + Sync>;

/// What a waiter's sweep left in its slot.
#[derive(Debug)]
pub enum Offer {
    /// Jobs reserved for it (non-empty, at most its clamp, FIFO by id).
    Jobs(Vec<Reserved>),
    /// The sweep serving it failed: its classified terminal (§24.8, "a failed poll ends its waiters'
    /// requests").
    Failed(ErrorPayload),
    /// A sweep of one of its queues failed while it was not being served: `Ok{jobs: []}`.
    Empty,
}

/// How the HANDLER finished a waiter (or that the waker did, by depositing an offer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// The waker deposited an [`Offer`].
    Offered,
    /// Wait or grace bound passed, or the request's deadline.
    Deadline,
    /// A client CANCEL on a live session.
    Cancelled,
    /// Session teardown.
    Teardown,
    /// The daemon's drain.
    Drained,
}

impl Ended {
    fn unreserve_cause(self) -> UnreserveCause {
        match self {
            Ended::Cancelled => UnreserveCause::Cancel,
            Ended::Teardown => UnreserveCause::Teardown,
            Ended::Offered | Ended::Deadline | Ended::Drained => UnreserveCause::Deadline,
        }
    }
}

/// What [`Waiter::wait`] returns to the handler.
#[derive(Debug)]
pub enum WaitOutcome {
    Offer(Offer),
    /// Nothing arrived by the wait (or grace, or request) deadline.
    Expired,
    /// A client CANCEL, before any sweep deposited jobs (§24.4: `Cancelled`).
    Cancelled,
    /// Session teardown.
    Teardown,
    /// The daemon is draining: `Ok{jobs: []}` (§24.8 "Drain").
    Drained,
}

#[derive(Debug)]
struct Slot {
    /// Index of the queue this waiter is still on its ARRIVAL sweep of; `queues.len()` once parked.
    arrival: usize,
    /// A sweep that includes it is in flight.
    busy: bool,
    /// Finished — by an offer or by its handler. Never cleared.
    ended: Option<Ended>,
    offer: Option<Offer>,
    /// The statements that served it: summed into its terminal's `stats`.
    stats: QueueStats,
}

/// One waiting RESERVE.
#[derive(Debug)]
pub struct Waiter {
    id: u64,
    /// Priority order, duplicates removed.
    queues: Vec<String>,
    /// Its `max_jobs`, clamped so its reply fits one frame (`checks::reserve_limit`).
    clamp: u16,
    wait_end: Instant,
    slot: Mutex<Slot>,
    notify: Notify,
}

impl Waiter {
    fn lock(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Eligible for a sweep of `queue` started now: not finished, not already in a sweep, inside its
    /// wait, and — while arriving — this is the queue its priority order has reached.
    fn eligible(&self, s: &Slot, queue: &str, now: Instant) -> bool {
        !s.busy && self.owed(s, queue, now)
    }

    /// Would be eligible for `queue` but for a sweep of ANOTHER queue serving it right now (review
    /// F3): such a waiter is still owed this queue's sweep the moment that one returns.
    fn owed(&self, s: &Slot, queue: &str, now: Instant) -> bool {
        s.ended.is_none()
            && now < self.wait_end
            && (s.arrival >= self.queues.len() || self.queues[s.arrival] == queue)
    }

    /// Deposit an offer iff nobody has finished this waiter. Returns the offer back otherwise.
    fn offer(&self, s: &mut Slot, offer: Offer) -> Result<(), Offer> {
        if s.ended.is_some() {
            return Err(offer);
        }
        s.ended = Some(Ended::Offered);
        s.offer = Some(offer);
        self.notify.notify_one();
        Ok(())
    }

    /// The statements that served this waiter so far.
    pub fn stats(&self) -> QueueStats {
        self.lock().stats
    }

    /// Wait for this waiter's outcome (see the module doc). `hard_end` is the wait bound plus grace,
    /// capped by the request's deadline. `cancel` fires on a client CANCEL AND on session teardown;
    /// `liveness` tells them apart (teardown ends it first).
    pub async fn wait(
        &self,
        hard_end: Instant,
        cancel: &CancellationToken,
        drain: &Drain,
        liveness: &Liveness,
    ) -> WaitOutcome {
        loop {
            let busy = {
                let mut s = self.lock();
                if let Some(offer) = s.offer.take() {
                    return WaitOutcome::Offer(offer);
                }
                let now = Instant::now();
                let finish = if cancel.is_cancelled() {
                    Some(if liveness.is_live() {
                        (Ended::Cancelled, WaitOutcome::Cancelled)
                    } else {
                        (Ended::Teardown, WaitOutcome::Teardown)
                    })
                } else if now >= hard_end || (!s.busy && now >= self.wait_end) {
                    Some((Ended::Deadline, WaitOutcome::Expired))
                } else if !s.busy && drain.is_draining() {
                    Some((Ended::Drained, WaitOutcome::Drained))
                } else {
                    None
                };
                if let Some((ended, outcome)) = finish {
                    // `ended` is set only by an offer (taken above) or here, so it is still None.
                    s.ended = Some(ended);
                    return outcome;
                }
                s.busy
            };
            // While a sweep serves it, only its offer, the grace bound or a cancel end the wait: a
            // drain lets that statement finish (its jobs are delivered, not released).
            let until = if busy {
                hard_end
            } else {
                hard_end.min(self.wait_end)
            };
            tokio::select! {
                () = self.notify.notified() => {}
                () = tokio::time::sleep_until(until) => {}
                () = cancel.cancelled() => {}
                () = drain.wait(), if !busy => {}
            }
        }
    }
}

/// Unregisters its waiter on drop, whatever path the handler leaves by.
pub struct WaitGuard {
    waker: Arc<Waker>,
    waiter: Arc<Waiter>,
}

impl std::ops::Deref for WaitGuard {
    type Target = Waiter;
    fn deref(&self) -> &Waiter {
        &self.waiter
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.waker.deregister(self.waiter.id);
    }
}

#[derive(Debug, Default)]
struct QueueState {
    fifo: VecDeque<u64>,
    in_flight: bool,
    /// A trigger that arrived while a sweep was in flight: ONE follow-up sweep (coalesced).
    pending: Option<Trigger>,
}

#[derive(Debug, Default)]
struct State {
    queues: HashMap<String, QueueState>,
    waiters: HashMap<u64, Arc<Waiter>>,
}

/// What the waker needs to run statements, bound at a store's first verified use.
struct Bound {
    runner: Runner,
    table: TableName,
}

/// One store's waker.
pub struct Waker {
    store: String,
    lease_s: u32,
    poll: Duration,
    stmt_timeout: Duration,
    max_payload_bytes: u32,
    /// The most jobs one sweep reserves (`checks::sweep_cap`).
    cap: u16,
    metrics: Arc<QueueMetrics>,
    bound: OnceLock<Bound>,
    state: Mutex<State>,
    next_id: AtomicU64,
    /// Sweep and unreserve statements spawned and not yet finished (diagnostics: [`Waker::settled`]).
    outstanding: AtomicU64,
    ticker: OnceLock<()>,
    weak: Weak<Waker>,
}

impl std::fmt::Debug for Waker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Waker")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl Waker {
    pub fn new(store: &StoreConfig, metrics: Arc<QueueMetrics>) -> Arc<Waker> {
        Arc::new_cyclic(|weak| Waker {
            store: store.name.clone(),
            lease_s: store.lease_s,
            poll: Duration::from_millis(u64::from(store.poll_ms.max(1))),
            stmt_timeout: Duration::from_millis(u64::from(store.waker_stmt_timeout_ms.max(1))),
            max_payload_bytes: store.max_payload_bytes,
            cap: checks::sweep_cap(store.max_payload_bytes),
            metrics,
            bound: OnceLock::new(),
            state: Mutex::new(State::default()),
            next_id: AtomicU64::new(1),
            outstanding: AtomicU64::new(0),
            ticker: OnceLock::new(),
            weak: weak.clone(),
        })
    }

    /// Bind the store's statement runner and verified table, once per process (the table is fixed
    /// at verification, §24.3). Later calls are no-ops.
    pub fn bind(&self, table: &TableName, runner: impl FnOnce() -> Runner) {
        let _ = self.bound.get_or_init(|| Bound {
            runner: runner(),
            table: table.clone(),
        });
    }

    pub fn metrics(&self) -> &Arc<QueueMetrics> {
        &self.metrics
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn arc(&self) -> Option<Arc<Waker>> {
        self.weak.upgrade()
    }

    /// How many waiters are registered now.
    pub fn waiters(&self) -> usize {
        self.lock().waiters.len()
    }

    /// Registered waiters that are parked right now — in no sweep, not finished (diagnostics; what a
    /// test waits for before it fires a hint, instead of sleeping).
    /// No sweep in flight on any queue and no unreserve statement outstanding: every reservation
    /// this waker made has reached its end — a slot, or the rows (diagnostics; what a test waits for
    /// before it reads the rows back, since a row read while a sweep is still uncommitted shows the
    /// pre-reservation state).
    pub fn settled(&self) -> bool {
        // A sweep task counts until `complete` has handed out its jobs and spawned (and counted) any
        // unreserve, so there is no instant at which a reservation is in neither count.
        self.outstanding.load(Ordering::SeqCst) == 0
    }

    pub fn idle_waiters(&self) -> usize {
        let st = self.lock();
        st.waiters
            .values()
            .filter(|w| {
                let s = w.lock();
                s.ended.is_none() && !s.busy
            })
            .count()
    }

    /// Register a waiting RESERVE (§24.8 "register, then sweep") and trigger its first sweep.
    pub fn register(&self, queues: &[String], clamp: u16, wait_end: Instant) -> Option<WaitGuard> {
        let me = self.arc()?;
        self.ensure_ticker();
        let mut ordered: Vec<String> = Vec::with_capacity(queues.len());
        for q in queues {
            if !ordered.contains(q) {
                ordered.push(q.clone());
            }
        }
        let waiter = Arc::new(Waiter {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            queues: ordered,
            clamp: clamp.max(1),
            wait_end,
            slot: Mutex::new(Slot {
                arrival: 0,
                busy: false,
                ended: None,
                offer: None,
                stats: QueueStats::default(),
            }),
            notify: Notify::new(),
        });
        let mut st = self.lock();
        for q in &waiter.queues {
            st.queues
                .entry(q.clone())
                .or_default()
                .fifo
                .push_back(waiter.id);
        }
        st.waiters.insert(waiter.id, Arc::clone(&waiter));
        self.metrics.set_waiters(st.waiters.len());
        let first = waiter.queues.first().cloned();
        if let Some(q) = first {
            self.trigger_locked(&mut st, &q, Trigger::Arrival);
        }
        drop(st);
        Some(WaitGuard { waker: me, waiter })
    }

    fn deregister(&self, id: u64) {
        let mut st = self.lock();
        if let Some(w) = st.waiters.remove(&id) {
            for q in &w.queues {
                if let Some(qs) = st.queues.get_mut(q) {
                    qs.fifo.retain(|&x| x != id);
                }
            }
        }
        // A queue with no waiters and nothing in flight is forgotten (its state is rebuilt on use).
        st.queues
            .retain(|_, qs| !qs.fifo.is_empty() || qs.in_flight);
        self.metrics.set_waiters(st.waiters.len());
    }

    /// A local wake hint for `queue` (§24.8 trigger 1). Counted whatever it finds; it sweeps only if a
    /// waiter is eligible for the queue. A hint is never correctness (§24.5).
    pub fn hint(&self, queue: &str, source: HintSource) {
        self.metrics.hint(source);
        let mut st = self.lock();
        self.trigger_locked(&mut st, queue, Trigger::Hint);
    }

    /// Sweep `queue` now if nothing is in flight for it, else remember ONE follow-up.
    fn trigger_locked(&self, st: &mut State, queue: &str, trigger: Trigger) {
        let Some(qs) = st.queues.get_mut(queue) else {
            return; // nobody waits on it
        };
        if qs.in_flight {
            qs.pending.get_or_insert(trigger);
            return;
        }
        self.start_sweep_locked(st, queue, trigger);
    }

    /// Select the batch — the longest FIFO prefix of eligible waiters whose summed clamp fits
    /// [`Waker::cap`], at least one — mark it busy, and spawn the ONE statement that serves it.
    fn start_sweep_locked(&self, st: &mut State, queue: &str, trigger: Trigger) {
        let Some(bound) = self.bound.get() else {
            return;
        };
        let Some(me) = self.arc() else { return };
        let now = Instant::now();
        let State { queues, waiters } = st;
        let Some(qs) = queues.get_mut(queue) else {
            return;
        };
        qs.pending = None;
        let mut batch: Vec<Arc<Waiter>> = Vec::new();
        let mut k: u32 = 0;
        let mut owed = false;
        for id in &qs.fifo {
            let Some(w) = waiters.get(id) else { continue };
            let mut s = w.lock();
            if !w.eligible(&s, queue, now) {
                owed |= s.busy && w.owed(&s, queue, now);
                continue;
            }
            if !batch.is_empty() && k + u32::from(w.clamp) > u32::from(self.cap) {
                break;
            }
            s.busy = true;
            k += u32::from(w.clamp);
            batch.push(Arc::clone(w));
        }
        if batch.is_empty() {
            // Review F3: the queue's only owed waiters are busy in another queue's sweep. Keep the
            // trigger: that sweep's completion runs it as soon as one of them is free, so a hint is
            // never lost to a waiter that was momentarily busy.
            if owed {
                qs.pending = Some(trigger);
            }
            return;
        }
        qs.in_flight = true;
        self.metrics.poll(trigger);
        let k = u16::try_from(k).unwrap_or(u16::MAX);
        let stmt = pgq::reserve(&bound.table, queue, self.lease_s, self.max_payload_bytes, k);
        let runner = Arc::clone(&bound.runner);
        let timeout = self.stmt_timeout;
        let queue = queue.to_string();
        self.outstanding.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _done = Settle(&me.outstanding);
            let outcome = sweep_outcome(runner(stmt, timeout).await, k);
            me.complete(&queue, batch, k, outcome);
        });
    }

    /// A sweep's statement returned: hand its jobs out in FIFO order (§24.8 "Serving"), or end its
    /// waiters with its failure; then run the one follow-up a trigger left pending.
    fn complete(
        &self,
        queue: &str,
        batch: Vec<Arc<Waiter>>,
        k: u16,
        outcome: Result<(Vec<Reserved>, QueueStats), ErrorPayload>,
    ) {
        let now = Instant::now();
        let mut st = self.lock();
        let mut next_arrivals: Vec<String> = Vec::new();
        // Queues a waiter freed by this sweep is owed a deferred trigger on (review F3).
        let mut freed: Vec<String> = Vec::new();
        let mut leftovers: Vec<Reserved> = Vec::new();
        let mut cause = None;
        match outcome {
            Ok((jobs, stats)) => {
                let full = jobs.len() >= usize::from(k);
                let mut jobs: VecDeque<Reserved> = jobs.into();
                for w in &batch {
                    let mut s = w.lock();
                    s.busy = false;
                    s.stats.queue_us += stats.queue_us;
                    s.stats.exec_us += stats.exec_us;
                    if let Some(e) = s.ended {
                        cause.get_or_insert(e);
                        continue;
                    }
                    let n = usize::from(w.clamp).min(jobs.len());
                    if n > 0 {
                        let mine: Vec<Reserved> = jobs.drain(..n).collect();
                        // `ended` is None, so the deposit cannot be refused.
                        let _ = w.offer(&mut s, Offer::Jobs(mine));
                        continue;
                    }
                    if s.arrival < w.queues.len() && w.queues[s.arrival] == queue {
                        s.arrival += 1;
                        if let Some(q) = w.queues.get(s.arrival) {
                            next_arrivals.push(q.clone());
                        }
                    }
                    freed.extend(w.queues.iter().filter(|q| q.as_str() != queue).cloned());
                    // Wake it: past its wait it now finishes; otherwise it keeps waiting.
                    w.notify.notify_one();
                }
                // Jobs a finished waiter could not take go to other waiters of this queue first.
                if !jobs.is_empty()
                    && let Some(qs) = st.queues.get(queue)
                {
                    for id in &qs.fifo {
                        if jobs.is_empty() {
                            break;
                        }
                        let Some(w) = st.waiters.get(id) else {
                            continue;
                        };
                        let mut s = w.lock();
                        if !w.eligible(&s, queue, now) {
                            continue;
                        }
                        let n = usize::from(w.clamp).min(jobs.len());
                        let mine: Vec<Reserved> = jobs.drain(..n).collect();
                        let _ = w.offer(&mut s, Offer::Jobs(mine));
                    }
                }
                leftovers.extend(jobs);
                if let Some(qs) = st.queues.get_mut(queue) {
                    qs.in_flight = false;
                    if full {
                        qs.pending.get_or_insert(Trigger::Refill);
                    }
                }
            }
            Err(ep) => {
                if ep.branch == branch::INDETERMINATE {
                    self.metrics.reserve_unconfirmed();
                }
                for w in &batch {
                    let mut s = w.lock();
                    s.busy = false;
                    let _ = w.offer(&mut s, Offer::Failed(ep.clone()));
                    w.notify.notify_one();
                }
                let State { queues, waiters } = &mut *st;
                if let Some(qs) = queues.get_mut(queue) {
                    qs.in_flight = false;
                    // A failed poll is not retried (I4): its queue's other idle waiters re-ask too.
                    qs.pending = None;
                    for id in &qs.fifo {
                        let Some(w) = waiters.get(id) else { continue };
                        let mut s = w.lock();
                        if !s.busy {
                            let _ = w.offer(&mut s, Offer::Empty);
                        }
                    }
                }
            }
        }
        if let Some(t) = st.queues.get_mut(queue).and_then(|qs| qs.pending.take()) {
            self.start_sweep_locked(&mut st, queue, t);
        }
        for q in next_arrivals {
            self.trigger_locked(&mut st, &q, Trigger::Arrival);
        }
        freed.sort();
        freed.dedup();
        for q in freed {
            let deferred = st
                .queues
                .get_mut(&q)
                .filter(|qs| !qs.in_flight)
                .and_then(|qs| qs.pending.take());
            if let Some(t) = deferred {
                self.start_sweep_locked(&mut st, &q, t);
            }
        }
        if st
            .queues
            .get(queue)
            .is_some_and(|qs| qs.fifo.is_empty() && !qs.in_flight)
        {
            st.queues.remove(queue);
        }
        drop(st);
        if !leftovers.is_empty() {
            let cause = cause.map_or(UnreserveCause::Deadline, Ended::unreserve_cause);
            self.unreserve(leftovers, cause);
        }
    }

    /// Unreserve jobs no live session received (§24.8 "Unreserve"): ONE fenced statement, never
    /// re-sent, counted either way; each restored job's queue is woken (§24.8 trigger 1). Spawned:
    /// the caller never waits for it.
    pub fn unreserve(&self, jobs: Vec<Reserved>, cause: UnreserveCause) {
        if jobs.is_empty() {
            return;
        }
        let (Some(bound), Some(me)) = (self.bound.get(), self.arc()) else {
            self.metrics.unreserve_failed(jobs.len() as u64);
            return;
        };
        let keys: Vec<pgq::Unreserve> = jobs.iter().map(|j| j.unreserve(self.lease_s)).collect();
        let stmt = pgq::unreserve(&bound.table, &keys);
        let runner = Arc::clone(&bound.runner);
        let timeout = self.stmt_timeout;
        let n = keys.len();
        self.outstanding.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _done = Settle(&me.outstanding);
            match runner(stmt, timeout).await {
                Ok((qr, _, _)) => match pgq::decode_unreserve(&qr.rows, n) {
                    Ok(restored) => {
                        me.metrics.unreserved(cause, restored.len() as u64);
                        let mut queues: Vec<String> =
                            restored.into_iter().map(|(_, q)| q).collect();
                        queues.sort();
                        queues.dedup();
                        for q in queues {
                            me.hint(&q, HintSource::Unreserve);
                        }
                    }
                    Err(pgq::Malformed) => {
                        tracing::warn!(store = %me.store, jobs = n, "ferrod: queue unreserve returned an unexpected result; the jobs wait for their lease");
                        me.metrics.unreserve_failed(n as u64);
                    }
                },
                Err((e, _)) => {
                    tracing::warn!(store = %me.store, jobs = n, error = %crate::slow_log::error_label(&e), "ferrod: queue unreserve failed; the jobs wait for their lease (not retried)");
                    me.metrics.unreserve_failed(n as u64);
                }
            }
        });
    }

    /// Start the store's coalesced poll (§24.8 trigger 2): every `POLL_MS`, one sweep for each queue
    /// with an eligible waiter and nothing in flight. Lives as long as the waker.
    fn ensure_ticker(&self) {
        self.ticker.get_or_init(|| {
            let weak = self.weak.clone();
            let poll = self.poll;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(poll);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let Some(w) = weak.upgrade() else { return };
                    w.poll_all();
                }
            });
        });
    }

    fn poll_all(&self) {
        let now = Instant::now();
        let mut st = self.lock();
        let due: Vec<String> = st
            .queues
            .iter()
            .filter(|(q, qs)| {
                !qs.in_flight
                    && qs.fifo.iter().any(|id| {
                        st.waiters
                            .get(id)
                            .is_some_and(|w| w.eligible(&w.lock(), q, now))
                    })
            })
            .map(|(q, _)| q.clone())
            .collect();
        for q in due {
            self.start_sweep_locked(&mut st, &q, Trigger::Interval);
        }
    }
}

/// Decrements an outstanding-work counter when dropped (the unreserve task ends however it ends).
struct Settle<'a>(&'a AtomicU64);

impl Drop for Settle<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A sweep statement's result as the waker uses it: its jobs and its stats, or its classified
/// terminal (§24.6's `OpContext` for an autocommit RESERVE: a write, `sent` honest). A result that
/// does not have the statement's shape RAN, so it is `Indeterminate`, as an autocommit RESERVE's is.
fn sweep_outcome(r: RunResult, k: u16) -> Result<(Vec<Reserved>, QueueStats), ErrorPayload> {
    match r {
        Ok((qr, queue_us, exec_us)) => match pgq::decode_reserve(&qr.rows, k) {
            Ok(jobs) => Ok((jobs, QueueStats { queue_us, exec_us })),
            Err(pgq::Malformed) => Err(ErrorPayload {
                code: ferro_proto::consts::errc::WRITE_UNCONFIRMED,
                branch: ferro_proto::consts::errc::WRITE_UNCONFIRMED_BRANCH,
                sqlstate: None,
                errno: None,
                message: "the waker's RESERVE statement ran but its result did not have the \
                          expected shape (was the table altered after it was verified? ferrod \
                          re-verifies at restart); its effect is unconfirmed"
                    .to_string(),
                detail: None,
                retry_after_ms: None,
            }),
        },
        Err((e, sent)) => Err(fate::classify_fate(
            e,
            OpContext {
                readonly: false,
                sent,
                in_tx: false,
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::value::Value;
    use futures::FutureExt;
    use std::ffi::OsString;
    use std::sync::atomic::AtomicUsize;

    fn store(poll_ms: &str) -> StoreConfig {
        let cfg = ferro_queue::config::QueueConfig::load(
            [
                ("FERRO_QUEUE_STORES", "jobs"),
                ("FERRO_QUEUE_JOBS_POOL", "main"),
                ("FERRO_QUEUE_JOBS_POLL_MS", poll_ms),
                ("FERRO_QUEUE_JOBS_LEASE_S", "30"),
                ("FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES", "65536"),
            ]
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &|_| Some(ferro_queue::PoolFamily::Postgres),
        );
        cfg.store("jobs").cloned().unwrap()
    }

    /// A fake backend: a queue of jobs per queue name, and a log of every statement.
    #[derive(Default)]
    struct Fake {
        jobs: Mutex<HashMap<String, VecDeque<i64>>>,
        statements: Mutex<Vec<String>>,
        reserves: AtomicUsize,
        unreserved: Mutex<Vec<i64>>,
        /// Hold every RESERVE until notified.
        gate: Option<Arc<Notify>>,
        fail: Mutex<Option<PoolError>>,
    }

    fn row(id: i64, queue: &str) -> Vec<Value> {
        vec![
            Value::I64(id),
            Value::I64(1),
            Value::I64(100),
            Value::Text(queue.into()),
            Value::Text("p".into()),
            Value::I64(1_031),
        ]
    }

    fn runner(fake: Arc<Fake>) -> Runner {
        Arc::new(move |stmt: Statement, _t: Duration| {
            let fake = Arc::clone(&fake);
            async move {
                fake.statements.lock().unwrap().push(stmt.sql.clone());
                if stmt.sql.starts_with("UPDATE") {
                    let ids: Vec<i64> = stmt
                        .params
                        .chunks(4)
                        .map(|c| match c[0] {
                            Value::I64(n) => n,
                            _ => 0,
                        })
                        .collect();
                    fake.unreserved.lock().unwrap().extend(&ids);
                    let rows = ids
                        .iter()
                        .map(|&id| vec![Value::I64(id), Value::Text("q".into())])
                        .collect();
                    return Ok((
                        QueryResult {
                            rows,
                            ..QueryResult::default()
                        },
                        0,
                        0,
                    ));
                }
                fake.reserves.fetch_add(1, Ordering::SeqCst);
                if let Some(g) = &fake.gate {
                    g.notified().await;
                }
                if let Some(e) = fake.fail.lock().unwrap().take() {
                    return Err((e, true));
                }
                let queue = match &stmt.params[0] {
                    Value::Text(q) => q.clone(),
                    _ => unreachable!(),
                };
                let k = match stmt.params[4] {
                    Value::I64(k) => k as usize,
                    _ => unreachable!(),
                };
                let mut all = fake.jobs.lock().unwrap();
                let list = all.entry(queue.clone()).or_default();
                let take = k.min(list.len());
                let rows = list.drain(..take).map(|id| row(id, &queue)).collect();
                Ok((
                    QueryResult {
                        rows,
                        ..QueryResult::default()
                    },
                    3,
                    7,
                ))
            }
            .boxed()
        })
    }

    fn waker(fake: &Arc<Fake>, poll_ms: &str) -> Arc<Waker> {
        let s = store(poll_ms);
        let w = Waker::new(&s, Arc::new(QueueMetrics::new("jobs", &[])));
        w.bind(&s.table, || runner(Arc::clone(fake)));
        w
    }

    fn put(fake: &Fake, queue: &str, ids: &[i64]) {
        fake.jobs
            .lock()
            .unwrap()
            .entry(queue.into())
            .or_default()
            .extend(ids);
    }

    fn qs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// A waiter's outcome with its grace bound `ms` from now. Bounded on the TEST's side too, so a
    /// handler that waited on the waker past its bound fails here instead of hanging the suite.
    async fn wait(g: &WaitGuard, ms: u64) -> WaitOutcome {
        tokio::time::timeout(
            Duration::from_millis(ms) + Duration::from_secs(5),
            g.wait(
                Instant::now() + Duration::from_millis(ms),
                &CancellationToken::new(),
                &Drain::new(),
                &Liveness::new(),
            ),
        )
        .await
        .expect("the handler answers by its own bound, never waiting on the waker")
    }

    fn ids(o: &WaitOutcome) -> Vec<i64> {
        match o {
            WaitOutcome::Offer(Offer::Jobs(j)) => j.iter().map(|r| r.id.0).collect(),
            _ => Vec::new(),
        }
    }

    /// A waiter whose queue already holds a job is served by its ARRIVAL sweep.
    #[tokio::test]
    async fn an_arrival_sweep_serves_an_available_job() {
        let fake = Arc::new(Fake::default());
        put(&fake, "default", &[5]);
        let w = waker(&fake, "60000");
        let g = w
            .register(
                &qs(&["default"]),
                1,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        let o = wait(&g, 6_000).await;
        assert_eq!(ids(&o), vec![5]);
        assert_eq!(w.metrics().polls(Trigger::Arrival), 1);
        assert_eq!(g.stats().exec_us, 7);
    }

    /// §24.8 (normative): "the waker starts no statement for a waiter past its `wait_ms`". A waiter
    /// whose wait has already ended when its sweep would start costs no statement; it is answered
    /// empty by its own timer.
    #[tokio::test]
    async fn a_waiter_past_its_wait_starts_no_statement() {
        let fake = Arc::new(Fake::default());
        put(&fake, "default", &[1]);
        let w = waker(&fake, "60000");
        let g = w.register(&qs(&["default"]), 1, Instant::now()).unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            fake.reserves.load(Ordering::SeqCst),
            0,
            "no statement for it"
        );
        assert!(matches!(wait(&g, 1_000).await, WaitOutcome::Expired));
        assert_eq!(
            fake.jobs.lock().unwrap()["default"].len(),
            1,
            "nothing reserved"
        );
    }

    /// Review F3: a hint for a queue whose only owed waiter is busy in ANOTHER queue's sweep is kept,
    /// and runs the moment that sweep frees the waiter. X waits on `a`; W on `[a, b]`. A sweep of `a`
    /// serves both and brings one job (X's); meanwhile `b` gets a job and a hint that finds W busy. W
    /// must get `b`'s job from the deferred sweep, not wait for the ten-minute poll.
    #[tokio::test]
    async fn a_hint_for_a_busy_waiters_other_queue_is_kept() {
        let gate = Arc::new(Notify::new());
        let fake = Arc::new(Fake {
            gate: Some(Arc::clone(&gate)),
            ..Fake::default()
        });
        let w = waker(&fake, "600000");
        let far = Instant::now() + Duration::from_secs(5);
        let step = |n: usize| {
            let fake = Arc::clone(&fake);
            let gate = Arc::clone(&gate);
            async move {
                while fake.reserves.load(Ordering::SeqCst) < n {
                    tokio::task::yield_now().await;
                }
                gate.notify_one();
            }
        };
        let x = w.register(&qs(&["a"]), 1, far).unwrap();
        step(1).await; // X's arrival sweep of `a`: empty
        let wb = w.register(&qs(&["a", "b"]), 1, far).unwrap();
        step(2).await; // W's arrival sweep of `a`: empty
        step(3).await; // W's arrival sweep of `b`: empty — W is parked
        while w.idle_waiters() < 2 {
            tokio::task::yield_now().await;
        }
        put(&fake, "a", &[1]);
        w.hint("a", HintSource::Autocommit); // sweep 4: `a` for X and W, held
        while fake.reserves.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        put(&fake, "b", &[2]);
        w.hint("b", HintSource::Autocommit); // W is busy in `a`: kept, not lost
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            fake.reserves.load(Ordering::SeqCst),
            4,
            "nothing to sweep `b` for yet"
        );
        gate.notify_one(); // `a` returns job 1 → X; W is freed → the kept `b` sweep starts
        assert_eq!(ids(&wait(&x, 5_000).await), vec![1]);
        step(5).await;
        assert_eq!(
            ids(&wait(&wb, 5_000).await),
            vec![2],
            "the kept hint served W"
        );
    }

    /// Priority at arrival holds against a HINT too: a waiter still waiting for its first sweep of
    /// `high` (one is in flight for another waiter) is not eligible for `default`, so a hint for
    /// `default` starts nothing, and the waiter gets `high`'s second job, not `default`'s.
    #[tokio::test]
    async fn a_hint_for_a_lower_queue_waits_for_the_arrival_sweep_of_a_higher_one() {
        let gate = Arc::new(Notify::new());
        let fake = Arc::new(Fake {
            gate: Some(Arc::clone(&gate)),
            ..Fake::default()
        });
        put(&fake, "high", &[1, 2]);
        let w = waker(&fake, "60000");
        let far = Instant::now() + Duration::from_secs(5);
        let a = w.register(&qs(&["high"]), 1, far).unwrap();
        while fake.reserves.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let b = w.register(&qs(&["high", "default"]), 1, far).unwrap();
        put(&fake, "default", &[9]);
        w.hint("default", HintSource::Autocommit);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            fake.reserves.load(Ordering::SeqCst),
            1,
            "B is still owed its sweep of `high`: the `default` hint sweeps nobody"
        );
        gate.notify_one(); // A's sweep: job 1
        assert_eq!(ids(&wait(&a, 5_000).await), vec![1]);
        while fake.reserves.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        gate.notify_one(); // B's arrival sweep of `high`: job 2
        assert_eq!(ids(&wait(&b, 5_000).await), vec![2], "high before default");
    }

    /// Priority at arrival: `high` is swept before `default`, and `default` only when `high`
    /// brought nothing.
    #[tokio::test]
    async fn arrival_follows_the_queues_priority_order() {
        let fake = Arc::new(Fake::default());
        put(&fake, "default", &[1]);
        put(&fake, "high", &[9]);
        let w = waker(&fake, "60000");
        let g = w
            .register(
                &qs(&["high", "default"]),
                1,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(ids(&wait(&g, 6_000).await), vec![9]);
        drop(g);
        let g = w
            .register(
                &qs(&["high", "default"]),
                1,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(
            ids(&wait(&g, 6_000).await),
            vec![1],
            "high empty: default next"
        );
        assert_eq!(fake.reserves.load(Ordering::SeqCst), 3);
    }

    /// A parked waiter costs nothing until a hint, and a hint for its queue serves it.
    #[tokio::test]
    async fn a_hint_serves_a_parked_waiter() {
        let fake = Arc::new(Fake::default());
        let w = waker(&fake, "60000");
        let g = w
            .register(
                &qs(&["default"]),
                1,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        // Let the arrival sweep come back empty.
        while w.metrics().polls(Trigger::Arrival) == 0 || g.lock().busy {
            tokio::task::yield_now().await;
        }
        let before = fake.reserves.load(Ordering::SeqCst);
        w.hint("other", HintSource::Autocommit);
        assert_eq!(
            fake.reserves.load(Ordering::SeqCst),
            before,
            "nobody waits on `other`"
        );
        put(&fake, "default", &[4]);
        w.hint("default", HintSource::Autocommit);
        assert_eq!(ids(&wait(&g, 6_000).await), vec![4]);
        assert_eq!(w.metrics().polls(Trigger::Hint), 1);
        assert_eq!(w.metrics().hints(HintSource::Autocommit), 2);
    }

    /// Coalescing: while a sweep is in flight, any number of triggers leave ONE follow-up.
    #[tokio::test]
    async fn triggers_during_a_sweep_coalesce_into_one_follow_up() {
        let gate = Arc::new(Notify::new());
        let fake = Arc::new(Fake {
            gate: Some(Arc::clone(&gate)),
            ..Fake::default()
        });
        let w = waker(&fake, "60000");
        let far = Instant::now() + Duration::from_secs(5);
        let mut guards = Vec::new();
        for _ in 0..50 {
            guards.push(w.register(&qs(&["default"]), 1, far).unwrap());
        }
        for _ in 0..20 {
            w.hint("default", HintSource::Autocommit);
        }
        while fake.reserves.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(fake.reserves.load(Ordering::SeqCst), 1, "single flight");
        put(&fake, "default", &[1, 2, 3]);
        gate.notify_one(); // the first sweep: it served the FIRST waiter only (registered alone)
        while fake.reserves.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        gate.notify_one(); // the one follow-up: every other waiter, k = 49
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            fake.reserves.load(Ordering::SeqCst),
            2,
            "one follow-up, not 69"
        );
        let mut got = Vec::new();
        for g in &guards {
            if let Some(Offer::Jobs(j)) = g.lock().offer.take() {
                got.extend(j.iter().map(|r| r.id.0));
            }
        }
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3], "each job to exactly one waiter");
    }

    /// The wait bound: a waiter parked past its wait is answered empty, and a statement still
    /// running at its grace bound does not hold the answer — the jobs it brings back for that waiter
    /// are UNRESERVED (deadline), never delivered.
    #[tokio::test]
    async fn past_the_grace_bound_the_jobs_are_unreserved_not_delivered() {
        let gate = Arc::new(Notify::new());
        let fake = Arc::new(Fake {
            gate: Some(Arc::clone(&gate)),
            ..Fake::default()
        });
        put(&fake, "default", &[8]);
        let w = waker(&fake, "60000");
        let g = w
            .register(
                &qs(&["default"]),
                1,
                Instant::now() + Duration::from_millis(20),
            )
            .unwrap();
        let o = wait(&g, 60).await;
        assert!(matches!(o, WaitOutcome::Expired), "{o:?}");
        drop(g);
        gate.notify_one();
        for _ in 0..200 {
            if !fake.unreserved.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(*fake.unreserved.lock().unwrap(), vec![8]);
        assert_eq!(w.metrics().unreserved_count(UnreserveCause::Deadline), 1);
        assert_eq!(w.metrics().hints(HintSource::Unreserve), 1);
    }

    /// A client CANCEL while a sweep is in flight answers `Cancelled` at once (no reservation
    /// statement has completed for it, §24.4); the jobs that sweep brings back are offered to another
    /// waiter of the queue first, and only the rest are unreserved (cause `cancel`).
    #[tokio::test]
    async fn a_cancelled_waiters_jobs_go_to_the_next_waiter_then_unreserve() {
        let gate = Arc::new(Notify::new());
        let fake = Arc::new(Fake {
            gate: Some(Arc::clone(&gate)),
            ..Fake::default()
        });
        put(&fake, "default", &[1, 2]);
        let w = waker(&fake, "60000");
        let far = Instant::now() + Duration::from_secs(5);
        let a = w.register(&qs(&["default"]), 2, far).unwrap();
        while fake.reserves.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let b = w.register(&qs(&["default"]), 1, far).unwrap(); // arrives while A's sweep runs
        let cancel = CancellationToken::new();
        cancel.cancel();
        let o = a.wait(far, &cancel, &Drain::new(), &Liveness::new()).await;
        assert!(matches!(o, WaitOutcome::Cancelled), "{o:?}");
        gate.notify_one(); // A's sweep returns [1, 2]
        let ob = wait(&b, 5_000).await;
        assert_eq!(ids(&ob), vec![1], "B (clamp 1) takes one of A's");
        for _ in 0..200 {
            if !fake.unreserved.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(*fake.unreserved.lock().unwrap(), vec![2]);
        assert_eq!(w.metrics().unreserved_count(UnreserveCause::Cancel), 1);
    }

    /// A failed sweep ends the waiters it served with its classified terminal and the queue's other
    /// idle waiters with `Ok{[]}`; nothing is re-sent (I4).
    #[tokio::test]
    async fn a_failed_sweep_ends_its_queues_waiters_and_is_not_retried() {
        let gate = Arc::new(Notify::new());
        let fake = Arc::new(Fake {
            gate: Some(Arc::clone(&gate)),
            ..Fake::default()
        });
        let w = waker(&fake, "60000");
        let far = Instant::now() + Duration::from_secs(5);
        let a = w.register(&qs(&["default"]), 1, far).unwrap();
        while fake.reserves.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let b = w.register(&qs(&["default"]), 1, far).unwrap();
        *fake.fail.lock().unwrap() = Some(PoolError::Sql {
            code: ferro_proto::consts::errc::QUERY_TIMEOUT,
            branch: ferro_proto::consts::errc::QUERY_TIMEOUT_BRANCH,
            sqlstate: Some("57014".into()),
            errno: None,
            message: "canceling statement due to statement timeout".into(),
        });
        gate.notify_one();
        let oa = wait(&a, 5_000).await;
        let WaitOutcome::Offer(Offer::Failed(ep)) = oa else {
            panic!("{oa:?}")
        };
        assert_eq!(ep.branch, branch::INDETERMINATE, "57014 on a sent RESERVE");
        assert!(matches!(
            wait(&b, 5_000).await,
            WaitOutcome::Offer(Offer::Empty)
        ));
        assert_eq!(w.metrics().reserve_unconfirmed_count(), 1);
        assert_eq!(fake.reserves.load(Ordering::SeqCst), 1, "not re-sent");
    }

    /// The coalesced poll: parked waiters cost one sweep per `POLL_MS` per queue, however many.
    #[tokio::test]
    async fn the_interval_poll_is_one_sweep_per_queue_not_per_waiter() {
        let fake = Arc::new(Fake::default());
        let w = waker(&fake, "40");
        let far = Instant::now() + Duration::from_secs(5);
        let mut guards = Vec::new();
        for _ in 0..100 {
            guards.push(w.register(&qs(&["a"]), 1, far).unwrap());
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let arrivals = w.metrics().polls(Trigger::Arrival);
        assert!(arrivals <= 3, "arrivals coalesce: {arrivals}");
        tokio::time::sleep(Duration::from_millis(400)).await;
        let interval = w.metrics().polls(Trigger::Interval);
        // The cost bound: never more than one sweep per tick for the queue, however many waiters
        // (<= 11 ticks fit 400 ms at 40 ms). The floor is loose so a slow runner cannot flip it.
        assert!(
            (2..=11).contains(&interval),
            "one poll per tick per queue: {interval}"
        );
        assert_eq!(w.waiters(), 100);
        drop(guards);
        assert_eq!(w.waiters(), 0);
        assert_eq!(w.metrics().waiters(), 0);
    }

    /// Teardown is told apart from a client CANCEL by the session's liveness, and drain answers an
    /// idle waiter at once.
    #[tokio::test]
    async fn teardown_and_drain_end_a_parked_waiter() {
        let fake = Arc::new(Fake::default());
        let w = waker(&fake, "60000");
        let far = Instant::now() + Duration::from_secs(5);
        let g = w.register(&qs(&["default"]), 1, far).unwrap();
        let live = Liveness::new();
        live.end();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let o = g.wait(far, &cancel, &Drain::new(), &live).await;
        assert!(matches!(o, WaitOutcome::Teardown), "{o:?}");
        let g = w.register(&qs(&["default"]), 1, far).unwrap();
        while g.lock().busy || w.metrics().polls(Trigger::Arrival) < 2 {
            tokio::task::yield_now().await;
        }
        let drain = Drain::new();
        drain.trigger();
        let o = g
            .wait(far, &CancellationToken::new(), &drain, &Liveness::new())
            .await;
        assert!(matches!(o, WaitOutcome::Drained), "{o:?}");
    }

    /// The batch is the FIFO prefix that fits the sweep cap, so one statement never reserves more
    /// than the budget; a FULL sweep is followed by a `refill` for the rest.
    #[tokio::test]
    async fn a_full_sweep_refills_for_the_waiters_it_left() {
        let fake = Arc::new(Fake::default());
        let w = waker(&fake, "60000");
        let cap = w.cap;
        put(&fake, "q", &(1..=i64::from(cap) + 5).collect::<Vec<_>>());
        let far = Instant::now() + Duration::from_secs(5);
        // Register everyone first, while nothing is bound, so one sweep sees them all.
        let mut guards = Vec::new();
        for _ in 0..usize::from(cap) + 5 {
            guards.push(w.register(&qs(&["q"]), 1, far).unwrap());
        }
        let mut got = Vec::new();
        for g in &guards {
            got.extend(ids(&wait(g, 5_000).await));
        }
        got.sort_unstable();
        assert_eq!(got.len(), usize::from(cap) + 5, "every job delivered once");
        got.dedup();
        assert_eq!(got.len(), usize::from(cap) + 5);
        assert!(w.metrics().polls(Trigger::Refill) >= 1);
    }
}
