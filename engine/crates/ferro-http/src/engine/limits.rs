//! Host-level admission limits (SPEC §23.6 step 4 (b), (c), (e); §23.8.6; §23.8.7; slice M6-F6).
//!
//! Every limit here acts **before anything is sent**, so each refusal is `Retryable` and carries
//! its `[http.causes]` token (§23.7.1's "before dispatch" rows). All of it is per UPSTREAM, shared
//! by every session on the host — under `PARTITION=uid` too, which partitions connections only
//! (§23.8.1); the per-connection limits `MAX_CONNECTIONS`/`MAX_DIALS` live in `pool`.
//!
//! - [`Breaker`] (§23.8.6): closed → open → half-open. Half-open admits exactly one request as the
//!   probe, which holds a [`BreakerTicket`] — an RAII guard (review F24). A ticket that is dropped
//!   without an outcome (a refusal by a later admission step, a `CANCEL`, the session dying, a
//!   panic unwinding through the exchange) reports "any other terminal", which RELEASES the probe
//!   slot and leaves the breaker half-open, so the slot can never leak.
//! - [`Hold`] (§23.8.7, `HONOR_RETRY_AFTER=1`): a 429 or 503 head carrying a `Retry-After` of at
//!   most `RETRY_AFTER_MAX_MS` holds the upstream; later admissions fail fast (`retry_after_hold`).
//! - [`RateBucket`] (§23.8.7): one token bucket per upstream (GCRA, exact integer arithmetic).
//!   With no token, `RATE_MAX_WAIT_MS=0` fails fast (`rate_limited` and the time to the next
//!   token); otherwise the request waits up to `min(RATE_MAX_WAIT_MS, remaining deadline)`.
//! - [`RequestGate`] (§23.6 step 4 (e)): `MAX_REQUESTS` in flight, at most `MAX_QUEUED` waiting
//!   for a slot (`queue_full`), each for at most `QUEUE_TIMEOUT_MS` (`queue_timeout`).
//!
//! The state machines take `now` as an argument, so their rules are tested without a clock.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::config::{self, BreakerCounts, HttpConfig, UpstreamEntry};
use crate::fate::{BeforeDispatch, SentNoHead, Situation};

/// Milliseconds until `until`, rounded UP and at least 1 (a `retry_after_ms` of 0 would invite an
/// immediate retry into the same refusal), saturating at `u32::MAX`.
fn ms_until(until: Instant, now: Instant) -> u32 {
    ceil_ms(until.saturating_duration_since(now))
}

fn ceil_ms(d: Duration) -> u32 {
    let ms = d.as_nanos().div_ceil(1_000_000).max(1);
    u32::try_from(ms).unwrap_or(u32::MAX)
}

// =================================================================================================
// The breaker
// =================================================================================================

/// The breaker's state, as an observer sees it (tests; the `ferro_http_breaker_state` gauge, F7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// What a request's terminal means to the breaker (§23.8.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerOutcome {
    /// A final head that is not a counted 5xx: the upstream answered.
    Success,
    /// A failure of the configured counted class (`BREAKER_COUNTS`).
    Counted,
    /// Any other terminal: Indeterminate, a non-counted timeout, `Cancelled`, a refusal, the
    /// session dying.
    Other,
}

/// Why the breaker refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerRefusal {
    /// Open: `retry_after_ms` is the time left.
    Open { retry_after_ms: u32 },
    /// Half-open, and the probe slot is held.
    ProbeBusy,
}

#[derive(Debug)]
enum St {
    Closed { failures: u32 },
    Open { until: Instant },
    HalfOpen { probing: bool },
}

#[derive(Debug)]
struct BreakerInner {
    st: St,
    /// Bumped every time the breaker LEAVES `Closed`, so a ticket issued in an earlier closed
    /// period — a request admitted before the breaker opened that finishes later — cannot move it.
    generation: u64,
}

/// One upstream's breaker (§23.8.6).
#[derive(Debug)]
pub struct Breaker {
    failures: u32,
    counts: BreakerCounts,
    open_for: Duration,
    inner: Mutex<BreakerInner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TicketKind {
    /// Admitted while closed, in this generation.
    Closed(u64),
    /// The half-open probe.
    Probe,
}

/// A request's admission by the breaker, held until its outcome is known. **An RAII guard:**
/// dropping it without [`BreakerTicket::record`] reports [`BreakerOutcome::Other`], so a probe that
/// is refused later, cancelled, dropped or unwound by a panic releases the half-open slot.
#[derive(Debug)]
pub struct BreakerTicket {
    breaker: Arc<Breaker>,
    kind: TicketKind,
    recorded: AtomicBool,
}

impl Breaker {
    pub fn new(cfg: &config::Breaker) -> Arc<Self> {
        Arc::new(Breaker {
            failures: cfg.failures.max(1),
            counts: cfg.counts,
            open_for: Duration::from_millis(u64::from(cfg.open_ms)),
            inner: Mutex::new(BreakerInner {
                st: St::Closed { failures: 0 },
                generation: 0,
            }),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BreakerInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The counted class (`BREAKER_COUNTS`).
    pub fn counts(&self) -> BreakerCounts {
        self.counts
    }

    /// The state at `now` (an open breaker whose time is up reads as half-open).
    pub fn state(&self, now: Instant) -> BreakerState {
        match self.lock().st {
            St::Closed { .. } => BreakerState::Closed,
            St::Open { until } if now < until => BreakerState::Open,
            St::Open { .. } | St::HalfOpen { .. } => BreakerState::HalfOpen,
        }
    }

    /// Admission step (b), the breaker half. Closed admits; open refuses with the time left; an
    /// open breaker whose time is up becomes half-open, and half-open admits exactly one probe.
    pub fn admit(self: &Arc<Self>, now: Instant) -> Result<BreakerTicket, BreakerRefusal> {
        let mut g = self.lock();
        if let St::Open { until } = g.st {
            if now < until {
                return Err(BreakerRefusal::Open {
                    retry_after_ms: ms_until(until, now),
                });
            }
            g.st = St::HalfOpen { probing: false };
        }
        let kind = match g.st {
            St::Closed { .. } => TicketKind::Closed(g.generation),
            St::HalfOpen { probing: true } => return Err(BreakerRefusal::ProbeBusy),
            St::HalfOpen { probing: false } => {
                g.st = St::HalfOpen { probing: true };
                TicketKind::Probe
            }
            St::Open { .. } => unreachable!("an elapsed open state was made half-open above"),
        };
        drop(g);
        Ok(BreakerTicket {
            breaker: Arc::clone(self),
            kind,
            recorded: AtomicBool::new(false),
        })
    }

    fn settle(&self, kind: TicketKind, outcome: BreakerOutcome, now: Instant) {
        let mut g = self.lock();
        match kind {
            TicketKind::Closed(generation) => {
                if generation != g.generation {
                    return;
                }
                let St::Closed { failures } = &mut g.st else {
                    return;
                };
                match outcome {
                    BreakerOutcome::Success => *failures = 0,
                    BreakerOutcome::Other => {}
                    BreakerOutcome::Counted => {
                        *failures += 1;
                        if *failures >= self.failures {
                            g.st = St::Open {
                                until: now + self.open_for,
                            };
                            g.generation += 1;
                        }
                    }
                }
            }
            TicketKind::Probe => {
                // Only the probe moves a half-open breaker, so it is still half-open and probing.
                debug_assert!(matches!(g.st, St::HalfOpen { probing: true }));
                g.st = match outcome {
                    BreakerOutcome::Success => St::Closed { failures: 0 },
                    BreakerOutcome::Counted => St::Open {
                        until: now + self.open_for,
                    },
                    BreakerOutcome::Other => St::HalfOpen { probing: false },
                };
            }
        }
    }
}

impl BreakerTicket {
    /// Whether this request is the half-open probe.
    pub fn is_probe(&self) -> bool {
        self.kind == TicketKind::Probe
    }

    /// Settle this request's outcome. The FIRST record wins: a probe whose head arrived has closed
    /// the breaker, and a body failure after it does not reopen it.
    pub fn record(&self, outcome: BreakerOutcome) {
        self.record_at(outcome, Instant::now());
    }

    /// [`record`](Self::record) at `now` (the unit tests' clock).
    pub fn record_at(&self, outcome: BreakerOutcome, now: Instant) {
        if !self.recorded.swap(true, Ordering::SeqCst) {
            self.breaker.settle(self.kind, outcome, now);
        }
    }

    /// [`record`](Self::record) with the outcome a classified failure means (§23.8.6).
    pub fn record_failure(&self, s: &Situation) {
        self.record(failure_outcome(s, self.breaker.counts));
    }

    /// [`record`](Self::record) for a final head with `status`.
    pub fn record_head(&self, status: u16) {
        self.record(head_outcome(status, self.breaker.counts));
    }
}

impl Drop for BreakerTicket {
    fn drop(&mut self) {
        self.record(BreakerOutcome::Other);
    }
}

/// What a classified failure means to the breaker. `connect` (the default class) is `dns`,
/// `connect_*` and `tls_handshake` — all not sent (§23.8.6). `connect+timeout` adds the `timeout`
/// of the "sent, no head" row: the upstream took the request and produced no head by the deadline
/// (SPEC §22.2 (dk)). Everything else is "any other terminal".
pub fn failure_outcome(s: &Situation, counts: BreakerCounts) -> BreakerOutcome {
    match s {
        Situation::BeforeDispatch(
            BeforeDispatch::Dns
            | BeforeDispatch::ConnectRefused
            | BeforeDispatch::ConnectUnreachable
            | BeforeDispatch::ConnectTimeout
            | BeforeDispatch::TlsHandshake,
        ) => BreakerOutcome::Counted,
        Situation::SentNoHead(SentNoHead::Timeout) if counts != BreakerCounts::Connect => {
            BreakerOutcome::Counted
        }
        _ => BreakerOutcome::Other,
    }
}

/// What a final head means to the breaker: an answer, unless `connect+timeout+5xx` counts its
/// status (502/503/504).
pub fn head_outcome(status: u16, counts: BreakerCounts) -> BreakerOutcome {
    if counts == BreakerCounts::ConnectTimeout5xx && matches!(status, 502..=504) {
        BreakerOutcome::Counted
    } else {
        BreakerOutcome::Success
    }
}

// =================================================================================================
// The Retry-After hold
// =================================================================================================

/// One upstream's Retry-After hold (§23.8.7), present only under `HONOR_RETRY_AFTER=1`.
#[derive(Debug)]
pub struct Hold {
    max: Duration,
    until: Mutex<Option<Instant>>,
}

impl Hold {
    pub fn new(max_ms: u32) -> Self {
        Hold {
            max: Duration::from_millis(u64::from(max_ms)),
            until: Mutex::new(None),
        }
    }

    /// Admission step (b), the hold half: `Err(retry_after_ms)` while the upstream is held.
    pub fn admit(&self, now: Instant) -> Result<(), u32> {
        match *self.until.lock().unwrap_or_else(|p| p.into_inner()) {
            Some(until) if now < until => Err(ms_until(until, now)),
            _ => Ok(()),
        }
    }

    /// A final head arrived: a 429 or a 503 whose ONE `Retry-After` is a delay of at most
    /// `RETRY_AFTER_MAX_MS` holds the upstream for that delay (a later, shorter one never shortens
    /// an existing hold). Returns the delay applied.
    pub fn observe<'v>(
        &self,
        status: u16,
        retry_after: impl Iterator<Item = &'v [u8]>,
        now: Instant,
    ) -> Option<Duration> {
        let d = hold_for(status, retry_after, self.max)?;
        let mut g = self.until.lock().unwrap_or_else(|p| p.into_inner());
        let until = now + d;
        if g.is_none_or(|u| u < until) {
            *g = Some(until);
        }
        Some(d)
    }
}

/// The hold a response asks for (§23.8.7, SPEC §22.2 (dk)): only a 429 or a 503; exactly one
/// `Retry-After` field whose value (optional whitespace around it) is `delta-seconds` (RFC 9110
/// §10.2.3: `1*DIGIT`); a delay above zero and at most `max`. Anything else holds nothing: an
/// absent, repeated or unparseable field, an HTTP-date (not interpreted in v1), or a delay above
/// the operator's ceiling.
pub fn hold_for<'v>(
    status: u16,
    mut retry_after: impl Iterator<Item = &'v [u8]>,
    max: Duration,
) -> Option<Duration> {
    if status != 429 && status != 503 {
        return None;
    }
    let v = retry_after.next()?;
    if retry_after.next().is_some() {
        return None;
    }
    let v = v.trim_ascii();
    if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    // At most 20 digits fit a u64; anything longer is above any ceiling.
    let secs: u64 = std::str::from_utf8(v).ok()?.parse().ok()?;
    let d = Duration::from_secs(secs);
    (secs > 0 && d <= max).then_some(d)
}

// =================================================================================================
// The rate bucket
// =================================================================================================

/// One upstream's token bucket (§23.8.7), as GCRA in exact integer nanoseconds: one token every
/// `interval`, and a burst of `RATE_BURST` tokens from a full bucket.
#[derive(Debug)]
pub struct RateBucket {
    base: Instant,
    /// Nanoseconds per token: `1e12 / RATE_PER_SEC` in milli-requests.
    interval: u128,
    /// The burst tolerance: `interval × (RATE_BURST − 1)`.
    tolerance: u128,
    max_wait: Duration,
    /// The theoretical arrival time of the next conforming request, in ns since `base`.
    tat: Mutex<u128>,
}

/// A token taken ahead of its time: the request waits until the token is due. Dropped before
/// [`RateWait::keep`] (a `CANCEL` or the drain cap while waiting), the token is given back.
#[derive(Debug)]
pub struct RateWait {
    bucket: Arc<RateBucket>,
    pub wait: Duration,
    kept: bool,
}

impl RateWait {
    /// The wait is over: the token is the request's.
    pub fn keep(mut self) {
        self.kept = true;
    }
}

impl Drop for RateWait {
    fn drop(&mut self) {
        if !self.kept {
            let mut tat = self.bucket.tat.lock().unwrap_or_else(|p| p.into_inner());
            *tat = tat.saturating_sub(self.bucket.interval);
        }
    }
}

/// The rate step's answer.
#[derive(Debug)]
pub enum RateDecision {
    /// A token now.
    Admit,
    /// A token after waiting.
    Wait(RateWait),
    /// No token within the allowed wait: `rate_limited`, with the time to the next token.
    Refuse { retry_after_ms: u32 },
}

impl RateBucket {
    pub fn new(rate: &config::Rate, base: Instant) -> Arc<Self> {
        let interval = 1_000_000_000_000u128 / u128::from(rate.per_sec_milli.max(1));
        Arc::new(RateBucket {
            base,
            interval,
            tolerance: interval * u128::from(rate.burst.max(1) - 1),
            max_wait: Duration::from_millis(u64::from(rate.max_wait_ms)),
            tat: Mutex::new(0),
        })
    }

    /// Admission step (c). `remaining` is the time left before the request's total deadline; the
    /// request may wait for its token up to `min(RATE_MAX_WAIT_MS, remaining)`.
    pub fn take(self: &Arc<Self>, now: Instant, remaining: Duration) -> RateDecision {
        let t = now.saturating_duration_since(self.base).as_nanos();
        let mut tat = self.tat.lock().unwrap_or_else(|p| p.into_inner());
        let next = (*tat).max(t);
        // The token is due once `next − tolerance` has passed.
        let wait_ns = next.saturating_sub(self.tolerance).saturating_sub(t);
        if wait_ns == 0 {
            *tat = next + self.interval;
            return RateDecision::Admit;
        }
        let wait = Duration::from_nanos(u64::try_from(wait_ns).unwrap_or(u64::MAX));
        if wait > self.max_wait.min(remaining) {
            return RateDecision::Refuse {
                retry_after_ms: ceil_ms(wait),
            };
        }
        *tat = next + self.interval;
        drop(tat);
        RateDecision::Wait(RateWait {
            bucket: Arc::clone(self),
            wait,
            kept: false,
        })
    }
}

// =================================================================================================
// The request gate: MAX_REQUESTS, MAX_QUEUED, QUEUE_TIMEOUT_MS
// =================================================================================================

/// One upstream's in-flight slots and waiting queue (§23.6 step 4 (e)). A request holds its slot
/// ([`OwnedSemaphorePermit`]) from admission to its terminal. The semaphore is FIFO and never lets a
/// newcomer take a slot ahead of a waiter.
#[derive(Debug)]
pub struct RequestGate {
    slots: Arc<Semaphore>,
    max_requests: usize,
    waiting: AtomicUsize,
    max_queued: usize,
}

/// Counts one request in the queue for exactly as long as it waits.
struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RequestGate {
    pub fn new(max_requests: u32, max_queued: u32) -> Self {
        let max_requests = usize::try_from(max_requests)
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        RequestGate {
            slots: Arc::new(Semaphore::new(max_requests)),
            max_requests,
            waiting: AtomicUsize::new(0),
            max_queued: usize::try_from(max_queued).unwrap_or(usize::MAX),
        }
    }

    /// Requests waiting for a slot (the `ferro_http_waiting` gauge, F7).
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }

    /// Requests holding a slot.
    pub fn in_flight(&self) -> usize {
        self.max_requests
            .saturating_sub(self.slots.available_permits())
    }

    /// A slot now, or a place in the queue (`queue_full` when `MAX_QUEUED` are already waiting) and
    /// a slot within `until`; at `until` the request is refused with `on_expiry` (`queue_timeout`,
    /// or `deadline` when the request's own deadline is the nearer bound). The caller races this
    /// with its stop token; dropping the future leaves the queue.
    pub async fn acquire(
        &self,
        until: Instant,
        on_expiry: BeforeDispatch,
    ) -> Result<OwnedSemaphorePermit, BeforeDispatch> {
        if let Ok(p) = Arc::clone(&self.slots).try_acquire_owned() {
            return Ok(p);
        }
        if self
            .waiting
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |w| {
                (w < self.max_queued).then_some(w + 1)
            })
            .is_err()
        {
            return Err(BeforeDispatch::QueueFull);
        }
        let _waiting = Waiting(&self.waiting);
        tokio::select! {
            biased;
            p = Arc::clone(&self.slots).acquire_owned() => {
                // The semaphore is never closed.
                p.map_err(|_| BeforeDispatch::QueueFull)
            }
            () = tokio::time::sleep_until(until) => Err(on_expiry),
        }
    }
}

// =================================================================================================
// Per upstream
// =================================================================================================

/// One upstream's admission state.
#[derive(Debug)]
pub struct UpstreamLimits {
    pub breaker: Arc<Breaker>,
    pub hold: Option<Hold>,
    pub rate: Option<Arc<RateBucket>>,
    pub gate: RequestGate,
}

/// Every enabled upstream's admission state, built once at start.
#[derive(Debug)]
pub struct Limits(std::collections::HashMap<String, Arc<UpstreamLimits>>);

impl Limits {
    pub fn new(config: &HttpConfig) -> Self {
        let now = Instant::now();
        Limits(
            config
                .entries()
                .filter_map(|(name, e)| match e {
                    UpstreamEntry::Enabled(up) => Some((
                        name.to_string(),
                        Arc::new(UpstreamLimits {
                            breaker: Breaker::new(&up.breaker),
                            hold: up
                                .retry_after
                                .honor
                                .then(|| Hold::new(up.retry_after.max_ms)),
                            rate: up.rate.as_ref().map(|r| RateBucket::new(r, now)),
                            gate: RequestGate::new(up.limits.max_requests, up.limits.max_queued),
                        }),
                    )),
                    UpstreamEntry::Disabled(_) => None,
                })
                .collect(),
        )
    }

    pub fn get(&self, upstream: &str) -> Option<&Arc<UpstreamLimits>> {
        self.0.get(upstream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fate::{DispatchedNotSent, HeadReceived};
    use crate::validate::PolicyCause;

    fn breaker(k: u32, counts: BreakerCounts, open_ms: u32) -> Arc<Breaker> {
        Breaker::new(&config::Breaker {
            failures: k,
            counts,
            open_ms,
        })
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Opens after K CONSECUTIVE counted failures; a success in between resets the count; "other"
    /// outcomes neither count nor reset.
    #[test]
    fn the_breaker_opens_after_k_consecutive_counted_failures() {
        let b = breaker(3, BreakerCounts::Connect, 5_000);
        let t0 = Instant::now();
        let rec = |o| b.admit(t0).unwrap().record_at(o, t0);
        rec(BreakerOutcome::Counted);
        rec(BreakerOutcome::Counted);
        rec(BreakerOutcome::Success);
        rec(BreakerOutcome::Counted);
        rec(BreakerOutcome::Counted);
        rec(BreakerOutcome::Other);
        assert_eq!(b.state(t0), BreakerState::Closed);
        rec(BreakerOutcome::Counted);
        assert_eq!(b.state(t0), BreakerState::Open);
        assert_eq!(
            b.admit(t0 + ms(1_000)).unwrap_err(),
            BreakerRefusal::Open {
                retry_after_ms: 4_000
            },
            "retry_after_ms is the time left"
        );
        assert_eq!(
            b.admit(t0 + Duration::from_micros(4_999_100)).unwrap_err(),
            BreakerRefusal::Open { retry_after_ms: 1 },
            "rounded up, never 0"
        );
        assert!(b.admit(t0 + ms(5_000)).unwrap().is_probe());
    }

    /// A ticket dropped without an outcome is "other": it neither counts nor resets.
    #[test]
    fn a_dropped_closed_ticket_neither_counts_nor_resets() {
        let b = breaker(2, BreakerCounts::Connect, 5_000);
        let t0 = Instant::now();
        b.admit(t0).unwrap().record_at(BreakerOutcome::Counted, t0);
        drop(b.admit(t0).unwrap());
        assert_eq!(b.state(t0), BreakerState::Closed);
        b.admit(t0).unwrap().record_at(BreakerOutcome::Counted, t0);
        assert_eq!(
            b.state(t0),
            BreakerState::Open,
            "the drop did not reset the count"
        );
    }

    /// Half-open admits exactly one probe; the probe's outcome decides; a dropped probe (no
    /// outcome) releases the slot and stays half-open.
    #[test]
    fn half_open_admits_exactly_one_probe_and_its_outcome_decides() {
        let b = breaker(1, BreakerCounts::Connect, 100);
        let t0 = Instant::now();
        b.admit(t0).unwrap().record_at(BreakerOutcome::Counted, t0);
        let t1 = t0 + ms(100);
        assert_eq!(b.state(t0 + ms(99)), BreakerState::Open);
        assert_eq!(b.state(t1), BreakerState::HalfOpen);
        let probe = b.admit(t1).unwrap();
        assert!(probe.is_probe());
        assert_eq!(b.admit(t1).unwrap_err(), BreakerRefusal::ProbeBusy);
        drop(probe); // any other terminal
        assert_eq!(b.state(t1), BreakerState::HalfOpen);
        let probe = b.admit(t1).unwrap();
        assert!(probe.is_probe(), "the next request probes");
        probe.record_at(BreakerOutcome::Other, t1);
        let probe = b.admit(t1).unwrap();
        assert!(probe.is_probe(), "an explicit other releases it too");
        probe.record_at(BreakerOutcome::Counted, t1);
        assert_eq!(
            b.admit(t1 + ms(40)).unwrap_err(),
            BreakerRefusal::Open { retry_after_ms: 60 },
            "a counted probe failure opens it again for BREAKER_OPEN_MS"
        );
        let t2 = t1 + ms(100);
        let probe = b.admit(t2).unwrap();
        probe.record_at(BreakerOutcome::Success, t2);
        probe.record_at(BreakerOutcome::Counted, t2); // first record wins
        drop(probe);
        assert_eq!(b.state(t2), BreakerState::Closed);
        assert!(!b.admit(t2).unwrap().is_probe());
    }

    /// A probe unwound by a panic releases its slot (the guard is RAII).
    #[test]
    fn a_probe_unwound_by_a_panic_releases_the_slot() {
        let b = breaker(1, BreakerCounts::Connect, 1);
        let t0 = Instant::now();
        b.admit(t0).unwrap().record_at(BreakerOutcome::Counted, t0);
        let t1 = t0 + ms(1);
        let b2 = Arc::clone(&b);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _probe = b2.admit(t1).unwrap();
            panic!("an exchange panicked");
        }));
        assert!(r.is_err());
        assert!(b.admit(t1).unwrap().is_probe(), "the slot was released");
    }

    /// A request admitted in an earlier closed period cannot move the breaker later.
    #[test]
    fn a_stale_ticket_moves_nothing() {
        let b = breaker(1, BreakerCounts::Connect, 10);
        let t0 = Instant::now();
        let old = b.admit(t0).unwrap();
        b.admit(t0).unwrap().record_at(BreakerOutcome::Counted, t0); // opens
        old.record_at(BreakerOutcome::Success, t0); // ignored while open
        assert_eq!(b.state(t0), BreakerState::Open);
        let t1 = t0 + ms(10);
        b.admit(t1).unwrap().record_at(BreakerOutcome::Success, t1); // the probe closes it
        let stale = b.admit(t1).unwrap();
        b.admit(t1).unwrap().record_at(BreakerOutcome::Counted, t1); // opens again
        let t2 = t1 + ms(10);
        b.admit(t2).unwrap().record_at(BreakerOutcome::Success, t2); // closed again
        stale.record_at(BreakerOutcome::Counted, t2); // from the previous closed period: ignored
        assert_eq!(b.state(t2), BreakerState::Closed);
        assert!(!b.admit(t2).unwrap().is_probe());
    }

    #[test]
    fn the_counted_classes() {
        use BreakerCounts::*;
        let counted = [
            Situation::BeforeDispatch(BeforeDispatch::Dns),
            Situation::BeforeDispatch(BeforeDispatch::ConnectRefused),
            Situation::BeforeDispatch(BeforeDispatch::ConnectUnreachable),
            Situation::BeforeDispatch(BeforeDispatch::ConnectTimeout),
            Situation::BeforeDispatch(BeforeDispatch::TlsHandshake),
        ];
        let never = [
            Situation::BeforeDispatch(BeforeDispatch::TlsVerify),
            Situation::BeforeDispatch(BeforeDispatch::Policy(PolicyCause::Address)),
            Situation::BeforeDispatch(BeforeDispatch::Deadline),
            Situation::BeforeDispatch(BeforeDispatch::QueueTimeout),
            Situation::DispatchedNotSent(DispatchedNotSent::Deadline),
            Situation::DispatchedNotSent(DispatchedNotSent::UnsentClosed),
            Situation::SentNoHead(SentNoHead::EofEmpty),
            Situation::SentNoHead(SentNoHead::Cancel),
            Situation::HeadReceived(HeadReceived::Timeout),
            Situation::HeadReceived(HeadReceived::ReadIdle),
        ];
        for c in [Connect, ConnectTimeout, ConnectTimeout5xx] {
            for s in &counted {
                assert_eq!(
                    failure_outcome(s, c),
                    BreakerOutcome::Counted,
                    "{s:?} {c:?}"
                );
            }
            for s in &never {
                assert_eq!(failure_outcome(s, c), BreakerOutcome::Other, "{s:?} {c:?}");
            }
        }
        let timeout = Situation::SentNoHead(SentNoHead::Timeout);
        assert_eq!(failure_outcome(&timeout, Connect), BreakerOutcome::Other);
        assert_eq!(
            failure_outcome(&timeout, ConnectTimeout),
            BreakerOutcome::Counted
        );
        assert_eq!(
            failure_outcome(&timeout, ConnectTimeout5xx),
            BreakerOutcome::Counted
        );
        for s in [200, 429, 500, 501, 505, 599] {
            assert_eq!(head_outcome(s, ConnectTimeout5xx), BreakerOutcome::Success);
        }
        for s in [502, 503, 504] {
            assert_eq!(head_outcome(s, ConnectTimeout5xx), BreakerOutcome::Counted);
            assert_eq!(head_outcome(s, ConnectTimeout), BreakerOutcome::Success);
            assert_eq!(head_outcome(s, Connect), BreakerOutcome::Success);
        }
    }

    fn hold(status: u16, values: &[&str]) -> Option<Duration> {
        hold_for(status, values.iter().map(|v| v.as_bytes()), ms(60_000))
    }

    #[test]
    fn which_responses_hold() {
        assert_eq!(hold(429, &["7"]), Some(Duration::from_secs(7)));
        assert_eq!(hold(503, &[" 60 "]), Some(Duration::from_secs(60)));
        assert_eq!(hold(503, &["61"]), None, "above RETRY_AFTER_MAX_MS");
        assert_eq!(hold(429, &[]), None, "no Retry-After: nothing to hold for");
        assert_eq!(hold(429, &["0"]), None);
        assert_eq!(hold(429, &["1", "1"]), None, "repeated");
        assert_eq!(hold(429, &["1.5"]), None);
        assert_eq!(hold(429, &["-1"]), None);
        assert_eq!(hold(429, &["+1"]), None);
        assert_eq!(hold(429, &[""]), None);
        assert_eq!(hold(429, &["Fri, 31 Dec 1999 23:59:59 GMT"]), None);
        assert_eq!(hold(429, &["99999999999999999999999"]), None, "overflow");
        for s in [200, 500, 502, 504, 408, 425] {
            assert_eq!(hold(s, &["1"]), None, "{s}");
        }
    }

    #[test]
    fn a_hold_refuses_until_it_expires_and_never_shortens() {
        let h = Hold::new(10_000);
        let t0 = Instant::now();
        assert_eq!(h.admit(t0), Ok(()));
        let ra = |s: &'static str| std::iter::once(s.as_bytes());
        assert_eq!(h.observe(429, ra("5"), t0), Some(Duration::from_secs(5)));
        assert_eq!(h.admit(t0 + ms(1_000)), Err(4_000));
        assert_eq!(
            h.observe(503, ra("1"), t0 + ms(1_000)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(h.admit(t0 + ms(2_500)), Err(2_500), "not shortened");
        assert_eq!(h.observe(200, ra("100"), t0), None);
        assert_eq!(h.admit(t0 + ms(5_000)), Ok(()));
    }

    fn bucket(per_sec_milli: u64, burst: u32, max_wait_ms: u32, base: Instant) -> Arc<RateBucket> {
        RateBucket::new(
            &config::Rate {
                per_sec_milli,
                burst,
                max_wait_ms,
            },
            base,
        )
    }

    fn admit(d: RateDecision) -> bool {
        matches!(d, RateDecision::Admit)
    }

    /// A full bucket admits RATE_BURST at once; then one token per interval; a refusal names the
    /// time to the next token.
    #[test]
    fn the_bucket_admits_a_burst_then_refills_at_the_rate() {
        let t0 = Instant::now();
        let b = bucket(2_000, 3, 0, t0); // 2/s, burst 3
        let far = Duration::from_secs(60);
        for _ in 0..3 {
            assert!(admit(b.take(t0, far)));
        }
        match b.take(t0, far) {
            RateDecision::Refuse { retry_after_ms } => assert_eq!(retry_after_ms, 500),
            d => panic!("{d:?}"),
        }
        match b.take(t0 + ms(200), far) {
            RateDecision::Refuse { retry_after_ms } => assert_eq!(retry_after_ms, 300),
            d => panic!("{d:?}"),
        }
        assert!(admit(b.take(t0 + ms(500), far)), "one token per 500 ms");
        assert!(!admit(b.take(t0 + ms(500), far)));
        assert!(admit(b.take(t0 + ms(1_000), far)));
        // A long idle refills to the burst, never past it.
        let t1 = t0 + Duration::from_secs(100);
        for _ in 0..3 {
            assert!(admit(b.take(t1, far)));
        }
        assert!(!admit(b.take(t1, far)));
    }

    /// RATE_PER_SEC in milli-requests: 0.333/s is one token per ~3.003 s.
    #[test]
    fn a_fractional_rate() {
        let t0 = Instant::now();
        let b = bucket(333, 1, 0, t0);
        let far = Duration::from_secs(60);
        assert!(admit(b.take(t0, far)));
        match b.take(t0, far) {
            RateDecision::Refuse { retry_after_ms } => assert_eq!(retry_after_ms, 3_004),
            d => panic!("{d:?}"),
        }
    }

    /// A wait is allowed up to min(RATE_MAX_WAIT_MS, remaining deadline); a waiter dropped before
    /// its token is due gives the token back.
    #[test]
    fn waiting_is_bounded_and_an_abandoned_wait_refunds() {
        let t0 = Instant::now();
        let b = bucket(1_000, 1, 2_000, t0); // 1/s, burst 1, wait ≤ 2 s
        let far = Duration::from_secs(60);
        assert!(admit(b.take(t0, far)));
        let w = match b.take(t0, far) {
            RateDecision::Wait(w) => w,
            d => panic!("{d:?}"),
        };
        assert_eq!(w.wait, ms(1_000));
        let w2 = match b.take(t0, far) {
            RateDecision::Wait(w) => w,
            d => panic!("{d:?}"),
        };
        assert_eq!(w2.wait, ms(2_000));
        assert!(
            matches!(
                b.take(t0, far),
                RateDecision::Refuse {
                    retry_after_ms: 3_000
                }
            ),
            "beyond RATE_MAX_WAIT_MS"
        );
        drop(w2); // abandoned: refunded
        w.keep();
        match b.take(t0, ms(1_500)) {
            RateDecision::Refuse { retry_after_ms } => {
                assert_eq!(retry_after_ms, 2_000, "bounded by the remaining deadline")
            }
            d => panic!("{d:?}"),
        }
        match b.take(t0, far) {
            RateDecision::Wait(w) => {
                assert_eq!(w.wait, ms(2_000), "the refunded slot is taken again");
                w.keep();
            }
            d => panic!("{d:?}"),
        }
    }

    /// MAX_QUEUED waiters at most; a waiter times out with the expiry event; a waiter that leaves
    /// frees its place; a released slot goes to the waiter (FIFO), not to a newcomer.
    #[tokio::test]
    async fn the_request_gate() {
        let g = Arc::new(RequestGate::new(1, 1));
        let far = Instant::now() + Duration::from_secs(60);
        let p = g.acquire(far, BeforeDispatch::QueueTimeout).await.unwrap();
        assert_eq!(g.in_flight(), 1);
        let g2 = Arc::clone(&g);
        let waiter =
            tokio::spawn(async move { g2.acquire(far, BeforeDispatch::QueueTimeout).await });
        while g.waiting() == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            g.acquire(far, BeforeDispatch::QueueTimeout)
                .await
                .unwrap_err(),
            BeforeDispatch::QueueFull
        );
        drop(p);
        let p2 = waiter.await.unwrap().unwrap();
        assert_eq!(g.waiting(), 0);
        assert_eq!(
            g.acquire(Instant::now() + ms(20), BeforeDispatch::Deadline)
                .await
                .unwrap_err(),
            BeforeDispatch::Deadline
        );
        assert_eq!(g.waiting(), 0, "a timed-out waiter leaves the queue");
        drop(p2);
        assert!(g.acquire(far, BeforeDispatch::QueueTimeout).await.is_ok());
    }

    /// MAX_QUEUED=0: nothing waits.
    #[tokio::test]
    async fn max_queued_zero_refuses_any_wait() {
        let g = RequestGate::new(1, 0);
        let far = Instant::now() + Duration::from_secs(60);
        let _p = g.acquire(far, BeforeDispatch::QueueTimeout).await.unwrap();
        assert_eq!(
            g.acquire(far, BeforeDispatch::QueueTimeout)
                .await
                .unwrap_err(),
            BeforeDispatch::QueueFull
        );
    }
}
