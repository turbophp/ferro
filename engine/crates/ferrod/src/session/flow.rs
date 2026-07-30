//! Flow control for server→client streams (SPEC §5.2: "server→client streams are credit-based
//! **per request** ... replenished via `WINDOW_UPDATE {request_id, frames, bytes}`") plus the
//! per-session aggregate byte cap (`Config::session_cap_bytes`). The default window size is
//! deliberately NOT repeated here as a literal — see `Config::credit_frames`/`credit_bytes` and
//! `ferro_proto::consts::DEFAULT_CREDIT_{FRAMES,BYTES}`, and SPEC §22.2's M1-S5 note for why the
//! byte figure is coupled to `MAX_FRAME_PAYLOAD` and thus not a stable number to quote in a doc
//! comment.
//!
//! Two layers live here:
//!
//! - **`Credit`** — the plain per-request window arithmetic (`try_debit`/`replenish`), and
//!   **`CreditCell`** — that window behind a `std::sync::Mutex` + a `tokio::sync::Notify`, giving
//!   the S5 stream producer an **async, cancel/deadline-aware** `debit_or_wait`. This is the B3
//!   hazard's home: a producer that runs out of credit parks here and MUST resume the instant a
//!   routed `WINDOW_UPDATE` replenishes (never a lost wakeup) — or UNWIND if the request is
//!   cancelled / the deadline passes (never a hang). The wakeup is per-request, so a single waiter,
//!   so `notify_one` is the right primitive.
//!
//! - **`SessionCap`** — the per-session aggregate byte cap, and its RAII reservation guard
//!   **`CapReserve`**. This is the M6 hazard's home: reserved bytes are released EXACTLY ONCE, and
//!   only when the guard drops — there is no public `release` a producer could forget to call,
//!   double-call, or call after the frame is already gone. Because the cap is per-session with
//!   potentially several concurrent streamed requests of heterogeneous demand, a release wakes ALL
//!   waiters (`notify_waiters`), each rechecks, and whichever now fits proceeds.
//!
//! Locking discipline (load-bearing): the counters (`Credit`, `used`) sit behind a
//! `std::sync::Mutex` — a short critical section, locked-mutate/read-then-unlocked. The lock is
//! NEVER held across an `.await`; every `try_debit`/`try_reserve`/`replenish`/read locks, does its
//! work, and drops the guard before any await. A std (not tokio) mutex is both correct and cheaper
//! here, and — the reason it MUST be a std mutex — it is locked synchronously from `CapReserve`'s
//! `Drop`.
//!
//! S5 wires the primitives, the registry storage, and `WINDOW_UPDATE` routing; the stream producer
//! that actually debits/reserves lands with DATA frames in a later S5 task, and the `SessionCap`
//! construction + threading into the `Responder` lands in Task 4a.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Why an async flow-control wait (`CreditCell::debit_or_wait` / `SessionCap::reserve_or_wait`)
/// returned before the resource became available. Both are terminal for the parked producer: it
/// stops producing and lets its request unwind toward its one terminal frame — it must NOT keep
/// waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitAborted {
    /// The request's `CancellationToken` fired (a routed `CANCEL`, or session teardown).
    Cancelled,
    /// The request's `timeout_ms` deadline passed while parked.
    Deadline,
}

/// A per-request credit window: `frames` counts remaining permitted DATA frames, `bytes` the
/// remaining permitted payload bytes. Seeded at registry-insert time from
/// `Config::credit_frames`/`credit_bytes` (which themselves default from
/// `ferro_proto::consts::DEFAULT_CREDIT_{FRAMES,BYTES}`), and replenished by a routed
/// `WINDOW_UPDATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Credit {
    frames: u32,
    bytes: u32,
}

impl Credit {
    /// Seed a fresh credit window.
    pub fn new(frames: u32, bytes: u32) -> Self {
        Credit { frames, bytes }
    }

    /// The remaining frame count.
    pub fn frames(&self) -> u32 {
        self.frames
    }

    /// The remaining byte budget.
    pub fn bytes(&self) -> u32 {
        self.bytes
    }

    /// Attempt to debit one frame worth `bytes` bytes from the window. Returns `false` — leaving
    /// the window entirely unchanged — if the window has no frames left or `bytes` exceeds the
    /// remaining byte budget; otherwise debits exactly one frame and `bytes` bytes and returns
    /// `true`.
    pub fn try_debit(&mut self, bytes: u32) -> bool {
        if self.frames == 0 || bytes > self.bytes {
            return false;
        }
        self.frames -= 1;
        self.bytes -= bytes;
        true
    }

    /// Replenish the window by `frames`/`bytes`, as delivered by a `WINDOW_UPDATE` frame.
    /// Saturating: a client cannot overflow the window into a wraparound by over-replenishing.
    pub fn replenish(&mut self, frames: u32, bytes: u32) {
        self.frames = self.frames.saturating_add(frames);
        self.bytes = self.bytes.saturating_add(bytes);
    }
}

/// A `Credit` window behind a `std::sync::Mutex` + a `tokio::sync::Notify`, so a stream producer
/// can BLOCK (`debit_or_wait`) when it runs out of credit and RESUME the instant a routed
/// `WINDOW_UPDATE` replenishes it — the B3 backpressure primitive. Owned by the registry as an
/// `Arc<CreditCell>` (one per in-flight request); the registry's `replenish` mutates + notifies,
/// and a clone travels to the producer/`Responder`.
#[derive(Debug)]
pub struct CreditCell {
    credit: Mutex<Credit>,
    notify: Notify,
}

impl CreditCell {
    /// Wrap a freshly-seeded `Credit` window.
    pub fn new(credit: Credit) -> Self {
        CreditCell {
            credit: Mutex::new(credit),
            notify: Notify::new(),
        }
    }

    /// Attempt one all-or-nothing debit (see [`Credit::try_debit`]). Locks the inner mutex for the
    /// duration of the arithmetic only — never across an await.
    pub fn try_debit(&self, bytes: u32) -> bool {
        self.credit.lock().unwrap().try_debit(bytes)
    }

    /// Apply a routed `WINDOW_UPDATE` replenishment, then wake the (single, per-request) waiter.
    /// The mutate-THEN-notify order is load-bearing: a parked `debit_or_wait` woken by this call
    /// must, on its recheck, see the credit this call added. `notify_one` (not `notify_waiters`)
    /// because a per-request window has at most one producer parked on it; if none is parked yet,
    /// `notify_one` stores a permit that the next `notified()` consumes — the other half of the
    /// B3 no-lost-wakeup guarantee.
    pub fn replenish(&self, frames: u32, bytes: u32) {
        self.credit.lock().unwrap().replenish(frames, bytes);
        self.notify.notify_one();
    }

    /// A by-value snapshot of the current window (`Credit` is `Copy`). Used by the registry's
    /// `credit_snapshot` for tests/introspection — reads under the lock, holds it only for the copy.
    pub fn snapshot(&self) -> Credit {
        *self.credit.lock().unwrap()
    }

    /// Debit one frame worth `bytes`, or park until a `WINDOW_UPDATE` replenishes enough — while
    /// remaining cancel/deadline-aware. Returns `Err` (without debiting anything) if the request's
    /// `cancel` token fires or `deadline` passes first.
    ///
    /// **B3 register-then-recheck.** The classic lost-wakeup bug is: check → fail → *then* build
    /// the `notified()` future → await, losing a `replenish`+`notify_one` that fired in the gap.
    /// The fix, exactly as below: build the `Notified` future FIRST (registering interest), THEN
    /// recheck `try_debit`, THEN await. A replenish landing in the gap is caught by the recheck;
    /// one landing after the recheck is caught by the notify (or its stored permit). The await
    /// races the token and the deadline, `biased` so cancel/timeout win a tie over a simultaneous
    /// notify.
    pub async fn debit_or_wait(
        &self,
        bytes: u32,
        cancel: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), WaitAborted> {
        loop {
            if self.try_debit(bytes) {
                return Ok(());
            }
            // Register interest BEFORE the recheck — see the doc comment above.
            let n = self.notify.notified();
            if self.try_debit(bytes) {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(WaitAborted::Cancelled),
                () = sleep_until_opt(deadline) => return Err(WaitAborted::Deadline),
                _ = n => {} // replenished -> loop rechecks
            }
        }
    }
}

/// The per-session aggregate byte cap (`Config::session_cap_bytes`, its OWN literal default — see
/// `config.rs` — deliberately NOT `ferro_proto::consts::MAX_FRAME_PAYLOAD`; a distinct concept: the
/// codec's per-frame ceiling vs. this session-wide running total across every in-flight request's
/// streamed bytes). Owned per session (in `run_with_handler`), shared as an `Arc` with the
/// producer(s); a successful reservation hands back a [`CapReserve`] guard whose `Drop` is the ONLY
/// release path.
#[derive(Debug)]
pub struct SessionCap {
    used: Mutex<u64>,
    cap: u64,
    notify: Notify,
}

impl SessionCap {
    /// A fresh cap with nothing reserved yet.
    pub fn new(cap: u64) -> Self {
        SessionCap {
            used: Mutex::new(0),
            cap,
            notify: Notify::new(),
        }
    }

    /// The currently reserved total. Locks only for the read.
    pub fn used(&self) -> u64 {
        *self.used.lock().unwrap()
    }

    /// Attempt to reserve `bytes` against the remaining aggregate cap. Returns `false` — leaving
    /// `used` entirely unchanged — if reserving would push the running total past `cap` (checked
    /// via `checked_add` so a pathological huge `bytes` can't wrap around and appear to fit);
    /// otherwise reserves and returns `true`. Private: a bare reserve with no matching release
    /// would leak the cap — callers go through `reserve_or_wait`, which returns the releasing guard.
    fn try_reserve(&self, bytes: u64) -> bool {
        let mut used = self.used.lock().unwrap();
        match used.checked_add(bytes) {
            Some(new_used) if new_used <= self.cap => {
                *used = new_used;
                true
            }
            _ => false,
        }
    }

    /// Reserve `bytes` against the cap, or park until a released guard frees enough — while
    /// remaining cancel/deadline-aware. On success returns a [`CapReserve`] guard that releases the
    /// reservation exactly once, on drop. Returns `Err` (having reserved nothing) if `cancel` fires
    /// or `deadline` passes first.
    ///
    /// Uses the IDENTICAL B3 register-then-recheck idiom as [`CreditCell::debit_or_wait`] — build
    /// the `Notified` first, recheck `try_reserve`, then the `biased` cancel/deadline/notify
    /// `select!` — so a release racing the wait can never be lost. (A try-reserve-then-await shape
    /// would reintroduce a lost wakeup on the cap.)
    pub async fn reserve_or_wait(
        self: &Arc<Self>,
        bytes: u64,
        cancel: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<CapReserve, WaitAborted> {
        loop {
            if self.try_reserve(bytes) {
                return Ok(CapReserve {
                    cap: Arc::clone(self),
                    bytes,
                });
            }
            // Register interest BEFORE the recheck (B3).
            let n = self.notify.notified();
            if self.try_reserve(bytes) {
                return Ok(CapReserve {
                    cap: Arc::clone(self),
                    bytes,
                });
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(WaitAborted::Cancelled),
                () = sleep_until_opt(deadline) => return Err(WaitAborted::Deadline),
                _ = n => {} // a guard released -> loop rechecks
            }
        }
    }
}

/// A live reservation against a [`SessionCap`]. Holds the reserved `bytes` until dropped; its
/// `Drop` releases them back to the cap and wakes every parked waiter — the M6 guarantee that a
/// reservation is released EXACTLY ONCE and never monotonically (no public `release` to forget,
/// double-call, or call after the frame is gone). In S5 this guard is moved into the writer's DATA
/// frame and dropped when the frame is sent; if the request tears down before the frame is sent,
/// dropping the guard on teardown releases it just the same.
#[derive(Debug)]
pub struct CapReserve {
    cap: Arc<SessionCap>,
    bytes: u64,
}

impl Drop for CapReserve {
    fn drop(&mut self) {
        {
            let mut used = self.cap.used.lock().unwrap();
            *used = used.saturating_sub(self.bytes);
        }
        // Wake ALL waiters (heterogeneous demand): each rechecks `try_reserve`, whichever now fits
        // proceeds. `notify_one` could wake a waiter that still doesn't fit while skipping one that
        // would. Both the lock drop above and this call are synchronous — sound in `Drop`.
        self.cap.notify.notify_waiters();
    }
}

/// Await a deadline if there is one, else never resolve. `None` (no deadline) becomes a future that
/// is `pending` forever, so the `select!` deadline arm simply never fires.
async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_debit_rejects_without_partially_applying() {
        let mut credit = Credit::new(2, 100);

        assert!(credit.try_debit(60));
        assert_eq!(credit.frames(), 1);
        assert_eq!(credit.bytes(), 40);

        // Exceeds the remaining byte budget: rejected, and the window is untouched.
        assert!(!credit.try_debit(50));
        assert_eq!(credit.frames(), 1);
        assert_eq!(credit.bytes(), 40);

        assert!(credit.try_debit(40));
        assert_eq!(credit.frames(), 0);
        assert_eq!(credit.bytes(), 0);

        // No frames left at all, even a zero-byte debit is rejected.
        assert!(!credit.try_debit(0));
    }

    #[test]
    fn replenish_adds_to_the_existing_window() {
        let mut credit = Credit::new(0, 0);
        credit.replenish(3, 200);
        assert_eq!(credit.frames(), 3);
        assert_eq!(credit.bytes(), 200);

        credit.replenish(1, 50);
        assert_eq!(credit.frames(), 4);
        assert_eq!(credit.bytes(), 250);
    }

    #[test]
    fn replenish_saturates_instead_of_overflowing() {
        let mut credit = Credit::new(u32::MAX, u32::MAX);
        credit.replenish(1, 1);
        assert_eq!(credit.frames(), u32::MAX);
        assert_eq!(credit.bytes(), u32::MAX);
    }

    #[test]
    fn session_cap_try_reserve_accounting_and_overflow() {
        let cap = SessionCap::new(100);

        assert!(cap.try_reserve(60));
        assert_eq!(cap.used(), 60);

        // Would exceed the cap: rejected, `used` untouched.
        assert!(!cap.try_reserve(50));
        assert_eq!(cap.used(), 60);

        assert!(cap.try_reserve(40));
        assert_eq!(cap.used(), 100);
        assert!(!cap.try_reserve(1));

        // A reserve so large it would overflow `used + bytes` must be rejected, not wrap.
        let cap = SessionCap::new(u64::MAX);
        assert!(cap.try_reserve(10));
        assert!(!cap.try_reserve(u64::MAX));
        assert_eq!(cap.used(), 10);
    }

    // -----------------------------------------------------------------------------------------
    // CreditCell — B3: register-then-recheck + cancel/deadline-aware wakeups
    // -----------------------------------------------------------------------------------------
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{Instant, timeout};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn debit_or_wait_wakes_a_parked_waiter_on_replenish() {
        let cell = Arc::new(CreditCell::new(Credit::new(0, 0)));
        let token = CancellationToken::new();
        let waiter = {
            let cell = Arc::clone(&cell);
            let token = token.clone();
            tokio::spawn(async move { cell.debit_or_wait(100, &token, None).await })
        };
        // Let the spawned task run up to its await point (enrolled as a Notify waiter).
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiter.is_finished(),
            "waiter must be parked on an empty cell"
        );
        // Replenish -> notify_one wakes the enrolled waiter; it rechecks and debits.
        cell.replenish(1, 100);
        let res = timeout(Duration::from_secs(5), waiter)
            .await
            .expect("must not hang")
            .expect("task join");
        assert_eq!(res, Ok(()));
        assert_eq!(cell.snapshot(), Credit::new(0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn debit_or_wait_never_hangs_when_replenish_races_the_await() {
        timeout(Duration::from_secs(10), async {
            for _ in 0..200 {
                let cell = Arc::new(CreditCell::new(Credit::new(0, 0)));
                let token = CancellationToken::new();
                let w = {
                    let cell = Arc::clone(&cell);
                    let token = token.clone();
                    tokio::spawn(async move { cell.debit_or_wait(100, &token, None).await })
                };
                // Replenish concurrently: it may land before the waiter parks (the B3 gap),
                // while parked, or after. Register-then-recheck + notify_one's stored permit
                // must resume it in every ordering.
                let r = {
                    let cell = Arc::clone(&cell);
                    tokio::spawn(async move { cell.replenish(1, 100) })
                };
                let (jw, jr) = tokio::join!(w, r);
                assert_eq!(jw.unwrap(), Ok(()), "no ordering may lose the wakeup");
                jr.unwrap();
            }
        })
        .await
        .expect("no ordering may hang");
    }

    #[tokio::test]
    async fn debit_or_wait_unwinds_on_cancel() {
        let cell = Arc::new(CreditCell::new(Credit::new(0, 0)));
        let token = CancellationToken::new();
        let w = {
            let cell = Arc::clone(&cell);
            let token = token.clone();
            tokio::spawn(async move { cell.debit_or_wait(100, &token, None).await })
        };
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !w.is_finished(),
            "a never-replenished wait must still be parked"
        );
        token.cancel();
        let res = timeout(Duration::from_secs(5), w)
            .await
            .expect("cancel must unwind, never hang")
            .expect("task join");
        assert_eq!(res, Err(WaitAborted::Cancelled));
    }

    #[tokio::test]
    async fn debit_or_wait_unwinds_on_past_deadline() {
        let cell = CreditCell::new(Credit::new(0, 0));
        let token = CancellationToken::new();
        let res = timeout(
            Duration::from_secs(5),
            cell.debit_or_wait(100, &token, Some(Instant::now())),
        )
        .await
        .expect("deadline must unwind, never hang");
        assert_eq!(res, Err(WaitAborted::Deadline));
    }

    #[tokio::test]
    async fn debit_or_wait_biased_cancel_wins_over_deadline() {
        let cell = CreditCell::new(Credit::new(0, 0));
        let token = CancellationToken::new();
        token.cancel(); // pre-cancelled AND a past deadline: both select arms ready at once.
        let res = timeout(
            Duration::from_secs(5),
            cell.debit_or_wait(100, &token, Some(Instant::now())),
        )
        .await
        .expect("no hang");
        assert_eq!(
            res,
            Err(WaitAborted::Cancelled),
            "biased select: cancel must outrank a simultaneously-ready deadline"
        );
    }

    // -----------------------------------------------------------------------------------------
    // SessionCap / CapReserve — M6: RAII release, never monotonic, never double, wake-all
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn cap_reserve_releases_exactly_on_drop() {
        let cap = Arc::new(SessionCap::new(1000));
        let token = CancellationToken::new();
        {
            let g = cap.reserve_or_wait(600, &token, None).await.unwrap();
            assert_eq!(cap.used(), 600);
            drop(g);
        }
        assert_eq!(
            cap.used(),
            0,
            "release must return used to exactly the prior value"
        );
        // A guard reserved then dropped WITHOUT ever reaching a writer still releases (no leak).
        let g = cap.reserve_or_wait(300, &token, None).await.unwrap();
        assert_eq!(cap.used(), 300);
        drop(g);
        assert_eq!(cap.used(), 0);
    }

    #[tokio::test]
    async fn cap_blocks_at_limit_then_unblocks_when_a_guard_drops() {
        let cap = Arc::new(SessionCap::new(100));
        let token = CancellationToken::new();
        let g1 = cap.reserve_or_wait(100, &token, None).await.unwrap();
        assert_eq!(cap.used(), 100);
        let waiter = {
            let cap = Arc::clone(&cap);
            let token = token.clone();
            tokio::spawn(async move { cap.reserve_or_wait(60, &token, None).await })
        };
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!waiter.is_finished(), "must block while the cap is full");
        assert_eq!(cap.used(), 100);
        drop(g1); // notify_waiters -> waiter rechecks -> 60 now fits.
        let g2 = timeout(Duration::from_secs(5), waiter)
            .await
            .expect("dropping a guard must unblock the waiter, never hang")
            .expect("task join")
            .unwrap();
        assert_eq!(cap.used(), 60);
        drop(g2);
        assert_eq!(cap.used(), 0);
    }

    #[tokio::test]
    async fn cap_release_wakes_all_waiters() {
        let cap = Arc::new(SessionCap::new(100));
        let token = CancellationToken::new();
        let g1 = cap.reserve_or_wait(100, &token, None).await.unwrap();
        let w_big = {
            let cap = Arc::clone(&cap);
            let t = token.clone();
            tokio::spawn(async move { cap.reserve_or_wait(60, &t, None).await })
        };
        let w_small = {
            let cap = Arc::clone(&cap);
            let t = token.clone();
            tokio::spawn(async move { cap.reserve_or_wait(40, &t, None).await })
        };
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !w_big.is_finished() && !w_small.is_finished(),
            "both heterogeneous-demand waiters must block while the cap is full"
        );
        // One release frees the whole 100; notify_waiters must wake BOTH so each rechecks —
        // 60 + 40 == 100, both now fit (notify_one could wake only one and strand the other).
        drop(g1);
        let g_big = timeout(Duration::from_secs(5), w_big)
            .await
            .expect("no hang")
            .expect("join")
            .unwrap();
        let g_small = timeout(Duration::from_secs(5), w_small)
            .await
            .expect("no hang")
            .expect("join")
            .unwrap();
        assert_eq!(cap.used(), 100);
        drop(g_big);
        drop(g_small);
        assert_eq!(cap.used(), 0);
    }

    #[tokio::test]
    async fn cap_reserve_unwinds_on_cancel_and_deadline() {
        let cap = Arc::new(SessionCap::new(100));
        let hold_token = CancellationToken::new();
        let _g = cap.reserve_or_wait(100, &hold_token, None).await.unwrap(); // fill the cap

        // cancel path
        let ctoken = CancellationToken::new();
        let w = {
            let cap = Arc::clone(&cap);
            let t = ctoken.clone();
            tokio::spawn(async move { cap.reserve_or_wait(10, &t, None).await })
        };
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!w.is_finished());
        ctoken.cancel();
        let cancelled = timeout(Duration::from_secs(5), w)
            .await
            .expect("cancel must unwind, never hang")
            .expect("join")
            .unwrap_err();
        assert_eq!(cancelled, WaitAborted::Cancelled);

        // deadline path
        let dtoken = CancellationToken::new();
        let deadlined = timeout(
            Duration::from_secs(5),
            cap.reserve_or_wait(10, &dtoken, Some(Instant::now())),
        )
        .await
        .expect("deadline must unwind, never hang")
        .unwrap_err();
        assert_eq!(deadlined, WaitAborted::Deadline);

        assert_eq!(cap.used(), 100, "failed reserves must not consume any cap");
    }
}
