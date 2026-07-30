//! Per-request flow-control credit: SPEC §5.2's "server→client streams are credit-based **per
//! request** ... replenished via `WINDOW_UPDATE {request_id, frames, bytes}`." The default window
//! size is deliberately NOT repeated here as a literal — see `Config::credit_frames`/`credit_bytes`
//! and `ferro_proto::consts::DEFAULT_CREDIT_{FRAMES,BYTES}` below for the current defaults, and
//! SPEC §22.2's M1-S5 note for why the byte figure is coupled to `MAX_FRAME_PAYLOAD` and thus not
//! a stable number to quote in a doc comment. S3 wires only the primitive plus `WINDOW_UPDATE`
//! routing (`session::registry` stores one `Credit` per in-flight request, `session::mod`'s reader
//! loop applies incoming `WINDOW_UPDATE` frames to it); no stream producer *debits* it yet
//! (`try_debit` exists ahead of that consumer, which lands with DATA frames in S5).
//!
//! Deliberately NOT the per-session aggregate cap (`Config::session_cap_bytes`) — that is a
//! distinct, session-wide concept layered on top in Task 6; this module is the per-request window
//! only.

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
    /// `true`. No producer calls this yet (S5's stream producers are the first consumer); it is
    /// exercised directly by this module's own tests in the meantime.
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

/// The per-session aggregate byte cap (`Config::session_cap_bytes`, its OWN literal default
/// `16 * 1024 * 1024` — see `config.rs` — deliberately NOT `ferro_proto::consts::MAX_FRAME_PAYLOAD`,
/// a distinct concept: the codec's per-frame ceiling vs. this session-wide running total across
/// every in-flight request's streamed bytes). No stream producer reserves against it yet (S5's
/// DATA frames are the first consumer); this is the primitive plus its own tests.
#[derive(Debug, Clone, Copy)]
pub struct SessionCap {
    used: u64,
    cap: u64,
}

impl SessionCap {
    /// A fresh cap with nothing reserved yet.
    pub fn new(cap: u64) -> Self {
        SessionCap { used: 0, cap }
    }

    /// Attempt to reserve `bytes` against the remaining aggregate cap. Returns `false` — leaving
    /// `used` entirely unchanged — if reserving would push the running total past `cap` (checked
    /// via `checked_add` so a pathological huge `bytes` can't wrap around and appear to fit);
    /// otherwise reserves and returns `true`.
    pub fn try_reserve(&mut self, bytes: u64) -> bool {
        match self.used.checked_add(bytes) {
            Some(new_used) if new_used <= self.cap => {
                self.used = new_used;
                true
            }
            _ => false,
        }
    }

    /// Release a previously reserved `bytes`. Saturating at zero: a caller releasing more than is
    /// currently reserved (which should never happen if reserve/release calls are paired
    /// correctly) cannot underflow `used` into a wraparound.
    pub fn release(&mut self, bytes: u64) {
        self.used = self.used.saturating_sub(bytes);
    }

    /// The currently reserved total.
    pub fn used(&self) -> u64 {
        self.used
    }

    /// The configured aggregate cap.
    pub fn cap(&self) -> u64 {
        self.cap
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
    fn session_cap_try_reserve_rejects_without_partially_applying() {
        let mut cap = SessionCap::new(100);

        assert!(cap.try_reserve(60));
        assert_eq!(cap.used(), 60);

        // Would exceed the cap: rejected, and `used` is untouched.
        assert!(!cap.try_reserve(50));
        assert_eq!(cap.used(), 60);

        assert!(cap.try_reserve(40));
        assert_eq!(cap.used(), 100);
        assert!(!cap.try_reserve(1));
    }

    #[test]
    fn session_cap_release_frees_capacity_and_saturates_at_zero() {
        let mut cap = SessionCap::new(100);
        cap.try_reserve(60);

        cap.release(20);
        assert_eq!(cap.used(), 40);
        assert!(cap.try_reserve(60));
        assert_eq!(cap.used(), 100);

        // Releasing more than is reserved saturates at zero rather than underflowing.
        cap.release(u64::MAX);
        assert_eq!(cap.used(), 0);
    }

    #[test]
    fn session_cap_try_reserve_rejects_overflowing_addition() {
        let mut cap = SessionCap::new(u64::MAX);
        cap.try_reserve(10);
        // A reserve so large it would overflow `used + bytes` must be rejected, not wrap.
        assert!(!cap.try_reserve(u64::MAX));
        assert_eq!(cap.used(), 10);
    }
}
