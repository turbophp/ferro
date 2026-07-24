//! Per-request flow-control credit: SPEC §5.2's "server→client streams are credit-based **per
//! request**: default window 64 frames / 4 MiB, replenished via `WINDOW_UPDATE {request_id,
//! frames, bytes}`." S3 wires only the primitive plus `WINDOW_UPDATE` routing (`session::registry`
//! stores one `Credit` per in-flight request, `session::mod`'s reader loop applies incoming
//! `WINDOW_UPDATE` frames to it); no stream producer *debits* it yet (`try_debit` exists ahead of
//! that consumer, which lands with DATA frames in S5).
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
}
