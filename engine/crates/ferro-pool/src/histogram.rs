//! A fixed-bucket histogram for SPEC §13's latency families (M2-C4b-2b).
//!
//! Hand-rolled for the same reason the scrape endpoint is (§22.2 (bs)): the workspace has no
//! metrics crate, and a histogram is one atomic array plus an atomic sum. Values are recorded in
//! MICROSECONDS —
//! the unit the engine already measures `queue_us` in — and rendered in SECONDS, Prometheus's base
//! unit, by [`fmt_seconds`], which works on integers so a bucket bound never renders as
//! `0.000024999999`.
//!
//! Buckets are stored NON-cumulatively and accumulated at render time: an observation touches one
//! bucket, not every bucket above it, which keeps the hot path to two `fetch_add`s and one search.
//!
//! **There is no separate count, on purpose.** The exposition format requires `_count` to equal the
//! `+Inf` bucket, and a count kept in its own atomic is read at a different instant from the
//! buckets, so under concurrent observation the two disagree — measured in review at roughly three
//! scrapes in four with four observing threads. [`Histogram::snapshot`] derives `_count` from the
//! same single pass over the buckets, so the invariant holds by construction. `_sum` is still a
//! separate read and may lead or lag the buckets by the observations in flight; the format
//! tolerates that, and every other client library has the same window unless it locks.

use std::sync::atomic::{AtomicU64, Ordering};

/// A histogram with `N` finite upper bounds (in microseconds) plus the implicit `+Inf` bucket.
#[derive(Debug)]
pub struct Histogram<const N: usize> {
    bounds_us: [u64; N],
    /// `buckets[i]` counts observations in `(bounds[i-1], bounds[i]]`; `buckets[N]` is `+Inf`.
    buckets: [AtomicU64; 33],
    sum_us: AtomicU64,
}

/// One read of a [`Histogram`], in the shape the exposition format wants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramSnapshot {
    /// `(upper bound in µs, CUMULATIVE count)` per finite bucket, then `(None, total)` for `+Inf`.
    pub buckets: Vec<(Option<u64>, u64)>,
    /// Sum of all observations, in microseconds.
    pub sum_us: u64,
    /// Number of observations — the `+Inf` cumulative of the SAME pass, never a separate read.
    pub count: u64,
}

impl<const N: usize> Histogram<N> {
    /// A histogram over `bounds_us`, which must be strictly increasing (asserted — a misordered
    /// bucket list silently mis-files every observation, and it is a constant, so it fails at the
    /// first construction, i.e. in every test).
    pub fn new(bounds_us: [u64; N]) -> Self {
        assert!(N < 33, "at most 32 finite buckets");
        assert!(
            bounds_us.windows(2).all(|w| w[0] < w[1]),
            "histogram bounds must be strictly increasing: {bounds_us:?}",
        );
        Self {
            bounds_us,
            buckets: [const { AtomicU64::new(0) }; 33],
            sum_us: AtomicU64::new(0),
        }
    }

    /// Record one observation of `us` microseconds. `le` is inclusive, as Prometheus defines it.
    pub fn observe_us(&self, us: u64) {
        let i = self.bounds_us.partition_point(|&b| b < us);
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    /// `(upper bound in µs, CUMULATIVE count)` for each finite bucket, then `(None, total)` for
    /// `+Inf` — the shape the exposition format wants.
    pub fn cumulative(&self) -> Vec<(Option<u64>, u64)> {
        let mut out = Vec::with_capacity(N + 1);
        let mut running = 0u64;
        for (i, &b) in self.bounds_us.iter().enumerate() {
            running += self.buckets[i].load(Ordering::Relaxed);
            out.push((Some(b), running));
        }
        running += self.buckets[N].load(Ordering::Relaxed);
        out.push((None, running));
        out
    }

    /// Sum of all observations, in microseconds.
    pub fn sum_us(&self) -> u64 {
        self.sum_us.load(Ordering::Relaxed)
    }

    /// Number of observations (one pass over the buckets).
    pub fn count(&self) -> u64 {
        self.snapshot().count
    }

    /// Buckets, sum and count from one read, with `count` equal to the `+Inf` bucket by
    /// construction (see the module doc).
    pub fn snapshot(&self) -> HistogramSnapshot {
        let buckets = self.cumulative();
        let count = buckets.last().map_or(0, |&(_, c)| c);
        HistogramSnapshot {
            buckets,
            sum_us: self.sum_us(),
            count,
        }
    }
}

/// Microseconds as an exact decimal number of seconds: `25` → `"0.000025"`, `1_500_000` → `"1.5"`.
pub fn fmt_seconds(us: u64) -> String {
    let whole = us / 1_000_000;
    let frac = us % 1_000_000;
    if frac == 0 {
        return whole.to_string();
    }
    let digits = format!("{frac:06}");
    format!("{whole}.{}", digits.trim_end_matches('0'))
}

/// SPEC §13 "checkout p50/p99": bounds for the CHECKOUT duration — permit wait plus recycle hygiene
/// or a fresh dial, i.e. the `queue_us` every EXEC already reports. Dense below a millisecond
/// because §16 sets the target at p50 < 60 µs and an operator needs to see which side of it the
/// pool sits.
///
/// **The tail runs PAST the default 5 s `checkout_timeout`, because a successful checkout can.**
/// `checkout_timeout` bounds the permit wait, the dial and EACH recycle cleanup separately (see
/// `Pool::checkout_declared`), so a checkout that waits for a permit and then recycles a slow
/// connection succeeds after more than one timeout's worth — measured in review at 243 ms against a
/// 150 ms `checkout_timeout`. Only SUCCESSFUL checkouts are observed here; one that fails ends in
/// an error terminal, which `ferro_errors_total` counts by code.
pub const CHECKOUT_BOUNDS_US: [u64; 19] = [
    25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000,
];

/// SPEC §13 "pin duration": bounds for how long a connection stays pinned to a transaction. Wider
/// and coarser than checkout, from a millisecond (a one-statement transaction) up, because the
/// question it answers is "which transactions hold connections long enough to starve the pool".
/// The 60 s bound is `ferrod`'s default `max_tx` (§6 `max_tx_duration`), the deadline that ends a
/// pin by rolling it back; 120 s and 300 s are headroom for an operator who raised it.
pub const PIN_BOUNDS_US: [u64; 13] = [
    1_000,
    5_000,
    10_000,
    50_000,
    100_000,
    500_000,
    1_000_000,
    5_000_000,
    10_000_000,
    30_000_000,
    60_000_000,
    120_000_000,
    300_000_000,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_land_in_the_inclusive_bucket_and_accumulate() {
        let h = Histogram::new([10, 100, 1_000]);
        for us in [5, 10, 11, 100, 999, 1_000, 1_001, 50_000] {
            h.observe_us(us);
        }
        // le=10: {5,10}; le=100: +{11,100}; le=1000: +{999,1000}; +Inf: +{1001,50000}
        assert_eq!(
            h.cumulative(),
            vec![(Some(10), 2), (Some(100), 4), (Some(1_000), 6), (None, 8)],
        );
        assert_eq!(h.count(), 8);
        assert_eq!(h.sum_us(), 5 + 10 + 11 + 100 + 999 + 1_000 + 1_001 + 50_000);
    }

    /// `_count` must equal the `+Inf` bucket in EVERY scrape, including one taken while other
    /// threads observe. A count kept in its own atomic broke this in roughly three reads in four
    /// under exactly this load (review finding F4); deriving it from the same pass cannot.
    #[test]
    fn a_snapshot_taken_during_observation_keeps_count_equal_to_the_inf_bucket() {
        let h = std::sync::Arc::new(Histogram::new(CHECKOUT_BOUNDS_US));
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let workers: Vec<_> = (0..4u64)
            .map(|t| {
                let (h, stop) = (h.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut us = t;
                    while !stop.load(Ordering::Relaxed) {
                        h.observe_us(us % 40_000_000);
                        us = us
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                    }
                })
            })
            .collect();
        let mut reads = 0u64;
        while reads < 20_000 {
            let s = h.snapshot();
            assert_eq!(s.buckets.last().map(|&(_, c)| c), Some(s.count));
            assert!(
                s.buckets.windows(2).all(|w| w[0].1 <= w[1].1),
                "cumulative is monotone"
            );
            reads += 1;
        }
        stop.store(true, Ordering::Relaxed);
        for w in workers {
            w.join().unwrap();
        }
        assert!(h.count() > 0, "the observers ran");
    }

    #[test]
    fn seconds_render_exactly() {
        assert_eq!(fmt_seconds(25), "0.000025");
        assert_eq!(fmt_seconds(250), "0.00025");
        assert_eq!(fmt_seconds(1_000), "0.001");
        assert_eq!(fmt_seconds(1_500_000), "1.5");
        assert_eq!(fmt_seconds(5_000_000), "5");
        assert_eq!(fmt_seconds(0), "0");
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn misordered_bounds_are_refused() {
        let _ = Histogram::new([10, 10, 20]);
    }

    /// The shipped bounds are themselves valid — constructing them is the check.
    #[test]
    fn the_shipped_bounds_are_strictly_increasing() {
        let _ = Histogram::new(CHECKOUT_BOUNDS_US);
        let _ = Histogram::new(PIN_BOUNDS_US);
    }
}
