//! Latency-distribution statistics over a sample of per-call durations in nanoseconds.
//!
//! **Percentile method: nearest-rank (SPEC §16.1 "boundary latency").** For a sorted sample of `N`
//! observations (ascending, 1-indexed), the `p`-th percentile is the value at rank
//! `ceil(p/100 * N)`, clamped to `[1, N]`. This is the classic nearest-rank definition (no
//! interpolation between neighbours): every reported percentile is an ACTUAL observed sample value,
//! never a synthetic average of two samples. That matters for an honest tail — a p99 we report is a
//! real call that actually happened, which is exactly what SPEC §16.1's "p99 boundary latency"
//! asks for. With `M = 100_000` samples the buckets are fine enough that p999 is meaningful.
//!
//! Worked example (the unit test below): the sample `1..=100` (N = 100) yields
//! p50 = 50 (rank `ceil(50)` = 50), p90 = 90, p99 = 99, p999 = 100 (rank `ceil(99.9)` = 100),
//! max = 100, min = 1, mean = 50.5.

use serde::{Deserialize, Serialize};

/// A latency distribution summary in nanoseconds (except `mean`, an f64 average of ns).
///
/// All percentile / min / max fields are ACTUAL observed sample values (nearest-rank). `mean` is
/// the arithmetic mean of the raw samples as an f64 so it is not silently truncated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    /// Number of measured samples the summary was computed from.
    pub samples_n: usize,
    pub min: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

/// The `p`-th percentile of a pre-sorted (ascending) ns sample by the nearest-rank method.
///
/// `sorted` MUST be sorted ascending. `p` is in `[0.0, 100.0]`. An empty sample yields `0` (the
/// caller is expected to guard against empty samples via [`Summary`]'s `samples_n`, but returning
/// `0` keeps this a total function rather than a panic).
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    let n = sorted.len();
    if n == 0 {
        return 0;
    }
    // rank = ceil(p/100 * N), clamped into [1, N]; index is rank - 1 (0-based).
    let rank = ((p / 100.0) * n as f64).ceil() as usize;
    let rank = rank.clamp(1, n);
    sorted[rank - 1]
}

/// Summarize a raw (unsorted) ns sample. Clones + sorts internally so the caller's buffer is left
/// untouched (an 800 KB clone for the M = 100k sample is negligible next to the bench run itself).
pub fn summarize(samples: &[u64]) -> Summary {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let mean = if n == 0 {
        0.0
    } else {
        // Sum in u128 so 100k * (worst-case ~ms in ns) cannot overflow before the division.
        let sum: u128 = sorted.iter().map(|&v| v as u128).sum();
        (sum as f64) / (n as f64)
    };
    Summary {
        samples_n: n,
        min: sorted.first().copied().unwrap_or(0),
        mean,
        p50: percentile(&sorted, 50.0),
        p90: percentile(&sorted, 90.0),
        p99: percentile(&sorted, 99.0),
        p999: percentile(&sorted, 99.9),
        max: sorted.last().copied().unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The documented worked example: the exact-integer vector 1..=100 pins every reported
    /// statistic to a known nearest-rank value, so a regression in the rank formula is caught.
    #[test]
    fn nearest_rank_on_1_to_100() {
        let sample: Vec<u64> = (1..=100).collect();
        let s = summarize(&sample);
        assert_eq!(s.samples_n, 100);
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 100);
        assert_eq!(s.p50, 50, "ceil(0.50*100)=50 -> sample[50]=50");
        assert_eq!(s.p90, 90, "ceil(0.90*100)=90 -> sample[90]=90");
        assert_eq!(s.p99, 99, "ceil(0.99*100)=99 -> sample[99]=99");
        assert_eq!(
            s.p999, 100,
            "ceil(0.999*100)=ceil(99.9)=100 -> sample[100]=100"
        );
        assert!(
            (s.mean - 50.5).abs() < 1e-9,
            "mean of 1..=100 is 50.5, got {}",
            s.mean
        );
    }

    #[test]
    fn percentile_boundaries() {
        let sample: Vec<u64> = (1..=100).collect();
        // p=0 clamps rank to 1 (the min); p=100 is rank N (the max).
        assert_eq!(percentile(&sample, 0.0), 1);
        assert_eq!(percentile(&sample, 100.0), 100);
    }

    #[test]
    fn summarize_is_order_independent() {
        // A reverse-sorted input must produce the same summary as the ascending one.
        let asc: Vec<u64> = (1..=100).collect();
        let desc: Vec<u64> = (1..=100).rev().collect();
        assert_eq!(summarize(&asc), summarize(&desc));
    }

    #[test]
    fn empty_sample_is_total_not_a_panic() {
        let s = summarize(&[]);
        assert_eq!(s.samples_n, 0);
        assert_eq!(s.p99, 0);
        assert_eq!(percentile(&[], 99.0), 0);
    }

    #[test]
    fn single_sample() {
        let s = summarize(&[42]);
        assert_eq!(s.min, 42);
        assert_eq!(s.max, 42);
        assert_eq!(s.p50, 42);
        assert_eq!(s.p999, 42);
        assert!((s.mean - 42.0).abs() < 1e-9);
    }
}
