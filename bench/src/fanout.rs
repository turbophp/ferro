//! The §16 fan-out result (M3-D1e): "10-query fan-out latency ≤ max(single query) + 2 ms under
//! Fibers", recorded as its own self-contained JSON beside the D12 trivial-call result.
//!
//! **What is compared.** `bench_fanout.php` runs k identical `SELECT 1 FROM pg_sleep(s)` statements
//! per iteration, so "max(single query)" is the latency of ONE such statement. Each iteration times,
//! back to back on one connection, a single synchronous call, a k-way fan-out under
//! `Ferro\Loop::run` (the §16 shape, "under Fibers"), and the same k awaited with `Ferro\await()`
//! (plain FPM, no scheduler). The target is evaluated on the DISTRIBUTIONS: at each percentile,
//! `fanout_pX − single_pX ≤ 2 ms`. The per-iteration paired difference is recorded too, but it is
//! not the verdict: it carries both samples' noise, so its tail overstates the cost of fan-out.
//!
//! **Why a sleep.** With `SELECT 1` the "slowest query" is mostly client and socket time, so the
//! measurement would be about serialization rather than overlap. A fixed server-side sleep makes the
//! answer unambiguous: k sequential statements cost k × s, k overlapped ones cost about s.

use serde::{Deserialize, Serialize};

use crate::manifest::Manifest;
use crate::stats::Summary;

/// Bumped whenever the on-disk fan-out shape changes.
pub const FANOUT_SCHEMA_VERSION: u32 = 1;
/// SPEC §16's budget: fan-out may cost at most 2 ms more than the slowest single query.
pub const BUDGET_NS: i64 = 2_000_000;
/// The §16 fan-out width.
pub const K: usize = 10;
/// The server-side sleep each statement carries, in ms.
pub const SLEEP_MS: u64 = 10;
/// Warmup iterations (each one dials pool connections up to k on the first pass).
pub const WARMUP: usize = 100;
/// Measured iterations. Each costs about 3 × (sleep + overhead), so ~1 minute in total.
pub const MEASURED: usize = 2_000;

/// A distribution of SIGNED nanosecond differences (nearest-rank, like [`Summary`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeltaSummary {
    pub samples_n: usize,
    pub min: i64,
    pub p50: i64,
    pub p90: i64,
    pub p99: i64,
    pub max: i64,
}

/// Nearest-rank summary of a signed sample (the same rank rule as `stats::percentile`).
pub fn summarize_delta(samples: &[i64]) -> DeltaSummary {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let pick = |p: f64| -> i64 {
        if n == 0 {
            return 0;
        }
        let rank = ((p / 100.0) * n as f64).ceil() as usize;
        sorted[rank.clamp(1, n) - 1]
    };
    DeltaSummary {
        samples_n: n,
        min: sorted.first().copied().unwrap_or(0),
        p50: pick(50.0),
        p90: pick(90.0),
        p99: pick(99.0),
        max: sorted.last().copied().unwrap_or(0),
    }
}

/// One fan-out mode's measurement against the single-query distribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeResult {
    /// `"fibers"` (`Ferro\Loop::run`, the §16 shape) or `"await"` (`Ferro\await()`, plain FPM).
    pub mode: String,
    pub summary: Summary,
    /// `fanout_p50 − single_p50`, ns.
    pub p50_over_single_ns: i64,
    /// `fanout_p99 − single_p99`, ns.
    pub p99_over_single_ns: i64,
    /// Per-iteration `fanout − single` (diagnostic, not the verdict — see the module doc).
    pub paired_delta: DeltaSummary,
    /// `p50_over_single_ns ≤ budget` AND `p99_over_single_ns ≤ budget`.
    pub met: bool,
}

/// Run parameters, recorded so the result is auditable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutParams {
    pub k: usize,
    pub sleep_ms: u64,
    pub warmup: usize,
    pub measured: usize,
    pub transport: String,
    /// The pool size the daemon ran with; it must be ≥ k or the fan-out queues on checkout.
    pub pool_max_size: usize,
    pub php_directives: Vec<String>,
    pub jit_effective: String,
}

/// The whole fan-out record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutResult {
    pub schema_version: u32,
    pub scenario: String,
    /// Where it was measured: `"gh-ubuntu-latest"` on the D17 reference runner, else a local label.
    pub env: String,
    /// `true` only on the D17 reference runner (a GitHub-hosted runner, SPEC §21 D17).
    pub reference: bool,
    pub budget_ns: i64,
    pub params: FanoutParams,
    pub manifest: Manifest,
    pub single: Summary,
    pub modes: Vec<ModeResult>,
}

impl FanoutResult {
    /// Build the record from the raw per-iteration samples (all three of equal length).
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        env: String,
        reference: bool,
        params: FanoutParams,
        manifest: Manifest,
        single: &[u64],
        fibers: &[u64],
        await_: &[u64],
    ) -> Self {
        let single_s = crate::stats::summarize(single);
        let modes = [("fibers", fibers), ("await", await_)]
            .into_iter()
            .map(|(mode, samples)| mode_result(mode, samples, single, &single_s))
            .collect();
        FanoutResult {
            schema_version: FANOUT_SCHEMA_VERSION,
            scenario: "fanout".to_string(),
            env,
            reference,
            budget_ns: BUDGET_NS,
            params,
            manifest,
            single: single_s,
            modes,
        }
    }

    /// The structural honesty check: refuses a record that would be a meaningless number.
    pub fn validate(&self) -> Result<(), String> {
        let n = self.params.measured;
        if n == 0 || self.single.samples_n != n {
            return Err(format!(
                "single recorded {} samples, expected {n}",
                self.single.samples_n
            ));
        }
        let mut seen: Vec<&str> = self.modes.iter().map(|m| m.mode.as_str()).collect();
        seen.sort_unstable();
        if seen != ["await", "fibers"] {
            return Err(format!("expected modes await + fibers, found {seen:?}"));
        }
        for m in &self.modes {
            if m.summary.samples_n != n || m.paired_delta.samples_n != n {
                return Err(format!("mode '{}' sample count != {n}", m.mode));
            }
        }
        if self.params.pool_max_size < self.params.k {
            return Err(format!(
                "pool max size {} < k {} — the fan-out would queue on checkout",
                self.params.pool_max_size, self.params.k
            ));
        }
        // A fan-out that did not overlap is not a measurement of fan-out. Sequential execution
        // costs at least k × sleep; anything that slow at the median means the k statements ran
        // one after another, whatever the budget arithmetic says.
        let sequential_ns = self.params.k as u64 * self.params.sleep_ms * 1_000_000;
        for m in &self.modes {
            if m.summary.p50 >= sequential_ns {
                return Err(format!(
                    "mode '{}' p50 {} ns ≥ k × sleep ({sequential_ns} ns): the statements did \
                     not overlap",
                    m.mode, m.summary.p50
                ));
            }
        }
        if self.manifest.ferrod_build_profile != "release" {
            return Err("ferrod_build_profile must be 'release' [V1]".to_string());
        }
        for (name, v) in [
            ("git_sha", &self.manifest.git_sha),
            ("cpu_model", &self.manifest.cpu_model),
            ("php_version", &self.manifest.php_version),
            ("timestamp_utc", &self.manifest.timestamp_utc),
        ] {
            if v.is_empty() {
                return Err(format!("manifest field '{name}' is empty"));
            }
        }
        Ok(())
    }
}

fn mode_result(mode: &str, samples: &[u64], single: &[u64], single_s: &Summary) -> ModeResult {
    let summary = crate::stats::summarize(samples);
    let paired: Vec<i64> = samples
        .iter()
        .zip(single)
        .map(|(&f, &s)| f as i64 - s as i64)
        .collect();
    let p50_over = summary.p50 as i64 - single_s.p50 as i64;
    let p99_over = summary.p99 as i64 - single_s.p99 as i64;
    ModeResult {
        mode: mode.to_string(),
        summary,
        p50_over_single_ns: p50_over,
        p99_over_single_ns: p99_over,
        paired_delta: summarize_delta(&paired),
        met: p50_over <= BUDGET_NS && p99_over <= BUDGET_NS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        let mut m = crate::manifest::collect_host();
        m.ferrod_build_profile = "release".into();
        m.git_sha = "abc".into();
        m.cpu_model = "cpu".into();
        m.php_version = "8.4".into();
        m.timestamp_utc = "now".into();
        m
    }

    fn params(n: usize) -> FanoutParams {
        FanoutParams {
            k: K,
            sleep_ms: SLEEP_MS,
            warmup: 1,
            measured: n,
            transport: "UDS".into(),
            pool_max_size: 16,
            php_directives: vec![],
            jit_effective: "on".into(),
        }
    }

    const MS: u64 = 1_000_000;

    #[test]
    fn delta_summary_is_nearest_rank_and_signed() {
        let d = summarize_delta(&(-50..50).collect::<Vec<i64>>());
        assert_eq!((d.min, d.max), (-50, 49));
        assert_eq!(d.p50, -1, "rank ceil(0.5*100)=50 → the 50th value, -1");
        assert_eq!(d.p99, 48);
    }

    #[test]
    fn the_budget_is_judged_at_p50_and_p99() {
        let n = 100;
        let single = vec![11 * MS; n];
        // Within budget everywhere.
        let ok = vec![12 * MS; n];
        // Median within budget, tail 5 ms over.
        let mut tail = vec![12 * MS; n];
        // Nearest rank: p99 of 100 samples is the 99th value, so the top two are slow.
        tail[n - 2] = 16 * MS;
        tail[n - 1] = 16 * MS;
        let r = FanoutResult::build(
            "t".into(),
            false,
            params(n),
            manifest(),
            &single,
            &ok,
            &tail,
        );
        r.validate().unwrap();
        let by = |m: &str| r.modes.iter().find(|x| x.mode == m).unwrap().clone();
        assert!(by("fibers").met);
        assert_eq!(by("fibers").p50_over_single_ns, MS as i64);
        assert!(!by("await").met, "a p99 5 ms over the single p99 misses");
        assert_eq!(by("await").p99_over_single_ns, 5 * MS as i64);
    }

    #[test]
    fn a_fanout_that_did_not_overlap_is_refused() {
        let n = 10;
        let single = vec![11 * MS; n];
        let sequential = vec![K as u64 * SLEEP_MS * MS + MS; n];
        let r = FanoutResult::build(
            "t".into(),
            false,
            params(n),
            manifest(),
            &single,
            &sequential,
            &single,
        );
        let e = r.validate().unwrap_err();
        assert!(e.contains("did not overlap"), "{e}");
    }

    #[test]
    fn a_short_pool_is_refused() {
        let n = 10;
        let s = vec![11 * MS; n];
        let mut p = params(n);
        p.pool_max_size = 8;
        let r = FanoutResult::build("t".into(), false, p, manifest(), &s, &s, &s);
        assert!(r.validate().unwrap_err().contains("queue on checkout"));
    }

    #[test]
    fn a_wrong_sample_count_is_refused() {
        let s = vec![11 * MS; 10];
        let r = FanoutResult::build("t".into(), false, params(11), manifest(), &s, &s, &s);
        assert!(r.validate().is_err());
    }
}
