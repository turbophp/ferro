//! The §16 fan-out result (M3-D1e): "10-query fan-out latency ≤ max(single query) + 2 ms under
//! Fibers", recorded as its own self-contained JSON beside the D12 trivial-call result.
//!
//! **What is compared.** `bench_fanout.php` runs k identical `SELECT pg_backend_pid() FROM
//! pg_sleep(s)` statements per iteration. Each iteration times, back to back on one connection, a
//! single synchronous call, a k-way fan-out under `Ferro\Loop::run` (the §16 shape, "under Fibers"),
//! and the same k awaited with `Ferro\await()` (plain FPM, no scheduler).
//!
//! **The comparator is ONE single query's distribution — the conservative reading.** §16 says
//! "max(single query)"; since all k statements are identical, the latency of one of them stands in
//! for it. The literal reading — the slowest of k independent single calls — is strictly larger
//! (it is a max over k draws), so judging against one call can only turn a MET into a MISSED, never
//! the reverse. The max-of-k figure is recorded beside it as `max_of_k_single` for context, not as
//! the verdict.
//!
//! **The verdict carries its own uncertainty.** A shared runner's p99 at M = 2000 is noisy at the
//! 2 ms scale (the review measured a 95% interval wider than the budget, and one statement through
//! the async path alone drawing +2.6 ms at p99), so a point estimate's MET/MISSED is not
//! reproducible. Each `*_over_single` therefore carries a 95% bootstrap interval, and the budget is
//! MET at a percentile only when the interval's UPPER bound is within it. D17's rule for two runs:
//! the target is met only if both runs meet it. The raw samples are kept so the record can be
//! re-analysed.
//!
//! **Overlap is proven, not inferred.** Every fan-out records how many DISTINCT backend pids
//! answered it; `validate()` refuses a record in which any fan-out was served by fewer than k
//! sessions — a half-serialized fan-out would otherwise pass any timing threshold loose enough to
//! survive noise.

use serde::{Deserialize, Serialize};

use crate::manifest::Manifest;
use crate::stats::Summary;

/// Bumped whenever the on-disk fan-out shape changes.
pub const FANOUT_SCHEMA_VERSION: u32 = 2;
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
/// Bootstrap resamples per interval.
pub const BOOTSTRAP: usize = 1_000;
/// The env label of the D17 reference runner (`FERRO_BENCH_ENV` in `bench.yml`).
pub const REFERENCE_ENV: &str = "gh-ubuntu-latest";
/// D17's own caveat, carried in every record.
pub const D17_NOTE: &str = "a shared GitHub-hosted runner is noisier than bare metal: a p99 \
     measured there is an upper bound on the target's p99 (SPEC §21 D17)";

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

fn rank_pick<T: Copy + Default>(sorted: &[T], p: f64) -> T {
    let n = sorted.len();
    if n == 0 {
        return T::default();
    }
    let rank = ((p / 100.0) * n as f64).ceil() as usize;
    sorted[rank.clamp(1, n) - 1]
}

/// Nearest-rank summary of a signed sample (the same rank rule as `stats::percentile`).
pub fn summarize_delta(samples: &[i64]) -> DeltaSummary {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    DeltaSummary {
        samples_n: sorted.len(),
        min: sorted.first().copied().unwrap_or(0),
        p50: rank_pick(&sorted, 50.0),
        p90: rank_pick(&sorted, 90.0),
        p99: rank_pick(&sorted, 99.0),
        max: sorted.last().copied().unwrap_or(0),
    }
}

/// A point estimate with its 95% bootstrap interval, all in ns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Estimate {
    pub point: i64,
    pub ci95_low: i64,
    pub ci95_high: i64,
}

/// A deterministic xorshift64* generator, so a record's intervals are reproducible from its samples.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn pct_of_resample(rng: &mut Rng, xs: &[u64], buf: &mut Vec<u64>, p: f64) -> u64 {
    buf.clear();
    buf.extend((0..xs.len()).map(|_| xs[rng.below(xs.len())]));
    buf.sort_unstable();
    rank_pick(buf, p)
}

/// `pct(fan) − pct(single)` with a 95% interval from `rounds` independent resamples of each sample.
pub fn over_single(fan: &[u64], single: &[u64], p: f64, rounds: usize, seed: u64) -> Estimate {
    let pct = |xs: &[u64]| {
        let mut s = xs.to_vec();
        s.sort_unstable();
        rank_pick(&s, p)
    };
    let point = pct(fan) as i64 - pct(single) as i64;
    let mut rng = Rng(seed | 1);
    let (mut a, mut b) = (Vec::new(), Vec::new());
    let mut diffs: Vec<i64> = (0..rounds)
        .map(|_| {
            pct_of_resample(&mut rng, fan, &mut a, p) as i64
                - pct_of_resample(&mut rng, single, &mut b, p) as i64
        })
        .collect();
    diffs.sort_unstable();
    Estimate {
        point,
        ci95_low: rank_pick(&diffs, 2.5),
        ci95_high: rank_pick(&diffs, 97.5),
    }
}

/// The distribution of the slowest of `k` single calls drawn with replacement — the literal
/// "max(single query)" — summarized at p50/p99 (context only; see the module doc).
pub fn max_of_k(single: &[u64], k: usize, rounds: usize, seed: u64) -> (u64, u64) {
    let mut rng = Rng(seed | 1);
    let mut maxes: Vec<u64> = (0..rounds)
        .map(|_| {
            (0..k)
                .map(|_| single[rng.below(single.len())])
                .max()
                .unwrap_or(0)
        })
        .collect();
    maxes.sort_unstable();
    (rank_pick(&maxes, 50.0), rank_pick(&maxes, 99.0))
}

/// MET at a percentile only when the interval's upper bound is within the budget.
pub fn met(e: &Estimate) -> bool {
    e.ci95_high <= BUDGET_NS
}

/// One fan-out mode's measurement against the single-query distribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeResult {
    /// `"fibers"` (`Ferro\Loop::run`, the §16 shape) or `"await"` (`Ferro\await()`, plain FPM).
    pub mode: String,
    pub summary: Summary,
    pub p50_over_single: Estimate,
    pub p99_over_single: Estimate,
    /// Per-iteration `fanout − single` (diagnostic, not the verdict: it carries both samples' noise).
    pub paired_delta: DeltaSummary,
    /// The fewest distinct backend sessions that answered any one fan-out (must be k).
    pub min_distinct_backends: usize,
    pub met_p50: bool,
    pub met_p99: bool,
}

/// Run parameters, recorded so the result is auditable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutParams {
    pub k: usize,
    pub sleep_ms: u64,
    pub warmup: usize,
    pub measured: usize,
    pub bootstrap_rounds: usize,
    pub transport: String,
    pub php_directives: Vec<String>,
    pub jit_effective: String,
}

/// The raw per-iteration samples, kept so the record can be re-analysed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawSamples {
    pub single_ns: Vec<u64>,
    pub fibers_ns: Vec<u64>,
    pub await_ns: Vec<u64>,
    pub fibers_distinct_backends: Vec<usize>,
    pub await_distinct_backends: Vec<usize>,
}

/// The whole fan-out record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutResult {
    pub schema_version: u32,
    pub scenario: String,
    /// Where it was measured: [`REFERENCE_ENV`] on the D17 reference runner, else a local label.
    pub env: String,
    /// `true` only on the D17 reference runner (a GitHub-hosted runner, SPEC §21 D17).
    pub reference: bool,
    pub note: String,
    pub budget_ns: i64,
    pub params: FanoutParams,
    pub manifest: Manifest,
    pub single: Summary,
    /// p50/p99 of the slowest of k single calls (context; see the module doc).
    pub max_of_k_single_p50_ns: u64,
    pub max_of_k_single_p99_ns: u64,
    pub modes: Vec<ModeResult>,
    pub raw: RawSamples,
}

impl FanoutResult {
    /// Build the record from the raw per-iteration samples.
    pub fn build(
        env: String,
        reference: bool,
        params: FanoutParams,
        manifest: Manifest,
        raw: RawSamples,
    ) -> Self {
        let single = crate::stats::summarize(&raw.single_ns);
        let rounds = params.bootstrap_rounds;
        let modes = [
            ("fibers", &raw.fibers_ns, &raw.fibers_distinct_backends, 1),
            ("await", &raw.await_ns, &raw.await_distinct_backends, 2),
        ]
        .into_iter()
        .map(|(mode, ns, distinct, seed)| {
            let p50 = over_single(ns, &raw.single_ns, 50.0, rounds, seed);
            let p99 = over_single(ns, &raw.single_ns, 99.0, rounds, seed + 100);
            let paired: Vec<i64> = ns
                .iter()
                .zip(&raw.single_ns)
                .map(|(&f, &s)| f as i64 - s as i64)
                .collect();
            ModeResult {
                mode: mode.to_string(),
                summary: crate::stats::summarize(ns),
                met_p50: met(&p50),
                met_p99: met(&p99),
                p50_over_single: p50,
                p99_over_single: p99,
                paired_delta: summarize_delta(&paired),
                min_distinct_backends: distinct.iter().copied().min().unwrap_or(0),
            }
        })
        .collect();
        let (mk50, mk99) = max_of_k(&raw.single_ns, params.k, rounds, 7);
        FanoutResult {
            schema_version: FANOUT_SCHEMA_VERSION,
            scenario: "fanout".to_string(),
            env,
            reference,
            note: D17_NOTE.to_string(),
            budget_ns: BUDGET_NS,
            params,
            manifest,
            single,
            max_of_k_single_p50_ns: mk50,
            max_of_k_single_p99_ns: mk99,
            modes,
            raw,
        }
    }

    /// The structural honesty check: refuses a record that would be a meaningless number.
    pub fn validate(&self) -> Result<(), String> {
        let n = self.params.measured;
        let raw = &self.raw;
        for (name, len) in [
            ("single_ns", raw.single_ns.len()),
            ("fibers_ns", raw.fibers_ns.len()),
            ("await_ns", raw.await_ns.len()),
            (
                "fibers_distinct_backends",
                raw.fibers_distinct_backends.len(),
            ),
            ("await_distinct_backends", raw.await_distinct_backends.len()),
        ] {
            if n == 0 || len != n {
                return Err(format!("raw.{name} has {len} samples, expected {n}"));
            }
        }
        let mut seen: Vec<&str> = self.modes.iter().map(|m| m.mode.as_str()).collect();
        seen.sort_unstable();
        if seen != ["await", "fibers"] {
            return Err(format!("expected modes await + fibers, found {seen:?}"));
        }
        // Overlap, proven per iteration: every fan-out must have been answered by k sessions.
        for m in &self.modes {
            if m.min_distinct_backends != self.params.k {
                return Err(format!(
                    "mode '{}': a fan-out was answered by only {} distinct backend sessions, not \
                     k = {} — the statements did not all overlap",
                    m.mode, m.min_distinct_backends, self.params.k
                ));
            }
        }
        if self.params.jit_effective != "on" {
            return Err(format!(
                "JIT intended on but effective '{}' [V6]",
                self.params.jit_effective
            ));
        }
        if self.params.k != K || self.params.sleep_ms != SLEEP_MS || self.budget_ns != BUDGET_NS {
            return Err("k, sleep or budget differs from the §16 constants".to_string());
        }
        // `reference` comes from the runner's own variables; the label must agree with it.
        if self.reference != (self.env == REFERENCE_ENV) {
            return Err(format!(
                "reference={} but env='{}': only the D17 runner ('{REFERENCE_ENV}') is the reference",
                self.reference, self.env
            ));
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

/// D17: the reference environment is a GitHub-HOSTED runner, read from the variables the runner
/// itself sets (`GITHUB_ACTIONS`, `RUNNER_ENVIRONMENT`), never from the operator-chosen label.
pub fn is_reference_runner(github_actions: Option<&str>, runner_environment: Option<&str>) -> bool {
    github_actions == Some("true") && runner_environment == Some("github-hosted")
}

/// Where this run happened: an explicit `FERRO_BENCH_ENV` wins (blank reads as unset); otherwise
/// `wsl2` or `local`.
pub fn env_label(explicit: Option<&str>, virtualization: &str) -> String {
    match explicit.map(str::trim) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ if virtualization == "WSL2" => "wsl2".to_string(),
        _ => "local".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

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
            bootstrap_rounds: 200,
            transport: "UDS".into(),
            php_directives: vec![],
            jit_effective: "on".into(),
        }
    }

    fn raw(single: Vec<u64>, fibers: Vec<u64>, await_: Vec<u64>) -> RawSamples {
        let n = single.len();
        RawSamples {
            single_ns: single,
            fibers_ns: fibers,
            await_ns: await_,
            fibers_distinct_backends: vec![K; n],
            await_distinct_backends: vec![K; n],
        }
    }

    fn build(p: FanoutParams, r: RawSamples) -> FanoutResult {
        FanoutResult::build("local".into(), false, p, manifest(), r)
    }

    fn mode(r: &FanoutResult, m: &str) -> ModeResult {
        r.modes.iter().find(|x| x.mode == m).unwrap().clone()
    }

    #[test]
    fn the_budget_is_spec_16s_two_milliseconds() {
        assert_eq!(BUDGET_NS, 2_000_000, "SPEC §16: ≤ max(single query) + 2 ms");
        assert_eq!(K, 10, "SPEC §16: a 10-query fan-out");
    }

    #[test]
    fn delta_summary_is_nearest_rank_signed_and_sorts_its_input() {
        let mut v: Vec<i64> = (-50..50).collect();
        v.reverse();
        let d = summarize_delta(&v);
        assert_eq!((d.min, d.max), (-50, 49));
        assert_eq!(d.p50, -1, "rank ceil(0.5*100)=50 → the 50th value, -1");
        assert_eq!(d.p90, 39);
        assert_eq!(d.p99, 48);
    }

    #[test]
    fn a_constant_sample_has_a_degenerate_interval_at_the_point() {
        let e = over_single(&[12 * MS; 50], &[11 * MS; 50], 99.0, 100, 3);
        assert_eq!(
            e,
            Estimate {
                point: MS as i64,
                ci95_low: MS as i64,
                ci95_high: MS as i64
            }
        );
    }

    #[test]
    fn the_estimate_compares_the_same_percentile_of_both_samples() {
        // single: 1..=100 ms; fan: single + 1 ms everywhere except a p99 bump.
        let single: Vec<u64> = (1..=100).map(|i| i * MS).collect();
        let mut fan: Vec<u64> = single.iter().map(|s| s + MS).collect();
        // Nearest rank: p99 of 100 is the 99th value, so bump the top two to move it.
        fan[98] += 10 * MS;
        fan[99] += 10 * MS;
        let p50 = over_single(&fan, &single, 50.0, 1, 1);
        let p99 = over_single(&fan, &single, 99.0, 1, 1);
        assert_eq!(p50.point, MS as i64);
        assert_eq!(p99.point, 11 * MS as i64);
    }

    #[test]
    fn met_is_judged_on_the_interval_upper_bound_inclusive() {
        let at = |h: i64| Estimate {
            point: 0,
            ci95_low: 0,
            ci95_high: h,
        };
        assert!(met(&at(BUDGET_NS)), "exactly at the budget is within it");
        assert!(!met(&at(BUDGET_NS + 1)));
        assert!(
            !met(&Estimate {
                point: BUDGET_NS - 1,
                ci95_low: 0,
                ci95_high: BUDGET_NS + 1
            }),
            "a point estimate inside the budget is not enough when the interval leaves it"
        );
    }

    #[test]
    fn the_verdict_is_per_percentile_and_per_mode() {
        let n = 100;
        let single = vec![11 * MS; n];
        let ok = vec![12 * MS; n];
        let mut tail = vec![12 * MS; n];
        tail[n - 2] = 16 * MS;
        tail[n - 1] = 16 * MS;
        let r = build(params(n), raw(single, ok, tail));
        r.validate().unwrap();
        let (f, a) = (mode(&r, "fibers"), mode(&r, "await"));
        assert!(f.met_p50 && f.met_p99);
        assert!(a.met_p50, "the median is within budget");
        assert!(!a.met_p99, "a p99 well over the single p99 misses");
        assert_eq!(a.p99_over_single.point, 5 * MS as i64);
    }

    #[test]
    fn each_mode_estimate_is_taken_at_its_own_percentile() {
        // A spread sample, so p50, p90 and p99 all differ.
        let single: Vec<u64> = (1..=100).map(|i| i * MS).collect();
        let fan: Vec<u64> = (1..=100).map(|i| (i * 2) * MS).collect();
        let r = build(params(100), raw(single.clone(), fan.clone(), single));
        let f = mode(&r, "fibers");
        assert_eq!(
            f.p50_over_single.point,
            50 * MS as i64,
            "p50: 100 ms − 50 ms"
        );
        assert_eq!(
            f.p99_over_single.point,
            99 * MS as i64,
            "p99: 198 ms − 99 ms"
        );
    }

    #[test]
    fn a_point_estimate_inside_the_budget_with_a_wide_interval_is_missed() {
        // One slow outlier among 100: the p99 POINT is the fast value (+1 ms), but a resample that
        // draws the outlier twice moves p99 to it, so the interval's upper bound leaves the budget.
        let single = vec![10 * MS; 100];
        let mut fan = vec![11 * MS; 100];
        fan[99] = 30 * MS;
        let e = over_single(&fan, &single, 99.0, 400, 5);
        assert_eq!(e.point, MS as i64);
        assert!(e.ci95_low <= e.point);
        assert!(e.ci95_high > BUDGET_NS, "{e:?}");
        assert!(!met(&e));
    }

    #[test]
    fn the_paired_delta_is_fanout_minus_single_per_iteration() {
        let r = build(
            params(3),
            raw(
                vec![10 * MS, 20 * MS, 30 * MS],
                vec![11 * MS, 23 * MS, 30 * MS],
                vec![10 * MS; 3],
            ),
        );
        let d = mode(&r, "fibers").paired_delta;
        assert_eq!((d.min, d.max), (0, 3 * MS as i64));
        assert_eq!(d.p50, MS as i64);
        assert_eq!(mode(&r, "await").paired_delta.min, -20 * MS as i64);
    }

    #[test]
    fn max_of_k_is_at_least_the_single_distribution() {
        let single: Vec<u64> = (1..=100).map(|i| i * MS).collect();
        let (p50, p99) = max_of_k(&single, K, 500, 9);
        assert!(
            p50 > 50 * MS,
            "the slowest of 10 draws sits well above the median"
        );
        assert!(p99 >= p50);
    }

    #[test]
    fn a_fanout_served_by_fewer_than_k_sessions_is_refused_in_either_mode() {
        for which in ["fibers", "await"] {
            let n = 10;
            let s = vec![11 * MS; n];
            let mut r = raw(s.clone(), s.clone(), s);
            let v = if which == "fibers" {
                &mut r.fibers_distinct_backends
            } else {
                &mut r.await_distinct_backends
            };
            v[n - 1] = K - 1; // one fan-out among n that one session served twice
            let e = build(params(n), r).validate().unwrap_err();
            assert!(
                e.contains(which) && e.contains("did not all overlap"),
                "{e}"
            );
        }
    }

    #[test]
    fn every_sample_count_is_checked() {
        let n = 10;
        let s = vec![11 * MS; n];
        for field in 0..5 {
            let mut r = raw(s.clone(), s.clone(), s.clone());
            match field {
                0 => r.single_ns.pop().map(|_| ()),
                1 => r.fibers_ns.pop().map(|_| ()),
                2 => r.await_ns.pop().map(|_| ()),
                3 => r.fibers_distinct_backends.pop().map(|_| ()),
                _ => r.await_distinct_backends.pop().map(|_| ()),
            };
            let mut p = params(n);
            p.bootstrap_rounds = 10;
            let rec = FanoutResult::build("local".into(), false, p, manifest(), r);
            let e = rec.validate().unwrap_err();
            assert!(e.contains("samples, expected 10"), "field {field}: {e}");
        }
    }

    #[test]
    fn reference_must_agree_with_the_env_label() {
        let n = 5;
        let s = vec![11 * MS; n];
        let mk = |env: &str, reference: bool| {
            FanoutResult::build(
                env.into(),
                reference,
                params(n),
                manifest(),
                raw(s.clone(), s.clone(), s.clone()),
            )
        };
        mk(REFERENCE_ENV, true).validate().unwrap();
        mk("local", false).validate().unwrap();
        assert!(
            mk("local", true)
                .validate()
                .unwrap_err()
                .contains("reference")
        );
        assert!(
            mk(REFERENCE_ENV, false)
                .validate()
                .unwrap_err()
                .contains("reference")
        );
        assert!(
            mk(REFERENCE_ENV, true).reference,
            "build carries the tag through"
        );
    }

    #[test]
    fn other_refusals() {
        let n = 5;
        let s = vec![11 * MS; n];
        let base = || build(params(n), raw(s.clone(), s.clone(), s.clone()));
        let mut r = base();
        r.modes.pop();
        assert!(r.validate().unwrap_err().contains("expected modes"));
        let mut r = base();
        r.params.jit_effective = "off".into();
        assert!(r.validate().unwrap_err().contains("JIT"));
        let mut r = base();
        r.budget_ns = 3_000_000;
        assert!(r.validate().unwrap_err().contains("§16 constants"));
        let mut r = base();
        r.manifest.ferrod_build_profile = "debug".into();
        assert!(r.validate().unwrap_err().contains("release"));
        let mut r = base();
        r.manifest.git_sha.clear();
        assert!(r.validate().unwrap_err().contains("git_sha"));
        let r = base();
        assert_eq!(r.scenario, "fanout");
        assert_eq!(r.note, D17_NOTE);
    }

    /// Every committed fan-out record must still deserialize under the current shape and pass
    /// `validate()`; a reference record must also come from the D17 runner.
    #[test]
    fn every_committed_fanout_record_validates() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("results");
        let mut seen = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if !name.ends_with("-fanout.json") {
                continue;
            }
            let r: FanoutResult =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            r.validate().unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(
                r.reference,
                name.contains(&format!("-{REFERENCE_ENV}-")),
                "{name}"
            );
            seen += 1;
        }
        assert!(
            seen >= 2,
            "D17 needs at least two recorded runs, found {seen}"
        );
    }

    #[test]
    fn the_reference_runner_is_read_from_the_runners_own_variables() {
        assert!(is_reference_runner(Some("true"), Some("github-hosted")));
        assert!(!is_reference_runner(Some("true"), Some("self-hosted")));
        assert!(!is_reference_runner(None, Some("github-hosted")));
        assert!(!is_reference_runner(Some("false"), Some("github-hosted")));
    }

    #[test]
    fn the_env_label_prefers_an_explicit_value_and_treats_blank_as_unset() {
        assert_eq!(
            env_label(Some(" gh-ubuntu-latest "), "docker"),
            "gh-ubuntu-latest"
        );
        assert_eq!(env_label(Some("  "), "WSL2"), "wsl2");
        assert_eq!(env_label(None, "WSL2"), "wsl2");
        assert_eq!(env_label(None, "docker"), "local");
    }
}
