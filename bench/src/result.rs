//! The committed D12 bench result: a self-contained, schema-validated JSON carrying every measured
//! distribution + the full environment manifest. `validate()` is the STRUCTURAL HONESTY self-check
//! — it refuses to let the orchestrator write a result that would be a meaningless number (too few
//! samples, a JIT mode that silently failed to engage, a debug ferrod, a missing manifest field).
//!
//! `validate()` returns a clear `Err(String)`; it NEVER panics — a bad shape is a reportable
//! failure, not a crash.

use serde::{Deserialize, Serialize};

use crate::manifest::Manifest;
use crate::stats::Summary;

/// Warmup iterations before measurement [V2]: ferrod lazy-pool steady-state + JIT trace compilation.
pub const WARMUP: usize = 2_000;
/// Measured iterations [V2]: a stable p99/p999 needs a large N; a small-N p99 is noise.
pub const MEASURED: usize = 100_000;

/// Bumped whenever the on-disk shape changes; `schema.json` documents the current version.
pub const SCHEMA_VERSION: u32 = 1;

/// One measured script invocation (a ferro JIT-off / JIT-on run, or the PDO baseline).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    /// Human label, e.g. `"ferro-jit-off"`, `"ferro-jit-on"`, `"pdo"`.
    pub label: String,
    /// `"ferro"` (the measured path) or `"pdo"` (the baseline).
    pub target: String,
    /// The JIT the orchestrator INTENDED for this run: `"off"`, `"on"`, or `"n/a"`.
    pub jit_intended: String,
    /// The EFFECTIVE JIT the php binary reported for this run (`opcache_get_status(false)['jit']`
    /// normalized to `"off"`/`"on"`) [V6]. For a `ferro` run `validate()` requires this == intended.
    pub jit_effective: String,
    /// The raw `opcache_get_status(false)['jit']` array, kept verbatim for auditability.
    pub jit_status: serde_json::Value,
    /// The literal `-d` directives the php binary ran under, in order [V6].
    pub php_directives: Vec<String>,
    /// `true` if the run could not be measured (e.g. `pdo_pgsql` absent) — the ferro number still
    /// records. A `ferro` run must NEVER be skipped.
    pub skipped: bool,
    pub skip_reason: Option<String>,
    /// The measured latency distribution (all-zero `Summary` when `skipped`).
    pub summary: Summary,
}

/// Ferro's added boundary overhead vs the PDO baseline for a given JIT mode: `ferro - pdo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Overhead {
    /// The JIT mode of the ferro run this compares (`"off"`/`"on"`).
    pub jit: String,
    /// p50(ferro) - p50(pdo), in ns (may be negative if ferro is faster).
    pub p50_ns: i64,
    /// p99(ferro) - p99(pdo), in ns.
    pub p99_ns: i64,
}

/// The M3-fibers fan-out scenario is not implemented in M0 — recorded as an explicit placeholder so
/// the result shape is stable across milestones.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fanout {
    pub placeholder: bool,
    pub blocked_on: String,
}

impl Default for Fanout {
    fn default() -> Self {
        Fanout {
            placeholder: true,
            blocked_on: "M3-fibers".to_string(),
        }
    }
}

/// Run parameters recorded alongside the numbers so the sample count / transport are auditable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunParams {
    pub scenario: String,
    pub warmup: usize,
    pub measured: usize,
    pub transport: String,
}

/// The provisional/reference tags + the §16 targets, recorded so the result carries its own
/// interpretation contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct D12Meta {
    /// A provisional (WSL2) measurement, not the bare-metal reference re-run [honesty].
    pub provisional: bool,
    /// `false` until a human signs off on a bare-metal/host-network re-run.
    pub reference: bool,
    /// SPEC §16.1 boundary-latency targets on loopback UDS, for at-a-glance comparison.
    pub target_p50_ns: u64,
    pub target_p99_ns: u64,
}

impl Default for D12Meta {
    fn default() -> Self {
        D12Meta {
            provisional: true,
            reference: false,
            target_p50_ns: 60_000,  // p50 < 60 µs
            target_p99_ns: 200_000, // p99 < 200 µs
        }
    }
}

/// The top-level committed result document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub schema_version: u32,
    pub d12: D12Meta,
    pub params: RunParams,
    pub manifest: Manifest,
    pub runs: Vec<Run>,
    pub overhead_vs_pdo: Vec<Overhead>,
    pub fanout: Fanout,
}

impl BenchResult {
    /// The structural honesty self-check [V2/V6/V1]. Returns `Err(msg)` describing the first
    /// violation; never panics.
    ///
    /// Enforced invariants:
    /// - exactly two `ferro` runs, `jit_intended` `"off"` and `"on"`, both non-skipped;
    /// - every non-skipped run measured EXACTLY `params.measured` samples [V2];
    /// - each `ferro` run's EFFECTIVE JIT equals its INTENDED JIT [V6] (a silently-disabled JIT on
    ///   WSL2 would otherwise mislabel the number);
    /// - the manifest pins a `release` ferrod [V1] and carries the required identifying fields;
    /// - the record is tagged `provisional && !reference` (this path only ever writes provisional).
    pub fn validate(&self) -> Result<(), String> {
        // ---- ferro runs: presence + JIT off/on coverage ----
        let ferro: Vec<&Run> = self.runs.iter().filter(|r| r.target == "ferro").collect();
        if ferro.len() != 2 {
            return Err(format!(
                "expected exactly 2 ferro runs (jit off + on), found {}",
                ferro.len()
            ));
        }
        let mut intents: Vec<&str> = ferro.iter().map(|r| r.jit_intended.as_str()).collect();
        intents.sort_unstable();
        if intents != ["off", "on"] {
            return Err(format!(
                "the two ferro runs must intend jit off and on, found {intents:?}"
            ));
        }

        // ---- per-run sample count + JIT-effective assertions ----
        for run in &self.runs {
            if run.skipped {
                if run.target == "ferro" {
                    return Err(format!(
                        "ferro run '{}' was skipped — the D12 number cannot be skipped",
                        run.label
                    ));
                }
                continue; // a skipped baseline (pdo) does not carry a sample count.
            }
            if run.summary.samples_n != self.params.measured {
                return Err(format!(
                    "run '{}' recorded {} samples, expected params.measured={} [V2]",
                    run.label, run.summary.samples_n, self.params.measured
                ));
            }
            if run.target == "ferro" && run.jit_effective != run.jit_intended {
                return Err(format!(
                    "run '{}' effective JIT '{}' != intended '{}' — the JIT silently failed to \
                     engage; the number would be mislabeled [V6]",
                    run.label, run.jit_effective, run.jit_intended
                ));
            }
        }

        // ---- manifest: release ferrod [V1] + required identifying fields ----
        if self.manifest.ferrod_build_profile != "release" {
            return Err(format!(
                "ferrod_build_profile must be 'release' [V1], found '{}'",
                self.manifest.ferrod_build_profile
            ));
        }
        let required: [(&str, &str); 6] = [
            ("git_sha", &self.manifest.git_sha),
            ("rustc_version", &self.manifest.rustc_version),
            ("cpu_model", &self.manifest.cpu_model),
            ("php_version", &self.manifest.php_version),
            ("packer_class", &self.manifest.packer_class),
            ("timestamp_utc", &self.manifest.timestamp_utc),
        ];
        for (name, value) in required {
            if value.is_empty() {
                return Err(format!("manifest field '{name}' is empty"));
            }
        }

        // ---- tags: this writer only ever emits a provisional record ----
        if !self.d12.provisional || self.d12.reference {
            return Err("a bench run written by this orchestrator must be tagged \
                        provisional=true, reference=false [honesty]"
                .to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Summary;

    fn summary(n: usize) -> Summary {
        Summary {
            samples_n: n,
            min: 1,
            mean: 2.0,
            p50: 2,
            p90: 3,
            p99: 4,
            p999: 5,
            max: 6,
        }
    }

    fn ferro_run(intended: &str, effective: &str, n: usize) -> Run {
        Run {
            label: format!("ferro-jit-{intended}"),
            target: "ferro".to_string(),
            jit_intended: intended.to_string(),
            jit_effective: effective.to_string(),
            jit_status: serde_json::Value::Null,
            php_directives: vec!["-d".to_string(), "opcache.enable_cli=1".to_string()],
            skipped: false,
            skip_reason: None,
            summary: summary(n),
        }
    }

    fn good_manifest() -> Manifest {
        Manifest {
            cpu_model: "Test CPU".to_string(),
            cpu_cores: 8,
            scaling_governor: "unknown".to_string(),
            kernel: "6.6.0".to_string(),
            virtualization: "WSL2".to_string(),
            rustc_version: "rustc 1.95.0".to_string(),
            ferrod_build_profile: "release".to_string(),
            pg_image: "postgres:17".to_string(),
            pg_digest: "postgres@sha256:deadbeef".to_string(),
            git_sha: "abc123".to_string(),
            git_dirty: false,
            timestamp_utc: "2026-07-27T00:00:00Z".to_string(),
            php_version: "8.4.18".to_string(),
            ext_msgpack: false,
            pdo_pgsql: true,
            gc_enabled: true,
            packer_class: "Ferro\\Protocol\\Msgpack\\PurePacker".to_string(),
        }
    }

    fn good_result() -> BenchResult {
        BenchResult {
            schema_version: SCHEMA_VERSION,
            d12: D12Meta::default(),
            params: RunParams {
                scenario: "trivial".to_string(),
                warmup: WARMUP,
                measured: MEASURED,
                transport: "UDS".to_string(),
            },
            manifest: good_manifest(),
            runs: vec![
                ferro_run("off", "off", MEASURED),
                ferro_run("on", "on", MEASURED),
                Run {
                    label: "pdo".to_string(),
                    target: "pdo".to_string(),
                    jit_intended: "on".to_string(),
                    jit_effective: "on".to_string(),
                    jit_status: serde_json::Value::Null,
                    php_directives: vec![],
                    skipped: false,
                    skip_reason: None,
                    summary: summary(MEASURED),
                },
            ],
            overhead_vs_pdo: vec![
                Overhead {
                    jit: "off".to_string(),
                    p50_ns: 0,
                    p99_ns: 0,
                },
                Overhead {
                    jit: "on".to_string(),
                    p50_ns: 0,
                    p99_ns: 0,
                },
            ],
            fanout: Fanout::default(),
        }
    }

    #[test]
    fn good_result_validates() {
        good_result()
            .validate()
            .expect("the canonical good result must validate");
    }

    #[test]
    fn round_trips_through_json() {
        let r = good_result();
        let json = serde_json::to_string(&r).unwrap();
        let back: BenchResult = serde_json::from_str(&json).unwrap();
        back.validate().unwrap();
    }

    #[test]
    fn rejects_wrong_sample_count() {
        let mut r = good_result();
        r.runs[0].summary.samples_n = MEASURED - 1;
        let err = r.validate().unwrap_err();
        assert!(err.contains("samples"), "got: {err}");
    }

    #[test]
    fn rejects_jit_mismatch() {
        let mut r = good_result();
        r.runs[1].jit_effective = "off".to_string(); // intended "on"
        let err = r.validate().unwrap_err();
        assert!(err.contains("effective JIT"), "got: {err}");
    }

    #[test]
    fn rejects_debug_ferrod() {
        let mut r = good_result();
        r.manifest.ferrod_build_profile = "debug".to_string();
        let err = r.validate().unwrap_err();
        assert!(err.contains("release"), "got: {err}");
    }

    #[test]
    fn rejects_skipped_ferro_run() {
        let mut r = good_result();
        r.runs[0].skipped = true;
        let err = r.validate().unwrap_err();
        assert!(err.contains("skipped"), "got: {err}");
    }

    #[test]
    fn rejects_missing_manifest_field() {
        let mut r = good_result();
        r.manifest.php_version = String::new();
        let err = r.validate().unwrap_err();
        assert!(err.contains("php_version"), "got: {err}");
    }

    #[test]
    fn rejects_missing_jit_mode() {
        let mut r = good_result();
        r.runs[1].jit_intended = "off".to_string(); // now both ferro runs are "off"
        r.runs[1].jit_effective = "off".to_string();
        let err = r.validate().unwrap_err();
        assert!(err.contains("jit off and on"), "got: {err}");
    }

    #[test]
    fn rejects_non_provisional_tag() {
        let mut r = good_result();
        r.d12.reference = true;
        let err = r.validate().unwrap_err();
        assert!(err.contains("provisional"), "got: {err}");
    }
}
