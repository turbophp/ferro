//! The environment manifest — SPEC §16 is explicit that a latency number is "meaningless without a
//! recorded reference environment". This collects the HOST-side facts (CPU, kernel, virtualization,
//! rustc, git, Postgres image/digest, timestamp). The PHP-runtime facts (PHP version, extensions,
//! GC, JIT, packer class) are emitted BY `bench_client.php` from the SAME php binary and folded in
//! by the orchestrator — they are NOT collected here, so the manifest can never disagree with the
//! interpreter that actually ran the measured path [V13].
//!
//! Every collector is best-effort and TOTAL: a missing/unreadable source yields `"unknown"` (or a
//! sensible default) rather than a panic, so a manifest is always produced.

use std::path::Path;
use std::process::Command;

use serde::{Deserialize, Serialize};

/// The complete recorded environment for a bench run. Host-side fields are filled by
/// [`collect_host`]; the `php_*` / `gc_enabled` / `packer_class` / `ext_*` fields are filled by the
/// orchestrator from the header `bench_client.php` emits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    // ---- host ----
    pub cpu_model: String,
    pub cpu_cores: usize,
    /// `/sys/.../scaling_governor`, or `"unknown"` (WSL2 has no cpufreq sysfs) [V13].
    pub scaling_governor: String,
    pub kernel: String,
    /// `"WSL2"`, another `systemd-detect-virt` value, or `"unknown"`.
    pub virtualization: String,
    pub rustc_version: String,
    /// Pinned to `"release"` by the orchestrator [V1] — the engine hop is only meaningful measured
    /// against a release build (a debug build is ~an order of magnitude slower, SPEC §16.1).
    pub ferrod_build_profile: String,
    pub pg_image: String,
    pub pg_digest: String,
    pub git_sha: String,
    pub git_dirty: bool,
    pub timestamp_utc: String,

    // ---- php runtime (filled from bench_client.php's header, same php binary) ----
    pub php_version: String,
    pub ext_msgpack: bool,
    pub pdo_pgsql: bool,
    pub gc_enabled: bool,
    pub packer_class: String,
}

/// Collect the host-side manifest fields. PHP-runtime fields are left at their defaults here
/// (`""` / `false`) and filled in by the orchestrator from the PHP header.
pub fn collect_host() -> Manifest {
    let (cpu_model, cpu_cores) = cpu_info();
    Manifest {
        cpu_model,
        cpu_cores,
        scaling_governor: scaling_governor(),
        kernel: run_trim("uname", &["-r"]).unwrap_or_else(|| "unknown".to_string()),
        virtualization: virtualization(),
        rustc_version: run_trim("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string()),
        // The orchestrator overrides this with "release" after resolving a release binary [V1].
        ferrod_build_profile: "unknown".to_string(),
        pg_image: pg_image(),
        pg_digest: pg_digest(),
        git_sha: run_trim("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string()),
        git_dirty: git_dirty(),
        timestamp_utc: run_trim("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"])
            .unwrap_or_else(|| "unknown".to_string()),
        php_version: String::new(),
        ext_msgpack: false,
        pdo_pgsql: false,
        gc_enabled: false,
        packer_class: String::new(),
    }
}

/// A compact UTC token for the result filename, e.g. `20260727T153012Z`. Best-effort; a `date`
/// failure falls back to a nanosecond-since-epoch token so a filename is always producible.
pub fn utc_filename_stamp() -> String {
    if let Some(s) = run_trim("date", &["-u", "+%Y%m%dT%H%M%SZ"])
        && !s.is_empty()
    {
        return s;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("t{nanos}")
}

/// `(model name, core count)` from `/proc/cpuinfo`; `("unknown", 0)` if unreadable.
fn cpu_info() -> (String, usize) {
    let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") else {
        return ("unknown".to_string(), 0);
    };
    let mut model = "unknown".to_string();
    let mut cores = 0usize;
    for line in text.lines() {
        if model == "unknown"
            && let Some(v) = line.strip_prefix("model name")
            && let Some((_, val)) = v.split_once(':')
        {
            model = val.trim().to_string();
        }
        if line.starts_with("processor") {
            cores += 1;
        }
    }
    (model, cores)
}

fn scaling_governor() -> String {
    std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// WSL2 is the reference env here — detect it from the kernel string first (reliable), then fall
/// back to `systemd-detect-virt`, then `"unknown"`.
fn virtualization() -> String {
    if let Ok(v) = std::fs::read_to_string("/proc/version") {
        let low = v.to_ascii_lowercase();
        if low.contains("microsoft") || low.contains("wsl") {
            return "WSL2".to_string();
        }
    }
    run_trim("systemd-detect-virt", &[])
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn git_dirty() -> bool {
    run_trim("git", &["status", "--porcelain"])
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
}

/// The Postgres image reference. Prefer the running testkit container's configured image; fall back
/// to the compose file; else `"unknown"`.
fn pg_image() -> String {
    if let Some(img) = run_trim(
        "docker",
        &["inspect", "--format", "{{.Config.Image}}", "testkit-pg-1"],
    ) && !img.is_empty()
    {
        return img;
    }
    compose_pg_image().unwrap_or_else(|| "unknown".to_string())
}

/// The Postgres image content digest. Prefer `docker inspect` of the pulled image; fall back to the
/// `@sha256:` pin in the compose file; else `"unknown"`.
fn pg_digest() -> String {
    // First resolve an image ref to inspect (the compose ref carries the digest inline).
    let image_ref = pg_image();
    if !image_ref.is_empty()
        && image_ref != "unknown"
        && let Some(dg) = run_trim(
            "docker",
            &[
                "inspect",
                "--format",
                "{{index .RepoDigests 0}}",
                &image_ref,
            ],
        )
        && dg.contains("sha256:")
    {
        return dg;
    }
    // Fall back to the digest pinned in the compose file (`image: postgres:17@sha256:...`).
    compose_pg_image()
        .and_then(|img| img.split_once('@').map(|(_, d)| d.to_string()))
        .filter(|d| d.contains("sha256:"))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Parse the `image:` line out of `testkit/docker-compose.yml` (relative to CWD, then repo root).
fn compose_pg_image() -> Option<String> {
    for candidate in [
        "testkit/docker-compose.yml",
        "../testkit/docker-compose.yml",
    ] {
        if let Ok(text) = std::fs::read_to_string(Path::new(candidate)) {
            for line in text.lines() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("image:") {
                    let img = rest.trim();
                    if img.contains("postgres") {
                        return Some(img.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Run `cmd args`, returning trimmed stdout on a clean exit, or `None` on any failure. Total — a
/// missing binary or non-zero exit is `None`, never a panic.
fn run_trim(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_host_never_panics_and_fills_defaults() {
        let m = collect_host();
        // Host fields are always set to *something* (a real value or "unknown"); PHP fields are
        // left empty for the orchestrator to fill.
        assert!(!m.kernel.is_empty());
        assert!(!m.timestamp_utc.is_empty());
        assert_eq!(m.ferrod_build_profile, "unknown"); // orchestrator sets "release"
        assert_eq!(m.php_version, "");
    }

    #[test]
    fn utc_stamp_is_nonempty() {
        assert!(!utc_filename_stamp().is_empty());
    }

    #[test]
    fn run_trim_missing_binary_is_none() {
        assert!(run_trim("this-binary-does-not-exist-ferro", &[]).is_none());
    }
}
