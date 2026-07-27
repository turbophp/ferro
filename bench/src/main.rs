//! `ferro-bench` orchestrator — the D12 boundary-latency benchmark (the M0 exit gate, SPEC §16.1).
//!
//! Flow (`cargo run -p ferro-bench -- --baseline pdo --scenario trivial`):
//!   1. [V9] REQUIRE `FERRO_TEST_PG_URL` + a reachable Postgres — skip-clean (exit 0) otherwise.
//!   2. Collect the host manifest; resolve the RELEASE `ferrod` binary [V1] (never a debug build).
//!   3. [V11] pick a short socket path (sun_path < 104), launch `ferrod` under the Drop guard [V8].
//!   4. [V7] readiness is delegated to `bench_client.php` (its bounded connect-retry around the
//!      first `SELECT 1`); on any failure BOTH the ferrod log and the PHP stderr are surfaced.
//!   5. Run `bench_client.php` under JIT OFF then JIT ON [V6], and `bench_pdo.php` [fairness].
//!   6. Compute the distributions, assemble the result (+ `overhead_vs_pdo` + manifest + fanout
//!      placeholder + provisional/reference tags), `validate()` it [V2/V6/V1], and write
//!      `bench/results/<UTC>-wsl2.json`. The Drop guard tears ferrod + socket down on the way out.
//!
//! Teardown safety: `run()` owns the `FerrodProc` and returns a `RunExit`; `main` only calls
//! `process::exit` AFTER `run()` has returned (so the guard's `Drop` has already fired). A panic
//! inside `run()` also unwinds through the guard's `Drop` [V8].

use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde::Deserialize;

use ferro_bench::ferrod_proc::FerrodProc;
use ferro_bench::manifest::{self, Manifest};
use ferro_bench::result::{
    BenchResult, D12Meta, Fanout, MEASURED, Overhead, Run, RunParams, SCHEMA_VERSION, WARMUP,
};
use ferro_bench::stats::{self, Summary};

/// The two outcomes `run()` can hand back: a clean skip (missing/unreachable stack -> exit 0, so
/// the gate stays green offline) or a hard failure (exit 1).
enum RunExit {
    Skip(String),
    Fail(String),
}

fn main() {
    match run() {
        Ok(path) => {
            eprintln!("ferro-bench: wrote {}", path.display());
        }
        Err(RunExit::Skip(msg)) => {
            eprintln!("ferro-bench: SKIP — {msg}");
            // exit 0: a missing live stack must not fail the offline gate (matches ferro-e2e).
        }
        Err(RunExit::Fail(msg)) => {
            eprintln!("ferro-bench: FAILED — {msg}");
            std::process::exit(1);
        }
    }
}

/// The JSON document each PHP script emits: a header of runtime facts + the raw ns samples.
#[derive(Debug, Deserialize)]
struct PhpOutput {
    header: PhpHeader,
    samples: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct PhpHeader {
    php_version: String,
    ext_msgpack: bool,
    pdo_pgsql: bool,
    gc_enabled: bool,
    jit_effective: String,
    jit_status: serde_json::Value,
    packer_class: String,
    #[allow(dead_code)]
    warmup_n: usize,
    #[allow(dead_code)]
    samples_n: usize,
    skipped: bool,
    skip_reason: Option<String>,
}

/// One planned PHP invocation.
struct RunSpec {
    label: &'static str,
    target: &'static str,
    jit_intended: &'static str,
    /// The literal `-d` directive values (without the `-d` token) [V6].
    directives: Vec<String>,
}

fn run() -> Result<PathBuf, RunExit> {
    let args = Args::parse();

    // ---- [V9] require FERRO_TEST_PG_URL + a reachable PG (skip-clean otherwise) ----
    let pg_url = match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            return Err(RunExit::Skip(
                "FERRO_TEST_PG_URL is unset — bring up the testkit PG and export it, e.g.\n  \
                 docker compose -f testkit/docker-compose.yml up -d\n  \
                 export FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro"
                    .to_string(),
            ));
        }
    };
    match pg_hostport(&pg_url) {
        Some((host, port)) if pg_reachable(&host, port) => {}
        Some((host, port)) => {
            return Err(RunExit::Skip(format!(
                "Postgres at {host}:{port} is unreachable — is `docker compose up` running?"
            )));
        }
        None => {
            return Err(RunExit::Fail(format!(
                "could not parse a host:port out of FERRO_TEST_PG_URL ({pg_url})"
            )));
        }
    }

    // Repo layout is anchored at COMPILE time (robust regardless of CWD): bench/ is this crate,
    // the repo root is its parent, the workspace target dir defaults under it.
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("bench crate always has a parent (the repo root)")
        .to_path_buf();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo_root.join("target"));

    // ---- [V1] resolve a RELEASE ferrod (never a debug build — the engine hop budget assumes it) ----
    let ferrod_bin = resolve_release_ferrod(&target_dir)?;
    eprintln!("ferro-bench: ferrod = {}", ferrod_bin.display());

    // ---- [V11] short socket path (sun_path < 104), launch under the Drop guard [V8] ----
    let pid = std::process::id();
    let uid = nix::unistd::getuid().as_raw();
    let base = {
        let run_user = PathBuf::from(format!("/run/user/{uid}"));
        if run_user.is_dir() {
            run_user
        } else {
            PathBuf::from("/tmp")
        }
    };
    let socket_path = base.join(format!("ferro-bench-{pid}.sock"));
    let log_path = base.join(format!("ferro-bench-{pid}.log"));
    let sock_len = socket_path.as_os_str().as_bytes().len();
    if sock_len >= 104 {
        return Err(RunExit::Fail(format!(
            "socket path {} is {sock_len} bytes — exceeds sockaddr_un.sun_path (104) [V11]",
            socket_path.display()
        )));
    }
    let _ = std::fs::remove_file(&socket_path); // clear any stale file before bind

    let mut ferrod = FerrodProc::spawn(&ferrod_bin, &socket_path, &pg_url, &log_path)
        .map_err(|e| RunExit::Fail(format!("failed to launch ferrod: {e}")))?;
    eprintln!(
        "ferro-bench: launched ferrod pid={} on {}",
        ferrod.pid(),
        socket_path.display()
    );

    // ---- resolve the php binary + the client autoloader [V5] ----
    let php_bin = std::env::var("FERRO_BENCH_PHP").unwrap_or_else(|_| "php".to_string());
    let autoload = repo_root.join("php/client/vendor/autoload.php");
    if !autoload.is_file() {
        return Err(RunExit::Fail(format!(
            "PHP client autoloader missing at {} — run (cd php/client && composer install) [V5]",
            autoload.display()
        )));
    }
    let client_script = repo_root.join("bench/bench_client.php");
    let pdo_script = repo_root.join("bench/bench_pdo.php");

    let w = WARMUP.to_string();
    let m = MEASURED.to_string();

    // ---- the three planned runs ----
    let specs = [
        RunSpec {
            label: "ferro-jit-off",
            target: "ferro",
            jit_intended: "off",
            directives: vec!["opcache.enable_cli=1".into(), "opcache.jit=off".into()],
        },
        RunSpec {
            label: "ferro-jit-on",
            target: "ferro",
            jit_intended: "on",
            directives: vec![
                "opcache.enable_cli=1".into(),
                "opcache.jit=tracing".into(),
                "opcache.jit_buffer_size=64M".into(),
            ],
        },
    ];

    let mut runs: Vec<Run> = Vec::new();
    let mut ferro_header: Option<PhpHeader> = None;

    for spec in &specs {
        // Guard: ferrod must still be alive before each measured run.
        if let Some(status) = ferrod.exited() {
            return Err(RunExit::Fail(format!(
                "ferrod exited before '{}' (status {status}). ferrod log:\n{}",
                spec.label,
                ferrod.read_log()
            )));
        }
        eprintln!(
            "ferro-bench: running {} (W={WARMUP}, M={MEASURED}) ...",
            spec.label
        );
        let out = run_php(
            &php_bin,
            &spec.directives,
            &client_script,
            &[
                autoload.to_string_lossy().as_ref(),
                socket_path.to_string_lossy().as_ref(),
                &w,
                &m,
            ],
        )
        .map_err(|e| {
            RunExit::Fail(format!(
                "{} failed: {e}\n--- ferrod log ---\n{}",
                spec.label,
                ferrod.read_log()
            ))
        })?;

        let summary = stats::summarize(&out.samples);
        eprintln!(
            "ferro-bench:   {} -> p50={}ns p99={}ns (n={})",
            spec.label, summary.p50, summary.p99, summary.samples_n
        );
        runs.push(Run {
            label: spec.label.to_string(),
            target: spec.target.to_string(),
            jit_intended: spec.jit_intended.to_string(),
            jit_effective: out.header.jit_effective.clone(),
            jit_status: out.header.jit_status.clone(),
            php_directives: spec.directives.clone(),
            skipped: out.header.skipped,
            skip_reason: out.header.skip_reason.clone(),
            summary,
        });
        if ferro_header.is_none() {
            ferro_header = Some(out.header);
        }
    }

    // ---- the PDO baseline (run under JIT on so the baseline is not handicapped) ----
    eprintln!("ferro-bench: running pdo baseline (W={WARMUP}, M={MEASURED}) ...");
    let pdo_directives = vec![
        "opcache.enable_cli=1".to_string(),
        "opcache.jit=tracing".to_string(),
        "opcache.jit_buffer_size=64M".to_string(),
    ];
    let pdo_out = run_php(
        &php_bin,
        &pdo_directives,
        &pdo_script,
        &[pg_url.as_str(), &w, &m],
    )
    .map_err(|e| RunExit::Fail(format!("pdo baseline failed: {e}")))?;
    let pdo_summary = stats::summarize(&pdo_out.samples);
    if pdo_out.header.skipped {
        eprintln!(
            "ferro-bench:   pdo -> SKIPPED ({})",
            pdo_out.header.skip_reason.as_deref().unwrap_or("unknown")
        );
    } else {
        eprintln!(
            "ferro-bench:   pdo -> p50={}ns p99={}ns (n={})",
            pdo_summary.p50, pdo_summary.p99, pdo_summary.samples_n
        );
    }
    let pdo_run = Run {
        label: "pdo".to_string(),
        target: "pdo".to_string(),
        jit_intended: "on".to_string(),
        jit_effective: pdo_out.header.jit_effective.clone(),
        jit_status: pdo_out.header.jit_status.clone(),
        php_directives: pdo_directives,
        skipped: pdo_out.header.skipped,
        skip_reason: pdo_out.header.skip_reason.clone(),
        summary: pdo_summary.clone(),
    };

    // ---- assemble the manifest (host + PHP facts from the SAME php binary [V13]) ----
    let ferro_header = ferro_header.expect("at least one ferro run recorded a header");
    let mut mf: Manifest = manifest::collect_host();
    mf.ferrod_build_profile = "release".to_string(); // [V1] — resolved a release binary above
    mf.php_version = ferro_header.php_version;
    mf.ext_msgpack = ferro_header.ext_msgpack;
    mf.pdo_pgsql = ferro_header.pdo_pgsql;
    mf.gc_enabled = ferro_header.gc_enabled;
    mf.packer_class = ferro_header.packer_class;

    // ---- overhead_vs_pdo per ferro JIT mode = ferro - pdo (only when the baseline recorded) ----
    let overhead_vs_pdo: Vec<Overhead> = if pdo_run.skipped {
        Vec::new()
    } else {
        runs.iter()
            .filter(|r| r.target == "ferro")
            .map(|r| Overhead {
                jit: r.jit_intended.clone(),
                p50_ns: r.summary.p50 as i64 - pdo_summary.p50 as i64,
                p99_ns: r.summary.p99 as i64 - pdo_summary.p99 as i64,
            })
            .collect()
    };

    runs.push(pdo_run);

    let result = BenchResult {
        schema_version: SCHEMA_VERSION,
        d12: D12Meta::default(),
        params: RunParams {
            scenario: args.scenario,
            warmup: WARMUP,
            measured: MEASURED,
            transport: "UDS".to_string(),
        },
        manifest: mf,
        runs,
        overhead_vs_pdo,
        fanout: Fanout::default(),
    };

    // ---- [V2/V6/V1] structural honesty self-check — refuse to write a meaningless number ----
    result
        .validate()
        .map_err(|e| RunExit::Fail(format!("result failed validate(): {e}")))?;

    // ---- write bench/results/<UTC>-wsl2.json ----
    let results_dir = repo_root.join("bench/results");
    std::fs::create_dir_all(&results_dir)
        .map_err(|e| RunExit::Fail(format!("failed to create {}: {e}", results_dir.display())))?;
    let out_path = results_dir.join(format!("{}-wsl2.json", manifest::utc_filename_stamp()));
    let json = serde_json::to_string_pretty(&result)
        .map_err(|e| RunExit::Fail(format!("failed to serialize result: {e}")))?;
    std::fs::write(&out_path, json.as_bytes())
        .map_err(|e| RunExit::Fail(format!("failed to write {}: {e}", out_path.display())))?;

    print_summary(&result);
    // Returning drops `ferrod` -> SIGTERM/SIGKILL + unlink socket [V8].
    Ok(out_path)
}

/// Parsed CLI args (`--baseline`/`--scenario`); both informational — `--baseline pdo` is the only
/// baseline and `--scenario` labels the recorded params.
struct Args {
    scenario: String,
}

impl Args {
    fn parse() -> Self {
        let mut scenario = "trivial".to_string();
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < argv.len() {
            match argv[i].as_str() {
                "--scenario" if i + 1 < argv.len() => {
                    scenario = argv[i + 1].clone();
                    i += 2;
                }
                "--baseline" if i + 1 < argv.len() => {
                    i += 2; // only "pdo" is supported; accepted + ignored for forward-compat.
                }
                _ => i += 1,
            }
        }
        Args { scenario }
    }
}

/// Run `php <directives> <script> <args>`, capturing stdout+stderr, and parse the JSON document.
/// On a non-zero exit or a parse failure, `Err` carries the PHP stderr (the caller appends the
/// ferrod log).
fn run_php(
    php_bin: &str,
    directives: &[String],
    script: &Path,
    args: &[&str],
) -> Result<PhpOutput, String> {
    let mut cmd = Command::new(php_bin);
    for d in directives {
        cmd.arg("-d").arg(d);
    }
    cmd.arg(script);
    for a in args {
        cmd.arg(a);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let out = cmd
        .output()
        .map_err(|e| format!("could not spawn '{php_bin}': {e}"))?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        return Err(format!(
            "php exited {} — stderr:\n{}",
            out.status,
            if stderr.trim().is_empty() {
                "(empty)"
            } else {
                stderr.trim()
            }
        ));
    }
    serde_json::from_slice::<PhpOutput>(&out.stdout).map_err(|e| {
        let head: String = String::from_utf8_lossy(&out.stdout)
            .chars()
            .take(200)
            .collect();
        format!(
            "failed to parse php output as JSON: {e}\nstderr:\n{stderr}\nstdout[..200]:\n{head}"
        )
    })
}

/// [V1] Resolve a RELEASE `ferrod`. `FERRO_FERROD_BIN` overrides but must itself be a release path
/// (guards against pointing the D12 number at a debug build); else `target/release/ferrod`; else a
/// clear "build it" error that names the debug build if that is all that exists.
fn resolve_release_ferrod(target_dir: &Path) -> Result<PathBuf, RunExit> {
    if let Some(override_path) = std::env::var_os("FERRO_FERROD_BIN") {
        let p = PathBuf::from(&override_path);
        if !is_executable(&p) {
            return Err(RunExit::Fail(format!(
                "FERRO_FERROD_BIN is set but not an executable file: {}",
                p.display()
            )));
        }
        if !p.to_string_lossy().contains("release") {
            return Err(RunExit::Fail(format!(
                "FERRO_FERROD_BIN ({}) does not look like a RELEASE build — the D12 number must \
                 measure a release ferrod [V1]. Point it at a release binary or unset it and run \
                 `cargo build -p ferrod --release`.",
                p.display()
            )));
        }
        return Ok(p);
    }

    let release = target_dir.join("release/ferrod");
    if is_executable(&release) {
        return Ok(release);
    }
    let debug = target_dir.join("debug/ferrod");
    if is_executable(&debug) {
        return Err(RunExit::Fail(format!(
            "found only a DEBUG ferrod at {} — the D12 number must measure a release build (a \
             debug build is ~an order of magnitude slower, SPEC §16.1) [V1]. Run:\n  \
             cargo build -p ferrod --release",
            debug.display()
        )));
    }
    Err(RunExit::Fail(format!(
        "no ferrod binary found under {} — run:\n  cargo build -p ferrod --release",
        target_dir.display()
    )))
}

fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p)
        .map(|m| m.is_file() && (m.permissions().mode() & 0o111 != 0))
        .unwrap_or(false)
}

/// Parse `host:port` out of a `postgres://user:pass@host:port/db` URL (port defaults to 5432).
fn pg_hostport(url: &str) -> Option<(String, u16)> {
    let rest = url.split("://").nth(1)?;
    let after_at = rest.rsplit_once('@').map(|(_, a)| a).unwrap_or(rest);
    let hostport = after_at.split('/').next()?;
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(5432)),
        None => (hostport.to_string(), 5432),
    };
    if host.is_empty() {
        return None;
    }
    Some((host, port))
}

/// A bounded TCP-connect probe to Postgres (the reachability gate [V9]).
fn pg_reachable(host: &str, port: u16) -> bool {
    use std::net::ToSocketAddrs;
    let Ok(mut addrs) = (host, port).to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok()
}

/// Print a compact human summary of the measured result to stderr (the numbers Task 3 records).
fn print_summary(r: &BenchResult) {
    eprintln!(
        "\n=== ferro-bench D12 (provisional, {}) ===",
        r.manifest.virtualization
    );
    eprintln!(
        "targets (SPEC §16.1): p50 < {}ns, p99 < {}ns",
        r.d12.target_p50_ns, r.d12.target_p99_ns
    );
    for run in &r.runs {
        if run.skipped {
            eprintln!(
                "  {:<14} SKIPPED ({})",
                run.label,
                run.skip_reason.as_deref().unwrap_or("?")
            );
            continue;
        }
        let s: &Summary = &run.summary;
        eprintln!(
            "  {:<14} p50={:>7}ns p90={:>7}ns p99={:>8}ns p999={:>9}ns  (jit {}->{})",
            run.label, s.p50, s.p90, s.p99, s.p999, run.jit_intended, run.jit_effective
        );
    }
    for o in &r.overhead_vs_pdo {
        eprintln!(
            "  overhead vs pdo (jit {}): p50 {:+}ns, p99 {:+}ns",
            o.jit, o.p50_ns, o.p99_ns
        );
    }
}
