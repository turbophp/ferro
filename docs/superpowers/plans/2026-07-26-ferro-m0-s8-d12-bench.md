# Ferro M0 · Slice S8 — D12 p99 bench (the M0 EXIT GATE) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

> **Plan v2 — rewritten after adversarial verification `wf_e822c6c1` (FIX_FIRST: 1 blocker + 5 methodology majors + fixes, all verified).** S8 is the M0 exit gate whose ENTIRE value is an HONEST, STABLE p50/p99 — the v1 plan would have committed a meaningless number on several axes. Every fix is folded below and flagged **[Vn]**.

**Goal:** Produce and **commit** the D12 boundary-latency measurement — the M0 exit condition (SPEC §16.1 / §21 D12). `ferro-bench` (Rust orchestrator) times the trivial-call path (PHP client → **release** `ferrod` → live `SELECT 1` → response) against a local PDO baseline in the same environment, computes a STABLE latency distribution, and writes ONE self-contained, schema-validated JSON with a complete environment manifest to `bench/results/`. **M0 does not close until `bench/results/*.json` exists and `ci/check-d12-recorded.sh` exits 0.** Depends: S5 (live `SELECT 1`), S7 (the PHP client that IS the measured path), S2 (the gate + Docker PG).

**Gate contract (verified `ci/check-d12-recorded.sh:5-6`):** "≥1 `bench/results/*.json`, else exit 1". So the exit is: a real run wrote a committed result. **M0 exit is RECORDING an honest measurement, not passing a latency threshold** (§16.1 D12: a p99 miss pulls the `ext-php-rs` accelerator into M1 — a human sign-off on recorded numbers, NOT a CI threshold). But the number must be MEASURED HONESTLY or the gate is worthless.

## Methodology constraints (the load-bearing part — each defends an honest number)
- **[V1/BLOCKER] Measure a RELEASE `ferrod`.** The engine hop is budgeted at 5–10 µs (§16.1); a debug build is ~an order of magnitude slower. `cargo build -p ferrod --release` is a prerequisite; the orchestrator uses `target/release/ferrod` (or a `FERRO_FERROD_BIN` that MUST resolve to a release path — fail clearly otherwise). Do NOT reuse the S7 `locateFerrod` debug-first ordering. Record `ferrod_build_profile: "release"` in the manifest so the engine half of the path is pinned alongside the git sha.
- **[V2] Pinned, large samples for a stable p99.** Warmup `W = 2000` (ferrod lazy-pool steady-state + JIT trace compilation); measured `M = 100_000` (a p99/p999 needs a large N; a small-N p99 is noise). `result.validate()` asserts `samples_n == M`. Report min/mean/p50/p90/p99/p999/max (M=100k makes p999 meaningful).
- **[V3] GC stays ON.** `bench_client.php` MUST NOT call `gc_disable()` — the recorded p99 must honestly include cyclic-GC pauses (that IS the §16.1 "pure PHP dies first at p99" signal). Record `gc_enabled: true`.
- **[V4] Tight timing window.** `hrtime(true)` IMMEDIATELY before and after `scalar('SELECT 1')` and nothing else between; push each delta into a pre-sized array; emit the whole sample stream to stdout ONCE after the measured loop. No `fwrite`/`echo` inside the timed window.
- **[V5] Autoloader.** `bench_client.php` MUST `require` `php/client/vendor/autoload.php` (path passed from `ferro-bench`); `(cd php/client && composer install)` is a prerequisite (a bare `php bench_client.php` has no autoloader → fatal → zero ferro samples).
- **[V6] Effective (not intended) JIT.** `bench_client.php` emits the EFFECTIVE JIT state from `opcache_get_status(false)['jit']`; the manifest records that and `result.validate()` asserts effective==intended, FAILING the run on mismatch (WSL2 JIT buffer can silently fail to engage). JIT-off = `php -d opcache.enable_cli=1 -d opcache.jit=off …` (keeps OPcache, isolates the JIT variable); JIT-on = `php -d opcache.enable_cli=1 -d opcache.jit=tracing -d opcache.jit_buffer_size=64M …`. Record the literal `-d` directive list per run.
- **[fairness] PDO baseline shape.** `bench_pdo.php`: construct `PDO` ONCE outside the loop; per iteration `$pdo->query('SELECT 1')->fetchColumn()` (unprepared query + fetch = one round trip + fetch, matching ferro's shape). NOT `ATTR_PERSISTENT`, NOT a prepared-once statement re-executed (either makes the baseline artificially fast → a misleading delta).
- **[V/honesty] The delta is not over-claimed.** Add `overhead_vs_pdo: {p50, p99}` per JIT mode = ferro − pdo. The README states the transport-library caveat: ferro's path is PHP→ferrod(Rust/tokio-postgres)→PG; the PDO path is PHP/libpq→PG — so the delta is Ferro's added boundary overhead *in this env*, not a pure hop-isolation. Provisional/WSL2 tagged; `ext-msgpack` absent → the **pure-PHP `PurePacker`** codec is measured (which `PackerFactory` hard-wires for encode+decode regardless of ext presence — record the actual packer class, note the ext-swap mitigation is unbuilt).

## Architecture
```
/bench/                         (add "bench" to root Cargo.toml members — the just-in-time note)
  Cargo.toml                    ferro-bench bin; deps: serde, serde_json, nix (feature "signal"); std only otherwise (a pure orchestrator — does NOT link the Rust client)
  src/main.rs                   orchestrator (below)
  src/manifest.rs               env manifest collector
  src/stats.rs                  percentiles (documented method) from a sorted u64 ns sample
  src/result.rs                 serde result types + validate() (structural self-check; ships a schema.json for reference)
  src/ferrod_proc.rs            a Child wrapper with a Drop impl [V8] (SIGTERM->poll->SIGKILL->unlink)
  bench_client.php              [V3/V4/V5/V6] the ferro path — the D12 number
  bench_pdo.php                 the PDO baseline
  README.md                     run instructions + the reference-env pinning + the transport caveat + the D12 interpretation
  results/.gitkeep              (exists) + the committed run JSON
```
**`ferro-bench` (`cargo run -p ferro-bench -- --baseline pdo --scenario trivial`):**
1. **[V9] REQUIRE `FERRO_TEST_PG_URL` + a reachable PG** — skip-clean with a clear message if unset/unreachable (match `ferro-e2e/main.rs` + `local-gate.sh`); do NOT manage Docker (the operator runs `docker compose up`).
2. Collect the manifest (below). Resolve the **release** ferrod binary ([V1]; `FERRO_FERROD_BIN` override, else `target/release/ferrod`, else a clear "run `cargo build -p ferrod --release`" error — `cargo run -p ferro-bench` does NOT build ferrod).
3. **[V11]** pick a socket path on a known-short base (`/run/user/<uid>` or `/tmp`), assert byte-length `< 104` (sun_path) with a clear error, launch ferrod (`FERRO_SOCK`/`FERRO_POOLS=default`/`FERRO_POOL_DEFAULT_DSN=$FERRO_TEST_PG_URL`) wrapped in the Drop guard [V8], stderr → a log file.
4. **[V7] readiness is delegated to `bench_client.php`**: its first action is `Ferro::connect` + a bounded connect-retry poll around the first `scalar('SELECT 1')` (a bare socket connect would succeed even with a down PG — pools connect lazily); the first success IS readiness. On failure surface BOTH ferrod stderr and the PHP stderr.
5. Run `bench_client.php` under JIT OFF then JIT ON ([V6]) and `bench_pdo.php`; capture each script's emitted ns samples via `std::process::Command` stdout.
6. `stats.rs` computes each distribution; assemble the result (ferro JIT-off + ferro JIT-on + pdo + `overhead_vs_pdo` + manifest + fanout placeholder `{placeholder:true, blocked_on:"M3-fibers"}` + `provisional:true, reference:false`); `validate()` (samples_n==M [V2], effective JIT==intended [V6]); write `bench/results/<UTC>-wsl2.json`; teardown via the Drop guard.

## Environment manifest (§16 "meaningless otherwise" — complete)
CPU model + cores (`/proc/cpuinfo`), **[V13] `scaling_governor`** (`/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`, `unknown` fallback), kernel (`uname -r`), **virtualization = WSL2** (`/proc/version`/`systemd-detect-virt`), rustc version, **`ferrod_build_profile: "release"` [V1]**, **[V13] PHP-runtime facts emitted BY `bench_client.php` from the SAME php binary** (PHP version, `ext-msgpack` present? [absent here], `pdo_pgsql` present?, `gc_enabled` [V3], effective JIT per run [V6], the actual packer class `Ferro\Protocol\Msgpack\PurePacker`), Postgres image + digest (prefer `docker inspect` of the running container, fall back to `testkit/docker-compose.yml`), git sha + dirty, run params (`W`, `M`, transport=UDS, the literal `-d` list per run [V6]), timestamp, `provisional:true`, `reference:false`.

## Tasks

### Task 1: `ferro-bench` crate — stats + manifest + result schema + ferrod Drop-guard + workspace wiring
- [ ] Add `"bench"` to root `Cargo.toml` members; `bench/Cargo.toml` (`ferro-bench` bin; `serde`/`serde_json`/`nix` feature `signal`; no ferro-client link). Keep workspace `build`/`clippy --all-targets -D warnings`/`test` green.
- [ ] `src/stats.rs`: percentiles by the **nearest-rank** method (document it) on a sorted `&[u64]` ns sample; unit-tested against a known vector (e.g. 1..=100 → p50/p90/p99/p999/max exact).
- [ ] `src/result.rs`: serde types (manifest + `runs:[{path, jit_intended, jit_effective, dirs:[-d…], samples_n, min/mean/p50/p90/p99/p999/max}] + overhead_vs_pdo + fanout placeholder + tags`); `validate()` asserts `samples_n==M`, `jit_effective==jit_intended`, required manifest fields present — returns a clear error, never a panic. Ship `bench/schema.json` for reference.
- [ ] `src/ferrod_proc.rs` [V8]: a `FerrodProc` owning the `Child` + socket path with `Drop` = SIGTERM (`nix::sys::signal::kill`) → poll `try_wait()` to a deadline → SIGKILL → unlink socket, so a panic in aggregation cannot leak ferrod/socket/PG conns.
- [ ] `src/manifest.rs`: collect the host-side manifest fields (never panic on a missing source → `unknown`).
- **Gate:** `cargo test -p ferro-bench` (stats vector + validate rejects a bad shape) + fmt + `clippy --workspace --all-targets -D warnings` + `cargo test --workspace`. **Commit** `feat(s8): ferro-bench crate — stats + manifest + result schema + ferrod Drop-guard + workspace wiring`.

### Task 2: the PHP bench scripts + orchestration
- [ ] `bench/bench_client.php` [V3/V4/V5/V6]: `require <repo>/php/client/vendor/autoload.php`; a bounded connect-retry around `Ferro::connect($sock,'default')` + first `scalar('SELECT 1')` (=readiness [V7]); `W` warmup; `M` measured, each = `$t=hrtime(true); $c->scalar('SELECT 1'); $samples[$i]=hrtime(true)-$t;` (nothing else in the window); after the loop emit the ns samples once + a small JSON header carrying `gc_enabled`, effective JIT (`opcache_get_status(false)['jit']`), PHP version, `ext-msgpack`/`pdo_pgsql` presence, packer class. No `gc_disable()`.
- [ ] `bench/bench_pdo.php`: `PDO` once (from `FERRO_TEST_PG_URL`), `W`+`M` loop of `$pdo->query('SELECT 1')->fetchColumn()`; same emit format; skip-with-note if `pdo_pgsql` absent (the ferro number still records).
- [ ] `src/main.rs` orchestration: [V9] require PG; resolve release ferrod [V1]; [V11] short-socket + launch under the Drop-guard; run client (JIT off `-d opcache.jit=off`, JIT on `-d opcache.jit=tracing -d opcache.jit_buffer_size=64M`) + pdo; parse samples + the header; compute + assemble + `validate()` + write `bench/results/<UTC>-wsl2.json`.
- **Gate:** `docker compose -f testkit/docker-compose.yml up -d` + `cargo build -p ferrod --release` + `(cd php/client && composer install)` + `FERRO_TEST_PG_URL=… cargo run -p ferro-bench -- --baseline pdo --scenario trivial` writes a schema-valid JSON (ferro JIT off/on + pdo + overhead + full manifest); clear skip/error if the stack is absent. **Commit** `feat(s8): PHP bench scripts (ferro client + PDO baseline) + JIT off/on orchestration`.

### Task 3: record the run + close the M0 exit gate
- [ ] Run live and **COMMIT** the produced `bench/results/<UTC>-wsl2.json` (provisional). Confirm `ci/check-d12-recorded.sh` exits 0 and `ci/local-gate.sh` is green.
- [ ] `bench/README.md`: interpret the numbers (measured p50/p99 pure-PHP JIT off vs on, vs §16 targets p50<60 µs / p99<200 µs; the `overhead_vs_pdo` delta + the transport caveat; the D12 decision — met on this provisional WSL2 env, or flags the M1 accelerator — a human sign-off note, NOT a CI failure).
- [ ] Update execution-design "M0 exit gate" + spec §22.1: D12 measurement committed (provisional/WSL2); the reference-env (bare-metal/host-network) re-run is the remaining human sign-off.
- **Gate:** `ci/check-d12-recorded.sh` exits 0 with the committed result; `ci/local-gate.sh` green. **Commit** `feat(s8): record provisional D12 bench result — M0 EXIT GATE MET + interpretation`.

## Self-Review / Gates
- M0 exit MET: a committed `bench/results/*.json` + `check-d12-recorded.sh` exit 0, and the number is HONEST (release ferrod, M=100k, GC on, effective-JIT-asserted, one-Session steady-state, fair unprepared PDO baseline, complete manifest, provisional/WSL2 tagged). Charter gates green with `bench` in the workspace.
- Verify the plan before executing: a focused pass on the folded fixes (release-binary resolution + profile-in-manifest; M/validate; the tight timing window; the effective-JIT assert; the Drop-guard teardown; the fair PDO shape; the overhead field + caveat).

## Execution Handoff
Subagent-driven: fresh implementer per task (TDD/gates), review after, whole-branch review before declaring M0 CLOSED. Live run uses the S2 Docker PG + a **release** `ferrod` + the S7 PHP client.
