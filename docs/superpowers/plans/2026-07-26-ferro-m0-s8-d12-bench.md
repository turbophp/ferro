# Ferro M0 · Slice S8 — D12 p99 bench (the M0 EXIT GATE) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Produce and **commit** the D12 boundary-latency measurement — the M0 exit condition (SPEC §16.1 / §21 D12). A `ferro-bench` harness times the trivial-call path (PHP client → `ferrod` → live `SELECT 1` → response) against a local PDO baseline over the same environment, computes the latency distribution (min/mean/p50/p90/p99/p999/max), and writes ONE self-contained, schema-validated JSON with a complete environment manifest to `bench/results/`. **M0 does not close until `bench/results/*.json` exists and `ci/check-d12-recorded.sh` exits 0.** Depends: S5 (the live `SELECT 1`), S7 (the PHP client that IS the measured path), S2 (the gate script + Docker PG).

**What the gate actually requires (verified):** `ci/check-d12-recorded.sh` = "≥1 `bench/results/*.json`, else exit 1". So the exit is: a real run wrote a committed result. **M0 exit is RECORDING the measurement, not passing a latency threshold** (§16.1 D12: a p99 miss pulls the `ext-php-rs` accelerator into M1 — a human sign-off on the recorded numbers, NOT a CI threshold). The bench must still measure honestly.

## Charter / methodology constraints
- **Fairness rule (exec-design §2):** the PDO baseline and the ferro path run in the SAME environment (same host/container, same PG) so the delta isolates Ferro's own boundary overhead vs raw PDO — NOT identical hop-counts (ferro has the extra `ferrod` hop; that IS the overhead being measured). The measured D12 number is the FERRO client's boundary overhead: client call → ferrod → `SELECT 1` → response, timed CLIENT-side in PHP (`bench_client.php` — "the D12 number").
- **Provisional on WSL2 (fairness):** this run is containerized on WSL2, so the result JSON is tagged `provisional: true, reference: false`; final D12 sign-off is a later bare-metal/host-network re-run (human, not blocking M0). `ext-msgpack` is ABSENT in this env → the **pure-PHP codec** path is measured, which is EXACTLY the "pure PHP p99" the D12 gate is about (§16.1). Note it in the manifest.
- **JIT off AND on:** run the PHP client bench under opcache JIT disabled AND enabled; record both distributions (the p99 story differs with/without JIT — §16.1).
- **No speculative optimization (rule 5):** the bench MEASURES; it does not tune the engine. The fan-out (Fibers) scenario ships as a `placeholder: true, blocked_on: "M3-fibers"` stub (serial), not implemented.
- **Charter gates** still green (fmt/clippy/test) with the new `bench` crate in the workspace; PHP untouched except the two bench scripts (which reuse the S7 client + PDO).

## Architecture — `ferro-bench` (Rust orchestrator) + two PHP timing scripts
```
/bench/                         (add "bench" to root Cargo.toml members — the just-in-time note)
  Cargo.toml                    ferro-bench bin crate
  src/main.rs                   orchestrator: manifest → ensure stack → run scripts → aggregate → write JSON
  src/manifest.rs               environment manifest collector
  src/stats.rs                  min/mean/p50/p90/p99/p999/max from a sorted sample
  src/result.rs                 the result schema (serde types) + a self-validation
  bench_client.php              PHP: Ferro::connect → warmup + N× scalar('SELECT 1'), emits per-iteration ns
  bench_pdo.php                 PHP: PDO → PG, warmup + N× 'SELECT 1', emits per-iteration ns (baseline)
  README.md                     how to run + the reference-environment pinning note (§16 "meaningless otherwise")
  results/.gitkeep              (exists) + the committed run JSON lands here
```
- `ferro-bench` (invoked `cargo run -p ferro-bench -- --baseline pdo --scenario trivial`): (1) collect the manifest; (2) ensure `ferrod` + Docker PG are up (launch `ferrod` like the S7 harness — env `FERRO_SOCK`/`FERRO_POOLS=default`/`FERRO_POOL_DEFAULT_DSN=$FERRO_TEST_PG_URL`, on a short `sys::temp` socket; or accept a running one); (3) run `bench_client.php` (JIT off, then JIT on) and `bench_pdo.php`, capturing each script's per-iteration timing samples (the PHP script does the client-side timing with `hrtime(true)` ns); (4) compute the distributions; (5) assemble the result JSON (both distributions × JIT modes + the baseline + the manifest + the fanout placeholder + `provisional/reference` tags) and WRITE it to `bench/results/<UTC-stamp>-wsl2.json`; (6) tear down what it launched.
- The PHP scripts are dependency-free (reuse `Ferro\Client` for the ferro path; `PDO`/`pdo_pgsql` for the baseline — `pdo_pgsql` availability is runtime-detected and skipped-with-a-note if absent, so the ferro number is still recorded).

## Environment manifest (§16 "numbers are meaningless otherwise" — must be complete)
CPU model (`/proc/cpuinfo`), core count, kernel (`uname -r`), virtualization (WSL2 — `/proc/version` / `systemd-detect-virt`), rustc version, PHP version + JIT status per run (on/off) + `ext-msgpack` present?/`pdo_pgsql` present?, Postgres image + digest (from `testkit/docker-compose.yml`), git commit sha + dirty flag, run params (warmup N, measured N, socket transport = UDS), timestamp, `provisional: true`, `reference: false`.

## Tasks

### Task 1: `ferro-bench` crate — manifest + stats + result schema + workspace wiring
- [ ] Add `"bench"` to root `Cargo.toml` `members`. Create `bench/Cargo.toml` (`ferro-bench` bin; deps minimal — serde/serde_json for the result, a stats impl in-crate, std for manifest collection via `std::process::Command`/`std::fs`). Keep the workspace `cargo build`/`clippy`/`test` green.
- [ ] `src/stats.rs`: percentiles (min/mean/p50/p90/p99/p999/max) from an `&mut [f64]`/`u64` ns sample (sort + nearest-rank or linear-interp — document which; deterministic). Unit-tested against a known sample.
- [ ] `src/manifest.rs`: collect the full manifest above. Unit/smoke test that each field is populated (or a documented `unknown` fallback, never a panic if a source is missing).
- [ ] `src/result.rs`: serde types for the result JSON (manifest + `runs: [{path: "ferro"|"pdo", jit: bool, samples_n, min/mean/p50/p90/p99/p999/max}] + fanout placeholder + tags`); a `validate()` that asserts the shape/required fields before writing (self-validation, since we have no external JSON-schema validator dependency — OR ship a `schema.json` and validate structurally). 
- **Gate:** `cargo test -p ferro-bench` (stats + manifest smoke) + `cargo fmt --check` + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo test --workspace` green (bench crate compiles into the workspace). **Commit** `feat(s8): ferro-bench crate — env manifest + latency stats + result schema + workspace wiring`.

### Task 2: the PHP timing scripts + orchestration + the live run
- [ ] `bench/bench_client.php`: `Ferro::connect($sock, 'default')`; warmup W iterations of `scalar('SELECT 1')`; then M measured iterations, each timed with `hrtime(true)`; emit the samples (e.g. newline-delimited ns to stdout, or a summary) for `ferro-bench` to aggregate. Reads socket + counts from argv/env.
- [ ] `bench/bench_pdo.php`: the baseline — `new PDO('pgsql:…', …)` from `FERRO_TEST_PG_URL`; same warmup/measured loop of `SELECT 1`; same emit format. Skip-with-a-note if `pdo_pgsql` is absent (the ferro number still records).
- [ ] `ferro-bench/main.rs` orchestration: launch/verify the stack; run `bench_client.php` under JIT OFF (`php -d opcache.enable_cli=0 …`) and JIT ON (`php -d opcache.enable_cli=1 -d opcache.jit_buffer_size=64M -d opcache.jit=tracing …`), and `bench_pdo.php`; parse each script's samples; compute distributions; assemble + `validate()` + write `bench/results/<UTC>-wsl2.json`; teardown.
- **Gate:** `cargo run -p ferro-bench -- --baseline pdo --scenario trivial` (with Docker PG up + `FERRO_TEST_PG_URL` + a built `ferrod`) writes a schema-valid JSON with the ferro (JIT off/on) + pdo distributions + a complete manifest; skip-clean / clear error if the stack is unavailable. **Commit** `feat(s8): PHP bench scripts (ferro client + PDO baseline) + JIT off/on orchestration + live D12 run`.

### Task 3: record the run + close the M0 exit gate
- [ ] Run the harness live and **COMMIT the produced `bench/results/<UTC>-wsl2.json`** (provisional). Confirm `ci/check-d12-recorded.sh` exits 0. Write `bench/README.md` interpreting the numbers (the measured p50/p99 for pure-PHP JIT off/on vs the §16 targets p50<60µs / p99<200µs, and the D12 decision: whether the p99 gate is met on this provisional WSL2 env or flags the accelerator for M1 — a human sign-off note, not a CI failure).
- [ ] Update the execution-design "M0 exit gate" + spec §22.1 to record that the D12 measurement is committed (provisional/WSL2), and note the reference-environment re-run as the remaining human sign-off.
- **Gate:** `ci/check-d12-recorded.sh` exits 0 with the committed result; `ci/local-gate.sh` green. **Commit** `feat(s8): record provisional D12 bench result (M0 exit gate met) + interpretation`.

## Self-Review / Gates
- The M0 exit gate is MET: a committed `bench/results/*.json` + `check-d12-recorded.sh` exit 0. Charter gates green with the `bench` crate in the workspace. The result is honestly tagged `provisional/reference:false`, `ext-msgpack:absent`, JIT off+on both recorded, fanout a documented placeholder.
- **Verify the plan before executing** (the pattern that caught a real issue every slice): a focused adversarial pass on — (1) the fairness framing (is comparing PHP-client→ferrod→PG vs PHP-PDO→PG a sound delta, and is the D12 number defined as the ferro client-side boundary overhead?); (2) the orchestration reliability (launching ferrod + the PHP scripts + JIT toggling from a Rust bin, teardown, the sun_path guard reused from S7); (3) `pdo_pgsql`/`ext-msgpack` absence handling (the ferro number must still record); (4) the manifest completeness vs §16's "meaningless otherwise"; (5) timing methodology (hrtime(true) ns, warmup, sample size for a stable p99, and whether per-iteration samples or a client-side histogram is emitted); (6) that the result JSON satisfies the (trivial) `check-d12-recorded.sh` contract AND is genuinely self-contained/reproducible.

## Execution Handoff
Subagent-driven: fresh implementer per task (TDD/gates), review after, whole-branch review before declaring M0 closed. The live run uses the S2 Docker PG + a built `ferrod` + the S7 PHP client.
