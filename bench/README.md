# Ferro bench — D12 boundary latency (the M0 exit gate)

This directory holds the **D12 measurement**, the M0 exit condition (SPEC §16.1 / §21 D12).
`ferro-bench` (a Rust orchestrator) times the trivial-call path

```
PHP client (Ferro\Ferro::connect -> Connection::scalar)  ->  release ferrod  ->  live SELECT 1  ->  response
```

against a **local PDO baseline in the same environment**, computes a stable latency
distribution, and writes ONE self-contained, schema-validated JSON with a complete environment
manifest to `bench/results/`.

**The gate is RECORDING an honest measurement, not passing a latency threshold.** Per SPEC §16.1
the D12 p99 gate is a *human sign-off on recorded numbers*, not a CI threshold — a p99 miss pulls
the `ext-php-rs` accelerator forward into M1. `ci/check-d12-recorded.sh` enforces only that
`≥ 1 bench/results/*.json` is committed. `M0 does not close until that file exists.`

## How to run

```bash
docker compose -f testkit/docker-compose.yml up -d            # the testkit Postgres (:55432)
export FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro
cargo build -p ferrod --release                               # [V1] the D12 number MUST measure a release ferrod
(cd php/client && composer install)                           # [V5] the client autoloader the bench requires
cargo run -p ferro-bench -- --baseline pdo --scenario trivial # writes bench/results/<UTC>-wsl2.json
```

If `FERRO_TEST_PG_URL` is unset or Postgres is unreachable the run **skips cleanly** (exit 0), so
the offline gate stays green. `FERRO_FERROD_BIN` overrides the binary but must be a release path.
`FERRO_BENCH_PHP` overrides the `php` binary.

## Methodology (why the number is honest)

Every choice below defends a *stable, honest* p50/p99 — they are load-bearing, not incidental:

- **Release ferrod [V1].** The engine hop is only meaningful against an optimized build; the
  orchestrator refuses a debug binary and records `manifest.ferrod_build_profile: "release"`.
- **Large, pinned samples [V2].** `W = 2000` warmup (ferrod lazy-pool steady-state + JIT trace
  compilation) then `M = 100000` measured; a small-N p99/p999 is noise. `validate()` asserts
  `samples_n == 100000` for every non-skipped run.
- **GC stays ON [V3].** No `gc_disable()` — the recorded p99 honestly includes cyclic-GC pauses
  (SPEC §16.1's "pure PHP dies first at p99" signal). `manifest.gc_enabled: true`.
- **Tight timing window [V4].** `hrtime(true)` immediately before/after `scalar('SELECT 1')`,
  nothing between; pre-sized sample array; the whole stream emitted once after the loop.
- **Effective, not intended, JIT [V6].** Each run reports `opcache_get_status(false)['jit']`;
  `validate()` fails the run if effective ≠ intended (a WSL2 JIT buffer can silently fail to
  engage). JIT-off keeps OPcache on and toggles only `opcache.jit=off` — isolating the JIT
  variable. Both `-d` directive lists are recorded per run.
- **Fair PDO baseline [fairness].** `PDO` constructed once; per iteration
  `$pdo->query('SELECT 1')->fetchColumn()` — an unprepared query + fetch, matching ferro's
  shape. No `ATTR_PERSISTENT`, no prepared-once statement (either would make the baseline
  artificially fast and overstate ferro's overhead).
- **Panic-safe teardown [V8].** ferrod is owned by a Drop guard (SIGTERM → poll → SIGKILL →
  unlink socket), so a panic in aggregation cannot leak the daemon, its socket, or its upstream
  PG connections.
- **Codec honesty.** `ext-msgpack` is **absent** in this environment, so the pure-PHP
  `Ferro\Protocol\Msgpack\PurePacker` is what is measured for both encode and decode
  (`PackerFactory` hard-wires it regardless — see its docblock). The `ext-msgpack` fast-path swap
  is a documented, **unbuilt** mitigation (SPEC §16.1).

## The recorded provisional result — `results/20260727T145858Z-wsl2.json`

Reference environment (from the manifest): 11th Gen Intel Core i7-11800H, 16 cores, kernel
`6.6.87.2-microsoft-standard-WSL2`, **virtualization WSL2**, `scaling_governor` unknown (WSL2 has
no cpufreq sysfs), PHP 8.4.18 (`ext-msgpack` absent, `pdo_pgsql` present, GC on), rustc 1.95.0,
release ferrod, Postgres `postgres:17@sha256:a426e44b…`. Tagged **`provisional: true`,
`reference: false`.**

Trivial call `SELECT 1`, `W = 2000`, `M = 100000`, transport UDS (client↔ferrod); the actual
query round trip in every path is to the **Dockerized** Postgres on `localhost:55432`.

| run | p50 | p90 | p99 | p999 | JIT (intended→effective) |
|---|---|---|---|---|---|
| ferro-jit-off | 840.3 µs | 1289.8 µs | 1618.7 µs | 1974.9 µs | off → off |
| ferro-jit-on  | 847.6 µs | 1285.6 µs | 1633.5 µs | 2414.1 µs | on → on |
| pdo (baseline)| 677.7 µs | 1095.6 µs | 1509.8 µs | 1805.7 µs | on → on |

**Ferro's added boundary overhead (`overhead_vs_pdo` = ferro − pdo):**

| JIT | Δ p50 | Δ p99 |
|---|---|---|
| off | +162.6 µs | +108.9 µs |
| on  | +170.0 µs | +123.7 µs |

SPEC §16.1 targets (loopback UDS): **p50 < 60 µs, p99 < 200 µs.**

## Interpretation (the D12 decision — a human sign-off note, NOT a CI failure)

**On this provisional WSL2 environment the §16.1 boundary targets are NOT met — and neither is the
raw PDO baseline.** The absolute numbers are *dominated by the environment, not by Ferro's
boundary*: the `SELECT 1` round trip here traverses the Docker userland proxy
(`localhost:55432` → container) over WSL2, and even the raw `PDO/libpq → Postgres` baseline costs
**~678 µs p50** — roughly 17× the 20–40 µs the §16.1 budget assumes for the backend round trip,
and already >10× the entire 60 µs p50 boundary target *before Ferro adds anything*. The absolute
p50/p99 columns therefore say more about WSL2+Docker networking than about the Ferro boundary.

**The meaningful Ferro-specific figure is `overhead_vs_pdo`** — Ferro's added cost over raw PDO in
the *identical* environment (both paths hit the same docker-proxy→PG transport, so that shared tax
cancels in the delta). That overhead is **~163–170 µs p50 / ~109–124 µs p99**. It too exceeds the
60 µs p50 / 200 µs p99 boundary budget, which is exactly the D12 signal:

1. **The bare-metal / host-network reference re-run is the remaining human sign-off.** These
   numbers are `provisional: true, reference: false`. WSL2 timer/scheduler jitter + the Docker
   userland proxy inflate exactly the metric D12 cares about. A re-run with Postgres on host
   networking (or bare metal) is required before the boundary budget can be judged fairly — and
   is the outstanding sign-off, not a code change.
2. **The `ext-php-rs` accelerator (frame codec + hydration, same wire contract) is on the table
   for M1** (SPEC §16.1 D12). The pure-PHP `PurePacker` is what these numbers measure; the
   `ext-msgpack` swap is unbuilt, and the reference re-run decides whether the accelerator is
   pulled forward. JIT on vs off barely moves the number here (the call is I/O-bound on the PG
   round trip), which is itself consistent with "the boundary is dominated by transport, not by
   PHP CPU" in this environment.

**Transport-library caveat.** Ferro's path is `PHP → ferrod (Rust, tokio-postgres) → Postgres`;
the PDO baseline's is `PHP (libpq) → Postgres`. So `overhead_vs_pdo` is Ferro's added boundary
overhead *in this environment* (an extra local UDS hop + the ferrod dispatch + a different
upstream driver), **not** a pure isolation of the client↔ferrod hop. The reference re-run should
carry the same caveat.

**Topology note (deviation from the execution-design's containerized intent).** This provisional
run launches a **host-process** release ferrod and runs the PHP bench **on the host**, both
pointed at the Dockerized testkit Postgres (the plan's directed topology). The execution-design
S8 envisioned a fully-containerized ferrod sidecar + bench. The fairness property is preserved —
the ferro and PDO paths share the identical `docker-proxy → PG` transport, so the `overhead_vs_pdo`
delta still isolates Ferro's own cost — but the fully-containerized topology is part of the
reference re-run scope.

## Result shape

`bench/schema.json` is the reference JSON Schema; the authoritative structural check is
`BenchResult::validate()` in `src/result.rs` (asserts sample count, effective==intended JIT,
release ferrod, required manifest fields, and the provisional/reference tags — a bad shape is
never written). The `fanout` field is an explicit `{placeholder:true, blocked_on:"M3-fibers"}`
until Fibers multiplexing lands.
