# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> This repo is the **Ferro implementation charter**. `ferro-spec-v0.2.md` is the contract; this file is the working agreement. Read the spec in full before writing any code.

## Current state (read first)

**M0 is complete and merged** (SPEC §22.1): the full engine vertical — `/proto` registry, wire codec, `ferrod` session layer, the hand-rolled PG pool, SQL EXEC + TX services, the PHP sync client, and the D12 bench — works end-to-end against real Postgres, with the D12 p99 measurement recorded in `bench/results/`.

**M1 is in progress.** M1-S1 (the real PG protocol-signal pin engine — `Client::transaction_status()` off a vendored `tokio-postgres` fork, replacing the M0 TX-lifecycle stub) is complete: the pool reads the RFQ `I`/`T`/`E` byte after every statement and pins/taints from it, with an unconditional Err-arm fail-safe for the cases where that byte can't be trusted. **M1-S2** (the assist lexer, `ferro-classify`) is complete: a dialect-aware keyword classifier that detects protocol-invisible session mutations — `LISTEN`/`UNLISTEN`, non-xact advisory locks, raw `PREPARE`/`EXECUTE`/`DEALLOCATE`, temp-object DDL, non-local `SET` — with a `pin_on_unknown` default and a per-pool `pin_functions` escape hatch, wired into `Checkout::apply_classify` as the ASSIST signal that taints the connection for hygiene; RFQ (S1) remains the transaction AUTHORITY (assist-not-authority). **M1-S3 (conditional hygiene at checkout) is complete** — a per-state reset profile driven by the S1/S2 pin state: a **tainted** connection gets the full `DISCARD ALL`; a **non-tainted** recycled PG connection gets a **targeted** profile (`CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;`) that closes every session-state leak class — including the §7.4 assist-lexer blind spots (a `set_config(...,false)` GUC, an in-`DO`-body advisory lock, a `WITH HOLD` cursor) — while preserving the engine's future namespaced prepares. The §7.2 "pipelined ahead of the first user statement" optimization is **DEFERRED** to a later perf slice (hygiene runs awaited-at-checkout for now). See `docs/superpowers/specs/2026-07-28-ferro-m1-execution-design.md` for the full M1 slice plan and rationale. **M1-S4 (the full §9.2 error taxonomy + write-fate matrix) is complete** — a central `fate.rs` `classify_fate(err, OpContext)` matrix is the ONE place a `PoolError` becomes a wire `ErrorPayload`, folding in the §19.3 `Indeterminate` rules (readonly/sent/in_tx-gated) and a `57014` (cancel/`statement_timeout`) override; `timeout_ms` + a per-request `CANCEL` are now ENFORCED on both the autocommit and tx-scoped EXEC paths — a cancelled/timed-out autocommit write drains to `Indeterminate` (unconfirmed non-execution), while an in-tx one rolls the transaction back and tombstones the `tx_id` → `Retryable` (the tx's fate is known: it will never commit); a serialization failure or deadlock — including one caught at COMMIT, the dominant real-world SERIALIZABLE-conflict shape (SSI defers the pivot check to COMMIT) — classifies `Retryable`. A live chaos suite (`ferrod`'s `chaos_fate_it.rs`) proves every §19.3 branch against real Dockerized Postgres and that a write is applied at-most-once (no transparent retry, ever — charter rule 3). Next up: **M1-S5** — the Streaming DATA-channel producer (the M0 D-S5-1 deferral).

## What Ferro is

A **per-host database access engine for PHP**. One Rust daemon (`ferrod`, tokio) owns all upstream DB connections, pools them in transaction mode, and multiplexes many PHP-FPM workers over a local Unix socket — while remaining a **drop-in** replacement for Doctrine DBAL and Laravel Eloquent (config-only adoption).

It is deliberately **not**: a database, a SQL rewriter, a result cache, or an ORM-in-Rust. The engine's job ends where live PHP object semantics begin. See Ground rule 6 and SPEC §3.

## Architecture at a glance

The whole design lives in the spec; this is the map, with section pointers so you read the 2–3 relevant sections, not all 47 KB.

```
PHP-FPM workers (N)  ── one UDS conn each, all requests multiplexed ──┐
  ferro/client         native typed API, Fibers-aware   §10, §10.1   │
  ferro/doctrine-dbal  DBAL Driver SPI → client          §14         │
  ferro/laravel        Illuminate Connection → client    §15         │
        │  UDS /run/ferro/{schema_hash}.sock  (TCP fallback FERRO_ADDR)
        ▼
  ferrod (Rust/tokio)                                                │
   session layer   handshake+epoch, auth, multiplexing, PING/PONG  §5
   SQL service     EXEC: param bind, result framing                §6
   TX service      tx_id pinning, savepoints, deadlines            §6, §7
   pin engine      protocol signals first, assist lexer second     §7.1–7.2
   pools           pg | mysql | sqlite | mssql (+ replicas)        §7, §8
   manifest store  query_id → plan (checked SQL / codegen)         §11
   admin/metrics   OTLP, Prometheus, slow log                      §13
```

Load-bearing ideas that shape almost every change (all enshrined in the charter's Ground rules):

- **Transactions pin to a `tx_id`, not to the client socket** (§4, §7). This is what makes Fiber-suspended requests and multiplexing correct.
- **Every in-flight request ends in exactly one `END` frame** (§5.2). All session state machines key off this.
- **The `Indeterminate` error branch** (§9.2, §19.3) — a write whose fate is unknown — is the spec's defining safety property. The engine *never* transparently retries; it classifies and reports, and retry is client policy. Auto-retry of an indeterminate write is licensed *only* by a manifest `idempotent: true`.
- **`boot_epoch`** (§5, §19.1): a changed epoch on reconnect voids all engine-side state; the client resilience loop is built around it.
- **`/proto` is the single source of truth** for method ids, flags, error codes, type tags (§20.2). Hand-written protocol constants in Rust or PHP are a defect.

## Where code will live (planned, SPEC §20.1)

```
/engine                     Cargo workspace (edition 2024, tokio multi-thread)
  crates/ferro-proto        wire types, codec, generated consts, vector tests
  crates/ferro-classify     assist lexer (§7.1)
  crates/ferro-pool         hand-rolled pool + pin state machine (D9)
  crates/ferro-backend-{pg,mysql,sqlite,mssql}
  crates/ferrod             daemon binary (session layer, services, admin)
  crates/ferro-cli          `ferro` CLI: schema sync / check / gen (D10)
/php
  client/                   ferro/client            (PSR-4 `Ferro\`, PHP ≥ 8.2)
  doctrine-dbal/            ferro/doctrine-dbal-driver
  laravel/                  ferro/laravel
/proto                      methods.toml, errors.toml, types.toml, vectors/
/bench                      harness + committed results with env manifests
/testkit                    docker-compose (pg, mysql, mssql), fixtures, suite runners
```

## Spec reading map (by task)

- **Any protocol/wire work** → §5, §5.1 (memfd), §5.2 (session/stream rules), §20.2. Update `/proto` + golden vectors + both codecs in the same change.
- **Pooling / pinning / hygiene** → §7.1–7.7; pin-cause labels in §13; the transaction-mode contract limitation in §7.4.
- **Errors / resilience / restart** → §9.2 (taxonomy), §19 (epochs, reconnect loop, fate matrix). Chaos test is the acceptance bar (§20.3).
- **Type system / hydration** → §9, §9.1 (policies-over-guesses).
- **Doctrine tier** → §14. **Eloquent tier** → §15. Both keep stock Grammar/Processor/platforms; only the execution layer changes.
- **Checked SQL / codegen / manifest** → §11.
- **Bench / M0 exit gate** → §16, §16.1 (the D12 p99 decision gate).
- **Deployment / lifecycle** → §18 (systemd socket activation), §7.7.
- **Decisions already settled** → §21 (D1–D12). Binding; don't re-litigate.

---

# Charter (the working agreement)

## Ground rules

1. **Decisions in SPEC §21 are binding.** Do not re-litigate them in code, comments, or refactors. If one proves impossible, stop and raise it — don't route around it.
2. **`/proto` is the single source of truth** for method ids, flags, error codes, and type tags. Any protocol change updates the registry, the golden vectors, and both codecs (Rust + PHP) in the same change set. Hand-written protocol constants anywhere are a defect.
3. **The engine never transparently retries user statements** (SPEC §3, §19.3). It classifies and reports; retry is client policy. This is load-bearing for the `Indeterminate` guarantee — do not "helpfully" add engine retries.
4. **Every in-flight request terminates in exactly one END frame.** All session-layer code is written against that invariant.
5. **Correctness over throughput until the M0 gate.** Optimize only against recorded bench numbers (SPEC §16.1), never speculatively.
6. **Scope discipline:** no ORM semantics in Rust, no SQL rewriting, no result caching, no read/write inference (SPEC §3). The drop-in tiers change execution, never SQL generation — Grammar/Processor and DBAL platforms stay stock.
7. **PHP client stays dependency-free at runtime.** Optional extensions (`ext-msgpack`, `ext-sockets`) are runtime-detected, never required.

## Build order

Follow SPEC §17 milestones; within M0, execute §17.1 tasks in order. M0 does not exit until the D12 bench measurement is recorded in `bench/results/` with its environment manifest.

## Definition of done (every task)

- Tests: unit + (where applicable) integration against `/testkit` backends; protocol work adds/updates golden vectors; pin-engine work adds a pin-cause assertion; §19 work extends the chaos harness.
- Gates green: `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`, PHPUnit, PHPStan level 9 on `php/client`.
- The relevant SPEC section still tells the truth. If implementation forced a deviation, amend the spec text and add a line to §22 in the same change.

## Commands

```
docker compose -f testkit/docker-compose.yml up -d     # backends
cargo test --workspace                                  # engine
(cd php/client && composer install && composer test)    # client
cargo run -p ferro-bench -- --baseline pdo              # M0 harness
```

Single-test invocations (once the workspaces exist):

```
cargo test -p ferro-proto codec::vectors                # one Rust test/module
cargo test --workspace -- --nocapture <name>            # by name, with output
(cd php/client && ./vendor/bin/phpunit --filter testFoo) # one PHPUnit test
(cd php/client && ./vendor/bin/phpstan analyse --level 9 src)  # static analysis
```

## When uncertain

- Protocol or semantic ambiguity → add it to SPEC §21 open items and ask; do not guess on the wire format or the fate rules.
- Everything else → make the smallest reasonable call, note it in SPEC §22, keep moving.
