# Ferro development-loop ledger

This file is the shared state of the **autonomous development loop**: a scheduled session fires
every two hours, performs exactly one iteration of the protocol below, and records it here. The
ledger is the memory that survives across sessions — if it isn't written here (or visible in an
open `claude/dev-loop/*` PR), the next iteration doesn't know it happened.

The loop's goal: **drive Ferro to v1** along the binding roadmap — SPEC §17's milestones in
order, to the §16 v1 exit criteria — one small, finished, adversarially-tested slice per
iteration. Authority order: `ferro-spec-v0.2.md` is the contract, `CLAUDE.md` is the working
agreement, SPEC §21 + the product decision log (`docs/product-vision.md` §11) are binding, and
nothing in this file overrides any of them.

## Where the project stands (sync with CLAUDE.md "Current state" each iteration)

- **M0 complete** (D12 measurement recorded). **M1 slices S1–S8b complete**: pin engine, assist
  lexer, conditional hygiene, fate matrix + chaos suites, streaming, MySQL/MariaDB backend,
  full type coverage, and the Doctrine DBAL 4 driver with its acceptance gate.
- **M1-S9 (the M1 exit gate) is COMPLETE**: all three measured gaps fixed on `main`
  (int2vector read, I64→text/bool bind, I64 ≥ 2^32), plus B1a/B1b (streamed `affected`,
  prepared-path streaming on PG), and the A5 re-measurement RECORDED: **PG 364/374,
  MySQL 374/385, MariaDB 373/384**, reproducible twice each, every non-pass triaged (b)/(c)
  (`docs/dbal-suite/2026-09-09-a5-results.md`). The §14 bar as written still lacks SQLite
  (no backend until C3) and the ORM suite (C-phase) — stated, not absorbed.
- Not started: M2 (Eloquent, observability, SQLite, DBAL ^3.8 bridge), M3 (Fibers, manifest,
  memfd, COPY), M4 (MSSQL, replica routing, `ferro top`), M5 (streams product, packaging).

## Protocol (one iteration)

0. **Sync**: fetch `origin/main`; read `CLAUDE.md` ("Current state" + charter) and this ledger
   from `origin/main`, in full.
1. **Tend the open PRs first.** List open PRs whose head branch starts with `claude/dev-loop`.
   A red-CI, merge-conflicted, or changes-requested PR from a previous iteration **is** this
   iteration's work — fix it and push before starting anything new. Items marked IN-FLIGHT below
   with an open PR are taken; do not duplicate them.
2. **Pick** the highest-priority OPEN item: current phase first, in item order. Before writing
   code, **verify the defect/gap is still real** against the current tree (re-run the follow-up
   doc's measurement, or write the failing test first). If already fixed, mark it DONE with
   evidence and take the next item.
3. **Implement one complete slice**: code + tests + spec truth, per the charter's definition of
   done. Protocol work updates `/proto` + golden vectors + both codecs in one change. Run every
   gate the environment allows (`cargo fmt --check`, `cargo clippy --workspace -- -D warnings`,
   `cargo test --workspace` against the testkit backends, PHPUnit, PHPStan L9, `/proto`
   regeneration zero-diff) and record **which gates actually ran** — an unrun gate is reported
   as unrun, never implied green.
4. **Adversarial pass**: before pushing, re-read the diff looking for what a reviewer would
   reject; every 4th iteration (or when the top item is blocked), spend the iteration on bug
   hunting instead of feature work — chaos-harness extension, a review pass over recent merges,
   or re-running the DBAL acceptance suite and diffing the recorded numbers.
5. **Record**: update this ledger in the same change — flip the item's state, append an
   iteration-log row. New bugs found but not fixed are appended to the **Found bugs** section
   with evidence, never left only in the session transcript.
6. **Ship**: commit on a fresh branch `claude/dev-loop/YYYYMMDD-HHMM-<slug>`, push with
   `-u origin`, open a PR to `main`, subscribe to its activity, and drive it to green.
   Never push to `main` directly, never force-push someone else's branch, never re-litigate a
   SPEC §21 or product-log P1–P12 decision.
7. **Merge — standing owner authorization (directive, 2026-09-09).** The loop MAY merge a
   `claude/dev-loop/*` PR itself when ALL of these hold: every CI check is GREEN on the current
   head; the PR is mergeable (no conflict); any PR it stacks on is merged FIRST (stack order);
   and no human review is pending or requesting changes. Merge with a merge commit (the repo's
   convention). **Never merge red, never merge with checks still running, never merge a PR the
   loop did not open.** Merging happens at the START of a firing (part of step 1's PR tending),
   so every merge is against a freshly verified state — and the phase-gate rule stands: moving
   to the next phase still requires the current phase's acceptance bar re-measured and recorded.

**Scope guardrails** (from the charter + product vision, restated because a loop drifts):
correctness over throughput — perf work only against recorded bench numbers (§16.1); no ORM
semantics in Rust, no SQL rewriting, no result caching, no read/write inference; the engine
never transparently retries; **v1 stays undiluted** — no post-v1 family work (HTTP engine,
queues, State, tenant pools) enters this backlog until the §16/§17 bar is met.

**Model & quota discipline.** Iterations run on the strongest model (Fable) because this codebase
is correctness-critical, but spend it where it pays and delegate the rest:

- **Delegate to cheaper subagents** (Agent tool with `model: "haiku"` or `"sonnet"`): broad
  codebase searches and "where does X live" questions (Explore agent), gate-log triage, follow-up
  doc cross-checks, drafting routine ledger/doc updates. Fan independent searches out in parallel.
- **Keep on the main (Fable) thread**: slice design, the actual Rust/PHP implementation, anything
  touching the fate matrix / pin engine / wire protocol, and the adversarial diff re-read.
- **Cheap no-ops**: decide "nothing actionable" early — check open PRs and the ledger before
  reading anything heavy; read spec *sections* (the CLAUDE.md reading map), never the whole spec.
- **Proportionality**: one iteration ≈ one small slice. If an item won't fit, split it in the
  ledger (A3 → A3a/A3b) rather than burning a session on an unfinished large diff.

Item states: `OPEN` → `IN-FLIGHT (PR #n)` → `DONE (PR #n, merged)`; `BLOCKED (reason)` where noted.

## Backlog, phased by roadmap

**Current phase: B (M1 loose ends).** Phase A (M1-S9) CLOSED 2026-09-09 in PR #10: every row
DONE, the acceptance bar re-measured on all three backends (PG 364/374, MySQL 374/385,
MariaDB 373/384 — every non-pass triaged (b)/(c)), CLAUDE.md updated in the closing PR. A phase
closes when every row is DONE (or explicitly BLOCKED with the block recorded), its milestone's
acceptance bar is re-measured and recorded, and CLAUDE.md's "Current state" is updated in the
closing PR.

### Phase A — M1-S9: the M1 exit gate

What stands between the recorded DBAL numbers and the §14 bar, in measured-impact order.

| # | Item | State | Notes |
|---|------|-------|-------|
| A1 | `I64 ≥ 2^32` unreadable by `php/client` | DONE (pre-loop) | Fixed on `main` as m1-s8c (`46205ca`); turnover is `PHP_INT_MAX`, not 2^32. `docs/followups/2026-08-11-i64-above-2e32-unreadable-in-php-client.md`. |
| A2 | `int2vector` on the PG read path | DONE (PR #4, merged) | `int2vector` (22) AND `oidvector` (30) admitted as TEXT (PG's own space-separated rendering, `::text`-oracle-proven live on real catalog cells); bind direction deliberately untouched; array class stays deferred (`int2[]` is the new boundary sentinel). SPEC §22.2 (ae). |
| A3 | PG bind widening `I64 → text/bool` | DONE (PR #5, merged) | `I64 → text` (TEXT only, decimal rendering, Format::Text) + `I64 → bool` **value-gated to 0/1** (the explicit §9.1 decision; any other integer refused pre-send). Lockstep proof re-derived: `I64(1)` added to `every_variant` — a value-gated widening is invisible to the cross product without its accept-side value. Proven live in both measured shapes. SPEC §22.2 (af). |
| A4 | ext-vs-pure msgpack packer conformance test | DONE (pre-loop, verified) | Shipped by m1-s8c as `php/client/tests/Conformance/PackerConformanceTest.php` (three arms, fixtures derived from `registry.lock.json`, loud-skip discipline with `FERRO_REQUIRE_EXT_MSGPACK=1`). Verified by EXECUTION, not inspection: 80 tests / 93 assertions green in-container (5 skips = the ext arms, extension absent here; CI's php lane installs it and is green on main); full offline suite 712/2033 matches m1-s8c's record; PHPStan L9 clean. |
| A5 | Re-run `testkit/dbal-suite.sh` on all three backends; record numbers | DONE (PR #10) | **PG 364/374 (+68 — exactly the predicted 50 int2vector + 16 bind + 2 BigInt), MySQL 374/385 (+2), MariaDB 373/384 (+2)**, two-run reproducibility verified on all three (six comparisons, ordered failure sets identical). Every remaining non-pass is in S8b's (b)/(c) triage — no category-(a) driver defect, no category-(e) engine gap remains. Full record + provenance: `docs/dbal-suite/2026-09-09-a5-results.md` (runs 34351348533 + 34357604689; the first dispatch's pg job died on a host-port ephemeral-range collision, root-caused, lane fixed with kernel-side port reservation). SQLite + ORM stay C-phase debts. |

### Phase B — M1 loose ends (open items CLAUDE.md names, before M2 starts)

| # | Item | State | Notes |
|---|------|-------|-------|
| B1a | Stream terminal `affected` reaches the client | DONE (PR #8, merged) | **The item's premise was FALSE** (verify-the-defect-first caught it): the wire + engine ALWAYS carried a truthful `affected` (command tag, post-drain); the client dropped it. No `/proto` change exists to make. `RawStream::{settled, affected, lastInsertId}` now settle on the Ok terminal — null-until-settled, never an invented 0. Proven live (500-row drain → affected 500). §22.2 (ag) corrects (ac). |
| B1b | Driver prepared path streams using B1a | DONE (PR #9, merged) | `runPrepared` streams on PG (`exec` deliberately keeps `fetch:none` — savepoints ride it); the open decision made: `Result::rowCount()` is **drain-then-answer** (drained rows stay fetchable; freed-before-terminal keeps 0). Closes §14's never-buffer clause on PG and restores `pdo_pgsql` `rowCount()` parity on streamed SELECTs. §22.2 (ah); known-incompatibilities entry replaced. |
| B2a | MySQL fork: owned-conn stream recovery (`into_conn`) | IN-FLIGHT | B2 split per proportionality (§22.2 (n) records TWO structural blockers: the fork's one-way owned route AND the `finalize_stream` restructure — one iteration can close one honestly). Fork gains a `done` flag (terminal no longer drops an owned conn; FusedStream semantics preserved), `ResultSetStream::into_conn()` + `QueryResult::into_conn()` (the latter for the no-result-set case that eats the conn via `stream_and_drop()→None`; dispatch signal `columns_ref().is_empty()`), one new `DriverError` variant. Live 4-part gate on BOTH engines asserting SESSION IDENTITY, `stream_recovery_it.rs`. UPSTREAM_PR_MYSQL_ASYNC.md Draft 2 (drafted-not-filed). (n) amended. |
| B2b | `ferro-pool`/`ferrod` wiring: MySQL `RowStream` over the recovered conn | OPEN | The `Option<Conn>` take/put-back dance in `MysqlConn`, `finalize_stream` restructure, flip `supports_row_streaming()`, repoint `mysql_it.rs`'s two refusal tests, extend the stream chaos coverage. Needs B2a merged. |
| B2c | Driver drops the buffer-on-MySQL fallback (D-S8b-2) | OPEN | `runPrepared`/`query` stream on MySQL too; repoint `ConnectionFateFlagTest` providers + known-incompatibilities `rowCount` entry (MySQL post-drain terminal reports 0 for SELECT — §22.2 (n)'s measured fact — so `rowCount()` parity needs stating, not assuming). Needs B2b. |
| B3 | `/proto` `TxNotFound` error code | OPEN | So `rollBack()` need not swallow `ERR_PROTOCOL` for a tombstoned `tx_id`. |
| B4 | Savepoint verbs in the assist-lexer safe-list | OPEN | `ferro-classify`. |
| B5 | Unbounded backend dial | OPEN | `docs/followups/2026-08-10-unbounded-backend-dial.md`. |
| B6 | Chunked `LARGE_OBJECT` bind | OPEN | |
| B7 | Tracker-clean hygiene `None`-skip (R2) | BLOCKED | Hygiene currently masks the isolation leak (S8a Task 13). Revisit only with the leak closed. |

### Phase C — M2 (SPEC §17): the Eloquent milestone

| # | Item | State | Notes |
|---|------|-------|-------|
| C1 | Eloquent tier (`ferro/laravel`) + PDO shim | OPEN | §15; Illuminate `Connection` execution layer only, stock Grammar/Processor. Multiple slices. |
| C2 | Illuminate integration suite green through a Ferro connection | OPEN | The M2 acceptance bar — and the P10 go-to-market prerequisite. Suite runner modeled on `testkit/dbal-suite.sh` (with its hard contact assertion — the SQLite-fallback lesson). |
| C3 | SQLite backend, engine-owned mode (§7.6) | OPEN | Also unblocks the SQLite column of the §14 DBAL bar — re-run A5's suite when it lands. |
| C4 | Observability: OTLP traces, Prometheus, slow log (§13) | OPEN | Redaction contract per product-vision §5: fingerprints only, closed label vocabularies. |
| C5 | DBAL `^3.8` bridge | OPEN | §14. |
| C6 | Known-incompatibilities doc page | OPEN | §14–15; seed from `docs/known-incompatibilities.md` + D-S8b-5 (ORM-on-PG SEQUENCE strategy). |

### Phase D — M3 (SPEC §17)

| # | Item | State | Notes |
|---|------|-------|-------|
| D1 | Fibers multiplexing in `ferro/client` (§10.1) | OPEN | |
| D2 | `ferro check`/`gen` + `idempotent` manifest + manifest handshake (§11) | OPEN | The only licensed auto-retry lives here. |
| D3 | memfd large-payload path behind `MEMFD_RX` (§5.1) | OPEN | |
| D4 | COPY API | OPEN | |

### Phase E — M4 + M5 (SPEC §17), then the v1 gate

| # | Item | State | Notes |
|---|------|-------|-------|
| E1 | MSSQL backend (mode per D1 outcome) | OPEN | |
| E2 | Manifest-only hardening mode | OPEN | |
| E3 | Replica routing + lag gating (§7.5) | OPEN | |
| E4 | `ferro top` | OPEN | |
| E5 | LISTEN/NOTIFY streams | OPEN | M5. |
| E6 | Runtime guidance (Octane; widen to FrankenPHP per product-vision §9, tested config) | OPEN | M5. |
| E7 | Packaging: deb/rpm/container sidecar, systemd socket-activated units (§18) | OPEN | M5. |
| E8 | **v1 exit measurement** (§16) | OPEN | On the recorded reference environment: boundary p50 < 60 µs / p99 < 200 µs, ≥5× connection reduction, fan-out ≤ max+2 ms, 1 GB stream RSS bounds, >95 % statement-cache hit rate. Honor the D12 accelerator decision. Results + env manifest into `bench/results/`. |

### Standing (any phase, any iteration)

- Deferred perf slices (§7.2 pipelined hygiene, the B4 two-channel split) — only against
  recorded bench numbers, per charter rule 5.
- Every 4th iteration: the adversarial/bug-hunt pass (protocol step 4).
- Keep CLAUDE.md's "Current state" truthful when a slice changes it.

## Found bugs (open; found by the loop, not yet fixed)

*None yet. Append with evidence: what was measured, where it lives, severity, what it blocks.*

## v1 definition

v1 = SPEC §17 milestones M1→M5 complete in order, with the two suite bars green (DBAL per §14 as
far as backends exist, Illuminate per §15/M2), the chaos suite green on every shipped backend,
and the §16 performance targets **measured and recorded** in `bench/results/` on the reference
environment. Deviations are recorded in SPEC §22, never silently absorbed. Post-v1 work (product
vision §4) is out of the loop's scope by P-log decision.

## Iteration log

Newest first. Every session that does loop work appends a row, even for a "nothing to do" or
failed iteration — a silent iteration is indistinguishable from a dead loop.

| When (UTC) | Iteration | Item(s) | Outcome | PR | Gates run |
|------------|-----------|---------|---------|----|-----------|
| 2026-09-09 | 10 (routine→session) | #10 merged (step 7) + B2 split → B2a | PR #10 (Phase A close) merged under the standing directive after fresh verification (5/5 green on `ad6d8d1`, mergeable, no review) — Phase A is closed ON MAIN. Then B2: the Explore map + §22.2 (n) confirmed it cannot fit one iteration (fork edit + pool-trait restructure + driver flip), so split B2a/B2b/B2c and shipped B2a — the vendored `mysql_async` fork's owned-connection stream recovery: terminal-`None` no longer closes an owned conn, `into_conn()` on both `ResultSetStream` and `QueryResult` (the no-result-set hole found during design: `stream_and_drop()→None` EATS the conn — B2b would have hit it on any streamed write), refusal variant for the borrowed route, 4-part live session-identity gate on both engines, Draft 2 in UPSTREAM_PR_MYSQL_ASYNC.md, (n) amended. | (this PR) | fmt, clippy --tests -p ferro-backend-mysql -D-clean, fork `cargo check` clean, workspace tests: see row (PG live locally; MySQL gates self-skip in-container — CI's integration lane, which sets both DSNs, is the authority for `stream_recovery_it.rs`) |
| 2026-09-09 | 9 (routine→session) | merge directive + A5 → **Phase A CLOSED** | First execution of protocol step 7: all six queued PRs verified 5/5 checks green on their exact expected heads, no human review pending, merged in stack order with merge commits, each pinned by `expectedHeadSha` — main is `6d507de`; A2/A3/B1a/B1b DONE. Then A5, same firing: dispatched family=all on post-merge main (run 34351348533); MySQL 374/385 + MariaDB 373/384 recorded reproducible; the pg job died on a host-port bind collision (55432 is in Linux's ephemeral range) — root-caused, lane fixed (kernel-side `ip_local_reserved_ports` + one retry), PG re-dispatched (run 34357604689, code tree = main exactly): **PG 364/374, the predicted +68 to the test**. Every remaining non-pass on all three backends is (b)/(c)-triaged. A5 DONE, **Phase A closed**, CLAUDE.md numbers updated. Loop lesson recorded: a measurement lane's first real dispatch IS its test — the port fix is the lane's first field defect, found and closed inside one firing. | #10 | The measurement itself (4+2 suite runs on GH runners, digest-pinned backends, contact assertion + reset lines verified in every run); PR #10 CI green on both heads; no local gates (docs + workflow diff only) |
| 2026-09-09 | 8 (routine→session) | B1b + merge directive | Prepared path streams on PG with `rowCount()` drain-then-answer; the three affected unit tests repointed with their premises named; live pin added (`executeStatement` returns 7 through the streamed path; prepared-SELECT `rowCount()` answers 10 where it answered 0). OWNER DIRECTIVE recorded as protocol step 7: standing authorization to merge green, mergeable, stack-ordered loop PRs at the start of each firing — never red, never mid-check, never someone else's. Effective next firing per the directive. | (this PR, stacked on #8) | driver offline 192/433 + live 36/176 (PG; 15 MySQL skips honest), client 714/2047, PHPStan L9 both, Rust//proto untouched. Local PG service crashed a THIRD time mid-gates (restart + clean re-run; CI authoritative). |
| 2026-09-09 | 7 (routine→session) | B1a | The verify-first rule earned its keep: B1's premise ("the stream terminal carries no `affected` — a /proto change") was measured FALSE — PROTOCOL.md §10 has carried it since S5 and the engine fills it from the command tag; the client's pump dropped the body. Shipped the client half: `StreamTerminal` settled-state cell + `RawStream::{settled, affected, lastInsertId}`, fixture gains scripted DATA frames, unsettled-on-abandon/error/body-less pinned. §22.2 (ag) corrects (ac); B1 split → B1a (this) / B1b (driver, OPEN). | (this PR) | php/client: unit 714/2047 offline + live 50/793 (PG-only; 9 MySQL skips honest — no MySQL in-container), RawStreamLiveTest 4/518 incl. the settled-500 proof through real ferrod, PHPStan L9 clean. Rust untouched; /proto untouched (measured: nothing to change). |
| 2026-09-09 | 6 (routine→session) | adversarial pass | A5 blocked on merges → the ledger's bug-hunt rule fired: high-effort code review over the unmerged A2+A3 stack. 4 verified findings, all in the vector walker, all FIXED on PR #4's branch pre-merge: (F1) the lower bound was the one recv-enforced field left unchecked — now refused; (F2) MEASURED via `int2vectorsend` that PG sends the empty vector as 1-D/0-items, not 0-D — the dead 0-D accept arm became a refusal and the wrong comment corrected; (F3) the hand-rolled walker documented as deliberate vs `postgres_protocol::array_from_sql` (named-refusal diagnostics); (F4) element OIDs via named `Type` constants. No severe finding survived verification; the §19.3 direction and both live shapes re-verified. | pushed to #4 | lib 73/73, live vector suite green (PG 16.13), fmt, clippy -D warnings, workspace 767/0 on re-run (one first-run failure = local PG service flake, twice-crashed container service; CI authoritative) |
| 2026-09-09 | 5 (routine→session) | A5 (enabler) | Built the `dbal-suite` measurement lane: a `workflow_dispatch` GH Actions job running `testkit/dbal-suite.sh` per family (matrix from the input), TWICE per the reproducibility rule, uploading both logs + ferrod.log as artifacts, with a broken-run gate that fails ONLY when a run produced no phpunit result line (a red suite is data, not a build failure — deliberately not in the per-push `ci` workflow). Needed because this container cannot run Docker; the recorded environment must be the digest-pinned backends anyway. | (this PR) | YAML validated; the lane itself is exercised on first dispatch — recorded honestly as UNRUN until then. Rust/PHP untouched this iteration. |
| 2026-09-09 | 4 (routine→session) | A4 | Verified DONE by execution (m1-s8c shipped it): conformance test runs 80/93 green in-container, ext arms loud-skip as designed and run in CI, offline suite 712/2033 matches the record, PHPStan L9 clean. Container note for future iterations: composer-through-proxy needs `env -u GITHUB_TOKEN COMPOSER_AUTH='{}'` (a placeholder GITHUB_TOKEN poisons composer's GitHub auth), and a dist-only package (`phpstan/phpstan`) needs its zipball built from a git clone at the locked ref and seeded into `~/.cache/composer/files/<name>/sha1(distUrl).zip` — the API zipball endpoint is proxy-gated. Phase A now has only A5, which needs PRs #4 + #5 MERGED first (the re-run must measure main). | (this PR) | php/client: phpunit 712/2033 offline + conformance 80/93, PHPStan L9 clean. Rust untouched this iteration. |
| 2026-09-09 | 3 (routine→session) | A3 | `I64 → text` + value-gated `I64 → bool` PG bind widening; lockstep proof re-derived (fixture blindness class named and closed); F64 arms untouched; both measured Doctrine shapes proven live incl. the pre-send refusal leaving the conn clean. First firing of the repointed Routine — it landed in-session and shipped, confirming the loop architecture fix. | (this PR, stacked on #4) | fmt, clippy -D warnings, cargo test --workspace 769/0 with PG live (system PG 16.13; MySQL/MariaDB self-skip locally, CI authoritative), /proto untouched, PHP untouched |
| 2026-09-08 | 2 (in-session) | A2 | `int2vector`+`oidvector` admitted on the PG read path as TEXT; renderer strict on the array-wire format (each malformed shape a named Backend refusal); live `::text` oracle on 50 `pg_index.indkey` + 50 `pg_proc.proargtypes` rows byte-equal; deferral guard repointed to `int2[]`. Loop note: iterations 1–2 of the fresh-session Routine produced zero pushed output — root cause was structural (fired sessions get no git outcome credentials and no GitHub tools), Routine repointed to fire into the orchestrator session. | (this PR) | fmt, clippy -D warnings, cargo test --workspace 767/0 with PG live (SYSTEM PG 16.13 — Docker unavailable in-container, blob CDN blocked by proxy policy; MySQL/MariaDB live suites self-skipped, CI closes that gap), /proto untouched (zero-diff vacuously); PHP suites untouched by this diff, left to CI |
| 2026-09-08 | bootstrap+1 | CI red on main | Found + fixed: the `rust` lane has been red on main since M1 landed — the ferrod fuzz dir is its own workspace root, so the root `[patch.crates-io]` never reached it and it resolved stock tokio-postgres/mysql_async (E0599 on the fork accessors). Patches mirrored into the fuzz manifest; same commit ported here. | #3 (ported into #2) | `cd engine/crates/ferrod/fuzz && cargo check` (the failing command) clean locally; full workspace gates left to CI |
| 2026-09-08 | bootstrap | — | Ledger created from a full pass over SPEC §16/§17, CLAUDE.md "Current state", docs/followups/, and docs/product-vision.md; backlog phased A–E to the v1 gate; 2-hour Routine registered; A1 found already DONE on main (m1-s8c). | #2 | n/a (docs-only) |
