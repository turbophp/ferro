# Ferro development-loop ledger

This file is the shared state of the **autonomous development loop**: a scheduled session fires
every two hours, performs exactly one iteration of the protocol below, and records it here. The
ledger is the memory that survives across sessions — if it isn't written here (or visible in an
open `claude/dev-loop/*` PR), the next iteration doesn't know it happened.

The loop's goal: **drive Ferro from its current state (M1-S8b complete) to v1** — the M1 exit gate
closed, then the remaining SPEC §17 milestones — one small, finished, adversarially-tested slice
per iteration. The charter in `CLAUDE.md` and `ferro-spec-v0.2.md` govern every iteration; nothing
in this file overrides them.

## Protocol (one iteration)

0. **Sync**: fetch `origin/main`; read `CLAUDE.md` ("Current state" + charter) and this ledger from
   `origin/main`, in full.
1. **Tend the open PRs first.** List open PRs whose head branch starts with `claude/dev-loop`.
   A red-CI, merge-conflicted, or changes-requested PR from a previous iteration **is** this
   iteration's work — fix it and push before starting anything new. Items marked IN-FLIGHT below
   with an open PR are taken; do not duplicate them.
2. **Pick** the highest-priority OPEN item from the backlog. Before writing code, **verify the
   defect/gap is still real** against the current tree (re-run the measurement in the follow-up
   doc, or write the failing test first). If it's already fixed, mark it DONE with evidence and
   pick the next item.
3. **Implement one complete slice**: code + tests + spec truth, per the charter's definition of
   done. Protocol work updates `/proto` + golden vectors + both codecs in one change. Run every
   gate the environment allows (`cargo fmt --check`, `cargo clippy --workspace -- -D warnings`,
   `cargo test --workspace` against the testkit backends, PHPUnit, PHPStan L9, `/proto`
   regeneration zero-diff) and record **which gates actually ran** — an unrun gate is reported as
   unrun, never implied green.
4. **Adversarial pass**: before pushing, re-read the diff looking for what a reviewer would
   reject; every 4th iteration (or when the top item is blocked), spend the iteration on bug
   hunting instead of feature work — chaos-harness extension, a `/code-review` pass over recent
   merges, or re-running the DBAL acceptance suite and diffing the recorded numbers.
5. **Record**: update this ledger in the same change — flip the item's state, append an
   iteration-log row. New bugs found but not fixed are appended to the backlog with evidence,
   never left only in the session transcript.
6. **Ship**: commit on a fresh branch `claude/dev-loop/YYYYMMDD-HHMM-<slug>`, push with
   `-u origin`, open a PR to `main`, subscribe to its activity, and drive it to green.
   **Never merge a PR** (humans merge), never push to `main`, never force-push someone else's
   branch, never re-litigate a SPEC §21 decision.

Item states: `OPEN` → `IN-FLIGHT (PR #n)` → `DONE (PR #n, merged)`; `BLOCKED (reason)` where noted.

## Backlog (priority order)

### P0 — the M1-S9 exit gate (what stands between the recorded DBAL numbers and the §14 bar)

| # | Item | State | Notes |
|---|------|-------|-------|
| 1 | `I64 ≥ 2^32` unreadable by `php/client` | DONE (pre-loop) | Fixed on `main` as m1-s8c (`46205ca`); the turnover is `PHP_INT_MAX`, not 2^32. Follow-up doc: `docs/followups/2026-08-11-i64-above-2e32-unreadable-in-php-client.md`. |
| 2 | `int2vector` on the PG read path | OPEN | One type; unblocks the stock PG schema manager, `doctrine/migrations`, and 50 of PG's 78 non-passing DBAL tests. `docs/followups/2026-08-11-pg-int2vector-blocks-the-schema-manager.md`. |
| 3 | PG bind widening `I64 → text/bool` | OPEN | 16 DBAL tests; re-**derive** the §19.3 directional lockstep proof, don't just re-run it. `docs/followups/2026-08-11-pg-bind-matrix-narrower-than-libpq.md`. |
| 4 | ext-vs-pure msgpack packer conformance test | OPEN | Was coupled to item 1 (same file, `PurePacker::be()`); verify whether m1-s8c already shipped it before writing it. |
| 5 | Re-run `testkit/dbal-suite.sh` on all three backends; record new numbers | OPEN | After 2–4 land. Diff against PG 296/374, MySQL 372/385, MariaDB 371/384. Two runs per backend (reproducibility), numbers + ordered failure set into the ledger and CLAUDE.md. |

### P1 — genuinely open M1 items (from CLAUDE.md "Next up")

| # | Item | State | Notes |
|---|------|-------|-------|
| 6 | `affected` on the stream terminal | OPEN | A `/proto` change; lets the prepared path stream and closes the last §14 never-buffer gap. Registry + vectors + both codecs in one change. |
| 7 | MySQL/MariaDB `query_stream` | OPEN | §22.2 (n), deferred at S6/S8b (D-S8b-2). |
| 8 | `/proto` `TxNotFound` error code | OPEN | So `rollBack()` need not swallow `ERR_PROTOCOL` for a tombstoned `tx_id`. |
| 9 | Savepoint verbs in the assist-lexer safe-list | OPEN | `ferro-classify`. |
| 10 | Unbounded backend dial | OPEN | `docs/followups/2026-08-10-unbounded-backend-dial.md`. |
| 11 | Tracker-clean hygiene `None`-skip (R2) | BLOCKED | Still blocked; hygiene currently masks the isolation leak (Task 13). Revisit only with the leak closed. |
| 12 | Chunked `LARGE_OBJECT` bind | OPEN | |

### P2 — the road to v1 (consult SPEC §17 for the binding milestone order)

| # | Item | State | Notes |
|---|------|-------|-------|
| 13 | ORM tier on PG + MySQL | OPEN | §14 bar names the ORM suite; SEQUENCE-strategy documentation for PG (D-S8b-5) ships with it. |
| 14 | SQLite backend | OPEN | §14's stated bar includes SQLite; unblocks the third DBAL column. |
| 15 | Laravel/Eloquent tier (`ferro/laravel`) | OPEN | §15; execution layer only, stock Grammar/Processor. |
| 16 | `ferro-cli` schema sync / check / gen (D10) + manifest store | OPEN | §11. |
| 17 | Deferred perf slices, only against recorded bench numbers | OPEN | §7.2 pipelined hygiene, the 2-channel control/data split (B4), the D12 bench re-run (§16.1). Charter rule 5: measure first. |

## v1 definition (working)

v1 = the SPEC §17 milestones complete through the drop-in bar: M1 exit gate closed (§14 bar met as
far as backends exist, deviations recorded in §22 rather than restated), DBAL **and** Eloquent
tiers config-only green, the chaos suite green on every shipped backend, and a fresh D12 bench
measurement recorded in `bench/results/` with its environment manifest. Refine this section
against SPEC §17 as milestones close — edits to it ride ordinary iteration PRs.

## Iteration log

Newest first. Every session that does loop work appends a row, even for a "nothing to do" or
failed iteration — a silent iteration is indistinguishable from a dead loop.

| When (UTC) | Iteration | Item(s) | Outcome | PR | Gates run |
|------------|-----------|---------|---------|----|-----------|
| 2026-09-08 | bootstrap | — | Ledger created; 2-hour Routine registered; backlog seeded from CLAUDE.md M1-S9 + follow-up docs; item 1 found already DONE on main (m1-s8c). | (bootstrap PR) | n/a (docs-only) |
