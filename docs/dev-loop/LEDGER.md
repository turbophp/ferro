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

**Current phase: C (M2 — the Eloquent milestone), scoping.** **Phase B (M1 loose ends) CLOSED
2026-09-10.** Delivered: B1a/B1b (the stream terminal's `affected` captured client-side, then the
driver's prepared path streaming on PG), B2 across six slices (`fork edit → reclaim seam → bound →
parkable conn → stream → driver flip`, closing SPEC §22.2 (n) and retiring D-S8b-2), B3 (`/proto`
`TxNotFound`), B5 (the bounded backend dial, both instances), B6a (the outbound frame-size guard).
Deliberately NOT done, each with its reason recorded in-row: B4 (WONTFIX — already solved by a
different mechanism, and the proposed "fix" had been rejected in writing by M1-S8a), B6b (chunked
`LARGE_OBJECT`, a real `/proto` slice, deferred to M2), B7 (deferred on the spec's own §7.2
condition). **No milestone acceptance bar is re-measured for this phase**, and that is deliberate:
Phase B carried no bar of its own — M1's bar was measured at Phase A close (A5) and none of B's
changes touch the DBAL-suite surface it measured. The next bar is M2's (C2).

**The loop lesson of Phase B, recorded because it changed outcomes four times:** the ledger is a
good QUEUE and a poor ORACLE. Verify an item's premise against the code before building it. B4's
premise was false (the classifier result is unreachable) and its fix had been deliberately rejected;
B5's ticket described the checkout dial but the hotter instance was the inline-awaited cancel dial;
B6 named a feature while a real defect sat beside it; B7's own recorded block was not the actual
gate. In each case the check took minutes and changed what shipped.

Phase A (M1-S9) CLOSED 2026-09-09 in PR #10: every row
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
| B2a | MySQL fork: owned-conn stream recovery (`into_conn`) | DONE (PR #11, merged) | B2 split per proportionality (§22.2 (n) records TWO structural blockers: the fork's one-way owned route AND the `finalize_stream` restructure — one iteration can close one honestly). Fork gains a `done` flag (terminal no longer drops an owned conn; FusedStream semantics preserved), `ResultSetStream::into_conn()` + `QueryResult::into_conn()` (the latter for the no-result-set case that eats the conn via `stream_and_drop()→None`; dispatch signal `columns_ref().is_empty()`), one new `DriverError` variant. Live 4-part gate on BOTH engines asserting SESSION IDENTITY, `stream_recovery_it.rs`. UPSTREAM_PR_MYSQL_ASYNC.md Draft 2 (drafted-not-filed). (n) amended. |
| B2b-1 | `ferro-pool` reclaim-hook seam for a conn-owning stream | DONE (PR #12, merged) | B2b split (proportionality): the `Option<Conn>` refactor across 42 MySQL-backend sites is MySQL-only/CI-only-provable, so land the backend-agnostic seam first. New `PoolBackend::reclaim_stream(&mut conn, rows) -> Result<u64>` (default: the stream's `rows_affected()` — PG/fake byte-identical), called by `RowStreamHandle::finish` AFTER the drain and BEFORE `finalize_stream` reads `tx_status(&conn)` — exactly the restructure §22.2 (n) said was needed so a conn-OWNING stream (MySQL moves the `Conn` into the stream) can hand it back before finalize runs. `finish`'s reclaim-Err arm treats the stream as errored (Rule-A force-taint) so the pool discards the husk. Fake gains `arm_reclaim_fail` + override; test `reclaim_hook_clean_recycles_but_a_failed_reclaim_discards_the_conn` proves both arms (same-id recycle vs fresh-id discard) on `max_size=1`. PG unchanged. |
| B2b-1b | FB-3: the reclaim step in `finish()` is BOUNDED | IN-FLIGHT | The HIGH iteration-12 finding, fixed at the pool level before B2b-2 arms it. `RowStreamHandle::finish` now wraps its `reclaim_stream` await in `tokio::time::timeout(config.checkout_timeout, ..)` — the SAME knob the checkout-time recycle already uses to bound its ROLLBACK/RESET cleanup, so no new operator knob. A timed-out reclaim is treated exactly like a failed one (`affected = 0`, Rule-A force-taint), and `finish` ALWAYS returns, so `ferrod`'s unraced `finish().await` can never strand the request's single terminal END (charter rule 4). Proven by `a_hung_reclaim_cannot_strand_finish` under `start_paused` (asserts the bound really elapsed), and MUTATION-PROVEN: with the timeout removed the test hangs indefinitely instead of failing fast — the exact FB-3 failure mode. The pre-existing unbounded DRAIN loop inside `finish` (there since S5) is deliberately NOT touched: bounding it would mean dropping a mid-protocol PG `RowStream`, a behavior change this slice will not make blind — recorded as FB-3b below. |
| B2b-2a | `MysqlConn` becomes PARKABLE (`Option<Conn>`) + the parked-state contract | IN-FLIGHT | B2b-2 split again (proportionality + risk): the 42-site refactor is mechanical and CI-only-provable, so land it WITHOUT flipping the capability — if CI rejects something, it is the refactor, not the streaming. `MysqlConn.mysql` is now `Option<Conn>` with `park`/`unpark`/`is_parked` + `driver`/`driver_mut` accessors, and **every method reachable while parked answers safely**: `is_closed` → dead (the FB-2 contract — the ONLY signal that makes the pool discard rather than recycle), `tx_status` → `Failed` (conservative; a falsely-clean `Idle` is the one unsafe answer), `take_session_mutated` → false, `last_insert_id` → None, and `cancel_handle` now reads a conn id STORED at connect so a cancel stays obtainable mid-stream (verified safe: `mysql_async` writes `inner.id` exactly once, in `handle_handshake`; `COM_RESET_CONNECTION` preserves the server thread id). Live proof on both engines: `conn_it.rs::{mysql,mariadb}_parked_conn_contract` — park ⇒ dead + conservative, unpark ⇒ SAME session id and the session marker intact. Capability still `false`; nothing parks in production yet. |
| B2b-2b | MySQL owned-conn `RowStream` + capability flip + ferrod | DONE (PR #16, merged) | Now a small slice on the B2b-2a husk: a real `MysqlRowStream` that owns the parked `Conn`, a `reclaim_stream` override that `unpark`s it via B2a's `into_conn`, flip `supports_row_streaming()` → true, repoint `mysql_it.rs`'s two refusal tests, add the live incremental proof. **SHIPPED:** `crate::stream` with a two-shape `MysqlRowStream` — `Rows` owns the parked `Conn` and drives the fork's `ResultSetStream`; `NoRows` is the dispatch that stops a no-result-set statement from taking the owned route (`stream_and_drop()` answers `None` there AND eats the connection), decided on the PREPARED column count. `reclaim` drains via B2a's `into_conn`, reads `affected` from the CONNECTION post-drain (the §22.2 (n) rule), unparks, and records the §7.1 taint. Capability flipped → both EXEC arms follow the ONE authority. `mysql_it.rs`'s two refusal tests REPLACED by positive proofs on both arms, each asserting exactly one END and — the load-bearing part — that the SESSION (autocommit) and the TRANSACTION (tx-scoped, incl. COMMIT) survive the park/reclaim round trip. **Honest residuals:** criterion (c) (abandoned owning stream ⇒ discarded) is proven at the UNIT level by B2b-2a's live parked-conn contract (parked ⇒ `is_closed` dead, which is the whole mechanism) but has NO end-to-end cancel-mid-stream test on MySQL yet — recorded as FB-4 below, not claimed; FB-3b (unbounded drain) untouched; and a `fetch:stream` `CALL` discards its rows, a pre-existing MySQL-backend blind spot documented in the module. |
| B2b-2 | MySQL owned-conn `RowStream` + capability flip + ferrod | OPEN | Now a LOCALIZED backend change on the B2b-1 seam: `MysqlConn.mysql` → `Option<Conn>` (take in `query_stream`, restore in `reclaim_stream`), a real `MysqlRowStream` driving `ResultSetStream` via B2a's `into_conn`, flip `supports_row_streaming()` → true, repoint `mysql_it.rs`'s two refusal tests + add the incremental live proof, and the ferrod arms follow the ONE authority automatically. CI-only-provable (needs live MySQL). Needs B2b-1 merged. **BLOCKING acceptance criteria from the iteration-12 hunt — (a) and (b) are now CLOSED (B2b-1b and B2b-2a respectively); (c) remains:** (a) ~~a bounded `reclaim` so a hung `COM_RESET_CONNECTION` cannot stall the terminal END~~ **DONE at the pool level (B2b-1b)** — B2b-2 still owns the MySQL half: a dropped/timed-out reclaim MUST leave the conn `is_closed`-dead (see (b)), and the DRAIN half of that bound is still open (FB-3b); (b) ~~MySQL `is_closed()` MUST report dead when the inner conn is parked~~ **DONE (B2b-2a)** — parked reads dead, `tx_status` answers `Failed`, proven live on both engines; (c) an abandoned owning stream (Drop-net path) MUST end up DISCARDED, not merely tainted (tainted alone recycles). A fake cannot model a parked-conn backend, so these need the live MySQL suite. |
| B2c | Driver drops the buffer-on-MySQL fallback (D-S8b-2) | IN-FLIGHT | **B2 IS COMPLETE WITH THIS.** Both pool-kind gates (`query`, `runPrepared`) removed — every family streams, retiring D-S8b-2. The `rowCount()` question this row flagged resolved without a code change: a MySQL SELECT still answers `0`, but now because its post-drain OK packet says `0` (§22.2 (n)'s second measured fact), not because the driver buffered — observable unchanged, mechanism different, and `docs/known-incompatibilities.md` now says which. `StreamingLiveTest::testMysqlIteratesCorrectlyEvenThoughItBuffers` — written to be the thing that fails "the day MySQL streaming lands" — did exactly that and is repointed to assert MySQL now MATERIALISES on interleave like PG, on the same `settledRowCount()` counter that used to read 0. Recorded consequences: the driver now assumes every backend streams (the client learns a pool's `kind`, never its capabilities), so a future non-streaming backend (SQLite, C3) needs streaming or a `HELLO_ACK` capability; and `Result::buffered()` is no longer constructed anywhere in `src` (only `ResultTest` exercises it) — left in place deliberately rather than widening this PR with an API removal, worth a tidy-up when someone is in there. `exec()`'s `fetchRaw` is untouched and still correct (savepoints ride it). |
| B3 | `/proto` `TxNotFound` error code | DONE | `0x300B`, NonRetryable. Two engine sites move (`resolve_active`'s not-found/forbidden arm, `actor_gone_terminal`'s non-tombstone arm); everything else stays `Protocol`. `ERR_PROTOCOL` is OUT of the client's `TX_ALREADY_GONE`, so a malformed `TxControl` body throws out of `rollBack()` again. Falsifier at all three tiers + live; both unit guards mutation-proven. SPEC §22.2 (ai); the (t) known-cost note is marked paid off. |
| B4 | Savepoint verbs in the assist-lexer safe-list | CLOSED — WONTFIX, item was stale | **Verified before building, and the premise did not survive.** The classifier really does return `Some(Unknown)` for `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` on both dialects (measured) — but that is UNREACHABLE: `Checkout::apply_classify_for` skips the lexer entirely on a `SavepointPassthrough` verdict, and outside a transaction the guard refuses the statement outright. Measured through a real `Checkout`: all three verbs give `tainted=false`. M1-S8a considered widening `SAFE_LEADING_KEYWORDS` and **rejected it in writing** — keying off the guard's verdict is NARROWER, so a savepoint reaching any path that did not pass the guard's three refusals is still conservatively classified. Already proven by `s8a_savepoint_passthrough_does_not_taint`. Implementing B4 would have undone a deliberate decision. |
| B5 | Unbounded backend dial | DONE | `Pool::checkout` bounds `backend.connect()` with `checkout_timeout` → `PoolError::Timeout` (fix direction 1; the knob is REUSED, not replaced, because the BOUNDED-recycle block below it already treats `checkout_timeout` as a per-step bound — so it was never a total budget). Both backends' `Cancel::cancel()` bound their SIDE dial at a fixed 2 s `CANCEL_DIAL_BUDGET` — that one mattered more, since `ferrod` awaits `cancel()` INLINE on the deadline/cancel arms, so an unbounded cancel stalled the teardown the deadline started. Guard asserts the RELEASED PERMIT (a `max_size: 1` pool must serve a second checkout), not elapsed time, because a leaked permit passes a naive timing check and still costs a slot forever; mutation-proven. TCP keepalive (direction 3) stays open — a different hazard. |
| B6a | Outbound frame-size guard (`php/client`) | DONE | **Found by verifying B6's premise; it was not what the item said.** `Header::decode` enforced `MAX_FRAME_PAYLOAD`; `Codec::encodeFrame` did NOT — measured: `encode()` of `payloadLen = MAX+1` succeeds and the client's own `decode()` then rejects it. Rust's `Encoder<OutFrame>` has always guarded (`session/codec.rs`), so this was a cross-codec asymmetry. It mattered because `ferrod` classifies an oversize `payload_len` as `Classification::Fatal` and CLOSES the session (correctly — the framing is desynchronised; already proven by `session_rules::oversize_payload_len_is_fatal`), so an oversize bind was a session kill instead of a local refusal. Guard added at the mirror site, before any byte reaches the transport; tests assert the inclusive boundary and that the session stays usable after a refusal; mutation-proven. |
| B6b | Chunked `LARGE_OBJECT` bind | DEFERRED to M2 | Carrying ONE payload across multiple frames is a `/proto` design change — registry + golden vectors + both codecs in one change set (charter rule 2) — plus an engine-side reassembly path with its own memory bound. That is a slice, not a Phase B loose end. B6a makes the current ceiling honest (a clear local error naming the limit) rather than a dropped connection, which is what B6b would replace. |
| B7 | Tracker-clean hygiene `None`-skip (R2) | DEFERRED (condition restated from the spec) | **This row's earlier note was imprecise and is corrected.** It read "hygiene currently masks the isolation leak (S8a Task 13)" — but that is a TEST-METHODOLOGY fact from §22.2 (s) (a next-tenant read cannot be a leak guard, because hygiene wipes the level first), not the gate on R2. **The project's own stated condition is SPEC §7.2:** "Enabling the `None`-skip waits until the tracker's in-proc coverage across all reuse-relevant session variables is proven." That coverage is not proven, and proving it is a real slice (enumerate the reuse-relevant variables per engine, then show the OK-packet tracker fires for each). Two supporting reasons, neither sufficient alone: §22.2 (s) records the engine-composed batched `SET TRANSACTION …; BEGIN` as ONE of three concrete holes such a skip must reconcile (it MASKS the intermediate statement's trackers); and R2 is a §16 statement-cache optimization with no recorded bench number motivating it, which charter rule 5's "optimize only against recorded bench numbers, never speculatively" clause covers. NOTE the first half of charter rule 5 ("correctness over throughput **until the M0 gate**") is time-bounded and M0 has passed, so it is the second clause that applies here, not the first. |

### Phase C — M2 (SPEC §17): the Eloquent milestone

| # | Item | State | Notes |
|---|------|-------|-------|
| C1b | `ferro/laravel` skeleton + resolver + `select()` | DONE | The package exists and executes. `FerroConnections::register()` wires `ferro-pgsql` through `Illuminate\Database\Connection::resolverFor()`; `FerroPostgresConnection extends PostgresConnection` and overrides `select()` ONLY, routed through `run()` so Illuminate's query log, `QueryExecuted` event and `QueryException` wrapping are all inherited. **`select()` is fate-declared a WRITE** — verified in `illuminate/database` v11.51.0 that `PostgresProcessor::processInsertGetId` runs `insert … returning id` through `selectFromWriteConnection()` = `select($q, $b, false)`, so "select() is a read" would have mis-declared the commonest Eloquent write on PG. `$useReadPdo` is recorded as a possible refinement and NOT taken: it is an application hint, not a guarantee (`DB::select('INSERT …')` passes `true`). **The contact gate was mutation-proven and FAILED the first mutation** — see the found-bugs section. 10 live tests / 53 assertions against real `ferrod` + PG 17, PHPStan L9 clean, CI lanes added (without them the package is a silent no-op). |
| C1c | Writes + transactions + the `attempts:` retry contract | DONE | `statement()`/`affectingStatement()`/`unprepared()` over `fetch:none` (`affected` from the engine, never `count($rows)`); transactions over a minimal `FerroPdoShim`. **`FerroQueryException` puts SQLSTATE in `getCode()` — PDO's convention, the OPPOSITE of the sibling Doctrine tier's errno convention — and that is mutation-proven live**: swap it to the sibling's convention and a real PG `40001` propagates OUT of `transaction(attempts: 3)` instead of retrying, with the client having correctly classified it `RetryableException` the whole time. **Design change worth carrying: the PDO shim moved EARLIER than §15 implies.** `ManagesTransactions` is written entirely against `getPdo()`, so five shimmed methods inherit the framework's counter, savepoint naming, events and retry loop unchanged instead of copying the trait. Possible because `getPdo()` has no return type. 20 live tests / 142 assertions, PHPStan L9 clean. |
| C1d | `cursor()` / `LazyCollection` | DONE | Small as scoped — `streamRaw()` has streamed on both families since B2, so this is tier wiring. `cursor()` is itself a Generator (as stock Illuminate's is), keeping the body lazy; a `finally` drives `RawStream::close()` for the `CANCEL`+drain on abandonment, covering both `break` and a dropped Generator. Buffered and streamed rows share ONE `hydrateOne()` so they cannot drift. **The abandonment guard asserts the NEXT query** — a missing cancel damages the following request, not the abandoned one. Mutation-proven, and the failure is worse than wrong data: without the `finally` the session HANGS behind ~50 000 unread frames, which is the shape of bug that reads as CI flakiness and gets re-run instead of fixed. Non-buffering is measured (200 000 rows under a 16 MiB bound), not asserted. 27 live tests / 204 assertions, PHPStan L9 clean. |
| C1e-1 | `FerroPdoShim::getAttribute(ATTR_SERVER_VERSION)` | DONE — and NON-SPECULATIVE by construction | C1e was deferred because nothing had demanded a PDO method and §15's list would have been guesswork. C2's gating verification then NAMED one: stock `PostgresGrammar::compileColumns()` version_compares `getServerVersion()` against 12.0 to pick its introspection SQL, and that is `getPdo()->getAttribute(PDO::ATTR_SERVER_VERSION)`. Backed by `poolInfo()->serverVersion`, on the wire since S8a. A missing version is LOUD, never defaulted — `version_compare(null, '12.0', '<')` is true, so a default would silently emit pre-12 SQL against a modern PG. Every other attribute still refuses BY NAME. **This is the build-the-measurement-first ordering paying off in one iteration.** |
| C1 | Eloquent tier (`ferro/laravel`) + PDO shim | IN PROGRESS — C1b + C1c + C1d + C1e-1 done; see `docs/dev-loop/PHASE-C-SCOPE.md` | §15; Illuminate `Connection` execution layer only, stock Grammar/Processor. Sliced C1a–C1e on the B2 model, each premise marked verified/UNVERIFIED. **Most C1 carries are already paid** (`fetchRaw`/`fetch:none`, `streamRaw` on BOTH families since B2, the imperative tx trio, `lastInsertId`, `server_version`) — the S8a-style carry slice is much smaller than S8b's was. **C1a was found AND decided while scoping:** `selectResultSets()` has no wire representation (`ExecOk` carries exactly one `cols`+`rows`) — but the wire is not its real blocker. A MySQL `CALL` returns no usable rows today (a prepared `CALL` declares ZERO result columns even when the procedure emits a result set; streamed → rows discarded, buffered → N cell-less rows, both verified in `ferro-backend-mysql/src/stream.rs`). Since a stored procedure is what `selectResultSets()` is FOR, a `/proto` multi-result-set change would ship a feature that still returns nothing. **Documented incompatibility for M2; the CALL blind spot is the true prerequisite and is engine work, not tier work.** |
| C2 | Illuminate integration suite green through a Ferro connection | **RUNNER LANDED + MEASURED TWICE** — 65/71 (`ferro-pgsql`) and **71/71 OK** (`pgsql` alias) after the C2b bind widening, each reproducible across two runs; `docs/laravel-suite/2026-09-10-c2-results.md` | The M2 acceptance bar — and the P10 go-to-market prerequisite. Suite runner modeled on `testkit/dbal-suite.sh` (with its hard contact assertion — the SQLite-fallback lesson). **§15's bar names MySQL + PG + SQLite, and the SQLite column is unreachable until C3 lands** — the same gap §14's bar had. Whether C3 should therefore precede C2 is an OPEN sequencing question, not settled. Report the columns that ran and name the one that could not; do not restate the bar as if met. **SCOPED against upstream SOURCE (laravel/framework v11.x + orchestra/testbench-core v9.0.0, both cloned and read):** C2 is materially HEAVIER than its DBAL sibling and is at least two slices. (1) Every test extends `Orchestra\Testbench\TestCase`, which boots a real `Application` + console `Kernel`, and `DatabaseMigrations` drives `artisan migrate:fresh` — so it needs testbench as a NEW dependency and a working Artisan, neither of which the DBAL suite needed. (2) **The silent-SQLite trap exists here too**, in `LoadConfiguration::bootstrap()`, and upstream's `phpunit.xml.dist` points `DB_CONNECTION` straight at it — but it keys on the connection NAME `testing`, and an unconfigured CUSTOM name throws loudly instead, so it is avoidable by construction: never reuse the name `testing`. (3) Testbench syncs env vars only for MYSQL/MARIADB/POSTGRES/MSSQL, so a custom driver's config must be supplied directly. (4) `migrate:fresh` depends on the stock schema builder's `dropAllTables()` — VERIFY that first next slice rather than assuming; the sibling's 50-test `int2vector` failure was exactly a stock-introspection gap. (5) 131 files / ~656 `test*` methods, one flat testsuite; upstream isolates by path + env, so a generated config can too. **Landed this iteration:** the `laravel_tests` database (testkit/postgres/init.sql) and the container-side reset (testkit/laravel/reset-pg.sql), both self-contained and independent of the runner's shape. **Container constraint recorded:** Docker's daemon is not running here (binary and compose v5.1.1 ARE present, so `command -v docker` misleads), so the runner is CI-only exactly like its sibling — and per this ledger's own note, a measurement lane's first real dispatch IS its test. **THIS ITERATION — the runner (`testkit/laravel-suite.sh`), the `laravel-suite` workflow, and the first recorded numbers.** Every premise above held except one worth naming: the runner is NOT CI-only after all — `FERRO_LARAVEL_SVC=psql` runs the identical reset SQL through a local `psql`, so a box with PostgreSQL but no Docker daemon (this container) can produce recordable numbers. **Two columns are recorded, not one**, because Illuminate resolves by driver NAME and six of upstream's own tests gate their assertions on `in_array($this->driver, ['pgsql','sqlsrv'])` — proven by running the identical code under the new opt-in `pgsql` alias and watching all six go green (SPEC §22.2 (am)). **The suite immediately found two tier defects its own tests were structurally blind to** (SPEC §22.2 (al)): the execution paths skipped Illuminate's `prepareBindings()`, so an Eloquent `Carbon` attribute reached the engine as a `TIMESTAMPTZ` and was refused against `$table->timestamps()`'s naive column (71 errors → 10); and the tier handed up the §9 value objects where Illuminate needs PDO scalars, which `pluck($col, $key)` breaks on outright (10 → 9). **All 9 remaining are triaged and none is a tier defect:** 6 are the driver-name artifact above, 3 are ONE engine gap — `I64` cannot bind to PG `float8` or `numeric`, the direct continuation of M1-S9's `I64 → text`/`bool` widening, written up with its value-gate design in `docs/followups/2026-09-10-pg-bind-i64-into-float8-and-numeric.md`. **C2b then shipped that widening** (SPEC §22.2 (an)) — `numeric` with no value gate, `float8` gated on exact representability, `float4` deliberately not widened — and the predicted +3/+3 landed exactly, leaving the curated subset CLEAN under the alias and the six name artifacts as the only non-passes under the default name. **Next: widen the allowlist.** The bar itself is still not met and the doc says so: MySQL is not run (the tier registers `ferro-pgsql` only), SQLite is unreachable until C3, and 71 tests is a curated subset of upstream's ~656 — "71/71" means the subset is clean, not that §15 is met. |
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

- **Batch scoping/doc work; push ONCE.** PR #21 was pushed to four times, each a docs-only
  amendment and each restarting a full 5-lane CI cycle (including the live integration lane against
  three real backends), plus three stale `check_suite.completed` wakes for superseded heads that had
  to be recognised rather than acted on. The cause was resolving premises incrementally against an
  open PR. Nothing in those later commits needed to be visible before the whole set was ready.
  **This does NOT apply to code slices**, where proving each push green before the next builds on it
  is the discipline that caught FB-5 (the B2a→B2c chain). It is specific to work that produces no
  usable artifact until it is complete.


- Deferred perf slices (§7.2 pipelined hygiene, the B4 two-channel split) — only against
  recorded bench numbers, per charter rule 5.
- Every 4th iteration: the adversarial/bug-hunt pass (protocol step 4).
- **RECOMMENDED TO THE OWNER, not yet scheduled — a D12 perf canary.** The §16 v1 targets (E8) have
  not been measured since M0, and E8 sits at the very END of the plan while charter rule 5 forbids
  optimizing before then. If M1's additions (streaming, the full type layer, the fate matrix) cost
  boundary latency, that is discovered at the most expensive possible moment. Re-running the
  existing D12 bench against current `main` is cheap and would turn that unknown into a number
  now. The loop has NOT taken this on its own initiative — it is out of the current phase order.
- Keep CLAUDE.md's "Current state" truthful when a slice changes it.

## Found bugs (open; found by the loop, not yet fixed)

All three are from the **iteration-12 adversarial pass** over the just-merged streaming path
(B2a fork + B2b-1 reclaim hook + the ferrod producer). All are **structural and DORMANT today** —
PostgreSQL's default `reclaim_stream` hook is synchronous and cannot hit any of them; they arm the
moment B2b-2 gives MySQL a conn-owning `RowStream`. Each is now a blocking acceptance criterion on
the B2b-2 row above.

- **FB-3 (HIGH, structural) — un-timed `handle.finish()` can strand the terminal END.**
  `engine/crates/ferrod/src/services/sql.rs:1024` (clean arm) and `:1083` (`abort_stream`) call
  `handle.finish().await` with NO cancel/deadline race and NO timeout — the one backend-touching
  await in the producer without one (`open`/`next`/`send_head`/`send_data` are all raced). B2b-1
  added a `reclaim_stream().await` inside `finish` (`ferro-pool/src/pool.rs:1085`); for a real
  MySQL backend that will do `COM_RESET_CONNECTION`-class I/O that can hang on a half-dead socket →
  `finish` never returns → the request never sends its single END frame (charter rule 4). The
  pre-existing drain loop inside `finish` was already unbounded, so B2b-1 widened an existing gap
  rather than creating it. **FIXED (iteration 13, B2b-1b)** at the pool level: the reclaim await is now bounded by
  `checkout_timeout` and a timeout is handled exactly like a reclaim failure, so `finish` always
  returns. Mutation-proven (removing the bound hangs the test). The MySQL-side half of the
  contract — that a dropped/timed-out reclaim leaves the conn `is_closed`-dead — remains B2b-2's
  (it is the FB-2 contract, and no fake can model a parked conn).
- **FB-7 (PROCESS/TEST-QUALITY, FOUND AND FIXED WITHIN C1b — a GATE that could not fail).** C1b's
  contact assertion was written as `select 1 as ferro_contact`, following the S8b lesson that a
  green suite must not be able to mean zero engine contact. Mutation-testing it showed the gate
  itself was **fakeable**: with `select()` stubbed to return a fixed row shaped like the probe's
  answer, the contact assertion PASSED and only two unrelated tests failed. A constant is exactly
  what a stub can guess — which is the same failure mode as the upstream DBAL `TestUtil` reporting
  `OK (105 tests)` against in-memory SQLite, one level up.
  **Fixed by making the probe unguessable and engine-identifying:** a per-call `random_bytes` nonce
  round-tripped through the query (a stub cannot know it, so its return is evidence something
  RECEIVED it) plus `version()` asserted to contain `PostgreSQL` (a nonce alone would be satisfied
  by any SQL database). Re-running the identical mutation against the strengthened gate now fails
  EVERY test at contact rather than two by accident.
  **The lesson generalises beyond this package:** "assert contact" is not a property, it is a
  property-shaped intention. A contact gate must be mutation-tested like any other guard, and a
  constant probe is not a gate.
- **FB-6 (MEDIUM, FOUND AND FIXED IN B6a — the third consecutive item whose stated premise did not
  survive checking).** `php/client` enforced `MAX_FRAME_PAYLOAD` on DECODE only. `Codec::encodeFrame`
  had no guard, so the client would put on the wire a frame **its own `Header::decode` would
  reject** — measured directly: `encode()` of `payloadLen = MAX_FRAME_PAYLOAD + 1` succeeds, and
  feeding those 16 bytes back to `decode()` throws `frame too large 16777217`. The Rust codec has
  guarded its ENCODER since M0 (`session/codec.rs`, `Encoder<OutFrame>`), so this was a cross-codec
  asymmetry of exactly the kind charter rule 2 exists to prevent — and the comment in `ferrod`'s own
  `session_rules::oversize_payload_len_is_fatal` even says the Rust encoder "would refuse to build
  such a frame", i.e. the missing mirror was documented in passing and never noticed.
  **Severity comes from the engine's (correct) response, not from the send:** an oversize
  `payload_len` is `Classification::Fatal`, so `ferrod` emits a `Protocol` terminal on `request_id 0`
  and CLOSES the connection — it has no choice, since the framing is desynchronised and a payload it
  refused to read cannot be skipped. So binding a `Ferro\Bytes` over 16 MiB (reachable through
  DBAL's `ParameterType::LARGE_OBJECT`) dropped the session instead of failing locally.
  **Two harmless second-order effects were found while testing and are recorded rather than hidden:**
  a refused request still consumes a `RequestIdAllocator` id (ids are monotonic and never reused, and
  since nothing was written the engine never saw it, so a gap reconciles with nothing), and
  `Session::$lastInFlight` is set before the write and so is left pointing at the refused call (it
  exists only to classify a LOSS per §19.3, no loss occurred, and the next `sendRequest` overwrites
  it before writing). Both are asserted in the test so a future reader does not re-discover them as
  suspected bugs.
- **FB-5 (HIGH, INTRODUCED AND FIXED IN B2c — recorded because the near-miss is the lesson).**
  Making `runPrepared()` stream on MySQL silently broke `lastInsertId()` for every streamed
  INSERT: `streamRaw()` CLEARS the connection-level key on the way in and, until this fix, never
  repopulated it — the terminal's key landed only in the per-stream `StreamTerminal` cell, while
  `Ferro\DBAL\Connection::lastInsertId()` reads it from the CONNECTION. Every MySQL INSERT through
  the driver threw `NoIdentityValue`, which is exactly what Doctrine ORM's `IdentityGenerator`
  calls. **Nothing offline caught it** — the whole driver + client suite was green locally; only
  CI's live `LastInsertIdLiveTest::testMysqlReportsTheGeneratedKey` failed. FIXED by propagating
  the ENGINE end to end — the first fix was necessary plumbing but insufficient, and CI said so a
  second time. **The real root cause was in `ferrod`:** `build_stream_terminal_body` has always had a
  `last_insert_id` slot, but `run_streamed_exec` hardcoded `None` into it, with a comment saying "a
  streaming backend that reports one wires it HERE" — correct while PostgreSQL (no such protocol
  field) was the only streaming backend, silently wrong the moment MySQL streamed. MySQL's own
  `stream::open` also discarded the key as `_lii`. So `PoolBackend::reclaim_stream` now returns a
  `Reclaimed { affected, last_insert_id }` instead of a bare `u64`, `StreamEnd` carries the key, the
  MySQL `NoRows` arm (which is the INSERT shape) keeps it, and the producer sends it. The offline
  guard now exists
  (`RawStreamTest::testAStreamedStatementsGeneratedKeyReachesTheConnection`, mutation-proven:
  removing the propagation fails it with the exact CI signature), plus an ENGINE-level live guard
  (`mysql_it.rs::mysql_streamed_insert_reports_its_generated_key`) asserting the key on the terminal
  itself. **Known asymmetry left
  deliberately:** the typed `stream()` generator still does not propagate, because it never
  decodes the Ok terminal body at all — widening it is a behavior change to a public read API,
  out of scope for a regression fix, and its own docs already say it reports no key.
- **FB-4 (LOW, coverage gap, opened by B2b-2b) — no end-to-end abandonment test for a MySQL stream.**
  An abandoned owning stream (client CANCEL / dropped handle) drops `MysqlRowStream::Rows`, which
  closes the moved-out `Conn` and leaves the wrapper PARKED — and parked reads `is_closed`-dead,
  so the pool discards it. Every link in that chain is tested (`conn_it.rs` parked-conn contract
  live on both engines; `query_stream.rs` for the pool's discard), but nothing drives the WHOLE
  path on MySQL the way `stream_it.rs::abandonment_recovery_after_cancel` does for PG. Add that
  test; until then criterion (c) is proven by construction plus unit coverage, not end to end.
- **FB-3b (MED, pre-existing since S5) — the DRAIN loop inside `finish()` is still unbounded.**
  `RowStreamHandle::finish` (`ferro-pool/src/pool.rs`) drains the remainder with
  `while self.next().await.is_some() {}` — no bound, and `ferrod` awaits `finish` unraced. FB-3's
  fix bounded the RECLAIM await beside it but deliberately left this alone: bounding it means
  dropping a mid-protocol `RowStream`, which changes PG behavior (a half-drained
  `tokio_postgres` stream) and must not be done blind. Lower severity than FB-3 was: PG's stream
  is channel-backed and driven by the connection task, so it fails rather than hanging in the
  cases seen so far. **Fix when a backend can actually hang a drain** — i.e. alongside B2b-2,
  where a live MySQL can prove the behavior either way.
- **FB-2 (MED, structural) — the "abandoned owning stream ⇒ conn discarded" invariant is
  documented but unenforced.** `RowStreamHandle`'s `Drop` net (`ferro-pool/src/pool.rs:1109`) sets
  only `self.checkout.tainted = true`; `Checkout::drop` (`:967`) recycles a conn whenever
  `!is_closed(&conn)` — **`tainted` alone does NOT prevent recycling**, only `is_closed()` does. So
  for a future conn-owning backend, safety rests entirely on its `is_closed()` correctly reporting
  dead when the inner conn was moved out; a naive impl (e.g. `inner.as_ref().map(is_broken).unwrap_or(false)`)
  would recycle a dead empty husk to the next tenant (cross-tenant leak, charter rule 6). No fake
  models a conn-owning `BackendRows`, so nothing in `ferro-pool` catches it. **Fix + live proof in
  B2b-2.**
- **FB-1 (LOW for Ferro) — the fork holds an owned conn open until stream-drop, not terminal-None.**
  `vendor/mysql-async/.../result_set_stream.rs` `done`-flag retention keeps the owned `Conn` alive
  until the `ResultSetStream` is dropped (or `into_conn`'d), vs. pre-fork closing it at the terminal
  `None`. Observable only for a caller that reads trailing accessors and delays before dropping.
  Non-issue for Ferro (the pool `into_conn`s immediately after the drain). **FIXED the doc overclaim
  this iteration** (the fork field doc + `UPSTREAM_PR_MYSQL_ASYNC.md` no longer assert "every
  pre-fork caller closed it anyway"); the behavior itself is correct and intended.

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
| 2026-09-10 | 17-20 (routine→session) | #17–#20 merged (step 7) + B3, B5, B6a → **Phase B CLOSED**, Phase C SCOPED | Four items in four PRs, each merged green before the next started. **B3** — `/proto` `TxNotFound` (`0x300B`): "that tx is gone" and "your frame is malformed" stop sharing a code, so `rollBack()` can swallow the first without swallowing the client's own codec defects; the falsifier (a `Protocol` terminal on rollBack now THROWS) exists at all three tiers + live, both unit guards mutation-proven; the vector harness also had to learn an END-guard, since this is the first committed TX-RESPONSE vector. **B5** — the unbounded dial, which had a hotter second instance than the ticket described: `ferrod` awaits `cancel()` INLINE on the deadline arms, so an unreachable backend stalled the teardown the deadline started; both dials bounded, and the guard asserts the RELEASED PERMIT (a leaked one passes a naive timing check and still costs a slot forever). **B6a** — found by checking B6's premise: the client enforced `MAX_FRAME_PAYLOAD` on DECODE only, so it would emit a frame its own decoder rejects, and since `ferrod` (correctly) treats that as FATAL, an oversize bind KILLED THE SESSION. **B4 closed WONTFIX** — measured unreachable, and M1-S8a had rejected the exact proposed fix in writing. **The phase's lesson: the ledger is a good queue and a poor oracle** — four items' premises did not survive checking, each check taking minutes and changing what shipped. | #17, #18, #19, #20 | 777→779 Rust tests live vs PG 17 (0 failed) across the slices, fmt + clippy `--all-targets -D warnings` clean, `/proto` regeneration zero-diff, client 721→724 / 2078 offline + 2871 live, driver 194 / 439, PHPStan L9 clean on both packages. MySQL/MariaDB: CI's integration lane is the authority (no Docker in-container) and was green on every head. |
| 2026-09-10 | 16 (routine→session) | #16 merged (step 7) + B2c → **Phase B item B2 COMPLETE** | PR #16 merged after its integration lane proved MySQL streaming live. Then the driver half: both pool-kind gates deleted, so `query()` and `runPrepared()` stream on every family and D-S8b-2 is retired — the engine work from B2a/B2b-1/B2b-1b/B2b-2a/B2b-2b is now actually reachable by a Doctrine app. The load-bearing check was the `rowCount()` question this row had flagged: it resolved with NO code change (MySQL still answers 0 for a SELECT — now because the post-drain packet says 0, not because the driver buffered), and the docs now state which. The one live guard that asserted the old asymmetry had been WRITTEN to fail on this day, and did; repointed to assert parity with PG on the same counter. | (this PR, +fix) | driver **194 / 436**, client **715 / 2052** (the new guard), PHPStan L9 clean on BOTH packages, Rust untouched. **CI caught a HIGH regression this slice introduced** (FB-5: streamed `lastInsertId()` broke on MySQL — ORM-fatal) that every offline gate missed; fixed, and the offline guard that was missing now exists and is mutation-proven. |
| 2026-09-10 | 15 (routine→session) | #15 merged (step 7) + B2b-2b | PR #15 (parkable conn) merged green — its two parked-conn guards ran for real against MySQL 8.4 + MariaDB 11.8 in CI, which is what made building on them safe. Then SPEC §22.2 (n)'s ENGINE deferral CLOSED: MySQL/MariaDB stream rows incrementally. New `crate::stream` (owned `ResultSetStream` + the no-result-set dispatch that would otherwise eat the connection), `reclaim` via B2a's `into_conn` reading `affected` from the conn post-drain, capability flipped, and `mysql_it.rs`'s two refusal tests replaced by positive both-arm proofs whose real assertion is that the session and the transaction SURVIVE the park/reclaim round trip. Residuals recorded, not buried: FB-4 (no e2e abandonment test on MySQL — criterion (c) is unit-proven, not end-to-end), FB-3b untouched, and a documented `CALL`-discards-rows blind spot that predates this slice. | (this PR) | fmt, workspace clippy `-D warnings`, `cargo test --workspace` **775 passed / 0 failed** with live PG; the MySQL streaming proofs self-skip in-container — **CI's integration lane is the authority**, and it is the first run that exercises the new code at all. `/proto` + PHP untouched. |
| 2026-09-09 | 14 (routine→session) | #14 merged (step 7) + B2b-2a | PR #14 (the FB-3 bound) merged green. Then took B2b-2 and split it once more on risk: the `Option<Conn>` refactor is 42 mechanical sites that only CI can prove, so it lands WITHOUT the capability flip — a CI failure now unambiguously means the refactor. `MysqlConn` is parkable, and the parked state is SAFE on every method the pool can reach while parked, which is the FB-2 finding turned into code: parked ⇒ `is_closed` dead (the only signal that discards rather than recycles), `tx_status` ⇒ `Failed` (never a falsely-clean `Idle`), and `cancel_handle` reads a connect-time id so a mid-stream cancel stays obtainable — that last change verified against the fork (`inner.id` is written once, at handshake; `COM_RESET_CONNECTION` keeps the thread id) rather than assumed. New live two-engine guard asserts park ⇒ dead, unpark ⇒ same session id + marker intact. | (this PR) | fmt, workspace clippy (`--tests`) clean, `cargo test --workspace` **775 passed / 0 failed** with live PG; the 2 new MySQL guards self-skip in-container (loud skip) — **CI's integration lane is the authority for them**. `/proto` + PHP untouched. |
| 2026-09-09 | 13 (routine→session) | #13 merged (step 7) + B2b-1b (FB-3) | PR #13 (adversarial findings) merged green. Then fixed the pass's own HIGH finding BEFORE B2b-2 can arm it: `finish()`'s reclaim await is now bounded by `checkout_timeout` (the knob the checkout-time recycle already uses for its ROLLBACK/RESET cleanup — no new operator knob), and a timeout is handled exactly like a reclaim failure (`affected = 0`, Rule-A force-taint, conn discarded), so `finish` ALWAYS returns and `ferrod`'s unraced `finish().await` can never strand the single terminal END. MUTATION-PROVEN: with the bound removed the new `start_paused` test hangs indefinitely (killed at 90 s) instead of failing fast — the exact FB-3 failure mode, reproduced before the fix was trusted. Deliberately NOT widened: the pre-existing unbounded DRAIN beside it (bounding it means dropping a mid-protocol PG stream) — recorded as FB-3b for B2b-2. Also flipped the stale B2a/B2b-1 rows to DONE. | (this PR) | fmt, workspace clippy `-D warnings`, `cargo test --workspace` **773 passed / 0 failed** with live PG 16.13 (MySQL/MariaDB suites self-skip in-container; CI authoritative), plus the mutation check. `/proto` + PHP untouched. |
| 2026-09-09 | 12 (routine→session) | #12 merged (step 7) + adversarial pass (overdue since it. 6) | PR #12 (B2b-1) merged green under the directive. B2b-2 (next item) is a 42-site MySQL refactor that is CI-only-provable (no MySQL in-container), so — and because the every-4th adversarial pass was overdue — spent the iteration bug-hunting the just-merged streaming path BEFORE B2b-2 builds on it. Own-thread review of the ferrod producer + a thorough subagent hunt (fork state machine, reclaim hook, producer) surfaced 3 structural findings, ALL dormant (PG's default reclaim can't hit them), all verified against the code: FB-3 (HIGH, un-timed `finish()` → hung MySQL reclaim strands the END, charter rule 4), FB-2 (MED, abandoned owning-stream discard is documented-not-enforced), FB-1 (LOW, fork holds owned conn to stream-drop; doc overclaim FIXED this iteration). FB-2/FB-3 recorded as BLOCKING acceptance criteria on B2b-2. Refuted-as-fine: refusal-arm correctness, double-finish/END, StreamEnd.affected wiring, ordering. | (this PR) | Adversarial (no code behavior change): the only edits are the Found-bugs/B2b-2 ledger rows and the FB-1 doc correction (fork field doc + UPSTREAM_PR). fmt-clean; `vendor/mysql-async` `cargo check` clean; no Rust/PHP/proto behavior touched so no suite re-run needed. |
| 2026-09-09 | 11 (routine→session) | #11 merged (step 7) + B2b split → B2b-1 | PR #11 (B2a) merged under the standing directive after fresh verification (5/5 green incl. the two-engine stream_recovery gate). Then B2b: reading the seam showed the MySQL owned-conn stream needs an `Option<Conn>` refactor across 42 backend sites, MySQL-only + CI-only-provable — so split B2b-1/B2b-2 and shipped B2b-1, the backend-agnostic reclaim-hook: `PoolBackend::reclaim_stream` called by `finish` AFTER the drain, BEFORE `finalize_stream`'s synchronous `tx_status` read (the exact §22.2 (n) restructure), with a reclaim-Err → discard-the-husk contract proven on the fake (both arms, `max_size=1` id observation). PG/fake behavior byte-identical (default hook). B2b-2 is now a localized backend change on this seam. | (this PR) | fmt, clippy (pool+both backends, `-D`-clean), `ferro-pool` 74 + query_stream 7 (incl. new reclaim test) + `ferro-backend-pg` live-PG suites all green (0 failed); MySQL/`/proto`/PHP untouched |
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
