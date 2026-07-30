# Ferro M1 · Slice S5 — Streaming DATA-channel producer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
>
> **v2** — folds the adversarial plan-verification (wf_3bebb1af, FIX_FIRST → 6 blockers + 5 majors, all confirmed against real code). The B4 single-conduit ordering was verified **SOUND** (the terminal rides a pre-reserved `control_tx` `OwnedPermit` sent only after `handle.await`, so it is FIFO after every DATA on the same conduit). The v1 hazards were re-opened elsewhere; v2 fixes them via one unifying design (below). See the **Verification fold** note at the end.

**Goal:** Implement the windowed streaming DATA-channel producer M0 deferred (D-S5-1). A `fetch:stream` EXEC now emits a `HEAD(cols)` frame then N `DATA(rows)` frames to the client under a per-request **credit window** (default 64 frames / 16 MiB), replenished by `WINDOW_UPDATE`, bounded by a per-session **cap** (default 16 MiB) the engine accounts + releases, with **exactly one terminal END emitted AFTER the last DATA frame** (FIFO, never overtaking). Constant engine memory (the backend pulls rows incrementally, not a full drain). Autocommit + tx-scoped EXEC both gain the streamed path; `fetch:rows` stays buffered. The PHP client gets a `stream()` read-loop that yields rows, sends `WINDOW_UPDATE`s, and cleanly cancels+drains on abandonment. This unblocks the Doctrine `iterate*()`-never-buffers contract (§14).

**Architecture (the unifying design — read before any task):** New `/proto` `service::STREAM` methods `HEAD`/`DATA`, plus un-rejecting `fetch:stream` on `ExecRequest` — a charter-rule-2 change (registry + golden vectors + both codecs in one task). The load-bearing decision that makes the concurrency correct:

- **One ordered conduit.** HEAD, every DATA, and the terminal all ride the **existing `control_tx`** (the writer's single `control_rx` select! loop drains them FIFO). B4 (terminal-never-overtakes-DATA) then holds *by construction*: `permit.send()` enqueues at send-time, DATA sends happen during the handler, the terminal `permit.send()` happens after `handle.await` → terminal is physically last. No second channel (the 2-channel priority-split is a throughput nicety **DEFERRED**, charter rule 5).
- **Pre-reserved permit + RAII cap guard = atomic reserve/handoff.** A streamed frame is enqueued by: (1) `debit_or_wait` the request's credit; (2) `reserve_or_wait` the session cap → returns a **`CapReserve` guard** holding `(Arc<SessionCap>, bytes)` whose `Drop` releases; (3) `control_tx.reserve_owned().await` → an `OwnedPermit`; (4) `permit.send(OutFrame::Stream{ payload, _cap: guard })` — **synchronous, infallible**. The writer writes the bytes and drops the frame → the guard's `Drop` releases the cap. Release is thus tied to the frame's lifetime: exactly-once on the send path, and *still* released if the frame is ever dropped un-sent (teardown) — no M6 monotonic wedge, no double-release, and no cancellable await between cap-reserve and hand-off.
- **The sink folds onto `Responder`.** `send_head`/`send_data` are methods on the existing per-request `Responder` (constructed with the request's `CreditCell` + the session's `SessionCap` + a `control_tx` clone). Non-streaming handlers simply never call them → the `HandlerFn` signature is **unchanged** → the ~8 scripted-handler tests compile untouched (truly additive; no new dispatch arg).
- **Cancel/timeout-aware waits.** `debit_or_wait`/`reserve_or_wait` `tokio::select!` their register-then-recheck await against the request's `cancel.cancelled()` and its `timeout_ms` deadline (mirroring S4's biased select). A producer parked on backpressure resumes on `WINDOW_UPDATE`, OR unwinds cleanly on cancel/timeout — it never hangs, and it always reaches the ONE terminal.
- **Two-phase pull-based backend stream.** `Checkout::query_stream` returns a `RowStreamHandle<'_>` borrowing `&mut conn`, with `cols()`, `async fn next()`, and a **mandatory `async fn finish()`** that runs the S1 RFQ read + Err-arm force-taint + S2 `apply_classify` on **every** exit (Ok/Err/cancel/disconnect), plus a `Drop` that force-taints if `finish` was skipped (safety net — a mid-protocol conn is always recycled, charter rule 6). No callback/one-call-hygiene shape (that is incompatible with backpressure and the borrow).

**Tech Stack:** Rust (edition 2024, tokio — `Notify`, mpsc `reserve_owned`/`OwnedPermit`, `futures::Stream`); `/proto` + `ferro-proto` hand-rolled positional codec + golden vectors; `ferro-pool`/`ferro-backend-pg` streaming trait; `ferrod` session producer; PHP client (dependency-free) streamed read + outbound CANCEL; live PG via `testkit`.

## Global Constraints (verbatim from SPEC §5.2 / the exec-design S5 / the mechanism map + the wf_70051761 hazard-list — every task assumes these)

- **The `wf_70051761` hazard-list IS the acceptance bar** (exec-design R4 — reuse verbatim as tests):
  - **B3 — credit backpressure must never lose a wakeup or hang, AND must be cancel/timeout-aware.** A producer that finds `try_debit` false and awaits a notify MUST use the register-then-recheck idiom: `let n = notify.notified(); if try_debit(bytes) { return Ok } else { select! { _ = n => recheck, _ = cancel.cancelled() => Err(Cancelled), _ = deadline => Err(Deadline) } }` — obtain the `Notified` future BEFORE the second `try_debit`; then race it against the request's cancel token and its `timeout_ms` deadline. `Notify::notify_one()` stores one permit if no waiter is parked, so the wakeup is race-free; the `select!` guarantees a parked producer ALSO observes `cancel_all()`'s per-request token (registry.rs) and the deadline. A wait that can only be woken by `WINDOW_UPDATE` (and not by cancel/timeout) is the B3 blocker — it hangs forever with a leaked Checkout and **no terminal END** when the client CANCELs/disconnects.
  - **B4 — the terminal END must never overtake a DATA frame.** HEAD, DATA, and the terminal END all ride the **SAME `control_tx`** (one ordered conduit); FIFO guarantees terminal-after-DATA. `permit.send()` enqueues at send-time: DATA sends happen during the handler, the supervisor's terminal `permit.send()` happens after `handle.await` (the handler future does not resolve until its last `send_data(...).await` has enqueued its frame). **Do NOT add a separate data channel** — a control-prioritized writer would let the terminal overtake buffered DATA (the v1 flaw; the 2-channel priority-split is DEFERRED, charter rule 5). Do NOT restructure the supervisor. (Confirm `control_tx` capacity ≥ 2 so the pre-reserved terminal permit never starves DATA; bump it if needed.)
  - **M5 — the streaming sink is additive, folded onto `Responder`.** `send_head`/`send_data` are methods on the existing per-request `Responder`, wired at `Responder` construction with the request's `CreditCell` + the session `SessionCap` + a `control_tx` clone. The `HandlerFn` signature does NOT change; non-streaming handlers never call these methods; the ~8 existing scripted-handler tests compile unchanged.
  - **M6 — `SessionCap` RELEASE is tied to the frame's `CapReserve` guard (never monotonic, never double).** `reserve_or_wait(bytes)` returns a `CapReserve` RAII guard holding `(Arc<SessionCap>, bytes)`; its `Drop` does `used -= bytes; notify_waiters()`. The guard is MOVED into `OutFrame::Stream`; the writer writes the frame and drops it → release. Reserve the **exact final encoded `payload_len`** (encode-then-account inside `send_*`), so `used` returns to baseline (0) after every stream — test it explicitly, including a HEAD-only-reserving stream (`used == 0` after the terminal). No path releases without a matching reserve (would floor `used` via `saturating_sub` and breach the cap); no path reserves without an eventual release (would wedge the baseline).
  - **M7 — `exec_us` excludes backpressure (committed measure).** Accumulate a `Duration` incremented ONLY around each backend-pull await — the initial `query_stream()` open plus every `RowStreamHandle::next().await` — and **never** around `send_head`/`send_data` (which include the credit/cap/permit waits). The strictly-sequential pull-then-send loop makes this exact; document that a future prefetch/pipelining would break the boundary and must revisit it.
  - **MINOR-12 — HEAD frame credit + cap accounting:** the HEAD frame debits credit AND reserves cap exactly like a DATA frame (uniform: every STREAM frame carries a `CapReserve` guard and debits the window). Test it (seed a 1-frame window → HEAD exhausts it → the first `send_data` blocks on `debit_or_wait` until a `WINDOW_UPDATE` arrives).
- **A single valid DATA frame must always be able to make progress (large-row rule).** `MAX_FRAME_PAYLOAD` = 16 MiB (`proto/methods.toml`). A DATA batch is sized to `≤ min(MAX_FRAME_PAYLOAD, current_window_bytes)` but always carries **≥ 1 row**. Two concrete, engine-checkable rules replace v1's unimplementable "reject bytes > max window": **(a)** a single row whose encoded size `> MAX_FRAME_PAYLOAD` → a per-request `Unsupported` terminal (mirror the buffered `build_terminal_body` cap in `sql.rs`), never a hang; **(b)** the credit-window **bytes floor is `MAX_FRAME_PAYLOAD`** — the default window is 64 frames / **16 MiB**, and the client MUST maintain a bytes-window `≥ MAX_FRAME_PAYLOAD` (the PHP client grants this initially and on replenish; documented protocol invariant), so any single valid frame (`≤ 16 MiB`) always fits without depending on lazy client replenish. **(User-confirmed as Option B — deliberately COUPLING `default_credit_bytes` to `MAX_FRAME_PAYLOAD`.) Task 1b makes the `/proto` default change (`methods.toml` 4→16 MiB + `registry.lock.json`/both-codecs regen) and amends SPEC §5.2 ("4 MiB"→"16 MiB"), the `config.rs` "do NOT couple" note (now deliberately coupled), and §22.** The buffered-path `fetch:rows` credit is unaffected (it never streams).
- **Exactly-one-END is inviolable (§5.2, charter rule 4).** Every streamed request still ends in exactly one terminal `Outcome` frame (Ok/Error/Cancelled), emitted after all DATA. A cancel/error/timeout mid-stream → stop producing, fire the out-of-band backend cancel, `handle.finish().await` (drain + RFQ + hygiene), then declare the ONE fated terminal (the S4 `classify_fate` rules still apply). The supervisor stays the sole terminal sender.
- **Charter rule 2 — `/proto` is the single source of truth.** S5 ADDS `service::STREAM` `HEAD`/`DATA` methods + framing + un-rejects `fetch:stream`. This updates `proto/methods.toml` + `registry.lock.json` (regen via the `gen-registry-lock` bin) + Rust `ferro-proto` consts + **hand-rolled** `StreamHead`/`StreamData` codec (NOT `msg!` — see Task 1) + PHP `Generated/Constants.php` + PHP message/codec + **new golden vectors** (`stream_head_*`, `stream_data_*`, via the `gen-vectors` bin) + explicit match arms in `golden_vectors.rs` + the PHP `streamVectors()` byte-match — ALL in Task 1 (one change set). Hand-written protocol constants are a defect.
- **Constant engine memory (§5.2 "never buffers unbounded results").** The backend pulls rows incrementally (`RowStreamHandle::next`), the producer emits frames as credit + cap allow; the engine never materializes the full result for a stream. `fetch:rows` STAYS buffered + `MAX_FRAME_PAYLOAD`-capped (unchanged); only `fetch:stream` gets the streamed path.
- **Charter rule 6 — the streamed path keeps the `is_bare_tx_control` guard** (a `fetch:stream` of a bare tx-control statement is still rejected `Unsupported`, exactly as `Checkout::query` does today) and the S1/S2/S3 pin/hygiene invariants (via `RowStreamHandle::finish` + the `Drop` force-taint net — a streamed conn NEVER returns to the pool clean-but-dirty).
- **The S4 fate rules apply to a streamed request too** — a mid-stream backend error/cancel/link-loss/timeout classifies via `classify_fate`: a streamed READ that dies mid-stream → `Retryable{ConnectionLost}` or `Cancelled`; an in-tx streamed request cancelled/timed-out → rollback + tombstone → `Retryable` (S4's in-tx rule). Reuse S4's machinery; do not fork it.
- **PHP client stays dependency-free** (the streamed read is pure PHP over the UDS stream; no ext required). `stream()` yields rows lazily, sends `WINDOW_UPDATE` as it consumes (granting a window `≥ MAX_FRAME_PAYLOAD`), and on abandonment (`foreach…break` / discarded Generator) sends an outbound CANCEL and drains to the terminal so the wire stays framed for the next request.
- **Charter gates** green; live tests skip without `FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro`.

## File Structure

```
proto/methods.toml           + [methods.stream] HEAD, DATA; fetch:stream un-reserved
proto/registry.lock.json     regen (bin: gen-registry-lock)
proto/vectors/               + stream_head_*.json, stream_data_*.json golden vectors (bin: gen-vectors)
proto/tools/gen-php.php       (unchanged mechanism) -> regenerates PHP Constants.php
engine/crates/ferro-proto/src/messages/sql.rs   + StreamHead{cols}, StreamData{rows} with the HAND-ROLLED positional codec
                                                   (Value/ColMeta encode/decode — NOT msg!, Value lacks Eq/Serialize/Deserialize)
engine/crates/ferro-proto/src/bin/gen-vectors.rs + stream_head/stream_data vector cases
engine/crates/ferro-proto/tests/golden_vectors.rs  + explicit STREAM HEAD/DATA match arms (the `_ => Outcome::decode` catch-all
                                                      would PANIC on a STREAM vector -> Task-1 gate RED without this)
php/client/src/Protocol/Generated/Constants.php   regen (STREAM svc + HEAD/DATA + FETCH_STREAM)
php/client/src/Protocol/*     + StreamHead/StreamData decode
php/client/tests/VectorConformanceTest.php        + streamVectors() provider + byte-match method (allow-list must include stream_*)
engine/crates/ferrod/src/session/
  flow.rs        + CreditCell (Credit + Notify; cancel/deadline-aware debit_or_wait, notify_one register-then-recheck);
                   SessionCap (used + Notify; reserve_or_wait -> CapReserve RAII guard, notify_waiters register-then-recheck)
  registry.rs    InFlight.credit -> Arc<CreditCell>; insert() hands the producer the CreditCell (+ existing cancel token);
                 replenish() -> cell.replenish (notifies)
  writer.rs      + OutFrame::Stream{ payload, _cap: CapReserve } on control_tx; write bytes then drop -> cap release.
                 AMEND the doc comment (lines ~3-7): single ordered conduit, 2-channel split DEFERRED (charter rule 5).
  mod.rs         WINDOW_UPDATE -> replenish -> notify (via the cell); thread the session SessionCap into handle_request_frame.
                 AMEND any doc comment describing a future 2nd data channel.
  responder.rs   + send_head(cols)/send_data(rows) on Responder (encode -> reject >MAX_FRAME_PAYLOAD -> debit_or_wait ->
                   reserve_or_wait(CapReserve) -> control_tx.reserve_owned -> permit.send(OutFrame::Stream)); construct
                   Responder with the request CreditCell + session SessionCap + control_tx clone. HandlerFn UNCHANGED.
engine/crates/ferro-pool/src/backend.rs + pool.rs   + PoolBackend::query_stream + Checkout::query_stream -> RowStreamHandle<'_>
                                                      (borrows &mut conn; cols/next/finish; is_bare_tx_control-guarded)
engine/crates/ferro-backend-pg/src/query.rs         + query_stream: incremental pull off the tokio-postgres RowStream; finish()
                                                      runs the RFQ read + Err-taint + apply_classify; Drop force-taints if unfinished
engine/crates/ferro-pool/src/fake.rs                FakeBackend::query_stream (scriptable rows + scriptable mid-stream error)
engine/crates/ferrod/src/services/sql.rs            handle_exec: fetch:stream -> the streamed producer (autocommit); exec_us pulls-only
engine/crates/ferrod/src/tx/{mod.rs,actor.rs}       tx-scoped fetch:stream -> the shared producer from the pinned conn
php/client/src/Client/Session.php                   + sendStreamRequest + outbound sendCancel(requestId); stream() lazy Generator
php/client/src/Client/*                             + stream(): fetch:stream, HEAD+DATA*, yield lazily, WINDOW_UPDATE, cancel+drain on abandon
engine/crates/ferrod/tests/stream_it.rs (new)       the hazard tests + a live large-result-under-small-window e2e + abandonment recovery
```

---

### Task 1: `/proto` STREAM channel — HEAD/DATA methods + hand-rolled codec + golden vectors + un-reject fetch:stream (charter rule 2)

**Files:** Modify `proto/methods.toml`, `proto/registry.lock.json` (regen), `engine/crates/ferro-proto/src/messages/sql.rs` (hand-rolled `StreamHead`/`StreamData` codec), `engine/crates/ferro-proto/src/bin/gen-vectors.rs`, `engine/crates/ferro-proto/tests/golden_vectors.rs`, `php/client/src/Protocol/Generated/Constants.php` (regen) + PHP message classes, `php/client/tests/VectorConformanceTest.php`; Create `proto/vectors/stream_head_*.json` + `stream_data_*.json`.

**Interfaces produced:** `service::STREAM` (exists=4) gains `method_stream::{HEAD, DATA}`; `StreamHead { cols: Vec<ColMeta> }` + `StreamData { rows: Vec<Vec<Value>> }` message types (Rust + PHP) with byte-locked golden vectors; `ExecRequest.fetch == FETCH_STREAM (2)` is a valid accepted value on the wire (the M0 rejection lifts in Task 4b).

- `methods.toml`: add `[methods.stream]` with `HEAD` + `DATA` method ids under `service::STREAM = 4`. Framing: `HEAD` carries the column metadata (`Vec<ColMeta>` — reuse the exact `ColMeta` shape `ExecOk` uses for the buffered path, so the client hydrator is shared); `DATA` carries a batch of rows (`Vec<Vec<Value>>` — the same `Value [tag,payload]` scalar codec as buffered rows). A DATA frame is one wire frame with the `STREAM` flag (`flags::STREAM = 0x01`, already defined) set; the terminal END frame is unchanged (`Outcome::Ok` with `{affected, stats}`, NO rows — the rows went out as DATA).
- **Codec placement (BLOCKER fix — must not use `msg!`):** `StreamData` carries `Vec<Vec<Value>>` and `Value` derives only `Debug/Clone/PartialEq` (no `Eq`/`Serialize`/`Deserialize`), while the `msg!` macro forces `#[derive(…, Eq, Serialize, Deserialize)]` and `messages.rs` explicitly states Value-carrying messages "cannot ride the `msg!`/rmp-serde path." Place `StreamHead` and `StreamData` in `messages/sql.rs` (alongside `ExecOk`) with the **hand-rolled positional codec** (`Value::encode`/`decode` + `ColMeta::encode`/`decode_at`), mirroring `ExecOk`'s own hand-rolled encode/decode. Do NOT add them to `messages.rs` via `msg!`.
- Regen `registry.lock.json` (`cargo run -p ferro-proto --bin gen-registry-lock`); the drift guard (`registry_sync.rs`) must pass. `ferro-proto` build.rs emits the new `method_stream::*` consts.
- **`gen-vectors` (BLOCKER fix — vectors must be reproducible):** add `stream_head` + `stream_data` cases to `engine/crates/ferro-proto/src/bin/gen-vectors.rs` (bin name `gen-vectors`, hyphen) so the committed vectors regenerate zero-diff.
- **`golden_vectors.rs` (BLOCKER fix — the catch-all panics):** `golden_vectors.rs` has a catch-all `_ => Outcome::decode(payload).unwrap()`; a STREAM (service=4) vector matches no arm, hits the fallback, and PANICS (a HEAD/DATA body is not an `[status, body]` Outcome) → the Task-1 gate `cargo test -p ferro-proto` goes RED. Add explicit `STREAM`/`HEAD` + `STREAM`/`DATA` match arms that decode the body, re-encode it, and assert byte-stable.
- Regen PHP `Constants.php` (`proto/tools/gen-php.php`); the PHP `RegistrySyncTest` must pass. Add PHP `StreamHead`/`StreamData` decode classes mirroring `ExecOk`'s decode (reuse the `ColMeta`/`Value` PHP decoders).
- **PHP byte-match (BLOCKER fix — the allow-list skips non-core):** `VectorConformanceTest.php` hard-codes an allow-list that skips non-core messages. Add a dedicated `streamVectors()` data provider + a byte-match method (pure-packer encode → assert equals the committed `stream_*` vector), and extend the allow-list so `stream_head_*`/`stream_data_*` get PHP byte-match coverage.
- **Golden vectors:** `stream_head_*.json` (a HEAD frame with N cols incl. a typed col) + `stream_data_*.json` (a DATA frame with a few rows incl. a Null and a typed value). Byte-match in BOTH codecs.
- **TDD:** Rust — `StreamHead`/`StreamData` round-trip (encode→decode) + the golden vectors byte-match (via the new explicit arms) + the registry drift guard green. PHP — `RegistrySyncTest` + the new `streamVectors()` byte-match green; `FETCH_STREAM` const present.
- **Gate:** `cargo test -p ferro-proto` (incl. golden_vectors, no panic) + `(cd php/client && composer test)` (conformance) + `cargo build --workspace`; the `/proto` regen is zero-diff-on-rerun; fmt/clippy.
- **Commit** `feat(m1-s5): /proto STREAM HEAD/DATA methods + hand-rolled codec + golden vectors + both codecs; un-reserve fetch:stream (charter rule 2)`.

---

### Task 1b: couple the default credit bytes-window to `MAX_FRAME_PAYLOAD` (user-confirmed Option B; large-row rule)

> Added after the plan-verification fold when the user chose Option B for the large-row hazard (blocker #3). A `/proto` default change (charter rule 2) + a spec amendment. Small and mechanical, but it is a registry change — regen + drift guards + both codecs, zero-diff on rerun.

**Files:** Modify `proto/methods.toml` (the `default_credit_bytes` preamble value), `proto/registry.lock.json` (regen via `gen-registry-lock`), `php/client/src/Protocol/Generated/Constants.php` (regen via `gen-php.php`), `engine/crates/ferrod/src/config.rs` (the "do NOT couple" doc comment + a validation assert), `ferro-spec-v0.2.md` (§5.2 default + §22 deviation line).

**Interfaces produced:** `ferro_proto::consts::DEFAULT_CREDIT_BYTES == 16777216` (16 MiB, `== MAX_FRAME_PAYLOAD`); PHP `Constants::DEFAULT_CREDIT_BYTES == 16777216`; `Config::credit_bytes` default therefore 16 MiB.

- `proto/methods.toml`: change the preamble `default_credit_bytes = 4194304  # 4 MiB …` → `= 16777216  # 16 MiB — deliberately == MAX_FRAME_PAYLOAD so any single valid DATA frame always fits the per-request credit window (M1-S5 large-row rule, user-confirmed Option B)`. Leave `default_credit_frames = 64` unchanged.
- Regen `registry.lock.json` (`cargo run -p ferro-proto --bin gen-registry-lock`) and PHP `Constants.php` (`php proto/tools/gen-php.php`). Both drift guards (`registry_sync.rs`, PHP `RegistrySyncTest`) must pass. Verify zero-diff on a second regen run.
- `config.rs`: the module doc + the line-12 comment currently say `credit_bytes` is DELIBERATELY NOT `MAX_FRAME_PAYLOAD` ("do NOT couple"). Rewrite that comment to state the deliberate COUPLING and why (the large-row progress guarantee — a single indivisible row up to `MAX_FRAME_PAYLOAD` must fit the initial per-request window because the client will not replenish before it sees the frame). Add a validation assert (where `Config` is validated/loaded) that `session_cap_bytes >= MAX_FRAME_PAYLOAD` AND `credit_bytes >= MAX_FRAME_PAYLOAD` (defensive — a mis-configuration below the frame ceiling would reintroduce the deadlock on the cap or the window). `session_cap_bytes` default (16 MiB) already satisfies it.
- `ferro-spec-v0.2.md`: amend §5.2's "default window 64 frames / 4 MiB" → "64 frames / 16 MiB" (with a parenthetical that the byte window is deliberately `== MAX_FRAME_PAYLOAD` for the large-row progress guarantee). Add a §22 deviation line recording the M1-S5 coupling decision (supersedes the earlier "do NOT couple" note).
- **TDD:** a `config` unit test — the default `Config` has `credit_bytes == ferro_proto::consts::MAX_FRAME_PAYLOAD`; the validation assert rejects a `Config` with `credit_bytes` or `session_cap_bytes` below `MAX_FRAME_PAYLOAD`. The registry drift guards (Rust + PHP) green.
- **Gate:** `cargo test -p ferro-proto -p ferrod` (drift + config) + `(cd php/client && composer test)` (RegistrySyncTest) + `cargo build --workspace`; zero-diff regen; fmt/clippy.
- **Commit** `feat(m1-s5): couple default_credit_bytes to MAX_FRAME_PAYLOAD (16 MiB, large-row rule, user Option B) + config assert + SPEC §5.2/§22`.

---

### Task 2: the credit-wakeup + session-cap primitives — cancel-aware waits + RAII cap release (B3, M6)

**Files:** Modify `engine/crates/ferrod/src/session/flow.rs`, `engine/crates/ferrod/src/session/registry.rs`, `engine/crates/ferrod/src/session/mod.rs`.

**Interfaces produced:**
- `CreditCell { credit: Mutex<Credit>, notify: Notify }` with `try_debit(bytes) -> bool`, `replenish(frames, bytes)` (mutates + `notify_one()`), and `async fn debit_or_wait(&self, bytes, cancel: &CancellationToken, deadline: Option<Instant>) -> Result<(), WaitAborted>` (register-then-recheck, cancel/deadline-aware).
- `SessionCap { used: Mutex<u64>, cap: u64, notify: Notify }` with `async fn reserve_or_wait(self: &Arc<Self>, bytes, cancel, deadline) -> Result<CapReserve, WaitAborted>`; `CapReserve { cap: Arc<SessionCap>, bytes }` whose `Drop` does `used -= bytes; notify_waiters()`.
- `enum WaitAborted { Cancelled, Deadline }`.
- `InFlight.credit: Arc<CreditCell>`.

- `flow.rs`: define `WaitAborted`. Wrap the existing `Credit` in `CreditCell` (`std::sync::Mutex<Credit>` — short critical sections, never held across await). `try_debit`/`replenish` lock the mutex; `replenish` calls `notify.notify_one()` after mutating (per-request → single waiter → `notify_one` is right). `debit_or_wait` — **B3 register-then-recheck + cancel/deadline-aware**:
  ```rust
  loop {
      if self.try_debit(bytes) { return Ok(()); }
      let n = self.notify.notified();            // register BEFORE the recheck
      if self.try_debit(bytes) { return Ok(()); }
      tokio::select! {
          biased;
          _ = cancel.cancelled() => return Err(WaitAborted::Cancelled),
          _ = sleep_until_opt(deadline) => return Err(WaitAborted::Deadline),
          _ = n => {}                            // replenished -> loop rechecks
      }
  }
  ```
  (`sleep_until_opt(None)` = `future::pending()`, so no deadline = never fires.)
- `flow.rs`: `SessionCap::reserve_or_wait` uses the **IDENTICAL register-then-recheck idiom** (create `notified()`, recheck `try_reserve`, then `select!` the notify vs cancel vs deadline) — NOT try-reserve-then-await (that reintroduces a lost wakeup on the cap). On success it returns a `CapReserve` guard. `release` is NOT a public method the producer calls — release happens **only** via `CapReserve::drop` (`used -= bytes; notify.notify_waiters()`). Use `notify_waiters()` (not `notify_one`) because the cap is per-session with potentially several concurrent streamed requests of heterogeneous demand — a release wakes ALL waiters, each rechecks `try_reserve`, the ones that now fit proceed (no unfairness/skip). The cap is per-CONNECTION/session (owned in `run_with_handler`), shared (`Arc`) with the producer(s) and travels (in `CapReserve`) into the writer.
- `registry.rs`: change `InFlight.credit: Credit` → `Arc<CreditCell>`; `insert(id, …)` seeds a fresh `CreditCell` from `Config::credit_frames`/`credit_bytes` exactly as the existing `Credit` is seeded today (now 64 frames / 16 MiB after Task 1b's default bump — so NO separate per-cell floor logic is needed; the default already equals `MAX_FRAME_PAYLOAD`) and returns a clone (alongside the existing `CancellationToken`) for the handler/producer; `replenish(id, frames, bytes)` calls `cell.replenish(…)` (which notifies). Keep `credit_snapshot` for tests (reads through the cell).
- `mod.rs`: the reader loop's `WINDOW_UPDATE` arm already calls `registry.replenish` — now that path notifies (via the cell). Confirm no other change there. (The `SessionCap` construction + threading into `Responder` lands in Task 4a.)
- **TDD (unit, deterministic — B3 + M6 are the crux):**
  - **B3 lost-wakeup:** a task `debit_or_wait`s an empty cell; a concurrent `replenish` (via `notify_one`) wakes it — AND the race variant: `replenish` fires BETWEEN the failed `try_debit` and the `select!` → the stored permit / recheck catches it, never hangs (`tokio::time::timeout` asserts no-hang).
  - **B3 cancel/timeout-aware:** a `debit_or_wait` on a cell that is NEVER replenished → `cancel.cancel()` unblocks it with `Err(Cancelled)` (bounded by `timeout`); a `deadline` in the past → `Err(Deadline)`. (This is the blocker the v1 plan missed — a wait that only `WINDOW_UPDATE` can wake.)
  - **M6 cap release via guard:** `reserve_or_wait` to the cap limit → the next `reserve_or_wait` blocks → **dropping** an earlier `CapReserve` unblocks it (`notify_waiters`); after all guards drop, `used == 0` (not monotonic). A guard dropped WITHOUT ever reaching a writer (simulate: reserve, then drop) still releases (no leak). A `timeout` asserts the blocked reserve resumes.
  - **M6 exact accounting:** reserve X, drop → `used` back to exactly the prior value (reserve==release symmetry; no `saturating_sub` floor-breach, no residue).
- **Gate:** `cargo test -p ferrod` (the flow tests + the existing WINDOW_UPDATE-routing tests green — `credit_snapshot` works through the cell); `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s5): CreditCell (cancel/deadline-aware debit_or_wait, B3) + SessionCap RAII CapReserve release (M6) wired into the registry`.

---

### Task 3: backend incremental row-pull — `query_stream` → `RowStreamHandle` (constant memory, mandatory finish)

**Files:** Modify `engine/crates/ferro-pool/src/backend.rs` (+ `pool.rs` `Checkout::query_stream`), `engine/crates/ferro-backend-pg/src/query.rs`, `engine/crates/ferro-pool/src/fake.rs`.

**Interfaces produced:** `Checkout::query_stream(&mut self, sql, params) -> Result<RowStreamHandle<'_>, PoolError>` — a **two-phase, pull-based, borrowed-`&mut conn`** handle (NOT a callback, NOT one-call-hygiene — those are incompatible with credit backpressure and the borrow):
```
RowStreamHandle<'a> {                       // borrows &'a mut Checkout/conn
    fn cols(&self) -> &[ColMeta];
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>>;   // pull ONE row (or None at end)
    async fn finish(self) -> Result<StreamEnd, PoolError>;               // MANDATORY: drain remainder if any,
                                                                         // RFQ read + Err-arm force-taint + apply_classify,
                                                                         // return { affected, stats }
}                                            // Drop: if finish() was not called, force-taint the conn (safety net)
```

- `ferro-backend-pg/query.rs`: `query_stream` `prepare`s + `query_raw`s (as `run` does) but returns `cols` (from the prepared statement) + a handle that lazily `try_next()`s the `RowStream`, mapping each row to `Vec<Value>` via `rowmap::extract_value` — NOT draining into a Vec. A mid-stream error maps via `error_map::map` (SQLSTATE-preserving) → `next()` yields `Err`. `finish()` reads `rows_affected` + the S1 RFQ `transaction_status` AFTER the RowStream is exhausted (the S1 **post-drain** rule), runs the unconditional Err-arm force-taint (S1) + `apply_classify` (S2), and returns `{ affected, stats }`. The `Drop` impl (sync) sets `conn.tainted = true` if `finish` was not called — a mid-protocol/partially-drained conn is ALWAYS recycled (charter rule 6), even on a panic/early-drop path (this is the fix for the v1 "drop the stream on cancel" cross-tenant leak: `finish()` is the normal path, `Drop` is the safety net).
- `ferro-pool/pool.rs`: `Checkout::query_stream` runs `is_bare_tx_control(sql)` FIRST (reject `Unsupported` — reuse the exec/query guard), then opens the backend handle. The `RowStreamHandle` borrows `&mut self` (the Checkout), so the caller cannot return the Checkout to the pool while the stream is live (the borrow enforces it); `finish()` (or the `Drop` net) is what leaves the Checkout correctly pinned/tainted before it returns.
- `ferro-pool/fake.rs`: `FakeBackend::query_stream` yields a scripted sequence of rows (settable), a scriptable mid-stream error, and records whether `finish` was called — so the producer tests are deterministic without live PG and can assert the taint-on-unfinished behaviour.
- **TDD:** unit (FakeBackend) — `query_stream` yields N rows incrementally (assert lazy: rows produced on `next()`, not all upfront); `Checkout::query_stream` rejects a bare tx-control statement (`Unsupported`); a mid-stream fake error → `next()` yields `Err` + `finish()` leaves the conn tainted; **an abandoned handle (dropped WITHOUT `finish`) leaves the Checkout `tainted == true`** (the recycle guard, `pool.rs` tx_open||tainted||profile, then fires — the cross-tenant-leak regression test). Live PG (skip without env) — `query_stream("SELECT generate_series(1, 1000)")` yields 1000 rows incrementally (no full Vec); after `finish()` the conn is correctly pinned/unpinned + returned clean.
- **Gate:** `cargo test -p ferro-pool -p ferro-backend-pg` (+ live); `cargo build --workspace`; fmt/clippy; the existing buffered `query`/`run` path untouched + green.
- **Commit** `feat(m1-s5): Checkout::query_stream -> RowStreamHandle (incremental pull, mandatory finish + Drop force-taint, is_bare_tx_control-guarded)`.

---

### Task 4a: the streaming sink on `Responder` — control_tx conduit + CapReserve guard (M5, B4, M6 hand-off, large-row rule)

**Files:** Modify `engine/crates/ferrod/src/session/{responder.rs,writer.rs,mod.rs}`.

**Interfaces consumed:** Task 1 (HEAD/DATA frames), Task 2 (`CreditCell`/`SessionCap`/`CapReserve`).
**Interfaces produced:** `Responder::send_head(&self, cols) -> Result<(), WaitAborted>` + `Responder::send_data(&self, rows) -> Result<(), WaitAborted>` (non-consuming — the Responder keeps its one-shot terminal role); a new `OutFrame::Stream { payload: Bytes, _cap: CapReserve }` writer item. **`HandlerFn` is UNCHANGED** (M5 — additive via `Responder`, not a new dispatch arg).

- `writer.rs`: add `OutFrame::Stream { payload, _cap: CapReserve }` to the `control_rx` item enum. The writer writes `payload` to the socket, then the `OutFrame` drops → `CapReserve::drop` releases the cap (M6 release point is the frame drop, not an explicit call). **Amend the doc comment** (lines ~3-7) that anticipated a "second, credit-limited data channel with control prioritized over data": the decision is the single ordered conduit (DATA + terminal share `control_tx` for FIFO/B4); the priority-split is DEFERRED (charter rule 5). (Also add a SPEC §22 deviation line — see the shared commit note.)
- `responder.rs`: construct `Responder` with the request's `Arc<CreditCell>` + the session `Arc<SessionCap>` + a `control_tx` clone (threaded from `handle_request_frame`, which gets the `SessionCap` from `run_with_handler` via `mod.rs`). `send_head`/`send_data` share one private `send_frame(kind, payload_parts)`:
  1. encode the frame payload (`StreamHead`/`StreamData` from Task 1) → `payload: Bytes`; compute `len = payload.len()`.
  2. if `len > MAX_FRAME_PAYLOAD` → return a per-request `Unsupported` signal (the caller in 4b turns this into the ONE terminal; mirrors the buffered `build_terminal_body` cap). This only triggers for a single oversized row.
  3. `self.credit.debit_or_wait(len, &self.cancel, self.deadline).await?` — B3 (bail clean; nothing reserved).
  4. `let cap = self.session_cap.reserve_or_wait(len, &self.cancel, self.deadline).await?` — M6 (RAII guard; bail clean).
  5. `let permit = self.control_tx.reserve_owned().await …` — channel backpressure (if the channel is closed → treat as link-loss; the `cap` guard drops → release).
  6. `permit.send(OutFrame::Stream { payload, _cap: cap })` — **synchronous, infallible**; the guard MOVES into the frame. No cancellable await sits between the cap reserve (4) and the hand-off (6): steps 5→6 do not drop the guard except on channel-close, where the drop correctly releases. (This is the M6 reservation-leak fix.)
  7. HEAD debits credit + reserves cap exactly like DATA (MINOR-12; uniform accounting).
- `mod.rs`: `run_with_handler` owns the per-connection `Arc<SessionCap>` (constructed once, cap = 16 MiB); `handle_request_frame` threads it + the request `CreditCell` (from `registry.insert`) + a `control_tx` clone into `Responder::new`. Non-streaming requests build the same `Responder` but never call `send_head`/`send_data` → zero behaviour change. Amend any `mod.rs` doc comment describing a future 2nd data channel.
- **B4 confirmation (do not regress):** DATA `permit.send()` calls happen during the handler; the supervisor's terminal `permit.send()` happens after `handle.await`; both on `control_tx` → FIFO → terminal last. Confirm `control_tx` capacity ≥ 2 (the pre-reserved terminal permit + ≥1 DATA in flight); bump if the current capacity is 1.
- **TDD (deterministic — the sink mechanics; no producer loop yet):**
  - `send_data` enqueues an `OutFrame::Stream` on `control_tx` carrying a `CapReserve`; the writer, on sending it, releases the cap (`used` back to baseline). Assert one frame out, cap released.
  - a `send_data` whose encoded payload > `MAX_FRAME_PAYLOAD` → the `Unsupported` signal (not a panic, not a hang).
  - `send_head` debits credit + reserves cap like a DATA frame (MINOR-12): seed a 1-frame window → `send_head` exhausts it → the next `send_data` blocks on `debit_or_wait` until a `replenish`.
  - the ~8 existing scripted-handler tests + the writer/supervisor tests compile + pass unchanged (M5 additive).
- **Gate:** `cargo test -p ferrod`; `cargo build --workspace`; fmt/clippy; **DoD doc-truth:** the writer.rs/mod.rs doc comments now describe the single conduit, and a SPEC §22 deviation line is added in this commit.
- **Commit** `feat(m1-s5): streaming sink on Responder — control_tx conduit + CapReserve release + large-row rule (M5/B4/M6 hand-off); amend writer/mod doc + SPEC §22`.

---

### Task 4b: the ferrod streaming producer (autocommit) — the pull→send loop under credit (B3, M7, fate)

**Files:** Modify `engine/crates/ferrod/src/services/sql.rs` (`handle_exec` streamed branch). Create the shared producer fn (used again by Task 5).

**Interfaces consumed:** Task 3 (`Checkout::query_stream` → `RowStreamHandle`), Task 4a (`Responder::send_head`/`send_data`).
**Interfaces produced:** `async fn run_streamed_exec(handle: RowStreamHandle<'_>, resp: &Responder, cancel, deadline, ctx: OpContext) -> Terminal` — the shared producer loop (autocommit + tx paths both call it).

- `sql.rs` `handle_exec`: replace the `FETCH_STREAM => end_error(unsupported)` arm with: checkout → `Checkout::query_stream(sql, params)` → the `RowStreamHandle`; `resp.send_head(handle.cols()).await`; then `run_streamed_exec(...)`.
- `run_streamed_exec` — the pull→send loop, biased-select over cancel/deadline (mirrors S4's `run_autocommit_exec`):
  - accumulate `exec_us` as a `Duration` incremented **only** around each pull await (M7 — the `query_stream` open time is added by the caller; each `handle.next().await` is timed and summed; `send_head`/`send_data` are NOT timed).
  - batch rows: pull rows via `handle.next().await` into a batch sized to `≤ min(MAX_FRAME_PAYLOAD, current window bytes)` but ≥ 1 row; on a full batch (or window boundary) `resp.send_data(batch).await?` (this awaits credit via `debit_or_wait` + the cap via `reserve_or_wait` — B3; the producer BLOCKS here on backpressure without hanging, and unwinds on cancel/timeout).
  - the loop's outer `select!` is `biased` over `cancel.cancelled()` / the `timeout_ms` deadline / the pull — so a cancel/timeout mid-stream is observed even while awaiting a row (a slow query) AND is threaded into `send_*` (a stall on backpressure). **`req.timeout_ms` is threaded in** (fixes the silent S4-timeout regression) — a streamed EXEC whose `timeout_ms` elapses mid-stream stops and declares the correctly-fated ONE terminal.
  - on stream end (`next()` → `None`): `handle.finish().await` (RFQ + hygiene + `{affected, stats}`), then the caller declares the ONE terminal `Outcome::Ok{affected, stats}` (NO rows) via the `Responder`. The handler future resolves ONLY after the last `send_data(...).await` returned → **B4** (terminal ordered after the last DATA).
  - on abort (cancel / deadline / a `send_*` `WaitAborted` / a `next()` `Err` / an oversized-row `Unsupported`): STOP producing, fire the out-of-band backend cancel (S4 style — for the pull), `handle.finish().await` (drain to RFQ + hygiene), then `classify_fate(err, OpContext{ readonly, sent: <any DATA already went out>, in_tx: false })` → the ONE terminal reflects the fate (a cancelled streamed autocommit read → `Cancelled`/`Retryable` per S4; NEVER `Indeterminate` for a read). Exactly-one-END holds — after whatever DATA already went out.
- **TDD (deterministic, FakeBackend — the hazards as tests, B3/B4/M6/M7 + timeout + MINOR-12):**
  - **B4 one-END never overtakes DATA:** a streamed EXEC producing 3 DATA frames → the wire sees HEAD, DATA×3, then exactly ONE terminal END, in that order (the terminal is last).
  - **B3 backpressure pause+resume, no lost-wakeup hang:** a 1-frame window → the producer sends 1 frame then BLOCKS; a `WINDOW_UPDATE` replenishes → it resumes + finishes (wrap in `tokio::time::timeout` to assert no hang); test the replenish-races-the-await variant.
  - **B3 cancel mid-backpressure:** a 1-frame window (never replenished) + a client CANCEL → the parked producer unwinds, `finish()` runs, exactly ONE terminal (`Cancelled`) is emitted, the conn is recycled — NO hang, NO leaked Checkout (the v1 blocker: a wait only `WINDOW_UPDATE` can wake).
  - **B3 timeout mid-stream:** a stream with `timeout_ms` that elapses mid-stream → stop + ONE fated terminal after the DATA already sent (fixes the S4-timeout silent regression).
  - **M6 session cap releases:** stream a multi-frame result → `SessionCap.used` returns to baseline (0) after the terminal (not monotonic); a 2nd stream on the same session works (no wedge). Plus the HEAD-only-reserving stream → `used == 0` after the terminal.
  - **M7 exec_us excludes backpressure:** a stream with an artificial credit stall → the reported `exec_us` ≈ the DB pull time, NOT the wall-clock incl. the block.
  - a mid-stream error → one terminal `Outcome::Error` with the right fate, after the DATA already sent.
- **Gate:** `cargo test -p ferrod` (the hazard tests + the existing session/scripted-handler tests green); `cargo build --workspace`; fmt/clippy; exactly-one-END invariant holds.
- **Commit** `feat(m1-s5): streaming producer (autocommit) — pull→send loop under credit, cancel/timeout-aware, exec_us pulls-only; B3/B4/M6/M7 + timeout + MINOR-12 as tests`.

---

### Task 5: tx-scoped EXEC streamed path

**Files:** Modify `engine/crates/ferrod/src/tx/{mod.rs,actor.rs}`, `engine/crates/ferrod/src/services/sql.rs` (the tx-scoped dispatch).

**Interfaces consumed:** Task 3 (`Checkout::query_stream`), Task 4a (`Responder::send_head`/`send_data`), Task 4b (`run_streamed_exec`).

- The tx actor owns the pinned `Checkout` for the tx's life. A `fetch:stream` EXEC inside a tx must stream from that pinned conn. Thread the streamed path through `TxCommand::Exec` → the actor: the actor `co.query_stream(...)`s the pinned conn (the `RowStreamHandle` borrows the actor's `&mut co` — the actor drives the pull→send loop inline, so the borrow lives only for the streamed command), calls the **shared `run_streamed_exec`** (do NOT duplicate the credit/cap/ordering/fate logic), then replies the terminal.
- The actor's existing timeout/cancel machinery (S4) applies: a cancel/timeout mid-stream inside a tx → `run_streamed_exec` aborts → the actor rolls back + tombstones → the ONE terminal is `classify_fate(..., in_tx: true)` = `Retryable` (S4's in-tx rule), after whatever DATA already went out. Exactly-one-END holds. The `RowStreamHandle::finish` (or the `Drop` net) runs on the pinned conn — but inside a tx the conn is NOT returned to the pool (it stays pinned to the tx until COMMIT/ROLLBACK), so `finish`'s taint just marks the conn; the actor's rollback path is what tombstones. Confirm the streamed path does NOT break the S4 cancel/abort teardown or the `cancel_and_abort_contend` collision invariant (the shared cancel token still tears down exactly once).
- **TDD (FakeBackend):** a tx-scoped `fetch:stream` streams HEAD+DATA then one terminal; a cancel mid-stream inside a tx → rollback + tombstone → one `Retryable` terminal after the DATA sent; a `timeout_ms` mid-stream inside a tx → same; the S4 `tx_it` tests + the collision test stay green.
- **Gate:** `cargo test -p ferrod` (incl. the S4 tx tests + the new streamed-tx tests); `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s5): tx-scoped fetch:stream — stream from the pinned conn via the actor (shared run_streamed_exec; S4 cancel/timeout/rollback holds)`.

---

### Task 6: PHP client `stream()` — lazy read + WINDOW_UPDATE + cancel-and-drain on abandonment

**Files:** Modify `php/client/src/Client/Session.php` (+ `sendStreamRequest` + outbound `sendCancel`), the top-level connection class (+ `stream()`), `php/client/src/Protocol/*` (StreamHead/StreamData decode from Task 1); Tests `php/client/tests/`.

**Interfaces produced:** `stream(class-or-shape, sql, params): iterable` (a Generator yielding hydrated rows lazily), sending `fetch:FETCH_STREAM`, reading HEAD then DATA* then the terminal END, sending `WINDOW_UPDATE` to replenish (window `≥ MAX_FRAME_PAYLOAD`), and — on abandonment — an outbound CANCEL + drain so the wire stays framed. `Session::sendCancel(requestId)` (new outbound CANCEL sender — PHP currently only DECODES `FLAG_CANCEL`).

- The current `Session::sendRequest` is strictly one-frame-in/one-frame-out (unchanged for buffered). Add `sendStreamRequest` (a streamed mode) that: writes the EXEC with `fetch:FETCH_STREAM`, **granting an initial credit window `≥ MAX_FRAME_PAYLOAD`** (the large-row protocol invariant); reads the HEAD frame (decode cols → the hydrator); loops reading DATA frames (each a batch → yield each hydrated row lazily via a Generator), sending a `WINDOW_UPDATE {request_id, frames, bytes}` back as it consumes each frame (or every K frames), keeping the window `≥ MAX_FRAME_PAYLOAD`; until it reads the terminal END (→ the `Outcome`, mapped via the S4 `FateClassifier` — a mid-stream error terminal throws after the pre-error rows). Rows already yielded are NOT rewound.
- **Abandonment safety (BLOCKER fix — wire desync):** the common `foreach…break` / discarded-Generator pattern must not leave DATA/END frames buffered on the single-in-flight `Session` (the next `sendRequest` would read stale frames → a genuine wire desync). The `stream()` Generator wraps its read loop in `try { … } finally { … }`: if the terminal was NOT reached when the Generator is destroyed (break / GC), the `finally` calls `Session::sendCancel(requestId)` then DRAINS remaining DATA/END frames until the terminal — leaving the socket cleanly framed. The server-side cancel-aware `debit_or_wait` (Task 4b) observes the CANCEL and unwinds a parked producer, so the CANCEL both frees the server and lets the client drain to the ONE terminal. (Belt-and-suspenders: `Session` tracks "a stream is open"; a `sendRequest` while a stream is open throws a documented `ProtocolException` rather than mis-reading frames.)
- **Never buffers:** the Generator yields each row as it arrives; the client holds at most a window's worth. This is the Doctrine `iterate*()`-never-buffers contract.
- **TDD (PHPUnit, against the FakeTransport/a scripted frame sequence):**
  - a scripted HEAD + DATA×3 + terminal-END → `stream()` yields the 3 batches' rows lazily (the Generator pulls frames on demand) + sends WINDOW_UPDATE frames; the terminal is consumed exactly once (one-END).
  - a mid-stream error terminal → the Generator throws the mapped exception after the pre-error rows.
  - **abandonment:** `foreach ($client->stream(...) as $row) { break; }` after 1 of 3 batches → the Generator's `finally` sends CANCEL + drains → the NEXT `sendRequest` on the same Session reads its OWN reply cleanly (no stale frame, no hang). Assert the CANCEL frame was written and the next request round-trips.
  - the buffered `query`/`rows` path is unchanged.
- **Gate:** `(cd php/client && composer test)` (PHPUnit) + `phpstan analyse --level 9 src`; the golden-vector conformance (Task 1) green.
- **Commit** `feat(m1-s5): PHP client stream() — lazy Generator over HEAD/DATA + WINDOW_UPDATE + outbound CANCEL/drain on abandonment (never buffers, no wire desync)`.

---

### Task 7: live end-to-end + the acceptance gate

**Files:** Create `engine/crates/ferrod/tests/stream_it.rs` (live, skip without `FERRO_TEST_PG_URL`); optionally a PHP live streamed-read test (reuse the S7 live harness).

- **Live large-result-under-small-window (the exec-design gate):** a `fetch:stream` `SELECT generate_series(1, 50000)` (a genuinely multi-frame result) through `ferrod` against real PG, with a SMALL credit **frames** window (e.g. 2 frames — the bytes window stays `≥ MAX_FRAME_PAYLOAD` per the invariant) → the client (or the test harness acting as the client) reads HEAD + many DATA frames, sending `WINDOW_UPDATE` to replenish, and receives exactly ONE terminal END after the last DATA. Assert: all 50000 rows received in order; exactly-one-END; the terminal never overtook a DATA; the session cap returned to baseline; no hang (a `timeout` bounds the test).
- **Live abandonment recovery:** open a stream, read a few frames, send CANCEL + drain (or drop the client stream) → a subsequent request on the same session round-trips cleanly (the wire is re-framed; the conn is recycled, not leaked).
- **PHP live (if the harness allows):** `$client->stream(...)` over a large result streams lazily end-to-end (constant PHP memory — `memory_get_peak_usage` stays bounded, not proportional to the result size); a `foreach…break` recovers cleanly.
- **The hazard gate as an integration:** confirm the B3/B4/M6/M7 + timeout + MINOR-12 unit tests (Task 4b) + this live e2e together cover the exec-design gate ("one-END under streamed EXEC; credit backpressure pauses+resumes on WINDOW_UPDATE without a lost-wakeup hang AND unwinds on cancel/timeout; the session cap releases; exec_us excludes backpressure; a large multi-frame result streams under a small window; abandonment recovers").
- **Gate:** live `cargo test -p ferrod --test stream_it` (with `FERRO_TEST_PG_URL`) + the PHP live test; `cargo test --workspace` green offline (skips); idempotent. `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s5): live streaming e2e — 50k-row result under a small window, one-END + cap-releases + constant memory + abandonment recovery`.

---

## Self-Review (author against SPEC §5.2 + exec-design S5 + the mechanism map + the wf_70051761 hazard-list)

- **Spec coverage (exec-design S5 gate):** `/proto` HEAD/DATA + vectors + both codecs (T1); the credit-wakeup B3 + session-cap M6 (T2); the constant-memory backend pull + mandatory finish (T3); the sink on `Responder` with B4/M5/M6-handoff/large-row (T4a); the producer with B3/M7/fate/timeout (T4b); tx-scoped streaming (T5); the PHP `stream()` never-buffers + abandonment (T6); the live large-result-under-small-window + abandonment + the hazard gate (T7). Every wf_70051761 finding is a named test.
- **Exactly-one-END + the S4 fate rules** hold on the streamed path (a cancel/error/timeout mid-stream → one terminal after the DATA, classified via `classify_fate`; a read never becomes `Indeterminate`).
- **`/proto` charter-rule-2** is a first-class task (T1), complete: registry + hand-rolled codec (not `msg!`) + `gen-vectors` cases + explicit `golden_vectors.rs` arms (no catch-all panic) + PHP `streamVectors()` byte-match + both decode classes.
- **Constant memory:** the backend pulls incrementally (T3); the producer is credit+cap-bounded (T2/T4); the PHP client yields lazily (T6). No path materializes a full large result.
- **The v1 blockers are closed** (verification wf_3bebb1af): B3 hang → cancel/deadline-aware `debit_or_wait`/`reserve_or_wait` + threaded `timeout_ms` (T2/T4b); cross-tenant leak → `RowStreamHandle::finish` + `Drop` force-taint (T3); large-row hang → reject `>MAX_FRAME_PAYLOAD` + window bytes floor (T4a/global); M6 reservation leak → `CapReserve` RAII guard, reserve/hand-off atomic (T2/T4a); PHP abandonment desync → outbound CANCEL + drain-in-`finally` (T6); Task-1 uncompilable `msg!` + vector panic → hand-rolled codec + explicit arms (T1). The B4 single-conduit ordering was verified sound and is preserved (T4a).

## Verification fold (wf_3bebb1af — FIX_FIRST, all 6 blockers + 5 majors + nice-to-haves)

| # | Sev | v1 defect | v2 fix (task) |
|---|-----|-----------|----------------|
| 1 | blocker | `debit_or_wait`/`reserve_or_wait` not cancel/timeout-aware → parked producer never sees `cancel_all`/`timeout_ms` → hang, no END, leaked Checkout | cancel/deadline `select!` in both waits; `req.timeout_ms` threaded into the loop (T2, T4b) |
| 2 | blocker | "drop the stream on cancel" drops a partial RowStream, skips post-drain hygiene → un-tainted conn → cross-tenant leak; callback/one-call `query_stream` unbuildable | two-phase pull `RowStreamHandle` + mandatory `finish()` (RFQ+taint+classify on every exit) + `Drop` force-taint net; abort = out-of-band cancel + drain + finish (T3, T4b, T5) |
| 3 | blocker | single valid DATA frame up to 16 MiB > 4 MiB window → `try_debit` false forever → hang; "reject > max window" unimplementable | reject single row `>MAX_FRAME_PAYLOAD` (Unsupported); window bytes floor = `MAX_FRAME_PAYLOAD`, default 16 MiB, client maintains it (T4a, global, T6) |
| 4 | blocker | M6 release only on writer-send; the cancel fix opens a reserve→enqueue drop window → monotonic cap wedge | `CapReserve` RAII guard released on frame `Drop`; reserve→hand-off has no cancellable await (pre-reserved permit + synchronous `permit.send`) (T2, T4a) |
| 5 | blocker | PHP `stream()` ignores generator abandonment → buffered frames desync the next request; no outbound CANCEL to unblock a parked server | Generator `finally` sends outbound CANCEL + drains to terminal; `Session::sendCancel` added; open-stream guard throws on misuse (T6) |
| 6 | blocker | `StreamData` via `msg!` won't compile (`Value` lacks `Eq/Serialize/Deserialize`); STREAM golden vectors panic `golden_vectors.rs` catch-all → Task-1 gate RED | hand-rolled codec in `messages/sql.rs`; `gen-vectors` cases; explicit `golden_vectors.rs` arms; PHP `streamVectors()` byte-match (T1) |
| 7 | major | handler-seam "compiles unchanged" false for a 3-arg `HandlerFn` | fold `send_head`/`send_data` onto `Responder`; `HandlerFn` unchanged; ~8 tests untouched (T4a) |
| 8 | major | reserve==release symmetry + HEAD-cap treatment unspecified → cap breach or baseline wedge | reserve exact encoded `payload_len`; guard releases that exact amount; HEAD reserves+debits like DATA; HEAD-only test (T2, T4a, T4b) |
| 9 | major | `exec_us` measure not committed (two non-equivalent strategies) | committed: sum the pull awaits (`query_stream` open + each `next().await`), never the sends (T4b) |
| 10 | major | `timeout_ms` silently dropped on the streamed path (S4-gate regression) | `timeout_ms` threaded into the biased select; mid-stream-timeout test (T4b, T5) |
| 11 | major | MINOR-12 (HEAD debits credit) has no test | HEAD-exhausts-a-1-frame-window test (T4a/T4b) |
| N1 | minor | cap `notify_one` heterogeneous-demand unfairness; `reserve_or_wait` idiom unstated | `notify_waiters()` on cap release; identical register-then-recheck idiom (T2) |
| N2 | minor | bin names `gen_registry_lock`/underscore wrong | hyphenated `gen-registry-lock` / `gen-vectors` (T1) |
| N3 | minor | single-conduit contradicts writer.rs/mod.rs doc comments; DoD doc-truth | amend the doc comments + add a SPEC §22 deviation line (T4a) |
| N4 | minor | T4 bundled the workspace seam change with the concurrency-critical loop (not bisectable) | split into T4a (sink plumbing, additive) + T4b (producer loop + hazard tests) |

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates). **T2 (credit/cap concurrency), T4a (the sink + CapReserve + B4 ordering), T4b (the producer loop + cancel/timeout), T5 (tx streaming) are concurrency-critical — review on a capable model**, probing the B3 lost-wakeup + cancel-awareness, the M6 guard release (no wedge, no double), the B4 terminal-after-DATA, exactly-one-END on every abort path, and the cross-tenant-leak regression (abandoned stream → tainted+recycled). T1 (/proto) review checks the rule-2 completeness (hand-rolled codec, both codecs, vectors, no catch-all panic). T6 review checks the abandonment drain (no wire desync). Whole-branch review before S6. Live tests against the testkit Docker PG.
