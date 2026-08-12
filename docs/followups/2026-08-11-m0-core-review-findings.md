# M0 core review (Fable, 2026-08-11) — findings and the adjusted path

> **STATUS 2026-08-12 — every ENGINEERING finding below is CLOSED by M1-S9a** (plan:
> `docs/superpowers/plans/2026-08-11-ferro-m1-s9a-core-hardening.md`; commits `625dba7`…`ba72c89`;
> spec: §5.2, §7, §7.1, §7.7, §9.2, §12, §18, §19.3, §20.3 and §22.2 (ai)–(an); delta ledger:
> `2026-08-11-s9a-spec-deltas.md`). Three residuals are recorded rather than closed and are named in
> the §22.2 entries: the streaming abort path's unbounded drain (ak), the hazard latch's cry-wolf on
> an in-transaction statement that errored without running (ai), and the accept loop's hot retry
> under `EMFILE` (al). **The ROADMAP findings below are NOT closed** — they are the M1-S9 exit
> gate's business (the bar renegotiation, the ORM suite, the kill-`ferrod` chaos harness, D12, TLS,
> the fork CVE-lag ledger, D7) and none of them was in S9a's scope.

The M1 slices each had an adversarial whole-branch review. **M0 never did.** This is that review,
run with a different model on the code every request crosses. Journals:
`.superpowers/sdd/2026-08-11-m0-core-review/`.

Base rate going in: S6's review found a §12 credential leak; S8a's found 24 issues; S8b's found 6
blockers and 15 majors **in a slice where all 14 tasks were green and every gate passed**. The
expectation was that M0 carries comparable density. It does.

## What is SOUND (measured, not assumed — this matters as much as the defects)

- **The wire codec and session state machine.** Exactly-one-END, fail-closed handshake, decode
  safety — all verified sound, one guard mutation-confirmed. Malformed/truncated/oversized frames
  are handled. This is the part most likely to have been rotten and it is not.
- **The pin authority and the Err-arm fail-safes.** No currently-exploitable cross-tenant leak.
- **PG hygiene.** The reset profiles close the session-state classes they claim to.

## BLOCKER — silent at-least-once on MySQL/MariaDB (CONFIRMED LIVE)

`fate.rs` classifies every in-transaction statement loss as `Retryable` on the premise that "the
transaction will never commit, so replay is safe". **On MySQL that premise is false.** An
implicit-commit statement (DDL, `LOCK TABLES`, `SET autocommit`, …) inside an explicit transaction
COMMITS everything before it and ends the transaction. So the `Retryable` verdict licenses a
framework to replay writes that already persisted: **at-most-once becomes at-least-once, silently.**

Measured on MySQL 8.4: `START TRANSACTION; INSERT 1; CREATE TABLE; INSERT 2;` then an abrupt
disconnect with no COMMIT — **both rows persisted.**

Concrete victim: **Doctrine Migrations on MySQL**, where transactional migrations are the default.
BEGIN → INSERT into the migration log → ALTER TABLE (implicit commit; the INSERT persists) → a later
statement times out → engine says `Retryable` → the framework replays → the log row double-applies.

**The engine already has the detection signal and drops it.** `apply_tx_status` (pool.rs:821) reads
`SERVER_STATUS_IN_TRANS` after every statement and `tx_open` flips false at the implicit commit —
but neither the tx actor nor the fate call site consults it, because `OpContext.in_tx` is a
CALL-SITE CONSTANT rather than the live pin state. That is the fix.

## MAJOR — a FALSE `Indeterminate`, and the unreproduced §19.3 lead RESOLVED

A statement on a connection that died BEFORE dispatch is reported `WriteUnconfirmed{Indeterminate}`.
Reproduced live on PG 17 **with the write provably unapplied**. Nothing was sent, so §19.3 reads
`Retryable`.

This is also the mechanism behind the sighting filed unreproduced at the end of M1-S8b: it is the
stream OPEN's error path, where `sent: true` is pre-built — **not** a pool checkout failure, which
cannot produce it. The lead is closed with a cause.

## MAJOR — the systemic "one wedged backend takes the pool down" class

Three unbounded `.await`s, same shape, each leaking a permit. Only S8a's version probe was ever
bounded, and it was bounded because that same review caught it.
1. **`connect()` at checkout** — `checkout_timeout` wraps ONLY the semaphore acquire. A backend that
   accepts TCP but never finishes the startup handshake hangs the checkout forever holding a permit.
   Once `max_size` tasks wedge this way the pool is at **zero usable capacity permanently, even
   after the backend recovers.** The module docs explicitly claim the timeout covers this. It does not.
2. **The reaper's `ping()`** — no timeout, holds an owned permit. One half-dead backend kills the
   reaper for the pool's lifetime (nothing is ever evicted again) and leaks the permit for good.
3. **The post-cancel drain** on the autocommit and tx-exec paths — unbounded, leaks a permit, and
   **sends no terminal**, which is charter rule 4 broken rather than merely a hang.

## MAJOR — availability: one local client can pin the host-wide daemon

No server-side idle/stuck-connection reaping, unbounded `accept()`, and the codec reserves up to
**16 MiB the instant a frame HEADER arrives**, holding it until the frame completes. `handshake_timeout`
guards only the first frame; `idle_in_tx`/`max_tx` guard only open transactions. There is no
`idle_timeout` and no `max_connections` knob. A client that sends a header declaring 16 MiB plus one
body byte, repeated across connections, exhausts the memory and fds of the ONE daemon every worker
on the host depends on.

## MAJOR — SIGTERM's "graceful drain" is not wired to anything

It stops `accept()` and nothing else — it never reaches live sessions, pools or tx actors. Every
restart is a 5-second-delayed hard kill, with in-flight transactions and open streams cut mid-wire.
§18 describes systemd socket activation on the assumption this works.

## MAJOR — a guard that cannot fail, on the safety property itself

`in_tx: true` for tx-scoped EXEC is **unobservable by the entire `ferrod` suite**, so the
in-transaction half of §19.3 — the branch deciding rollback-and-tombstone versus `Indeterminate` —
is effectively untested. The project's dominant defect class, landed on its defining property.

## MINOR but notable — a credential leak inside the credential-leak fix

`loggable_scheme` was added in S6 *because* `dsn.split("://")` leaked a schemeless DSN's password to
a WARN log. It leaks for `user:pass://…`, where the credentials precede the first `://` and there is
no real scheme. Second occurrence of the same class in the same code path.
Also: `Checkout::drop` does `idle.lock().unwrap()` — a poisoned mutex double-panics into a process
abort, taking every worker's connections with it.

## Roadmap findings (the strategic dimension)

- **The M1 exit bar should be renegotiated.** §17 still says "DBAL 4 suite green"; §14 already says
  something else. Replace "green" — which this project has proved can be meaningless — with the
  form S8c actually built: a baseline exact-match plus a triage table where every non-passing test
  is categorised, and the category "an engine gap this run measured and did not close" is EMPTY.
  That is checkable and strictly stronger.
- **§20.3's kill-`ferrod` fate-correctness harness has never been built.** It is the acceptance test
  for the property the whole design exists to protect, and it was in neither bar.
- **D12 is exiting M1 unadjudicated.** Every §16 performance number the product pitch rests on is
  unmeasured, and the one provisional measurement points the wrong way.
- **Upstream TLS does not exist and is in no bar.** A daemon holding every credential on the host,
  talking to databases that may not be on localhost.
- **The vendored forks' real exposure is CVE-lag**, not API drift — four unfiled `tokio-postgres`
  accessors plus a `mysql_async` fork, with the ledger already behind the fork.
- **§17's M2–M5 do not contain the alpha being steered toward.** The written milestones predate
  everything M1 learned.
- Unspecified and will surface at soak: restart thundering-herd, pool queue-depth bounds,
  noisy-neighbour policy. Also open: `TYPE_REGISTRY_HASH` now makes every type addition a hard
  cross-fleet handshake break, and the upgrade/rollback ordering that implies is unwritten.
- D7 (naming/trademark) was a "before M1" task; M1 is about to exit with "Ferro" still a placeholder.

## THE ADJUSTED PATH

**M1-S9a — core hardening (NEW, and it comes before the exit gate).** Nothing ships to a user with a
silent at-least-once bug in it. In order:
1. the MySQL implicit-commit blocker — make `OpContext.in_tx` the LIVE pin state, not a call-site
   constant, and add the guard that makes `in_tx` observable so the branch stops being untested;
2. the false `Indeterminate` on a pre-dispatch connection death;
3. the wedged-backend class — bound all three awaits, with the permit released on every path;
4. availability — server-side idle reaping, `max_connections`, and a bounded partial-frame reserve;
5. SIGTERM drain actually wired to sessions, pools and tx actors;
6. the `loggable_scheme` leak and the `unwrap()` abort.

**M1-S9 — the exit gate**, with the bar renegotiated as above, plus the ORM suite finally run and
SQLite formally removed from the bar rather than quietly dropped.

**Then alpha**, revised — my earlier four-item bar was wrong, it was missing three:
TLS · the §20.3 kill-`ferrod` harness · D12 adjudicated · the ORM suite run · a slow log and basic
counters · a multi-day soak with restarts and failovers · a security pass.

**Beta** remains out on operability: zero observability, zero packaging, no Laravel tier.
