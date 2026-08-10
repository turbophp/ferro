# Ferro M1-S8b — The `ferro/doctrine-dbal-driver` Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ship `php/doctrine-dbal` (`ferro/doctrine-dbal-driver`) — a Doctrine DBAL **4** driver whose EXECUTION layer talks to `ferrod` through the existing `ferro/client`, so an existing Doctrine/Symfony application switches by **configuration only**: `driverClass` + `driverOptions`, with Grammar/Processor, the DBAL platforms and the stock schema managers untouched (charter rule 6). Everything the driver needs that the client does not yet expose is added to `ferro/client` additively; the one engine change is the PG canonical-TEXT bind pre-flight, which is the difference between "drop-in" and "every dated or decimal INSERT fails on PostgreSQL".

**Architecture:** Fourteen independently-green slices in three bands. **Band A (Tasks 1–4)** widens the seams the driver stands on — three additive `ferro/client` methods (a raw positional fetch with an EXPLICIT `readonly` fate flag, a raw positional STREAM, an isolation-carrying `begin`), one `SessionInterface` accessor, and one engine bind change — each proven on its own with no DBAL dependency anywhere. **Band B (Tasks 5–13)** builds the package itself, starting from a walking-skeleton driver proven live through the REAL `Doctrine\DBAL\DriverManager` against both pools, then adding platform selection, binds, results, the type boundary, transactions, the exception converter, streaming and isolation — one committable deliverable each. **Band C (Task 14)** is acceptance: a `/testkit` runner for a curated upstream DBAL functional subset with a HARD contact assertion, the recorded numbers, and the SPEC §14 / §22.2 amendments the measurements force.

**Tech Stack:** PHP ≥ 8.2 for `php/doctrine-dbal` (depends on `doctrine/dbal ^4.0` and, via a composer **path** repository, on `ferro/client`; `ferro/client` itself gains **no** runtime dependency — charter rule 7); PHP ≥ 8.2 dependency-free for the `ferro/client` additions; Rust (edition 2024) for the one `ferro-backend-pg` bind change; PHPUnit 11 + PHPStan level 9 as the package gates, mirroring `php/client` exactly.

**Revision: v2.** Written against the code at HEAD `56ae1c2` (branch `m1-build`) and against four adversarial research probes that installed `doctrine/dbal 4.4.4` and `doctrine/orm 3` into a scratchpad, ran the stock DBAL functional suite against the live testkit containers, and built a throwaway DBAL driver over `Ferro\Client\Connection` that answered real queries through the real `DriverManager` on both a PG and a MySQL pool. Findings that changed the SHAPE of this plan, not its wording:

- **SPEC §14 was written against a DBAL-3-shaped SPI.** `ServerInfoAwareConnection` and `VersionAwarePlatformDriver` do not exist in 4.x; `getDatabasePlatform()` is handed a `ServerVersionProvider`, not the connection; `lastInsertId()` takes no sequence argument and must THROW; the driver `Result` has no iterate hook. Three §14 sentences are unimplementable as written and are amended by Task 14.
- **The DBAL 4 SPI carries NO read/write signal** (Task 1's whole rationale). Every result-producing client method hard-codes `readonly=true`, so a driver built on `Connection::query()` would classify a lost `INSERT … RETURNING id` as **Retryable** — "provably did not apply" — for a write whose fate is genuinely unknown. That is the exact safety inversion this project exists to prevent, and it is why Task 1 exists before anything else.
- **PostgreSQL refuses the stock DBAL type layer's binds** (Task 4). `DateTimeType`, `DateType`, `TimeType`, `DecimalType`, `JsonType` and `GuidType` all hand the driver a PHP **string**, and `bind::check_param`'s `Value::Text` arm accepts only `varchar/text/bpchar/name/unknown`. MySQL has no such pre-flight, so a plan developed against MySQL alone would ship a PG driver that fails on every dated or decimal insert.
- **DBAL's stock type layer is a silently-corrupting calendar parser** (Task 9). Measured on 4.4.4: `'2026-00-05'` → `2025-12-05`, `'0000-00-00 00:00:00'` → `-0001-11-30`, PG's legal `'24:00:00'` → `00:00:00`. All three with **no exception**. A green functional suite does not catch this class.
- **`setTransactionIsolation()` is a silent no-op under transaction-mode pooling** (Task 13), and the naive "did the next tenant inherit it" test cannot fail because hygiene masks it either way (SPEC §22.2 (s) already proved that).
- **The upstream DBAL suite cannot select a third-party `driverClass`** (Task 14). `TestUtil::getConnectionParams()` checks only `$params['driver']` and silently returns `['driver' => 'pdo_sqlite', 'memory' => true]` — measured. A "green suite" claim would be vacuously true against in-memory SQLite with zero Ferro contact.

### What changed in v2, and why

v1 was verified by five adversarial agents working against live code and live databases. They produced **9 BLOCKERs and 4 MAJORs**, nearly all MEASURED rather than reasoned; their journals are in `.superpowers/sdd/2026-08-10-ferro-m1-s8b-dbal-driver/verify/`. Most of the findings were wording or symbol errors, fixed in place. **Four changed the DESIGN, and an implementer should know which parts were rebuilt rather than tweaked:**

1. **Task 4's `PgText::to_sql` stays TYPE-AWARE.** v1 made it type-blind (`out.extend_from_slice(self.0.as_bytes())` ignoring `ty`), which — measured both directions — un-armed clause (3) of `s8a_every_arm_treats_a_domain_exactly_as_its_base`: `ltree` and `dom_of_ltree` would write identical bytes BY CONSTRUCTION, so the one guard S8a's review round added the `ltree` fixture to arm could no longer fail. (HEAD + the mutation = RED; v1's Task 4 + the same mutation = GREEN.) v2 branches on the resolved base: text-verbatim for the eight newly-widened targets, the existing delegated path for everything that already worked. Zero regression surface, `ltree` keeps its version byte, clause (3) stays armed — and `encode_format` branches the same way, with a new domain-format assertion so the branch condition itself is mutation-covered.
2. **Task 12's memory guard, `materialize()` and the abandonment path were rebuilt.** The headline guard sampled `memory_get_usage()` AFTER the loop, where the buffered run has already released everything — measured 552 B streamed vs 472 B buffered, BOTH GREEN, while PEAK differed ~12 500×. It now measures PEAK **and** samples mid-loop. `materialize()`'s `foreach` over an advanced `Generator` throws `Cannot rewind a generator that was already run` on its FIRST real use; it is now an explicit `valid()/current()/next()` drain. Two of the four named mutations could not fail — one was a provable no-op, the other tested `materialize()` while calling itself the CANCEL path — so the driver now holds a **`\WeakReference`** to the open `Result` and the `Result` frees itself on destruction, which makes abandonment genuinely CANCEL, makes the difference OBSERVABLE (`settledRowCount()`), and turns both mutations into real ones.
3. **Task 14's acceptance number is now reproducible.** With v1's no-op `initializeDatabase()` the identical command run twice gave 23 then 33 errors (restoring upstream's `TestUtil` returned it to 0/0 — causation proven); v1 also pointed the suite at the SHARED `ferro` database, which it would have silted up permanently for every other live suite in the repo. v2 adds a container-side reset before phpunit, gives PostgreSQL its own `doctrine_tests` database, makes "started from a fresh reset" part of the recorded environment manifest, and ships the three missing public `TestUtil` methods up front instead of "after the first fatal".
4. **The `readonly = false` decision keeps its cost stated in full.** `readonly` is read in TWO places in `fate.rs`, and the second is the **57014 override**, where `!in_tx && !readonly && sent` is INDETERMINATE — so a plain `SELECT` that trips a server-side `statement_timeout` is reported to a Doctrine app as "your write may or may not have landed". The binding decision to declare write for everything is NOT re-litigated, but §22.2 (ac) now states the 57014 half explicitly, the engine docblock and `CLAUDE.md` that assert "a streamed READ never becomes `Indeterminate`" are amended in the same change set, `docs/known-incompatibilities.md` lists it as a real drop-in behaviour difference, and Task 11 gains a live guard that pins both cells — which also gives `driverOptions.readonly` its first behavioural test.

---

## Global Constraints

Every task's requirements implicitly include this section. Each hazard below was verified against the code at HEAD `56ae1c2`, against `doctrine/dbal 4.4.4` in a scratchpad, or against the live containers, and carries its `file:line` or its measurement.

### Contract rules (non-negotiable, copied from `CLAUDE.md`)

- **Charter rule 2 — `/proto` is the single source of truth.** **This slice makes NO `/proto` change.** Everything the driver needs is already on the wire: `ErrorPayload.sqlstate` + `errno` (S8a Task 3), `BeginRequest.isolation` (S8a Task 8), `HelloAck.pools[].{name,kind,server_version}` (S8a Tasks 11/12), `ExecOk.last_insert_id` (S8a Task 2), `ExecRequest.timeout_ms` (M0). If any task finds itself wanting a new method id, flag/error code or type tag, **STOP and raise it** — that is a registry + golden-vectors + BOTH-codecs change set, not a driver detail. Two known candidates are explicitly **deferred**: a dedicated `TxNotFound` error code (which would disambiguate `Connection::rollBack()`'s `ERR_PROTOCOL` swallow) and a `stale: true` flag on `PoolInfo.server_version`.
- **Charter rule 3 — the engine never transparently retries** a user statement, **and neither does this driver.** The driver constructs its `Ferro\Client\Connection` with `RetryPolicy::none()` so the client's own autocommit read-retry cannot double up with DBAL's. `Ferro\DBAL\IndeterminateWriteException` must **NEVER** implement `Doctrine\DBAL\Exception\RetryableException`.
- **Charter rule 4 — every in-flight request terminates in exactly ONE `END` frame.** Every streamed `Result` this slice adds must reach exactly one terminal, including on `free()`, on abandonment, and on an exception mid-iteration.
- **Charter rule 6 — scope discipline.** The driver changes **EXECUTION**, never SQL GENERATION. Grammar/Processor, the DBAL platform classes and the stock schema managers stay STOCK — this slice never subclasses a platform, never rewrites a statement, never caches a result, and never infers read-vs-write from SQL text. Where the driver converts a VALUE at its own boundary (Task 9) that is the driver's own conversion step, explicitly blessed by `RawStringValuePolicy`'s docblock; where it refuses a statement (Task 13) that is a loud refusal, not a rewrite.
- **Charter rule 7 — `ferro/client` stays runtime-dependency-free.** `php/client/composer.json` keeps `"require": {"php": ">=8.2"}` and nothing else. The dependency direction is `php/doctrine-dbal` → `php/client`, never back. `doctrine/dbal` drags `psr/cache`, `psr/log` and `doctrine/deprecations` into the consuming app; that is fine for the driver package and forbidden for the client.
- **SPEC §19.3 — the directional bind rule.** A bind pre-flight may be **stricter** than the concrete impl it fronts; it must **NEVER** be looser, because a looser pre-flight lets the failure land in `to_sql_checked`, whose error carries no `DbError`, which `is_session_fatal` reads as a lost connection, which §19.3 turns into a **false `Indeterminate`** for a statement that never left the process (`engine/crates/ferro-backend-pg/src/bind.rs:186-215`). Task 4 moves `accepts` and the impl **in the same edit**.
- **No cross-tenant connection-state leak.** Nothing in this slice may leave state on a pooled connection the next tenant inherits. This is why Task 13 refuses `SET SESSION TRANSACTION ISOLATION LEVEL …` instead of "making it work".

### THE dominant defect class in this project — guards that cannot fail

The M1-S7 review found nine guards that were structurally incapable of failing; the S8a whole-branch review found 24 more issues, including guards that passed for the wrong reason and properties asserted from vantage points where they were unobservable. **This slice adds a tenth failure mode the earlier ones did not have: a test suite that runs green against the wrong database.** `TestUtil::getConnectionParams()` silently falls back to in-memory SQLite when `driver` is unset (measured — it returned `array('driver' => 'pdo_sqlite', 'memory' => true)` with `db_driverClass` set), `pdo_sqlite` is loaded on this box, and neither `--fail-on-skipped` nor `ci/assert-no-skips.sh` would notice: nothing skips, everything genuinely passes, on SQLite.

**Binding rules for this plan:**

1. **Every live PHP test must prove DATABASE CONTACT structurally, not by passing.** `LiveTestCase::waitUntilReady()` is that proof for the client tier (a full HELLO + a real `SELECT 1` against the real upstream before any test body runs) and every S8b live class inherits it. Any runner that drives an UPSTREAM suite must additionally assert driver identity **before** the first test — Task 14 does this.
2. **Prefer compile-forced, then derived, then behavioural.** In PHP there is no compile-forced `match`; the nearest equivalents are an `enum`-exhaustive `match` with no `default` arm (PHP throws `\UnhandledMatchError` on a new case) and a test that derives its expected set from the same enum. Use them.
3. **A negative built from a hand-made input is a tautology.** Asserting `errno === null` on an `ErrorPayload` the test itself constructed with `errno: null` cannot fail. Every negative in this plan is either driven from a real producer (a live PG/MySQL error, the real bind path) or asserts a MIRROR property across a table that contains both the positive and the negative case.
4. **Every guard a task adds must be proven by MUTATION**: revert the production change (or flip one line), re-run the guard, record that it goes RED, restore. A task step that adds a guard without a mutation step is incomplete. Each task below names its mutation explicitly.
5. **`expectException` names a LEAF class.** `Ferro\Client\Error\FerroException` is the ROOT of a FLAT tree — every client exception extends it directly — so `expectException(FerroException::class)` passes for anything, including a test's own setup DDL failing. Likewise `Doctrine\DBAL\Exception` is a bare marker INTERFACE implemented by argument errors as well as driver errors: never assert on it alone.

- **`ci/assert-no-skips.sh` is live and shared** by `.github/workflows/ci.yml` (the `integration` job) and `ci/local-gate.sh`. It is applied to the **Rust** log only; the PHP tier's equivalent is `--fail-on-skipped` scoped by PATH to `tests/Live`. A PHP live test that skips therefore FAILS CI, which is why every MySQL-needing live test requires MySQL provisioned in the `php` CI job (it is, since S8a).
- **`--fail-on-skipped` proves a test RAN, never that it touched a database.** See rule 1.

### Verified hazards — a naive implementation is WRONG

**The DBAL 4 SPI (all read from `doctrine/dbal 4.4.4`'s `src/`, not from memory)**

1. **`Doctrine\DBAL\Driver` has exactly 3 methods**: `connect(#[SensitiveParameter] array $params): Driver\Connection`, `getDatabasePlatform(Doctrine\DBAL\ServerVersionProvider $versionProvider): AbstractPlatform`, `getExceptionConverter(): Doctrine\DBAL\Driver\API\ExceptionConverter`. There is **no** `Doctrine\DBAL\Driver\ExceptionConverter` (non-`API`) in 4.x.
2. **`ServerInfoAwareConnection` and `VersionAwarePlatformDriver` DO NOT EXIST in 4.x.** `getServerVersion(): string` lives on `Doctrine\DBAL\ServerVersionProvider`, which `Driver\Connection` **extends**, and the return type is **non-nullable**. A `nil` `PoolInfo.server_version` cannot be represented — the driver can only resolve it or throw.
3. **`Driver\Connection` required methods:** `prepare(string $sql): Statement`, `query(string $sql): Result`, `quote(string $value): string` (**no `$type` arg in 4.x**), `exec(string $sql): int|string`, `lastInsertId(): int|string` (**no argument, must THROW when there is no identity value**), `beginTransaction(): void`, `commit(): void`, `rollBack(): void`, `getNativeConnection()` (untyped, `@return resource|object`), plus the inherited `getServerVersion(): string`.
4. **`Driver\Statement` has 2 methods:** `bindValue(int|string $param, mixed $value, Doctrine\DBAL\ParameterType $type): void` and `execute(): Result`. `execute()` takes no params array in 4.x. Stock implementations WIDEN `$type` with a default of `ParameterType::STRING`; ours may too.
5. **`Doctrine\DBAL\ParameterType` is a PURE (unbacked) enum** with exactly 7 cases: `NULL`, `INTEGER`, `STRING`, `LARGE_OBJECT`, `BOOLEAN`, `BINARY`, `ASCII`. No `->value`; map with `match`. A `match` over it with **no `default` arm** is the closest thing to a compile-forced guard PHP offers (a new case throws `\UnhandledMatchError`).
6. **`Driver\Result` requires 9 methods** — `fetchNumeric(): array|false`, `fetchAssociative(): array|false`, `fetchOne(): mixed`, `fetchAllNumeric(): array`, `fetchAllAssociative(): array`, `fetchFirstColumn(): array`, `rowCount(): int|string`, `columnCount(): int`, `free(): void` — **plus a docblock-only `@method string getColumnName(int $index)` which is effectively mandatory**: `Doctrine\DBAL\Result::getColumnName()` throws `LogicException` via `method_exists` when it is absent, `Connection::executeCacheQuery()` loops it to build the cache key, `AbstractResultMiddleware` forwards it with the same guard, and all 8 bundled driver Results implement it. Stock behaviour for a bad index is `throw Doctrine\DBAL\Exception\InvalidColumnIndex::new($index)`.
7. **`Doctrine\DBAL\Driver\FetchUtils`** (`@internal`, static helpers — NOT a trait) supplies canonical `fetchOne`/`fetchAllNumeric`/`fetchAllAssociative`/`fetchFirstColumn` built purely on `fetchNumeric()`/`fetchAssociative()`. Every stock driver Result delegates to it. **Implementing `fetchAll*` via `FetchUtils` is fine; implementing `fetchAssociative()` on top of a pre-buffered array is what would break §14's never-buffer requirement.**
8. **`Doctrine\DBAL\Result::iterateAssociative()` is literally `while (($row = $this->fetchAssociative()) !== false) yield $row;`** — the driver `Result` has **no iterate hook and no signal that the consumer is iterating**. §14's "streaming used automatically when the consumer iterates" therefore falls out of making `fetchAssociative()` pull incrementally, and nothing else.
9. **An exception that is not a `Doctrine\DBAL\Driver\Exception` escapes DBAL's conversion entirely.** `Connection::executeQuery()` catches exactly `Driver\Exception`. Every `Ferro\Client\Error\*` crossing the driver boundary MUST be wrapped.
10. **`Doctrine\DBAL\Driver\AbstractException`** is the non-`@internal` base: `__construct(string $message, ?string $sqlState = null, int $code = 0, ?Throwable $previous = null)` + `getSQLState(): ?string`. The vendor errno rides in `getCode()`.
11. **Only `DeadlockException` and `LockWaitTimeoutException` implement `RetryableException`.** Every specialised DBAL exception takes the same 2-arg ctor `(Driver\Exception $e, ?Query $q)`.
12. **The stock converters key on DIFFERENT fields per family.** `API\PostgreSQL\ExceptionConverter` keys on **SQLSTATE** (40001/40P01, 23502, 23503, 23505, 3D000, 3F000, 42601, 42702, 42703, 42P01, 42P07, 08006, plus two message substrings for `ConnectionLost`); `API\MySQL\ExceptionConverter` keys on **`$exception->getCode()`**, i.e. the vendor errno (1213, 1205, 1062/1557/1569/1586, 1050, 1051/1146, …). So our driver exception must carry the 5-char SQLSTATE in `getSQLState()` and the **integer errno** in `getCode()`.
13. **`DriverManager::createDriver()` does `return new $driverClass();`** — `Ferro\DBAL\Driver` MUST have a **no-argument constructor**, and a Driver instance is created **per DBAL Connection**, so remembering the pool kind from the last `connect()` is sound for one connection but must never be assumed populated (see hazard 15).
14. **SPEC §14's `'ferro' => [...]` config key FAILS PHPStan level 9**, which is a charter Definition-of-Done gate. `Doctrine\DBAL\Driver::connect()` is `@phpstan-param Params`, and `Params` is a SEALED array shape (`application_name?, charset?, dbname?, defaultTableOptions?, driver?, driverClass?, driverOptions?: array<mixed>, host?, keepReplica?, memory?, password?, path?, persistent?, port?, primary?, replica?, serverVersion?, sessionMode?, user?, wrapperClass?, unix_socket?`) with **no `ferro` key**. Measured: reading `$params['ferro']['pool']` produced two `nullCoalesce.offset` errors at level 9; `$params['driverOptions']['pool']` produced none. **`driverOptions` is the sanctioned slot**, every key read out of it needs an explicit `is_string()`/`is_int()` narrowing, and §14 must be amended.
15. **`Doctrine\DBAL\Connection::getDatabasePlatform()` can resolve the platform WITHOUT connecting.** If `$params['serverVersion']` (or `$params['primary']['serverVersion']`) is set it builds a `StaticServerVersionProvider` and never asks the driver connection at all. So `Driver::getDatabasePlatform()` may be reached with **no pool kind learned** — Task 6 must handle that branch explicitly and loudly, never by guessing a family.
16. **`Doctrine\DBAL\Connection::connect(): DriverConnection` is `protected`** — a `wrapperClass` subclass CAN reach our driver connection through it. That is the mechanism Task 13 uses.
17. **DBAL nests transactions CLIENT-SIDE.** `beginTransaction()` calls the driver's only at nesting level 1; deeper levels run `createSavepoint()` → `executeStatement($platform->createSavePoint($name))`, i.e. **ordinary SQL**. `rollBack()` at level 1 with `autoCommit === false` immediately re-calls `beginTransaction()`. `setTransactionIsolation()` likewise emits `executeStatement($platform->getSetTransactionIsolationSQL($level))`. **Therefore plain statements must route onto the pinned `tx_id` while a transaction is open — the single most important internal invariant of the driver connection.**
18. **`Doctrine\DBAL\Statement::bindValue()` runs `$type->convertToDatabaseValue($value, $platform)` and `$type->getBindingType()` BEFORE calling our `bindValue()`** — so by the time a value reaches the driver it has already been stringified by the PLATFORM's format strings, and DBAL expects to receive back a string its own `convertToPHPValue()` can parse with those same strings. The canonical-text boundary is a **two-way** conversion the driver owns.
19. **DBAL 4 passes named `:name` parameters straight to the driver**; `Driver\Mysqli\Statement::bindValue` simply `assert(is_int($param))`. A positional-only Ferro driver has direct precedent.

**The read/write signal, and why `Connection::query()` is unusable (Task 1)**

20. **Every result-producing client method hard-codes `readonly=true`**: `Connection::query()` (`php/client/src/Client/Connection.php:256`), `queryOne()` (`:277`), `scalar()` (`:290`), `rows()` (`:300`), `stream()` (`:357`). `exec()` hard-codes `readonly=false` **and** `FETCH_NONE`, so it can never return rows.
21. **The engine gates the §19.3 Indeterminate split on the CLIENT-DECLARED `readonly` flag alone** (no SQL inference anywhere). So a driver built on `query()` would tell an application that a lost `INSERT … RETURNING id` **provably did not apply**. That is a safety inversion, not a nuisance.
22. **The DBAL 4 SPI carries no read/write signal.** `Connection::executeQuery()` with **zero** params calls the driver's `query()` directly and with params calls `prepare()`+`execute()`; `executeStatement()` calls `exec()` with zero params and the **same** `prepare()`+`execute()` with params. An application may legitimately call `executeQuery('INSERT … RETURNING id')`. Charter rule 6 forbids inferring the answer from the SQL. **Therefore the driver declares `readonly = false` for EVERY statement** unless the operator explicitly opts the whole connection in with `driverOptions['readonly' => true]` (explicit configuration, not inference). Conservative by construction: it never costs safety. **It does cost more than retryability — see hazard 83**: `readonly` is read in TWO places in `fate.rs`, and the second is the 57014 override, so a plain `SELECT` cancelled server-side or killed by `statement_timeout` surfaces as an `IndeterminateWriteException`. That is the price of the decision, it is stated in full rather than softened, and Task 11 pins it with a live guard so it cannot be silently "fixed" later.
23. **`Connection::dispatch()` is PRIVATE** (`Connection.php:782`) and already returns exactly the shape a DBAL `Result` needs — `array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}` with POSITIONAL rows. `query()`/`rows()` `array_combine` on top of it (collapsing duplicate column names), and `scalar()` takes `$row[0]`. **There is no public positional-row fetch**, so `fetchNumeric()`/`fetchAllNumeric()`/`fetchFirstColumn()` have no route today.
24. **`ExecOk.affected` and `count($rows)` are DIFFERENT numbers.** The research spike shipped `rowCount() === 0` for an `UPDATE` that affected 1 row because its `Result` returned `count($rows)`. `Ferro\DBAL\Result` must carry `affected` alongside the rows.

**Streaming (Tasks 2, 12)**

25. **The session is strictly SINGLE-IN-FLIGHT.** `Session::assertNoOpenStream()` (`php/client/src/Client/Session.php:370-380`) makes `sendRequest()` **and** `openStream()` throw `ProtocolException` while a stream is open. The canonical Doctrine batch idiom — `foreach ($conn->iterateAssociative($sql) as $row) { $conn->executeStatement($upd, …); }` — therefore THROWS under a stream-backed `iterate*()` unless the driver does something about it.
26. **`Session::abandonStream()` is IDEMPOTENT by construction** (`Session.php:344-353`: `if (!$this->streamOpen || $this->streamRequestId !== $requestId) { return; }`), so a `free()` path may call it unconditionally.
27. **A generator that never STARTS never runs its `finally`.** `Connection::stream()` is safe because `openStream()` happens INSIDE the generator body. Task 2's `streamRaw()` deliberately opens EAGERLY (the DBAL `Statement::execute()` contract runs the statement), so a `RawStream` that is destroyed without being iterated would leak an open stream. `RawStream::close()` must therefore call `abandonStream()` itself; hazard 26 makes that safe after a normal drain.
28. **MySQL/MariaDB row streaming is STILL `Unsupported`** (`engine/crates/ferro-backend-mysql/src/conn.rs`, `supports_row_streaming() == false`, SPEC §22.2 (n)) and stays deferred — binding controller decision D-S8b-2. `iterate*()` therefore **streams on PostgreSQL and BUFFERS on MySQL**, a documented asymmetry recorded in §22.2 by Task 14.

**The PG bind pre-flight (Task 4)**

29. **`bind::check_param(v, ty)`'s `Value::Text` arm is `<PgText as ToSql>::accepts(ty)`** (`ferro-backend-pg/src/bind.rs:473`), and `PgText` is declared `pg_domain_aware_param! { PgText wraps String }` (`bind.rs:159`), so its `accepts` is `String`'s: `matches!(*ty, VARCHAR|TEXT|BPCHAR|NAME|UNKNOWN) || matches!(ty.name(), "citext"|"ltree"|"lquery"|"ltxtquery")` after the domain unwrap. **A PHP string bound into a PG `date`/`time`/`timestamp`/`timestamptz`/`numeric`/`uuid`/`json`/`jsonb` column is refused PRE-SEND.**
30. **`PgText`'s `encode_format` is BINARY** (the `pg_domain_aware_param!` macro delegates to `<String as ToSql>::encode_format`, which takes the trait's `Format::Binary` default). **Widening `accepts` alone would be a WIRE bug**, not just a policy change: PG would read the UTF-8 bytes of `2026-08-05` as a 4-byte binary `date`. The widened `PgText` must be an explicit `impl` that sends `Format::Text` **for the widened targets only**. Text is NOT byte-identical to binary for everything `<&str as ToSql>` accepts: it also admits `citext`, `ltree`, `lquery` and `ltxtquery` BY NAME (postgres-types-0.2.14 `src/lib.rs:1148-1153`), and for the last three the binary form is `0x01 || text` (`buf.put_u8(1)`, postgres-protocol-0.6.12 `src/types/mod.rs:1067-1072`). The equality holds for `varchar`/`text`/`bpchar`/`name`/`unknown`/`citext` and nowhere else — which is why Task 4 branches in BOTH `to_sql` and `encode_format` rather than going type-blind.
31. **`bind.rs::s7_a_bare_text_never_binds_to_a_temporal_or_numeric_column` (`bind.rs:1588`) EXPLICITLY pins the narrowness** with the rationale "a sentinel would be miscast", and its docblock says in as many words "Widening `Value::Text`'s accepts would break that". Task 4 must therefore **replace** it with a value-aware gate that still refuses PG's special datetime literals and the `NaN`/`Infinity` numeric literals for temporal/numeric targets — not delete it.
32. **MySQL has NO equivalent pre-flight** (`ferro-backend-mysql/src/bind.rs`: `COM_STMT_PREPARE` exposes no inferred parameter types, so validation is arity + canonical shape only). The same driver "works" on MySQL and hard-fails on PG. **Every bind task in this plan asserts on ALL THREE engines.**
33. **A failure that lands SERVER-SIDE is a known fate, not an `Indeterminate`.** A malformed date text bound into a `date` column produces a real `DbError` (`22007`), so `is_session_fatal` is false, `error_map` keys it, and `classify_fate` yields `NonRetryable`. This is what makes Task 4's widening safe: it moves a refusal from pre-send to server-side, never into the unclassifiable band.

**The type boundary (Task 9) — MEASURED against `doctrine/dbal 4.4.4`**

34. **Platform format strings, measured:** `PostgreSQL120Platform` — datetime `Y-m-d H:i:s`, **datetimetz `Y-m-d H:i:sO`**, date `Y-m-d`, time `H:i:s`. `MySQL84Platform` and `MariaDB110700Platform` — datetime `Y-m-d H:i:s`, **datetimetz `Y-m-d H:i:s`** (no offset at all), date `Y-m-d`, time `H:i:s`.
35. **`datetimetz` is BROKEN in both directions on every platform for our canonical text.** Measured `convertToPHPValue`: `'2026-08-05 13:45:07+0000'` → OK on PG, **THROW** on MySQL/MariaDB; `'2026-08-05 13:45:07'` → **THROW** on PG, OK on MySQL/MariaDB; **any** microsecond form → **THROW everywhere**. `DateTimeTzType` has no fallback.
36. **`datetime` DOES accept microseconds** — `'2026-08-05 13:45:07.250000'` → `DateTime(…250000)` on all three, because `DateTimeType::convertToPHPValue` falls back to `new DateTime($value)` when `createFromFormat` fails. **This falsifies the claim in `RawStringValuePolicy`'s own docblock** that the stock format string rejects the canonical fraction; Task 14 fixes that docblock.
37. **`time` has NO fallback:** `'13:45:07'` OK, `'13:45:07.250000'` **THROW** on all three.
38. **THE SILENT-CORRUPTION SET, measured, no exception raised:** `date '2026-00-05'` → `DateTime(2025-12-05)`; `datetime '0000-00-00 00:00:00'` → `DateTime(-0001-11-30)`; `time '24:00:00'` (a legal PG value) → `DateTime(1970-01-02 00:00:00)`, i.e. `format('H:i:s')` reads back `00:00:00`. `proto/PROTOCOL.md` §3.2 warns about exactly this parser class in prose; this is the measurement.
39. **`decimal`, `json`, `guid`, `binary`, `boolean`, the integer family and `float` all round-trip fine.** `DecimalType` passes the string through verbatim including `'NaN'`, `'Infinity'`, a preserved display scale and a 200-digit value; `BlobType` wraps a string into a `php://temp` **resource**.
40. **The bind direction keys on `(ParameterType, PHP type)`, never on the PHP type alone.** Measured: `BooleanType::convertToDatabaseValue(true)` returns **`int(1)`** with `ParameterType::BOOLEAN`; `FloatType`/`DecimalType`/`BigIntType` all bind `ParameterType::STRING` carrying a float / a numeric string; `BlobType` binds `LARGE_OBJECT` carrying a raw string. Binding `int(1)` as `TAG_I64` against a PG `boolean` column is refused by the narrow per-tag `accepts`.
41. **`Ferro\Client\Value\ValuePolicy::decode(int $tag, mixed $data): mixed` is PER-CELL TAG-AWARE by construction.** The driver's type boundary therefore needs **no** client API change to see column tags: it ships its own `ValuePolicy`. (`Connection::stream()` and `ExecCodec::decode()` both drop the ColMeta tag deliberately; the per-cell tag is the decode authority.)
42. **`CanonicalText` already supplies the sentinel predicates** the refusal needs: `dateIsSentinel(string): bool`, `timestampIsInstant(string): bool`, `timestamptzIsInstant(string): bool`, `timeIsNegative(string): bool` (`php/client/src/Client/Value/CanonicalText.php:172, 229, 261, 199`).

**Platform selection (Task 6)**

43. **MEASURED live version strings** (`SELECT version()`, which is exactly `ferrod`'s probe at `engine/crates/ferrod/src/pools.rs:114`, cached **verbatim and unnormalised**): PG = `PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`; MySQL = `8.4.11`; MariaDB = `11.8.8-MariaDB-ubu2404`.
44. **PG's verbatim string THROWS `InvalidPlatformVersion`** in the stock `AbstractPostgreSQLDriver` (its regex is anchored: `/^(?P<major>\d+)…/`). Measured: `'17.10 (Debian 17.10-1.pgdg13+1)'` → `PostgreSQL120Platform`, `'17.10'` → `PostgreSQL120Platform`. **Normalisation is mandatory on the PG path.**
45. **The MySQL-family string is LOAD-BEARING and must NOT be normalised.** MariaDB is detected ONLY by `stripos($version, 'mariadb') !== false`. Measured: `'11.8.8-MariaDB-ubu2404'` → `MariaDB110700Platform`, but `'11.8.8'` → **`MySQL84Platform`, a silently wrong dialect**. A single uniform "version normaliser" ships that bug.
46. **`PoolInfo.kind` is NEVER nil** (it is inferred from the DSN scheme, `PoolKind::wire_name()` → `"postgres"` / `"mysql"`), and **MariaDB reports `"mysql"`** — so MariaDB-vs-MySQL is decided by the VERSION STRING alone.
47. **`Session::poolInfo(): list<PoolInfo>` exists on the CONCRETE `Session` only** (`Session.php:234`) — it is **not** on `SessionInterface` (which declares exactly `sendRequest`, `bootEpoch`, `lastInFlight`, `close`) and `Client\Connection` exposes no accessor at all. Task 1 closes that.
48. **`poolInfo()` is a SNAPSHOT cached once in `hello()`.** "Re-read the pool metadata" on the same session can NEVER yield a new value. Only a new handshake (which the ReconnectLoop may have already performed) or an ordinary `SELECT version()` can.
49. **`ferrod`'s probe constants** (`engine/crates/ferrod/src/pools.rs`): `VERSION_PROBE_BUDGET` 1.5 s, `VERSION_TTL` 600 s, `VERSION_RETRY_BACKOFF` 5 s. `PoolEntry::begin_probe()` refuses a probe while one is in flight and inside the backoff, so an immediate re-handshake after a `nil` very likely returns `nil` again.
50. **`$params['serverVersion']` is DBAL's own zero-cost escape hatch** and must be named in the loud failure message.

**Client API + transactions (Tasks 1, 3, 10, 13)**

51. **`Connection::begin(bool $readonly = false)` hard-codes `'isolation' => null`** (`Connection.php:460-508`); the engine side is DONE and dialect-aware (`ferrod/src/tx/actor.rs::compose_begin_sql`). Task 3 appends the parameter **LAST** so no caller breaks.
52. **`Ferro\Protocol\Isolation: int { ReadCommitted = 0, RepeatableRead = 1, Serializable = 2 }` exists** and its docblock names S8b's `setTransactionIsolation(SERIALIZABLE)` as "the first real caller". Both copies are locked by `tests/Conformance/IsolationCrossLanguageTest`; S8b must EXERCISE that lock, not merely leave it present.
53. **`SET SESSION TRANSACTION ISOLATION LEVEL …` (MySQL) and `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …` (PG) are the two forms SPEC §22.2 (s) names as FORBIDDEN**, because the SESSION form persists on the pooled connection past COMMIT. Under Ferro today they taint the checkout (`ferro-classify` → `PinTrigger::Set` → `Checkout::apply_classify` sets `tainted`) and S3 hygiene wipes them, so `setTransactionIsolation()` **appears to succeed and has no effect on any later transaction** while `getTransactionIsolation()` keeps reporting the cached level. §22.2 (s) already proved that a "did the next tenant inherit it" assertion CANNOT FAIL — hygiene masks it either way.
54. **`Connection::rollBack()` swallows `ConnectionLostException|TransportException` unconditionally**, and swallows `Retryable`/`NonRetryable` only for `ERR_TX_DEADLINE` / `ERR_PROTOCOL`. Everything else rethrows. The driver must not add a second swallow on top.
55. **`Connection::lastInsertId()` CLEARS on a failed statement** — a deliberate, argued divergence from PDO (`dispatch()` nulls it on the way IN). **PostgreSQL always reports `null`**, by design; it is never emulated with a follow-up query because on a transaction-mode pool that lands on a different connection.
56. **`Ferro\Bytes` already exists** (`php/client/src/Bytes.php`) and `ExecCodec::bindOne` has an explicit `Bytes` arm → `TAG_BYTES`. **Every bare PHP string binds `TAG_TEXT`**, and a non-UTF-8 payload fails in the engine's msgpack `read_str` as `invalid utf8` — a generic "malformed ExecRequest", not a diagnosable bind error. So `ParameterType::BINARY`/`LARGE_OBJECT` MUST be wrapped by the driver.
57. **`ExtPacker::packBin` is production-dead today** and two packer methods delegate to `PurePacker` with no ext==pure conformance test. Task 7 creates the first `TAG_BYTES` call path in a package whose CI job DOES provision `ext-msgpack`; run it there at least once.
58. **`RetryPolicy::none()` is `new self(retryReads: false, maxAttempts: 1)`** and `Connection::begin()`'s docblock explicitly tells a driver to use it.

**ORM + pooling shapes that are BROKEN BY CONSTRUCTION (documented, not fixed, in Task 14)**

59. **`Doctrine\ORM\Id\IdentityGenerator::generateId()` is `(int) $em->getConnection()->lastInsertId();`** and `ClassMetadataFactory::determineIdGeneratorStrategy` defaults PostgreSQL to `GENERATOR_TYPE_IDENTITY` under DBAL 4. With `lastInsertId()` always `null` on PG, **ORM + PostgreSQL + the default IDENTITY strategy cannot insert through Ferro.** The remedy is configuration (`setIdentityGenerationPreferences(… => SEQUENCE)`), and §14's ORM acceptance clause must be narrowed accordingly.
60. **`Doctrine\ORM\Query\Exec\MultiTableDeleteExecutor::execute()` issues CREATE TEMPORARY TABLE, INSERT, DELETE and DROP as FOUR separate `executeStatement()` calls with no transaction.** On a transaction-mode pool statements 2-4 land on different connections. The workaround is an explicit application transaction; this is a first-class known-incompatibility entry.
61. **`Connection::setAutoCommit(false)`** makes DBAL open a transaction immediately at connect and re-open one after every commit — legal, but it pins a backend connection for the whole request and turns Ferro's core win off. Document it.
62. **Ordinary DDL does NOT taint** (`CREATE`/`ALTER`/`DROP`/`TRUNCATE`/`GRANT`/`REVOKE`/`COMMENT`/`LOCK`/`DISCARD` are in `ferro-classify`'s `SAFE_LEADING_KEYWORDS`), and **savepoint passthrough no longer taints** (`Checkout::apply_classify_for` skips the lexer for `TxControlVerdict::SavepointPassthrough`). Do NOT re-budget either as a cost.
63. **The `DISCARD ALL` typeinfo-cache defect (SPEC §22.2 (m)) is explicitly predicted to surface here**, because schema-manager and migrations traffic hits custom OIDs on reset connections. Safety is intact (`NonRetryable`, never `Indeterminate`) but a *second, distinct* custom OID on a reset connection degrades from the loud `Unsupported` to a bare `26000`.
64. **The unbounded backend dial** (`docs/followups/2026-08-10-unbounded-backend-dial.md`, ~127 s measured) means a DBAL app's first query against a down backend hangs rather than failing fast. Not an S8b defect; it WILL be reported as one. Name it in the driver README.

**The harness (Tasks 5, 14)**

65. **`php/client` is the ONLY PHP package and there is NO composer workspace** — no `composer.json` at the repo root or at `php/`. `.gitignore` already covers `php/**/vendor` and `**/.phpunit.cache`; `composer.lock` is **not** ignored and `php/client/composer.lock` IS committed.
66. **The composer PATH repository recipe is VERIFIED working**: `{"repositories":[{"type":"path","url":"../client"}], "require":{"ferro/client":"@dev"}}` installs `vendor/ferro/client` as a SYMLINK. **`"ferro/client": "*"` FAILS** (`php/client/composer.json` has no `version` field, so composer derives `dev-<branch>` and rejects it against the default stability) — the inline `@dev` flag is the fix and leaves `doctrine/dbal` on stable 4.4.4.
67. **Cross-package `autoload-dev` is VERIFIED working**: the ROOT package's `autoload-dev` is always honoured and the symlink makes `../client/tests/` real, so `"Ferro\\Tests\\": "../client/tests/"` makes `Ferro\Tests\Live\LiveTestCase` loadable from the driver package. `LiveTestCase::locateFerrod()` uses `dirname(__DIR__, 4)`, which still resolves correctly because the FILE physically lives at `php/client/tests/Live/`.
68. **Namespace coexistence is VERIFIED**: client autoloads `Ferro\ => php/client/src`, driver autoloads `Ferro\DBAL\ => php/doctrine-dbal/src`; composer's longest-PSR-4-prefix rule resolves each correctly. **The one hazard is adding `php/client/src/DBAL/` — never do that.**
69. **`LiveTestCase` spawns and reaps a `ferrod` per TEST** (~0.5 s each) and configures it ENTIRELY BY ENV: `FERRO_SOCK`, `FERRO_POOLS=default[,mysql]`, `FERRO_POOL_DEFAULT_DSN`, `FERRO_POOL_MYSQL_DSN`. The pool KIND is inferred from the DSN SCHEME — there is no `kind=` knob. Its `waitUntilReady()` does a full HELLO + `SELECT 1` before any test body runs.
70. **`ci/local-gate.sh` and `.github/workflows/ci.yml`'s `php` job hard-code EVERY PHP lane to `php/client`.** Adding the package without editing BOTH files means the driver is never installed, tested or statically analysed in CI — a silent no-op by omission.
71. **NEVER run `ci/local-gate.sh --live` while developing this slice against the shared containers**: its EXIT trap runs `docker compose down -v`. `testkit/smoke.sh` and `testkit/e2e-demo.sh` have the same class of trap. Any new `/testkit` runner must NOT copy it.
72. **The packagist DIST of `doctrine/dbal` ships `LICENSE composer.json src` ONLY** — no `tests/`, no `phpunit.xml.dist`. The upstream suite needs `--prefer-source` or a pinned `git clone`. And `composer install` in that checkout FAILS out of the box: composer's security audit blocks `squizlabs/php_codesniffer` (advisory PKSA-rdkp-vv9z-mjkg) via `doctrine/coding-standard` and `slevomat/coding-standard`; removing those three dev deps is sufficient.
73. **MEASURED upstream baselines, stock drivers, live containers.** `pdo_pgsql` vs PG 17.10, whole `tests/`: `Tests: 3913, Assertions: 5794, Failures: 1, Skipped: 556, Incomplete: 4`. `tests/Functional` only: `Tests: 1077, Failures: 1, Skipped: 512` ⇒ **~565 functional tests actually execute on PG**. The one failure is environmental (`testReturnsDatabaseNameWithoutDatabaseNameParameter` expects `postgres`; our superuser DB is `ferro`) — **even the stock driver is not 100% green on our containers.**
74. **MEASURED: the MySQL half does not even START.** `pdo_mysql` vs MySQL 8.4.11, `tests/Functional`: `Tests: 1077, Errors: 1057` — every test errors in `TestUtil::initializeDatabase()` with `1044 Access denied for user 'ferro'@'%' to database 'doctrine_tests'`. `SHOW GRANTS FOR CURRENT_USER()` is `GRANT USAGE ON *.*` + `GRANT ALL PRIVILEGES ON ferro.*`. PG only worked because its `ferro` role is `rolsuper = t`.
75. **`TestUtil::initializeDatabase()` opens a PRIVILEGED connection with `dbname` UNSET and runs `dropDatabase`/`createDatabase` on every run.** Under Ferro the DSN lives in the engine and PHP holds no credentials (D8) — a structural impedance mismatch, not a bug to fix in the driver.
76. **`TestUtil::isDriverOneOf()` keys on the driver NAME and has 60 call sites.** A `driverClass`-only connection matches nothing, so every vendor-gated test takes the "other" branch. That is the correct answer for us (claiming `pdo_pgsql` would opt us into PDO-specific expectations), and it must be a deliberate, recorded choice.
77. **Ferro has NO SQLite backend** (`engine/crates` has `pg` + `mysql` only; `AnyPool { Pg | Mysql }`), so the SQLite third of §14's acceptance bar is unreachable in S8b.

**Added in v2 — measured by the adversarial verification pass**

78. **`foreach` over an advanced `Generator` THROWS.** `foreach` calls `Generator::rewind()`, which raises `Exception: Cannot rewind a generator that was already run` once the generator has moved past its first yield. MEASURED on PHP 8.4.18. Any "drain the rest of this generator" code must use an explicit `while ($g->valid()) { $out[] = $g->current(); $g->next(); }` loop — which is what Task 12's own streamed `fetchNumeric()` already does.
79. **`memory_get_usage()` sampled AFTER a loop cannot distinguish streaming from buffering.** MEASURED over dbal 4.4.4's real code paths with 100 000 × (int, 64-char text): residual growth was **552 B streamed vs 472 B buffered** — the buffered run has already released everything by the time the loop exits, because `Doctrine\DBAL\Result` has no `__destruct` and the only live reference is the returned Generator's bound `$this`. The same run's **peak** growth was **2 728 B vs 34 109 720 B** and the **mid-loop** growth **2 040 B vs 33 302 432 B**. Any never-buffer guard must read `memory_get_peak_usage()` (with `memory_reset_peak_usage()`, PHP ≥ 8.2 — the client's floor) and/or sample INSIDE the loop.
80. **`Doctrine\DBAL\Result` has NO `__destruct`, and DBAL never calls the driver `Result::free()` on abandonment.** The only `__destruct` under `dbal/src` are `Driver/PgSQL/{Connection,Statement,Result}`, `Driver/Mysqli/Statement` and `Logging/Connection`. So `break`-ing out of an `iterateAssociative()` frees the DBAL wrapper by REFCOUNT and nothing else — which means a driver that keeps its own STRONG reference to the open result (v1 did) is the only thing keeping the stream alive, and the abandonment path silently becomes "transfer the entire remaining result set on the next statement". Task 12 holds a `\WeakReference` for exactly this reason — which closes the CANONICAL idiom (`foreach ($conn->iterateAssociative($sql) as $row)`, where the generator is a temporary and PHP destroys the driver `Result` by refcount at the `break`) but NOT a BOUND iterator (`$it = $conn->iterateAssociative($sql); foreach ($it as $row) { break; }`), where the reference stays live and the remainder is still transferred. Measured by the controller on PHP 8.4.18 + dbal 4.4.4 after v2 was written (the containers were stopped during v2). A live reference is indistinguishable from a caller who may still fetch, so this is a PHP refcount fact, not a design choice — Task 12 tests both shapes and Task 14 documents the second.
81. **PHP does not reject surplus arguments to a user-defined function.** A 3-parameter constructor called with 4 arguments binds the first three and DISCARDS the fourth — under `declare(strict_types=1)` that surfaces as a `TypeError` about the WRONG parameter (MEASURED: `Argument #3 ($c) must be of type bool, string given`). A constructor signature that differs between two tasks therefore fails with a message that points at neither task.
82. **`SELECT pg_cancel_backend(pg_backend_pid())` cancels its OWN statement**, producing SQLSTATE `57014` (`query_canceled`) on an ordinary AUTOCOMMIT statement with no session state, no second connection and no timeout. That is the only self-contained way this plan has to reach the engine's 57014 override from PHP: the PHP client never sends `ExecRequest.timeout_ms` (there is no timeout parameter anywhere in `php/client/src/Client/Connection.php`), and PG's `statement_timeout` cannot be set on a transaction-mode pool by a preceding statement — a non-local `SET` TAINTS the checkout but does not PIN it, so the next statement may land on a different connection.
83. **`readonly` is read TWICE in `fate.rs`, and the second place is the 57014 override.** `engine/crates/ferrod/src/services/fate.rs:71-114`: `in_tx → TxDeadline{Retryable}`; `!in_tx && readonly → Cancelled{NonRetryable}`; `!in_tx && !readonly && sent → WriteUnconfirmed{INDETERMINATE}`. So the driver's "declare write for everything" decision (hazard 22) does not merely cost retryability on a lost connection — it turns **every server-side-cancelled or `statement_timeout`-ed SELECT into an `IndeterminateWriteException`**. Task 11 pins both cells; §22.2 (ac), the engine docblock at `sql.rs:1053` and `CLAUDE.md`'s S5 paragraph are all amended by Task 14, because all three currently state "a streamed READ never becomes `Indeterminate`" without the client-side condition.
84. **The upstream `TestUtil` methods the allowlisted tests actually call** (MEASURED with `grep -rhoE 'TestUtil::[a-zA-Z]+'` over the allowlisted paths of the real 4.4.4 clone): `isDriverOneOf` ×13, **`getPrivilegedConnection` ×2** (`tests/Functional/TransactionTest.php:112`, `tests/Functional/Schema/OracleSchemaManagerTest.php:113`), `getConnectionParams` ×2, `getConnection` ×1. `getPrivilegedConnectionParameters` is **private** upstream (`tests/TestUtil.php:176`) and nothing outside the class calls it. Elsewhere in `tests/`: `isPdoStringifyFetchesEnabled` ×4 and `generateResultSetQuery` ×1 (`tests/Functional/Connection/FetchTest.php:20`) — the latter is a real 14-line SQL generator, not a policy answer.
85. **Upstream's `initializeDatabase()` is the functional suite's ONLY reset, and removing it makes the number non-reproducible.** MEASURED against a CORRECT driver (stock `PDO\PgSQL` selected by `driverClass`, i.e. zero driver defects) on live PG 17, running the plan's exact allowlist three times and changing ONLY `TestUtil`: upstream → `Errors 0, Failures 0`; v1's replacement → `Errors 23, Failures 3`; the SAME command again → `Errors 33, Failures 1`; upstream restored → `0/0`. The dominant modes are all leftover state (11× a leftover SEQUENCE reaching a name filter as a `Schema\Sequence`, 9× `TableExistsException`, 4× duplicate schema, 3× `Dependent objects still exist` because upstream's `dropTableIfExists` issues a plain `DROP TABLE`, 1× duplicate type). A reset is therefore a HARD precondition of recording a number.

### Definition of done (charter DoD, EVERY task)

- `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` — green **offline** (live tests skip, never fail, when the `FERRO_TEST_*_URL` vars are unset).
- `(cd php/client && ./vendor/bin/phpunit)` green; `(cd php/client && ./vendor/bin/phpstan analyse src --level 9)` clean.
- **From Task 5 on:** `(cd php/doctrine-dbal && ./vendor/bin/phpunit)` green and `(cd php/doctrine-dbal && ./vendor/bin/phpstan analyse src --level 9)` clean.
- The live tiers, run BY HAND with the shared containers (never `ci/local-gate.sh --live`, hazard 71):
  `(cd php/client && ./vendor/bin/phpunit tests/Live --fail-on-skipped)` and, from Task 5 on,
  `(cd php/doctrine-dbal && ./vendor/bin/phpunit tests/Live --fail-on-skipped)`.
- **Every guard added is mutation-proven** (see "guards that cannot fail").
- **No `/proto` change.** If one seems necessary, stop and raise it.
- The relevant SPEC section still tells the truth; a forced deviation is amended in the spec text **plus** a §22.2 line in the same change.

### Live test environment

The containers are ALREADY UP and are SHARED. Do not tear them down.

```
PG      postgres://ferro:ferro@127.0.0.1:55432/ferro
MySQL   mysql://ferro:ferro@127.0.0.1:33060/ferro
MariaDB mysql://ferro:ferro@127.0.0.1:33061/ferro
```

The standard invocation for a PHP live tier (verified working, 17.9 s for the client's 35 live tests):

```bash
cd /home/abdullak/projects/ferro/php/client && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live --fail-on-skipped
```

Build the daemon first with `cargo build -p ferrod` (the workspace target is at the REPO ROOT: `target/debug/ferrod`).

---

## File Structure

**Created — `ferro/client` (Tasks 1–3)**

- `php/client/src/Client/RawStream.php` — `Ferro\Client\RawStream`: the eagerly-opened, positional streamed read handle (`columns()` / `rows()` / `close()`). Its single responsibility is to make an open stream **explicitly closable**, because Task 2's eager open means a never-iterated generator would otherwise leak the stream (hazard 27).
- `php/client/tests/Unit/RawFetchTest.php`, `php/client/tests/Unit/RawStreamTest.php`, `php/client/tests/Client/ConnectionBeginIsolationTest.php`.
- `php/client/tests/Live/RawFetchLiveTest.php`, `php/client/tests/Live/RawStreamLiveTest.php`, `php/client/tests/Live/BeginIsolationLiveTest.php`, `php/client/tests/Live/ValuePolicyFacadeLiveTest.php`.

**Created — `php/doctrine-dbal` (Tasks 5–13)**

- `php/doctrine-dbal/composer.json`, `composer.lock`, `phpunit.xml.dist`, `phpstan.neon.dist`, `README.md`.
- `php/doctrine-dbal/src/Driver.php` — `Ferro\DBAL\Driver`: the 3-method SPI entry. Owns `connect()`, platform delegation and the converter factory; remembers the pool KIND learned at connect (hazard 15).
- `php/doctrine-dbal/src/DriverOptions.php` — the typed, PHPStan-level-9-clean parse of `$params` (`driverOptions` + `unix_socket` + `serverVersion`). One responsibility: turn `array<string,mixed>` into narrowed scalars, loudly.
- `php/doctrine-dbal/src/Connection.php` — `Ferro\DBAL\Connection implements Doctrine\DBAL\Driver\Connection`: the execution layer over `Ferro\Client\Connection`.
- `php/doctrine-dbal/src/Statement.php` — `bindValue()`'s `(ParameterType, PHP type)` → canonical mapping and `execute()`.
- `php/doctrine-dbal/src/Result.php` — the 9 SPI methods + `getColumnName()`, in BOTH modes (materialised rows, or a live `RawStream`).
- `php/doctrine-dbal/src/PlatformVersion.php` — the PG-ONLY version normaliser plus the family fork. Its own file because getting it "uniform" is the single measured way to ship a wrong SQL dialect (hazard 45).
- `php/doctrine-dbal/src/Value/DbalValuePolicy.php` — `implements Ferro\Client\Value\ValuePolicy`: the per-cell type boundary (TIMESTAMPTZ re-render, sentinel refusal).
- `php/doctrine-dbal/src/Value/TemporalFormat.php` — the two per-kind format strings, with a parity test against the stock platform accessors.
- `php/doctrine-dbal/src/ExceptionConverter.php` — `implements Doctrine\DBAL\Driver\API\ExceptionConverter`: the third-branch interception, then delegation to the STOCK per-family converter.
- `php/doctrine-dbal/src/IndeterminateWriteException.php` — `Ferro\DBAL\IndeterminateWriteException extends Doctrine\DBAL\Exception\DriverException`. **Must not implement `RetryableException`.**
- `php/doctrine-dbal/src/RetryableDriverException.php` — the §9.2 `Retryable` branch when the stock table produced a bare `DriverException`; `implements Doctrine\DBAL\Exception\RetryableException`.
- `php/doctrine-dbal/src/Exception/DriverException.php` — `extends Doctrine\DBAL\Driver\AbstractException`; carries `(sqlstate, errno)` where the stock converters read them.
- `php/doctrine-dbal/src/Exception/NoIdentityValue.php`, `Exception/ServerVersionUnavailable.php`, `Exception/BackendFamilyUnknown.php`, `Exception/NonRepresentableValue.php`, `Exception/UnsupportedStatement.php`.
- `php/doctrine-dbal/src/Wrapper/FerroConnection.php` — the `wrapperClass`: intercepts `setTransactionIsolation()` typed, emitting no SQL.
- `php/doctrine-dbal/tests/Unit/*.php`, `tests/Support/*.php`, `tests/Live/*.php` (per task, listed in each).

**Created — testkit / docs (Task 14)**

- `testkit/dbal-suite.sh` — the curated upstream runner. **No `docker compose down` trap of any kind.**
- `testkit/dbal/TestUtil.ferro.php` — the patched upstream `TestUtil` (honours `db_driverClass`; no `dropDatabase`/`createDatabase`).
- `testkit/dbal/bootstrap.php` — registers the three autoloaders and runs the HARD contact assertion before the first test.
- `testkit/dbal/allowlist.txt` — the curated `tests/Functional` paths, with a one-line reason per exclusion.
- `docs/dbal-suite/2026-08-10-results.md` — the recorded numbers + environment manifest.
- `docs/known-incompatibilities.md` — the §14 doc-page stub (M2 owns the full catalogue).

**Modified**

- `php/client/src/Client/Connection.php` — `fetchRaw()`, `streamRaw()`, `poolInfo()`, `begin()`'s isolation parameter.
- `php/client/src/Client/SessionInterface.php` — `poolInfo(): list<PoolInfo>`.
- `php/client/tests/Support/FakeSession.php` — the new interface method.
- `php/client/src/Ferro.php` — a `?ValuePolicy $values` parameter on `connect()`/`connectTcp()`/`assemble()`.
- `php/client/src/Client/Value/RawStringValuePolicy.php` — the docblock claim falsified by hazard 36.
- `engine/crates/ferro-backend-pg/src/bind.rs` — the widened `PgText` + the value-aware temporal/numeric literal gate + the rewritten narrowness test.
- `ci/local-gate.sh`, `.github/workflows/ci.yml` — the four `php/doctrine-dbal` lanes (hazard 70).
- `testkit/mysql-init.sql` — the `doctrine_tests` database + grant (hazard 74).
- `ferro-spec-v0.2.md` §14 + §22.2; `CLAUDE.md` (the stale "Next up" paragraph).

**Explicitly NOT modified**

- `php/client/composer.json`'s `require` — charter rule 7; the client gains no runtime dependency.
- Any `Doctrine\DBAL\Platforms\*` class, any Grammar/Processor — charter rule 6. The driver SELECTS a stock platform; it never subclasses one.
- `/proto` — no registry, vector or codec change in this slice.
- `engine/crates/ferro-backend-mysql/src/conn.rs`'s `query_stream` — MySQL streaming stays deferred (D-S8b-2, §22.2 (n)).
- `ferro-backend-mysql`'s `clean_reset_profile()` — the tracker-clean `None`-skip stays `Some(Full)`; it is what currently prevents a `SET SESSION` isolation leak, and Task 13 depends on that.

---

## §14 coverage map

Every sentence of SPEC §14, and the task that discharges it. Three of them are amended by Task 14 because they describe an SPI that does not exist in DBAL 4; none is silently dropped.

| §14 requirement | Task(s) | Note |
|---|---|---|
| `Ferro\DBAL\Driver::connect()` → a connection bound to a pool session | 5 | `driverOptions.pool`; `RetryPolicy::none()` |
| `getDatabasePlatform()` from `HELLO_ACK` metadata + server version | 5, 6 | 5 = kind + the PG-only normalisation; 6 = where the version comes from |
| `getExceptionConverter()` maps the §9.2 tree, uniformly across backends | 11 | delegates to the STOCK per-family tables |
| `Ferro\DBAL\IndeterminateWriteException` for the third branch | 11 | and never `RetryableException` |
| `prepare()` / `query()` / `exec()` | 5, 12 | `query()` is the streaming path |
| `lastInsertId()` | 10 | throws on PG; **§14's sequence-name argument is impossible in DBAL 4** → amended by 14 |
| `beginTransaction()` / `commit()` / `rollBack()` → TX service frames | 5, 10 | on the S8a imperative trio |
| savepoints via DBAL's normal savepoint path | 10 | the pinned-`tx_id` invariant, driven through DBAL's real nesting API |
| `quote()` client-side per platform (D5) | 5 | per-FAMILY (MySQL also escapes backslashes), locked against the stock accessors |
| `getServerVersion()` from the handshake | 6 | + the nil-version DECISION, implemented |
| `getNativeConnection()` (documented break) | 5, 14 | returns `Ferro\Client\Connection`; §14 said `Session` → amended |
| `bindValue()` `ParameterType` → canonical | 7 | keyed on the PAIR, measured |
| `Result::fetch*()` from row frames | 5, 8, 12 | buffered + streamed modes |
| `rowCount()` from `affected` | 8 | never `count($rows)`; the per-family SELECT divergence pinned |
| streaming when the consumer iterates | 12 | bounded to the parameterless read path, recorded in §22.2 (ac) |
| the configuration example | 5, 14 | `driverOptions`, because `'ferro'` fails PHPStan level 9 → §14 amended |
| DBAL middlewares / schema managers / migrations work unchanged | 5, 14 | a live middleware test; the schema managers via the upstream `Schema` subset |
| the known-incompatibilities doc page | 14 | stub now, full catalogue in M2 per §14 |
| **Acceptance:** functional suite green on PG + MySQL + SQLite; ORM suite on PG + MySQL | 14 | restated and recorded — SQLite impossible, ORM deferred, `TestUtil` patched, numbers measured |
| `read_pool` in the config example | 5, 14 | has no charter-compliant consumer (rule 6 forbids inference); shipped as an explicit `driverOptions.readonly` connection and documented |

---

## Task 1: `Connection::fetchRaw()` + pool metadata on `SessionInterface` — the two accessors the driver cannot exist without

The DBAL 4 SPI carries **no read/write signal** (hazard 22) and every result-producing client method hard-codes `readonly = true` (hazard 20). Building the driver on `Connection::query()` would therefore report a lost `INSERT … RETURNING id` as **Retryable** — "provably did not apply" — for a write whose fate is genuinely unknown (hazard 21). `fetchRaw()` is the fix: the caller declares the fate. It also closes two other gaps in one method — POSITIONAL rows (there is no public route to them today, hazard 23) and `affected` alongside the rows (hazard 24).

**Files:**
- Modify: `php/client/src/Client/Connection.php` (add `fetchRaw()` next to `rows()` at `:296-302`; add `poolInfo()` next to `session()` at `:153-157`)
- Modify: `php/client/src/Client/SessionInterface.php` (add `poolInfo()`)
- Modify: `php/client/tests/Support/FakeSession.php` (implement the new interface method)
- Test: `php/client/tests/Unit/RawFetchTest.php` (Create), `php/client/tests/Live/RawFetchLiveTest.php` (Create)

**Interfaces:**
- Produces:
  - `Ferro\Client\Connection::fetchRaw(string $sql, array $params = [], bool $readonly = false, bool $wantRows = true): array` returning `array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}`.
  - `Ferro\Client\Connection::poolInfo(): ?Ferro\Protocol\PoolInfo` — the entry for THIS connection's pool, resolved LIVE from `session()` on every call (never cached).
  - `Ferro\Client\SessionInterface::poolInfo(): list<Ferro\Protocol\PoolInfo>`.
- Consumes: `Ferro\Client\Connection::dispatch(string, array, bool, int): array` (private, `Connection.php:782`); `Ferro\Client\ExecCodec::FETCH_ROWS = 0` / `FETCH_NONE = 1`; `Ferro\Protocol\PoolInfo{name, kind, serverVersion}`; `Ferro\Client\Session::poolInfo()` (already implemented, `Session.php:234`).

- [ ] **Step 1: Write the failing unit test**

Create `php/client/tests/Unit/RawFetchTest.php`:

```php
<?php // /php/client/tests/Unit/RawFetchTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\PoolInfo;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 1 — `fetchRaw()` is the ONLY client entry point whose `readonly` fate flag is chosen
 * by the CALLER. Every other result-producing method hard-codes `readonly=true`
 * ({@see Connection::query} `:256`, `rows` `:300`, `scalar` `:290`, `stream` `:357`), and the engine
 * gates the §19.3 Indeterminate split on that flag ALONE. A DBAL driver has no read/write signal to
 * give (`executeQuery('INSERT … RETURNING id')` reaches the same code path as a SELECT), so it must
 * be able to say "write" for everything — otherwise a lost `INSERT … RETURNING` is reported
 * `Retryable`, i.e. "provably did not apply", for a write whose fate is unknown.
 *
 * The table below carries BOTH flag values so the assertion is a mirror property, not a one-sided
 * negative that cannot fail.
 */
final class RawFetchTest extends TestCase
{
    /** @return array<string, array{0: bool}> */
    public static function fates(): array
    {
        return ['declared write' => [false], 'declared read' => [true]];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('fates')]
    public function testTheCallerChosenReadonlyFlagReachesTheWire(bool $readonly): void
    {
        $session = (new FakeSession())->push(
            FakeSession::execOk([
                'cols' => [['name' => 'id', 'tag' => C::TAG_I64]],
                'rows' => [[['tag' => C::TAG_I64, 'data' => 7]]],
                'affected' => 3,
                'last_insert_id' => null,
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $conn = new Connection($session, 'default');

        $conn->fetchRaw('INSERT INTO t (v) VALUES (1) RETURNING id', [], $readonly);

        [, , $payload] = $session->lastRequest();
        $off = 0;
        $req = ExecRequest::mapFromWire((array) PackerFactory::forEncode()->unpack($payload, $off));
        self::assertSame($readonly, $req['readonly'], 'fetchRaw must send the caller-chosen fate flag verbatim');
    }

    /**
     * POSITIONAL rows, and `affected` SEPARATE from `count($rows)` — the two things a DBAL `Result`
     * needs and no public method provides. `query()`/`rows()` `array_combine` (which collapses
     * duplicate column names, breaking `fetchNumeric()`), and the research spike shipped
     * `rowCount() === 0` for an UPDATE that affected 1 row precisely because it used `count($rows)`.
     */
    public function testItReturnsPositionalRowsAndTheAffectedCountSeparately(): void
    {
        $session = (new FakeSession())->push(
            FakeSession::execOk([
                'cols' => [['name' => 'x', 'tag' => C::TAG_I64], ['name' => 'x', 'tag' => C::TAG_TEXT]],
                'rows' => [[['tag' => C::TAG_I64, 'data' => 1], ['tag' => C::TAG_TEXT, 'data' => 'a']]],
                'affected' => 9,
                'last_insert_id' => ['tag' => C::TAG_I64, 'data' => 42],
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $conn = new Connection($session, 'default');

        $raw = $conn->fetchRaw('SELECT 1 AS x, \'a\' AS x', [], true);

        self::assertSame(['x', 'x'], $raw['cols'], 'duplicate column names must survive, not collapse');
        self::assertSame([[1, 'a']], $raw['rows'], 'rows are POSITIONAL');
        self::assertSame(9, $raw['affected'], 'affected is the terminal field, not count($rows)');
        self::assertSame(42, $raw['last_insert_id']);
    }

    /** `wantRows: false` is `fetch=none` — a write that must not drag a result set back. */
    public function testWantRowsFalseSendsFetchNone(): void
    {
        $session = (new FakeSession())->thenExecOk(null);
        $conn = new Connection($session, 'default');

        $conn->fetchRaw('DELETE FROM t', [], false, false);

        [, , $payload] = $session->lastRequest();
        $off = 0;
        $req = ExecRequest::mapFromWire((array) PackerFactory::forEncode()->unpack($payload, $off));
        self::assertSame(1, $req['fetch'], 'fetch:none is 1 (ExecCodec::FETCH_NONE)');
    }

    /**
     * `poolInfo()` resolves LIVE off `session()` every call. Caching it would be wrong: the
     * ReconnectLoop replaces the Session object, and a restarted engine can advertise a different
     * `server_version` — which is exactly the value the platform (i.e. the SQL dialect) is chosen
     * from.
     */
    public function testPoolInfoResolvesThisConnectionsPoolAndNothingElse(): void
    {
        $session = new FakeSession();
        $session->poolInfo = [
            new PoolInfo('default', 'postgres', 'PostgreSQL 17.10 (Debian)'),
            new PoolInfo('mysql', 'mysql', '8.4.11'),
        ];
        $conn = new Connection($session, 'mysql');

        $info = $conn->poolInfo();
        self::assertNotNull($info);
        self::assertSame('mysql', $info->name);
        self::assertSame('mysql', $info->kind);
        self::assertSame('8.4.11', $info->serverVersion);

        self::assertNull((new Connection($session, 'nope'))->poolInfo(), 'an unadvertised pool is null, never a guess');
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit tests/Unit/RawFetchTest.php
```
Expected: FAIL — `Error: Call to undefined method Ferro\Client\Connection::fetchRaw()` (and `::poolInfo()`).

- [ ] **Step 3: Add `poolInfo()` to `SessionInterface`**

In `php/client/src/Client/SessionInterface.php`, add the `use` and the method (keep the file's existing `use Ferro\Protocol\Outcome;`):

```php
use Ferro\Protocol\PoolInfo;
```

```php
    /**
     * The pool metadata this session's `HELLO_ACK` advertised — name, backend family, and the
     * backend's own `version()` string VERBATIM (or null when the engine has not learned it).
     *
     * On the interface (M1-S8b) rather than on the concrete {@see Session} alone because the
     * Doctrine tier chooses its SQL DIALECT from it: `Ferro\DBAL\Driver::getDatabasePlatform()`
     * needs the backend family, and MariaDB-vs-MySQL is decided by the version string alone (both
     * report `kind = "mysql"`). Reaching it through an `instanceof Session` narrowing would make
     * every fake session in a driver unit test unusable.
     *
     * It is a SNAPSHOT taken once during the handshake — re-reading it on the same session can
     * never yield a new value. A caller that needs a fresher answer must re-handshake (which the
     * {@see ReconnectLoop} may already have done, replacing this object) or ask the backend.
     *
     * @return list<PoolInfo>
     */
    public function poolInfo(): array;
```

`Ferro\Client\Session` already implements this exactly (`Session.php:234-238`); it needs no edit.

- [ ] **Step 4: Implement it on `FakeSession`**

In `php/client/tests/Support/FakeSession.php`, add the import `use Ferro\Protocol\PoolInfo;` and, next to `lastInFlight()` (`:114`):

```php
    /**
     * What this fake's `HELLO_ACK` "advertised". Public so a test states the pool topology it is
     * asserting about in one line, next to the assertion, instead of through a builder.
     *
     * @var list<PoolInfo>
     */
    public array $poolInfo = [];

    /** @return list<PoolInfo> */
    public function poolInfo(): array { return $this->poolInfo; }
```

- [ ] **Step 5: Implement `fetchRaw()` and `poolInfo()` on `Connection`**

In `php/client/src/Client/Connection.php`, add `use Ferro\Protocol\PoolInfo;` to the imports, then add immediately after `rows()` (`:296-302`):

```php
    /**
     * The RAW statement entry point: positional rows, the terminal's own `affected` count, the
     * generated key — and, uniquely on this class, a `readonly` fate flag the CALLER chooses.
     *
     * **Why this exists (M1-S8b).** Every other result-producing method here hard-codes
     * `readonly = true` ({@see query}, {@see queryOne}, {@see scalar}, {@see rows}, {@see stream}),
     * and the engine gates the §19.3 Indeterminate split on that flag ALONE — it never infers a
     * read from the SQL. That is correct for the native API, where the method name IS the
     * declaration. It is wrong for a driver: the Doctrine DBAL 4 SPI carries no read/write signal
     * at all (`Connection::executeQuery('INSERT … RETURNING id')` with no parameters reaches the
     * driver's `query()`, and the prepared path serves `executeQuery` and `executeStatement`
     * alike), and charter rule 6 forbids inferring one from the statement text. A driver built on
     * {@see query} would therefore hand the application `Retryable` — "provably did not apply" —
     * for a write whose fate is genuinely unknown. Here the caller says which it is, and a caller
     * that cannot tell says `false` and gets the conservative answer.
     *
     * Two secondary gaps close with it: the rows come back POSITIONAL (so a driver's
     * `fetchNumeric()` is possible at all, and duplicate column names do not collapse the way
     * {@see rows}' `array_combine` collapses them), and `affected` arrives ALONGSIDE the rows
     * rather than being inferred from `count($rows)` — the two are different numbers.
     *
     * Inside an imperative transaction ({@see begin}) this routes through the pinned `tx_id` like
     * every other statement method, because it shares {@see dispatch}.
     *
     * @param list<mixed> $params positional bind values (`?` → `$n`).
     * @param bool $readonly the §19.3 fate declaration: `false` (the default) means a lost
     *   statement is `Indeterminate`, `true` means it is `Retryable`. Declaring `true` for a
     *   statement that writes is UNSAFE; declaring `false` for a read is merely conservative.
     * @param bool $wantRows `fetch=rows` when true, `fetch=none` when false.
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    public function fetchRaw(
        string $sql,
        array $params = [],
        bool $readonly = false,
        bool $wantRows = true,
    ): array {
        return $this->dispatch(
            $sql,
            $params,
            $readonly,
            $wantRows ? ExecCodec::FETCH_ROWS : ExecCodec::FETCH_NONE,
        );
    }
```

and immediately after `session()` (`:153-157`):

```php
    /**
     * This connection's OWN pool metadata from `HELLO_ACK` — or null if the engine does not
     * advertise a pool by that name.
     *
     * Resolved LIVE off {@see session} on every call and deliberately NOT cached: the
     * {@see ReconnectLoop} replaces the Session object on a reconnect, and a restarted engine can
     * advertise a different `server_version`. That value is what the Doctrine tier turns into a
     * PLATFORM, i.e. into which SQL dialect it emits, so a stale copy is a silently wrong dialect.
     *
     * Null means "this engine does not have that pool", which is a configuration error worth
     * reporting as itself — never a reason to guess a backend family.
     */
    public function poolInfo(): ?PoolInfo
    {
        foreach ($this->session()->poolInfo() as $info) {
            if ($info->name === $this->pool) {
                return $info;
            }
        }
        return null;
    }
```

- [ ] **Step 6: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit tests/Unit/RawFetchTest.php
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS, whole suite green, PHPStan clean.

- [ ] **Step 7: Write the live test**

Create `php/client/tests/Live/RawFetchLiveTest.php`:

```php
<?php // /php/client/tests/Live/RawFetchLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;

/**
 * M1-S8b Task 1, live: `fetchRaw()` against a real ferrod on a real PostgreSQL, proving the three
 * properties a DBAL `Result` stands on — positional rows, `affected` from the terminal (not
 * `count($rows)`), and the caller's `readonly` flag actually travelling — plus `poolInfo()`
 * answering for the pool this connection is bound to.
 */
final class RawFetchLiveTest extends LiveTestCase
{
    public function testPositionalRowsAndAffectedComeBackSeparately(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_raw');
        $c->exec('CREATE TABLE s8b_raw (id int primary key, note text)');
        $c->exec('INSERT INTO s8b_raw (id, note) VALUES (1, \'a\'), (2, \'b\')');

        $read = $c->fetchRaw('SELECT id, note FROM s8b_raw ORDER BY id', [], true);
        self::assertSame(['id', 'note'], $read['cols']);
        self::assertSame([[1, 'a'], [2, 'b']], $read['rows'], 'rows must be POSITIONAL');

        // An UPDATE touching 2 rows through the ROWS fetch mode: `affected` is 2 while `rows` is
        // empty. A Result that returned count($rows) here would report 0 — the research spike's bug.
        $upd = $c->fetchRaw('UPDATE s8b_raw SET note = \'z\'', [], false, false);
        self::assertSame(2, $upd['affected']);
        self::assertSame([], $upd['rows']);

        $c->exec('DROP TABLE s8b_raw');
    }

    /**
     * `INSERT … RETURNING` through `fetchRaw` with `readonly = false` — the exact shape that made
     * this method necessary. It must return the row AND be declared a write.
     */
    public function testInsertReturningIsAWriteThatStillYieldsRows(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_ret');
        $c->exec('CREATE TABLE s8b_ret (id serial primary key, note text)');

        $res = $c->fetchRaw('INSERT INTO s8b_ret (note) VALUES ($1) RETURNING id', ['hello'], false);
        self::assertCount(1, $res['rows']);
        self::assertIsInt($res['rows'][0][0]);

        $c->exec('DROP TABLE s8b_ret');
    }

    public function testPoolInfoAnswersForThisConnectionsPool(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $info = $c->poolInfo();
        self::assertNotNull($info, 'the default pool must be advertised');
        self::assertSame('default', $info->name);
        self::assertSame('postgres', $info->kind, 'the kind is inferred from the DSN scheme and is never nil');
    }
}
```

- [ ] **Step 8: Run the live test**

```bash
cargo build -p ferrod
cd /home/abdullak/projects/ferro/php/client && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/RawFetchLiveTest.php --fail-on-skipped
```
Expected: PASS (3 tests).

- [ ] **Step 9: MUTATION-PROVE both guards**

1. In `Connection::fetchRaw`, hard-code `true` in place of `$readonly`. Re-run `tests/Unit/RawFetchTest.php`. Expected: RED on `testTheCallerChosenReadonlyFlagReachesTheWire` for the "declared write" row. Restore.
2. In `Connection::fetchRaw`, return `$this->codec->assocRows($this->dispatch(...))`-shaped rows instead of the raw ones (i.e. `array_combine` them). Re-run. Expected: RED on `testItReturnsPositionalRowsAndTheAffectedCountSeparately` (the duplicate `x` column collapses). Restore.
3. In `Connection::poolInfo`, cache the result in a property on first call. Re-run the whole client suite. Expected: still green — **this mutation does NOT go red, and that is the point**: the caching hazard is unobservable offline. Record it, and note that Task 6's live nil-version test is where it becomes observable. Restore.

- [ ] **Step 10: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/client/src/Client/Connection.php \
        php/client/src/Client/SessionInterface.php \
        php/client/tests/Support/FakeSession.php \
        php/client/tests/Unit/RawFetchTest.php \
        php/client/tests/Live/RawFetchLiveTest.php
git commit -m "feat(m1-s8b): fetchRaw() — the one client entry point whose readonly fate the CALLER declares

Every other result-producing method hard-codes readonly=true, and the engine
gates the SS19.3 Indeterminate split on that flag alone. The DBAL 4 SPI carries
no read/write signal, so a driver built on query() would report a lost
INSERT ... RETURNING as Retryable — provably-did-not-apply — for a write whose
fate is unknown. fetchRaw() also gives the driver positional rows and the
terminal's own affected count, neither of which had a public route.

poolInfo() moves onto SessionInterface because the Doctrine tier chooses its SQL
DIALECT from it, and MariaDB-vs-MySQL is decided by the version string alone.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 2: `Connection::streamRaw()` + `Ferro\Client\RawStream` — the positional streamed read, closable

§14 requires `iterateAssociative()` et al. to never buffer, which (hazard 8) reduces to making the driver's `fetchAssociative()` pull incrementally. `Connection::stream()` cannot serve that: it hard-codes `readonly = true` (hazard 20), yields ASSOC rows (so `fetchNumeric()` is impossible and duplicate columns collapse), and exposes the column names only from inside the generator — while DBAL calls `columnCount()`/`getColumnName()` before any fetch.

**Files:**
- Create: `php/client/src/Client/RawStream.php`
- Modify: `php/client/src/Client/Connection.php` (add `streamRaw()` + the private `pumpRaw()` next to `stream()` at `:341-426`)
- Test: `php/client/tests/Unit/RawStreamTest.php` (Create), `php/client/tests/Live/RawStreamLiveTest.php` (Create)

**Interfaces:**
- Produces:
  - `Ferro\Client\RawStream::__construct(array $cols, \Generator $rows, ?StreamingSessionInterface $session, int $requestId)`; `columns(): list<string>`; `rows(): \Generator` (yields `list<mixed>`); `close(): void` (idempotent).
  - `Ferro\Client\Connection::streamRaw(string $sql, array $params = [], bool $readonly = false): RawStream`.
- Consumes: `Ferro\Client\StreamingSessionInterface::{openStream,readStreamFrame,sendWindowUpdate,abandonStream}`; `Ferro\Client\ExecCodec::{encode,decodeRow,FETCH_STREAM}`; `Ferro\Client\Connection::throwIfError()` (private, already present).

- [ ] **Step 1: Write the failing unit test**

Create `php/client/tests/Unit/RawStreamTest.php`:

```php
<?php // /php/client/tests/Unit/RawStreamTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\RawStream;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 2 — `streamRaw()` opens EAGERLY (the DBAL `Statement::execute()` contract runs the
 * statement) and hands back a handle whose columns are readable BEFORE the first row, because
 * `Doctrine\DBAL\Result::columnCount()`/`getColumnName()` are callable before any fetch.
 *
 * The eager open creates a hazard {@see Connection::stream} does not have: there, `openStream()`
 * runs INSIDE the generator, so a generator that is never started never opened anything. Here the
 * stream is already open, so a handle that is dropped without being iterated would leave it open
 * and desync the session. {@see RawStream::close} is what closes that hole, and the third test
 * below is the guard for it.
 */
final class RawStreamTest extends TestCase
{
    /** The caller's fate flag reaches the wire, exactly as in the buffered path. */
    public function testTheCallerChosenReadonlyFlagReachesTheStreamedRequest(): void
    {
        $session = (new FakeSession())->thenStreamEnd();
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('INSERT INTO t (v) VALUES (1) RETURNING id', [], false);
        self::assertSame([], $stream->columns(), 'an immediate terminal advertises no columns');

        [, , $payload] = $session->lastRequest();
        $off = 0;
        $req = ExecRequest::mapFromWire((array) PackerFactory::forEncode()->unpack($payload, $off));
        self::assertFalse($req['readonly'], 'a streamed write must NOT be declared readonly');
        self::assertSame(2, $req['fetch'], 'fetch:stream is 2 (ExecCodec::FETCH_STREAM)');
    }

    /** A stream handle is closable, and closing it twice is harmless. */
    public function testCloseIsIdempotent(): void
    {
        $session = (new FakeSession())->thenStreamEnd();
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT 1', [], true);
        $stream->close();
        $stream->close();
        self::assertTrue(true, 'close() must never throw on a second call');
    }

    /**
     * THE guard for the eager-open hazard: a handle that is closed WITHOUT ever being iterated must
     * still abandon the engine-side stream. `FakeSession` records every `abandonStream` call, so
     * this is behavioural, not a signature assertion.
     */
    public function testClosingAnUniteratedStreamStillAbandonsItOnTheWire(): void
    {
        $session = (new FakeSession())->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]]);
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT id FROM t', [], true);
        self::assertSame(['id'], $stream->columns());
        self::assertSame(0, $session->abandonCount, 'nothing abandoned yet');

        $stream->close();
        self::assertSame(1, $session->abandonCount, 'a never-iterated stream must still be abandoned');
    }
}
```

This needs two small additions to `FakeSession` (Step 3): a `thenStreamHead()` fixture and a public `abandonCount`.

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit tests/Unit/RawStreamTest.php
```
Expected: FAIL — `Error: Call to undefined method Ferro\Client\Connection::streamRaw()`.

- [ ] **Step 3: Extend `FakeSession` with a HEAD fixture and an abandon counter**

In `php/client/tests/Support/FakeSession.php`, add next to `thenStreamEnd()` (`:193-198`):

```php
    /**
     * Queue a stream that opens with a HEAD carrying `$cols` and then never produces another frame
     * unless the test drives one. Enough to pin what the OPEN advertised and what happens when the
     * handle is dropped — the multi-frame path is exercised against a real Session over a
     * FakeTransport by `ConnectionStreamTest`.
     *
     * @param list<array{name:string,tag:int}> $cols
     */
    public function thenStreamHead(array $cols): self
    {
        $this->streamHeads[] = $cols;
        return $this;
    }
```

and next to the other public counters (`:56`):

```php
    /** @var list<list<array{name:string,tag:int}>> queued HEAD column lists, one per openStream */
    private array $streamHeads = [];

    /** How many times {@see abandonStream} was called — the eager-open leak guard reads it. */
    public int $abandonCount = 0;
```

Then change `openStream()` (`:226`) so it prefers a queued HEAD over a queued terminal, and count abandons in `abandonStream()` (`:262`):

```php
    /** @return array{type:'head', requestId:int, cols:list<array{name:string,tag:int}>}|array{type:'end', requestId:int, outcome:Outcome} */
    public function openStream(int $service, int $method, string $payload): array
    {
        $this->sent[] = [$service, $method, $payload];
        $this->lastInFlight = [$service, $method];
        $rid = ++$this->streamRid;
        if ($this->streamHeads !== []) {
            return ['type' => 'head', 'requestId' => $rid, 'cols' => array_shift($this->streamHeads)];
        }
        $outcome = $this->streamScript[$this->streamPos++]
            ?? throw new \LogicException('FakeSession: no stream reply queued');
        return ['type' => 'end', 'requestId' => $rid, 'outcome' => $outcome];
    }

    public function abandonStream(int $requestId): void
    {
        ++$this->abandonCount;
    }
```

- [ ] **Step 4: Create `Ferro\Client\RawStream`**

Create `php/client/src/Client/RawStream.php`:

```php
<?php // /php/client/src/Client/RawStream.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ProtocolException;

/**
 * An OPEN streamed read: the column names, a lazy Generator of POSITIONAL rows, and an explicit
 * {@see close}. Produced by {@see Connection::streamRaw}, consumed by the Doctrine tier's
 * `Result`.
 *
 * **Why the columns are eager and the rows are lazy.** `Doctrine\DBAL\Result::columnCount()` and
 * `getColumnName()` are callable before any fetch, and `Doctrine\DBAL\Statement::execute()` is
 * expected to have RUN the statement by the time it returns — so the `HEAD` frame must already be
 * read. The DATA frames must not be: `Doctrine\DBAL\Result::iterateAssociative()` is literally
 * `while (($row = $this->fetchAssociative()) !== false) yield $row;`, so "never buffer" reduces
 * entirely to pulling one row at a time here.
 *
 * **Why {@see close} exists at all.** {@see Connection::stream} needs no such method: it opens the
 * stream INSIDE its generator, so a generator that is never started never opened anything, and a
 * generator that IS started runs its `finally` (a `CANCEL` + drain) when it is destroyed. This
 * handle opens EAGERLY, so a caller that builds one and drops it without iterating would leave the
 * engine-side stream open and the very next request would read its frames as its own reply — a wire
 * desync. `close()` closes that hole. It is safe to call unconditionally and repeatedly:
 * {@see Session::abandonStream} is idempotent by construction (it returns immediately when no
 * stream with that id is open), so calling it after a normal drain is a no-op.
 */
final class RawStream
{
    private bool $closed = false;

    /**
     * @param list<string> $cols the column names from the `HEAD` frame, in order.
     * @param \Generator<int, list<mixed>> $rows one POSITIONAL row per iteration.
     * @param ?StreamingSessionInterface $session null when the stream reached its terminal during
     *   the open (a known fate decided before any HEAD/DATA went out) — nothing to abandon.
     */
    public function __construct(
        private readonly array $cols,
        private readonly \Generator $rows,
        private readonly ?StreamingSessionInterface $session,
        private readonly int $requestId,
    ) {}

    /** @return list<string> */
    public function columns(): array
    {
        return $this->cols;
    }

    /**
     * The row generator. Iterating it consumes DATA frames and replenishes the credit window; a
     * mid-stream error terminal throws the mapped taxonomy exception AFTER the rows that already
     * arrived.
     *
     * @return \Generator<int, list<mixed>>
     */
    public function rows(): \Generator
    {
        if ($this->closed) {
            throw new ProtocolException('RawStream::rows() after close()');
        }
        return $this->rows;
    }

    /** Whether {@see close} has been called. */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * Abandon whatever is left: `CANCEL` + drain to the ONE terminal (charter rule 4). Idempotent,
     * and a no-op when the stream already finished normally.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->session?->abandonStream($this->requestId);
    }
}
```

- [ ] **Step 5: Implement `streamRaw()` + `pumpRaw()` on `Connection`**

In `php/client/src/Client/Connection.php`, add immediately after `stream()` (`:426`):

```php
    /**
     * The RAW streamed read the Doctrine tier needs: POSITIONAL rows, a CALLER-chosen `readonly`
     * fate flag, and the column names available BEFORE the first row.
     *
     * It differs from {@see stream} in exactly those three ways and shares everything else — the
     * per-frame `WINDOW_UPDATE`, the mid-stream error terminal, the `CANCEL`+drain on abandonment.
     * The reasons are the same three that made {@see fetchRaw} necessary: `stream()` hard-codes
     * `readonly = true`, which would mis-fate a streamed `INSERT … RETURNING`; it yields ASSOC rows,
     * which makes `fetchNumeric()` impossible and collapses duplicate column names; and it exposes
     * the column names only from inside the generator, while `Doctrine\DBAL\Result::columnCount()`
     * is callable before any fetch.
     *
     * The open is EAGER — see {@see RawStream} for why that makes {@see RawStream::close}
     * mandatory rather than optional.
     *
     * @param list<mixed> $params
     * @param bool $readonly the §19.3 fate declaration; see {@see fetchRaw}.
     */
    public function streamRaw(string $sql, array $params = [], bool $readonly = false): RawStream
    {
        $session = $this->tx?->session() ?? $this->session();
        if (!$session instanceof StreamingSessionInterface) {
            throw new ProtocolException(
                'streamRaw() requires a session implementing StreamingSessionInterface (the concrete Session)',
            );
        }
        $payload = $this->codec->encode(
            $this->pool,
            $sql,
            $params,
            $readonly,
            ExecCodec::FETCH_STREAM,
            $this->tx?->txId(),
        );
        // Same contract as `stream()`: a streamed statement reports no generated key and CLEARS the
        // previous one. Unlike `stream()`, this method is not itself a generator, so the clear
        // happens here — the statement really has been issued by the time we return.
        $this->lastInsertId = null;

        $opened = $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $payload);
        if ($opened['type'] === 'end') {
            // A known fate decided before any HEAD/DATA went out. Throws on an error terminal;
            // otherwise there is genuinely nothing to read and nothing to abandon.
            $this->throwIfError($opened['outcome']);
            return new RawStream([], (static function (): \Generator { yield from []; })(), null, 0);
        }
        $rid = $opened['requestId'];
        $cols = array_map(static fn (array $c): string => $c['name'], $opened['cols']);
        return new RawStream($cols, $this->pumpRaw($session, $rid), $session, $rid);
    }

    /**
     * The DATA pump behind {@see streamRaw}: one POSITIONAL row per yield, a `WINDOW_UPDATE` after
     * each consumed frame, and the same `finally` discipline as {@see stream} — a `CANCEL`+drain
     * iff the terminal was never reached AND no wire operation has already failed (a second wire op
     * on a broken connection would mask or replace the real exception).
     *
     * @return \Generator<int, list<mixed>>
     */
    private function pumpRaw(StreamingSessionInterface $session, int $rid): \Generator
    {
        $reachedTerminal = false;
        $wireFailed = false;
        try {
            while (true) {
                try {
                    $frame = $session->readStreamFrame($rid);
                } catch (\Throwable $e) {
                    $wireFailed = true;
                    throw $e;
                }
                if ($frame['type'] === 'end') {
                    $reachedTerminal = true;
                    $this->throwIfError($frame['outcome']);
                    return;
                }
                foreach ($frame['rows'] as $rawRow) {
                    yield $this->codec->decodeRow($rawRow);
                }
                try {
                    $session->sendWindowUpdate($rid, 1, $frame['bytes']);
                } catch (\Throwable $e) {
                    $wireFailed = true;
                    throw $e;
                }
            }
        } finally {
            if (!$reachedTerminal && !$wireFailed) {
                $session->abandonStream($rid);
            }
        }
    }
```

- [ ] **Step 6: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit tests/Unit/RawStreamTest.php && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS, whole suite green, PHPStan clean.

- [ ] **Step 7: Write the live test**

Create `php/client/tests/Live/RawStreamLiveTest.php`:

```php
<?php // /php/client/tests/Live/RawStreamLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;

/**
 * M1-S8b Task 2, live: `streamRaw()` against a real ferrod on real PostgreSQL. The three properties
 * that matter are (a) the rows arrive POSITIONAL and in order, (b) the columns are readable before
 * the first row, and (c) a stream ABANDONED before its terminal leaves the session usable — which
 * is the only observable proof that the CANCEL+drain really happened.
 */
final class RawStreamLiveTest extends LiveTestCase
{
    public function testColumnsAreReadableBeforeAnyRowAndRowsArePositional(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_stream');
        $c->exec('CREATE TABLE s8b_stream (id int primary key, note text)');
        $c->exec('INSERT INTO s8b_stream SELECT g, \'n\' || g FROM generate_series(1, 500) g');

        $stream = $c->streamRaw('SELECT id, note FROM s8b_stream ORDER BY id', [], true);
        self::assertSame(['id', 'note'], $stream->columns(), 'columns must be readable before the first row');

        $seen = 0;
        foreach ($stream->rows() as $row) {
            self::assertSame($seen + 1, $row[0], 'rows are POSITIONAL and in order');
            ++$seen;
        }
        self::assertSame(500, $seen);

        $c->exec('DROP TABLE s8b_stream');
    }

    /**
     * ABANDONMENT. Break out after 10 rows, `close()` the handle, then run another statement on the
     * SAME connection. If the CANCEL+drain did not happen, the next request reads the leftover
     * DATA frames as its own reply and this throws a ProtocolException — so the plain assertion
     * below IS the guard.
     */
    public function testAbandoningAStreamLeavesTheSessionUsable(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_abandon');
        $c->exec('CREATE TABLE s8b_abandon (id int primary key)');
        $c->exec('INSERT INTO s8b_abandon SELECT g FROM generate_series(1, 5000) g');

        $stream = $c->streamRaw('SELECT id FROM s8b_abandon ORDER BY id', [], true);
        $seen = 0;
        foreach ($stream->rows() as $_row) {
            if (++$seen === 10) {
                break;
            }
        }
        $stream->close();

        self::assertSame(5000, $c->scalar('SELECT count(*) FROM s8b_abandon'));
        $c->exec('DROP TABLE s8b_abandon');
    }
}
```

- [ ] **Step 8: Run the live test**

```bash
cd /home/abdullak/projects/ferro/php/client && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/RawStreamLiveTest.php --fail-on-skipped
```
Expected: PASS (2 tests).

- [ ] **Step 9: MUTATION-PROVE the guards**

1. Delete the body of `RawStream::close()` (leave it empty). Re-run `tests/Unit/RawStreamTest.php`. Expected: RED on `testClosingAnUniteratedStreamStillAbandonsItOnTheWire`. Re-run the LIVE `testAbandoningAStreamLeavesTheSessionUsable`. Expected: RED with a `ProtocolException` (the next request reads the leftover frames). Restore. **Both must go red** — the unit guard alone would not prove the wire recovers.
2. In `streamRaw()`, hard-code `true` for `$readonly`. Re-run the unit test. Expected: RED on `testTheCallerChosenReadonlyFlagReachesTheStreamedRequest`. Restore.

- [ ] **Step 10: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/client/src/Client/RawStream.php \
        php/client/src/Client/Connection.php \
        php/client/tests/Support/FakeSession.php \
        php/client/tests/Unit/RawStreamTest.php \
        php/client/tests/Live/RawStreamLiveTest.php
git commit -m "feat(m1-s8b): streamRaw() + RawStream — positional streamed rows with a caller-chosen fate

Doctrine's Result::iterateAssociative() is a loop over fetchAssociative(), so
never-buffer reduces to pulling one row at a time. stream() cannot serve that:
it hard-codes readonly=true, yields assoc rows, and hides the column names
inside the generator while DBAL calls columnCount() before any fetch.

The open is EAGER (Statement::execute() must have run the statement), which is
why RawStream::close() exists: unlike stream(), a handle dropped without being
iterated would leave the engine-side stream open and desync the session.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 3: `begin()` learns an isolation level, and `Ferro::connect()` learns a `ValuePolicy`

Two additive client changes the driver cannot work around. **(a)** `Connection::begin()` hard-codes `'isolation' => null` (hazard 51) while the wire field, the `Isolation` enum and the dialect-aware `compose_begin_sql` all shipped in S8a — so the ONLY thing missing between a DBAL `setTransactionIsolation(SERIALIZABLE)` and a genuinely serializable transaction is this parameter. **(b)** `Ferro::connect()` accepts only `?TypePolicyOptions $types`, and `Connection`'s `values:` and `types:` are mutually exclusive, so **the resilient facade cannot currently produce a connection that decodes with a custom `ValuePolicy`** — which is precisely what the driver needs (Task 9).

**Files:**
- Modify: `php/client/src/Client/Connection.php` (`begin()` at `:460-508`)
- Modify: `php/client/src/Ferro.php` (`connect()`, `connectTcp()`, `assemble()`)
- Test: `php/client/tests/Client/ConnectionBeginIsolationTest.php` (Create), `php/client/tests/Live/BeginIsolationLiveTest.php` (Create), `php/client/tests/Live/ValuePolicyFacadeLiveTest.php` (Create — the `values:` observable, which cannot be reached offline)

**Interfaces:**
- Produces:
  - `Ferro\Client\Connection::begin(bool $readonly = false, ?Ferro\Protocol\Isolation $isolation = null): void` — the new parameter is appended LAST so every existing call site keeps working.
  - `Ferro\Ferro::connect(string $socketPath, string $pool = 'default', float $connectTimeout = 2.0, float $ioTimeout = 5.0, ?RetryPolicy $policy = null, ?TypePolicyOptions $types = null, ?ValuePolicy $values = null): Connection` and the identical tail on `connectTcp()`.
- Consumes: `Ferro\Protocol\Isolation` (`ReadCommitted = 0`, `RepeatableRead = 1`, `Serializable = 2`); `Ferro\Protocol\BeginRequest::encode(array, PackerInterface)`; `Ferro\Client\Value\ValuePolicy`.

- [ ] **Step 1: Write the failing unit test**

Create `php/client/tests/Client/ConnectionBeginIsolationTest.php`:

```php
<?php // /php/client/tests/Client/ConnectionBeginIsolationTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Protocol\BeginRequest;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Isolation;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 3 — the isolation byte finally travels.
 *
 * The engine half shipped in M1-S8a (`compose_begin_sql(dialect, isolation, readonly)`, unit-tested
 * per cell, with the batched `SET TRANSACTION …; START TRANSACTION …` form on MySQL that does NOT
 * leak onto the pooled connection). The wire field and the `Isolation` enum shipped with it. The
 * only missing link was this parameter — and until it existed, a Doctrine
 * `setTransactionIsolation(SERIALIZABLE)` was a SILENT no-op (SPEC §22.2 (s)).
 *
 * The provider walks EVERY enum case plus the absent case, derived from `Isolation::cases()`, so a
 * fourth case added to the enum makes this test fail rather than silently skipping the new value.
 */
final class ConnectionBeginIsolationTest extends TestCase
{
    /** @return array<string, array{0: ?Isolation, 1: ?int}> */
    public static function levels(): array
    {
        $out = ['pool default (absent)' => [null, null]];
        foreach (Isolation::cases() as $case) {
            $out[$case->name] = [$case, $case->value];
        }
        return $out;
    }

    /** @return array{0: FakeSession, 1: Connection} a session whose BEGIN is answered Ok(tx_id=7) */
    private function wired(): array
    {
        $session = (new FakeSession())->push(
            Outcome::ok(BeginResponse::encode(['tx_id' => 7], PackerFactory::forEncode())),
            [C::SERVICE_TX, C::METHOD_TX_BEGIN],
        );
        return [$session, new Connection($session, 'default')];
    }

    /** @return array{pool:string,isolation:?int,readonly:bool} */
    private function sentBegin(FakeSession $session): array
    {
        [, , $payload] = $session->lastRequest();
        $off = 0;
        return BeginRequest::mapFromWire((array) PackerFactory::forEncode()->unpack($payload, $off));
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('levels')]
    public function testTheIsolationByteReachesTheBeginRequest(?Isolation $iso, ?int $expected): void
    {
        [$session, $conn] = $this->wired();
        $conn->begin(false, $iso);
        self::assertSame($expected, $this->sentBegin($session)['isolation']);
    }

    /** Appended LAST: the pre-S8b one-argument call site must keep compiling and behaving. */
    public function testTheReadonlyOnlyCallShapeStillWorks(): void
    {
        [$session, $conn] = $this->wired();
        $conn->begin(true);
        $sent = $this->sentBegin($session);
        self::assertNull($sent['isolation'], 'no isolation was asked for');
        self::assertTrue($sent['readonly'], 'readonly still travels');
    }

    /**
     * The mutual exclusion `Ferro::connect(values:)` has to respect. **This pins a PRE-EXISTING
     * invariant** (`Connection::__construct` already rejects the pair at HEAD, `Connection.php:120-127`)
     * — it passes before any Task 3 edit and is here so the facade's new `values:` parameter cannot
     * be wired in a way that bypasses it, not as evidence that the parameter works.
     *
     * **The OBSERVABLE that `values:` is not an inert knob is the LIVE test in Step 6**, and it has
     * to be: `Ferro::assemble()` is private, `connect()` needs a real socket, and a reflection
     * parameter COUNT passes just as well for a `connect()` that accepts `$values` and then drops
     * it — which is exactly what `assemble()`'s own docblock records happening once already with
     * `$types` ("dropping $types from either assemble(...) call left PHPUnit green AND PHPStan
     * level 9 clean while Ferro::connect(types: …) became an inert public knob"). v1 asserted the
     * count here and called it an observable; it is not one, so it is gone.
     */
    public function testConnectionRefusesAValuePolicyAlongsideTypeOptions(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Connection(
            new FakeSession(),
            'default',
            values: new RawStringValuePolicy(),
            types: new \Ferro\Client\Value\TypePolicyOptions(),
        );
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit tests/Client/ConnectionBeginIsolationTest.php
```
Expected: FAIL — `ArgumentCountError: Too many arguments to function Ferro\Client\Connection::begin(), 2 passed and at most 1 expected`.

- [ ] **Step 3: Add the isolation parameter to `begin()`**

In `php/client/src/Client/Connection.php`, add `use Ferro\Protocol\Isolation;` to the imports, replace the last paragraph of `begin()`'s docblock (the one starting "Isolation is deliberately NOT a parameter") with:

```php
     * **Isolation (M1-S8b).** `$isolation` is the SPEC §9.1-style "policies over guesses" answer to
     * a problem that has no other fix on a transaction-mode pool: Doctrine sets isolation with a
     * `SET SESSION TRANSACTION ISOLATION LEVEL …` statement, which lands on whichever pooled
     * connection the checkout hands out, TAINTS it (the S2 assist lexer classifies a non-local
     * `SET`), and is then WIPED by S3 hygiene before the next `BEGIN` — so the application asks for
     * SERIALIZABLE, gets READ COMMITTED, and nothing anywhere reports an error (SPEC §22.2 (s),
     * which also records that the obvious "did the next tenant inherit it" test cannot fail,
     * because hygiene masks the leak either way). Carried HERE, the level rides
     * `BeginRequest.isolation` and the engine composes the correct per-transaction form for the
     * dialect — `BEGIN ISOLATION LEVEL …` on PostgreSQL, the batched
     * `SET TRANSACTION …; START TRANSACTION …` on MySQL/MariaDB, which deliberately does NOT use
     * the connection-persisting `SESSION` form. `null` means the pool default.
```

then change the signature and the payload:

```php
    public function begin(bool $readonly = false, ?Isolation $isolation = null): void
    {
```

```php
        // The ENUM CASE, not `$isolation?->value`. `BeginRequest::encode` has an
        // `$iso instanceof Isolation` arm precisely so the enum rides a byte-locked path
        // (`IsolationCrossLanguageTest`, SPEC §22.2 (w)); unwrapping it here would route around the
        // one lock that pins the PHP mapping to the Rust one.
        $payload = BeginRequest::encode(
            ['pool' => $this->pool, 'isolation' => $isolation, 'readonly' => $readonly],
            $this->encodePacker,
        );
```

- [ ] **Step 4: Add the `values:` parameter to the facade**

In `php/client/src/Ferro.php`, add `use Ferro\Client\Value\ValuePolicy;`, then append the parameter to BOTH factories and to `assemble()`. `connect()`:

```php
    /**
     * @param ?ValuePolicy $values a ready-made §9.1 decode policy, MUTUALLY EXCLUSIVE with `$types`
     *   (a ValuePolicy already embeds whichever options it was built with, so passing both would
     *   silently discard one — {@see Connection::__construct} rejects the combination). This exists
     *   for the M1-S8b Doctrine tier, whose whole type boundary is a custom ValuePolicy: without it
     *   the facade's resilient wiring (ReconnectLoop + FateClassifier + epoch tracking) would have
     *   to be rebuilt inside the driver package.
     */
    public static function connect(
        string $socketPath,
        string $pool = 'default',
        float $connectTimeout = 2.0,
        float $ioTimeout = 5.0,
        ?RetryPolicy $policy = null,
        ?TypePolicyOptions $types = null,
        ?ValuePolicy $values = null,
    ): Connection {
        $factory = static function () use ($socketPath, $connectTimeout, $ioTimeout): SessionInterface {
            $session = new Session(Transport::connectUnix($socketPath, $connectTimeout, $ioTimeout));
            $session->hello();
            return $session;
        };
        return self::assemble($factory, $pool, $policy, $types, $values);
    }
```

`connectTcp()` gains the identical `?ValuePolicy $values = null,` tail and forwards it the same way. `assemble()` gains it as a **REQUIRED** parameter, for the reason its own docblock already gives about `$types`:

```php
    private static function assemble(
        \Closure $factory,
        string $pool,
        ?RetryPolicy $policy,
        ?TypePolicyOptions $types,
        ?ValuePolicy $values,
    ): Connection {
```

```php
        return new Connection(
            session: $session,
            pool: $pool,
            reconnect: $loop,
            policy: $policy,
            fate: new FateClassifier($policy->retryReads),
            values: $values,
            types: $types,
        );
```

- [ ] **Step 5: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit tests/Client/ConnectionBeginIsolationTest.php && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS — **6 tests**: `testTheIsolationByteReachesTheBeginRequest` runs 4 data rows (the absent case plus `Isolation::cases()`, which has exactly 3 — `php/client/src/Protocol/Isolation.php`), plus `testTheReadonlyOnlyCallShapeStillWorks` and `testConnectionRefusesAValuePolicyAlongsideTypeOptions`. Whole suite green, PHPStan clean.

- [ ] **Step 6: Write the live test — a BEHAVIOURAL isolation proof, not a variable read-back**

SPEC §22.2 (s) records why `SELECT @@transaction_isolation` is the wrong assertion: `SET TRANSACTION …` without `SESSION`/`GLOBAL` applies to the NEXT transaction only and is not reflected in that variable. On PostgreSQL there IS a per-transaction observable — `current_setting('transaction_isolation')` inside the open transaction — and that is what this test uses.

Create `php/client/tests/Live/BeginIsolationLiveTest.php`:

```php
<?php // /php/client/tests/Live/BeginIsolationLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;
use Ferro\Protocol\Isolation;

/**
 * M1-S8b Task 3, live: the isolation byte really changes the transaction, on both engine families.
 *
 * PostgreSQL exposes the level of the CURRENTLY OPEN transaction through
 * `current_setting('transaction_isolation')`, so it can be asserted directly. MySQL cannot be
 * asserted that way — `SET TRANSACTION …` (the non-SESSION form the engine emits, deliberately) is
 * not reflected in `@@transaction_isolation`, which keeps reporting the session default with a
 * HYPHEN (`REPEATABLE-READ`). SPEC §22.2 (s) records that trap in full. The MySQL half therefore
 * asserts the level took by its EFFECT: under SERIALIZABLE a plain `SELECT` becomes a locking read,
 * so a row read inside the transaction cannot be updated by a second connection until it commits.
 */
final class BeginIsolationLiveTest extends LiveTestCase
{
    public function testPostgresBeginCarriesTheRequestedLevel(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());

        $c->begin(false, Isolation::Serializable);
        self::assertSame('serializable', $c->scalar("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        $c->begin(false, Isolation::RepeatableRead);
        self::assertSame('repeatable read', $c->scalar("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        // The absent case must NOT be silently coerced to a level — it must be the pool default.
        $c->begin();
        self::assertSame('read committed', $c->scalar("SELECT current_setting('transaction_isolation')"));
        $c->commit();
    }

    public function testMysqlSerializableTurnsAPlainSelectIntoALockingRead(): void
    {
        $pool = $this->requireMysqlPool();
        $a = $this->connectConnection(RetryPolicy::none(), $pool);
        $b = $this->connectConnection(RetryPolicy::none(), $pool);

        $a->exec('DROP TABLE IF EXISTS s8b_iso');
        $a->exec('CREATE TABLE s8b_iso (id INT PRIMARY KEY, v INT) ENGINE=InnoDB');
        $a->exec('INSERT INTO s8b_iso (id, v) VALUES (1, 1)');
        $a->exec('SET SESSION innodb_lock_wait_timeout = 1');

        $a->begin(false, Isolation::Serializable);
        self::assertSame(1, $a->scalar('SELECT v FROM s8b_iso WHERE id = 1'));

        // Connection B must now BLOCK and time out (1205), because A's plain SELECT took a shared
        // lock under SERIALIZABLE. Under the pool default (REPEATABLE READ) this update succeeds.
        $blocked = false;
        try {
            $b->exec('UPDATE s8b_iso SET v = 2 WHERE id = 1');
        } catch (\Ferro\Client\Error\RetryableException $e) {
            $blocked = true;
            self::assertSame(1205, $e->errno(), 'lock wait timeout is MySQL errno 1205');
        }
        self::assertTrue($blocked, 'SERIALIZABLE must make the read block a concurrent write');

        $a->commit();
        $a->exec('DROP TABLE s8b_iso');
    }
}
```

Create `php/client/tests/Live/ValuePolicyFacadeLiveTest.php` — the observable half of Task 3(b), which has no offline form:

```php
<?php // /php/client/tests/Live/ValuePolicyFacadeLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Decimal;
use Ferro\Ferro;

/**
 * M1-S8b Task 3(b) — `Ferro::connect(values:)` is not an inert knob.
 *
 * Asserted through the OBSERVABLE — what a DECIMAL cell decodes to — because the failure mode this
 * guards against is precisely a facade that accepts the argument and drops it on the floor.
 * `Ferro::assemble()`'s own docblock records that exact thing happening once already with `$types`:
 * "dropping $types from either assemble(...) call left PHPUnit green AND PHPStan level 9 clean while
 * Ferro::connect(types: …) became an inert public knob". A reflection parameter COUNT — which is
 * what plan v1 asserted here — passes over that bug.
 *
 * It is a LIVE test because there is no offline route: `assemble()` is private and `connect()` needs
 * a real socket, so the only honest way to observe the wiring is to decode a real cell.
 */
final class ValuePolicyFacadeLiveTest extends LiveTestCase
{
    public function testTheFacadeForwardsAValuePolicyAllTheWayToTheDecoder(): void
    {
        $raw = Ferro::connect(
            $this->socketPath,
            'default',
            2.0,
            5.0,
            RetryPolicy::none(),
            null,
            new RawStringValuePolicy(),
        );
        $got = $raw->scalar("SELECT CAST('1.50' AS numeric)");
        self::assertIsString($got, 'RawStringValuePolicy hands up the canonical wire text verbatim');
        self::assertSame('1.50', $got, 'the display scale survives — 1.50, not 1.5');

        // …and the SAME query on a connection built WITHOUT the argument still gets the §9.1 default,
        // which is what makes the assertion above a discriminator rather than a description of the
        // default behaviour.
        $def = Ferro::connect($this->socketPath, 'default', 2.0, 5.0, RetryPolicy::none());
        $obj = $def->scalar("SELECT CAST('1.50' AS numeric)");
        self::assertInstanceOf(Decimal::class, $obj, 'the default policy still decodes DECIMAL to an object');
        self::assertSame('1.50', (string) $obj);
    }
}
```

- [ ] **Step 7: Run the live tests**

```bash
cd /home/abdullak/projects/ferro/php/client && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/BeginIsolationLiveTest.php tests/Live/ValuePolicyFacadeLiveTest.php --fail-on-skipped
```
Expected: PASS (3 tests — 2 isolation, 1 value policy).

- [ ] **Step 8: MUTATION-PROVE the guards**

1. In `begin()`, replace `'isolation' => $isolation` with `'isolation' => null` in the `BeginRequest::encode(...)` payload. (Step 3 passes the ENUM CASE deliberately, so there is no `$isolation?->value` anywhere in `begin()` to mutate — v1's mutation text named code this plan never writes, and an implementer who "fixed" `begin()` to use `?->value` would be routing around the byte lock Step 3's comment exists to protect.) Re-run the unit test: RED on the three enum rows. Re-run the live test: RED on both (PG reports `read committed`; MySQL's update succeeds). Restore. **Both halves must go red** — the unit test alone would not prove the engine honoured the byte.
2. Swap `Isolation::RepeatableRead` and `Isolation::Serializable`'s integer values in `php/client/src/Protocol/Isolation.php`. Re-run `tests/Conformance/IsolationCrossLanguageTest` (expected RED — it locks both copies) **and** the PG live test (expected RED — the level is wrong). Restore. This is the §22.2 (w) cross-language lock finally being EXERCISED by a real caller rather than merely existing.
3. In `Ferro::assemble()`, drop `values: $values` from the `new Connection(...)` call (leave the parameter on `connect()`/`connectTcp()`/`assemble()` in place — that is the shape the bug actually takes). Re-run the OFFLINE suite and PHPStan: **both stay green**, which is the finding. Re-run `tests/Live/ValuePolicyFacadeLiveTest.php`: RED — the `numeric` cell comes back a `Ferro\Decimal` instead of `'1.50'`. Restore. This is the mutation that separates "the knob exists" from "the knob does something".

- [ ] **Step 9: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/client/src/Client/Connection.php \
        php/client/src/Ferro.php \
        php/client/tests/Client/ConnectionBeginIsolationTest.php \
        php/client/tests/Live/BeginIsolationLiveTest.php \
        php/client/tests/Live/ValuePolicyFacadeLiveTest.php
git commit -m "feat(m1-s8b): begin() carries an isolation level; Ferro::connect() accepts a ValuePolicy

The engine half (compose_begin_sql, dialect-aware, per-transaction forms only)
and the Isolation enum shipped in S8a. This is the missing parameter. Without
it, Doctrine's setTransactionIsolation() is a SILENT no-op on a transaction-mode
pool: its SET SESSION statement taints the checkout and hygiene wipes it before
the next BEGIN, so the app asks for SERIALIZABLE and gets READ COMMITTED.

Proven behaviourally on both families — current_setting() on PG, and on MySQL by
the effect (SERIALIZABLE turns a plain SELECT into a locking read), because
SET TRANSACTION is deliberately not reflected in @@transaction_isolation.

Ferro::connect() gains values: so the resilient facade can build a connection
with a custom decode policy — the whole Doctrine type boundary is one.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 4: The PG canonical-TEXT bind — widen it to PG's own text-input types, keep the sentinel discipline

**This is the drop-in blocker.** Stock DBAL's type layer hands the driver a PHP **string** for `datetime`, `date`, `time`, `decimal`, `json` and `guid` (hazard 18/40), every PHP string binds `TAG_TEXT` (hazard 56), and `bind::check_param`'s `Value::Text` arm accepts only `varchar/text/bpchar/name/unknown` (hazard 29). So on PostgreSQL **every dated, decimal, JSON or UUID INSERT through the stock type layer is refused pre-send**. MySQL has no such pre-flight (hazard 32), which is why this must be fixed and tested on PG specifically.

The fix is not "turn the pre-flight off". It is: a canonical `Value::Text` payload may bind to any PG type **whose TEXT INPUT SYNTAX is what that payload carries** — which is exactly what `pg_canonical_text_param!` already asserts for the seven tagged text types, and exactly what `pdo_pgsql` does for every parameter it sends. Two properties are preserved deliberately:

- **§19.3 directionality.** `check_param`'s arm delegates to `<PgText as ToSql>::accepts` (`bind.rs:473`), so `accepts` and the impl move in the SAME edit and remain bit-identical by construction. The failure that used to be a pre-send refusal becomes a real server-side `22007`/`22P02` `DbError` — a KNOWN fate (`NonRetryable`), never the unclassifiable band (hazard 33).
- **The sentinel discipline `s7_a_bare_text_never_binds_to_a_temporal_or_numeric_column` exists for** (hazard 31). A bare string `'infinity'` must still never become a PG timestamp sentinel. That is now enforced by a VALUE-aware gate in `check_param` — the same shape as the S8a integer range gate — rather than by refusing the whole tag. This is a REFUSAL keyed on the slot's type, not an inference of a tag from content: nothing here decides that `'2026-08-05'` "is a date".

**Files:**
- Modify: `engine/crates/ferro-backend-pg/src/bind.rs` (`PgText` at `:158-163`, `check_param` at `:449-500`, the test at `:1583-1608`)
- Test: `engine/crates/ferro-backend-pg/src/bind.rs` (unit) + `engine/crates/ferro-backend-pg/tests/pg_types_it.rs` (live)

**Interfaces:**
- Produces: `PgText` becomes an explicit `ToSql` impl (no longer generated by `pg_domain_aware_param!`) whose `accepts` is `<String as ToSql>::accepts(base) || is_text_input_target(base)`, and whose `to_sql` **and** `encode_format` BRANCH on the resolved base — verbatim text + `Format::Text` for the eight newly-widened targets, the existing delegated `<String as ToSql>` path (and therefore `Format::Binary`) for everything `String` already accepted. `bind::is_text_input_target(&Type) -> bool` is the shared predicate. `bind::check_param` gains a value-aware refusal for PG's special temporal/numeric literals against a temporal/numeric slot.
- Consumes: `ferro_proto::value::Value`; `resolve_domain(&Type) -> &Type` (`bind.rs`, the S8a domain unwrap); `tokio_postgres::types::{Type, Format, ToSql, IsNull}`.

> **v2 — why `to_sql` is TYPE-AWARE and not the one-liner it looks like it should be.** Plan v1 wrote `fn to_sql(&self, _ty: &Type, out)` as an unconditional `out.extend_from_slice(self.0.as_bytes())`. That is wire-correct for every type in the widened set, and it silently DESTROYS a guard. `s8a_every_arm_treats_a_domain_exactly_as_its_base` has three clauses, and clause (3) — the payload BYTES a domain and its base produce are identical — is falsifiable ONLY because `PgText` currently delegates to `<&str as ToSql>::to_sql`, which is the fixture's **one name-sensitive encoder**: it matches on `ty.name()` and prepends a version byte for `ltree`/`lquery`/`ltxtquery` (`postgres-protocol-0.6.12/src/types/mod.rs:1067-1072`, `buf.put_u8(1)`). `bind.rs:1130-1152` says so in its own words — the `ltree` fixture entry is "the ONLY thing standing between the payload half of §22.2 (g) and unfalsifiability", added by S8a's review round after clause (3) shipped unfalsifiable once. **MEASURED both directions:** at HEAD, mutating the macro's `to_sql` to use the unresolved `ty` gives `s8a_every_arm_treats_a_domain_exactly_as_its_base ... FAILED  left: [1, 120]  right: [120]`; with v1's Task 4 applied, the SAME mutation gives `... ok`. A future edit that drops `resolve_domain` from a `to_sql` — the exact §19.3 false-`Indeterminate` bug S8a shipped and fixed once — would pass the whole offline suite again. So the branch stays, `ltree` keeps its version byte, and Step 8 gains the re-mutation.

- [ ] **Step 1: Write the failing unit test**

In `engine/crates/ferro-backend-pg/src/bind.rs`, REPLACE the whole `s7_a_bare_text_never_binds_to_a_temporal_or_numeric_column` test (`:1583-1608`) with the pair below. The old test's PROPERTY survives — a sentinel is still refused — but it is now expressed as a value-aware gate instead of a whole-tag ban, which is what makes the DBAL tier possible at all.

```rust
    /// **M1-S8b: a canonical TEXT payload binds where PG's own TEXT INPUT SYNTAX is what it
    /// carries.** Stock Doctrine DBAL stringifies every temporal, decimal, JSON and UUID value in
    /// its type layer and binds it with `ParameterType::STRING`, so on PostgreSQL every such INSERT
    /// used to be refused pre-send. The widening is not a loosening of the §19.3 direction: the
    /// pre-flight still delegates to the very predicate `to_sql_checked` will apply, and the
    /// failure it now permits lands as a real server-side `22007`/`22P02` `DbError`, i.e. a KNOWN
    /// fate, never the unclassifiable band.
    #[test]
    fn s8b_bare_text_binds_to_every_type_whose_input_syntax_is_text() {
        for ty in [
            Type::TEXT,
            Type::VARCHAR,
            Type::BPCHAR,
            Type::NAME,
            Type::UNKNOWN,
            Type::NUMERIC,
            Type::DATE,
            Type::TIME,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::UUID,
            Type::JSON,
            Type::JSONB,
        ] {
            assert!(
                accepts(&Value::Text("2026-08-05".to_string()), &ty),
                "a canonical TEXT param must bind to {ty:?} — DBAL's type layer sends every \
                 temporal/decimal/json/uuid value as a string"
            );
        }
        // Still NARROW where text is NOT the input form: an integer, a boolean and a byte array
        // have binary-only bind paths here, and the S8a narrowing that made `serial` PKs work
        // must not be undone by this widening.
        for ty in [Type::INT2, Type::INT4, Type::INT8, Type::BOOL, Type::BYTEA, Type::FLOAT8] {
            assert!(
                !accepts(&Value::Text("42".to_string()), &ty),
                "a bare TEXT param must NOT bind to {ty:?}"
            );
        }
        // The domain unwrap still applies on the widened path (S8a). Built INLINE, in the `900_0xx`
        // synthetic band and NOT reusing an existing fixture oid, exactly as every other domain in
        // this file is built (`bind.rs:784, 804, 813, 880, 982, 1501`). There is no `domain_over()`
        // helper — plan v1 said there was, and `grep -rn "fn domain_over" engine/` returns nothing.
        let dom_date = Type::new(
            "dom_date".to_string(),
            900_020,
            tokio_postgres::types::Kind::Domain(Type::DATE),
            "public".to_string(),
        );
        assert!(accepts(&Value::Text("2026-08-05".to_string()), &dom_date));

        // **The FORMAT must resolve the domain too, and it is a separate branch from `to_sql`.**
        // `PgText::encode_format` decides whether PG reads these bytes as text or as a 4-byte binary
        // `date`; a version that tested the UNRESOLVED type would send `Format::Binary` for
        // `dom_date` while sending `Format::Text` for `date`, i.e. a wire bug reachable only through
        // a domain — and `s8a_every_arm_treats_a_domain_exactly_as_its_base` compares `to_sql`
        // BYTES, not formats, so nothing else in the tree would notice.
        assert!(matches!(
            PgText("2026-08-05".to_string()).encode_format(&dom_date),
            Format::Text
        ));
        assert!(matches!(
            PgText("2026-08-05".to_string()).encode_format(&Type::DATE),
            Format::Text
        ));
        // …and the types that were ALREADY accepted keep the binary format they have always had, so
        // this task's regression surface on the shipping path is empty.
        let dom_text = Type::new(
            "dom_text_fmt".to_string(),
            900_021,
            tokio_postgres::types::Kind::Domain(Type::TEXT),
            "public".to_string(),
        );
        for ty in [Type::TEXT, Type::VARCHAR, dom_text] {
            assert!(
                matches!(PgText("x".to_string()).encode_format(&ty), Format::Binary),
                "an already-accepted string type must keep its binary format: {ty:?}"
            );
        }
    }

    /// **The sentinel discipline, preserved — as a VALUE-aware gate, not a whole-tag ban.** PG's
    /// input parser turns the bare words `infinity`, `now`, `today`, … into real values, so a
    /// string that happens to hold one must not become a timestamp sentinel just because it landed
    /// in a temporal slot. Same for `NaN` / `Infinity` against `numeric`. The refusal names the
    /// canonical tag route (`Ferro\Date`, `Ferro\NaiveTimestamp`, `Ferro\Decimal`), which IS how a
    /// caller expresses a sentinel on purpose.
    ///
    /// This is a REFUSAL keyed on the SLOT's type, never an inference of a TAG from content:
    /// nothing here decides that `'2026-08-05'` "is a date".
    #[test]
    fn s8b_a_bare_text_sentinel_is_still_refused_for_a_temporal_or_numeric_slot() {
        for lit in ["infinity", "-infinity", "Infinity", "NOW", "today", "Tomorrow", "yesterday", "epoch", "allballs"] {
            for ty in [Type::DATE, Type::TIME, Type::TIMESTAMP, Type::TIMESTAMPTZ] {
                let err = check_param(&Value::Text(lit.to_string()), &ty)
                    .expect_err("a PG special datetime literal must be refused for a temporal slot");
                // **Case matters and the token is spelled ONCE, here and in Step 4's message.**
                // `str::contains` is case-sensitive; plan v1 asserted lowercase `"special"` against
                // a message containing only `SPECIAL`, which fails on the first of these 36
                // iterations against a CORRECT implementation (measured), and whose cheapest
                // "repair" is to delete the assertion — at which point the guard stops
                // distinguishing "refused because it is a SPECIAL literal" from "refused because
                // the whole TEXT tag is banned", the ONE distinction this rewrite exists to make.
                assert!(
                    err.contains("SPECIAL"),
                    "the refusal must say WHY, got {err:?}"
                );
                // …and it must be ACTIONABLE: name the SLOT type and the tagged escape route.
                // `contains("SPECIAL")` alone passes for any message containing the word.
                assert!(
                    err.contains(ty.name()),
                    "the refusal must name the slot type, got {err:?}"
                );
                assert!(
                    err.contains("Ferro\\Date"),
                    "the refusal must name the tagged route that DOES bind a sentinel, got {err:?}"
                );
            }
        }
        for lit in ["NaN", "nan", "Infinity", "-Infinity"] {
            check_param(&Value::Text(lit.to_string()), &Type::NUMERIC)
                .expect_err("a numeric special literal must be refused for a numeric slot");
        }
        // …and the SAME strings are perfectly ordinary values in a text column, which is the whole
        // reason the gate is keyed on the slot rather than on the content.
        for lit in ["infinity", "NaN", "today"] {
            check_param(&Value::Text(lit.to_string()), &Type::TEXT)
                .expect("a special literal is just a string in a text column");
        }
        // …and a sentinel that arrived TAG-INTACT still binds, exactly as before.
        assert!(accepts(&Value::Date("infinity".into()), &Type::DATE));
    }
```

Both tests live in the existing `mod tests` block, so they can call the private `accepts`, `check_param`, `PgText` and `is_text_input_target` directly. `Format` is already imported there (`s7_newtypes_send_text_format` matches on it).

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p ferro-backend-pg s8b_bare_text -- --nocapture
cargo test -p ferro-backend-pg s8b_a_bare_text_sentinel -- --nocapture
```
Expected: FAIL. The first with `a canonical TEXT param must bind to Numeric` (the current `accepts` is `String`'s list); the second with `expect_err` panicking because `check_param` currently returns `Err` for the WRONG reason (the whole tag is refused, so the message is `canonical Text cannot bind to PG type date`), which is what makes `err.contains("SPECIAL")` fail. Both messages are what confirm the test is measuring the new behaviour and not the old.

- [ ] **Step 3: Replace `PgText` with an explicit, widened impl**

In `engine/crates/ferro-backend-pg/src/bind.rs`, DELETE the `pg_domain_aware_param! { … PgText wraps String }` block (`:155-163`) and put this in its place:

```rust
/// The PG types a canonical `TAG_TEXT` payload may bind to IN ADDITION to the string types
/// `String`'s own `accepts` already covers (`varchar`, `text`, `bpchar`, `name`, `unknown`, plus the
/// name-keyed `citext`/`ltree`/`lquery`/`ltxtquery`).
///
/// **The membership rule is one sentence: PG's TEXT INPUT SYNTAX for this type is exactly what a
/// canonical text payload carries** — which is the same rule the seven `pg_canonical_text_param!`
/// newtypes assert per tag, and the same thing `pdo_pgsql` relies on for every parameter it sends.
/// `int2`/`int4`/`int8`, `bool`, `float4`/`float8` and `bytea` are deliberately NOT here: the
/// canonical wire forms for those are `I64`/`Bool`/`F64`/`Bytes`, which have their own narrow
/// binary bind paths (the S8a `PgInt` narrowing is what made a `serial` primary key work), and
/// admitting text there would disable those pre-flights for no caller that exists.
///
/// A function rather than a `const [Type; 8]`, matching the array-literal-plus-`contains` idiom
/// `pg_canonical_text_param!` already uses (`[$(Type::$ty),+].contains(resolve_domain(ty))`). It
/// takes the ALREADY-RESOLVED base: all three call sites resolve first, and each for its own
/// reason, so resolving again in here would hide which of them forgot to.
fn is_text_input_target(base: &Type) -> bool {
    [
        Type::NUMERIC,
        Type::DATE,
        Type::TIME,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
        Type::UUID,
        Type::JSON,
        Type::JSONB,
    ]
    .contains(base)
}

/// `TEXT` → the string types (unchanged, delegated, BINARY) **plus** [`is_text_input_target`]'s
/// eight, written verbatim in PG's **text** wire format.
///
/// **Why this is not `pg_domain_aware_param! { PgText wraps String }` any more (M1-S8b).** Two
/// reasons, and the second is a wire bug waiting to happen:
///
///  1. `<String as ToSql>::accepts` admits only the string types, so a stock Doctrine DBAL insert —
///     whose type layer stringifies every `datetime`/`date`/`time`/`decimal`/`json`/`guid` value and
///     binds it as `ParameterType::STRING` — was refused pre-send on EVERY such column. MySQL has no
///     equivalent pre-flight, so the same driver "worked" there and hard-failed here.
///  2. That macro delegates `encode_format`, and `<String as ToSql>` takes the trait's
///     `Format::Binary` default. Widening `accepts` alone would therefore hand PG the UTF-8 bytes of
///     `2026-08-05` and tell it they are a 4-byte BINARY `date`.
///
/// **Both `to_sql` and `encode_format` BRANCH on the resolved base, and the branch is load-bearing
/// in two independent ways.**
///
/// *Correctness:* it is NOT true that "the text-format bytes are the binary-format bytes for every
/// string type this already accepted". `<&str as ToSql>::accepts` (postgres-types-0.2.14
/// `src/lib.rs:1148-1153`) also admits `citext`, `ltree`, `lquery` and `ltxtquery` BY NAME, and for
/// the last three the BINARY form is `0x01 || text` while the text form is bare text
/// (`<&str as ToSql>::to_sql` matches on `ty.name()`). Text == binary holds for
/// `varchar`/`text`/`bpchar`/`name`/`unknown`/`citext` and for nothing else. Keeping the delegated
/// path for those types means this task's regression surface on everything that already worked is
/// EMPTY, rather than "believed harmless".
///
/// *Falsifiability:* that same name-sensitive encoder is what makes clause (3) of
/// [`s8a_every_arm_treats_a_domain_exactly_as_its_base`] able to fail at all. The `ltree` entry in
/// [`every_target_type`] was added by S8a's review round precisely because every other entry is
/// bound by an impl that ignores its `Type`, which left the payload-BYTES clause unfalsifiable. A
/// type-blind `to_sql` here would make `ltree` and `dom_of_ltree` write identical bytes BY
/// CONSTRUCTION and quietly revert that fix — measured: the mutation that is RED at HEAD goes GREEN.
///
/// **§19.3 is intact.** [`check_param`]'s `Value::Text` arm delegates to THIS `accepts`, so the
/// pre-flight is bit-identical to the predicate `to_sql_checked` applies — the two cannot drift. And
/// what the widening admits is not an unclassifiable failure: a malformed date text now fails
/// SERVER-side with a real `22007` `DbError`, which `is_session_fatal` reads as non-fatal and
/// `error_map` classifies `NonRetryable`. The direction the rule forbids — a pre-flight LOOSER than
/// its impl — is not what changed; both moved together, in this edit.
#[derive(Debug)]
struct PgText(String);

impl ToSql for PgText {
    /// Verbatim canonical text for the widened targets (nothing is re-rendered, re-parsed or
    /// validated — a round trip through a date/numeric type would lose a display scale or a
    /// sentinel); the unchanged delegated path, **against the RESOLVED base**, for everything
    /// `String` already accepted.
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let base = resolve_domain(ty);
        if is_text_input_target(base) {
            out.extend_from_slice(self.0.as_bytes());
            return Ok(IsNull::No);
        }
        <String as ToSql>::to_sql(&self.0, base, out)
    }

    fn accepts(ty: &Type) -> bool {
        let base = resolve_domain(ty);
        <String as ToSql>::accepts(base) || is_text_input_target(base)
    }

    /// Text format for THIS param only (the RESULT format stays binary, hazard 17) — and only for
    /// the widened targets. The string types keep the `Format::Binary` they have always had, which
    /// is what `ltree`'s `0x01 || text` payload requires.
    fn encode_format(&self, ty: &Type) -> Format {
        let base = resolve_domain(ty);
        if is_text_input_target(base) {
            Format::Text
        } else {
            <String as ToSql>::encode_format(&self.0, base)
        }
    }

    to_sql_checked!();
}
```

- [ ] **Step 4: Add the value-aware sentinel gate to `check_param`**

In `check_param` (`bind.rs:449`), after the existing `let base = resolve_domain(ty);` line (`:491`) and alongside the S8a integer range gate, add:

```rust
    // ---- M1-S8b: the VALUE-aware half of the widened TEXT bind. ------------------------------
    // `PgText::accepts` now admits the types whose input syntax is text, which is what makes the
    // Doctrine tier possible. But PG's parser also turns a handful of BARE WORDS into real values —
    // `infinity` into a timestamp sentinel, `now`/`today` into a clock reading, `NaN` into a numeric
    // — so a string that happens to hold one must not acquire that meaning merely by landing in a
    // temporal or numeric slot. A caller that MEANS a sentinel says so with the tag
    // (`Ferro\Date('infinity')`, `Ferro\Decimal('NaN')`), which still binds.
    //
    // This is a REFUSAL keyed on the SLOT's type. It is NOT content sniffing: nothing here infers a
    // TAG from a payload, and the identical string is accepted without comment for a text column.
    if let Value::Text(s) = v {
        let refused = match *base {
            Type::DATE | Type::TIME | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                is_pg_special_datetime_literal(s)
            }
            Type::NUMERIC => is_pg_special_numeric_literal(s),
            _ => false,
        };
        if refused {
            return Err(format!(
                "canonical Text {s:?} is one of PostgreSQL's SPECIAL input literals for {}, so \
                 binding it as a bare string would silently give it that meaning; send it with its \
                 own canonical tag instead (Ferro\\Date / Ferro\\Time / Ferro\\NaiveTimestamp / \
                 Ferro\\Decimal), which binds it deliberately",
                base.name()
            ));
        }
    }
```

and add the two predicates next to `value_kind` (`bind.rs:600`):

```rust
/// PostgreSQL's special date/time input literals (`datetime.c`'s `deltatktbl`/`datetktbl` special
/// entries), case-insensitively. Deliberately a CLOSED list rather than a parse attempt: the
/// question is not "is this a valid date" — PG answers that itself, loudly, server-side — but "would
/// binding this bare string silently MEAN something other than a literal date".
fn is_pg_special_datetime_literal(s: &str) -> bool {
    const SPECIALS: [&str; 8] = [
        "infinity",
        "-infinity",
        "+infinity",
        "now",
        "today",
        "tomorrow",
        "yesterday",
        "epoch",
    ];
    let t = s.trim();
    SPECIALS.iter().any(|k| t.eq_ignore_ascii_case(k)) || t.eq_ignore_ascii_case("allballs")
}

/// PostgreSQL's special `numeric` input literals. `NaN` compares unequal to everything including
/// itself, and `Infinity` is unbounded — either one silently acquired by a bare string is a value
/// no application asked for.
fn is_pg_special_numeric_literal(s: &str) -> bool {
    const SPECIALS: [&str; 5] = ["nan", "infinity", "-infinity", "+infinity", "inf"];
    let t = s.trim();
    SPECIALS.iter().any(|k| t.eq_ignore_ascii_case(k))
        || t.eq_ignore_ascii_case("-inf")
        || t.eq_ignore_ascii_case("+inf")
}
```

- [ ] **Step 5: Run the unit tests — they must now pass**

```bash
cargo test -p ferro-backend-pg bind:: -- --nocapture
cargo fmt --check && cargo clippy -p ferro-backend-pg --all-targets -- -D warnings
```
Expected: PASS, including the pre-existing lockstep proofs — none of which this change may weaken. The names, verified against `bind.rs` (v1 named a test that does not exist, `accepts_mirrors_value_to_boxed`, which would have reported "0 tests" as a pass):

| test | line | what it pins |
|---|---|---|
| `accepts_mirrors_boxed_binding` | 648 | `accepts` and `value_to_boxed` were flipped together |
| `s7_accepts_is_narrow_per_tag` | 1223 | the per-tag narrowness table |
| `s7_accepts_is_never_looser_than_the_boxed_impl` | 1446 | the §19.3 direction, over the cross product |
| `s8a_i64_binds_to_every_integer_width_and_f64_to_both_floats` | 673 | the S8a narrowing |
| `s8a_domain_nesting_is_bounded_and_the_bound_refuses` | — | the resolver bound |
| **`s8a_every_arm_treats_a_domain_exactly_as_its_base`** | **1490** | **see below — it must be RE-MUTATED, not merely re-run** |

`s7_accepts_is_narrow_per_tag` currently has no `(Value::Text, …)` row at all, so nothing there needs updating; if a future edit adds one for a widened type, UPDATE it and record it in the test's docblock rather than silencing the test.

**`s8a_every_arm_treats_a_domain_exactly_as_its_base` is the one that needs more than a green tick.** After this task `PgText` is no longer generated by `pg_domain_aware_param!`, so the mutation that test's own docblock records ("`pg_domain_aware_param`'s `to_sql` uses the UNRESOLVED `ty`") no longer reaches the `ltree` fixture at all — the macro then fronts only `PgBool` and `PgBytes`, whose inner impls ignore the `Type`. Re-mutating it is Step 8 item 4, and it is mandatory: without it this task silently converts a guard S8a's review round repaired into one that cannot fail.

- [ ] **Step 6: Write the live PG test — a DBAL-SHAPED insert of stringified values**

Append to `engine/crates/ferro-backend-pg/tests/pg_types_it.rs`:

```rust
/// **M1-S8b: the shape stock Doctrine DBAL actually sends.** Its type layer converts every
/// `datetime`/`date`/`time`/`decimal`/`json`/`guid` value to a PHP STRING and binds it with
/// `ParameterType::STRING`, which reaches the engine as `TAG_TEXT`. Before this task every one of
/// these columns refused the bind pre-send, so a Doctrine application could not INSERT a dated or
/// decimal row on PostgreSQL at all.
///
/// Falsifiable: revert `PgText::accepts` to `<String as ToSql>::accepts` and each row below fails
/// with `parameter 1: canonical Text cannot bind to PG type …`; revert `encode_format` to the
/// delegated (binary) default and the rows fail SERVER-side with a decode error instead — which is
/// why both halves of Step 3 are one edit.
#[tokio::test]
async fn s8b_a_stringified_dbal_value_binds_to_its_typed_column() {
    let Some(url) = pg_url() else {
        println!("skip: FERRO_TEST_PG_URL unset");
        return;
    };
    let pool = pool_for(&url);
    let mut co = pool.checkout().await.expect("checkout");
    co.exec("DROP TABLE IF EXISTS s8b_bind", &[]).await.expect("drop");
    co.exec(
        "CREATE TABLE s8b_bind (d date, t time, ts timestamp, tz timestamptz, \
         n numeric(12,4), u uuid, j jsonb)",
        &[],
    )
    .await
    .expect("create");

    co.exec(
        "INSERT INTO s8b_bind (d, t, ts, tz, n, u, j) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        &[
            Value::Text("2026-08-05".into()),
            Value::Text("13:45:07".into()),
            Value::Text("2026-08-05 13:45:07".into()),
            Value::Text("2026-08-05 13:45:07+00".into()),
            Value::Text("1.2500".into()),
            Value::Text("0b7f0a5e-1f4a-4b7d-8f4e-2a9c1d3e5f60".into()),
            Value::Text("{\"a\":1}".into()),
        ],
    )
    .await
    .expect("a stringified DBAL row must INSERT");

    let got = co
        .query("SELECT n::text, d::text FROM s8b_bind", &[])
        .await
        .expect("read back");
    assert_eq!(got.rows.len(), 1);

    // And the SENTINEL discipline still holds on the same connection: a bare string sentinel is
    // refused with a KNOWN fate, pre-send, naming the tagged route.
    let err = co
        .exec("INSERT INTO s8b_bind (ts) VALUES ($1)", &[Value::Text("infinity".into())])
        .await
        .expect_err("a bare TEXT sentinel must still be refused");
    let msg = format!("{err:?}");
    assert!(msg.contains("SPECIAL"), "the refusal must name the reason, got {msg}");

    co.exec("DROP TABLE s8b_bind", &[]).await.expect("cleanup");
}
```

Use the file's existing `pg_url()` / `pool_for()` helpers verbatim; if the file's helper names differ, use the ones it already defines rather than inventing new ones. The `skip:` line is mandatory — `ci/assert-no-skips.sh` uses it to catch a live lane that made no database contact.

- [ ] **Step 7: Run the live test**

```bash
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
  cargo test -p ferro-backend-pg --test pg_types_it s8b_a_stringified_dbal_value -- --nocapture
```
Expected: PASS.

- [ ] **Step 8: MUTATION-PROVE the guards**

1. Revert `PgText::accepts` to `<String as ToSql>::accepts(resolve_domain(ty))`. Re-run both unit tests and the live test. Expected: RED everywhere (`canonical Text cannot bind to PG type date`). Restore.
2. Change `PgText::encode_format`'s widened branch to `Format::Binary` (i.e. make the whole method `<String as ToSql>::encode_format(&self.0, resolve_domain(ty))`). Re-run the unit test: RED on the two new `dom_date`/`DATE` format assertions. Re-run the LIVE test: RED with a server-side decode failure on the `date` column. Restore. Both halves matter: v1 claimed the unit tests would stay green here, which is only true without the format assertions Step 1 now carries.
3. Delete the `if let Value::Text(s) = v` gate from `check_param`. Re-run `s8b_a_bare_text_sentinel_is_still_refused_for_a_temporal_or_numeric_slot` and the live test's sentinel half. Expected: RED in both (and the live `INSERT` would now SUCCEED, storing a real `infinity` — the silent miscast the gate exists to prevent). Restore.
4. **Re-arm the domain byte-equality clause.** In `PgText::to_sql`, change the delegated branch to pass the UNRESOLVED type: `<String as ToSql>::to_sql(&self.0, ty, out)`. Re-run `cargo test -p ferro-backend-pg s8a_every_arm_treats_a_domain_exactly_as_its_base -- --nocapture`. Expected: **RED**, with `resolving a domain must change what the bind is CHECKED against, never what it WRITES: Text("x") against ltree  left: [1, 120]  right: [120]` — the `dom_of_ltree` domain writes no version byte because `<&str as ToSql>::to_sql` keys on `ty.name()` and `"dom_of_ltree"` is not `"ltree"`. Restore. **If this comes back GREEN, STOP**: it means `PgText::to_sql` became type-blind again and clause (3) of §22.2 (g)'s payload half is unfalsifiable — the precise state plan v1 would have shipped, and the state S8a's review round already had to repair once.
5. In `PgText::encode_format`, change the branch condition to test the UNRESOLVED type (`is_text_input_target(ty)` instead of `is_text_input_target(base)`), leaving `to_sql` correct. Re-run `s8b_bare_text_binds_to_every_type_whose_input_syntax_is_text`. Expected: RED on the `dom_date` format assertion — a domain over `date` would be sent as BINARY while its base is sent as TEXT. Restore. This one exists because `s8a_every_arm_treats_a_domain_exactly_as_its_base` compares `to_sql` BYTES and never inspects a format, so nothing else in the tree can see this bug.

- [ ] **Step 9: Full offline gate + commit**

```bash
cd /home/abdullak/projects/ferro
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git add engine/crates/ferro-backend-pg/src/bind.rs \
        engine/crates/ferro-backend-pg/tests/pg_types_it.rs
git commit -m "feat(m1-s8b): a canonical TEXT param binds where PG's own text INPUT syntax is what it carries

Stock Doctrine DBAL stringifies every datetime/date/time/decimal/json/guid value
in its type layer and binds it as ParameterType::STRING, which reaches us as
TAG_TEXT. PgText accepted only the string types, so on PostgreSQL every such
INSERT was refused pre-send — the drop-in blocker. MySQL has no equivalent
pre-flight, so the same driver worked there and hard-failed here.

PgText is now an explicit impl: accepts = String's list + the eight types whose
TEXT input syntax is what a canonical payload carries, and both to_sql and
encode_format BRANCH on the resolved base — verbatim text + Format::Text for the
eight, the unchanged delegated path (and Format::Binary) for everything String
already took. The macro delegated encode_format to String's BINARY default, which
would have told PG that the UTF-8 bytes of a date were a 4-byte binary date; and a
type-BLIND to_sql would have been wrong for ltree/lquery/ltxtquery (binary there
is 0x01 || text) and would have un-armed the ltree fixture that makes the domain
byte-equality clause of s8a_every_arm_treats_a_domain_exactly_as_its_base able to
fail at all.

SS19.3 direction is intact — check_param delegates to this same accepts, so the
two moved in one edit — and the sentinel discipline the old narrowness protected
is preserved as a VALUE-aware gate: PG's special literals (infinity, now, NaN, ...)
are still refused for a temporal/numeric slot, naming the tagged route, while the
identical string is an ordinary value in a text column.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 5: The package, the gate lanes, and a WALKING-SKELETON driver proven live through the real `DriverManager`

Everything after this task refines a driver that already answers real queries. This task creates `php/doctrine-dbal`, wires its four lanes into BOTH `ci/local-gate.sh` and `.github/workflows/ci.yml` (hazard 70 — forgetting either makes the whole package a silent no-op), and ships the smallest driver that satisfies the SPI end to end on both engine families. It also ships the platform SELECTION rule (given a kind and a version string, pick a stock platform); Task 6 owns where that version string comes from.

**Files:**
- Create: `php/doctrine-dbal/composer.json`, `php/doctrine-dbal/phpunit.xml.dist`, `php/doctrine-dbal/phpstan.neon.dist`
- Create: `php/doctrine-dbal/src/{Driver,DriverOptions,Connection,Statement,Result,PlatformVersion,FixedVersion}.php`, `php/doctrine-dbal/src/Exception/{DriverException,BackendFamilyUnknown,NoIdentityValue}.php`
- Create: `php/doctrine-dbal/tests/Unit/{PlatformVersionTest,DriverOptionsTest,DriverQuoteTest}.php`, `php/doctrine-dbal/tests/Live/{DbalLiveTestCase,DriverSmokeLiveTest}.php`
- Modify: `ci/local-gate.sh`, `.github/workflows/ci.yml`

**Interfaces:**
- Produces:
  - `Ferro\DBAL\Driver implements Doctrine\DBAL\Driver` — `connect(array $params): Ferro\DBAL\Connection`, `getDatabasePlatform(ServerVersionProvider $vp): AbstractPlatform`, `getExceptionConverter(): Doctrine\DBAL\Driver\API\ExceptionConverter`; plus `kind(): ?string` (the pool family learned at the last `connect()`, `'postgres'` / `'mysql'` / null).
  - `Ferro\DBAL\DriverOptions::fromParams(array $params): self` with public readonly `?string $socketPath`, `?string $host`, `int $port`, `string $pool`, `bool $readonly`, `float $connectTimeout`, `float $ioTimeout`.
  - `Ferro\DBAL\PlatformVersion::normalise(string $kind, string $raw): string` and `::platformFor(string $kind, string $rawVersion): AbstractPlatform`; constants `KIND_POSTGRES = 'postgres'`, `KIND_MYSQL = 'mysql'`.
  - `Ferro\DBAL\FixedVersion implements Doctrine\DBAL\ServerVersionProvider`.
  - `Ferro\DBAL\Connection implements Doctrine\DBAL\Driver\Connection` — `__construct(Ferro\Client\Connection $ferro, string $poolName, string $poolKind, bool $readonly)`, the 9 SPI methods, `runPrepared(string $sql, list<mixed> $params): Doctrine\DBAL\Driver\Result`, `ferro(): Ferro\Client\Connection`, `poolName(): string`, `poolKind(): string`. **This 4-parameter signature is FINAL from here on** — every later task consumes it verbatim (Task 6 needs the name for its loud failure, Tasks 7-13 all construct it).
  - `Ferro\DBAL\Statement implements Doctrine\DBAL\Driver\Statement`.
  - `Ferro\DBAL\Result implements Doctrine\DBAL\Driver\Result`; `Result::buffered(array $cols, array $rows, int $affected): self`.
  - `Ferro\DBAL\Exception\DriverException extends Doctrine\DBAL\Driver\AbstractException`; `::fromFerro(\Ferro\Client\Error\FerroException $e): self`.
- Consumes: Task 1's `Ferro\Client\Connection::{fetchRaw, poolInfo}`; Task 3's `Ferro\Ferro::connect(..., values:)`; `Ferro\Client\RetryPolicy::none()`; `Ferro\Client\Value\RawStringValuePolicy`; `Ferro\Protocol\PoolInfo{name, kind, serverVersion}`.

- [ ] **Step 1: Create the package skeleton**

Create `php/doctrine-dbal/composer.json`:

```json
{
    "name": "ferro/doctrine-dbal-driver",
    "description": "Ferro driver for Doctrine DBAL 4 — config-only adoption of the Ferro engine",
    "type": "library",
    "license": "Apache-2.0",
    "repositories": [
        { "type": "path", "url": "../client" }
    ],
    "require": {
        "php": ">=8.2",
        "doctrine/dbal": "^4.0",
        "ferro/client": "@dev"
    },
    "require-dev": {
        "phpstan/phpstan": "^2.0",
        "phpunit/phpunit": "^11.0"
    },
    "autoload": { "psr-4": { "Ferro\\DBAL\\": "src/" } },
    "autoload-dev": {
        "psr-4": {
            "Ferro\\DBAL\\Tests\\": "tests/",
            "Ferro\\Tests\\": "../client/tests/"
        }
    },
    "scripts": { "test": "phpunit", "stan": "phpstan analyse src --level 9" },
    "config": { "sort-packages": true }
}
```

Three things here are load-bearing and were each verified by installing them:
- `"ferro/client": "@dev"` — **not `"*"`**. `php/client/composer.json` has no `version` field, so composer derives `dev-<branch>` and `"*"` fails with `does not match your minimum-stability`. The inline `@dev` flag fixes it WITHOUT loosening stability for `doctrine/dbal`, which stays on stable 4.4.4.
- `"url": "../client"` resolves relative to THIS composer.json and installs `vendor/ferro/client` as a SYMLINK, so client edits are live in the driver.
- `"Ferro\\Tests\\": "../client/tests/"` in **autoload-dev** — composer does not install a path dependency's own autoload-dev, but the ROOT package's is always honoured and the symlink makes the directory real. This is what lets the driver's live tests reuse `Ferro\Tests\Live\LiveTestCase` (whose `dirname(__DIR__, 4)` repo-root walk still resolves, because the file physically lives at `php/client/tests/Live/`). PHPUnit still only discovers this package's `tests/`.

Create `php/doctrine-dbal/phpunit.xml.dist` (identical shape to `php/client`'s — ONE suite; the live/offline split is a runtime skip plus a path-scoped `--fail-on-skipped`, never a phpunit group):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<phpunit xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:noNamespaceSchemaLocation="vendor/phpunit/phpunit/phpunit.xsd"
         bootstrap="vendor/autoload.php"
         colors="true"
         cacheDirectory=".phpunit.cache">
    <testsuites>
        <testsuite name="ferro-doctrine-dbal">
            <directory>tests</directory>
        </testsuite>
    </testsuites>
</phpunit>
```

Create `php/doctrine-dbal/phpstan.neon.dist`:

```
parameters:
    level: 9
    paths:
        - src
```

Install:

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && composer install --no-interaction
```
Expected: `doctrine/dbal 4.4.x`, `doctrine/deprecations`, `psr/cache`, `psr/log`, and `ferro/client` as a symlink. **Verify charter rule 7 immediately**: `git diff --stat php/client/composer.json` must be EMPTY — the dependency points driver → client and never back.

- [ ] **Step 2: Write the failing unit tests**

Create `php/doctrine-dbal/tests/Unit/PlatformVersionTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/PlatformVersionTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MariaDB110700Platform;
use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\PlatformVersion;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5 — the platform fork, with the two OPPOSITE requirements that make a single uniform
 * "version normaliser" ship a wrong SQL dialect.
 *
 * MEASURED against the live containers and fed through the STOCK abstract drivers:
 *   pg      `PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-…`  -> THROWS InvalidPlatformVersion
 *   pg      `17.10 (Debian 17.10-1.pgdg13+1)`                          -> PostgreSQL120Platform
 *   mysql   `8.4.11`                                                   -> MySQL84Platform
 *   mariadb `11.8.8-MariaDB-ubu2404`                                   -> MariaDB110700Platform
 *   mariadb `11.8.8`   (i.e. the suffix stripped)                      -> MySQL84Platform  *** WRONG ***
 *
 * So normalisation is MANDATORY on the PG path (the stock regex is anchored at `^` and our cached
 * string starts with the literal word "PostgreSQL") and FORBIDDEN on the MySQL path (MariaDB is
 * detected ONLY by `stripos($version, 'mariadb')`). The literal strings below are the ones ferrod
 * actually caches — `SELECT version()`, verbatim and unnormalised.
 */
final class PlatformVersionTest extends TestCase
{
    private const PG_LIVE = 'PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu, '
        . 'compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit';
    private const MYSQL_LIVE = '8.4.11';
    private const MARIADB_LIVE = '11.8.8-MariaDB-ubu2404';

    public function testTheLivePostgresStringSelectsThePostgresPlatform(): void
    {
        self::assertInstanceOf(
            PostgreSQL120Platform::class,
            PlatformVersion::platformFor(PlatformVersion::KIND_POSTGRES, self::PG_LIVE),
        );
    }

    public function testTheLiveMysqlAndMariadbStringsSelectDIFFERENTPlatforms(): void
    {
        self::assertInstanceOf(
            MySQL84Platform::class,
            PlatformVersion::platformFor(PlatformVersion::KIND_MYSQL, self::MYSQL_LIVE),
        );
        $maria = PlatformVersion::platformFor(PlatformVersion::KIND_MYSQL, self::MARIADB_LIVE);
        self::assertInstanceOf(MariaDB110700Platform::class, $maria);
        self::assertNotInstanceOf(
            MySQL84Platform::class,
            $maria,
            'stripping "-MariaDB" would silently select the MySQL dialect for a MariaDB server',
        );
    }

    /** The normaliser must touch the PG string and leave the MySQL-family string BYTE-IDENTICAL. */
    public function testNormalisationIsPostgresOnly(): void
    {
        self::assertStringStartsWith(
            '17.10',
            PlatformVersion::normalise(PlatformVersion::KIND_POSTGRES, self::PG_LIVE),
        );
        self::assertSame(
            self::MARIADB_LIVE,
            PlatformVersion::normalise(PlatformVersion::KIND_MYSQL, self::MARIADB_LIVE),
            'the MySQL-family string is load-bearing and must pass through verbatim',
        );
        self::assertSame(
            self::MYSQL_LIVE,
            PlatformVersion::normalise(PlatformVersion::KIND_MYSQL, self::MYSQL_LIVE),
        );
    }

    /** An unknown family is a LOUD failure, never a default platform (SPEC §14). */
    public function testAnUnknownFamilyThrows(): void
    {
        $this->expectException(BackendFamilyUnknown::class);
        PlatformVersion::platformFor('sqlite', '3.45');
    }
}
```

Create `php/doctrine-dbal/tests/Unit/DriverOptionsTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/DriverOptionsTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\DBAL\DriverOptions;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5 — configuration arrives through `driverOptions`, NOT through SPEC §14's `ferro`
 * key. That is not a style preference: `Doctrine\DBAL\Driver::connect()` is `@phpstan-param Params`,
 * `Params` is a SEALED array shape with no `ferro` key, and reading `$params['ferro']['pool']`
 * MEASURED as two `nullCoalesce.offset` errors at PHPStan level 9 — which is a charter
 * Definition-of-Done gate. `driverOptions?: array<mixed>` is the sanctioned slot. §14 is amended by
 * Task 14.
 */
final class DriverOptionsTest extends TestCase
{
    public function testItReadsTheSocketPoolAndReadonlyFlagOutOfDriverOptions(): void
    {
        $o = DriverOptions::fromParams([
            'driverOptions' => ['socket' => '/run/ferro/dev.sock', 'pool' => 'main', 'readonly' => true],
        ]);
        self::assertSame('/run/ferro/dev.sock', $o->socketPath);
        self::assertSame('main', $o->pool);
        self::assertTrue($o->readonly);
    }

    /** `unix_socket` is a first-class DBAL param and naturally carries the ferrod socket path. */
    public function testUnixSocketParamIsAccepted(): void
    {
        $o = DriverOptions::fromParams(['unix_socket' => '/run/ferro/dev.sock']);
        self::assertSame('/run/ferro/dev.sock', $o->socketPath);
        self::assertSame('default', $o->pool, 'the pool defaults to "default"');
        self::assertFalse($o->readonly, 'a connection is a WRITE connection unless declared otherwise');
    }

    /** TCP is the FERRO_ADDR fallback; host+port travel through the ordinary DBAL params. */
    public function testHostAndPortSelectTheTcpTransport(): void
    {
        $o = DriverOptions::fromParams(['host' => '127.0.0.1', 'port' => 7777]);
        self::assertNull($o->socketPath);
        self::assertSame('127.0.0.1', $o->host);
        self::assertSame(7777, $o->port);
    }

    /** Neither a socket nor a host is a configuration error worth reporting as itself. */
    public function testNoTransportAtAllThrowsWithAnActionableMessage(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/unix_socket|driverOptions/');
        DriverOptions::fromParams([]);
    }

    /** A wrongly-typed option is refused, not silently coerced (level 9 narrows, but so do we). */
    public function testAWronglyTypedOptionIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        DriverOptions::fromParams(['driverOptions' => ['socket' => 42, 'pool' => 'main']]);
    }
}
```

Create `php/doctrine-dbal/tests/Unit/DriverQuoteTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/DriverQuoteTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\Connection;
use Ferro\DBAL\PlatformVersion;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5 — `quote()` is PER-FAMILY, and the two branches are locked against the stock
 * platform accessors rather than restated.
 *
 * `AbstractPlatform::quoteStringLiteral()` doubles the single quote; `AbstractMySQLPlatform`
 * OVERRIDES it to escape backslashes first, because MySQL treats `\` as an escape character inside
 * a string literal. A driver that emitted the PostgreSQL form on a MySQL connection would mangle
 * every value containing a backslash — silently, since the result is still valid SQL.
 *
 * `quote()` must not need a platform (and therefore must not need a server version), because it has
 * to keep working on a pool whose version is unknown. So the two branches live in the driver, and
 * this test is what stops them drifting from Doctrine's.
 */
final class DriverQuoteTest extends TestCase
{
    /** @return array<string, array{0: string, 1: string}> the values that discriminate the two forms */
    public static function values(): array
    {
        return [
            'plain' => ["o'brien", "o'brien"],
            'backslash' => ['C:\\path\\to', 'C:\\path\\to'],
            'both' => ["a'b\\c", "a'b\\c"],
        ];
    }

    private function driverConn(string $kind): Connection
    {
        // `Ferro\Client\Connection` is FINAL (php/client/src/Client/Connection.php:43), so it is
        // constructed directly over a scripted-nothing FakeSession rather than subclassed. `quote()`
        // never sends a frame, so nothing needs queueing.
        //
        // Four arguments: ($ferro, $poolName, $poolKind, $readonly). The pool NAME ('p') is unused
        // by `quote()` and is there because it is part of the constructor from this task onward —
        // see the `__construct` docblock in Step 5 for why it is not added later.
        return new Connection(new FerroClientConnection(new FakeSession(), 'default'), 'p', $kind, false);
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('values')]
    public function testEachFamilyMatchesItsStockPlatform(string $in, string $same): void
    {
        self::assertSame($in, $same); // the provider carries the value once; this pins the shape

        self::assertSame(
            (new PostgreSQL120Platform())->quoteStringLiteral($in),
            $this->driverConn(PlatformVersion::KIND_POSTGRES)->quote($in),
        );
        self::assertSame(
            (new MySQL84Platform())->quoteStringLiteral($in),
            $this->driverConn(PlatformVersion::KIND_MYSQL)->quote($in),
        );
    }

    /** …and the two families genuinely differ, so neither branch is dead code. */
    public function testTheTwoFamiliesDifferOnABackslash(): void
    {
        self::assertNotSame(
            $this->driverConn(PlatformVersion::KIND_POSTGRES)->quote('a\\b'),
            $this->driverConn(PlatformVersion::KIND_MYSQL)->quote('a\\b'),
        );
    }
}
```

with the two extra imports `use Ferro\Client\Connection as FerroClientConnection;` and `use Ferro\Tests\Support\FakeSession;` — the latter reachable because this package's `autoload-dev` maps `Ferro\Tests\` to `../client/tests/`.

- [ ] **Step 3: Run them and watch them fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit
```
Expected: FAIL — `Error: Class "Ferro\DBAL\PlatformVersion" not found`.

- [ ] **Step 4: Implement `FixedVersion`, `PlatformVersion`, `BackendFamilyUnknown`, `DriverOptions`**

Create `php/doctrine-dbal/src/FixedVersion.php`:

```php
<?php // /php/doctrine-dbal/src/FixedVersion.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\ServerVersionProvider;

/**
 * A `ServerVersionProvider` over a string we already hold. DBAL ships its own
 * (`Doctrine\DBAL\Connection\StaticServerVersionProvider`), but it is an internal detail of the
 * wrapper `Connection` rather than a documented extension point, and this is eight lines.
 */
final class FixedVersion implements ServerVersionProvider
{
    public function __construct(private readonly string $version) {}

    public function getServerVersion(): string
    {
        return $this->version;
    }
}
```

Create `php/doctrine-dbal/src/Exception/BackendFamilyUnknown.php`:

```php
<?php // /php/doctrine-dbal/src/Exception/BackendFamilyUnknown.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * The driver could not decide WHICH SQL dialect to emit. SPEC §14 is explicit that this must fail
 * loudly rather than fall back to a default platform: a wrong platform is a wrong SQL grammar for
 * every subsequent statement, which is a class of bug a clean error is not.
 */
final class BackendFamilyUnknown extends AbstractException
{
    public static function forKind(string $kind): self
    {
        return new self(sprintf(
            'Ferro: the pool advertises backend family "%s", for which this driver has no Doctrine '
            . 'platform. M1 supports "postgres" and "mysql" (MariaDB reports "mysql" and is '
            . 'distinguished by its version string). No default platform is guessed, because a '
            . 'wrong platform means a wrong SQL dialect for every statement.',
            $kind,
        ));
    }

    public static function beforeConnect(string $version): self
    {
        return new self(sprintf(
            'Ferro: the Doctrine platform was requested before any connection was opened, and the '
            . 'configured serverVersion "%s" does not name a backend family. Either remove the '
            . '`serverVersion` connection parameter so the driver learns the family from the engine '
            . 'handshake, or write a family-bearing version string (e.g. "PostgreSQL 17.10" or '
            . '"11.8.8-MariaDB"). No family is guessed: PostgreSQL and MySQL are different SQL '
            . 'dialects.',
            $version,
        ));
    }
}
```

Create `php/doctrine-dbal/src/PlatformVersion.php`:

```php
<?php // /php/doctrine-dbal/src/PlatformVersion.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\AbstractMySQLDriver;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Ferro\DBAL\Exception\BackendFamilyUnknown;

/**
 * Turns (backend family, raw `version()` string) into a STOCK Doctrine platform.
 *
 * **We choose the STRING; Doctrine chooses the PLATFORM.** The version ladders
 * (`>= 8.4 MySQL84Platform`, `>= 11.7 MariaDB110700Platform`, `>= 12.0 PostgreSQL120Platform`, …)
 * live in DBAL's own abstract drivers and move between DBAL releases; restating them here would be
 * a second source of truth that silently rots. So this class delegates to
 * `AbstractPostgreSQLDriver::getDatabasePlatform()` / `AbstractMySQLDriver::getDatabasePlatform()`
 * through a platform-only anonymous subclass, and its whole job is the ONE transform those two
 * cannot do for themselves.
 *
 * **That transform is asymmetric, and getting it uniform is the measured way to ship a wrong SQL
 * dialect.** `ferrod` caches the backend's own `version()` output VERBATIM (`pools.rs`'s
 * `VERSION_SQL`, and `PoolInfo`'s own docblock says normalising it is the consuming tier's job):
 *
 *  - **PostgreSQL** answers `PostgreSQL 17.10 (Debian …) on x86_64-…`, and the stock parser is
 *    ANCHORED (`/^(?P<major>\d+)…/`). Measured: that string throws `InvalidPlatformVersion` on
 *    EVERY connection. Stripping the leading product name is mandatory.
 *  - **MySQL/MariaDB** answer `8.4.11` and `11.8.8-MariaDB-ubu2404`, and MariaDB is detected ONLY by
 *    `stripos($version, 'mariadb') !== false`. Measured: normalising `11.8.8-MariaDB-ubu2404` down
 *    to `11.8.8` selects `MySQL84Platform` — a MariaDB server driven with MySQL's grammar, silently.
 *    So the MySQL-family string passes through BYTE-IDENTICAL.
 *
 * Charter rule 6 is intact: no platform is subclassed, no SQL is generated here. We select.
 */
final class PlatformVersion
{
    /** The `PoolInfo.kind` wire values (`PoolKind::wire_name()` in `ferrod`). Never nil. */
    public const KIND_POSTGRES = 'postgres';
    public const KIND_MYSQL = 'mysql';

    /**
     * Strip PostgreSQL's leading product name and NOTHING else; leave every other family verbatim.
     *
     * Minimal by design: `'17.10 (Debian 17.10-1.pgdg13+1)'` is measured to parse fine, so there is
     * no reason to extract a bare `major.minor` and every reason not to (each extra rule is another
     * chance to discard a suffix that turns out to be load-bearing, which is exactly what the
     * MariaDB case is).
     */
    public static function normalise(string $kind, string $raw): string
    {
        if ($kind !== self::KIND_POSTGRES) {
            return $raw;
        }
        return preg_replace('/^\s*PostgreSQL\s+/i', '', $raw) ?? $raw;
    }

    /** @throws BackendFamilyUnknown */
    public static function platformFor(string $kind, string $rawVersion): AbstractPlatform
    {
        $provider = new FixedVersion(self::normalise($kind, $rawVersion));
        return match ($kind) {
            self::KIND_POSTGRES => self::postgres()->getDatabasePlatform($provider),
            self::KIND_MYSQL => self::mysql()->getDatabasePlatform($provider),
            default => throw BackendFamilyUnknown::forKind($kind),
        };
    }

    /**
     * Derive the family from a version string alone — the ONLY option on the
     * platform-before-connect path (`Doctrine\DBAL\Connection::getDatabasePlatform()` builds a
     * static provider from `$params['serverVersion']` and never asks the driver connection).
     * Returns null when the string names no family; the caller must then FAIL, never guess.
     */
    public static function familyFromVersion(string $version): ?string
    {
        if (stripos($version, 'postgres') !== false) {
            return self::KIND_POSTGRES;
        }
        if (stripos($version, 'mariadb') !== false || stripos($version, 'mysql') !== false) {
            return self::KIND_MYSQL;
        }
        return null;
    }

    private static function postgres(): AbstractPostgreSQLDriver
    {
        return new class extends AbstractPostgreSQLDriver {
            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): DriverConnection
            {
                throw new \LogicException('platform-only delegate: this driver never connects');
            }
        };
    }

    private static function mysql(): AbstractMySQLDriver
    {
        return new class extends AbstractMySQLDriver {
            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): DriverConnection
            {
                throw new \LogicException('platform-only delegate: this driver never connects');
            }
        };
    }
}
```

Create `php/doctrine-dbal/src/DriverOptions.php`:

```php
<?php // /php/doctrine-dbal/src/DriverOptions.php
declare(strict_types=1);
namespace Ferro\DBAL;

/**
 * The typed read of DBAL's `$params`. One responsibility: turn `array<string,mixed>` into narrowed
 * scalars, loudly, so nothing downstream has to guess.
 *
 * **Configuration lives in `driverOptions`, not in a top-level `ferro` key.** SPEC §14's example
 * shows `'ferro' => ['pool' => …]`, but `Doctrine\DBAL\Driver::connect()` is `@phpstan-param Params`
 * and `Params` is a SEALED array shape with no such key — reading it MEASURED as two
 * `nullCoalesce.offset` errors at PHPStan level 9, which is a charter Definition-of-Done gate (and
 * Symfony's `doctrine.dbal` config rejects unknown top-level keys too). `driverOptions?: array<mixed>`
 * is the sanctioned slot. §14 is amended in the same slice.
 *
 * Recognised keys:
 *   `unix_socket` (top level) or `driverOptions.socket` — the ferrod UDS path.
 *   `host` + `port` (top level) — the `FERRO_ADDR` TCP fallback.
 *   `driverOptions.pool` — the engine pool name; defaults to `default`.
 *   `driverOptions.readonly` — declare EVERY statement on this connection a read for §19.3 fate
 *      purposes. Off by default and deliberately explicit: the DBAL SPI carries no read/write
 *      signal and charter rule 6 forbids inferring one, so the safe default is "write". This is the
 *      charter-compliant shape of §14's `read_pool` idea — a second, explicitly-configured
 *      connection, never inference.
 *   `driverOptions.connect_timeout` / `driverOptions.io_timeout` — seconds, floats.
 */
final class DriverOptions
{
    private function __construct(
        public readonly ?string $socketPath,
        public readonly ?string $host,
        public readonly int $port,
        public readonly string $pool,
        public readonly bool $readonly,
        public readonly float $connectTimeout,
        public readonly float $ioTimeout,
    ) {}

    /** @param array<string,mixed> $params */
    public static function fromParams(array $params): self
    {
        $raw = $params['driverOptions'] ?? [];
        if (!is_array($raw)) {
            throw new \InvalidArgumentException('Ferro: `driverOptions` must be an array.');
        }
        /** @var array<string,mixed> $opts */
        $opts = $raw;

        $socket = self::optString($opts, 'socket');
        if ($socket === null && isset($params['unix_socket']) && is_string($params['unix_socket'])) {
            $socket = $params['unix_socket'];
        }
        $host = null;
        if (isset($params['host']) && is_string($params['host'])) {
            $host = $params['host'];
        }
        $port = 0;
        if (isset($params['port']) && is_int($params['port'])) {
            $port = $params['port'];
        }
        if ($socket === null && $host === null) {
            throw new \InvalidArgumentException(
                'Ferro: no engine transport configured. Set `unix_socket` (or '
                . '`driverOptions.socket`) to the ferrod socket path, or `host`+`port` for the TCP '
                . 'fallback. Ferro holds no database credentials in PHP — the DSN lives in the '
                . 'engine (SPEC §12 / D8).',
            );
        }

        return new self(
            $socket,
            $host,
            $port === 0 ? 7777 : $port,
            self::optString($opts, 'pool') ?? 'default',
            self::optBool($opts, 'readonly'),
            self::optFloat($opts, 'connect_timeout') ?? 2.0,
            self::optFloat($opts, 'io_timeout') ?? 5.0,
        );
    }

    /** @param array<string,mixed> $opts */
    private static function optString(array $opts, string $key): ?string
    {
        if (!array_key_exists($key, $opts)) {
            return null;
        }
        $v = $opts[$key];
        if (!is_string($v)) {
            throw new \InvalidArgumentException("Ferro: driverOptions.$key must be a string.");
        }
        return $v;
    }

    /** @param array<string,mixed> $opts */
    private static function optBool(array $opts, string $key): bool
    {
        if (!array_key_exists($key, $opts)) {
            return false;
        }
        $v = $opts[$key];
        if (!is_bool($v)) {
            throw new \InvalidArgumentException("Ferro: driverOptions.$key must be a bool.");
        }
        return $v;
    }

    /** @param array<string,mixed> $opts */
    private static function optFloat(array $opts, string $key): ?float
    {
        if (!array_key_exists($key, $opts)) {
            return null;
        }
        $v = $opts[$key];
        if (!is_float($v) && !is_int($v)) {
            throw new \InvalidArgumentException("Ferro: driverOptions.$key must be a number.");
        }
        return (float) $v;
    }
}
```

- [ ] **Step 5: Implement the walking-skeleton `DriverException`, `Result`, `Statement`, `Connection`, `Driver`**

Create `php/doctrine-dbal/src/Exception/DriverException.php`:

```php
<?php // /php/doctrine-dbal/src/Exception/DriverException.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Ferro\Client\Error\CarriesErrorPayload;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;

/**
 * EVERY Ferro client exception crossing the driver boundary becomes one of these. That is not
 * tidiness: `Doctrine\DBAL\Connection::executeQuery()` catches exactly `Doctrine\DBAL\Driver\Exception`,
 * so anything else escapes DBAL's conversion entirely and reaches the application raw, past every
 * `catch (Doctrine\DBAL\Exception)` an app or framework has.
 *
 * It carries the pair the STOCK converters read: the 5-character SQLSTATE in `getSQLState()` (which
 * `API\PostgreSQL\ExceptionConverter` keys on) and the integer vendor errno in `getCode()` (which
 * `API\MySQL\ExceptionConverter` keys on). PostgreSQL never supplies an errno — its identity IS the
 * SQLSTATE — so `getCode()` is 0 there, which is exactly what the PG table expects.
 *
 * `branch()` preserves the §9.2 fate the wire declared, because DBAL's tree has no third branch and
 * {@see \Ferro\DBAL\ExceptionConverter} needs it to mint one.
 */
final class DriverException extends AbstractException
{
    private function __construct(
        string $message,
        ?string $sqlState,
        int $code,
        private readonly ?int $branch,
        ?\Throwable $previous,
    ) {
        parent::__construct($message, $sqlState, $code, $previous);
    }

    public static function fromFerro(FerroException $e): self
    {
        $sqlstate = null;
        $errno = null;
        $branch = null;
        if ($e instanceof RetryableException
            || $e instanceof IndeterminateException
            || $e instanceof NonRetryableException
        ) {
            /** @var RetryableException|IndeterminateException|NonRetryableException $e */
            $sqlstate = $e->sqlstate();
            $errno = $e->errno();
            $branch = $e->branch();
        }
        return new self($e->getMessage(), $sqlstate, $errno ?? 0, $branch, $e);
    }

    /** A driver-side failure with no wire payload (a bad option, an unreadable value). */
    public static function local(string $message, ?\Throwable $previous = null): self
    {
        return new self($message, null, 0, null, $previous);
    }

    /** The §9.2 branch byte (1 Retryable, 2 Indeterminate, 3 NonRetryable), or null. */
    public function branch(): ?int
    {
        return $this->branch;
    }
}
```

Note the `use CarriesErrorPayload;` import is only for the docblock reference and may be dropped if PHPStan flags it as unused.

Create `php/doctrine-dbal/src/Result.php` (walking-skeleton form — Task 8 completes it, Task 12 adds the streamed mode):

```php
<?php // /php/doctrine-dbal/src/Result.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Exception\InvalidColumnIndex;

/**
 * A DBAL driver result over Ferro's `{cols, rows, affected}`.
 *
 * `rowCount()` is the TERMINAL's `affected`, never `count($this->rows)` — they are different
 * numbers, and conflating them reports 0 for an `UPDATE` that changed rows (the exact bug the
 * research spike shipped).
 */
final class Result implements ResultInterface
{
    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     */
    private function __construct(
        private array $cols,
        private array $rows,
        private readonly int $affected,
        private int $cursor = 0,
    ) {}

    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     */
    public static function buffered(array $cols, array $rows, int $affected): self
    {
        return new self($cols, $rows, $affected);
    }

    /** @return list<mixed>|false */
    public function fetchNumeric(): array|false
    {
        return $this->rows[$this->cursor++] ?? false;
    }

    /** @return array<string,mixed>|false */
    public function fetchAssociative(): array|false
    {
        $row = $this->fetchNumeric();
        if ($row === false) {
            return false;
        }
        return array_combine($this->cols, $row);
    }

    public function fetchOne(): mixed
    {
        return FetchUtils::fetchOne($this);
    }

    /** @return list<list<mixed>> */
    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    /** @return list<array<string,mixed>> */
    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    /** @return list<mixed> */
    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    public function rowCount(): int
    {
        return $this->affected;
    }

    public function columnCount(): int
    {
        return count($this->cols);
    }

    public function getColumnName(int $index): string
    {
        return $this->cols[$index] ?? throw InvalidColumnIndex::new($index);
    }

    public function free(): void
    {
        $this->rows = [];
        $this->cols = [];
        $this->cursor = 0;
    }
}
```

Create `php/doctrine-dbal/src/Statement.php` (walking skeleton — Task 7 replaces `bindValue`'s body with the full `(ParameterType, PHP type)` mapping):

```php
<?php // /php/doctrine-dbal/src/Statement.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use Ferro\DBAL\Exception\DriverException;

/**
 * A prepared statement. Ferro has no separate PREPARE round trip at this tier — the engine owns
 * statement caching — so `prepare()` records the SQL and `execute()` sends it with the bound
 * parameters as one `EXEC`.
 *
 * **Positional parameters only.** DBAL 4 hands a named `:name` straight to the driver, and the
 * stock `Driver\Mysqli\Statement::bindValue` simply `assert(is_int($param))`; refusing them loudly
 * here is exactly as capable as the stock mysqli driver, and a silent misbind would be worse.
 */
final class Statement implements StatementInterface
{
    /** @var array<int,mixed> 1-based, exactly as DBAL numbers them */
    private array $values = [];

    public function __construct(
        private readonly Connection $conn,
        private readonly string $sql,
    ) {}

    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING): void
    {
        if (!is_int($param)) {
            throw DriverException::local(
                'Ferro: named parameters are not supported; use positional `?` placeholders '
                . '(Doctrine expands named parameters above the driver when you pass them to '
                . 'executeQuery()/executeStatement()).',
            );
        }
        $this->values[$param] = $value;
    }

    public function execute(): ResultInterface
    {
        ksort($this->values);
        return $this->conn->runPrepared($this->sql, array_values($this->values));
    }
}
```

Create `php/doctrine-dbal/src/Connection.php`:

```php
<?php // /php/doctrine-dbal/src/Connection.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Ferro\Client\Connection as FerroConnection;
use Ferro\Client\Error\FerroException;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Exception\NoIdentityValue;

/**
 * The EXECUTION layer. Everything above it — Grammar, the platforms, the schema managers, the
 * migrations runner — stays stock (charter rule 6); this class only decides HOW a statement reaches
 * the engine.
 *
 * **Every statement is declared a WRITE for §19.3 fate purposes** unless the whole connection was
 * configured `driverOptions.readonly`. The DBAL 4 SPI carries no read/write signal — `executeQuery()`
 * with no parameters reaches `query()`, `executeStatement()` with no parameters reaches `exec()`,
 * and BOTH use the same `prepare()`+`execute()` path when parameters are present, so
 * `executeQuery('INSERT … RETURNING id')` is indistinguishable from a SELECT — and charter rule 6
 * forbids inferring one from the SQL text. Declaring "write" costs a lost READ its retryability
 * (it is reported `Indeterminate` rather than `Retryable`); declaring "read" would cost a lost
 * WRITE its honesty, which is the failure this project exists to refuse.
 */
final class Connection implements DriverConnection
{
    /**
     * **The pool NAME is here from Task 5 on, not added later.** Nothing in this task reads it, but
     * Task 6's `ServerVersionUnavailable` message must name the pool (a driver may serve several)
     * and Tasks 7-13 all construct this class. Threading a parameter through afterwards would mean
     * editing every call site those tasks wrote — and a 4-argument call against a 3-argument
     * constructor does not fail where you would expect: PHP binds the first three and DISCARDS the
     * fourth, so under `strict_types` it surfaces as a `TypeError` naming the WRONG parameter
     * (hazard 81).
     */
    public function __construct(
        private readonly FerroConnection $ferro,
        private readonly string $poolName,
        private readonly string $poolKind,
        private readonly bool $readonly,
    ) {}

    /** The underlying Ferro client connection — also what {@see getNativeConnection} returns. */
    public function ferro(): FerroConnection
    {
        return $this->ferro;
    }

    /** The `driverOptions.pool` this connection was opened against. */
    public function poolName(): string
    {
        return $this->poolName;
    }

    /** `postgres` or `mysql`, from `HELLO_ACK`. Never nil. */
    public function poolKind(): string
    {
        return $this->poolKind;
    }

    public function prepare(string $sql): StatementInterface
    {
        return new Statement($this, $sql);
    }

    public function query(string $sql): ResultInterface
    {
        return $this->runPrepared($sql, []);
    }

    public function exec(string $sql): int
    {
        try {
            return $this->ferro->fetchRaw($sql, [], $this->readonly, false)['affected'];
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * The ONE place a statement with parameters reaches the engine. `Statement::execute()` and
     * {@see query} both land here, which is what keeps the fate declaration and (from Task 10) the
     * pinned-transaction routing in a single place.
     *
     * @param list<mixed> $params
     */
    public function runPrepared(string $sql, array $params): ResultInterface
    {
        try {
            $raw = $this->ferro->fetchRaw($sql, $params, $this->readonly, true);
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
        return Result::buffered($raw['cols'], $raw['rows'], $raw['affected']);
    }

    /**
     * D5: present for compatibility, discouraged — parameters are the supported path.
     *
     * **It is per-FAMILY, and that is not cosmetic.** `AbstractPlatform::quoteStringLiteral()`
     * doubles the single quote, but `AbstractMySQLPlatform` overrides it to escape BACKSLASHES
     * first, because MySQL treats `\` as an escape character inside a string literal. Emitting the
     * PostgreSQL form on a MySQL connection would mangle every value containing a backslash. The
     * family is always known (`PoolInfo.kind` is never nil), so this needs no platform and
     * therefore no server version — which matters, because `quote()` must keep working on a pool
     * whose version is unknown. `DriverQuoteTest` locks both branches against the stock platform
     * accessors, so a DBAL change to either goes red here.
     */
    public function quote(string $value): string
    {
        if ($this->poolKind === PlatformVersion::KIND_MYSQL) {
            $value = str_replace('\\', '\\\\', $value);
        }
        return "'" . str_replace("'", "''", $value) . "'";
    }

    public function lastInsertId(): int|string
    {
        $id = $this->ferro->lastInsertId();
        if ($id === null) {
            throw NoIdentityValue::forKind($this->poolKind);
        }
        return $id;
    }

    public function beginTransaction(): void
    {
        try {
            $this->ferro->begin($this->readonly);
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    public function commit(): void
    {
        try {
            $this->ferro->commit();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    public function rollBack(): void
    {
        try {
            $this->ferro->rollBack();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    public function getServerVersion(): string
    {
        return $this->ferro->poolInfo()?->serverVersion ?? '';
    }

    /**
     * SPEC §14's documented break: this is a `Ferro\Client\Connection`, not a `PDO`. Anything doing
     * `pg_escape_string($native, …)` or `$native->real_escape_string()` will fatal — that is the
     * incompatibility, and it is listed in `docs/known-incompatibilities.md`.
     */
    public function getNativeConnection(): FerroConnection
    {
        return $this->ferro;
    }
}
```

Create `php/doctrine-dbal/src/Exception/NoIdentityValue.php`:

```php
<?php // /php/doctrine-dbal/src/Exception/NoIdentityValue.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * `Doctrine\DBAL\Driver\Connection::lastInsertId(): int|string` is non-nullable and DBAL 4 requires
 * it to THROW when there is no identity value (UPGRADE.md: "Connection::lastInsertId() throws an
 * exception when there's no identity value").
 *
 * On PostgreSQL that is ALWAYS: the protocol carries no such field, and Ferro refuses to emulate it
 * with a follow-up `lastval()` because on a transaction-mode pool that lands on a DIFFERENT
 * connection and returns a silently wrong key. The message names the two working answers.
 */
final class NoIdentityValue extends AbstractException
{
    public static function forKind(string $kind): self
    {
        return new self(
            $kind === \Ferro\DBAL\PlatformVersion::KIND_POSTGRES
                ? 'Ferro: PostgreSQL reports no generated key on the wire, and Ferro will not '
                    . 'emulate lastInsertId() with a follow-up query — on a transaction-mode pool '
                    . 'that runs on a different connection and returns a wrong key. Use '
                    . '`INSERT … RETURNING id`, or configure Doctrine ORM to use the SEQUENCE '
                    . 'identity strategy on PostgreSQL.'
                : 'Ferro: the last statement reported no generated key. lastInsertId() reflects the '
                    . 'MOST RECENT statement and is cleared by a statement that fails, so read it '
                    . 'immediately after a successful INSERT into an AUTO_INCREMENT column.',
        );
    }
}
```

Create `php/doctrine-dbal/src/Driver.php`:

```php
<?php // /php/doctrine-dbal/src/Driver.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\API\PostgreSQL\ExceptionConverter as PostgreSQLExceptionConverter;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\Exception\DriverException;
use Ferro\Ferro;

/**
 * The `ferro/doctrine-dbal-driver` entry point. Configure it with `driverClass`:
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'   => Ferro\DBAL\Driver::class,
 *     'unix_socket'   => '/run/ferro/app.sock',
 *     'driverOptions' => ['pool' => 'main'],
 * ]],
 * ```
 *
 * `DriverManager::createDriver()` does `return new $driverClass();`, so this class MUST have a
 * no-argument constructor and everything arrives through `$params`.
 */
final class Driver implements DriverInterface
{
    /** The backend family of the LAST pool this driver connected to, or null before any connect. */
    private ?string $kind = null;

    /** @param array<string,mixed> $params */
    public function connect(#[\SensitiveParameter] array $params): Connection
    {
        $o = DriverOptions::fromParams($params);
        // RetryPolicy::none() is deliberate and is what `Ferro\Client\Connection::begin()`'s own
        // docblock tells a driver to use: DBAL (or the application above it) owns the retry
        // decision, and the client's autocommit read-retry must not double up with it.
        // RawStringValuePolicy hands up the canonical wire text verbatim — the driver-native shape
        // a DBAL type layer expects. Task 9 replaces it with the DBAL-specific policy.
        $ferro = $o->socketPath !== null
            ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, new RawStringValuePolicy())
            : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, new RawStringValuePolicy());

        $info = $ferro->poolInfo();
        if ($info === null) {
            throw DriverException::local(sprintf(
                'Ferro: the engine does not advertise a pool named "%s". Configured pools come from '
                . 'ferrod\'s FERRO_POOLS; check `driverOptions.pool`.',
                $o->pool,
            ));
        }
        $this->kind = $info->kind;
        return new Connection($ferro, $o->pool, $info->kind, $o->readonly);
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        $version = $versionProvider->getServerVersion();
        // The family the handshake told us, when we have one. Otherwise this is the
        // platform-before-connect path (`$params['serverVersion']` short-circuits the connection
        // entirely), where the version string is the only signal there is.
        $kind = $this->kind ?? PlatformVersion::familyFromVersion($version);
        if ($kind === null) {
            throw BackendFamilyUnknown::beforeConnect($version);
        }
        return PlatformVersion::platformFor($kind, $version);
    }

    public function getExceptionConverter(): ExceptionConverterInterface
    {
        // Task 11 replaces this with Ferro\DBAL\ExceptionConverter, which intercepts the §9.2
        // Indeterminate branch and then delegates to the STOCK per-family converter.
        return new PostgreSQLExceptionConverter();
    }

    /** The backend family learned at the last {@see connect}, or null. */
    public function kind(): ?string
    {
        return $this->kind;
    }
}
```

- [ ] **Step 6: Run the unit tests + PHPStan — they must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS (every test in `tests/Unit`) and PHPStan clean at level 9. If PHPStan flags `Ferro\Client\Error\CarriesErrorPayload` as an unused import in `DriverException`, delete the import.

- [ ] **Step 7: Write the live smoke test — with a HARD contact assertion**

Create `php/doctrine-dbal/tests/Live/DbalLiveTestCase.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/DbalLiveTestCase.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Tests\Live\LiveTestCase;

/**
 * The base class for every S8b live test. It inherits `Ferro\Tests\Live\LiveTestCase` wholesale —
 * reached through this package's own `autoload-dev` mapping of `Ferro\Tests\` to `../client/tests/`,
 * which works because the path repository installs `vendor/ferro/client` as a SYMLINK.
 *
 * Inheriting rather than re-implementing is deliberate: `LiveTestCase::waitUntilReady()` does a full
 * HELLO plus a real `SELECT 1` against the real upstream before any test body runs, and that
 * readiness probe is the STRUCTURAL proof of database contact for the PHP tier. A hand-rolled base
 * class that merely started a process and connected a socket would let "N tests passed" mean zero
 * database contact — which is precisely how the upstream DBAL suite reports green against
 * in-memory SQLite (see Task 14).
 *
 * Cost note: `LiveTestCase` spawns and reaps a ferrod PER TEST (~0.5 s). That is acceptable for this
 * package's own conformance tier; the curated UPSTREAM subset in Task 14 launches one ferrod per
 * RUN instead.
 */
abstract class DbalLiveTestCase extends LiveTestCase
{
    /** @param array<string,mixed> $extraOptions */
    protected function dbal(string $pool = 'default', array $extraOptions = []): DbalConnection
    {
        $conn = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => $pool] + $extraOptions,
        ]);
        // THE CONTACT ASSERTION. Without it, a driver that quietly fell back to something else
        // would still make every assertion below pass.
        self::assertInstanceOf(
            FerroClientConnection::class,
            $conn->getNativeConnection(),
            'this DBAL connection is not a Ferro one — the test would be measuring the wrong engine',
        );
        return $conn;
    }
}
```

`$this->socketPath` is already a `protected string` property on `LiveTestCase` (`php/client/tests/Live/LiveTestCase.php:49`), set to the per-class socket the daemon was launched on — so no change to the client's harness is needed. Do NOT copy `LiveTestCase` into this package: its `locateFerrod()` walks `dirname(__DIR__, 4)` to find the repo root, which is correct only because the file physically lives at `php/client/tests/Live/`.

Create `php/doctrine-dbal/tests/Live/DriverSmokeLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/DriverSmokeLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;

/**
 * M1-S8b Task 5 — the walking skeleton, driven through the REAL
 * `Doctrine\DBAL\DriverManager::getConnection(['driverClass' => …])` against a real ferrod on real
 * PostgreSQL and real MySQL. If this passes, the SPI wiring is right and every later task is a
 * refinement of something that already works.
 */
final class DriverSmokeLiveTest extends DbalLiveTestCase
{
    public function testAQueryAStatementAndATransactionAllWorkOnPostgres(): void
    {
        $c = $this->dbal();
        self::assertInstanceOf(PostgreSQLPlatform::class, $c->getDatabasePlatform());

        $c->executeStatement('DROP TABLE IF EXISTS s8b_smoke');
        $c->executeStatement('CREATE TABLE s8b_smoke (id int primary key, note text)');
        self::assertSame(
            2,
            $c->executeStatement('INSERT INTO s8b_smoke (id, note) VALUES (1, \'a\'), (2, \'b\')'),
            'executeStatement returns the terminal affected count, not count($rows)',
        );

        self::assertSame(
            [['id' => 1, 'note' => 'a'], ['id' => 2, 'note' => 'b']],
            $c->fetchAllAssociative('SELECT id, note FROM s8b_smoke ORDER BY id'),
        );
        self::assertSame('b', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = ?', [2]));
        self::assertSame([[1, 'a'], [2, 'b']], $c->fetchAllNumeric('SELECT id, note FROM s8b_smoke ORDER BY id'));

        $c->beginTransaction();
        $c->executeStatement('UPDATE s8b_smoke SET note = \'z\' WHERE id = 1');
        self::assertSame('z', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = 1'));
        $c->rollBack();
        self::assertSame('a', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = 1'), 'the rollback reached the pinned tx');

        $c->executeStatement('DROP TABLE s8b_smoke');
    }

    public function testTheSameDriverServesAMysqlPoolAndSelectsAMysqlPlatform(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->dbal($pool);
        self::assertInstanceOf(
            AbstractMySQLPlatform::class,
            $c->getDatabasePlatform(),
            'one driverClass, two families — the platform comes from HELLO_ACK, not from the class',
        );

        $c->executeStatement('DROP TABLE IF EXISTS s8b_smoke');
        $c->executeStatement('CREATE TABLE s8b_smoke (id INT PRIMARY KEY, note VARCHAR(32))');
        $c->executeStatement('INSERT INTO s8b_smoke (id, note) VALUES (1, \'a\')');
        self::assertSame('a', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = ?', [1]));
        $c->executeStatement('DROP TABLE s8b_smoke');
    }

    /**
     * §14 claims "DBAL middlewares (logging, schema managers, migrations) operate above the driver
     * SPI and work unchanged". Middlewares wrap the DRIVER (`Driver\Middleware::wrap(Driver): Driver`,
     * applied by `DriverManager` before the wrapper Connection is built), so this is testable in
     * four lines — and worth testing, because a middleware also wraps our `Result`, and
     * `AbstractResultMiddleware::getColumnName()` forwards through a `method_exists` guard that
     * would throw if we had skipped that method.
     */
    public function testTheDriverComposesWithADbalMiddleware(): void
    {
        $seen = [];
        $middleware = new class ($seen) implements \Doctrine\DBAL\Driver\Middleware {
            /** @param list<string> $seen */
            public function __construct(private array &$seen) {}

            public function wrap(\Doctrine\DBAL\Driver $driver): \Doctrine\DBAL\Driver
            {
                $seen = &$this->seen;
                return new class ($driver, $seen) extends \Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware {
                    /** @param list<string> $seen */
                    public function __construct(\Doctrine\DBAL\Driver $driver, private array &$seen)
                    {
                        parent::__construct($driver);
                    }

                    /** @param array<string,mixed> $params */
                    public function connect(#[\SensitiveParameter] array $params): \Doctrine\DBAL\Driver\Connection
                    {
                        $this->seen[] = 'connect';
                        return parent::connect($params);
                    }
                };
            }
        };

        $config = new \Doctrine\DBAL\Configuration();
        $config->setMiddlewares([$middleware]);
        $c = \Doctrine\DBAL\DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'default'],
        ], $config);

        self::assertSame([[1]], $c->fetchAllNumeric('SELECT 1'));
        self::assertSame(['connect'], $seen, 'the middleware really wrapped the driver');
        // The Result travelled through the middleware stack, so getColumnName() was forwarded
        // through its method_exists guard rather than throwing a LogicException.
        self::assertSame('one', $c->executeQuery('SELECT 1 AS one')->getColumnName(0));
    }
}
```

- [ ] **Step 8: Run the live smoke**

```bash
cargo build -p ferrod
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live --fail-on-skipped
```
Expected: PASS (3 tests).

- [ ] **Step 9: Wire the four lanes into BOTH gate files**

In `ci/local-gate.sh`, immediately after the `== php: phpstan ==` line, add:

```bash
echo "== php(dbal): install ==" ; (cd php/doctrine-dbal && composer install --no-interaction --quiet)
echo "== php(dbal): phpunit ==" ; (cd php/doctrine-dbal && ./vendor/bin/phpunit)
if [ "$live" = 1 ]; then
  echo "== php(dbal): phpunit (live, skips fatal) =="
  (cd php/doctrine-dbal && ./vendor/bin/phpunit tests/Live --fail-on-skipped)
fi
echo "== php(dbal): phpstan ==" ; (cd php/doctrine-dbal && ./vendor/bin/phpstan analyse src --level 9)
```

In `.github/workflows/ci.yml`, in the `php` job immediately after the `phpstan analyse src --level 9` step, add the four matching steps:

```yaml
      # M1-S8b: the Doctrine driver package. It has its OWN vendor/ (there is no composer
      # workspace), so it needs its own install/test/stan lanes — omitting them would make the whole
      # package a silent no-op in CI.
      - run: (cd php/doctrine-dbal && composer install --no-interaction)
      - run: (cd php/doctrine-dbal && ./vendor/bin/phpunit)
      - run: (cd php/doctrine-dbal && ./vendor/bin/phpunit tests/Live --fail-on-skipped)
      - run: (cd php/doctrine-dbal && ./vendor/bin/phpstan analyse src --level 9)
```

Commit `php/doctrine-dbal/composer.lock` — `php/client/composer.lock` is committed and `.gitignore` does not cover lock files.

- [ ] **Step 10: MUTATION-PROVE the guards**

1. In `PlatformVersion::normalise`, make the PG branch return `$raw` unchanged. Re-run the unit tests: RED on `testTheLivePostgresStringSelectsThePostgresPlatform` (`InvalidPlatformVersion`). Re-run the live smoke: RED on the PG platform assertion. Restore.
2. In `PlatformVersion::normalise`, drop the `$kind !== self::KIND_POSTGRES` guard so it strips on every family, and additionally strip a `-MariaDB…` suffix. Re-run the unit tests: RED on `testTheLiveMysqlAndMariadbStringsSelectDIFFERENTPlatforms` — **this is the wrong-dialect bug being caught**. Restore.
3. In `DbalLiveTestCase::dbal()`, delete the `assertInstanceOf(FerroClientConnection::class, …)` contact assertion and point `driverClass` at a stock `Doctrine\DBAL\Driver\PDO\SQLite\Driver` with `'memory' => true`. Re-run the smoke: **most of it still passes**. That is the false-green hazard in miniature; restore both, and keep the assertion first in the method.
4. Delete the four `php/doctrine-dbal` steps from `.github/workflows/ci.yml`, run `git diff --stat` and confirm nothing else notices. Restore. (No test can catch this one — it is why hazard 70 is written down.)

- [ ] **Step 11: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal ci/local-gate.sh .github/workflows/ci.yml
git commit -m "feat(m1-s8b): the ferro/doctrine-dbal-driver package — walking skeleton, proven live through DriverManager

One driverClass serves both engine families: the platform is selected from
HELLO_ACK's pool kind plus the backend's own version() string. The normalisation
is asymmetric on purpose — PG's verbatim string is REJECTED by the stock anchored
parser, while the MySQL-family string is load-bearing (stripping '-MariaDB'
silently selects MySQL's dialect for a MariaDB server, measured).

Configuration goes through driverOptions, not SPEC SS14's 'ferro' key: Params is a
sealed phpstan shape and reading 'ferro' measured as two level-9 errors, which is
a charter DoD gate.

Every statement is declared a WRITE for SS19.3 fate purposes — the DBAL SPI carries
no read/write signal and charter rule 6 forbids inferring one.

Adds the four package lanes to BOTH ci/local-gate.sh and ci.yml.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 6: `getServerVersion()` — defer, resolve once, then FAIL LOUDLY (the §14 decision, implemented)

`Doctrine\DBAL\ServerVersionProvider::getServerVersion(): string` is **non-nullable** (hazard 2), while `PoolInfo.server_version` is `str | nil` and `nil` is a NORMAL recurring value on a healthy system — a `VERSION_TTL` expiry racing a re-probe, a transient probe failure inside the 5 s `VERSION_RETRY_BACKOFF`, or a backend that is simply down at connect (hazard 49). SPEC §14 makes the handling a **DECISION REQUIRED**, and the decision is binding: **defer resolution; if the version is still unknown when it is actually needed, fail loudly naming the pool. Never a silent default platform** — a wrong platform is a wrong SQL dialect for every subsequent statement.

Deferral is FREE: `Driver::connect()` never needs the version, and `Doctrine\DBAL\Connection::getDatabasePlatform()` resolves the platform once, lazily, on first demand (hazard 15). Resolution is ONE `SELECT version()` through the ordinary SQL path — the same statement `ferrod`'s own probe uses, a leading `SELECT` (so it neither pins nor taints), and the only mechanism that can actually produce a NEW answer (re-reading `poolInfo()` cannot: it is a handshake snapshot, hazard 48).

**Files:**
- Modify: `php/doctrine-dbal/src/Connection.php` (`getServerVersion()` only — the constructor already carries the pool name, from Task 5)
- Create: `php/doctrine-dbal/src/Exception/ServerVersionUnavailable.php`
- Modify: `php/client/tests/Live/LiveTestCase.php` (make the launched pool set overridable)
- Test: `php/doctrine-dbal/tests/Unit/ServerVersionTest.php` (Create), `php/doctrine-dbal/tests/Live/ServerVersionLiveTest.php` (Create)

**Interfaces:**
- Produces: `Ferro\DBAL\Connection::getServerVersion(): string` resolving-then-caching; `Ferro\DBAL\Exception\ServerVersionUnavailable::forPool(string $pool, string $kind, ?\Throwable $previous): self`.
- Consumes (UNCHANGED since Task 5, do not re-declare it): `Ferro\DBAL\Connection::__construct(FerroConnection $ferro, string $poolName, string $poolKind, bool $readonly)` and `poolName(): string`. Task 5 already threads the pool name in and `Driver::connect()` already passes `$o->pool`; this task only adds the cache field and rewrites the method body.
- Produces (client harness): `Ferro\Tests\Live\LiveTestCase::launchedPoolDsns(): array<string,string>` becomes `protected` and overridable, and `launchFerrod()` derives every `FERRO_POOL_<NAME>_DSN` from it.
- Consumes: Task 1's `Ferro\Client\Connection::{fetchRaw, poolInfo}`; Task 5's `PlatformVersion`, `DriverOptions`, `Exception\DriverException`.

- [ ] **Step 1: Write the failing unit test**

Create `php/doctrine-dbal/tests/Unit/ServerVersionTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/ServerVersionTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\DBAL\Exception\ServerVersionUnavailable;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 6 — the SPEC §14 nil-`server_version` decision, in its message form.
 *
 * The behavioural half is live (a pool whose backend is DOWN); what is asserted here is that the
 * loud failure is ACTIONABLE, because "fail loudly" is only better than "guess a platform" if the
 * operator can act on it. Four things must be in the text, and each is a separate assertion so a
 * message rewrite that drops one goes red:
 *   1. WHICH pool (a driver may serve several).
 *   2. That the family IS known — only the version within it is not.
 *   3. That `nil` is a NORMAL transient state, so "wait and retry" is a real fix.
 *   4. The `serverVersion` connection parameter, by its literal name, as the deterministic fix.
 */
final class ServerVersionTest extends TestCase
{
    public function testTheLoudFailureIsActionable(): void
    {
        $e = ServerVersionUnavailable::forPool('main', 'postgres', null);
        $msg = $e->getMessage();

        self::assertStringContainsString('"main"', $msg, 'name the pool');
        self::assertStringContainsString('postgres', $msg, 'the family IS known');
        self::assertStringContainsString('transient', $msg, 'nil is a normal recurring state');
        self::assertStringContainsString('serverVersion', $msg, 'name the operator escape hatch');
        self::assertStringNotContainsString(
            'defaulting',
            $msg,
            'no default platform is ever guessed — a wrong platform is a wrong SQL dialect',
        );
    }

    /** It is a `Driver\Exception`, so it is well-formed if it ever reaches the converter. */
    public function testItIsADriverException(): void
    {
        self::assertInstanceOf(
            \Doctrine\DBAL\Driver\Exception::class,
            ServerVersionUnavailable::forPool('main', 'mysql', null),
        );
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/ServerVersionTest.php
```
Expected: FAIL — `Error: Class "Ferro\DBAL\Exception\ServerVersionUnavailable" not found`.

- [ ] **Step 3: Create the exception**

Create `php/doctrine-dbal/src/Exception/ServerVersionUnavailable.php`:

```php
<?php // /php/doctrine-dbal/src/Exception/ServerVersionUnavailable.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * The engine could not tell us the backend's version, and Doctrine needs one to choose a PLATFORM —
 * i.e. to choose which SQL dialect every subsequent statement is written in.
 *
 * SPEC §14: on a `nil` `server_version` the driver must "fail loudly or defer resolution, never
 * silently fall back to a default platform". This is the end of the deferral: the handshake did not
 * advertise a version and asking the backend directly did not work either.
 */
final class ServerVersionUnavailable extends AbstractException
{
    public static function forPool(string $pool, string $kind, ?\Throwable $previous): self
    {
        return new self(
            sprintf(
                'Ferro: the server version for pool "%s" is unknown, so no Doctrine platform can be '
                . 'chosen. The backend FAMILY is known ("%s") — only the version within it is not, '
                . 'and on MySQL-family pools that is what distinguishes MariaDB from MySQL. This is '
                . 'a normal TRANSIENT state, not necessarily a fault: the engine learns the version '
                . 'lazily and caches it with a TTL, so a cache expiry racing a re-probe, a probe '
                . 'failure inside its retry backoff, or a backend that is currently unreachable all '
                . 'produce it — retrying later may simply succeed. For a deterministic fix, set the '
                . '`serverVersion` connection parameter (e.g. \'serverVersion\' => \'17.10\' or '
                . '\'11.8.8-MariaDB\'), which Doctrine uses instead of asking the connection at all. '
                . 'No platform is guessed here, because a wrong platform is a wrong SQL dialect for '
                . 'every statement that follows.',
                $pool,
                $kind,
            ),
            null,
            0,
            $previous,
        );
    }
}
```

- [ ] **Step 4: Implement the deferred resolution on `Connection`**

In `php/doctrine-dbal/src/Connection.php`, add a cache field and replace `getServerVersion()`. **The constructor is unchanged** — Task 5 already declares `__construct(FerroConnection $ferro, string $poolName, string $poolKind, bool $readonly)` and `poolName()`, and `Driver::connect()` already passes `$o->pool`. Do not re-thread it.

```php
    private ?string $serverVersion = null;
```

```php
    /**
     * The backend's own `version()` string, VERBATIM — normalisation is
     * {@see \Ferro\DBAL\PlatformVersion}'s job, and it is asymmetric (mandatory on PostgreSQL,
     * forbidden on the MySQL family).
     *
     * **The SPEC §14 nil-version decision, implemented: DEFER, then FAIL LOUDLY.** The return type
     * is a non-nullable `string`, so "unknown" cannot be represented — the only honest options are
     * to resolve it or to throw. `HELLO_ACK` carries `server_version` as `str | nil`, and `nil` is a
     * NORMAL recurring value on a healthy system (a TTL expiry racing a re-probe, a probe failure
     * inside its 5 s backoff, a backend that is down at connect), so it must never be treated as an
     * error state by itself.
     *
     * Deferral is free: nothing here runs at connect. Doctrine resolves the platform lazily on
     * first demand, which is typically well after connect — by which time the engine's detached
     * probe has usually landed a value.
     *
     * When it has not, resolution is ONE `SELECT version()` through the ordinary SQL path. That is
     * the same statement `ferrod`'s own probe issues; it is a leading `SELECT`, so the assist lexer
     * leaves the connection unpinned and untainted; and it is the ONLY mechanism that can produce a
     * NEW answer — re-reading `poolInfo()` cannot, because that is a snapshot taken once during
     * this session's handshake. It is declared `readonly = true` because it is the DRIVER'S OWN
     * statement, not a user statement whose intent would have to be inferred.
     *
     * The result is cached for the life of this connection: one round trip, ever.
     */
    public function getServerVersion(): string
    {
        if ($this->serverVersion !== null) {
            return $this->serverVersion;
        }
        $advertised = $this->ferro->poolInfo()?->serverVersion;
        if ($advertised !== null && $advertised !== '') {
            return $this->serverVersion = $advertised;
        }
        try {
            $raw = $this->ferro->fetchRaw('SELECT version()', [], true);
        } catch (FerroException $e) {
            throw ServerVersionUnavailable::forPool($this->poolName, $this->poolKind, $e);
        }
        $v = $raw['rows'][0][0] ?? null;
        if (!is_string($v) || $v === '') {
            throw ServerVersionUnavailable::forPool($this->poolName, $this->poolKind, null);
        }
        return $this->serverVersion = $v;
    }
```

Add `use Ferro\DBAL\Exception\ServerVersionUnavailable;` to the imports. `php/doctrine-dbal/src/Driver.php` needs **no** edit here: its `connect()` already reads `return new Connection($ferro, $o->pool, $info->kind, $o->readonly);` from Task 5.

- [ ] **Step 5: Make the client's live harness able to launch a pool whose backend is DOWN**

The behavioural guard needs a pool that cannot answer. In `php/client/tests/Live/LiveTestCase.php`, make the pool set overridable and derive the env generically. Change `launchedPoolDsns()` from `private` to `protected` and give it the merge point:

```php
    /**
     * The `name => DSN` map this harness hands `ferrod`, in the same order as {@see launchedPools}.
     * Both that method and {@see launchedPoolKinds} read it, so the pool set is stated ONCE.
     *
     * A subclass may add pools by overriding {@see extraPoolDsns} — which is how the M1-S8b driver
     * tier launches a pool whose backend is deliberately UNREACHABLE, the only way to observe the
     * `server_version: nil` branch that SPEC §14's platform decision turns on.
     *
     * @return array<string, string>
     */
    protected function launchedPoolDsns(): array
    {
        $pools = ['default' => $this->pgUrl];
        if ($this->mysqlUrl !== '') {
            $pools[self::MYSQL_POOL] = $this->mysqlUrl;
        }
        return $pools + $this->extraPoolDsns();
    }

    /**
     * Extra `name => DSN` pools for a subclass. Empty by default.
     *
     * @return array<string, string>
     */
    protected function extraPoolDsns(): array
    {
        return [];
    }
```

and in `launchFerrod()`, replace the two hard-coded `FERRO_POOL_*_DSN` assignments with the generic loop (mirroring `ferrod`'s own `env_name()` normalisation — uppercase, every non-alphanumeric to `_`):

```php
        $env['FERRO_POOLS'] = implode(',', $this->launchedPools());
        foreach ($this->launchedPoolDsns() as $name => $dsn) {
            $envName = strtoupper((string) preg_replace('/[^A-Za-z0-9]/', '_', $name));
            $env['FERRO_POOL_' . $envName . '_DSN'] = $dsn;
        }
```

The `$pgUrl` parameter of `launchFerrod` becomes unused; keep the signature (it is called from `setUp` and `restartFerrod`) and let the map be the single source, or drop the parameter and update both call sites — either is fine, but do not leave two sources of truth for the DSN.

- [ ] **Step 6: Write the live test**

Create `php/doctrine-dbal/tests/Live/ServerVersionLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/ServerVersionLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\Exception\ServerVersionUnavailable;

/**
 * M1-S8b Task 6, live — the SPEC §14 nil-version decision, observed rather than argued.
 *
 * The `dead` pool points at a port nothing listens on, so `ferrod` boots fine (pools are LAZY —
 * `Pool::new` dials nothing) and its `HELLO_ACK` advertises the pool with `server_version: nil`.
 * That is the ONLY way to see this branch: on a healthy pool the version is learned within a
 * second or two, and a test that waited for a TTL expiry would take ten minutes.
 */
final class ServerVersionLiveTest extends DbalLiveTestCase
{
    /** @return array<string,string> */
    protected function extraPoolDsns(): array
    {
        // Port 1 refuses immediately (ECONNREFUSED), so this fails FAST rather than sitting in the
        // OS connect timeout — see docs/followups/2026-08-10-unbounded-backend-dial.md for why a
        // black-holed address would be a very different, much slower, test.
        return ['dead' => 'postgres://ferro:ferro@127.0.0.1:1/ferro'];
    }

    public function testAHealthyPoolResolvesItsVersionAndSelectsTheRightPlatform(): void
    {
        $c = $this->dbal();
        self::assertInstanceOf(PostgreSQL120Platform::class, $c->getDatabasePlatform());
        self::assertStringContainsString(
            'PostgreSQL',
            $c->getServerVersion(),
            'the VERBATIM engine string reaches the driver; normalisation happens inside PlatformVersion',
        );
    }

    public function testAPoolWhoseBackendIsDownFailsLOUDLYAndNamesItself(): void
    {
        $c = $this->dbal('dead');
        // Connecting SUCCEEDS — the handshake never depends on backend availability, which is what
        // makes "defer" a real strategy rather than a fig leaf.
        self::assertInstanceOf(\Ferro\Client\Connection::class, $c->getNativeConnection());

        try {
            $c->getDatabasePlatform();
            self::fail('a nil server_version must not silently produce a default platform');
        } catch (ServerVersionUnavailable $e) {
            self::assertStringContainsString('"dead"', $e->getMessage());
            self::assertStringContainsString('serverVersion', $e->getMessage());
        }
    }

    /**
     * The operator escape hatch, on the SAME dead pool: Doctrine builds a static provider from
     * `serverVersion` and never asks our connection at all, so the platform resolves with zero
     * round trips even though the backend is unreachable.
     */
    public function testTheServerVersionParamShortCircuitsTheWholeProblem(): void
    {
        $c = \Doctrine\DBAL\DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'dead'],
            'serverVersion' => 'PostgreSQL 17.10',
        ]);
        self::assertInstanceOf(PostgreSQL120Platform::class, $c->getDatabasePlatform());
    }
}
```

- [ ] **Step 7: Run both tiers**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
cargo build -p ferrod
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live --fail-on-skipped
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit
```
Expected: PASS everywhere. The client's own live tier must be re-run too, because `launchFerrod` changed:

```bash
cd /home/abdullak/projects/ferro/php/client && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live --fail-on-skipped
```

- [ ] **Step 8: MUTATION-PROVE the guards**

1. In `getServerVersion()`, replace the final `throw` with `return '0';`. Re-run the live tests. Expected: RED on `testAPoolWhoseBackendIsDownFailsLOUDLYAndNamesItself` — and note WHAT it would have produced: `PostgreSQLPlatform` (the pre-12 fallback) for a PG 17 server, i.e. a silently downgraded dialect. Restore.
2. Delete the `SELECT version()` fallback so `getServerVersion()` throws whenever `poolInfo()` is nil. Re-run: `testAHealthyPoolResolvesItsVersionAndSelectsTheRightPlatform` stays GREEN (the healthy pool advertises a version) — so this mutation is NOT caught, and that is worth recording: the deferral's VALUE (resolving a transient nil) is not observable in a test that never sees a transient nil. Add nothing; record it in the commit message as a known coverage limit. Restore.
3. In `Connection::getServerVersion()`, cache the value in a `static` rather than an instance property. Re-run `testAPoolWhoseBackendIsDownFailsLOUDLYAndNamesItself` after `testAHealthyPoolResolvesItsVersionAndSelectsTheRightPlatform`. Expected: RED (the dead pool would inherit the healthy pool's version — a real cross-connection dialect leak). Restore.

- [ ] **Step 9: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src php/doctrine-dbal/tests \
        php/client/tests/Live/LiveTestCase.php
git commit -m "feat(m1-s8b): getServerVersion() — defer, resolve once with SELECT version(), then fail loudly

SPEC SS14's DECISION REQUIRED, implemented. getServerVersion() is a non-nullable
string, HELLO_ACK's server_version is str|nil, and nil is a NORMAL recurring
value (TTL expiry racing a re-probe, a probe failure inside its backoff, a
backend down at connect). Deferral is free — Doctrine resolves the platform
lazily — and resolution is ONE SELECT version() through the ordinary SQL path,
the only mechanism that can produce a NEW answer (poolInfo() is a handshake
snapshot).

When that fails the driver throws, naming the pool, the known family, the
transient nature of nil, and the serverVersion parameter as the deterministic
fix. No default platform is ever guessed: a wrong platform is a wrong dialect.

Proven against a pool whose backend is deliberately unreachable, which required
making the client live harness's pool set overridable.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 7: `Statement::bindValue()` — the `(ParameterType, PHP type)` → canonical-tag mapping

SPEC §14 calls this "`bindValue()` with DBAL `ParameterType`→canonical mapping". Measured (hazard 40), it is **not** a `ParameterType` → tag table: `BooleanType::convertToDatabaseValue(true)` returns `int(1)` with `ParameterType::BOOLEAN`, and `FloatType`/`DecimalType`/`BigIntType` all bind `ParameterType::STRING` carrying a float or a numeric string. So the mapping keys on the **pair**. Getting it wrong is not cosmetic: binding `int(1)` as `TAG_I64` against a PG `boolean` column is refused by the narrow per-tag `accepts`, and a bare PHP string carrying binary data fails in the engine's msgpack reader as `invalid utf8` — a generic "malformed ExecRequest", not a diagnosable bind error (hazard 56).

**Files:**
- Create: `php/doctrine-dbal/src/ParameterBinder.php`
- Modify: `php/doctrine-dbal/src/Statement.php` (`bindValue()`)
- Test: `php/doctrine-dbal/tests/Unit/ParameterBinderTest.php` (Create), `php/doctrine-dbal/tests/Live/BindTypesLiveTest.php` (Create)

**Interfaces:**
- Produces: `Ferro\DBAL\ParameterBinder::toCanonical(mixed $value, Doctrine\DBAL\ParameterType $type): mixed` — returns a value `Ferro\Client\ExecCodec::bindOne()` can tag (`null`, `bool`, `int`, `float`, `string`, or `Ferro\Bytes`).
- Consumes: `Doctrine\DBAL\ParameterType` (pure enum, 7 cases); `Ferro\Bytes::__construct(string $value)`; Task 5's `Ferro\DBAL\Exception\DriverException::local()`.

- [ ] **Step 1: Write the failing unit test**

Create `php/doctrine-dbal/tests/Unit/ParameterBinderTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/ParameterBinderTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\ParameterType;
use Ferro\Bytes;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\ParameterBinder;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 7 — the bind mapping keys on the PAIR `(ParameterType, PHP type)`, never on the PHP
 * type alone. Measured against `doctrine/dbal 4.4.4`'s own type layer:
 *   `BooleanType::convertToDatabaseValue(true)` → `int(1)` with `ParameterType::BOOLEAN`
 *   `FloatType` / `DecimalType` / `BigIntType`  → `ParameterType::STRING` carrying a float or a
 *                                                 numeric string
 *   `BlobType`                                  → `ParameterType::LARGE_OBJECT` carrying a string
 * A binder keyed on the PHP type alone would send that `int(1)` as `TAG_I64`, which PostgreSQL's
 * narrow per-tag pre-flight refuses against a `boolean` column.
 */
final class ParameterBinderTest extends TestCase
{
    /**
     * Every `ParameterType` case, exercised. The provider is DERIVED from `ParameterType::cases()`
     * so an eighth case added by a future DBAL release makes this test fail (the row is missing)
     * rather than silently going unmapped.
     *
     * @return array<string, array{0: ParameterType, 1: mixed, 2: mixed}>
     */
    public static function pairs(): array
    {
        $expected = [
            'NULL' => [null, null],
            'BOOLEAN' => [1, true],
            'INTEGER' => ['42', 42],
            'STRING' => ['hello', 'hello'],
            'ASCII' => ['hello', 'hello'],
            'BINARY' => ["\x00\xff", new Bytes("\x00\xff")],
            'LARGE_OBJECT' => ["\x00\xff", new Bytes("\x00\xff")],
        ];
        $out = [];
        foreach (ParameterType::cases() as $case) {
            self::assertArrayHasKey($case->name, $expected, "unmapped ParameterType::{$case->name}");
            $out[$case->name] = [$case, $expected[$case->name][0], $expected[$case->name][1]];
        }
        return $out;
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('pairs')]
    public function testEveryParameterTypeMapsToACanonicalValue(ParameterType $type, mixed $in, mixed $out): void
    {
        self::assertEquals($out, ParameterBinder::toCanonical($in, $type));
    }

    /** Under `STRING`, the PHP type decides — that is what makes floats and ints work at all. */
    public function testStringTypeDispatchesOnThePhpType(): void
    {
        self::assertSame(1.5, ParameterBinder::toCanonical(1.5, ParameterType::STRING));
        self::assertSame(7, ParameterBinder::toCanonical(7, ParameterType::STRING));
        self::assertTrue(ParameterBinder::toCanonical(true, ParameterType::STRING));
        self::assertNull(ParameterBinder::toCanonical(null, ParameterType::STRING));
        self::assertSame('1.2500', ParameterBinder::toCanonical('1.2500', ParameterType::STRING));
    }

    /** NULL survives every type — DBAL binds a null with whatever type the column implies. */
    public function testNullSurvivesEveryParameterType(): void
    {
        foreach (ParameterType::cases() as $case) {
            self::assertNull(ParameterBinder::toCanonical(null, $case), "null under {$case->name}");
        }
    }

    /** A stream (what `BlobType` may hand us) is materialised, not stringified into "Resource id #N". */
    public function testALargeObjectStreamIsMaterialised(): void
    {
        $h = fopen('php://memory', 'r+');
        self::assertNotFalse($h);
        fwrite($h, "\x01\x02\x03");
        rewind($h);
        $out = ParameterBinder::toCanonical($h, ParameterType::LARGE_OBJECT);
        self::assertInstanceOf(Bytes::class, $out);
        self::assertSame("\x01\x02\x03", $out->value);
    }

    /** An object with no canonical shape is a LOUD driver error, never a silent cast. */
    public function testAnUnbindableValueThrows(): void
    {
        $this->expectException(DriverException::class);
        ParameterBinder::toCanonical(new \stdClass(), ParameterType::STRING);
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/ParameterBinderTest.php
```
Expected: FAIL — `Error: Class "Ferro\DBAL\ParameterBinder" not found`.

- [ ] **Step 3: Implement `ParameterBinder`**

Create `php/doctrine-dbal/src/ParameterBinder.php`:

```php
<?php // /php/doctrine-dbal/src/ParameterBinder.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\ParameterType;
use Ferro\Bytes;
use Ferro\DBAL\Exception\DriverException;

/**
 * SPEC §14's "`bindValue()` with DBAL `ParameterType` → canonical mapping".
 *
 * **It keys on the PAIR, not on the `ParameterType` alone and not on the PHP type alone**, because
 * DBAL's own type layer produces mismatched pairs on purpose. Measured against 4.4.4:
 * `BooleanType::convertToDatabaseValue(true)` returns `int(1)` tagged `BOOLEAN`; `FloatType`,
 * `DecimalType` and `BigIntType` all tag `STRING` while carrying a float or a numeric string;
 * `BlobType` tags `LARGE_OBJECT` and carries a raw string (or, from a `bindValue` a user wrote
 * themselves, a stream resource). A binder keyed on the PHP type would send that `int(1)` as
 * `TAG_I64`, and PostgreSQL's narrow per-tag bind pre-flight refuses an integer against a `boolean`
 * column — a hard, pre-send `NonRetryable` on every boolean insert.
 *
 * `BINARY` / `LARGE_OBJECT` become {@see Bytes}, which is the ONLY way to reach `TAG_BYTES` from
 * PHP: every bare PHP string binds `TAG_TEXT`, whose msgpack `str` payload the engine's reader
 * validates as UTF-8 — so a binary blob sent as a string fails as a generic "malformed
 * ExecRequest", not as a diagnosable bind error.
 *
 * The `match` has NO `default` arm. That is the closest thing PHP offers to a compile-forced guard:
 * an eighth `ParameterType` case in a future DBAL release throws `\UnhandledMatchError` here instead
 * of being silently funnelled into the string path.
 */
final class ParameterBinder
{
    public static function toCanonical(mixed $value, ParameterType $type): mixed
    {
        if ($value === null) {
            return null;
        }
        return match ($type) {
            ParameterType::NULL => null,
            ParameterType::BOOLEAN => self::asBool($value),
            ParameterType::INTEGER => self::asInt($value),
            ParameterType::BINARY, ParameterType::LARGE_OBJECT => new Bytes(self::asBinary($value)),
            ParameterType::STRING, ParameterType::ASCII => self::natural($value),
        };
    }

    private static function asBool(mixed $v): bool
    {
        if (is_bool($v)) {
            return $v;
        }
        if (is_int($v)) {
            return $v !== 0;
        }
        if (is_string($v) && ($v === '0' || $v === '1')) {
            return $v === '1';
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind %s as ParameterType::BOOLEAN.',
            get_debug_type($v),
        ));
    }

    private static function asInt(mixed $v): int
    {
        if (is_int($v)) {
            return $v;
        }
        if (is_string($v) && preg_match('/^-?\d+$/', $v) === 1) {
            // A `bigint` above PHP_INT_MAX would silently wrap here, which is exactly the class of
            // corruption this project refuses. Let it through only when it round-trips.
            $n = (int) $v;
            if ((string) $n === $v) {
                return $n;
            }
            throw DriverException::local(sprintf(
                'Ferro: integer parameter %s does not fit a PHP int; bind it as a string so it '
                . 'travels as canonical text.',
                $v,
            ));
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind %s as ParameterType::INTEGER.',
            get_debug_type($v),
        ));
    }

    private static function asBinary(mixed $v): string
    {
        if (is_string($v)) {
            return $v;
        }
        if (is_resource($v)) {
            $s = stream_get_contents($v);
            if ($s === false) {
                throw DriverException::local('Ferro: could not read the stream bound as a binary parameter.');
            }
            return $s;
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind %s as binary; expected a string or a stream resource.',
            get_debug_type($v),
        ));
    }

    /**
     * Under `STRING`/`ASCII` the PHP type decides, because DBAL routes floats, ints and even bools
     * through `STRING`. A stream is materialised rather than stringified into "Resource id #7".
     */
    private static function natural(mixed $v): mixed
    {
        if (is_bool($v) || is_int($v) || is_float($v) || is_string($v)) {
            return $v;
        }
        if (is_resource($v)) {
            return new Bytes(self::asBinary($v));
        }
        if ($v instanceof \Stringable) {
            return (string) $v;
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind a value of type %s. Doctrine\'s type layer converts values before '
            . 'they reach the driver, so this usually means a custom Type returned an object from '
            . 'convertToDatabaseValue().',
            get_debug_type($v),
        ));
    }
}
```

- [ ] **Step 4: Route `Statement::bindValue()` through it**

In `php/doctrine-dbal/src/Statement.php`, add `use Ferro\DBAL\ParameterBinder;` and replace the assignment:

```php
        $this->values[$param] = ParameterBinder::toCanonical($value, $type);
```

- [ ] **Step 5: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS, PHPStan clean.

- [ ] **Step 6: Write the live test — every ParameterType through the real DBAL type layer, on ALL THREE engines**

Create `php/doctrine-dbal/tests/Live/BindTypesLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/BindTypesLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Types;

/**
 * M1-S8b Task 7, live — a stock Doctrine type-layer round trip on PostgreSQL, where the bind
 * pre-flight is NARROW and therefore where the mapping is actually tested. MySQL has no such
 * pre-flight at all (its `COM_STMT_PREPARE` exposes no inferred parameter types), so a driver
 * developed against MySQL alone would look correct and fail on every typed PG column — which is why
 * the PG half comes first here and is the more detailed one.
 */
final class BindTypesLiveTest extends DbalLiveTestCase
{
    public function testTheStockTypeLayerRoundTripsOnPostgres(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_bind');
        $c->executeStatement(
            'CREATE TABLE s8b_bind (id int primary key, flag boolean, n bigint, s text, '
            . 'b bytea, d date, ts timestamp, num numeric(12,4), j jsonb, u uuid)',
        );

        // Bound through DBAL's own $types map, i.e. through convertToDatabaseValue() +
        // getBindingType() — the path a real application takes.
        $c->executeStatement(
            'INSERT INTO s8b_bind (id, flag, n, s, b, d, ts, num, j, u) '
            . 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                1,
                true,
                4294967296,
                'hello',
                "\x00\x01\xff",
                new \DateTimeImmutable('2026-08-05'),
                new \DateTimeImmutable('2026-08-05 13:45:07'),
                '1.2500',
                ['a' => 1],
                '0b7f0a5e-1f4a-4b7d-8f4e-2a9c1d3e5f60',
            ],
            [
                Types::INTEGER,
                Types::BOOLEAN,
                Types::BIGINT,
                Types::STRING,
                ParameterType::BINARY,
                Types::DATE_MUTABLE,
                Types::DATETIME_MUTABLE,
                Types::DECIMAL,
                Types::JSON,
                Types::GUID,
            ],
        );

        $row = $c->fetchAssociative('SELECT * FROM s8b_bind WHERE id = ?', [1]);
        self::assertIsArray($row);
        self::assertTrue($row['flag']);
        self::assertSame('hello', $row['s']);
        self::assertSame('1.2500', $row['num'], 'a decimal keeps its display scale end to end');
        self::assertSame('0b7f0a5e-1f4a-4b7d-8f4e-2a9c1d3e5f60', $row['u']);

        $c->executeStatement('DROP TABLE s8b_bind');
    }

    /** The binary round trip, on both families — the only route to TAG_BYTES from PHP. */
    public function testBinaryRoundTripsOnBothFamilies(): void
    {
        $blob = "\x00\x01\x02\xfe\xff";

        $pg = $this->dbal();
        $pg->executeStatement('DROP TABLE IF EXISTS s8b_blob');
        $pg->executeStatement('CREATE TABLE s8b_blob (id int primary key, b bytea)');
        $pg->executeStatement('INSERT INTO s8b_blob (id, b) VALUES (?, ?)', [1, $blob], [ParameterType::INTEGER, ParameterType::BINARY]);
        self::assertSame($blob, $pg->fetchOne('SELECT b FROM s8b_blob WHERE id = ?', [1]));
        $pg->executeStatement('DROP TABLE s8b_blob');

        $my = $this->dbal($this->requireMysqlPool());
        $my->executeStatement('DROP TABLE IF EXISTS s8b_blob');
        $my->executeStatement('CREATE TABLE s8b_blob (id INT PRIMARY KEY, b VARBINARY(64))');
        $my->executeStatement('INSERT INTO s8b_blob (id, b) VALUES (?, ?)', [1, $blob], [ParameterType::INTEGER, ParameterType::BINARY]);
        self::assertSame($blob, $my->fetchOne('SELECT b FROM s8b_blob WHERE id = ?', [1]));
        $my->executeStatement('DROP TABLE s8b_blob');
    }
}
```

- [ ] **Step 7: Run the live test — and run it ONCE with `ext-msgpack` loaded**

```bash
cargo build -p ferrod
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/BindTypesLiveTest.php --fail-on-skipped
```
Expected: PASS. **This is the first `TAG_BYTES` call path in a package whose CI job provisions `ext-msgpack`** — and `ExtPacker::packBin` is production-dead today with no ext==pure conformance test (hazard 57). CI will exercise the extension path; if a local run with the extension available is possible, do it and record the result, because a divergence there is a wire-format bug nothing else would catch.

- [ ] **Step 8: MUTATION-PROVE the guards**

1. In `ParameterBinder::toCanonical`, collapse the `BOOLEAN` arm into `natural($value)`. Re-run the unit test: RED on the `BOOLEAN` provider row. Re-run the LIVE PG test: RED with `parameter 2: canonical I64 cannot bind to PG type bool`. Restore. **Both must go red** — this is exactly the pair-vs-single-key bug.
2. In the `BINARY, LARGE_OBJECT` arm, return the raw string instead of wrapping it in `Bytes`. Re-run the live binary test: RED (`invalid utf8` from the engine's reader). Restore.
3. Delete one `ParameterType` row from the `$expected` map in the test's provider. Re-run: RED with `unmapped ParameterType::…`. Restore. (This is the guard that a new DBAL case cannot slip through unmapped.)

- [ ] **Step 9: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src/ParameterBinder.php php/doctrine-dbal/src/Statement.php php/doctrine-dbal/tests
git commit -m "feat(m1-s8b): bindValue maps the PAIR (ParameterType, PHP type) to a canonical value

Measured against DBAL 4.4.4: BooleanType hands the driver int(1) tagged BOOLEAN,
Float/Decimal/BigInt hand it a float or numeric string tagged STRING, Blob hands
it a raw string tagged LARGE_OBJECT. A binder keyed on the PHP type alone sends
that int(1) as TAG_I64, which PostgreSQL's narrow per-tag pre-flight refuses
against a boolean column — a hard pre-send failure on every boolean insert.

BINARY/LARGE_OBJECT become Ferro\Bytes, the only route to TAG_BYTES from PHP: a
bare string binds TAG_TEXT, whose payload the engine validates as UTF-8, so a
blob sent as a string fails as a generic malformed-request instead of a
diagnosable bind error.

The match has no default arm, so a future eighth ParameterType throws instead of
being silently funnelled into the string path.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 8: The `Result` contract — all nine methods, `getColumnName()`, and the `rowCount()` divergence, pinned

`Ferro\DBAL\Result` shipped with Task 5 and has no tests. This task pins its contract, because three of its behaviours are easy to get subtly wrong and none of them is caught by "the smoke test passes": `rowCount()` is the terminal's `affected` and NOT `count($rows)`; `getColumnName()` is docblock-optional in the SPI but effectively mandatory (hazard 6); and `free()` must leave the object in the same state the stock `PgSQL\Result` leaves it in, because DBAL calls it from `Doctrine\DBAL\Result::free()` and applications call it on early exit.

**Files:**
- Modify: `php/doctrine-dbal/src/Result.php`
- Test: `php/doctrine-dbal/tests/Unit/ResultTest.php` (Create), `php/doctrine-dbal/tests/Live/ResultLiveTest.php` (Create)

**Interfaces:**
- Produces: the finished `Ferro\DBAL\Result` — `fetchNumeric`, `fetchAssociative`, `fetchOne`, `fetchAllNumeric`, `fetchAllAssociative`, `fetchFirstColumn`, `rowCount(): int`, `columnCount(): int`, `getColumnName(int): string`, `free(): void`, plus `Result::buffered(list<string>, list<list<mixed>>, int): self`.
- Consumes: `Doctrine\DBAL\Driver\FetchUtils` (`@internal` static helpers); `Doctrine\DBAL\Exception\InvalidColumnIndex::new(int)`.

- [ ] **Step 1: Write the failing unit test**

Create `php/doctrine-dbal/tests/Unit/ResultTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/ResultTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Exception\InvalidColumnIndex;
use Ferro\DBAL\Result;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 8 — the driver `Result` contract.
 *
 * `getColumnName()` is declared on `Doctrine\DBAL\Driver\Result` only as a docblock
 * `@method`, which makes it look optional. It is not: `Doctrine\DBAL\Result::getColumnName()`
 * throws a `LogicException` via `method_exists` when it is missing,
 * `Connection::executeCacheQuery()` loops it to build the cache key, and
 * `AbstractResultMiddleware` forwards it with the same guard — so omitting it silently disables
 * DBAL's result cache and breaks any middleware wrapping our result. All eight bundled driver
 * Results implement it; so do we.
 */
final class ResultTest extends TestCase
{
    private function sample(): Result
    {
        return Result::buffered(['id', 'note'], [[1, 'a'], [2, 'b']], 2);
    }

    public function testTheWholeFetchFamily(): void
    {
        self::assertSame([1, 'a'], $this->sample()->fetchNumeric());
        self::assertSame(['id' => 1, 'note' => 'a'], $this->sample()->fetchAssociative());
        self::assertSame(1, $this->sample()->fetchOne());
        self::assertSame([[1, 'a'], [2, 'b']], $this->sample()->fetchAllNumeric());
        self::assertSame(
            [['id' => 1, 'note' => 'a'], ['id' => 2, 'note' => 'b']],
            $this->sample()->fetchAllAssociative(),
        );
        self::assertSame([1, 2], $this->sample()->fetchFirstColumn());
    }

    public function testAnExhaustedResultReturnsFalseAndThenKeepsReturningFalse(): void
    {
        $r = $this->sample();
        $r->fetchNumeric();
        $r->fetchNumeric();
        self::assertFalse($r->fetchNumeric());
        self::assertFalse($r->fetchNumeric());
        self::assertFalse($r->fetchAssociative());
        self::assertNull(Result::buffered(['x'], [], 0)->fetchOne(), 'fetchOne on an empty result');
    }

    /**
     * `rowCount()` is the TERMINAL's `affected`, never `count($rows)`. The research spike shipped
     * `rowCount() === 0` for an `UPDATE` that changed one row precisely because it conflated them,
     * and `Doctrine\DBAL\Connection::executeStatement()` returns exactly this number.
     */
    public function testRowCountIsTheAffectedCountNotTheRowCount(): void
    {
        self::assertSame(7, Result::buffered([], [], 7)->rowCount(), 'a write: rows empty, affected 7');
        self::assertSame(0, Result::buffered(['id'], [[1], [2], [3]], 0)->rowCount(), 'never count($rows)');
    }

    public function testColumnNamesAndTheInvalidIndexContract(): void
    {
        $r = $this->sample();
        self::assertSame(2, $r->columnCount());
        self::assertSame('id', $r->getColumnName(0));
        self::assertSame('note', $r->getColumnName(1));

        $this->expectException(InvalidColumnIndex::class);
        $r->getColumnName(2);
    }

    /**
     * `free()` matches the stock `PgSQL\Result` contract: idempotent, and afterwards `fetchNumeric()`
     * is `false` and `columnCount()` is `0`.
     */
    public function testFreeIsIdempotentAndLeavesAnEmptyResult(): void
    {
        $r = $this->sample();
        $r->free();
        $r->free();
        self::assertFalse($r->fetchNumeric());
        self::assertSame(0, $r->columnCount());
        self::assertSame([], $r->fetchAllAssociative());
    }

    /**
     * DUPLICATE COLUMN NAMES collapse in the associative shape and survive in the numeric one. That
     * is PDO's behaviour too, and it is exactly why `fetchNumeric()` had to be built on positional
     * rows rather than on `array_values()` of an associative row.
     */
    public function testDuplicateColumnNamesCollapseAssociativelyAndSurviveNumerically(): void
    {
        $r = Result::buffered(['x', 'x'], [[1, 2]], 1);
        self::assertSame([1, 2], $r->fetchNumeric());

        $r2 = Result::buffered(['x', 'x'], [[1, 2]], 1);
        self::assertSame(['x' => 2], $r2->fetchAssociative(), 'the last column wins, as in PDO');
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/ResultTest.php
```
Expected: FAIL — `testFreeIsIdempotentAndLeavesAnEmptyResult` passes, but `testAnExhaustedResultReturnsFalseAndThenKeepsReturningFalse` fails on the second `fetchAssociative()` (Task 5's `fetchAssociative` calls `array_combine` on `false`), and `testDuplicateColumnNamesCollapseAssociativelyAndSurviveNumerically` fails for the same reason if the row count differs. Read the actual failure before fixing — the point of the step is to see WHICH ones the walking skeleton got wrong.

- [ ] **Step 3: Harden `Result`**

In `php/doctrine-dbal/src/Result.php`, replace `fetchAssociative()` and `free()`/`columnCount()` with the guarded forms, and add the contract docblocks:

```php
    /**
     * @return array<string,mixed>|false
     *
     * DUPLICATE column names collapse here (the last wins) exactly as they do under PDO. That is
     * why {@see fetchNumeric} is built on POSITIONAL rows from the wire rather than on
     * `array_values()` of this — the numeric shape must not lose a column.
     */
    public function fetchAssociative(): array|false
    {
        $row = $this->fetchNumeric();
        if ($row === false) {
            return false;
        }
        if (count($row) !== count($this->cols)) {
            throw DriverException::local(sprintf(
                'Ferro: result row has %d cells but the header declared %d columns.',
                count($row),
                count($this->cols),
            ));
        }
        return array_combine($this->cols, $row);
    }
```

```php
    /**
     * The TERMINAL's `affected` count — never `count($this->rows)`, which is a different number
     * (`Doctrine\DBAL\Connection::executeStatement()` returns exactly this value, and a `SELECT`
     * carries rows while affecting nothing).
     *
     * **A documented cross-backend divergence:** for a `SELECT`, PostgreSQL's command tag reports
     * the row count while MySQL reports `0`. DBAL treats `rowCount()` on a SELECT as
     * driver-specific and undefined, and every stock driver has the same divergence, so this is
     * reported as-is rather than normalised — normalising it would mean counting rows, which is
     * exactly the conflation above.
     */
    public function rowCount(): int
    {
        return $this->affected;
    }

    public function columnCount(): int
    {
        return count($this->cols);
    }

    /** Idempotent, matching the stock `PgSQL\Result`: afterwards there are no rows and no columns. */
    public function free(): void
    {
        $this->rows = [];
        $this->cols = [];
        $this->cursor = 0;
    }
```

Add `use Ferro\DBAL\Exception\DriverException;` to the imports.

- [ ] **Step 4: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS.

- [ ] **Step 5: Write the live test — the `rowCount()` divergence, DERIVED per backend**

Create `php/doctrine-dbal/tests/Live/ResultLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/ResultLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

/**
 * M1-S8b Task 8, live — the two `Result` behaviours that only a real backend can show: the
 * `affected` count on a real write, and the documented `rowCount()`-on-a-SELECT divergence.
 *
 * The divergence is asserted per FAMILY, derived from the pool's own kind rather than hard-coded
 * per test, so a backend that changes its answer fails here instead of being absorbed.
 */
final class ResultLiveTest extends DbalLiveTestCase
{
    public function testAffectedCountsComeFromTheTerminalOnBothFamilies(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_res');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_res (id int primary key, n int)'
                    : 'CREATE TABLE s8b_res (id INT PRIMARY KEY, n INT)',
            );
            $c->executeStatement('INSERT INTO s8b_res (id, n) VALUES (1,1),(2,1),(3,2)');

            self::assertSame(2, $c->executeStatement('UPDATE s8b_res SET n = 9 WHERE n = 1'), "[$kind] UPDATE");
            self::assertSame(1, $c->executeStatement('DELETE FROM s8b_res WHERE id = 3'), "[$kind] DELETE");
            self::assertSame(0, $c->executeStatement('DELETE FROM s8b_res WHERE id = 99'), "[$kind] no-op DELETE");

            $c->executeStatement('DROP TABLE s8b_res');
        }
    }

    /**
     * `rowCount()` after a SELECT: PostgreSQL's command tag carries the row count, MySQL's carries
     * 0. DBAL documents `rowCount()` on a SELECT as driver-specific; this pins what OURS does per
     * family so a silent change is caught.
     */
    public function testRowCountAfterASelectIsTheDocumentedPerFamilyValue(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_res2');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_res2 (id int primary key)'
                    : 'CREATE TABLE s8b_res2 (id INT PRIMARY KEY)',
            );
            $c->executeStatement('INSERT INTO s8b_res2 (id) VALUES (1),(2),(3)');

            $result = $c->executeQuery('SELECT id FROM s8b_res2');
            self::assertCount(3, $result->fetchAllNumeric(), "[$kind] the rows are all there");
            self::assertSame(
                $kind === 'postgres' ? 3 : 0,
                $result->rowCount(),
                "[$kind] rowCount() after a SELECT is the documented per-family value",
            );

            $c->executeStatement('DROP TABLE s8b_res2');
        }
    }

    /** @return array<string,string> kind => pool name, for whichever pools this run has */
    private function families(): array
    {
        return ['postgres' => 'default', 'mysql' => $this->requireMysqlPool()];
    }
}
```

- [ ] **Step 6: Run the live test**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/ResultLiveTest.php --fail-on-skipped
```
Expected: PASS. If MySQL reports something other than 0 for `rowCount()` after a SELECT, DO NOT relax the assertion — record the measured value in the test and in the docblock, since the point is to pin what actually happens.

- [ ] **Step 7: MUTATION-PROVE the guards**

1. Change `rowCount()` to `return count($this->rows);`. Re-run the unit test: RED on `testRowCountIsTheAffectedCountNotTheRowCount`. Re-run the live test: RED on the `UPDATE`/`DELETE` rows (they return 0 rows). Restore.
2. Delete `getColumnName()` entirely. Re-run the unit test: RED. Additionally run a one-off script calling `Doctrine\DBAL\Result::getColumnName(0)` on a real result — it throws `LogicException("The driver result … does not support accessing the column name.")`, which is what the docblock claim rests on. Restore.
3. In `free()`, clear only `$this->rows` and leave `$this->cols`. Re-run: RED on `columnCount()` after free. Restore.

- [ ] **Step 8: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src/Result.php php/doctrine-dbal/tests
git commit -m "test(m1-s8b): pin the driver Result contract — affected vs count(rows), getColumnName, free

rowCount() is the terminal's affected count and never count(rows); conflating
them reports 0 for an UPDATE that changed rows, which is what the research spike
shipped. getColumnName() looks optional (the SPI declares it only as a docblock
@method) but DBAL's own result cache and every result middleware require it.
free() matches the stock PgSQL\\Result contract: idempotent, no rows, no columns.

Records the per-family rowCount()-after-SELECT divergence (PG reports the row
count, MySQL reports 0) rather than normalising it — normalising would mean
counting rows, which is the conflation above.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 9: `DbalValuePolicy` — the type boundary, where a silently-wrong date is prevented

**This is the highest-severity correctness task in the slice, and its failure mode is invisible to a green test suite.** Measured against `doctrine/dbal 4.4.4` (hazard 38), the stock type layer is a silently-corrupting calendar parser: `date '2026-00-05'` → `DateTime(2025-12-05)`, `datetime '0000-00-00 00:00:00'` → `DateTime(-0001-11-30)`, `time '24:00:00'` (a legal PostgreSQL value) → `00:00:00`. No exception in any of the three. A functional test that writes and reads back an ordinary date passes while those corrupt.

And the reverse: `datetimetz` is BROKEN in both directions for our canonical text on every platform (hazard 35). PG accepts only `Y-m-d H:i:sO`; MySQL/MariaDB accept only `Y-m-d H:i:s`; **any** microsecond form throws everywhere.

The driver's own `ValuePolicy` is where both are handled, and no client API change is needed to do it: `ValuePolicy::decode(int $tag, mixed $data)` is per-cell TAG-AWARE by construction (hazard 41). Charter rule 6 is intact — this is the driver's own conversion step, which `RawStringValuePolicy`'s docblock explicitly blesses; no platform is subclassed and no SQL is generated.

**Files:**
- Create: `php/doctrine-dbal/src/Value/DbalValuePolicy.php`, `php/doctrine-dbal/src/Value/TemporalFormat.php`, `php/doctrine-dbal/src/Exception/NonRepresentableValue.php`
- Modify: `php/doctrine-dbal/src/Driver.php` (use the new policy instead of `RawStringValuePolicy`)
- Test: `php/doctrine-dbal/tests/Unit/{DbalValuePolicyTest,TemporalFormatTest}.php` (Create), `php/doctrine-dbal/tests/Live/TypeBoundaryLiveTest.php` (Create)

**Interfaces:**
- Produces:
  - `Ferro\DBAL\Value\TemporalFormat::forKind(string $kind): self` with public readonly `string $dateTimeTz`.
  - `Ferro\DBAL\Value\DbalValuePolicy implements Ferro\Client\Value\ValuePolicy` — `__construct()` (no args), `bindBackend(string $kind): void` (one-shot), `decode(int $tag, mixed $data): mixed`.
  - `Ferro\DBAL\Exception\NonRepresentableValue::forTag(string $what, string $value, string $why): self`.
- Consumes: `Ferro\Protocol\Generated\Constants` (`TAG_*`); `Ferro\Client\Value\CanonicalText::{requireNull,requireBool,requireInt,requireFloat,requireString,requireBytes,u64,dateIsSentinel,timestampIsInstant,timestamptzIsInstant,timeIsNegative,unsupportedTag}`; Task 5's `PlatformVersion::{KIND_POSTGRES,KIND_MYSQL}`.

- [ ] **Step 1: Write the failing unit tests**

Create `php/doctrine-dbal/tests/Unit/TemporalFormatTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/TemporalFormatTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MariaDB110700Platform;
use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Value\TemporalFormat;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 9 — the driver holds two format-string literals so that its value policy does not
 * have to resolve a PLATFORM (which would need a server version, which may not exist yet). That
 * duplication is only safe if it is LOCKED against the stock accessors, which is what this test is:
 * if a DBAL release changes `getDateTimeTzFormatString()` for either family, this goes red rather
 * than the driver silently emitting a shape DBAL can no longer parse.
 *
 * MEASURED on 4.4.4: PostgreSQL `Y-m-d H:i:sO`, MySQL and MariaDB `Y-m-d H:i:s` (no offset at all).
 */
final class TemporalFormatTest extends TestCase
{
    public function testOurLiteralsEqualTheStockPlatformAccessors(): void
    {
        self::assertSame(
            (new PostgreSQL120Platform())->getDateTimeTzFormatString(),
            TemporalFormat::forKind(PlatformVersion::KIND_POSTGRES)->dateTimeTz,
        );
        self::assertSame(
            (new MySQL84Platform())->getDateTimeTzFormatString(),
            TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz,
        );
        self::assertSame(
            (new MariaDB110700Platform())->getDateTimeTzFormatString(),
            TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz,
            'MariaDB and MySQL share the format, which is why one KIND covers both',
        );
    }

    public function testTheTwoFamiliesGenuinelyDiffer(): void
    {
        self::assertNotSame(
            TemporalFormat::forKind(PlatformVersion::KIND_POSTGRES)->dateTimeTz,
            TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz,
            'if these ever became equal, the per-kind branch would be dead code and this test a tautology',
        );
    }
}
```

Create `php/doctrine-dbal/tests/Unit/DbalValuePolicyTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/DbalValuePolicyTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Doctrine\DBAL\Types\Type;
use Ferro\DBAL\Exception\NonRepresentableValue;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Value\DbalValuePolicy;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 9 — the type boundary.
 *
 * Two opposite jobs. **(a) Make `TIMESTAMPTZ` readable at all**: our canonical text is RFC3339
 * (`2026-08-05T13:45:07Z`) and DBAL's `DateTimeTzType` has NO fallback, so on PostgreSQL it accepts
 * only `Y-m-d H:i:sO` and on MySQL only `Y-m-d H:i:s`. Measured: every canonical form throws on
 * every platform. **(b) Refuse the values DBAL's parser would corrupt SILENTLY.** Measured on
 * 4.4.4, with NO exception raised:
 *     date     '2026-00-05'           -> DateTime(2025-12-05)
 *     datetime '0000-00-00 00:00:00'  -> DateTime(-0001-11-30)
 *     time     '24:00:00'             -> 00:00:00
 * `proto/PROTOCOL.md` §3.2 warns about that parser class in prose; the measurement is why this
 * policy refuses rather than hoping.
 *
 * The second test below is the load-bearing one: it drives the refused values through the STOCK
 * DBAL type layer to prove the corruption is real, and then through the policy to prove we stop it.
 */
final class DbalValuePolicyTest extends TestCase
{
    private function pg(): DbalValuePolicy
    {
        $p = new DbalValuePolicy();
        $p->bindBackend(PlatformVersion::KIND_POSTGRES);
        return $p;
    }

    private function mysql(): DbalValuePolicy
    {
        $p = new DbalValuePolicy();
        $p->bindBackend(PlatformVersion::KIND_MYSQL);
        return $p;
    }

    /** A whole-second TIMESTAMPTZ is re-rendered into the platform's own format, per family. */
    public function testTimestampTzIsRerenderedPerFamilyAndParsesBack(): void
    {
        $pg = $this->pg()->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z');
        self::assertSame('2026-08-05 13:45:07+0000', $pg);
        $back = Type::getType('datetimetz')->convertToPHPValue($pg, new PostgreSQL120Platform());
        self::assertInstanceOf(\DateTimeInterface::class, $back);
        self::assertSame('2026-08-05T13:45:07+00:00', $back->format('Y-m-d\TH:i:sP'));

        self::assertSame('2026-08-05 13:45:07', $this->mysql()->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z'));
    }

    /**
     * THE SILENT-CORRUPTION SET. Each row is first driven through the STOCK type layer to
     * demonstrate that it converts WITHOUT an exception to the WRONG value, then through the policy
     * to prove we refuse it. Without the first half this test would only be asserting our own
     * behaviour; with it, it is a standing proof that the refusal is load-bearing.
     *
     * @return array<string, array{0: int, 1: string, 2: string, 3: string}>
     *   tag, wire text, DBAL type name, the WRONG value stock DBAL produces
     */
    public static function corrupting(): array
    {
        return [
            'MySQL zero-in-date'  => [C::TAG_DATE, '2026-00-05', 'date', '2025-12-05'],
            'MySQL zero date'     => [C::TAG_TIMESTAMP, '0000-00-00 00:00:00', 'datetime', '-0001-11-30'],
            'PG legal 24:00:00'   => [C::TAG_TIME, '24:00:00', 'time', '00:00:00'],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('corrupting')]
    public function testTheSilentlyCorruptingValuesAreRefused(int $tag, string $wire, string $dbalType, string $wrong): void
    {
        $stock = Type::getType($dbalType)->convertToPHPValue($wire, new PostgreSQL120Platform());
        self::assertInstanceOf(
            \DateTimeInterface::class,
            $stock,
            "stock DBAL must still SILENTLY convert $wire — if it started throwing, this refusal "
            . 'could be reconsidered',
        );
        self::assertStringContainsString($wrong, $stock->format('Y-m-d H:i:s'), 'and to the wrong value');

        $this->expectException(NonRepresentableValue::class);
        $this->pg()->decode($tag, $wire);
    }

    /** PG's `infinity` sentinels are refused too — loudly, naming the native API. */
    public function testSentinelsAreRefusedWithAnActionableMessage(): void
    {
        foreach ([[C::TAG_DATE, 'infinity'], [C::TAG_TIMESTAMP, '-infinity'], [C::TAG_TIMESTAMPTZ, 'infinity']] as [$tag, $v]) {
            try {
                $this->pg()->decode($tag, $v);
                self::fail("$v must be refused");
            } catch (NonRepresentableValue $e) {
                self::assertStringContainsString('Ferro\\Client\\Connection', $e->getMessage());
            }
        }
    }

    /**
     * A sub-second TIMESTAMPTZ has NO representation DBAL can parse (measured: every microsecond
     * form throws on every platform), so it is refused rather than TRUNCATED. Truncating would be a
     * silent precision loss, which is the same class of defect as the corruption above.
     */
    public function testASubSecondTimestampTzIsRefusedRatherThanTruncated(): void
    {
        $this->expectException(NonRepresentableValue::class);
        $this->pg()->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z');
    }

    /** Everything else passes through as canonical text — the tags DBAL already handles correctly. */
    public function testTheUnaffectedTagsAreVerbatim(): void
    {
        $p = $this->pg();
        self::assertNull($p->decode(C::TAG_NULL, null));
        self::assertTrue($p->decode(C::TAG_BOOL, true));
        self::assertSame(7, $p->decode(C::TAG_I64, 7));
        self::assertSame(1.5, $p->decode(C::TAG_F64, 1.5));
        self::assertSame('hi', $p->decode(C::TAG_TEXT, 'hi'));
        self::assertSame('1.2500', $p->decode(C::TAG_DECIMAL, '1.2500'), 'display scale survives');
        self::assertSame('NaN', $p->decode(C::TAG_DECIMAL, 'NaN'), 'DecimalType passes NaN through');
        self::assertSame('{"a":1}', $p->decode(C::TAG_JSON, '{"a":1}'));
        self::assertSame('2026-08-05', $p->decode(C::TAG_DATE, '2026-08-05'));
        self::assertSame('13:45:07', $p->decode(C::TAG_TIME, '13:45:07'));
        self::assertSame(
            '2026-08-05 13:45:07.250000',
            $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000'),
            'a NAIVE timestamp keeps its microseconds — DateTimeType has a new DateTime() fallback',
        );
    }

    /** A temporal cell before the backend is known is a LOUD driver error, never a guess. */
    public function testDecodingATemporalTagBeforeBindBackendThrows(): void
    {
        $this->expectException(\Ferro\DBAL\Exception\DriverException::class);
        (new DbalValuePolicy())->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z');
    }
}
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/TemporalFormatTest.php tests/Unit/DbalValuePolicyTest.php
```
Expected: FAIL — `Error: Class "Ferro\DBAL\Value\TemporalFormat" not found`.

- [ ] **Step 3: Create `TemporalFormat` and `NonRepresentableValue`**

Create `php/doctrine-dbal/src/Value/TemporalFormat.php`:

```php
<?php // /php/doctrine-dbal/src/Value/TemporalFormat.php
declare(strict_types=1);
namespace Ferro\DBAL\Value;

use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\PlatformVersion;

/**
 * The one platform format string the value policy needs, per backend family.
 *
 * It is a LITERAL here rather than a call to `$platform->getDateTimeTzFormatString()` for a
 * structural reason: resolving a platform requires a server VERSION, which may not exist yet
 * (SPEC §14's nil-version case) — but a row can arrive before it does. Holding the literal keeps
 * the decode path independent of platform resolution, and
 * {@see \Ferro\DBAL\Tests\Unit\TemporalFormatTest} locks it against the stock accessors so a DBAL
 * release that changes either string turns this into a RED test rather than a driver that emits a
 * shape DBAL can no longer parse.
 *
 * MEASURED on doctrine/dbal 4.4.4: PostgreSQL `Y-m-d H:i:sO`; MySQL and MariaDB `Y-m-d H:i:s` —
 * i.e. the MySQL family's `datetimetz` carries NO offset at all, which is DBAL's own mapping of a
 * type MySQL does not have.
 */
final class TemporalFormat
{
    private function __construct(public readonly string $dateTimeTz) {}

    public static function forKind(string $kind): self
    {
        return new self(match ($kind) {
            PlatformVersion::KIND_POSTGRES => 'Y-m-d H:i:sO',
            PlatformVersion::KIND_MYSQL => 'Y-m-d H:i:s',
            default => throw BackendFamilyUnknown::forKind($kind),
        });
    }
}
```

Create `php/doctrine-dbal/src/Exception/NonRepresentableValue.php`:

```php
<?php // /php/doctrine-dbal/src/Exception/NonRepresentableValue.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * A value that is perfectly legal in the database and on Ferro's wire, but has no representation
 * Doctrine's type layer can parse without CORRUPTING it.
 *
 * Refusing is the whole point. Measured against doctrine/dbal 4.4.4, its stock converters turn
 * `2026-00-05` into `2025-12-05`, `0000-00-00 00:00:00` into `-0001-11-30` and `24:00:00` into
 * `00:00:00` — with no exception. A loud refusal makes a readable-in-the-native-API column
 * unreadable through DBAL; a silent conversion makes it WRONG, which is worse and is the class of
 * defect this project exists to refuse.
 *
 * It is a `Doctrine\DBAL\Driver\Exception`, so `Doctrine\DBAL\Result::fetchAssociative()` converts
 * it like any other driver error rather than letting it escape unconverted.
 */
final class NonRepresentableValue extends AbstractException
{
    public static function forTag(string $what, string $value, string $why): self
    {
        return new self(sprintf(
            'Ferro: the %s value %s cannot be handed to Doctrine\'s type layer — %s. It is a valid '
            . 'value and Ferro can read it: use the native Ferro\\Client\\Connection API, or cast '
            . 'the column in SQL (e.g. `col::text`) if you only need to display it. It is refused '
            . 'rather than converted because Doctrine\'s stock converters would accept it SILENTLY '
            . 'and produce a different value.',
            $what,
            var_export($value, true),
            $why,
        ));
    }
}
```

- [ ] **Step 4: Create `DbalValuePolicy`**

Create `php/doctrine-dbal/src/Value/DbalValuePolicy.php`:

```php
<?php // /php/doctrine-dbal/src/Value/DbalValuePolicy.php
declare(strict_types=1);
namespace Ferro\DBAL\Value;

use Ferro\Client\Value\CanonicalText;
use Ferro\Client\Value\ValuePolicy;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Exception\NonRepresentableValue;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The Doctrine tier's decode policy: canonical wire text for everything DBAL parses correctly, a
 * per-family re-render for the one tag it cannot parse at all, and a LOUD REFUSAL for the values it
 * would parse into something else.
 *
 * **Why a policy and not a conversion inside `Result`.** `ValuePolicy::decode(int $tag, mixed $data)`
 * is handed the per-cell TYPE TAG, which is the only place that information exists on the client:
 * both `ExecCodec::decode()` and `Connection::stream()` deliberately drop the `ColMeta` tag from
 * their column lists, because the per-cell tag is the decode authority. So the driver gets tag
 * awareness for free, with no client API change.
 *
 * **Charter rule 6 is intact.** This is the driver's own conversion step —
 * {@see \Ferro\Client\Value\RawStringValuePolicy}'s docblock says in as many words that the S8 tier
 * "must supply its own conversion rather than lean on the stock type layer for those two tags". No
 * platform is subclassed, no SQL is generated, no result is cached.
 *
 * The three behaviours, each MEASURED against doctrine/dbal 4.4.4:
 *
 *  1. **`TIMESTAMPTZ` is re-rendered.** Canonical text is RFC3339 with a literal `Z`
 *     (`2026-08-05T13:45:07Z`); `DateTimeTzType` has NO fallback and accepts only
 *     `Y-m-d H:i:sO` on PostgreSQL and `Y-m-d H:i:s` on the MySQL family. Every canonical form
 *     throws on every platform, so without this the tag is simply unreadable through DBAL.
 *  2. **Sub-second `TIMESTAMPTZ` is REFUSED, not truncated.** No microsecond form parses on any
 *     platform, and truncating to whole seconds would be a silent precision loss.
 *  3. **Calendar-impossible values are REFUSED.** `date '2026-00-05'` → `2025-12-05`,
 *     `datetime '0000-00-00 00:00:00'` → `-0001-11-30`, `time '24:00:00'` → `00:00:00`. All three
 *     measured, all three with NO exception raised.
 *
 * Everything else is the canonical text verbatim, exactly as `RawStringValuePolicy` hands it up:
 * `DECIMAL` keeps its display scale and its `NaN`/`Infinity` payloads (DBAL's `DecimalType` is a
 * pass-through), `JSON` is the raw document, `UUID` the 36-char lowercase form, `DATE` `Y-m-d`,
 * `TIME` `H:i:s`, and a NAIVE `TIMESTAMP` keeps its microseconds because `DateTimeType` DOES have a
 * `new DateTime($value)` fallback (this last point contradicts a claim in
 * `RawStringValuePolicy`'s docblock, which Task 14 corrects).
 */
final class DbalValuePolicy implements ValuePolicy
{
    private ?TemporalFormat $fmt = null;

    /**
     * Bind the backend family, ONCE, as soon as the handshake reveals it.
     *
     * The policy has to be constructed BEFORE the connection (it is a constructor argument of
     * `Ferro\Client\Connection`), and the family is only known AFTER the handshake — so the wiring
     * is necessarily two-step. It is a one-shot setter rather than a mutable property so that the
     * "which dialect am I decoding for" question can never change under a live connection, and
     * {@see decode} throws rather than guessing if a temporal cell somehow arrives first.
     */
    public function bindBackend(string $kind): void
    {
        if ($this->fmt !== null) {
            throw DriverException::local('Ferro: DbalValuePolicy::bindBackend() called twice.');
        }
        $this->fmt = TemporalFormat::forKind($kind);
    }

    public function decode(int $tag, mixed $data): mixed
    {
        return match ($tag) {
            C::TAG_NULL => CanonicalText::requireNull($data),
            C::TAG_BOOL => CanonicalText::requireBool($data),
            C::TAG_I64 => CanonicalText::requireInt($data),
            C::TAG_F64 => CanonicalText::requireFloat($data),
            C::TAG_TEXT => CanonicalText::requireString($data, $tag),
            C::TAG_BYTES => CanonicalText::requireBytes($data),
            C::TAG_U64 => CanonicalText::u64($data),
            C::TAG_DECIMAL, C::TAG_UUID, C::TAG_JSON => CanonicalText::requireString($data, $tag),
            C::TAG_DATE => $this->date(CanonicalText::requireString($data, $tag)),
            C::TAG_TIME => $this->time(CanonicalText::requireString($data, $tag)),
            C::TAG_TIMESTAMP => $this->timestamp(CanonicalText::requireString($data, $tag)),
            C::TAG_TIMESTAMPTZ => $this->timestampTz(CanonicalText::requireString($data, $tag)),
            default => throw CanonicalText::unsupportedTag($tag),
        };
    }

    private function date(string $t): string
    {
        if (CanonicalText::dateIsSentinel($t)) {
            throw NonRepresentableValue::forTag(
                'DATE',
                $t,
                'it is a sentinel or a zero-in-date, and Doctrine\'s DateType would convert it '
                . 'without complaint to a DIFFERENT calendar date',
            );
        }
        return $t;
    }

    private function time(string $t): string
    {
        if (CanonicalText::timeIsNegative($t)) {
            throw NonRepresentableValue::forTag('TIME', $t, 'Doctrine has no representation for a negative time');
        }
        if (str_contains($t, '.')) {
            throw NonRepresentableValue::forTag(
                'TIME',
                $t,
                'Doctrine\'s TimeType parses only `H:i:s` and has no fallback, so the fraction '
                . 'would have to be dropped',
            );
        }
        $colon = strpos($t, ':');
        if ($colon !== false && (int) substr($t, 0, $colon) > 23) {
            throw NonRepresentableValue::forTag(
                'TIME',
                $t,
                'it is a time-of-day beyond 24 hours (legal in PostgreSQL and in a MySQL TIME '
                . 'interval), which Doctrine\'s TimeType silently wraps to the next day',
            );
        }
        return $t;
    }

    private function timestamp(string $t): string
    {
        if (!CanonicalText::timestampIsInstant($t)) {
            throw NonRepresentableValue::forTag(
                'TIMESTAMP',
                $t,
                'it is a sentinel or a zero datetime, and Doctrine\'s DateTimeType would convert it '
                . 'without complaint to a DIFFERENT instant',
            );
        }
        return $t;
    }

    private function timestampTz(string $t): string
    {
        $fmt = $this->fmt ?? throw DriverException::local(
            'Ferro: a TIMESTAMPTZ cell arrived before the backend family was known; the driver '
            . 'binds it during connect(), so this indicates the policy was used outside the driver.',
        );
        if (!CanonicalText::timestamptzIsInstant($t)) {
            throw NonRepresentableValue::forTag(
                'TIMESTAMPTZ',
                $t,
                'it is a sentinel or a zero timestamp, and Doctrine\'s DateTimeTzType would either '
                . 'reject it or convert it to a different instant',
            );
        }
        if (str_contains($t, '.')) {
            throw NonRepresentableValue::forTag(
                'TIMESTAMPTZ',
                $t,
                'Doctrine\'s DateTimeTzType parses only whole seconds on every platform and has no '
                . 'fallback, so the sub-second part could only be dropped',
            );
        }
        $dt = \DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s\Z', $t, new \DateTimeZone('UTC'));
        if ($dt === false) {
            throw NonRepresentableValue::forTag('TIMESTAMPTZ', $t, 'it is not canonical RFC3339 UTC text');
        }
        return $dt->format($fmt->dateTimeTz);
    }
}
```

- [ ] **Step 5: Wire the policy into `Driver::connect()`**

In `php/doctrine-dbal/src/Driver.php`, add `use Ferro\DBAL\Value\DbalValuePolicy;`, drop the now-unused `use Ferro\Client\Value\RawStringValuePolicy;`, and replace `new RawStringValuePolicy()` with the two-step wiring:

```php
        $policy = new DbalValuePolicy();
        $ferro = $o->socketPath !== null
            ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $policy)
            : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $policy);

        $info = $ferro->poolInfo();
        if ($info === null) {
            throw DriverException::local(/* … unchanged … */);
        }
        // The family is only knowable AFTER the handshake, and the policy is a CONSTRUCTOR argument
        // of the connection — hence the two-step wiring. Nothing has decoded a cell yet: HELLO_ACK
        // carries no TypedValues, and no user statement can have run.
        $policy->bindBackend($info->kind);
        $this->kind = $info->kind;
        return new Connection($ferro, $o->pool, $info->kind, $o->readonly);
```

- [ ] **Step 6: Run the unit tests — they must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS.

- [ ] **Step 7: Write the live test — a real round trip on all three engines**

Create `php/doctrine-dbal/tests/Live/TypeBoundaryLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/TypeBoundaryLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Types\Types;
use Ferro\DBAL\Exception\NonRepresentableValue;

/**
 * M1-S8b Task 9, live — a real `timestamptz` written and read back through the STOCK Doctrine type
 * layer, on a real PostgreSQL, plus the refusal of a real `24:00:00` that PostgreSQL genuinely
 * stores.
 *
 * The refusal half is the important one: it is the only test in the slice that observes a value
 * which is legal in the database, legal on the wire, readable through the native Ferro API, and
 * SILENTLY WRONG through stock Doctrine.
 */
final class TypeBoundaryLiveTest extends DbalLiveTestCase
{
    public function testATimestamptzRoundTripsThroughTheStockTypeLayer(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_tz');
        $c->executeStatement('CREATE TABLE s8b_tz (id int primary key, at timestamptz)');

        $when = new \DateTimeImmutable('2026-08-05 13:45:07', new \DateTimeZone('UTC'));
        $c->executeStatement(
            'INSERT INTO s8b_tz (id, at) VALUES (?, ?)',
            [1, $when],
            [Types::INTEGER, Types::DATETIMETZ_IMMUTABLE],
        );

        $back = $c->fetchOne('SELECT at FROM s8b_tz WHERE id = ?', [1]);
        self::assertIsString($back);
        $obj = \Doctrine\DBAL\Types\Type::getType(Types::DATETIMETZ_IMMUTABLE)
            ->convertToPHPValue($back, $c->getDatabasePlatform());
        self::assertInstanceOf(\DateTimeInterface::class, $obj);
        self::assertSame(
            $when->getTimestamp(),
            $obj->getTimestamp(),
            'the instant must survive the wire and both conversions',
        );

        $c->executeStatement('DROP TABLE s8b_tz');
    }

    public function testAPostgresTwentyFourHourTimeIsRefusedRatherThanSilentlyWrapped(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_t24');
        $c->executeStatement('CREATE TABLE s8b_t24 (id int primary key, t time)');
        // PostgreSQL accepts and STORES this; it is not a malformed value.
        $c->executeStatement("INSERT INTO s8b_t24 (id, t) VALUES (1, TIME '24:00:00')");

        try {
            $c->fetchOne('SELECT t FROM s8b_t24 WHERE id = ?', [1]);
            self::fail('24:00:00 must be refused — Doctrine would read it back as 00:00:00');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(NonRepresentableValue::class, $e->getPrevious());
        }

        // …and it IS readable through the native API, which is what the refusal message says.
        self::assertSame('24:00:00', (string) $c->getNativeConnection()->scalar('SELECT t FROM s8b_t24 WHERE id = 1'));

        $c->executeStatement('DROP TABLE s8b_t24');
    }

    public function testAMysqlZeroDateIsRefusedRatherThanSilentlyShifted(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->dbal($pool);
        $c->executeStatement('DROP TABLE IF EXISTS s8b_zero');
        $c->executeStatement('CREATE TABLE s8b_zero (id INT PRIMARY KEY, d DATE)');
        $c->executeStatement("SET SESSION sql_mode = ''");
        $c->executeStatement("INSERT INTO s8b_zero (id, d) VALUES (1, '2026-00-05')");

        try {
            $c->fetchOne('SELECT d FROM s8b_zero WHERE id = ?', [1]);
            self::fail('a zero-in-date must be refused — Doctrine would read it back as 2025-12-05');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(NonRepresentableValue::class, $e->getPrevious());
        }

        $c->executeStatement('DROP TABLE s8b_zero');
    }
}
```

Note the `SET SESSION sql_mode = ''` in the last test is a deliberate, documented part of the fixture: it is how a zero-in-date gets INTO the table at all, it taints the checkout (the assist lexer classifies a non-local `SET`), and hygiene wipes it at the next checkout — which is fine here because the fixture and the read are in the same test. If Task 13's refusal of isolation SQL is generalised to all `SET SESSION`, this fixture must switch to an engine-side `sql_mode` or the test must be rewritten; do not silently drop it.

- [ ] **Step 8: Run the live test**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/TypeBoundaryLiveTest.php --fail-on-skipped
```
Expected: PASS (3 tests).

- [ ] **Step 9: MUTATION-PROVE the guards**

1. In `DbalValuePolicy::timestampTz`, return `$t` unchanged (no re-render). Re-run the unit test: RED. Re-run the live test: RED (`InvalidFormat` from `DateTimeTzType`). Restore.
2. In `TemporalFormat::forKind`, return `'Y-m-d H:i:sO'` for BOTH families. Re-run `TemporalFormatTest`: RED on the MySQL row. Restore. (This is the family-collapse bug the parity test exists for.)
3. Delete the `date()` sentinel refusal (return `$t`). Re-run `DbalValuePolicyTest`: RED on the zero-in-date row. Re-run the MySQL live test: RED — **and note what it would otherwise have returned: `2025-12-05` for a stored `2026-00-05`, silently**. Restore.
4. In `timestampTz`, truncate the fraction instead of refusing it (`$t = preg_replace('/\.\d+/', '', $t)`). Re-run the unit test: RED on `testASubSecondTimestampTzIsRefusedRatherThanTruncated`. Restore.

- [ ] **Step 10: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src php/doctrine-dbal/tests
git commit -m "feat(m1-s8b): DbalValuePolicy — re-render TIMESTAMPTZ, refuse what Doctrine would silently corrupt

Measured against doctrine/dbal 4.4.4, its stock type layer is a silently-
corrupting calendar parser: date '2026-00-05' -> 2025-12-05, datetime
'0000-00-00 00:00:00' -> -0001-11-30, time '24:00:00' -> 00:00:00, all three with
NO exception. And datetimetz is unreadable in the other direction: it accepts
only Y-m-d H:i:sO on PG and Y-m-d H:i:s on MySQL, so every canonical RFC3339 form
throws on every platform.

The driver's own ValuePolicy handles both — ValuePolicy::decode is per-cell
tag-aware by construction, so no client API change was needed. A whole-second
TIMESTAMPTZ is re-rendered per family; a sub-second one is REFUSED rather than
truncated (silent precision loss is the same defect class); sentinels and
calendar-impossible values are refused with a message naming the native API.

The two format-string literals are locked against the stock platform accessors,
so a DBAL release that changes either turns a test red.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 10: Transactions — DBAL's savepoints must land on the SAME pinned `tx_id`

Hazard 17 names this the single most important internal invariant of the driver connection. DBAL nests transactions CLIENT-SIDE: only nesting level 1 reaches the driver's `beginTransaction()`; every deeper level runs `executeStatement($platform->createSavePoint($name))`, i.e. an **ordinary statement**. If plain statements did not route onto the pinned `tx_id`, a `SAVEPOINT` would land on a *different* pooled connection than its `BEGIN` and DBAL would hold a rollback point that does not exist.

Ferro gets this right by construction — `Ferro\Client\Connection::dispatch()` routes through `$this->tx` whenever an imperative transaction is open, and `fetchRaw()` shares that dispatch — but "by construction" is exactly the kind of claim that must be driven through the REAL DBAL savepoint path rather than asserted.

**Files:**
- Modify: `php/doctrine-dbal/src/Connection.php` (`beginTransaction()` reads the pending isolation set by Task 13; `lastInsertId()` docblock)
- Test: `php/doctrine-dbal/tests/Live/TransactionLiveTest.php` (Create), `php/doctrine-dbal/tests/Live/LastInsertIdLiveTest.php` (Create)

**Interfaces:**
- Produces: no new public API. `Ferro\DBAL\Connection::beginTransaction()/commit()/rollBack()` keep their Task 5 signatures.
- Consumes: `Ferro\Client\Connection::{begin,commit,rollBack,inTransaction,lastInsertId,fetchRaw}`; `Doctrine\DBAL\Connection::setNestTransactionsWithSavepoints(bool)`; Task 5's `Ferro\DBAL\Exception\NoIdentityValue`.

- [ ] **Step 1: Write the failing live test**

Create `php/doctrine-dbal/tests/Live/TransactionLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/TransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

/**
 * M1-S8b Task 10 — the driver connection's most important internal invariant: while a transaction
 * is open, EVERY plain statement rides its pinned `tx_id`.
 *
 * DBAL nests transactions client-side. Only level 1 calls the driver's `beginTransaction()`; deeper
 * levels call `createSavepoint()`, which is `executeStatement($platform->createSavePoint($name))` —
 * an ORDINARY statement. On a transaction-mode pool a statement that did not carry the `tx_id`
 * would be checked out onto a DIFFERENT backend connection, so the `SAVEPOINT` would be created in
 * a session that knows nothing about the `BEGIN`, and `ROLLBACK TO` would fail or (worse) silently
 * roll back nothing.
 *
 * The test drives DBAL's real nesting API rather than issuing savepoint SQL by hand, because the
 * point is that the STOCK path works.
 */
final class TransactionLiveTest extends DbalLiveTestCase
{
    public function testDbalNestedTransactionsUseSavepointsOnThePinnedTransaction(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->setNestTransactionsWithSavepoints(true);

            $c->executeStatement('DROP TABLE IF EXISTS s8b_tx');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_tx (id int primary key, n int)'
                    : 'CREATE TABLE s8b_tx (id INT PRIMARY KEY, n INT) ENGINE=InnoDB',
            );
            $c->executeStatement('INSERT INTO s8b_tx (id, n) VALUES (1, 0)');

            $c->beginTransaction();                       // level 1 -> the driver's beginTransaction
            $c->executeStatement('UPDATE s8b_tx SET n = 1 WHERE id = 1');

            $c->beginTransaction();                       // level 2 -> SAVEPOINT, as ordinary SQL
            $c->executeStatement('UPDATE s8b_tx SET n = 2 WHERE id = 1');
            self::assertSame(2, (int) $c->fetchOne('SELECT n FROM s8b_tx WHERE id = 1'), "[$kind] inner write visible");
            $c->rollBack();                               // level 2 -> ROLLBACK TO SAVEPOINT

            self::assertSame(
                1,
                (int) $c->fetchOne('SELECT n FROM s8b_tx WHERE id = 1'),
                "[$kind] the savepoint rollback undid ONLY the inner write — proving the SAVEPOINT "
                . 'was created on the same pinned connection as the BEGIN',
            );

            $c->commit();                                 // level 1 -> the driver's commit
            self::assertSame(1, (int) $c->fetchOne('SELECT n FROM s8b_tx WHERE id = 1'), "[$kind] committed");

            $c->executeStatement('DROP TABLE s8b_tx');
        }
    }

    /** A rollback at level 1 really reaches the engine, on both families. */
    public function testTopLevelRollbackDiscardsEverything(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_tx2');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_tx2 (id int primary key)'
                    : 'CREATE TABLE s8b_tx2 (id INT PRIMARY KEY) ENGINE=InnoDB',
            );

            $c->beginTransaction();
            $c->executeStatement('INSERT INTO s8b_tx2 (id) VALUES (1)');
            self::assertSame(1, (int) $c->fetchOne('SELECT count(*) FROM s8b_tx2'), "[$kind] visible inside");
            $c->rollBack();
            self::assertSame(0, (int) $c->fetchOne('SELECT count(*) FROM s8b_tx2'), "[$kind] gone after rollback");

            $c->executeStatement('DROP TABLE s8b_tx2');
        }
    }

    /**
     * `setAutoCommit(false)` makes DBAL open a transaction at connect and re-open one after every
     * commit. It is LEGAL under Ferro and it pins a backend connection for the whole request — i.e.
     * it turns the engine's central win off. Asserted here so the behaviour is known and recorded,
     * and listed in `docs/known-incompatibilities.md`.
     */
    public function testAutoCommitFalseWorksAndPinsAConnection(): void
    {
        $c = $this->dbal();
        $c->setAutoCommit(false);
        $c->executeStatement('DROP TABLE IF EXISTS s8b_tx3');
        $c->executeStatement('CREATE TABLE s8b_tx3 (id int primary key)');
        $c->commit();

        $c->executeStatement('INSERT INTO s8b_tx3 (id) VALUES (1)');
        $c->commit();
        self::assertSame(1, (int) $c->fetchOne('SELECT count(*) FROM s8b_tx3'));

        $c->executeStatement('DROP TABLE s8b_tx3');
        $c->commit();
    }

    /** @return array<string,string> */
    private function families(): array
    {
        return ['postgres' => 'default', 'mysql' => $this->requireMysqlPool()];
    }
}
```

Create `php/doctrine-dbal/tests/Live/LastInsertIdLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/LastInsertIdLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Ferro\DBAL\Exception\NoIdentityValue;

/**
 * M1-S8b Task 10 — `lastInsertId()`, and the honest answer on PostgreSQL.
 *
 * DBAL 4's SPI is `lastInsertId(): int|string` with NO sequence-name argument (that overload was
 * REMOVED in 4.0, which makes SPEC §14's "sequence-name argument supported for PG" unimplementable
 * — Task 14 amends it) and it must THROW when there is no identity value.
 *
 * On PostgreSQL there is never one: the protocol carries no such field, and Ferro refuses to
 * emulate it with `SELECT lastval()` because on a transaction-mode pool the follow-up runs on a
 * DIFFERENT connection and returns a silently wrong key. So PG throws, and the message names the
 * two working answers (`INSERT … RETURNING`, or the ORM's SEQUENCE identity strategy).
 */
final class LastInsertIdLiveTest extends DbalLiveTestCase
{
    public function testMysqlReportsTheGeneratedKey(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid');
        $c->executeStatement('CREATE TABLE s8b_lid (id BIGINT AUTO_INCREMENT PRIMARY KEY, note VARCHAR(16))');

        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['a']);
        $first = (int) $c->lastInsertId();
        self::assertGreaterThan(0, $first);

        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['b']);
        self::assertSame($first + 1, (int) $c->lastInsertId());

        $c->executeStatement('DROP TABLE s8b_lid');
    }

    public function testPostgresThrowsAndNamesTheAlternative(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid');
        $c->executeStatement('CREATE TABLE s8b_lid (id serial primary key, note text)');
        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['a']);

        try {
            $c->lastInsertId();
            self::fail('PostgreSQL reports no generated key; the SPI requires a throw');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            $prev = $e->getPrevious();
            self::assertInstanceOf(NoIdentityValue::class, $prev);
            self::assertStringContainsString('RETURNING', $prev->getMessage());
            self::assertStringContainsString('SEQUENCE', $prev->getMessage());
        }

        // …and the documented alternative genuinely works through the same driver.
        $id = $c->fetchOne('INSERT INTO s8b_lid (note) VALUES (?) RETURNING id', ['b']);
        self::assertIsInt($id);

        $c->executeStatement('DROP TABLE s8b_lid');
    }

    /**
     * The key survives being read after a statement inside a TRANSACTION, which is where nearly
     * every real INSERT happens — the client propagates it up from the tx path deliberately.
     */
    public function testTheKeyIsVisibleInsideATransaction(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid2');
        $c->executeStatement('CREATE TABLE s8b_lid2 (id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT) ENGINE=InnoDB');

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO s8b_lid2 (n) VALUES (1)');
        $inTx = (int) $c->lastInsertId();
        self::assertGreaterThan(0, $inTx);
        $c->commit();

        self::assertSame($inTx, (int) $c->fetchOne('SELECT id FROM s8b_lid2 LIMIT 1'));
        $c->executeStatement('DROP TABLE s8b_lid2');
    }
}
```

- [ ] **Step 2: Run them and see which fail**

```bash
cargo build -p ferrod
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/TransactionLiveTest.php tests/Live/LastInsertIdLiveTest.php --fail-on-skipped
```
Expected: the savepoint and rollback tests may already PASS (the client's `dispatch()` routes through the open transaction, so the invariant holds by construction) — that is fine and is the point of writing them: they are a REGRESSION guard for an invariant that is currently implicit. `testPostgresThrowsAndNamesTheAlternative` fails if `NoIdentityValue`'s message does not yet name both alternatives. Read the actual output before changing anything.

- [ ] **Step 3: Make the failures pass**

Whatever Step 2 reported. The two changes most likely to be needed:

In `php/doctrine-dbal/src/Connection.php`, document the invariant on `runPrepared()` so it cannot be "optimised away" later:

```php
    /**
     * The ONE place a statement reaches the engine.
     *
     * **THE INVARIANT: while a transaction is open, this rides its pinned `tx_id`.** It does so
     * because `Ferro\Client\Connection::dispatch()` — which `fetchRaw()` shares with every other
     * statement method — forks on its own open transaction handle. That is not an optimisation
     * detail: Doctrine nests transactions CLIENT-SIDE, so a nested `beginTransaction()` is an
     * ordinary `executeStatement($platform->createSavePoint($name))` arriving right here. A
     * statement that did not carry the `tx_id` would be checked out onto a DIFFERENT backend
     * connection, and Doctrine would hold a rollback point that exists in no session.
     * `TransactionLiveTest::testDbalNestedTransactionsUseSavepointsOnThePinnedTransaction` is the
     * guard; it drives Doctrine's real nesting API, not hand-written savepoint SQL.
     *
     * @param list<mixed> $params
     */
```

and in `NoIdentityValue::forKind`, ensure the PostgreSQL branch names BOTH `INSERT … RETURNING` and the ORM's `SEQUENCE` strategy (Task 5's text already does; verify against the assertion rather than assuming).

- [ ] **Step 4: Re-run — all must pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live --fail-on-skipped
```
Expected: PASS.

- [ ] **Step 5: MUTATION-PROVE the guards**

1. In `Ferro\Client\Connection::dispatch()` (the client, temporarily), force the autocommit branch — `return $this->dispatchAutocommit(...)` unconditionally. Re-run `TransactionLiveTest`: RED on the savepoint test (on PostgreSQL the `SAVEPOINT` lands outside any transaction and the engine's tx-control guard refuses it; on MySQL a bare `SAVEPOINT` under autocommit is silently ignored and the `ROLLBACK TO` then raises `1305`). **Record which failure each family produced** — they are different, and the MySQL one is the more dangerous shape. Restore.
2. In `Connection::lastInsertId()`, return `0` instead of throwing when the key is null. Re-run `LastInsertIdLiveTest`: RED on the PG test. Restore.
3. In `Connection::commit()`, swallow the exception instead of rethrowing. Re-run: `testTopLevelRollbackDiscardsEverything` stays green; nothing catches it. Record that as a known coverage gap — Task 11's converter tests are where a swallowed terminal becomes observable. Restore.

- [ ] **Step 6: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src/Connection.php php/doctrine-dbal/tests/Live
git commit -m "test(m1-s8b): pin the driver's core invariant — DBAL savepoints ride the pinned tx_id

Doctrine nests transactions client-side: only level 1 reaches the driver, and
every deeper level is an ordinary executeStatement(createSavePoint(...)). On a
transaction-mode pool a statement that did not carry the tx_id would be checked
out onto a different backend connection, so the SAVEPOINT would exist in no
session the BEGIN knows about. Driven through Doctrine's real nesting API rather
than hand-written savepoint SQL.

lastInsertId() throws on PostgreSQL, naming both working answers. DBAL 4 removed
the sequence-name overload, so SS14's 'sequence-name argument supported for PG' is
unimplementable — amended in the acceptance task.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 11: The `ExceptionConverter` — a SAFETY surface, not a mapping table

§9.2's third branch — **Indeterminate**, "we cannot tell you whether your write landed" — has **no DBAL equivalent**, which is exactly why §14 specifies `Ferro\DBAL\IndeterminateWriteException`. It must never be flattened into something an application or framework retry loop would treat as retryable: charter rule 3 says the engine never transparently retries, and the driver must not create a path that makes someone else do it.

The rest of the mapping stays **stock**. DBAL's `API\PostgreSQL\ExceptionConverter` keys on SQLSTATE and `API\MySQL\ExceptionConverter` keys on `getCode()` (the vendor errno), and S8a already put both on the wire (hazard 12). Reproducing those tables here would be a second source of truth that rots; the converter delegates.

**Files:**
- Create: `php/doctrine-dbal/src/ExceptionConverter.php`, `php/doctrine-dbal/src/IndeterminateWriteException.php`, `php/doctrine-dbal/src/RetryableDriverException.php`
- Modify: `php/doctrine-dbal/src/Driver.php` (`getExceptionConverter()`)
- Test: `php/doctrine-dbal/tests/Unit/ExceptionConverterTest.php` (Create), `php/doctrine-dbal/tests/Live/ExceptionMappingLiveTest.php` (Create)

**Interfaces:**
- Produces:
  - `Ferro\DBAL\ExceptionConverter implements Doctrine\DBAL\Driver\API\ExceptionConverter` — `__construct(string $kind)`, `convert(Doctrine\DBAL\Driver\Exception $e, ?Doctrine\DBAL\Query $q): Doctrine\DBAL\Exception\DriverException`.
  - `Ferro\DBAL\IndeterminateWriteException extends Doctrine\DBAL\Exception\DriverException` — and, deliberately, implements NOTHING else.
  - `Ferro\DBAL\RetryableDriverException extends Doctrine\DBAL\Exception\DriverException implements Doctrine\DBAL\Exception\RetryableException`.
- Consumes: Task 5's `Ferro\DBAL\Exception\DriverException::branch()`; `Ferro\Protocol\Generated\Constants::{BRANCH_RETRYABLE, BRANCH_INDETERMINATE, BRANCH_NON_RETRYABLE}`; the stock `Doctrine\DBAL\Driver\API\{PostgreSQL,MySQL}\ExceptionConverter`; Task 5's `PlatformVersion::KIND_*`.

- [ ] **Step 1: Write the failing unit test**

Create `php/doctrine-dbal/tests/Unit/ExceptionConverterTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/ExceptionConverterTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException as FerroRetryable;
use Ferro\DBAL\Exception\DriverException as FerroDriverException;
use Ferro\DBAL\ExceptionConverter;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ErrorPayload;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 11 — the converter is a SAFETY surface.
 *
 * §9.2's third branch (Indeterminate — "the write was transmitted and its fate is UNKNOWN") has no
 * DBAL equivalent. Flattening it into a generic `DriverException` would be survivable; flattening
 * it into anything a retry loop treats as retryable would replay a write that may already have
 * landed. `Doctrine\DBAL\Exception\RetryableException` is a bare marker interface that Symfony
 * Messenger, `doctrine/orm`'s retry helpers and hand-rolled loops all key on — so the FIRST
 * assertion below is `assertNotInstanceOf`, and it is the most important line in this file.
 *
 * The rest delegates to the STOCK per-family converters. Reproducing their tables here would be a
 * second source of truth that silently rots as DBAL adds vendor codes.
 */
final class ExceptionConverterTest extends TestCase
{
    /**
     * The client's taxonomy exceptions take the decoded `ErrorPayload` and NOTHING else — the
     * message is built from it by `CarriesErrorPayload::__construct`. `IndeterminateException`
     * additionally takes a client-side `cause` label with a default.
     */
    private function ferro(string $sqlstate, ?int $errno, int $branch): FerroDriverException
    {
        $payload = new ErrorPayload(1, $branch, $sqlstate, $errno, 'boom', null, null);
        $e = match ($branch) {
            2 => new IndeterminateException($payload),
            1 => new FerroRetryable($payload),
            default => new NonRetryableException($payload),
        };
        return FerroDriverException::fromFerro($e);
    }

    /** THE safety assertion. */
    public function testAnIndeterminateWriteIsNeverRetryable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('08006', null, 2), null);

        self::assertInstanceOf(IndeterminateWriteException::class, $out);
        self::assertNotInstanceOf(
            RetryableException::class,
            $out,
            'an Indeterminate write must NEVER be marked retryable — a framework retry loop would '
            . 'replay a write that may already have landed (charter rule 3)',
        );
        self::assertInstanceOf(\Doctrine\DBAL\Exception::class, $out, 'still catchable as a DBAL error');
    }

    /**
     * The Indeterminate interception happens BEFORE the family table, so a SQLSTATE that the stock
     * PG converter maps to something specific still comes out as an indeterminate write. Without
     * this ordering, a `40001` whose branch was Indeterminate would surface as a `DeadlockException`
     * — which IS retryable.
     */
    public function testTheIndeterminateBranchWinsOverTheFamilyTable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('40001', null, 2), null);
        self::assertInstanceOf(IndeterminateWriteException::class, $out);
        self::assertNotInstanceOf(DeadlockException::class, $out);
    }

    /** PostgreSQL keys on SQLSTATE; the stock table does the work. */
    public function testPostgresDelegatesToTheStockSqlstateTable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        self::assertInstanceOf(
            UniqueConstraintViolationException::class,
            $c->convert($this->ferro('23505', null, 3), null),
        );
        self::assertInstanceOf(
            DeadlockException::class,
            $c->convert($this->ferro('40P01', null, 1), null),
        );
    }

    /** MySQL keys on the vendor errno in `getCode()` — the S8a errno-on-wire carry, consumed. */
    public function testMysqlDelegatesToTheStockErrnoTable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_MYSQL);
        self::assertInstanceOf(
            UniqueConstraintViolationException::class,
            $c->convert($this->ferro('23000', 1062, 3), null),
        );
        self::assertInstanceOf(
            DeadlockException::class,
            $c->convert($this->ferro('40001', 1213, 1), null),
        );
    }

    /**
     * A Ferro `Retryable` the stock table does not recognise (a pool checkout timeout, a lost read)
     * must still SAY it is retryable, or the §9.2 branch is lost at the boundary. Only Deadlock and
     * LockWaitTimeout carry DBAL's marker, so this is the case that needs our own class.
     */
    public function testAnUnrecognisedRetryableStillCarriesTheRetryableMarker(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('57P03', null, 1), null);
        self::assertInstanceOf(RetryableException::class, $out);
        self::assertNotInstanceOf(IndeterminateWriteException::class, $out);
    }

    /** A NonRetryable the stock table does not recognise stays a plain DriverException. */
    public function testAnUnrecognisedNonRetryableIsNotUpgraded(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('XX000', null, 3), null);
        self::assertNotInstanceOf(RetryableException::class, $out);
        self::assertNotInstanceOf(IndeterminateWriteException::class, $out);
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/ExceptionConverterTest.php
```
Expected: FAIL — `Error: Class "Ferro\DBAL\ExceptionConverter" not found`.

- [ ] **Step 3: Create the two Ferro-specific exceptions**

Create `php/doctrine-dbal/src/IndeterminateWriteException.php`:

```php
<?php // /php/doctrine-dbal/src/IndeterminateWriteException.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Exception\DriverException;

/**
 * SPEC §9.2's THIRD BRANCH, which Doctrine's exception tree does not have: the write was
 * TRANSMITTED and its fate is UNKNOWN. It may have been applied; it may not.
 *
 * **It deliberately implements NOTHING beyond `DriverException`.** In particular it must never
 * implement `Doctrine\DBAL\Exception\RetryableException`: that is a bare marker interface which
 * Symfony Messenger, ORM retry helpers and every hand-rolled `catch (RetryableException)` loop key
 * on, and replaying an indeterminate write is precisely the at-most-once violation charter rule 3
 * exists to prevent. The engine never transparently retries; neither does this driver; and nothing
 * this driver produces may invite a third party to.
 *
 * Extending `DriverException` (rather than inventing a parallel root) keeps it catchable as
 * `Doctrine\DBAL\Exception`, so an application that catches broadly still sees it — it just cannot
 * mistake it for something safe to repeat.
 *
 * The honest application responses are: report it, reconcile it (look for the row), or fail. There
 * is no fourth option, and that is the point of the branch existing at all.
 */
final class IndeterminateWriteException extends DriverException
{
}
```

Create `php/doctrine-dbal/src/RetryableDriverException.php`:

```php
<?php // /php/doctrine-dbal/src/RetryableDriverException.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\RetryableException;

/**
 * A §9.2 `Retryable` — the statement provably did NOT apply — that Doctrine's stock table does not
 * recognise.
 *
 * DBAL carries its `RetryableException` marker on exactly TWO classes, `DeadlockException` and
 * `LockWaitTimeoutException`, so the stock tables cover only the vendor codes for those. Ferro's
 * Retryable branch is broader by design: a pool checkout that timed out, a connect failure, a lost
 * READ. Those are the cases where retrying is not merely safe but correct, and letting them fall
 * out as a bare `DriverException` would discard the one piece of information §9.2 exists to
 * provide.
 *
 * Used ONLY when the stock converter produced a bare `DriverException`. When it produced a
 * specific class, that class wins — it is more informative, and Deadlock/LockWaitTimeout already
 * carry the marker.
 */
final class RetryableDriverException extends DriverException implements RetryableException
{
}
```

- [ ] **Step 4: Create the converter**

Create `php/doctrine-dbal/src/ExceptionConverter.php`:

```php
<?php // /php/doctrine-dbal/src/ExceptionConverter.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\API\MySQL\ExceptionConverter as MySQLExceptionConverter;
use Doctrine\DBAL\Driver\API\PostgreSQL\ExceptionConverter as PostgreSQLExceptionConverter;
use Doctrine\DBAL\Driver\Exception as DriverExceptionInterface;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Query;
use Ferro\DBAL\Exception\DriverException as FerroDriverException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * SPEC §14's "maps the §9.2 tree to DBAL exceptions uniformly across backends, plus
 * `Ferro\DBAL\IndeterminateWriteException` for the third branch".
 *
 * **Three rules, in this order.**
 *
 *  1. **`Indeterminate` wins over everything.** It is checked FIRST, before the family table,
 *     because the SQLSTATE of an indeterminate write is often one the stock table maps to something
 *     specific — a `40001` whose fate is unknown would otherwise surface as a `DeadlockException`,
 *     which carries DBAL's `RetryableException` marker, which invites a framework to replay a write
 *     that may already have landed.
 *  2. **Everything else delegates to the STOCK per-family converter.** PostgreSQL's keys on
 *     SQLSTATE, MySQL's on the vendor errno in `getCode()`, and M1-S8a put both on the wire
 *     precisely so those tables are reachable. Restating them here would be a second source of
 *     truth that rots as DBAL adds codes — and charter rule 6's spirit is that the drop-in tiers
 *     reuse Doctrine's own knowledge rather than re-deriving it.
 *  3. **A `Retryable` the stock table did not recognise is upgraded**, and only then. DBAL marks
 *     just Deadlock and LockWaitTimeout as retryable, while Ferro's Retryable branch also covers a
 *     pool checkout timeout, a connect failure and a lost read — cases where retrying is correct
 *     and where a bare `DriverException` would silently discard that.
 *
 * A `TypePolicyException` never reaches here: the client raises it client-side, outside the
 * Retryable/Indeterminate/NonRetryable branches, and its own docblock instructs this tier not to
 * report it as a driver protocol failure.
 */
final class ExceptionConverter implements ExceptionConverterInterface
{
    public function __construct(private readonly string $kind) {}

    public function convert(DriverExceptionInterface $exception, ?Query $query): DbalDriverException
    {
        $branch = $exception instanceof FerroDriverException ? $exception->branch() : null;

        if ($branch === C::BRANCH_INDETERMINATE) {
            return new IndeterminateWriteException($exception, $query);
        }

        $stock = $this->kind === PlatformVersion::KIND_MYSQL
            ? new MySQLExceptionConverter()
            : new PostgreSQLExceptionConverter();
        $converted = $stock->convert($exception, $query);

        // `get_class(...) === DbalDriverException::class` is deliberate and is NOT the same as an
        // `instanceof` check: every specialised class IS a DriverException, and we only want to
        // upgrade the ones the stock table left GENERIC.
        if ($branch === C::BRANCH_RETRYABLE && get_class($converted) === DbalDriverException::class) {
            return new RetryableDriverException($exception, $query);
        }

        return $converted;
    }
}
```

- [ ] **Step 5: Wire it into the `Driver`**

In `php/doctrine-dbal/src/Driver.php`, replace `getExceptionConverter()` and drop the stock import:

```php
    /**
     * The family is the one learned at the last {@see connect}. Before any connect there is nothing
     * to convert yet — Doctrine only asks for the converter when a driver exception has already
     * been raised, which requires a connection — so PostgreSQL's table is a harmless default here
     * and, unlike a PLATFORM, choosing it wrongly cannot change any SQL that is emitted.
     */
    public function getExceptionConverter(): ExceptionConverterInterface
    {
        return new ExceptionConverter($this->kind ?? PlatformVersion::KIND_POSTGRES);
    }
```

- [ ] **Step 6: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS.

- [ ] **Step 7: Write the live test — real errors from real engines**

Create `php/doctrine-dbal/tests/Live/ExceptionMappingLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/ExceptionMappingLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Ferro\DBAL\IndeterminateWriteException;

/**
 * M1-S8b Task 11, live — the converter driven from REAL backend errors on both families, because a
 * table-driven unit test proves the table and not the wire. In particular the MySQL half is what
 * proves the S8a errno-on-wire carry actually arrives: DBAL's MySQL converter keys exclusively on
 * `getCode()`, so if the errno were missing every one of these would fall through to a bare
 * `DriverException` and this test would go red.
 */
final class ExceptionMappingLiveTest extends DbalLiveTestCase
{
    public function testRealErrorsMapToTheStockClassesOnBothFamilies(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_err');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_err (id int primary key)'
                    : 'CREATE TABLE s8b_err (id INT PRIMARY KEY)',
            );
            $c->executeStatement('INSERT INTO s8b_err (id) VALUES (1)');

            try {
                $c->executeStatement('INSERT INTO s8b_err (id) VALUES (1)');
                self::fail("[$kind] a duplicate key must throw");
            } catch (UniqueConstraintViolationException $e) {
                self::assertNotInstanceOf(RetryableException::class, $e, "[$kind] a unique violation is deterministic");
            }

            try {
                $c->executeStatement('SELECT * FROM s8b_no_such_table');
                self::fail("[$kind] a missing table must throw");
            } catch (TableNotFoundException) {
            }

            try {
                $c->executeStatement('SELEKT 1');
                self::fail("[$kind] a syntax error must throw");
            } catch (SyntaxErrorException) {
            }

            $c->executeStatement('DROP TABLE s8b_err');
        }
    }

    /**
     * A real DEADLOCK, and the point is not that it maps to `DeadlockException` but that
     * `DeadlockException` carries DBAL's `RetryableException` marker while an indeterminate write
     * does not. The two branches must be distinguishable by an application's retry loop.
     */
    public function testARealDeadlockIsRetryableWhileAnIndeterminateWriteIsNot(): void
    {
        $pool = $this->requireMysqlPool();
        $a = $this->dbal($pool);
        $b = $this->dbal($pool);

        $a->executeStatement('DROP TABLE IF EXISTS s8b_dl');
        $a->executeStatement('CREATE TABLE s8b_dl (id INT PRIMARY KEY, n INT) ENGINE=InnoDB');
        $a->executeStatement('INSERT INTO s8b_dl (id, n) VALUES (1,0),(2,0)');

        $a->beginTransaction();
        $b->beginTransaction();
        $a->executeStatement('UPDATE s8b_dl SET n = 1 WHERE id = 1');
        $b->executeStatement('UPDATE s8b_dl SET n = 1 WHERE id = 2');

        $caught = null;
        try {
            $a->executeStatement('UPDATE s8b_dl SET n = 2 WHERE id = 2');
            $b->executeStatement('UPDATE s8b_dl SET n = 2 WHERE id = 1');
            self::fail('one of the two transactions must be chosen as the deadlock victim');
        } catch (RetryableException $e) {
            $caught = $e;
        } finally {
            foreach ([$a, $b] as $conn) {
                if ($conn->isTransactionActive()) {
                    try {
                        $conn->rollBack();
                    } catch (\Throwable) {
                        // the victim's transaction is already gone
                    }
                }
            }
        }
        self::assertNotNull($caught);
        self::assertNotInstanceOf(
            IndeterminateWriteException::class,
            $caught,
            'a deadlock victim provably did NOT apply — it is Retryable, never Indeterminate',
        );

        $a->executeStatement('DROP TABLE s8b_dl');
    }

    /**
     * **The COST of `readonly = false`, pinned so it cannot change silently.**
     *
     * `readonly` is read in TWO places in `fate.rs`, and the second is the **57014 override**
     * (`engine/crates/ferrod/src/services/fate.rs:71-114`): with `!in_tx`, a cancelled or
     * timed-out statement is `Cancelled{NonRetryable}` when the client declared `readonly` and
     * `WriteUnconfirmed{INDETERMINATE}` when it did not. The driver declares WRITE for everything
     * (hazard 22 — the DBAL 4 SPI carries no read/write signal and charter rule 6 forbids inferring
     * one), so **a plain `SELECT` killed by a server-side cancel or an operator's
     * `statement_timeout` surfaces as `Ferro\DBAL\IndeterminateWriteException`** — "your write may
     * or may not have landed", for a statement that wrote nothing.
     *
     * That is the price of the decision, not a bug, and it is listed in
     * `docs/known-incompatibilities.md` and in §22.2 (ac). What this test does is make it
     * FALSIFIABLE in both directions, so it can neither be quietly "fixed" by inferring
     * read-vs-write from SQL text nor quietly forgotten. It is also the ONLY behavioural test of
     * `driverOptions.readonly` anywhere in the slice — every other assertion about it only proves
     * the option is parsed.
     *
     * `SELECT pg_cancel_backend(pg_backend_pid())` cancels its OWN statement, producing a genuine
     * `57014` on an ordinary autocommit statement with no session state and no second connection
     * (hazard 82). It has to be that: the PHP client never sends `ExecRequest.timeout_ms`, and a
     * preceding `SET statement_timeout` would land on a different pooled connection — a non-local
     * `SET` taints the checkout but does not pin it.
     *
     * PostgreSQL only, deliberately. `fate.rs` is shared VERBATIM across backends (the S6 slice
     * reused it untouched), the 57014 override's own unit table
     * (`fate.rs::fate_57014_total_over_all_axes`) proves the cell for every `(readonly, sent,
     * in_tx)` combination, and `mysql_chaos_it.rs` already drives the MySQL errno mapping into it.
     * One family pins the SHAPE; duplicating it would add a second flaky path, not a second proof.
     */
    public function testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne(): void
    {
        $sql = 'SELECT pg_cancel_backend(pg_backend_pid())';

        $write = $this->dbal();
        try {
            $write->executeQuery($sql);
            self::fail('a self-cancelled statement must raise 57014');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $e,
                'on the DEFAULT (write-declared) connection a 57014 is §19.3 Indeterminate — this is '
                . 'the documented cost of declaring every DBAL statement a write',
            );
            self::assertSame('57014', $e->getSQLState());
        }

        $read = $this->dbal('default', ['readonly' => true]);
        try {
            $read->executeQuery($sql);
            self::fail('a self-cancelled statement must raise 57014 here too');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertNotInstanceOf(
                IndeterminateWriteException::class,
                $e,
                'driverOptions.readonly is what buys back the clean "statement cancelled" answer',
            );
            self::assertNotInstanceOf(RetryableException::class, $e, 'Cancelled is NonRetryable on the wire');
            self::assertSame('57014', $e->getSQLState());
        }
    }

    /** @return array<string,string> */
    private function families(): array
    {
        return ['postgres' => 'default', 'mysql' => $this->requireMysqlPool()];
    }
}
```

- [ ] **Step 8: Run the live test**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/ExceptionMappingLiveTest.php --fail-on-skipped
```
Expected: PASS (3 tests). The deadlock test is inherently racy in WHICH connection is chosen as the victim; it must assert only that one of them was, never which.

**If `testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne` does not raise at all**, the self-cancel did not take: check the SQLSTATE the engine actually reported (`ferrod`'s log, or `$e->getSQLState()`), and if PG returned success rather than `57014`, wrap it as `SELECT pg_cancel_backend(pg_backend_pid()), pg_sleep(1)` so there is a still-running statement for the signal to interrupt. Do NOT weaken the test to "some exception was raised": the whole point is WHICH class each connection gets.

- [ ] **Step 9: MUTATION-PROVE the guards**

1. Make `IndeterminateWriteException` `implements RetryableException`. Re-run the unit test: RED on `testAnIndeterminateWriteIsNeverRetryable`. **This is the single most important mutation in the slice** — record it explicitly. Restore.
2. Move the `BRANCH_INDETERMINATE` check to AFTER the stock delegation. Re-run: RED on `testTheIndeterminateBranchWinsOverTheFamilyTable` (the `40001` comes back a `DeadlockException`, which is retryable). Restore.
3. In `Ferro\DBAL\Exception\DriverException::fromFerro`, pass `0` instead of `$errno`. Re-run the LIVE test: RED on the MySQL half of `testRealErrorsMapToTheStockClassesOnBothFamilies` (the errno-keyed table can no longer see 1062/1146/1064) while the PostgreSQL half stays green — which is exactly the asymmetry hazard 12 describes. Restore.
4. Change `get_class($converted) === DbalDriverException::class` to `$converted instanceof DbalDriverException`. Re-run: RED on `testPostgresDelegatesToTheStockSqlstateTable` (`40P01` would be replaced by our generic retryable, losing `DeadlockException`). Restore.
5. In `DriverOptions::fromParams()`, hard-code `readonly` to `false` (ignore the option). Re-run the LIVE test: RED on the SECOND half of `testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne` — the read-only connection now also gets an `IndeterminateWriteException`. Restore. Then hard-code it to `true` instead: RED on the FIRST half. Restore. **Both directions matter**: the first proves `driverOptions.readonly` actually reaches the wire's fate flag, the second proves the default really is "write", which is the safety decision the whole of Task 1 exists to serve.

- [ ] **Step 10: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src php/doctrine-dbal/tests
git commit -m "feat(m1-s8b): the ExceptionConverter — the Indeterminate branch, and nothing that invites a retry

SS9.2's third branch has no DBAL equivalent. IndeterminateWriteException extends
DriverException and deliberately implements NOTHING else: RetryableException is a
bare marker that Messenger, ORM helpers and hand-rolled loops key on, and
replaying an indeterminate write is the at-most-once violation charter rule 3
exists to prevent. It is checked BEFORE the family table, because the SQLSTATE of
an indeterminate write is often one the stock table maps to a retryable class.

Everything else delegates to the STOCK per-family converters — PG keys on
SQLSTATE, MySQL on the vendor errno in getCode(), both of which S8a put on the
wire. A Retryable the stock table left generic is upgraded to a marked class, so
the SS9.2 branch is not lost at the boundary.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 12: Streaming — `iterate*()` on a parameterless read never buffers, and an interleaved statement still works

§14 requires `iterateAssociative()` et al. never to buffer. Hazard 8 reduces that to one thing: `fetchAssociative()` must pull incrementally. Two constraints bound where that is possible:

1. **The prepared path CANNOT stream.** `Doctrine\DBAL\Connection::executeStatement()` with parameters is `$stmt->execute()->rowCount()` (read from `src/Connection.php:891-911`), and a streamed request's terminal carries **no `affected` field** — the `HEAD`/`DATA`/`END` producer has none. Streaming there would make `executeStatement()` return 0 for every parameterized write: a silently wrong return value, which is worse than buffering. So the driver streams on the **`query()`** path — the zero-parameter `executeQuery`, where DBAL itself has declared the statement a query and never asks for a row count — and buffers on the prepared path. *(The durable fix is an `affected` field on the stream terminal, which is a `/proto` change: registry + golden vectors + both codecs. It is DEFERRED and flagged, not smuggled in here.)*
2. **MySQL/MariaDB cannot stream at all** — `PoolBackend::supports_row_streaming()` is false there (§22.2 (n), controller decision D-S8b-2). `iterate*()` streams on PostgreSQL and buffers on MySQL.

And one hazard the design must answer rather than document away: the session is strictly **single-in-flight** (hazard 25), so an open stream makes every other statement throw — which would break the canonical Doctrine batch idiom `foreach ($conn->iterateAssociative($sql) as $row) { $conn->executeStatement($upd, …); }`. The driver settles the open stream (drains its remainder into memory) before issuing anything else: pure iteration never buffers, and interleaving degrades to buffering instead of throwing.

> **v2 — the ABANDONMENT half was rebuilt, and it is the part most worth reading.** Plan v1 had `Ferro\DBAL\Connection` hold a **strong** reference to the open `Result`. Combined with hazard 80 (`Doctrine\DBAL\Result` has no `__destruct` and DBAL never calls the driver `Result::free()`), that reference was the *only* thing keeping an abandoned stream alive — so `break`-ing out of an `iterateAssociative()` did not CANCEL anything: the next statement's `settleOpenStream()` quietly transferred **the entire remaining result set** over the wire. At 100 000 rows that is invisible in a test; on a real table it is an OOM. v1's own live guard passed through that `materialize()` path while describing itself as "the `free()`/CANCEL path", and its named mutation (deleting `close()` from `free()`) could not fail, because `free()` was never called.
>
> v2 holds a **`\WeakReference<Result>`** instead, and gives `Result` a `__destruct` that frees itself. That single change makes the two cases genuinely different and, crucially, DISTINGUISHABLE: when the DBAL-side generator is gone the driver `Result` is unreferenced, PHP destroys it by refcount at the `break`, and its own `free()` sends the `CANCEL`; when the caller can still fetch (the interleave idiom), the reference is live and `settleOpenStream()` materialises. `settledRowCount()` makes which of the two happened observable from a test — and from production, where "how many rows did this connection have to drain because a stream was still open" is a real operator question.
>
> **MEASURED LIMIT of that design — it closes the canonical idiom and NOT a bound iterator, and this is a PHP refcount fact, not a choice.** v2 was written with the testkit containers stopped, so this was verified afterwards by the controller (PHP 8.4.18 + doctrine/dbal 4.4.4; repro `scratchpad/dbalchk/destruct.php`, a driver `Result` carrying a `__destruct` probe, wrapped in a real `Doctrine\DBAL\Result`, iterated through the real `iterateAssociative()` generator):
>
> | shape | `__destruct` at `break`? |
> |---|---|
> | (A) generator is a TEMPORARY — `foreach ($conn->iterateAssociative($sql) as $row) { … break; }` | **YES**, immediately, without `gc_collect_cycles()` |
> | (B) generator BOUND first — `$it = $conn->iterateAssociative($sql); foreach ($it as $row) { … break; }` | **NO** while `$it` is in scope; yes only after `unset($it)` |
> | (C) full drain | YES |
>
> Case (A) is the canonical Doctrine idiom and the one this design closes. In case (B) the `WeakReference` is still live, so `settleOpenStream()` cannot tell "the caller abandoned it" from "the caller may still fetch" — it takes the `materialize()` branch and transfers the whole remainder, i.e. the OOM trap this rebuild exists to prevent, still open for that shape. The driver **cannot** distinguish them: a live reference is a live reference. So do not write a guard that claims abandonment always cancels.
>
> Two consequences for this task, both mandatory: the live abandonment test must exercise **both** shapes — asserting `settledRowCount() === 0` for (A), and asserting the (B) number is the full remainder rather than pretending it is 0 — so the limit is pinned by a test instead of discovered in production; and the known-incompatibilities doc (Task 14) must state it in the operator's language: *bind an iterator to a variable, abandon it, and the rest of the result set is transferred on your next statement — iterate the call directly, or `unset()` the iterator.*

**Files:**
- Modify: `php/doctrine-dbal/src/Result.php` (the streamed mode + `__destruct`), `php/doctrine-dbal/src/Connection.php` (`query()` streams; every other wire op settles the open stream first)
- Test: `php/doctrine-dbal/tests/Unit/StreamedResultTest.php` (Create), `php/doctrine-dbal/tests/Live/StreamingLiveTest.php` (Create)

**Interfaces:**
- Produces: `Ferro\DBAL\Result::streamed(Ferro\Client\RawStream $stream): self`; `Ferro\DBAL\Result::materialize(): int` (drain the remainder into memory, return how many rows that cost; idempotent, returns 0 thereafter); `Ferro\DBAL\Result::isStreaming(): bool`; `Ferro\DBAL\Result::__destruct()`. `Ferro\DBAL\Connection::settleOpenStream(): void` (private) and `Ferro\DBAL\Connection::settledRowCount(): int`.
- Consumes: Task 2's `Ferro\Client\RawStream::{columns,rows,close,isClosed}` and `Ferro\Client\Connection::streamRaw()`; Task 5's `Ferro\DBAL\Connection::poolKind()`; `Ferro\Client\Value\...` unchanged.

- [ ] **Step 1: Write the failing unit test**

Create `php/doctrine-dbal/tests/Unit/StreamedResultTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/StreamedResultTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\Client\RawStream;
use Ferro\DBAL\Result;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 12 — the streamed `Result` mode, over a hand-built `RawStream` (no engine).
 *
 * What is pinned here is the LAZINESS itself: a `Result` that pulled its generator eagerly would
 * satisfy every functional assertion in this file and still buffer 50 000 rows, so the tests below
 * count how far the generator has advanced rather than only checking the values that come out.
 */
final class StreamedResultTest extends TestCase
{
    /**
     * @param list<list<mixed>> $rows
     * @param ?FakeSession $session pass one to observe the `CANCEL`; `null` builds a wire-less
     *   stream, which is fine for the pure fetch/laziness tests and USELESS for anything asserting
     *   that `close()` reached the engine — `RawStream::close()` is `$this->session?->abandonStream()`,
     *   so with a null session it provably touches nothing.
     */
    private function stream(array $rows, ?int &$pulled = null, ?FakeSession $session = null): RawStream
    {
        $pulled = 0;
        $gen = (static function () use ($rows, &$pulled): \Generator {
            foreach ($rows as $r) {
                ++$pulled;
                yield $r;
            }
        })();
        return new RawStream(['id', 'note'], $gen, $session, 7);
    }

    public function testFetchingPullsExactlyOneRowAtATime(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled));

        self::assertSame(0, $pulled, 'constructing a streamed Result must not pull anything');
        self::assertSame(['id' => 1, 'note' => 'a'], $r->fetchAssociative());
        self::assertSame(1, $pulled, 'one fetch, one row');
        self::assertSame([2, 'b'], $r->fetchNumeric());
        self::assertSame(2, $pulled);
        self::assertSame(['id' => 3, 'note' => 'c'], $r->fetchAssociative());
        self::assertFalse($r->fetchNumeric(), 'exhausted');
        self::assertFalse($r->fetchAssociative(), 'and stays exhausted');
    }

    /** Columns are readable before the first row — DBAL calls `columnCount()` before any fetch. */
    public function testColumnsAreAvailableWithoutPullingARow(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a']], $pulled));
        self::assertSame(2, $r->columnCount());
        self::assertSame('note', $r->getColumnName(1));
        self::assertSame(0, $pulled);
    }

    /**
     * `materialize()` drains the REMAINDER into memory and leaves the already-fetched rows
     * consumed — the interleaving escape hatch. Idempotent, and afterwards the result is an
     * ordinary buffered one. The RETURN value is how many rows the drain cost, which is what
     * {@see \Ferro\DBAL\Connection::settledRowCount} surfaces.
     *
     * **It must NOT be written as `foreach ($this->gen as $row)`.** `foreach` calls
     * `Generator::rewind()`, which throws `Cannot rewind a generator that was already run` once the
     * generator has advanced past its first yield — and the streamed `fetchNumeric()` above advances
     * it on every call (hazard 78, measured on PHP 8.4.18). That is why this test fetches a row
     * BEFORE materialising: without the fetch, the bug is invisible.
     */
    public function testMaterializeDrainsTheRemainderAndIsIdempotent(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled));
        $r->fetchNumeric();

        self::assertSame(2, $r->materialize(), 'the drain cost two rows');
        self::assertSame(0, $r->materialize(), 'idempotent: the second call drains nothing');
        self::assertSame(3, $pulled, 'everything is now in memory');
        self::assertFalse($r->isStreaming());
        self::assertSame([[2, 'b'], [3, 'c']], $r->fetchAllNumeric(), 'the UNCONSUMED rows remain, in order');
    }

    /**
     * `free()` on a streamed result closes the stream and empties it — and the close reaches the
     * WIRE, which is what a `FakeSession` (rather than v1's `null`) is here to witness. With a null
     * session `RawStream::close()` is `$this->session?->abandonStream(...)`, i.e. provably a no-op,
     * so the v1 form of this test could not tell a real `CANCEL` from no `CANCEL` at all.
     */
    public function testFreeClosesTheStreamOnTheWire(): void
    {
        $pulled = 0;
        $session = new FakeSession();
        $stream = $this->stream([[1, 'a'], [2, 'b']], $pulled, $session);
        $r = Result::streamed($stream);
        $r->fetchNumeric();
        self::assertSame(0, $session->abandonCount, 'nothing abandoned while the result is live');

        $r->free();
        self::assertSame(1, $session->abandonCount, 'free() must CANCEL + drain to the ONE terminal');
        self::assertTrue($stream->isClosed());
        self::assertFalse($r->fetchNumeric());
        self::assertSame(0, $r->columnCount());
    }

    /**
     * **Destruction frees.** This is the whole abandonment design in one assertion: when the caller
     * drops the result (which is what `break`-ing out of `Doctrine\DBAL\Result::iterateAssociative()`
     * does — the Generator holds the only reference, and `Doctrine\DBAL\Result` has no `__destruct`,
     * hazard 80), the driver `Result` must send the `CANCEL` itself. Nothing else will: DBAL never
     * calls the driver's `free()` on abandonment, and from Step 4 the driver `Connection` holds only
     * a `\WeakReference`, precisely so this destruction can happen.
     */
    public function testDroppingAStreamedResultCancelsTheStream(): void
    {
        $pulled = 0;
        $session = new FakeSession();
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled, $session));
        $r->fetchNumeric();

        unset($r);            // the last reference — PHP frees it here, by refcount
        self::assertSame(1, $session->abandonCount, 'a dropped streamed Result must abandon its stream');
        self::assertSame(1, $pulled, 'and must NOT have drained the remainder to get there');
    }

    /**
     * A streamed read reports `rowCount() === 0`, because the HEAD/DATA/END producer carries no
     * `affected` field at all. This is the reason the PREPARED path does not stream:
     * `Doctrine\DBAL\Connection::executeStatement()` RETURNS this number.
     */
    public function testAStreamedResultReportsNoAffectedCount(): void
    {
        $pulled = 0;
        self::assertSame(0, Result::streamed($this->stream([[1, 'a']], $pulled))->rowCount());
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/StreamedResultTest.php
```
Expected: FAIL — `Error: Call to undefined method Ferro\DBAL\Result::streamed()`.

- [ ] **Step 3: Add the streamed mode to `Result`**

In `php/doctrine-dbal/src/Result.php`, add the import `use Ferro\Client\RawStream;`, a nullable stream field, the factory, and the pull logic:

```php
    private ?RawStream $stream = null;
    private ?\Generator $gen = null;

    /**
     * A LAZY result over an open streamed read. `fetchNumeric()` pulls exactly one row per call,
     * which is what makes `Doctrine\DBAL\Result::iterateAssociative()` — literally
     * `while (($row = $this->fetchAssociative()) !== false) yield $row;` — never buffer.
     */
    public static function streamed(RawStream $stream): self
    {
        $r = new self($stream->columns(), [], 0);
        $r->stream = $stream;
        $r->gen = $stream->rows();
        return $r;
    }

    public function isStreaming(): bool
    {
        return $this->stream !== null;
    }

    /**
     * Drain whatever is left of the stream into memory and become an ordinary buffered result.
     * Returns the number of rows that cost — 0 when there was nothing to drain, which is also what
     * every call after the first returns.
     *
     * The escape hatch for the canonical Doctrine batch idiom
     * `foreach ($conn->iterateAssociative($sql) as $row) { $conn->executeStatement(…); }`. The Ferro
     * session is strictly SINGLE-IN-FLIGHT — `Session::assertNoOpenStream()` throws on any request
     * while a stream is open — so without this, that idiom would raise a `ProtocolException` that
     * every user would read as a driver bug. With it, pure iteration never buffers and interleaving
     * degrades to buffering, which is what PDO does unconditionally.
     *
     * **The drain is an explicit `valid()/current()/next()` loop and must stay one.** `foreach` over
     * a `Generator` calls `Generator::rewind()`, which THROWS `Cannot rewind a generator that was
     * already run` as soon as the generator has advanced past its first yield (hazard 78) — and
     * {@see fetchNumeric} advances it on every call. A `foreach` here therefore dies on the FIRST
     * real use, from the first line of `exec()`/`runPrepared()`/`beginTransaction()`, i.e. attributed
     * to an innocent statement. (Plan v1 wrote the `foreach`; this is the measured repair, and it is
     * the same loop shape `fetchNumeric` already uses.)
     *
     * Idempotent. Already-fetched rows stay consumed. `$this->rows` is invariantly `[]` for a
     * streamed result — {@see streamed} builds it that way and the streamed branch of
     * {@see fetchNumeric} never appends — so there is no cursor arithmetic to do here; v1's
     * `array_slice($this->rows, $this->cursor)` was a no-op describing a mixed buffered/streamed
     * state this class cannot reach.
     */
    public function materialize(): int
    {
        if ($this->gen === null) {
            return 0;
        }
        $rest = [];
        while ($this->gen->valid()) {
            $rest[] = $this->gen->current();
            $this->gen->next();
        }
        $this->rows = $rest;
        $this->cursor = 0;
        $this->gen = null;
        $this->stream = null;
        return count($rest);
    }

    /**
     * **The abandonment path.** When the consumer stops iterating early, the DBAL-side Generator is
     * destroyed, `Doctrine\DBAL\Result` (which has no `__destruct`) is released with it, and this
     * object becomes unreferenced — because {@see \Ferro\DBAL\Connection} holds only a
     * `\WeakReference` to it. Nothing in DBAL calls `free()` on that path (hazard 80), so this is
     * where the `CANCEL` comes from. Without it, `break`-ing out of a large `iterateAssociative()`
     * would leave the stream open and the NEXT statement would transfer the entire remaining result
     * set — invisible at 100 000 rows in a test, an OOM on a real table.
     *
     * `free()` is idempotent and `Session::abandonStream()` is idempotent by construction
     * (`Session.php:344-353`), so this is safe after a normal drain. The `catch` is not defensive
     * padding: at request shutdown the transport may already be gone, and an exception escaping a
     * destructor during shutdown is a fatal error that would mask whatever actually went wrong.
     */
    public function __destruct()
    {
        try {
            $this->free();
        } catch (\Throwable) {
            // nothing useful can be done from a destructor; the session's own state machine and the
            // engine's ONE-terminal rule (charter rule 4) are what guarantee the stream is closed.
        }
    }
```

and rewrite `fetchNumeric()` / `free()` to serve both modes:

```php
    /** @return list<mixed>|false */
    public function fetchNumeric(): array|false
    {
        if ($this->gen !== null) {
            if (!$this->gen->valid()) {
                $this->gen = null;
                $this->stream = null;
                return false;
            }
            $row = $this->gen->current();
            $this->gen->next();
            return $row;
        }
        return $this->rows[$this->cursor++] ?? false;
    }
```

```php
    /**
     * Idempotent, matching the stock `PgSQL\Result`: afterwards there are no rows and no columns.
     * On a STREAMED result it also abandons the open stream (`CANCEL` + drain to the ONE terminal),
     * without which the next request on this session would read the leftover DATA frames as its own
     * reply.
     */
    public function free(): void
    {
        $this->stream?->close();
        $this->stream = null;
        $this->gen = null;
        $this->rows = [];
        $this->cols = [];
        $this->cursor = 0;
    }
```

- [ ] **Step 4: Make `Connection::query()` stream, and settle an open stream before anything else**

In `php/doctrine-dbal/src/Connection.php`, add the field and the settle helper, then change `query()`, `exec()`, `runPrepared()` and the transaction trio to settle first:

```php
    /**
     * A WEAK reference, and that is the entire abandonment design.
     *
     * A STRONG reference here (plan v1) would be the only thing keeping an abandoned stream alive:
     * `Doctrine\DBAL\Connection::iterateAssociative()` returns
     * `$this->executeQuery(…)->iterateAssociative()`, so the only other reference to the
     * `Doctrine\DBAL\Result` — and through it to this driver `Result` — is the returned Generator's
     * bound `$this`. `Doctrine\DBAL\Result` has no `__destruct` and DBAL never calls the driver's
     * `free()` (hazard 80). So with a strong reference the driver can NEVER tell "the caller
     * abandoned this" from "the caller may still fetch from this", and both end up draining the
     * whole remainder on the next statement.
     *
     * Weakly: when the consumer stops iterating, the Generator dies, the DBAL `Result` dies, the
     * driver `Result` becomes unreferenced, PHP frees it by refcount THERE AND THEN, and its
     * `__destruct` sends the `CANCEL`. `get()` then returns null and this method has nothing to do.
     * When the consumer is still iterating, `get()` returns the live result and it materialises.
     *
     * @var ?\WeakReference<Result> PHPStan level 9 will not infer the generic parameter.
     */
    private ?\WeakReference $openStream = null;

    /**
     * How many rows this connection has had to drain because a streamed result was still open when
     * another statement was issued.
     *
     * **0 for pure iteration and 0 for a properly abandoned iteration**; non-zero only for the
     * interleave idiom, where it is the size of the remainder that had to be buffered. It is what
     * makes the two abandonment cases observable from a test — and it answers a real operator
     * question, which is why it is a public accessor rather than test scaffolding.
     */
    public function settledRowCount(): int
    {
        return $this->settledRows;
    }

    private int $settledRows = 0;

    /**
     * Bring any open streamed `Result` into memory before this connection issues anything else.
     *
     * The Ferro session is strictly single-in-flight: `Session::assertNoOpenStream()` throws on any
     * request while a stream is open. Rather than surface that as a `ProtocolException` — which
     * would break `foreach ($conn->iterateAssociative(…)) { $conn->executeStatement(…); }`, an idiom
     * every Doctrine codebase uses — the open result drains its remainder here. Pure iteration
     * still never buffers; interleaving degrades to what PDO does unconditionally.
     *
     * A result whose caller is GONE is not drained: it has already cancelled itself on destruction.
     */
    private function settleOpenStream(): void
    {
        $ref = $this->openStream;
        $this->openStream = null;
        $open = $ref?->get();
        if ($open instanceof Result) {
            $this->settledRows += $open->materialize();
        }
    }
```

```php
    /**
     * The ZERO-PARAMETER read path. `Doctrine\DBAL\Connection::executeQuery()` calls this directly
     * when there are no parameters, and — crucially — NEVER asks the result for a row count, so
     * this is the one place the driver can stream without breaking anything.
     *
     * **Why the prepared path does not stream.** `executeStatement()` with parameters is
     * `$stmt->execute()->rowCount()`, and a streamed request's terminal carries no `affected` field
     * (the HEAD/DATA/END producer has none), so streaming there would make every parameterized
     * write return 0 — a silently wrong value, which is worse than buffering. Adding `affected` to
     * the stream terminal is a `/proto` change (registry + golden vectors + both codecs) and is
     * DEFERRED, not smuggled in here.
     *
     * **Why MySQL buffers.** `PoolBackend::supports_row_streaming()` is false for MySQL/MariaDB
     * (SPEC §22.2 (n)); the refusal would be a clean `Unsupported`, but paying a round trip to
     * discover it on every query is not worth it when the pool kind is already known.
     */
    public function query(string $sql): ResultInterface
    {
        $this->settleOpenStream();
        if ($this->poolKind !== PlatformVersion::KIND_POSTGRES) {
            return $this->runPrepared($sql, []);
        }
        try {
            $stream = $this->ferro->streamRaw($sql, [], $this->readonly);
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
        $result = Result::streamed($stream);
        // WEAK on purpose — see the field's docblock. The caller's own reference (via
        // `Doctrine\DBAL\Result`) is the one that decides whether this result is still alive.
        $this->openStream = \WeakReference::create($result);
        return $result;
    }
```

Add `$this->settleOpenStream();` as the FIRST line of `exec()`, `runPrepared()`, `beginTransaction()`, `commit()`, `rollBack()` and `getServerVersion()`'s resolution path. `PlatformVersion` needs no import — it shares the `Ferro\DBAL` namespace with `Connection`.

**One consequence to be deliberate about:** `$result` is returned immediately, so the ONLY strong reference is the caller's. A caller that does `$conn->query($sql);` and discards the result (statement position, no assignment) destroys it at the end of that statement, which cancels the stream — which is correct, and identical to what the buffered path already does with its rows. What must NOT happen is the driver quietly taking a strong reference "just to be safe": that is v1's bug, and it converts every early `break` into a full transfer.

- [ ] **Step 5: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS — **6 tests** in `StreamedResultTest` (`testFetchingPullsExactlyOneRowAtATime`, `testColumnsAreAvailableWithoutPullingARow`, `testMaterializeDrainsTheRemainderAndIsIdempotent`, `testFreeClosesTheStreamOnTheWire`, `testDroppingAStreamedResultCancelsTheStream`, `testAStreamedResultReportsNoAffectedCount`) plus everything Tasks 5-11 already added. PHPStan needs `@var \WeakReference<Result>` on the `$openStream` property (level 9 will not infer the generic), and `materialize()`'s return type changed from `void` to `int`, so check that no other caller ignores it in a `void` context.

- [ ] **Step 6: Write the live test — a MEASURED memory bound, and the interleave idiom**

Create `php/doctrine-dbal/tests/Live/StreamingLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/StreamingLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

/**
 * M1-S8b Task 12, live — §14's "`iterateAssociative()` et al. never buffer", MEASURED rather than
 * asserted structurally.
 *
 * A functional assertion ("the rows come out in order") passes just as well over a fully-buffered
 * result, so the guard here is a MEMORY DELTA. **It has to be PEAK and/or MID-LOOP, never the
 * residual after the loop** (hazard 79): `Doctrine\DBAL\Connection::iterateAssociative()` returns
 * `$this->executeQuery(…)->iterateAssociative()`, so the only reference to the buffered rows is the
 * Generator's bound `$this`, and PHP releases the whole array when the `foreach` ends — BEFORE a
 * post-loop `memory_get_usage()` runs. Measured over dbal 4.4.4's real code paths at this exact row
 * count and shape, plan v1's post-loop metric was **552 B streamed vs 472 B buffered — both green**,
 * i.e. the headline guard for the whole task could not fail. The same run's peak was **2 728 B vs
 * 34 109 720 B** (~12 500×) and its mid-loop sample **2 040 B vs 33 302 432 B**.
 *
 * The two are compared in the same process against the same query, so the threshold is a ratio
 * rather than an absolute number and does not depend on the machine. The assertion is on
 * `max(peak, midLoop)` for each mode so the guard is not hostage to one metric — if a future PHP
 * changes how peak is accounted, the mid-loop sample still discriminates, and vice versa.
 */
final class StreamingLiveTest extends DbalLiveTestCase
{
    private const ROWS = 100_000;

    private function seed(\Doctrine\DBAL\Connection $c): void
    {
        $c->executeStatement('DROP TABLE IF EXISTS s8b_stream');
        $c->executeStatement('CREATE TABLE s8b_stream (id int primary key, note text)');
        $c->executeStatement(
            'INSERT INTO s8b_stream SELECT g, repeat(\'x\', 64) FROM generate_series(1, ' . self::ROWS . ') g',
        );
    }

    public function testIteratingDoesNotBufferWhileFetchAllDoes(): void
    {
        $c = $this->dbal();
        $this->seed($c);
        $sql = 'SELECT id, note FROM s8b_stream ORDER BY id';

        // ---- STREAMED. Peak is reset first (PHP >= 8.2, which is this package's floor), and a
        // sample is taken mid-loop while the rows — if they were being buffered — would still be
        // held. Asserting on max() of the two means neither metric alone has to carry the guard.
        gc_collect_cycles();
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $seen = 0;
        $streamedMid = 0;
        foreach ($c->iterateAssociative($sql) as $row) {
            self::assertSame($seen + 1, $row['id']);
            ++$seen;
            if ($seen === intdiv(self::ROWS, 2)) {
                $streamedMid = memory_get_usage() - $before;
            }
        }
        $streamed = max(memory_get_peak_usage() - $before, $streamedMid);
        self::assertSame(self::ROWS, $seen, 'every row arrived, in order');

        // ---- BUFFERED, measured exactly the same way, in the same process, on the same query.
        gc_collect_cycles();
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $all = $c->fetchAllAssociative($sql);
        $bufferedMid = memory_get_usage() - $before;      // still holding $all
        $buffered = max(memory_get_peak_usage() - $before, $bufferedMid);
        self::assertCount(self::ROWS, $all);
        unset($all);

        // The measured separation is ~12 500x (2 728 B vs 34 109 720 B peak at these dimensions), so
        // a 1/50 threshold is two orders of magnitude of headroom below the real gap and still
        // catches any implementation that materialises — a buffering `query()` lands at ~1x.
        // Grounded in that measurement rather than picked: see the class docblock.
        self::assertGreaterThan(
            10_000_000,
            $buffered,
            'the BUFFERED arm must actually buffer, or the comparison below is vacuous',
        );
        self::assertLessThan(
            intdiv($buffered, 50),
            max($streamed, 1),
            sprintf(
                'iterating must not buffer: streamed peaked at %d bytes (mid-loop %d), fetchAll at '
                . '%d (mid-loop %d) — if these are comparable, iterateAssociative() is materialising '
                . 'and §14 is unmet',
                $streamed,
                $streamedMid,
                $buffered,
                $bufferedMid,
            ),
        );

        $c->executeStatement('DROP TABLE s8b_stream');
    }

    /**
     * THE INTERLEAVE IDIOM. The Ferro session is single-in-flight, so the inner statement would
     * throw a `ProtocolException` without `settleOpenStream()`. This is the test that keeps the
     * streaming optimisation from shipping as a user-visible defect.
     */
    public function testWritingInsideAnIterationWorks(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_inter');
        $c->executeStatement('CREATE TABLE s8b_inter (id int primary key, n int)');
        $c->executeStatement('INSERT INTO s8b_inter SELECT g, 0 FROM generate_series(1, 200) g');

        $touched = 0;
        foreach ($c->iterateAssociative('SELECT id FROM s8b_inter ORDER BY id') as $row) {
            $c->executeStatement('UPDATE s8b_inter SET n = 1 WHERE id = ?', [$row['id']]);
            ++$touched;
        }
        self::assertSame(200, $touched);
        self::assertSame(200, (int) $c->fetchOne('SELECT count(*) FROM s8b_inter WHERE n = 1'));

        // …and it worked by MATERIALISING, which is the honest description of what interleaving
        // costs. Asserted so the two paths are told apart: the abandonment test below asserts the
        // opposite on the same counter, and one of them being wrong makes the other red.
        self::assertGreaterThan(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'the interleave idiom degrades to buffering — that is the documented cost',
        );

        $c->executeStatement('DROP TABLE s8b_inter');
    }

    /**
     * **Abandoning an iteration CANCELS it** — and the assertion that says so is the row counter,
     * not "the connection still works".
     *
     * Plan v1's version of this test asserted only that a later statement succeeded, and it passed
     * through `materialize()`: the driver held a STRONG reference to the open result, so `break`
     * cancelled nothing and the next statement quietly transferred the remaining 99 975 rows. It
     * went green while silently blessing an OOM trap, and its named mutation (deleting `close()`
     * from `free()`) could not fail because `free()` was never reached.
     *
     * With the `\WeakReference` the caller's `break` destroys the driver `Result`, whose
     * `__destruct` sends the `CANCEL`. `settledRowCount()` is how that is observed: **0** here,
     * non-zero in the interleave test above.
     */
    public function testAbandoningAnIterationCancelsInsteadOfDrainingTheRemainder(): void
    {
        $c = $this->dbal();
        $this->seed($c);

        $seen = 0;
        foreach ($c->iterateAssociative('SELECT id, note FROM s8b_stream ORDER BY id') as $_row) {
            if (++$seen === 25) {
                break;
            }
        }
        self::assertSame(25, $seen);

        // The connection is usable — necessary, and on its own not sufficient.
        self::assertSame(self::ROWS, (int) $c->fetchOne('SELECT count(*) FROM s8b_stream'));

        // THE assertion: the remainder was never transferred.
        self::assertSame(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'an abandoned iteration must CANCEL, not drain 99 975 rows into memory on the next statement',
        );

        $c->executeStatement('DROP TABLE s8b_stream');
    }

    /**
     * `Doctrine\DBAL\Connection::connect()` is `protected`, but `getNativeConnection()` hands back
     * the `Ferro\Client\Connection`, not our driver `Connection` — so reach the driver connection
     * the way DBAL's own tests do, through the wrapper's protected accessor.
     */
    private function driverConnection(\Doctrine\DBAL\Connection $c): \Ferro\DBAL\Connection
    {
        $driver = (new \ReflectionMethod($c, 'connect'))->invoke($c);
        self::assertInstanceOf(\Ferro\DBAL\Connection::class, $driver);
        return $driver;
    }

    /**
     * MySQL BUFFERS, and that is a documented asymmetry rather than a defect (SPEC §22.2 (n) —
     * MySQL row streaming is deferred). Asserted so the asymmetry is known and so the day MySQL
     * streaming lands, this test is what says "now change the driver too".
     */
    public function testMysqlIteratesCorrectlyEvenThoughItBuffers(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_stream_my');
        $c->executeStatement('CREATE TABLE s8b_stream_my (id INT PRIMARY KEY)');
        $c->executeStatement('INSERT INTO s8b_stream_my (id) VALUES (1),(2),(3)');

        $ids = [];
        foreach ($c->iterateAssociative('SELECT id FROM s8b_stream_my ORDER BY id') as $row) {
            $ids[] = (int) $row['id'];
        }
        self::assertSame([1, 2, 3], $ids);

        $c->executeStatement('DROP TABLE s8b_stream_my');
    }
}
```

- [ ] **Step 7: Run the live test**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/StreamingLiveTest.php --fail-on-skipped
```
Expected: PASS (4 tests). Record the four measured byte counts from the memory assertion (streamed peak/mid, buffered peak/mid) in the commit message — they are the evidence the claim rests on, and a later reader needs them to tell a regression from a machine difference.

- [ ] **Step 8: MUTATION-PROVE the guards**

Five mutations. **Every one has been checked against the code this task writes** — plan v1 shipped four, of which three could not redden anything (one was a provable no-op, one tested a path that was never taken, one asserted a metric that is ~0 in both arms).

1. In `Connection::query()`, return `$this->runPrepared($sql, [])` unconditionally (never stream). Re-run the live test: RED on `testIteratingDoesNotBufferWhileFetchAllDoes` — the streamed arm now peaks at the buffered arm's ~34 MB and the `assertLessThan(buffered/50)` fails. Restore. **This is the mutation that proves §14's requirement is actually met and not merely claimed**, and it only bites because the metric is peak/mid-loop: with v1's post-loop `memory_get_usage()` this mutation left the test GREEN (measured: 552 B streamed vs 472 B buffered).
2. Delete the `settleOpenStream()` call from `runPrepared()`. Re-run: RED on `testWritingInsideAnIterationWorks` with a `ProtocolException` about an open stream. Restore.
3. In `Result::materialize()`, replace the `while ($this->gen->valid())` drain with `foreach ($this->gen as $row) { $rest[] = $row; }`. Re-run the unit test: RED on `testMaterializeDrainsTheRemainderAndIsIdempotent` with `Exception: Cannot rewind a generator that was already run`, and RED on the live `testWritingInsideAnIterationWorks` for the same reason. Restore. (This replaces v1's mutation #3, which asked the implementer to drop an `array_slice($this->rows, $this->cursor)` where `$this->rows === []` and `$this->cursor === 0` invariantly — it rewrote `array_merge([], $rest)` to `array_merge([], $rest)`. That `array_slice` is gone from v2 along with the docblock sentence describing the state it implied.)
4. Delete `$this->stream?->close()` from `free()`. Re-run the unit test: RED on `testFreeClosesTheStreamOnTheWire` and on `testDroppingAStreamedResultCancelsTheStream` (`abandonCount` stays 0). Re-run the live test: RED on `testAbandoningAnIterationCancelsInsteadOfDrainingTheRemainder` — the stream is never abandoned, so the next statement's `assertNoOpenStream()` raises a `ProtocolException`. Restore. (v1's version of this mutation could not fail: `free()` was never called on that path at all.)
5. **The abandonment design itself.** In `Connection::query()`, change `$this->openStream = \WeakReference::create($result);` to a strong `$this->openStream = $result;` (and the `?->get()` in `settleOpenStream()` accordingly). Re-run the live test: RED on `testAbandoningAnIterationCancelsInsteadOfDrainingTheRemainder` — `settledRowCount()` comes back **99 975** instead of 0, because the strong reference kept the result alive and the next statement drained the whole remainder. Restore. This is the mutation that reproduces v1's actual behaviour, and the number it prints is the OOM trap stated in rows.

- [ ] **Step 9: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src/Result.php php/doctrine-dbal/src/Connection.php php/doctrine-dbal/tests
git commit -m "feat(m1-s8b): iterate*() streams on the parameterless read path; an interleaved statement still works

Doctrine's Result::iterateAssociative() is a loop over fetchAssociative(), so
never-buffer reduces to pulling one row at a time. The driver streams on query()
— the zero-parameter executeQuery path, where DBAL never asks for a row count —
and buffers on the prepared path, because executeStatement() RETURNS
rowCount() and a streamed terminal carries no affected field. Adding one is a
/proto change and stays deferred.

The session is single-in-flight, so an open stream would make the canonical
batch idiom (iterate + write) throw. The open result drains its remainder before
any other statement: pure iteration never buffers, interleaving degrades to what
PDO does unconditionally.

The driver holds a WeakReference to the open Result, and the Result frees itself
on destruction. That is what makes abandonment actually CANCEL: DBAL's Result has
no __destruct and never calls the driver's free(), so a strong reference here
would be the only thing keeping an abandoned stream alive and `break` out of a
large iterateAssociative() would transfer the entire remainder on the next
statement. settledRowCount() makes the two cases observable — 0 for pure
iteration and for abandonment, non-zero only for interleaving.

MySQL buffers (SS22.2 (n), streaming deferred) — a documented asymmetry with its
own test, so the day MySQL streaming lands there is something that says so.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 13: Isolation — capture it typed in a `wrapperClass`, and REFUSE the raw statement

`Doctrine\DBAL\Connection::setTransactionIsolation()` caches the level and runs `executeStatement($platform->getSetTransactionIsolationSQL($level))` — `SET SESSION TRANSACTION ISOLATION LEVEL …` on MySQL, `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …` on PostgreSQL. Those are exactly the two forms SPEC §22.2 (s) names as FORBIDDEN, because the SESSION form persists on the pooled connection past COMMIT. Under Ferro today the statement taints the checkout and hygiene wipes it, so **it appears to succeed and has no effect on any later transaction** while `getTransactionIsolation()` keeps reporting the level DBAL cached. §22.2 (s) also records that the obvious "did the next tenant inherit it" test CANNOT FAIL — hygiene masks the leak either way.

Two changes, in this order of preference:

- **Capture it TYPED, above the SQL.** `Ferro\DBAL\Wrapper\FerroConnection extends Doctrine\DBAL\Connection` overrides `setTransactionIsolation()`, stores the level, emits **no SQL**, and hands it to the driver connection for the next `BEGIN` — where Task 3's `begin(readonly, isolation)` puts it on `BeginRequest.isolation` and the engine composes the correct per-transaction form for the dialect. There is no statement inspection anywhere in this path: the wrapper receives a `TransactionIsolationLevel` enum. It is enabled with one more line of the same config array (`'wrapperClass' => …`), so config-only adoption holds.
- **Refuse the raw statement when the wrapper is absent.** The driver connection recognises the two fixed platform-generated prefixes and throws. This is a REFUSAL, not a rewrite: nothing is modified or re-emitted, and the alternative is the silent wrong-isolation this task exists to eliminate. It also catches an application that writes the statement itself.

**Files:**
- Create: `php/doctrine-dbal/src/Wrapper/FerroConnection.php`, `php/doctrine-dbal/src/Exception/UnsupportedStatement.php`
- Modify: `php/doctrine-dbal/src/Connection.php` (`setIsolation()`, `beginTransaction()`, the refusal guard)
- Test: `php/doctrine-dbal/tests/Unit/IsolationRefusalTest.php` (Create), `php/doctrine-dbal/tests/Live/IsolationLiveTest.php` (Create)

**Interfaces:**
- Produces: `Ferro\DBAL\Wrapper\FerroConnection extends Doctrine\DBAL\Connection` — overrides `setTransactionIsolation(TransactionIsolationLevel): void` and `getTransactionIsolation(): TransactionIsolationLevel`. `Ferro\DBAL\Connection::setIsolation(?Ferro\Protocol\Isolation): void`. `Ferro\DBAL\Wrapper\FerroConnection::isIsolationStatement(string $sql): bool` — **public static**, on the WRAPPER class, because both callers need it: the unit test asserts the closed prefix set directly, and `Ferro\DBAL\Connection::exec()`/`runPrepared()` call it to raise the refusal when no wrapper is configured. (v1's Interfaces line named it `Ferro\DBAL\Connection::isolationStatement` and called it private; the code in Steps 1, 4 and 5 and the Step-8 mutation all use the name above. This is the spelling.) `Ferro\DBAL\Exception\UnsupportedStatement::isolation(string $sql): self`.
- Consumes: Task 3's `Ferro\Client\Connection::begin(bool $readonly, ?Isolation $isolation)`; `Ferro\Protocol\Isolation`; `Doctrine\DBAL\TransactionIsolationLevel`.

- [ ] **Step 1: Write the failing unit test**

Create `php/doctrine-dbal/tests/Unit/IsolationRefusalTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Unit/IsolationRefusalTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\Exception\UnsupportedStatement;
use Ferro\DBAL\Wrapper\FerroConnection;
use Ferro\Protocol\Isolation;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 13 — the isolation statement, refused; and the level, mapped.
 *
 * The strings the refusal matches are NOT invented here: they are generated by the STOCK platforms
 * below, so a DBAL release that changes their wording makes this test red instead of silently
 * turning the refusal into a no-op — which would restore the silent wrong-isolation bug.
 */
final class IsolationRefusalTest extends TestCase
{
    /** @return array<string, array{0: string}> */
    public static function stockIsolationSql(): array
    {
        $out = [];
        foreach (TransactionIsolationLevel::cases() as $level) {
            $out['pg ' . $level->name] = [(new PostgreSQL120Platform())->getSetTransactionIsolationSQL($level)];
            $out['mysql ' . $level->name] = [(new MySQL84Platform())->getSetTransactionIsolationSQL($level)];
        }
        return $out;
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('stockIsolationSql')]
    public function testEveryStockIsolationStatementIsRecognised(string $sql): void
    {
        self::assertTrue(
            FerroConnection::isIsolationStatement($sql),
            "the driver must recognise the statement Doctrine actually emits: $sql",
        );
    }

    /** …and nothing else is. A refusal that fired on ordinary SQL would be far worse than the bug. */
    public function testOrdinarySqlIsNotMistakenForIt(): void
    {
        foreach ([
            'SELECT 1',
            "UPDATE t SET note = 'SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE'",
            'SET LOCAL statement_timeout = 100',
            "INSERT INTO log (msg) VALUES ('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED')",
        ] as $sql) {
            self::assertFalse(FerroConnection::isIsolationStatement($sql), "must NOT match: $sql");
        }
    }

    public function testTheRefusalNamesTheWrapperAsTheFix(): void
    {
        $e = UnsupportedStatement::isolation('SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE');
        self::assertStringContainsString('wrapperClass', $e->getMessage());
        self::assertStringContainsString(FerroConnection::class, $e->getMessage());
    }

    /**
     * The `TransactionIsolationLevel` → `Ferro\Protocol\Isolation` mapping, DERIVED from the DBAL
     * enum so a new case fails here rather than being silently dropped.
     *
     * `READ_UNCOMMITTED` maps to `ReadCommitted`, which is what `Ferro\Protocol\Isolation`'s own
     * docblock specifies (PostgreSQL treats them as the same level). On MySQL that is a genuine
     * UPGRADE to a stricter level — never a weaker one — and it is listed in
     * `docs/known-incompatibilities.md`.
     */
    public function testEveryDbalLevelMaps(): void
    {
        $expected = [
            'READ_UNCOMMITTED' => Isolation::ReadCommitted,
            'READ_COMMITTED' => Isolation::ReadCommitted,
            'REPEATABLE_READ' => Isolation::RepeatableRead,
            'SERIALIZABLE' => Isolation::Serializable,
        ];
        foreach (TransactionIsolationLevel::cases() as $case) {
            self::assertArrayHasKey($case->name, $expected, "unmapped TransactionIsolationLevel::{$case->name}");
            self::assertSame($expected[$case->name], FerroConnection::toFerroIsolation($case));
        }
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit/IsolationRefusalTest.php
```
Expected: FAIL — `Error: Class "Ferro\DBAL\Wrapper\FerroConnection" not found`.

- [ ] **Step 3: Create the refusal exception and the wrapper**

Create `php/doctrine-dbal/src/Exception/UnsupportedStatement.php`:

```php
<?php // /php/doctrine-dbal/src/Exception/UnsupportedStatement.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Ferro\DBAL\Wrapper\FerroConnection;

/**
 * A statement Ferro refuses to run, because running it would SUCCEED and do nothing.
 */
final class UnsupportedStatement extends AbstractException
{
    public static function isolation(string $sql): self
    {
        return new self(sprintf(
            'Ferro refuses this statement: %s. On a transaction-mode pool a session-level isolation '
            . 'setting is meaningless — it lands on whichever pooled connection the checkout hands '
            . 'out, taints it, and is wiped by connection hygiene before the next BEGIN, so the '
            . 'statement would report success and have no effect on any later transaction. Ferro '
            . 'carries isolation per-TRANSACTION instead: add '
            . '\'wrapperClass\' => %s::class to this connection\'s configuration and '
            . 'Doctrine\'s setTransactionIsolation() will be honoured on the next '
            . 'beginTransaction(). Refused rather than ignored because a silently wrong isolation '
            . 'level is the failure this engine exists to prevent.',
            $sql,
            FerroConnection::class,
        ));
    }
}
```

Create `php/doctrine-dbal/src/Wrapper/FerroConnection.php`:

```php
<?php // /php/doctrine-dbal/src/Wrapper/FerroConnection.php
declare(strict_types=1);
namespace Ferro\DBAL\Wrapper;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\Protocol\Isolation;

/**
 * The optional `wrapperClass` that makes `setTransactionIsolation()` actually work.
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'  => Ferro\DBAL\Driver::class,
 *     'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class,
 *     'unix_socket'  => '/run/ferro/app.sock',
 * ]],
 * ```
 *
 * **Why it exists.** Doctrine's own `setTransactionIsolation()` runs
 * `executeStatement($platform->getSetTransactionIsolationSQL($level))` — the SESSION form. On a
 * transaction-mode pool that statement lands on an arbitrary pooled connection, taints it, and is
 * wiped by hygiene before the next `BEGIN`: it reports success and changes nothing, while
 * `getTransactionIsolation()` keeps returning the level Doctrine cached. SPEC §22.2 (s) names both
 * spellings as the FORBIDDEN form for exactly this reason, and records that the obvious "did the
 * next tenant inherit it" test cannot fail because hygiene masks the leak either way.
 *
 * This override captures the level as a TYPED enum, above the SQL layer, and hands it to the driver
 * connection to ride `BeginRequest.isolation` on the next transaction — where the engine composes
 * the correct PER-TRANSACTION form for the dialect (`BEGIN ISOLATION LEVEL …` on PostgreSQL, the
 * batched `SET TRANSACTION …; START TRANSACTION …` on MySQL). **No SQL is inspected, rewritten or
 * generated here** — charter rule 6 is untouched; the wrapper simply never emits the statement.
 */
class FerroConnection extends DbalConnection
{
    private ?TransactionIsolationLevel $ferroLevel = null;

    public function setTransactionIsolation(TransactionIsolationLevel $level): void
    {
        $inner = $this->connect();
        if (!$inner instanceof FerroDriverConnection) {
            // Wrapping a non-Ferro driver: behave exactly like stock Doctrine.
            parent::setTransactionIsolation($level);
            return;
        }
        $this->ferroLevel = $level;
        $inner->setIsolation(self::toFerroIsolation($level));
    }

    public function getTransactionIsolation(): TransactionIsolationLevel
    {
        return $this->ferroLevel ?? parent::getTransactionIsolation();
    }

    /**
     * DBAL's level → Ferro's wire enum.
     *
     * `READ_UNCOMMITTED` becomes `ReadCommitted`, which is what `Ferro\Protocol\Isolation`'s own
     * docblock specifies: PostgreSQL treats the two as the same level and the wire enum has no
     * fourth value. On MySQL that is a genuine UPGRADE to a stricter level — never a weaker one —
     * and it is recorded in `docs/known-incompatibilities.md`.
     */
    public static function toFerroIsolation(TransactionIsolationLevel $level): Isolation
    {
        return match ($level) {
            TransactionIsolationLevel::READ_UNCOMMITTED,
            TransactionIsolationLevel::READ_COMMITTED => Isolation::ReadCommitted,
            TransactionIsolationLevel::REPEATABLE_READ => Isolation::RepeatableRead,
            TransactionIsolationLevel::SERIALIZABLE => Isolation::Serializable,
        };
    }

    /**
     * Whether `$sql` is one of the two isolation statements Doctrine's platforms generate.
     *
     * A CLOSED, prefix-anchored test on the two fixed strings — not open-ended SQL parsing. It is
     * anchored so a literal appearing inside an INSERT or a comparison cannot trip it, which
     * matters: a refusal that fired on ordinary SQL would be far worse than the bug it prevents.
     */
    public static function isIsolationStatement(string $sql): bool
    {
        $t = ltrim($sql);
        foreach ([
            'SET SESSION TRANSACTION ISOLATION LEVEL',
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL',
        ] as $prefix) {
            if (strncasecmp($t, $prefix, strlen($prefix)) === 0) {
                return true;
            }
        }
        return false;
    }
}
```

- [ ] **Step 4: Teach the driver connection to carry the level and to refuse the statement**

In `php/doctrine-dbal/src/Connection.php`, add the field, the setter, the guard, and pass the level to `begin()`:

```php
    private ?Isolation $pendingIsolation = null;

    /**
     * The isolation level the NEXT {@see beginTransaction} will carry, set by
     * {@see \Ferro\DBAL\Wrapper\FerroConnection::setTransactionIsolation}. Null means the pool
     * default.
     *
     * It is sticky, matching Doctrine's own semantics: `setTransactionIsolation()` applies to every
     * subsequent transaction, not just the next one.
     */
    public function setIsolation(?Isolation $isolation): void
    {
        $this->pendingIsolation = $isolation;
    }
```

```php
    public function beginTransaction(): void
    {
        $this->settleOpenStream();
        try {
            $this->ferro->begin($this->readonly, $this->pendingIsolation);
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }
```

and add the refusal at the top of BOTH statement entry points (`exec()` and `runPrepared()`), immediately after `settleOpenStream()`:

```php
        if (FerroConnection::isIsolationStatement($sql)) {
            // Refused, not ignored and not rewritten. Left alone this statement SUCCEEDS and does
            // nothing: it lands on an arbitrary pooled connection, taints it, and hygiene wipes the
            // level before the next BEGIN — so the application asks for SERIALIZABLE and silently
            // gets the pool default (SPEC §22.2 (s), which also records that the obvious
            // "did the next tenant inherit it" test cannot fail, because hygiene masks it either
            // way). The message names the one-line configuration fix.
            throw UnsupportedStatement::isolation($sql);
        }
```

Add `use Ferro\DBAL\Exception\UnsupportedStatement;`, `use Ferro\DBAL\Wrapper\FerroConnection;` and `use Ferro\Protocol\Isolation;` to the imports.

- [ ] **Step 5: Run the unit test — it must now pass**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && ./vendor/bin/phpunit tests/Unit && ./vendor/bin/phpstan analyse src --level 9
```
Expected: PASS.

- [ ] **Step 6: Write the live test — a BEHAVIOURAL isolation proof**

Create `php/doctrine-dbal/tests/Live/IsolationLiveTest.php`:

```php
<?php // /php/doctrine-dbal/tests/Live/IsolationLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\Exception\UnsupportedStatement;
use Ferro\DBAL\Wrapper\FerroConnection;

/**
 * M1-S8b Task 13, live.
 *
 * The isolation assertion is made from a vantage point where it is OBSERVABLE:
 * `current_setting('transaction_isolation')` INSIDE the open transaction. SPEC §22.2 (s) records
 * why the tempting alternatives cannot fail — a session-variable read-back reports the session
 * default whatever happened, and a "did the next tenant inherit it" check is masked by hygiene in
 * both directions.
 */
final class IsolationLiveTest extends DbalLiveTestCase
{
    private function wrapped(): \Doctrine\DBAL\Connection
    {
        $c = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'wrapperClass' => FerroConnection::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'default'],
        ]);
        self::assertInstanceOf(\Ferro\Client\Connection::class, $c->getNativeConnection());
        return $c;
    }

    public function testTheWrapperMakesSetTransactionIsolationTakeEffect(): void
    {
        $c = $this->wrapped();
        $c->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);

        $c->beginTransaction();
        self::assertSame(
            'serializable',
            $c->fetchOne("SELECT current_setting('transaction_isolation')"),
            'the level must reach the BEGIN itself, not a session variable',
        );
        $c->commit();

        // Sticky, matching Doctrine's semantics: it applies to EVERY subsequent transaction.
        $c->beginTransaction();
        self::assertSame('serializable', $c->fetchOne("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        $c->setTransactionIsolation(TransactionIsolationLevel::READ_COMMITTED);
        $c->beginTransaction();
        self::assertSame('read committed', $c->fetchOne("SELECT current_setting('transaction_isolation')"));
        $c->commit();
    }

    /**
     * WITHOUT the wrapper the raw statement is REFUSED. The alternative — letting it through — is
     * the silent no-op this whole task exists to eliminate, and it is invisible: the statement
     * succeeds, `getTransactionIsolation()` reports the requested level, and every later
     * transaction runs at the pool default.
     */
    public function testWithoutTheWrapperTheRawIsolationStatementIsRefusedLoudly(): void
    {
        $c = $this->dbal();
        try {
            $c->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);
            self::fail('the raw SET SESSION … statement must be refused, not silently ignored');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(UnsupportedStatement::class, $e->getPrevious());
            self::assertStringContainsString('wrapperClass', $e->getMessage());
        }

        // …and the connection is still perfectly usable afterwards.
        self::assertSame(1, $c->fetchOne('SELECT 1'));
    }

    /** The same, on MySQL, where the statement text differs and the level genuinely differs too. */
    public function testTheWrapperAlsoWorksOnMysql(): void
    {
        $pool = $this->requireMysqlPool();
        $a = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'wrapperClass' => FerroConnection::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => $pool],
        ]);
        $b = $this->dbal($pool);

        $a->executeStatement('DROP TABLE IF EXISTS s8b_iso_dbal');
        $a->executeStatement('CREATE TABLE s8b_iso_dbal (id INT PRIMARY KEY, v INT) ENGINE=InnoDB');
        $a->executeStatement('INSERT INTO s8b_iso_dbal (id, v) VALUES (1, 1)');
        $b->executeStatement('SET SESSION innodb_lock_wait_timeout = 1');

        $a->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);
        $a->beginTransaction();
        self::assertSame(1, (int) $a->fetchOne('SELECT v FROM s8b_iso_dbal WHERE id = 1'));

        // Under SERIALIZABLE that plain SELECT took a shared lock, so B must block and time out.
        $blocked = false;
        try {
            $b->executeStatement('UPDATE s8b_iso_dbal SET v = 2 WHERE id = 1');
        } catch (\Doctrine\DBAL\Exception\RetryableException) {
            $blocked = true;
        }
        self::assertTrue($blocked, 'SERIALIZABLE must make the read block a concurrent write');

        $a->commit();
        $a->executeStatement('DROP TABLE s8b_iso_dbal');
    }
}
```

Note: `$b->executeStatement('SET SESSION innodb_lock_wait_timeout = 1')` is NOT an isolation statement and is therefore not refused; it taints the checkout and hygiene wipes it, which is fine for the duration of one test. If a future task broadens the refusal to all `SET SESSION`, this fixture must move to an engine-side setting.

- [ ] **Step 7: Run the live test**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod \
./vendor/bin/phpunit tests/Live/IsolationLiveTest.php --fail-on-skipped
```
Expected: PASS (3 tests).

- [ ] **Step 8: MUTATION-PROVE the guards**

1. In `FerroConnection::setTransactionIsolation`, call `parent::setTransactionIsolation($level)` instead of the typed capture. Re-run the live test: RED on `testTheWrapperMakesSetTransactionIsolationTakeEffect` — **and the failure mode is the bug itself**: without the driver's refusal it would have been a silent `read committed`; with the refusal it is now a loud `UnsupportedStatement`. Record both. Restore.
2. Delete the `isIsolationStatement` guard from `exec()`/`runPrepared()`. Re-run: RED on `testWithoutTheWrapperTheRawIsolationStatementIsRefusedLoudly`. Restore.
3. Change `strncasecmp(...) === 0` to `stripos($t, $prefix) !== false` (unanchored). Re-run the unit test: RED on `testOrdinarySqlIsNotMistakenForIt` (the `INSERT INTO log` row). Restore. This is the false-positive direction, and it matters more than the false-negative one.
4. Change `toFerroIsolation`'s `SERIALIZABLE` arm to `Isolation::RepeatableRead`. Re-run: RED on `testEveryDbalLevelMaps` AND on the live PG assertion. Restore. This is the §22.2 (w) hand-duplicated-enum hazard finally under a real caller.

- [ ] **Step 9: Commit**

```bash
cd /home/abdullak/projects/ferro
git add php/doctrine-dbal/src php/doctrine-dbal/tests
git commit -m "feat(m1-s8b): isolation is captured TYPED in a wrapperClass, and the raw statement is refused

Doctrine's setTransactionIsolation() runs SET SESSION TRANSACTION ISOLATION
LEVEL (or PG's SET SESSION CHARACTERISTICS AS ...), the two forms SS22.2 (s) names
as forbidden. On a transaction-mode pool it lands on an arbitrary connection,
taints it, and hygiene wipes it before the next BEGIN — so it reports success and
changes nothing, while getTransactionIsolation() keeps reporting the level
Doctrine cached. SS22.2 (s) also records that the obvious cross-tenant test cannot
fail, because hygiene masks the leak either way.

Ferro\\DBAL\\Wrapper\\FerroConnection captures the level as a typed enum ABOVE the
SQL layer and rides BeginRequest.isolation on the next transaction, where the
engine composes the correct per-transaction form for the dialect. No SQL is
inspected or rewritten — the wrapper simply never emits the statement.

Without the wrapper the driver REFUSES the statement, anchored on the two fixed
platform-generated prefixes, naming the one-line configuration fix. Refused
rather than ignored: a silently wrong isolation level is the failure this engine
exists to prevent.

Proven behaviourally — current_setting() inside the open transaction on PG, and a
real lock conflict on MySQL.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 14: Acceptance — the upstream subset with a HARD contact assertion, the recorded numbers, and the SPEC amendments the measurements force

§14's stated bar is "DBAL 4 functional test suite green on PG + MySQL + SQLite; ORM functional suite green on PG + MySQL". Four measured facts make that unreachable as written, and the plan's obligation is to say so explicitly, scope what IS reachable, and record the gap — never to quietly narrow the claim:

1. **Ferro has no SQLite backend** (hazard 77). One third of the bar is impossible in M1.
2. **The upstream suite cannot select a third-party `driverClass`** (hazard 72/measured): `TestUtil::getConnectionParams()` checks only `$params['driver']` and silently returns `['driver' => 'pdo_sqlite', 'memory' => true]`. `pdo_sqlite` is loaded, so the entire functional suite would run GREEN against in-memory SQLite with **zero Ferro contact**, and neither `--fail-on-skipped` nor `assert-no-skips.sh` would notice.
3. **MySQL cannot run the suite at all today** (hazard 74/measured): `TestUtil::initializeDatabase()` does `dropDatabase`/`createDatabase` and the testkit `ferro` user has only `GRANT ALL ON ferro.*` — 1057 of 1077 functional tests ERROR at setup.
4. **Even the STOCK `pdo_pgsql` driver is not green on our containers** (hazard 73/measured): 1 failure, environmental. The bar has to be "green modulo a recorded, explained list" against measured denominators (~565 executing PG functional tests), not "green".

And a fifth, found by the v1 verification pass and every bit as disqualifying:

5. **A suite with no reset produces a number that DEGRADES on every run** (hazard 85/measured): with v1's no-op `initializeDatabase()` and a KNOWN-GOOD driver, the identical command gave `Errors 23, Failures 3` and then `Errors 33, Failures 1`, while upstream's `TestUtil` gave `0/0` before and after — causation proven. **A recorded number that gets worse the more often you run it is worse than no number at all**, because the triage table then attributes leftover-state noise to the driver and an implementer under "fix every (a)" pressure chases phantom defects. v1 also pointed the suite at the SHARED `ferro` database, which every other live suite in this repo uses: ~40 tables, 8+ sequences, 5 schemas, a domain type and several views created and abandoned in it, permanently, with nothing ever cleaning them. Both are fixed in Steps 1, 2 and 5: PostgreSQL gets its OWN `doctrine_tests` database, the runner performs a container-side reset before phpunit, and "started from a fresh reset" is part of the recorded environment manifest.

**Files:**
- Create: `testkit/dbal-suite.sh`, `testkit/dbal/TestUtil.ferro.php`, `testkit/dbal/bootstrap.php`, `testkit/dbal/allowlist.txt`, `testkit/dbal/phpunit.ferro.xml`, `testkit/dbal/reset-pg.sql`, `testkit/dbal/reset-mysql.sql`
- Create: `docs/dbal-suite/2026-08-10-results.md`, `docs/known-incompatibilities.md`, `php/doctrine-dbal/README.md`
- Modify: `testkit/mysql-init.sql`, `testkit/postgres/init.sql`, `ferro-spec-v0.2.md` (§14 + §22.2), `CLAUDE.md`, `php/client/src/Client/Value/RawStringValuePolicy.php`, `engine/crates/ferrod/src/services/sql.rs` (the `abort_stream` docblock — see Step 10)

**Interfaces:**
- Produces: `testkit/dbal-suite.sh [--pool <name>] [--dsn <url>]` — clones a PINNED `doctrine/dbal` tag, patches its `TestUtil`, launches ONE `ferrod` for the run, asserts driver identity, and executes the allowlisted `tests/Functional` paths.
- Consumes: everything from Tasks 1–13.

- [ ] **Step 1: Give the suite its OWN database on BOTH families**

`TestUtil::initializeDatabase()` drops and creates a database on every run. Under Ferro the DSN lives in the engine and PHP holds no credentials (D8), so the patched `TestUtil` (Step 2) cannot do that — but the suite still needs a database, and **it must not be the shared `ferro` one**. The upstream functional suite creates and abandons ~40 tables, 8+ sequences, 5 schemas, a domain type and several views; pointing it at `ferro` would silt up the database every other live suite in this repo uses, permanently, with no step that ever cleans it.

MySQL/MariaDB — append to `testkit/mysql-init.sql`:

```sql
-- M1-S8b: the upstream Doctrine DBAL functional suite gets its OWN database. Ferro's patched
-- TestUtil does NOT drop/create it (PHP holds no credentials — SPEC §12 / D8); `testkit/dbal-suite.sh`
-- resets it container-side before every recorded run. The grant is scoped to that database only; the
-- `ferro` user deliberately does NOT get CREATE DATABASE.
CREATE DATABASE IF NOT EXISTS doctrine_tests;
GRANT ALL PRIVILEGES ON doctrine_tests.* TO 'ferro'@'%';
FLUSH PRIVILEGES;
```

PostgreSQL — the `ferro` role is `Superuser, Create DB` (verified with `\du`), so it can own a second database; add it to `testkit/postgres/init.sql` alongside the existing fixtures:

```sql
-- M1-S8b: as above. NEVER point the upstream suite at the shared `ferro` database.
SELECT 'CREATE DATABASE doctrine_tests OWNER ferro'
 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'doctrine_tests') \gexec
```

Apply both to the LIVE containers (the init scripts only run on a fresh volume, and we must not `down -v` a shared stack):

```bash
cd /home/abdullak/projects/ferro
for svc in mysql mariadb; do
  docker compose -f testkit/docker-compose.yml exec -T "$svc" \
    mysql -uroot -pferro -e "CREATE DATABASE IF NOT EXISTS doctrine_tests; GRANT ALL PRIVILEGES ON doctrine_tests.* TO 'ferro'@'%'; FLUSH PRIVILEGES;"
done
docker compose -f testkit/docker-compose.yml exec -T mysql mysql -uferro -pferro -e "SHOW GRANTS FOR CURRENT_USER();"

docker compose -f testkit/docker-compose.yml exec -T pg \
  psql -U ferro -d ferro -tAc "SELECT 1 FROM pg_database WHERE datname='doctrine_tests'" | grep -q 1 \
  || docker compose -f testkit/docker-compose.yml exec -T pg psql -U ferro -d ferro -c 'CREATE DATABASE doctrine_tests OWNER ferro'
docker compose -f testkit/docker-compose.yml exec -T pg psql -U ferro -d doctrine_tests -c 'SELECT current_database()'
```
Expected: the MySQL grants list now includes `GRANT ALL PRIVILEGES ON \`doctrine_tests\`.* TO \`ferro\`@\`%\``, and the PG command prints `doctrine_tests`. If the root password differs in `testkit/docker-compose.yml`, read it from there rather than guessing. If the `ferro` role turns out NOT to have `CREATEDB` on this box, run the `CREATE DATABASE` as the container's superuser instead — do NOT fall back to the shared `ferro` database.

Then write the two reset scripts the runner will apply before every recorded run. `testkit/dbal/reset-pg.sql`:

```sql
-- M1-S8b: the upstream functional suite's ONLY reset. Upstream gets idempotence from
-- TestUtil::initializeDatabase()'s dropDatabase/createDatabase, which Ferro structurally cannot do
-- (PHP holds no credentials, SPEC §12/D8), so it happens HERE, container-side, with no PHP
-- credentials involved — the same shape the MySQL grant in this step already uses.
--
-- Without it the recorded number is not reproducible: measured against a KNOWN-GOOD driver, the same
-- command gave 23 then 33 errors on consecutive runs (hazard 85). CASCADE is required — upstream's
-- own dropTableIfExists issues a plain DROP TABLE and leaves dependent objects behind.
DROP SCHEMA IF EXISTS testschema CASCADE;
DROP SCHEMA IF EXISTS nested CASCADE;
DROP SCHEMA IF EXISTS another CASCADE;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public AUTHORIZATION ferro;
GRANT ALL ON SCHEMA public TO ferro;
```

`testkit/dbal/reset-mysql.sql`:

```sql
-- M1-S8b: see reset-pg.sql. MySQL has no schema/database distinction to work around, so the whole
-- database is recreated.
DROP DATABASE IF EXISTS doctrine_tests;
CREATE DATABASE doctrine_tests;
GRANT ALL PRIVILEGES ON doctrine_tests.* TO 'ferro'@'%';
FLUSH PRIVILEGES;
```

- [ ] **Step 2: Write the patched `TestUtil`**

Create `testkit/dbal/TestUtil.ferro.php` — a drop-in replacement for the upstream `tests/TestUtil.php`, copied OVER it by the runner:

```php
<?php // testkit/dbal/TestUtil.ferro.php  ->  copied over <dbal>/tests/TestUtil.php
declare(strict_types=1);

namespace Doctrine\DBAL\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;

/**
 * Ferro's replacement for doctrine/dbal's own `tests/TestUtil`.
 *
 * TWO upstream behaviours make the stock file unusable against a third-party driver, and BOTH are
 * measured rather than assumed:
 *
 *  1. `getConnectionParams()` returns the mapped `$GLOBALS['db_*']` params ONLY when
 *     `$params['driver']` is set, and otherwise returns `['driver' => 'pdo_sqlite', 'memory' => true]`.
 *     `db_driverClass` is DISCARDED. Since `pdo_sqlite` is loaded almost everywhere, the entire
 *     functional suite then runs GREEN against in-memory SQLite with zero Ferro contact — and
 *     nothing skips, so no skip-detector catches it. This file honours `db_driverClass`.
 *  2. `initializeDatabase()` opens a PRIVILEGED connection with `dbname` unset and runs
 *     `dropDatabase()` + `createDatabase()` on every run. Ferro cannot serve that: the DSN lives in
 *     the ENGINE and PHP has no credentials at all (SPEC §12 / D8), there is nothing for a
 *     client-side `dbname` to mean, and dropping a database a live pool holds connections to is
 *     refused anyway.
 *
 *     **That method is also the functional suite's ONLY reset**, so removing it without replacing it
 *     makes the suite non-idempotent: measured against a KNOWN-GOOD driver, the same command gave
 *     `Errors 23` and then `Errors 33` on consecutive runs, while upstream's version gave `0/0`
 *     before and after. The replacement is `testkit/dbal-suite.sh`'s container-side reset, which
 *     needs no PHP credentials — the same shape the MySQL grant already uses. This method stays a
 *     no-op, and the RUNNER is where idempotence now lives; a recorded number MUST come from a run
 *     that performed the reset.
 *
 * `isDriverOneOf()` answers FALSE for every name, and that is a deliberate decision with 60 call
 * sites behind it: claiming `pdo_pgsql`/`pdo_mysql` would opt Ferro into PDO-specific expectations
 * and into whole vendor sub-trees written against those extensions. Answering nothing means every
 * vendor-gated test takes its "other" branch, which is the honest description of what Ferro is.
 *
 * **The public surface below is the MEASURED one, not a guess** (hazard 84): over the allowlisted
 * paths of the real 4.4.4 clone, `grep -rhoE 'TestUtil::[a-zA-Z]+' tests/` gives `isDriverOneOf` ×13,
 * `getPrivilegedConnection` ×2, `getConnectionParams` ×2, `getConnection` ×1; elsewhere in `tests/`,
 * `isPdoStringifyFetchesEnabled` ×4 and `generateResultSetQuery` ×1. Plan v1 declared
 * `getPrivilegedConnectionParameters` — which is PRIVATE upstream and called by nothing outside the
 * class — and omitted `getPrivilegedConnection`, which an allowlisted test calls. Exactly backwards,
 * and the resulting `Call to undefined method` errors would have gone into the triage table looking
 * like driver defects.
 */
final class TestUtil
{
    private static ?Connection $connection = null;

    /** @return array<string,mixed> */
    public static function getConnectionParams(): array
    {
        $params = [];
        foreach (['driverClass', 'host', 'port', 'user', 'password', 'dbname', 'unix_socket'] as $key) {
            if (isset($GLOBALS['db_' . $key]) && $GLOBALS['db_' . $key] !== '') {
                $params[$key] = $GLOBALS['db_' . $key];
            }
        }
        if (isset($GLOBALS['db_driver_options']) && is_string($GLOBALS['db_driver_options'])) {
            /** @var array<string,mixed> $decoded */
            $decoded = json_decode($GLOBALS['db_driver_options'], true, 512, JSON_THROW_ON_ERROR);
            $params['driverOptions'] = $decoded;
        }
        if (isset($GLOBALS['db_serverVersion']) && $GLOBALS['db_serverVersion'] !== '') {
            $params['serverVersion'] = $GLOBALS['db_serverVersion'];
        }
        if (!isset($params['driverClass'])) {
            throw new \RuntimeException(
                'Ferro TestUtil: db_driverClass is not set. This runner exists precisely because the '
                . 'upstream TestUtil would silently fall back to in-memory SQLite here.',
            );
        }
        return $params;
    }

    public static function getConnection(): Connection
    {
        if (self::$connection !== null) {
            return self::$connection;
        }
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return self::$connection = DriverManager::getConnection(self::getConnectionParams(), $config);
    }

    /**
     * Pre-provisioned and RESET by `testkit/dbal-suite.sh`, container-side; see the class docblock.
     * A no-op here is only sound because that reset exists — do not remove one without the other.
     */
    public static function initializeDatabase(): void
    {
    }

    public static function isDriverOneOf(string ...$names): bool
    {
        return false;
    }

    /**
     * Upstream this is a connection with credentials that can drop and create databases. **Ferro has
     * no such thing, and cannot**: the DSN lives in the engine and PHP holds no credentials at all
     * (SPEC §12 / D8). So "privileged" here means exactly one thing — a SECOND, independent
     * connection to the same pool — which is what the two allowlisted call sites actually need
     * (`tests/Functional/TransactionTest.php:112` uses it to observe an in-progress transaction from
     * outside it). A test that genuinely needs DDL privileges Ferro's user does not have will fail
     * loudly on that DDL, which is the correct outcome and is triaged as category (c).
     *
     * Note it is deliberately NOT `getConnection()`: sharing the connection would make the
     * cross-connection observation it exists for meaningless.
     */
    public static function getPrivilegedConnection(): Connection
    {
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return DriverManager::getConnection(self::getConnectionParams(), $config);
    }

    /**
     * Whether PDO is configured to stringify fetched values. Ferro is not PDO and has no such mode:
     * every column arrives typed from the `/proto` tag registry.
     */
    public static function isPdoStringifyFetchesEnabled(): bool
    {
        return false;
    }

    // `generateResultSetQuery(array $rows, AbstractPlatform $platform): string` goes HERE, COPIED
    // BYTE-FOR-BYTE from the pinned clone at `<work>/dbal-4.4.4/tests/TestUtil.php` (4.4.4 lines
    // 257-270), together with `use Doctrine\DBAL\Platforms\AbstractPlatform;`. It is platform-SQL
    // GENERATION, not a policy answer — there is nothing Ferro-specific to decide — and a rewritten
    // version would silently change what the tests using it assert. Transcribe it; do not
    // reconstruct it from the signature.
}
```

The clone is what the runner (Step 5) does first, but this step comes earlier — so do the clone by hand now, once, and read the method out of it. The runner will reuse the same checkout:

```bash
cd /home/abdullak/projects/ferro
git clone --depth 1 --branch 4.4.4 https://github.com/doctrine/dbal.git .dbal-suite/dbal-4.4.4 2>/dev/null || true
sed -n '/function generateResultSetQuery/,/^    }/p' .dbal-suite/dbal-4.4.4/tests/TestUtil.php
grep -n 'function getPrivilegedConnection\b' .dbal-suite/dbal-4.4.4/tests/TestUtil.php
```
The second command is the census check: it must find a PUBLIC `getPrivilegedConnection()`. `.dbal-suite/` is the runner's work directory and is already covered by the repo's ignore rules for build output — if it is not, add it to `.gitignore` in this step rather than committing a vendored clone.

**If the first run still reports a `Call to undefined method TestUtil::…`**, add exactly that method with a one-line docblock saying what it answers and why — but the census above is measured, so treat a miss as a signal that the allowlist grew rather than as routine. Two rules for anything added: a method that GENERATES SQL is copied verbatim from upstream (like `generateResultSetQuery`), and a method that answers a POLICY question gets a Ferro answer with the reason written down. Never a speculative addition, and never a silent one.

- [ ] **Step 3: Write the bootstrap with the HARD contact assertion**

Create `testkit/dbal/bootstrap.php`:

```php
<?php // testkit/dbal/bootstrap.php
declare(strict_types=1);

// Three autoloaders: the driver package's (which also pulls in ferro/client through its path
// repository), and DBAL's own autoload-dev PSR-4 root, which a CONSUMER install never registers.
require __DIR__ . '/../../php/doctrine-dbal/vendor/autoload.php';

$dbal = getenv('FERRO_DBAL_SRC');
if ($dbal === false || $dbal === '') {
    fwrite(STDERR, "FERRO_DBAL_SRC is unset\n");
    exit(1);
}
$loader = require $dbal . '/vendor/autoload.php';
$loader->addPsr4('Doctrine\\DBAL\\Tests\\', $dbal . '/tests');

// ---------------------------------------------------------------------------------------------
// THE CONTACT ASSERTION. It runs BEFORE the first test, and it is the whole reason this file
// exists. The upstream TestUtil silently falls back to in-memory SQLite when it cannot find a
// driver, and the functional suite then passes — genuinely, with nothing skipped — against the
// wrong engine. `--fail-on-skipped` cannot catch that; only asking the connection what it IS can.
// ---------------------------------------------------------------------------------------------
$conn = Doctrine\DBAL\Tests\TestUtil::getConnection();
$native = $conn->getNativeConnection();
if (!$native instanceof Ferro\Client\Connection) {
    fwrite(STDERR, sprintf(
        "FERRO CONTACT ASSERTION FAILED: the suite's connection is a %s, not a Ferro one.\n"
        . "Refusing to run: a green result here would mean nothing.\n",
        get_debug_type($native),
    ));
    exit(1);
}
$version = $conn->getServerVersion();
$platform = get_class($conn->getDatabasePlatform());
fwrite(STDOUT, sprintf("[ferro] driver=%s platform=%s server=%s\n", get_class($conn->getDriver()), $platform, $version));
// A real round trip, so "connected" cannot mean "constructed an object".
if ((int) $conn->fetchOne('SELECT 1') !== 1) {
    fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: SELECT 1 did not return 1\n");
    exit(1);
}
```

- [ ] **Step 4: Write the allowlist and the phpunit config**

Create `testkit/dbal/allowlist.txt` — one upstream path per line, `#` comments allowed. Start with the sub-trees that exercise the driver SPI and the stock schema manager, and record a REASON for every exclusion:

```
# M1-S8b curated subset of doctrine/dbal's tests/Functional.
# Each EXCLUDED sub-tree is listed at the bottom with its reason; nothing is silently dropped.
tests/Functional/DataAccessTest.php
tests/Functional/ResultTest.php
tests/Functional/StatementTest.php
tests/Functional/TransactionTest.php
tests/Functional/TypeConversionTest.php
tests/Functional/WriteTest.php
tests/Functional/ExceptionTest.php
tests/Functional/ModifyLimitQueryTest.php
tests/Functional/PrimaryReadReplicaConnectionTest.php
tests/Functional/Schema
tests/Functional/Types
#
# EXCLUDED, with reasons:
#   tests/Functional/Driver/{PDO,PgSQL,Mysqli,SQLite3,OCI8,SQLSrv,IBMDB2}
#       vendor-extension sub-trees; they markTestSkipped unless isDriverOneOf names them, and ours
#       names nothing (see TestUtil.ferro.php).
#   tests/Functional/Driver/AbstractDriverTestCase.php
#       testConnectsWithoutDatabaseNameParameter / testReturnsDatabaseNameWithoutDatabaseNameParameter
#       assume the driver can connect to a SERVER with no database. Ferro's DSN lives in the engine
#       (SPEC §12 / D8), so there is nothing for a client-side dbname to mean.
#   tests/Functional/LockMode
#       opens a second connection and relies on cross-connection locking; in scope for a later
#       slice, excluded here so the first recorded number is not dominated by one hard case.
#   tests/Functional/Ticket, tests/Functional/Query, tests/Functional/SQL, tests/Functional/Platform
#       platform-SQL-shape tests rather than driver-execution tests; charter rule 6 means we neither
#       change nor own that behaviour.
```

Create `testkit/dbal/phpunit.ferro.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<phpunit bootstrap="bootstrap.php" colors="true" cacheDirectory=".phpunit.cache" failOnWarning="false">
    <testsuites>
        <testsuite name="ferro-dbal-subset">
            <!-- Filled in by testkit/dbal-suite.sh from allowlist.txt; committed EMPTY so the file
                 is never a second, drifting copy of the allowlist. -->
        </testsuite>
    </testsuites>
    <php>
        <var name="db_driverClass" value="Ferro\DBAL\Driver"/>
    </php>
</phpunit>
```

- [ ] **Step 5: Write the runner**

Create `testkit/dbal-suite.sh` (`chmod +x`):

```bash
#!/usr/bin/env bash
# M1-S8b: run a CURATED subset of doctrine/dbal's own functional suite against Ferro.
#
# NO `docker compose down` TRAP OF ANY KIND. testkit/smoke.sh and testkit/e2e-demo.sh both tear the
# stack down on EXIT; copying that here would destroy the databases every other suite is using.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_DBAL_TAG:-4.4.4}"
pool="${FERRO_DBAL_POOL:-default}"
# The suite gets its OWN database on every family. NEVER the shared `ferro` one: this suite creates
# and abandons ~40 tables, 8+ sequences, 5 schemas, a domain type and several views, and nothing
# would ever clean them out of a database every other live suite in this repo uses.
dsn="${FERRO_DBAL_DSN:-postgres://ferro:ferro@127.0.0.1:55432/doctrine_tests}"
# Which container to reset, and how. `--no-reset` exists for fast iteration; a RECORDED run must not
# use it (see the results file's environment manifest).
svc="${FERRO_DBAL_SVC:-pg}"
work="${FERRO_DBAL_WORK:-$root/.dbal-suite}"
src="$work/dbal-$tag"
reset=1
args=()
for a in "$@"; do
  case "$a" in
    --no-reset) reset=0 ;;
    *) args+=("$a") ;;
  esac
done

mkdir -p "$work"

# 1. The PINNED source. The packagist DIST ships `src/` only — no tests, no phpunit.xml.dist — so a
#    git clone is the only way to get the suite, and the tag must be pinned or the bar drifts.
if [ ! -d "$src" ]; then
  git clone --depth 1 --branch "$tag" https://github.com/doctrine/dbal.git "$src"
  # composer install FAILS out of the box: the security audit blocks squizlabs/php_codesniffer
  # (advisory PKSA-rdkp-vv9z-mjkg) via doctrine/coding-standard and slevomat/coding-standard.
  # phpunit needs none of the three.
  (cd "$src" && composer remove --dev --no-update --no-interaction \
      doctrine/coding-standard slevomat/coding-standard squizlabs/php_codesniffer || true)
  (cd "$src" && composer install --no-interaction --no-progress)
fi

# 2. The patched TestUtil, copied over the upstream one, and VERIFIED — a silently-failed patch is
#    exactly how this suite goes green against SQLite.
cp "$root/testkit/dbal/TestUtil.ferro.php" "$src/tests/TestUtil.php"
grep -q 'db_driverClass is not set' "$src/tests/TestUtil.php" \
  || { echo "::error:: TestUtil patch did not apply"; exit 1; }

# 3. The driver package must be installed (its vendor/ is its own).
(cd "$root/php/doctrine-dbal" && composer install --no-interaction --no-progress --quiet)

# 4. ONE ferrod for the whole run — not one per test. The suite shares a single Connection across
#    every test (FunctionalTestCase::$sharedConnection), so this is the right granularity.
cargo build -p ferrod --manifest-path "$root/Cargo.toml"
sock="$(mktemp -u /tmp/ferro-dbal-XXXXXX.sock)"
FERRO_SOCK="$sock" FERRO_POOLS="$pool" "FERRO_POOL_$(echo "$pool" | tr '[:lower:]-' '[:upper:]_')_DSN=$dsn" \
  "$root/target/debug/ferrod" >"$work/ferrod.log" 2>&1 &
ferrod_pid=$!
trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT   # ONLY our own daemon.
for _ in $(seq 1 50); do [ -S "$sock" ] && break; sleep 0.1; done
[ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }

# 5. The phpunit config, with the allowlist expanded into <file>/<directory> entries. Generated
#    rather than committed expanded, so allowlist.txt stays the single source of truth.
cfg="$work/phpunit.generated.xml"
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<phpunit bootstrap="'"$root"'/testkit/dbal/bootstrap.php" colors="true" cacheDirectory="'"$work"'/.phpunit.cache">'
  echo '  <testsuites><testsuite name="ferro-dbal-subset">'
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    if [ -d "$src/$line" ]; then echo "    <directory>$src/$line</directory>"
    else echo "    <file>$src/$line</file>"; fi
  done < "$root/testkit/dbal/allowlist.txt"
  echo '  </testsuite></testsuites>'
  echo '  <php>'
  echo '    <var name="db_driverClass" value="Ferro\DBAL\Driver"/>'
  echo '    <var name="db_unix_socket" value="'"$sock"'"/>'
  echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;'"$pool"'&quot;}"/>'
  echo '  </php>'
  echo '</phpunit>'
} > "$cfg"

# 6. THE RESET — the suite's only source of idempotence, and a hard precondition of recording a
#    number. Upstream gets it from TestUtil::initializeDatabase()'s dropDatabase/createDatabase,
#    which Ferro structurally cannot do (PHP holds no credentials, SPEC §12/D8), so it happens
#    container-side with no PHP credentials at all — the same shape as the MySQL grant.
#
#    MEASURED, against a KNOWN-GOOD driver, with no reset: the same command gave `Errors 23,
#    Failures 3` and then `Errors 33, Failures 1`; with upstream's TestUtil it gave 0/0 before and
#    after. A number that degrades on every run is worse than no number, because the triage table
#    then blames the driver for leftover state.
if [ "$reset" = 1 ]; then
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d doctrine_tests < "$root/testkit/dbal/reset-pg.sql"
      echo "[ferro] reset: pg/doctrine_tests from testkit/dbal/reset-pg.sql"
      ;;
    mysql|mariadb)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T "$svc" \
        mysql -uroot -pferro < "$root/testkit/dbal/reset-mysql.sql"
      echo "[ferro] reset: $svc/doctrine_tests from testkit/dbal/reset-mysql.sql"
      ;;
    *) echo "::error:: unknown FERRO_DBAL_SVC=$svc"; exit 1 ;;
  esac
else
  echo "[ferro] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
fi

# 7. Run it. The bootstrap's contact assertion runs first and exits non-zero if the connection is
#    not a Ferro one.
FERRO_DBAL_SRC="$src" "$src/vendor/bin/phpunit" -c "$cfg" "${args[@]+"${args[@]}"}"
```

What matters is that the reset runs **before phpunit**. Its position relative to the `ferrod` launch is not load-bearing — the pool's connections are idle at that point and idle sessions hold no locks — but if `DROP SCHEMA public CASCADE` ever fails with a lock conflict, move the whole block above step 4 rather than reaching for `--no-reset`. A reset that cannot run is a finding to report, not a step to skip: without it the recorded number is not reproducible.

- [ ] **Step 6: Run it and RECORD the numbers**

```bash
cd /home/abdullak/projects/ferro
FERRO_DBAL_SVC=pg \
  ./testkit/dbal-suite.sh 2>&1 | tee /tmp/dbal-pg.log
FERRO_DBAL_SVC=mysql FERRO_DBAL_POOL=mysql FERRO_DBAL_DSN=mysql://ferro:ferro@127.0.0.1:33060/doctrine_tests \
  ./testkit/dbal-suite.sh 2>&1 | tee /tmp/dbal-mysql.log
FERRO_DBAL_SVC=mariadb FERRO_DBAL_POOL=mariadb FERRO_DBAL_DSN=mysql://ferro:ferro@127.0.0.1:33061/doctrine_tests \
  ./testkit/dbal-suite.sh 2>&1 | tee /tmp/dbal-mariadb.log
```
Expected output order, and all three lines are load-bearing: `[ferro] reset: …` (idempotence), then `[ferro] driver=… platform=… server=…` (the contact assertion), then a real result line. **A log that is missing the reset line is not a recordable run.**

**REPRODUCIBILITY CHECK, before recording anything.** Run the PostgreSQL invocation TWICE and diff the two result lines:

```bash
FERRO_DBAL_SVC=pg ./testkit/dbal-suite.sh 2>&1 | tail -3 | tee /tmp/dbal-pg-run1.txt
FERRO_DBAL_SVC=pg ./testkit/dbal-suite.sh 2>&1 | tail -3 | tee /tmp/dbal-pg-run2.txt
diff /tmp/dbal-pg-run1.txt /tmp/dbal-pg-run2.txt && echo "REPRODUCIBLE"
```
Expected: identical, and `REPRODUCIBLE` printed. If they differ, the reset is incomplete — find what the suite leaves behind (`\dt`, `\ds`, `\dn` in `doctrine_tests`) and add it to `reset-pg.sql` before going further. **Do not record a number from a run whose repeat differs**: that was the state plan v1 would have shipped, where 23 errors became 33 with no code change.

**The result will not be green on the first run, and that is expected** — the job of this step is to produce the numbers, not to reach a target. Triage each failure into exactly one of: (a) a driver defect to fix now; (b) a documented Ferro semantic (pooling, `lastInsertId` on PG, a refused sentinel); (c) an upstream assumption Ferro cannot satisfy (no client-side dbname, PDO-specific expectations); (d) out of scope for M1. Fix every (a). Record every (b)/(c)/(d) with its test name. **There is now no (e) "leftover state from a previous run"** — if a failure looks like one, the reset is what to fix.

- [ ] **Step 7: Write the recorded results**

Create `docs/dbal-suite/2026-08-10-results.md` — the environment manifest plus the triaged list, in the shape `bench/results/` uses. **The angle-bracketed fields and the `…` table cells below are MEASUREMENTS to be filled in from the Step 6 run, not text to be committed as-is**; a committed file still containing them is an incomplete task.

```markdown
# Doctrine DBAL 4 functional subset vs Ferro — recorded results

**Date:** 2026-08-10 · **Slice:** M1-S8b · **Runner:** `testkit/dbal-suite.sh`

## Environment manifest
- doctrine/dbal: 4.4.4 (pinned git tag, cloned by the runner — the packagist dist ships no tests)
- PHP: <fill from `php -v`> · ext-msgpack: <loaded / not loaded>
- Engine: `ferrod` at `<git rev>` · one daemon per RUN
- Backends: PostgreSQL 17.10 (:55432), MySQL 8.4.11 (:33060), MariaDB 11.8.8 (:33061)
- Database: **`doctrine_tests` on every family — never the shared `ferro` database**
- **Reset: YES.** Every run below started from a freshly reset database
  (`testkit/dbal/reset-pg.sql` / `reset-mysql.sql`, applied container-side by the runner, which
  prints the `[ferro] reset: …` line). `--no-reset` was NOT used.
- **Reproducibility: verified.** The PostgreSQL invocation was run twice and the result lines were
  identical (`<paste the two lines>`). This check is mandatory: without a reset, the same command
  measured `Errors 23` and then `Errors 33` against a known-good driver.
- Subset: `testkit/dbal/allowlist.txt` (exclusions and their reasons are in that file)

## Baseline for comparison (STOCK drivers, same containers, measured before this slice)
- `pdo_pgsql`, whole `tests/`: Tests 3913, Assertions 5794, Failures 1, Skipped 556, Incomplete 4
- `pdo_pgsql`, `tests/Functional`: Tests 1077, Failures 1, Skipped 512 ⇒ ~565 actually execute
- `pdo_mysql`, `tests/Functional`: Tests 1077, Errors 1057 — every test errored in
  `TestUtil::initializeDatabase()` with `1044 Access denied … to database 'doctrine_tests'`.
  **Even the stock driver was not green on these containers**, which is why the bar below is stated
  as "green modulo a recorded list".

## Ferro results
| backend | tests | assertions | failures | errors | skipped | time |
|---|---|---|---|---|---|---|
| PostgreSQL 17.10 | … | … | … | … | … | … |
| MySQL 8.4.11 | … | … | … | … | … | … |
| MariaDB 11.8.8 | … | … | … | … | … | … |

## Every non-passing test, triaged
| test | backend | category | note |
|---|---|---|---|
| … | … | (b) documented Ferro semantic | … |

Categories: (a) driver defect — **none may remain in this table**; (b) a documented Ferro semantic;
(c) an upstream assumption Ferro cannot satisfy; (d) explicitly out of scope for M1.

## Not run, and why
- **SQLite:** Ferro has no SQLite backend (`engine/crates` has `pg` + `mysql` only). SPEC §14's
  acceptance clause named it; it is deferred to the milestone that adds `ferro-backend-sqlite`.
- **The ORM functional suite:** `doctrine/orm` is not a dependency of this repository and its
  harness has its own injection problem. Deferred, with the ORM+PostgreSQL+IDENTITY blocker
  recorded in `docs/known-incompatibilities.md`.
- **The excluded upstream sub-trees:** see `testkit/dbal/allowlist.txt`, which carries a reason per
  exclusion.
```

- [ ] **Step 8: Write the known-incompatibilities page and the package README**

Create `docs/known-incompatibilities.md` — §14 budgets the full catalogue for M2; this is the stub it grows from, and every entry below is something MEASURED during S8b:

```markdown
# Ferro drop-in: known incompatibilities

Ferro is a drop-in for Doctrine DBAL 4 by CONFIGURATION. These are the places where a real
application can still notice the difference. Each one is a deliberate consequence of the engine's
model, not a defect to be fixed quietly. The full per-package catalogue is budgeted for M2.

## Connection object
- **`getNativeConnection()` returns a `Ferro\Client\Connection`, not a `PDO`.** Anything calling
  `pg_escape_string($native, …)`, `$native->real_escape_string()` or `PDO::` methods will fatal.
- **No database credentials exist in PHP.** The DSN lives in the engine (SPEC §12 / D8), so tooling
  that shells out to `pg_dump`/`mysqldump` with the application's config cannot work; ops provisions
  separate dump credentials.

## Identity and keys
- **`lastInsertId()` throws on PostgreSQL.** PG's protocol carries no such field and Ferro refuses
  to emulate it with `SELECT lastval()`, because on a transaction-mode pool the follow-up runs on a
  DIFFERENT connection and returns a silently wrong key. Use `INSERT … RETURNING id`.
- **Doctrine ORM + PostgreSQL + the default IDENTITY strategy cannot insert.**
  `IdentityGenerator::generateId()` is `(int) $conn->lastInsertId()`. Configure the SEQUENCE
  identity strategy on PostgreSQL.

## Transactions and session state
- **`setTransactionIsolation()` requires `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class`.**
  Without it the raw `SET SESSION …` statement is REFUSED, loudly. On a transaction-mode pool that
  statement would report success and change nothing.
- **`READ UNCOMMITTED` is upgraded to `READ COMMITTED`** (never weakened). PostgreSQL treats them as
  the same level; on MySQL this is a genuine, documented tightening.
- **`setAutoCommit(false)` pins a backend connection for the whole request**, which turns Ferro's
  central win off. It works; it is just expensive.
- **ORM multi-table DELETE/UPDATE on class-table inheritance needs an explicit transaction.**
  `MultiTableDeleteExecutor` issues CREATE TEMPORARY TABLE, INSERT, DELETE and DROP as four separate
  statements with no transaction; on a transaction-mode pool statements 2-4 land on different
  connections. Wrap the query in `$conn->transactional(…)`.

## Values
- **A value Doctrine's type layer would parse INCORRECTLY is refused, not converted.** PostgreSQL's
  legal `24:00:00`, MySQL zero and zero-in dates, `infinity` sentinels, and a sub-second
  `timestamptz` all raise a driver error instead of silently becoming a different value (measured:
  stock DBAL turns `2026-00-05` into `2025-12-05` and `24:00:00` into `00:00:00`, with no
  exception). Read those columns through the native `Ferro\Client\Connection` API, or cast them in
  SQL.
- **A `LARGE_OBJECT` bind is materialised in memory** and is bounded by the 16 MiB maximum frame
  payload. A chunked bind would be a protocol change.

## Errors
- **A `SELECT` that is cancelled server-side or exceeds `statement_timeout` surfaces as
  `Ferro\DBAL\IndeterminateWriteException`, not as a cancellation.** The DBAL 4 SPI carries no
  read/write signal — `executeQuery('INSERT … RETURNING id')` is indistinguishable from a `SELECT`
  at the driver boundary — and Ferro refuses to guess from SQL text, so the driver declares every
  statement a WRITE. That is the safe direction (a lost write is never reported as "provably did not
  apply"), and this is its cost: for an autocommit statement outside a transaction, the engine's
  fate matrix routes a `57014` by the declared `readonly` flag alone. **Do not add a blanket retry
  on `IndeterminateWriteException` to work around it** — that is exactly the at-most-once violation
  the branch exists to prevent. If a connection genuinely only reads, declare it:
  `'driverOptions' => ['readonly' => true]`, which restores the clean "statement cancelled or timed
  out" answer. In a transaction the question does not arise: a cancelled statement rolls the
  transaction back and is reported `Retryable`.

## Performance and shape
- **`iterateAssociative()` streams on PostgreSQL for parameterless queries and buffers otherwise**
  (and always buffers on MySQL/MariaDB, where engine-side row streaming is deferred).
- **Abandoning an iteration early cancels the stream**, and a statement issued *while* an iteration
  is still open drains the remainder into memory first (the session is single-in-flight). The
  canonical `foreach (iterate…) { executeStatement(…) }` idiom therefore works, at the cost of
  buffering what is left — which is what PDO does unconditionally. `Ferro\DBAL\Connection::settledRowCount()`
  reports how many rows that has cost on this connection.
- **The first query against a backend that is DOWN can block for the OS connect timeout** (~127 s
  measured) rather than failing fast. Tracked in `docs/followups/2026-08-10-unbounded-backend-dial.md`.
- **`Ferro\Pg\Copy`** — the first-class replacement for `pdo_pgsql` COPY hacks named in SPEC §14 —
  does not exist yet. Deferred.
```

Create `php/doctrine-dbal/README.md` with the install snippet, the full configuration shape (`driverClass`, `unix_socket`, `driverOptions.pool`, `driverOptions.readonly`, `wrapperClass`, `serverVersion`), and a pointer to `docs/known-incompatibilities.md`.

- [ ] **Step 9: Amend SPEC §14 and add the §22.2 entries**

Rewrite the parts of §14 that describe a DBAL-3-shaped SPI. The specific edits, each forced by a measurement:

- `getDatabasePlatform()` "selects the platform from `HELLO_ACK` pool metadata + server version" → say that DBAL 4 hands the driver a `ServerVersionProvider` (not the connection), so the driver remembers the pool KIND from `connect()` and normalises the version string **PostgreSQL-only**.
- `lastInsertId()` "sequence-name argument supported for PG" → **DELETE**. DBAL 4 removed the overload; the method takes no argument and must THROW when there is no identity value.
- The configuration example's `'ferro' => [...]` key → `driverOptions`, with `unix_socket` and `wrapperClass` shown.
- `getNativeConnection()` "returns the `Ferro\Client\Session`" → `Ferro\Client\Connection` (which is what the driver holds and what is useful; both satisfy the SPI's `resource|object`).
- "streaming used automatically when the consumer iterates (`iterateAssociative()` et al. never buffer)" → the measured, bounded claim from Task 12.
- The acceptance clause → the restated bar, pointing at `docs/dbal-suite/2026-08-10-results.md`.
- Record the nil-`server_version` DECISION as DECIDED (defer → resolve with one `SELECT version()` → fail loudly naming the pool), and note `params['serverVersion']` as the operator escape hatch.

Then append to §22.2, continuing the letters after **(x)**:

```markdown
  **(y) SPEC §14 was written against a DBAL-3-shaped SPI, and three of its sentences were unimplementable (M1-S8b).** Read from `doctrine/dbal 4.4.4`'s own `src/`: `ServerInfoAwareConnection` and `VersionAwarePlatformDriver` DO NOT EXIST in 4.x — `getServerVersion(): string` is on `Doctrine\DBAL\ServerVersionProvider`, which `Driver\Connection` extends, and it is NON-NULLABLE; `Driver::getDatabasePlatform()` is handed a `ServerVersionProvider`, **not** the connection, so it cannot read `HELLO_ACK` metadata and the driver must remember the pool KIND from the last `connect()`; and `lastInsertId()` takes **no** argument (UPGRADE.md: "Removed support for `Connection::lastInsertId($name)`") and must THROW when there is no identity value, which makes §14's "sequence-name argument supported for PG" impossible to express. §14's configuration example was also wrong for a different reason: `Driver::connect()` is `@phpstan-param Params`, `Params` is a SEALED array shape with **no `ferro` key**, and reading `$params['ferro']['pool']` measured as two `nullCoalesce.offset` errors at PHPStan level 9 — a charter Definition-of-Done gate. `driverOptions?: array<mixed>` is the sanctioned slot and every key read out of it carries an explicit `is_string()`/`is_int()` narrowing. All four are amended in §14 in the same change set.

  **(z) The §14 acceptance bar is NOT reachable as written, and the reachable part is recorded rather than silently narrowed (M1-S8b).** Four measured obstacles. **(1) SQLite is impossible** — `engine/crates` has `pg` + `mysql` only, so one third of "green on PG + MySQL + SQLite" waits for `ferro-backend-sqlite`. **(2) The upstream suite cannot select a third-party driver**: `TestUtil::getConnectionParams()` checks only `$params['driver']` and otherwise returns `['driver' => 'pdo_sqlite', 'memory' => true]` — measured, with `db_driverClass` set — so the entire functional suite would run GREEN against in-memory SQLite with ZERO Ferro contact, and neither `--fail-on-skipped` nor `ci/assert-no-skips.sh` would catch it (nothing skips; everything genuinely passes, on the wrong engine). `/testkit`'s runner therefore ships a REPLACEMENT `TestUtil` and a bootstrap whose FIRST action is to assert `getNativeConnection() instanceof Ferro\Client\Connection` and to round-trip a real `SELECT 1`. **(3) MySQL could not run the suite at all**: `initializeDatabase()` does `dropDatabase`/`createDatabase` and the testkit user had only `GRANT ALL ON ferro.*` — measured as 1057 errors out of 1077 functional tests. Ferro cannot serve that shape anyway (the DSN lives in the engine, PHP holds no credentials — §12/D8), so the database is pre-provisioned in `testkit/mysql-init.sql` and the patched `initializeDatabase()` is a no-op. **(4) Even the STOCK `pdo_pgsql` driver is not green on our containers** (1 environmental failure). The bar is therefore restated as **"the curated subset in `testkit/dbal/allowlist.txt`, green modulo a recorded and triaged list"**, measured against the recorded stock denominators (whole suite 3913/556 skipped; functional-only 1077/512 skipped ⇒ ~565 executing on PG), with every exclusion carrying its reason in the allowlist and every non-passing test triaged in `docs/dbal-suite/2026-08-10-results.md`. `isDriverOneOf()` answers FALSE for every name — a deliberate choice with 60 call sites behind it: claiming `pdo_pgsql`/`pdo_mysql` would opt Ferro into PDO-specific expectations. **The ORM functional suite is DEFERRED**, and one blocker is already known: `Doctrine\ORM\Id\IdentityGenerator::generateId()` is `(int) $conn->lastInsertId()` and DBAL 4 defaults PostgreSQL to the IDENTITY strategy, which cannot work while PG reports no generated key — the remedy is configuration (the SEQUENCE strategy) and it is documented rather than engineered around.

  **(aa) A canonical `TAG_TEXT` param now binds wherever PostgreSQL's own TEXT INPUT SYNTAX is what it carries — the drop-in blocker, closed without loosening §19.3 (M1-S8b Task 4).** Stock Doctrine's type layer stringifies every `datetime`/`date`/`time`/`decimal`/`json`/`guid` value and binds it as `ParameterType::STRING`, which reaches the engine as `TAG_TEXT`; `PgText`'s `accepts` was `String`'s (`varchar`/`text`/`bpchar`/`name`/`unknown` + the name-keyed `citext`/`ltree`/…), so **every such INSERT was refused pre-send on PostgreSQL** while MySQL — which has no bind pre-flight at all — accepted the identical driver. `PgText` is now an explicit impl accepting those types **plus** `numeric`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `json` and `jsonb`. **The format change is not cosmetic:** the `pg_domain_aware_param!` macro delegated `encode_format` to `<String as ToSql>`, which takes the trait's `Format::Binary` default, so widening `accepts` ALONE would have told PostgreSQL that the UTF-8 bytes of `2026-08-05` were a 4-byte binary `date`. **Both `to_sql` and `encode_format` therefore BRANCH on the resolved base** — verbatim text and `Format::Text` for the eight new targets, the unchanged delegated path (and `Format::Binary`) for everything `String` already accepted — and the branch is load-bearing twice over. It is required for CORRECTNESS, because "text bytes are the binary bytes for every string type this already took" is FALSE: `<&str as ToSql>::accepts` also admits `citext`, `ltree`, `lquery` and `ltxtquery` by NAME, and for the last three the binary form is `0x01 || text`. And it is required for FALSIFIABILITY, because that same name-sensitive encoder is the only thing that makes the payload-bytes clause of `s8a_every_arm_treats_a_domain_exactly_as_its_base` able to fail — the `ltree` fixture entry S8a's review round added for exactly that purpose. A type-blind `to_sql` would make a domain and its base write identical bytes by construction and silently revert that repair: MEASURED, the mutation that is RED at HEAD goes GREEN. The regression surface on everything that already worked is consequently empty rather than believed-harmless. **§19.3's direction is intact:** `check_param`'s `Value::Text` arm delegates to this same `accepts`, so the pre-flight and the impl are bit-identical by construction and moved in ONE edit; and what the widening admits is not an unclassifiable failure but a real server-side `22007`/`22P02` `DbError`, which `is_session_fatal` reads as non-fatal and `error_map` classifies `NonRetryable`. **The sentinel discipline the old narrowness protected is PRESERVED as a value-aware gate**: PostgreSQL's special input literals (`infinity`, `-infinity`, `now`, `today`, `tomorrow`, `yesterday`, `epoch`, `allballs`, and `NaN`/`Infinity` for `numeric`) are still refused for a temporal or numeric slot, naming the tagged route (`Ferro\Date`, `Ferro\NaiveTimestamp`, `Ferro\Decimal`) that expresses a sentinel deliberately — while the identical string remains an ordinary value in a `text` column. That is a REFUSAL keyed on the slot's declared type, not an inference of a tag from content: nothing decides that `'2026-08-05'` "is a date".

  **(ab) Doctrine's stock type layer is a silently-corrupting calendar parser, and the driver REFUSES rather than converts (M1-S8b Task 9).** Measured on 4.4.4, with **no exception raised**: `date '2026-00-05'` → `DateTime(2025-12-05)`; `datetime '0000-00-00 00:00:00'` → `DateTime(-0001-11-30)`; `time '24:00:00'` — a value PostgreSQL genuinely stores — → `00:00:00`. `proto/PROTOCOL.md` §3.2 warned about this parser class in prose; that is the measurement. The mirror problem is `datetimetz`, which is unreadable in the other direction: `DateTimeTzType` has no fallback and accepts only `Y-m-d H:i:sO` on PostgreSQL and `Y-m-d H:i:s` on the MySQL family, so **every** canonical RFC3339 form throws on **every** platform, and every microsecond form throws too. The driver's own `ValuePolicy` handles both — `ValuePolicy::decode(int $tag, mixed $data)` is per-cell TAG-AWARE by construction, so no client API change was needed to see column tags. A whole-second `TIMESTAMPTZ` is re-rendered into the platform's own format (per family, from two literals LOCKED against the stock accessors by a unit test, so a DBAL release that changes either goes red); a sub-second one is **refused rather than truncated**, because silent precision loss is the same defect class as the corruption above; and sentinels, zero dates, negative times, sub-second times and `24:00:00` are refused with a message naming the native API as the way to read them. Charter rule 6 is untouched: this is the driver's own conversion step, which `RawStringValuePolicy`'s docblock already blessed. **One claim in that docblock was FALSIFIED and corrected in the same change set:** `AbstractPlatform::getDateTimeFormatString()` does NOT reject a canonical fractional `TIMESTAMP` — `DateTimeType::convertToPHPValue` falls back to `new DateTime($value)`, measured — so the naive-timestamp fraction survives untouched and only the two `datetimetz` claims stand.

  **(ac) `iterate*()` streams on the PARAMETERLESS read path only, and every DBAL statement is fate-declared a WRITE (M1-S8b Tasks 1, 12).** Two bounded honesty statements about §14's wording. **Streaming:** `Doctrine\DBAL\Result::iterateAssociative()` is literally a loop over `fetchAssociative()`, so "never buffer" reduces to pulling one row at a time — but `Connection::executeStatement()` with parameters is `$stmt->execute()->rowCount()`, and a streamed request's terminal carries **no `affected` field**, so streaming the prepared path would make every parameterized write return 0. The driver therefore streams on `query()` — the zero-parameter `executeQuery`, where DBAL never asks for a row count — and buffers elsewhere; and MySQL/MariaDB buffer everywhere, because engine-side row streaming there is still deferred ((n)). Adding `affected` to the stream terminal is the durable fix and is a `/proto` change (registry + golden vectors + both codecs), deliberately NOT smuggled into a driver slice. Because the client session is strictly single-in-flight, an open streamed `Result` drains its remainder before the connection issues anything else — so the canonical `foreach (iterate…) { executeStatement(…) }` idiom keeps working (it degrades to what PDO does unconditionally) instead of throwing a `ProtocolException` every user would read as a driver bug. **Abandonment is a different case and must not collapse into that one:** DBAL never calls a driver `Result::free()` when a consumer stops iterating (`Doctrine\DBAL\Result` has no `__destruct`; the only reference is the returned Generator's bound `$this`), so the driver holds a `\WeakReference` to the open result and the result cancels its own stream on destruction. A driver holding a STRONG reference would be the only thing keeping an abandoned stream alive, and `break`-ing out of a large `iterateAssociative()` would then transfer the ENTIRE remaining result set on the next statement — invisible in a 100 000-row test, an OOM on a real table. `Ferro\DBAL\Connection::settledRowCount()` makes the two observable: 0 for pure iteration and for abandonment, non-zero only for interleaving. **Fate:** the DBAL 4 SPI carries NO read/write signal — `executeQuery('INSERT … RETURNING id')` with no parameters reaches the driver's `query()`, and the prepared path serves `executeQuery` and `executeStatement` alike — and charter rule 6 forbids inferring one from the SQL text. Every result-producing method on `Ferro\Client\Connection` hard-codes `readonly = true`, so a driver built on them would have reported a lost `INSERT … RETURNING` as **Retryable**, i.e. "provably did not apply", for a write whose fate is genuinely unknown. `Connection::fetchRaw()`/`streamRaw()` exist to let the CALLER declare the fate, the driver declares **write** for everything, and a read-only connection is an explicit `driverOptions['readonly' => true]` — configuration, never inference. Conservative by construction: it never costs safety. **What it DOES cost is stated in full, because it is more than retryability.** `readonly` is read in TWO places in `fate.rs`, and the second is the **57014 override** (`fate.rs:71-114`): with `!in_tx`, a cancelled or timed-out statement is `Cancelled{NonRetryable}` when the client declared `readonly` and `WriteUnconfirmed{Indeterminate}` when it did not. So under this driver **a plain `SELECT` killed by an operator's `statement_timeout` — a normal production setting — surfaces as `Ferro\DBAL\IndeterminateWriteException`**, "your write may or may not have landed", for a statement that wrote nothing. That is cry-wolf on the one exception that must never be routinely retried, and it is a NEW failure shape rather than a lost one: the same statement through the native client API gets a clean "statement cancelled or timed out". Two consequences are discharged in this same change set. (1) The property `run_streamed_exec`'s docblock states — "A streamed READ never becomes `Indeterminate` (classify_fate routes it by `readonly`)" — was never unconditional, and `CLAUDE.md`'s M1-S5 paragraph restated it without the condition; **both are amended here** to say that the guarantee belongs to a client that DECLARES `readonly`, and that the M1-S8b Doctrine driver deliberately does not. (2) It is listed in `docs/known-incompatibilities.md` as a drop-in behaviour difference an operator will hit, and pinned by a live guard (`ExceptionMappingLiveTest::testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne`) that asserts BOTH cells — with `driverOptions['readonly' => true]` a self-cancelled `SELECT` is not an `IndeterminateWriteException`, and on the default write connection it is — so the cost is falsifiable and cannot be silently "fixed" later by inferring read-vs-write from SQL text.

  **(ad) `setTransactionIsolation()` is captured TYPED in an optional `wrapperClass`, and the raw statement is REFUSED (M1-S8b Task 13).** Doctrine's own implementation runs `executeStatement($platform->getSetTransactionIsolationSQL($level))` — `SET SESSION TRANSACTION ISOLATION LEVEL …` on MySQL, `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …` on PostgreSQL — which are exactly the two forms (s) names as FORBIDDEN. On a transaction-mode pool the statement lands on an arbitrary pooled connection, taints it, and hygiene wipes it before the next `BEGIN`: it reports SUCCESS and changes nothing, while `getTransactionIsolation()` keeps returning the level Doctrine cached. (s) also records that the obvious cross-tenant test CANNOT FAIL, because hygiene masks the leak in both directions. `Ferro\DBAL\Wrapper\FerroConnection` overrides the method, captures the level as a typed enum ABOVE the SQL layer, emits nothing, and hands it to the driver connection to ride `BeginRequest.isolation` on the next transaction — where `compose_begin_sql` composes the correct per-transaction form for the dialect. No SQL is inspected, rewritten or generated: the wrapper simply never emits the statement. Without the wrapper the driver REFUSES the statement, prefix-anchored on the two fixed platform-generated strings (anchored specifically so a literal inside an `INSERT` cannot trip it — a false refusal on ordinary SQL would be worse than the bug), with a message naming the one-line configuration fix. `READ UNCOMMITTED` maps to `ReadCommitted`, per `Ferro\Protocol\Isolation`'s own documented mapping — a genuine tightening on MySQL, never a weakening, and listed in `docs/known-incompatibilities.md`. This slice is also the **first real caller** of the hand-duplicated `Isolation` enum that (w) describes, so its cross-language lock is finally exercised by behaviour rather than merely present.
```

- [ ] **Step 10: Correct `CLAUDE.md`, the engine docblock, and the falsified client docblock**

**First, the claim §22.2 (ac) makes false.** Charter DoD: "the relevant SPEC section still tells the truth", and that obligation covers source docblocks that assert a property, because they are what the next reader trusts. Two places state, unconditionally, something that only holds for a client declaring `readonly`:

In `engine/crates/ferrod/src/services/sql.rs` (the `abort_stream` docblock, `:1053`), replace:

```rust
/// then declare the ONE terminal via `classify_fate` under `ctx` (`sent: true` — the statement is
/// dispatched; see [`run_streamed_exec`]'s doc). A streamed READ never becomes `Indeterminate`
/// (classify_fate routes it by `readonly`).
```

with:

```rust
/// then declare the ONE terminal via `classify_fate` under `ctx` (`sent: true` — the statement is
/// dispatched; see [`run_streamed_exec`]'s doc).
///
/// **A stream the CLIENT DECLARED `readonly` never becomes `Indeterminate`** — `classify_fate`
/// routes it by that flag, and by nothing else: the engine performs no read/write inference (charter
/// rule 6). The guarantee is therefore the client's to claim, not the engine's to provide. M1-S8b's
/// Doctrine DBAL driver deliberately declares `readonly = false` for EVERY statement, because the
/// DBAL 4 SPI carries no read/write signal at all, so under that driver a cancelled or
/// `statement_timeout`-ed SELECT DOES classify `Indeterminate` (SPEC §22.2 (ac)). That is the
/// documented cost of the safe default, not a defect here.
```

and in `CLAUDE.md`'s M1-S5 paragraph, change "a streamed **read** is never `Indeterminate`" to "a streamed read **whose client declared it `readonly`** is never `Indeterminate` (the engine never infers; §22.2 (ac) records what that costs the DBAL tier, which declares every statement a write)".

**Then the stale "Next up" paragraph.** `CLAUDE.md`'s closing paragraph was written before S8a finished and is now doubly stale — it lists as carries several items S8a already closed (errno on the wire, pool metadata, the imperative transaction trio, the `I64` narrowing bind + `Kind::Domain` unwrap, `Ferro\Bytes`, dialect-aware isolation `BEGIN`) and it predates this slice entirely. Replace it with an M1-S8b summary in the same style as the other slice paragraphs, and make the new "Next up" list the things that are GENUINELY open: MySQL `query_stream` (§22.2 (n)), the tracker-clean hygiene `None`-skip (R2), `affected` on the stream terminal (a `/proto` change), the `TxNotFound` error code (a `/proto` change), a SQLite backend, and the ORM tier.

In `php/client/src/Client/Value/RawStringValuePolicy.php`, fix the falsified claim (hazard 36): `AbstractPlatform::getDateTimeFormatString()` does **not** reject a fractional `TIMESTAMP` — `DateTimeType::convertToPHPValue` falls back to `new DateTime($value)` — so only the two `datetimetz` claims stand. Cite the measurement and point at `Ferro\DBAL\Value\DbalValuePolicy` as the tier that owns the conversion.

- [ ] **Step 11: Full gate**

```bash
cd /home/abdullak/projects/ferro
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
(cd php/doctrine-dbal && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
```
then the live tiers BY HAND (never `ci/local-gate.sh --live`, hazard 71):
```bash
export FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro"
export FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro"
export FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro"
export FERRO_FERROD_BIN=/home/abdullak/projects/ferro/target/debug/ferrod
cargo build -p ferrod
cargo test --workspace -- --nocapture 2>&1 | tee live.log && ./ci/assert-no-skips.sh live.log && rm -f live.log
(cd php/client && ./vendor/bin/phpunit tests/Live --fail-on-skipped)
(cd php/doctrine-dbal && ./vendor/bin/phpunit tests/Live --fail-on-skipped)
```
Expected: all green.

- [ ] **Step 12: MUTATION-PROVE the acceptance guard**

1. In `testkit/dbal/bootstrap.php`, delete the `instanceof Ferro\Client\Connection` block, and in the generated phpunit config replace `db_driverClass` with `db_driver=pdo_sqlite`. Re-run `./testkit/dbal-suite.sh`. Expected: it RUNS and reports a substantial number of PASSING tests — **against in-memory SQLite, with zero Ferro contact**. That is the false-green hazard reproduced deliberately. Restore both, re-run, and confirm the contact line prints and the run is genuinely Ferro's.
2. In `testkit/dbal-suite.sh`, remove the `grep -q 'db_driverClass is not set'` patch verification and corrupt the `cp` source path. Re-run. Expected: the runner fails loudly at the grep once restored; without it, the run silently uses the upstream `TestUtil` and falls back to SQLite. Restore.
3. Add a `trap 'docker compose … down -v' EXIT` to the runner and run it. Expected: **the shared databases are destroyed.** Do NOT actually perform this mutation — it is recorded here as the reason the runner has no such trap, and hazard 71 is why. Verify by reading instead: `grep -n 'down' testkit/dbal-suite.sh` must return nothing.
4. **The reset.** Run the PG invocation with `--no-reset` twice in a row after a normal run, and diff the result lines. Expected: they DIFFER, and the second is worse — the leftover-state degradation reproduced deliberately (the reviewer measured 23 → 33 errors this way against a driver with zero defects). Then run twice WITH the reset: identical. This is the mutation that proves the recorded number means something. Note the runner already prints `[ferro] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded`, so a `--no-reset` log can never be mistaken for a recordable one.
5. Point `FERRO_DBAL_DSN` at the shared `ferro` database instead of `doctrine_tests`, and run `\dt` in it afterwards. Expected: dozens of `dbal_*`/`test_*` tables now sitting in the database every other live suite uses. Do NOT actually perform this mutation either — verify by reading: `grep -n 'doctrine_tests' testkit/dbal-suite.sh` must show the default DSN, and no invocation anywhere in this plan may name `/ferro` as the suite's database.

- [ ] **Step 13: Commit**

```bash
cd /home/abdullak/projects/ferro
git add testkit/dbal-suite.sh testkit/dbal testkit/mysql-init.sql \
        docs/dbal-suite docs/known-incompatibilities.md \
        php/doctrine-dbal/README.md \
        php/client/src/Client/Value/RawStringValuePolicy.php \
        ferro-spec-v0.2.md CLAUDE.md
git commit -m "docs(m1-s8b): the acceptance runner with a hard contact assertion, the recorded numbers, and the SS14 amendments

SS14's bar is not reachable as written and this says so rather than narrowing the
claim quietly. Ferro has no SQLite backend; the upstream TestUtil discards
db_driverClass and silently falls back to in-memory SQLite (measured), so the
whole functional suite would run GREEN with zero Ferro contact and no
skip-detector would catch it; the testkit MySQL user could not create a database,
so 1057 of 1077 functional tests errored at setup; and even the stock pdo_pgsql
driver has one failure on these containers.

testkit/dbal-suite.sh clones a PINNED tag, replaces TestUtil, launches ONE ferrod
per run, and refuses to execute a single test until it has asserted the
connection is a Ferro one and round-tripped a real SELECT 1. It carries NO
docker compose down trap of any kind.

Amends SS14 for the DBAL 4 SPI it was never written against (no
ServerInfoAwareConnection, no sequence-name lastInsertId, driverOptions instead
of a sealed-shape-violating 'ferro' key) and adds SS22.2 (y)..(ad).

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Slice close-out

**What M1-S8b delivers.** `ferro/doctrine-dbal-driver`: a Doctrine DBAL 4 driver whose execution layer talks to `ferrod` through `ferro/client`, with Grammar/Processor, the DBAL platforms and the stock schema managers untouched. One `driverClass` serves both engine families; the platform is selected from `HELLO_ACK`'s pool kind plus the backend's version string, normalised PostgreSQL-only. Four additive `ferro/client` methods (`fetchRaw`, `streamRaw`, `poolInfo`, `begin`'s isolation parameter) and one engine bind change make it possible; nothing in `/proto` moved.

**What it deliberately does NOT do.**
- **No `/proto` change.** Two candidates are flagged and deferred: `affected` on the stream terminal (which would let the prepared path stream) and a dedicated `TxNotFound` error code.
- **No SQLite**, so one third of §14's acceptance clause waits for `ferro-backend-sqlite`.
- **No ORM functional suite**, with the ORM+PostgreSQL+IDENTITY blocker documented rather than engineered around.
- **No MySQL row streaming** (§22.2 (n), controller decision D-S8b-2) — `iterate*()` buffers there.
- **No `Ferro\Pg\Copy`**, no `read_pool` inference (charter rule 6 forbids it; the charter-compliant shape is a second, explicitly-configured connection).

**Carries into the next slice.**
1. `affected` on the stream terminal — a `/proto` change that would let the prepared path stream and close the last gap in §14's never-buffer clause.
2. MySQL/MariaDB `query_stream` (§22.2 (n)), which would remove the documented streaming asymmetry.
3. The tracker-clean hygiene `None`-skip (R2) — still blocked, and Task 13 makes it more important, not less.
4. The ORM tier, and with it the `SEQUENCE`-strategy documentation for PostgreSQL.
5. A `TxNotFound` error code, to disambiguate `Connection::rollBack()`'s `ERR_PROTOCOL` swallow.
6. The DBAL `^3.8` bridge (D2, an M2 deliverable) and the full known-incompatibilities catalogue (§14 budgets it for M2).
