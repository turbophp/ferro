# Ferro drop-in: known incompatibilities

Ferro is a drop-in for **Doctrine DBAL 4** by CONFIGURATION — `driverClass` + **`wrapperClass`** +
`driverOptions` — with
Grammar/Processor, the DBAL platforms and the stock schema managers untouched. These are the places
where a real application can still notice the difference. Each one is a deliberate consequence of the
engine's model — a per-host daemon that pools upstream connections in **transaction mode** and holds
the only database credentials — not a defect waiting to be fixed quietly. Every entry below was
MEASURED during M1-S8b; the acceptance numbers behind them are in
[`docs/dbal-suite/2026-08-11-results.md`](dbal-suite/2026-08-11-results.md), re-measured after the
compatibility pass in [`2026-08-11-s8c-results.md`](dbal-suite/2026-08-11-s8c-results.md).

> **`wrapperClass` is in that list on purpose: it is REQUIRED, not a refinement.** Omit
> `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class` and an indeterminate write inside
> `$conn->transactional(…)` does not merely get a worse message — its **fate is destroyed**.
> Measured on PostgreSQL 17.10, killing the session so the failure lands on `COMMIT`:
>
> | configuration | what your application catches |
> |---|---|
> | without the wrapper | `Doctrine\DBAL\Exception\NoActiveTransaction` — *"There is no active transaction."* Not a `DriverException`, not retryable, no fate |
> | with the wrapper | `Ferro\DBAL\IndeterminateWriteException` — *"the write may or may not have applied"* |
>
> So the *"catch `DriverException` instead"* remedy given below is only true with the wrapper
> installed. See **A lost connection is not `ConnectionLost`** and **Transactions and session state**.

SPEC §14 budgets the full per-package catalogue for M2. This is the page it grows from.

---

## Errors

### A cancelled or timed-out `SELECT` is reported as an **indeterminate write**

This is the first entry because it is the one that looks alarming and must not be "fixed" by the
obvious workaround.

A statement that PostgreSQL cancels server-side — including one killed by an operator's
`statement_timeout`, a normal production setting — surfaces through this driver as
`Ferro\DBAL\IndeterminateWriteException`: *"your write may or may not have landed"*, for a statement
that wrote nothing.

**Why.** The DBAL 4 SPI carries **no read/write signal**. `executeQuery('INSERT … RETURNING id')` is
indistinguishable from a `SELECT` at the driver boundary, and charter rule 6 forbids inferring the
answer from SQL text. So the driver declares every statement a **write**. The engine's §19.3 fate
matrix reads that flag in two places, and the second is the `57014` (query cancelled) override: with
no transaction open, a cancelled statement is *cancelled/non-retryable* when the client declared
`readonly` and *write-unconfirmed/**indeterminate*** when it did not.

That is the **safe** direction — a lost write is never reported as "provably did not apply" — and
this is its cost. It is a new failure *shape*, not a lost one: the same statement through the native
`Ferro\Client` API, which declares `readonly` for reads, gets a clean "statement cancelled or timed
out".

- **Do NOT add a blanket retry on `IndeterminateWriteException`.** That is exactly the
  at-most-once violation the branch exists to prevent, and it is why
  `IndeterminateWriteException` deliberately does **not** implement
  `Doctrine\DBAL\Exception\RetryableException`.
- **If a connection genuinely only reads, say so:** `'driverOptions' => ['readonly' => true]`
  restores the clean cancellation answer for that connection. Explicit configuration, never
  inference. **It refuses only one of the five autocommit write routes, and it changes the `BEGIN`
  the engine emits — read *Read-only connections* below before you set it.**
- **Inside a transaction the question usually does not arise.** A cancelled statement rolls the
  transaction back, the `tx_id` is tombstoned, and the fate is *known*: `Retryable`. **The one
  exception is MySQL/MariaDB after an implicit commit** — once part of the transaction has already
  been committed, "the transaction will never commit" stops being true and the fate becomes
  `Indeterminate` instead. See *MySQL/MariaDB: an implicit commit changes what a lost statement
  reports* below.

Pinned by `ExceptionMappingLiveTest::testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne`,
which asserts BOTH cells, so the cost is falsifiable and cannot later be silently "fixed" by guessing
read-vs-write from SQL.

### A cancelled statement arrives with a **NULL SQLSTATE** and `getCode() === 0`

An application that branches on `$e->getSQLState()` sees `null` where PDO would give `57014`. The
engine's §19.3 `57014` override rebuilds the error payload and drops both the SQLSTATE and the vendor
errno. The fate is carried by the exception **class** (`Ferro\DBAL\IndeterminateWriteException` vs a
bare `DriverException`) and by the `/proto` code on the chained client exception. Every *other*
error keeps its SQLSTATE, and on the MySQL family its vendor errno as well — which is what the stock
`API\MySQL\ExceptionConverter` keys on.

### A lost connection is **not** `Doctrine\DBAL\Exception\ConnectionLost`

Where DBAL has one bucket, Ferro has two, because the difference is the whole point of the engine:

| what happened | Ferro | why |
|---|---|---|
| the statement's fate is **known** (never transmitted, a declared read, or an in-tx statement whose transaction is now dead **and has committed nothing**) | `Ferro\DBAL\RetryableDriverException` (implements `RetryableException`) | retry is safe |
| the statement's fate is **unknown** | `Ferro\DBAL\IndeterminateWriteException` (deliberately NOT retryable) | retry could apply it twice |

Neither extends `ConnectionLost`, on purpose: frameworks treat `ConnectionLost` as
reconnect-and-retry. Measured at the acceptance gate — upstream's `TransactionTest::testCommitFailure`
and friends kill the backend session and expect `ConnectionLost`; under Ferro they get the refined
answer instead. **If your application catches `ConnectionLost`, catch `Doctrine\DBAL\Exception\DriverException`
instead and branch on the two Ferro classes.**

**That remedy is necessary and not sufficient, and here is the case it does not cover.** DBAL's
`Connection::handleDriverException()` calls `$this->close()` — which resets the transaction nesting
level to 0 — only when the converted exception `instanceof ConnectionLost`. Ferro's classes
deliberately are not, so the wrapper's bookkeeping takes a different path than any stock driver's,
and inside `$conn->transactional(…)` the exception your application actually receives is
`Doctrine\DBAL\Exception\NoActiveTransaction` — *"There is no active transaction."* —
which extends `Doctrine\DBAL\ConnectionException` and **not** `DriverException`, so a `catch
(DriverException)` sees nothing. Measured on **PostgreSQL, MySQL and MariaDB**, through upstream's
own `TransactionTest::testTransactionalFailureDuringCommit` run on the stock connection class. With
the wrapper configured, the same test reports the driver's real verdict
(`IndeterminateWriteException`) instead — equal counts, opposite meanings, which is why the
acceptance runner now ASSERTS the wrapper rather than trusting a number to notice.

**Configure `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class`** — it restores the
driver's verdict, and it is what makes the `DriverException` remedy above true inside
`transactional()`. The full mechanism, and what to do if you need a different wrapper class, is
under *Transactions and session state* below; it is the entry to read if you read only one.

### A type-policy refusal arrives as a plain `DriverException`, with no fate

A value the client's §9.1 policy refuses (`Ferro\Client\Error\TypePolicyException`) carries **no fate
branch at all** — it is a client-side policy refusal, not a database error. It does reach the
exception converter (anything that is not a `Doctrine\DBAL\Driver\Exception` would escape DBAL's
conversion entirely), and it comes out a plain `Doctrine\DBAL\Exception\DriverException`: never an
indeterminate write, never upgraded to retryable. The driver's own refusals
(`Ferro\DBAL\Exception\NonRepresentableValue`, `UnsupportedStatement`, `ServerVersionUnavailable`,
`BackendFamilyUnknown`) take the same branch-less path. A malformed payload remains a
`ProtocolException`.

---

## Read-only connections

`'driverOptions' => ['readonly' => true]` is offered twice above as the answer to the indeterminate
`SELECT`, and it is. It also changes behaviour in two ways an operator will not guess from that
sentence — it refuses one write route outright, and it changes the SQL the engine emits for `BEGIN`
— so both are stated here rather than left to be discovered during an incident.

### It is enforced only PARTLY — but it can no longer weaken a fate

Two separate questions, and it matters that they have different answers: *does the flag stop a
write?* (partly) and *can the flag make a failed write look safe to retry?* (**no longer**).

**The fate half is closed.** A `readonly` declaration can no longer turn a lost autocommit statement
into a "provably did not apply" verdict. Measured on PostgreSQL 17, the same parameterised writing
statement destroying its own session, on two connections differing only in the flag:

| what happened | `readonly` unset (default) | `readonly => true` |
|---|---|---|
| connection lost mid-statement, autocommit | `Ferro\DBAL\IndeterminateWriteException` | `Ferro\DBAL\IndeterminateWriteException` |

Earlier builds answered `RetryableDriverException` in the right-hand cell — a write whose fate was
genuinely unknown handed to a framework retry loop with the marker meaning *"replaying this is
safe"*. That was the exact inversion of the guarantee this page is about, and the driver now
**re-mints** it: for an autocommit statement on a `readonly` connection, a retryable verdict is
downgraded to indeterminate, because the declaration is unverifiable and the write may have landed.
In-transaction losses, pool timeouts and deadlocks keep their true verdicts.

**The enforcement half is deliberately incomplete, and here is exactly where the line is.** Charter
rule 6 forbids inferring read-vs-write from SQL text, and the DBAL 4 SPI carries no read/write
signal — `executeQuery('INSERT … RETURNING id')` is indistinguishable from a `SELECT`. What the SPI
*does* carry is which method the application called. So exactly one entry point is refused pre-send:
`Doctrine\DBAL\Connection::executeStatement($sql)` **with no parameters**, which is DBAL's write API
and a fact about the caller, not the SQL. Measured, autocommit, on a `readonly => true` connection:

| route | result |
|---|---|
| `executeStatement($sql)` — no parameters | **refused** before it is sent |
| `executeStatement($sql, [$params])` | the write **lands** |
| `executeQuery($sql)` / `executeQuery($sql, [$params])` | the write **lands** |
| `prepare($sql)->executeStatement()` | the write **lands** |

Four of the five still write, because they share a code path with parameterised *reads* that
refusing would break. **So `readonly` is a declaration you must still be able to defend:** a
connection carrying it must not be able to reach a write — including a rarely-taken audit `INSERT`
in an error path — and what protects at-most-once when one slips through is the fate half above, not
the refusal. Full enforcement needs the engine to run readonly autocommit statements inside a
server-enforced read-only transaction, which is a `ferrod` change and is not done.

### It DOES change the SQL the engine emits for `BEGIN`

Inside an explicit transaction the flag is enforced, by the backend, because the engine composes the
`BEGIN` with it: `BEGIN … READ ONLY` on PostgreSQL, `START TRANSACTION READ ONLY` on the MySQL
family. Measured, both families:

```
pg     SQLSTATE 25006  cannot execute INSERT in a read-only transaction
mysql  SQLSTATE 25006  Cannot execute statement in a READ ONLY transaction.   (errno 1792)
```

So a write path that happens to be wrapped in `transactional()` fails loudly the first time it runs,
while the same write outside a transaction succeeds silently — the two halves of the flag disagree,
and that is worth knowing before an incident rather than during one.

---

## Connection object

- **`getNativeConnection()` returns a `Ferro\Client\Connection`, not a `PDO`.** Anything calling
  `pg_escape_string($native, …)`, `$native->real_escape_string()` or a `PDO::` method will fatal.
- **`getNativeConnection()` does not escape the driver's type boundary.** It hands back the very
  connection the driver built, which carries `Ferro\DBAL\Value\DbalValuePolicy` — so a column the
  driver refuses is refused there too. To read a `24:00:00`, a zero-in date, an `infinity` or a
  sub-second `timestamptz`, open a connection of your OWN through `Ferro\Ferro::connect()` (default
  `M1ValuePolicy`, or `RawStringValuePolicy` for the raw canonical text). The refusal is a
  **driver-tier policy**, not an engine limitation.
- **No database credentials exist in PHP.** The DSN lives in the engine (SPEC §12 / D8). The DBAL
  `user`, `password`, `dbname` and `charset` parameters are therefore inert — measured at the
  acceptance gate, where upstream's `testInvalidUserName` / `testInvalidPassword` / `testInvalidHost`
  cannot fail and `testInheritCharsetFromPrimary` reports the engine's `utf8mb4` rather than the
  requested `latin1`. Tooling that shells out to `pg_dump`/`mysqldump` with the application's config
  cannot work; ops provisions separate dump credentials.
  **`host`/`port` are the exception: they are read, and they select the FERRO daemon's TCP endpoint**
  (the `FERRO_ADDR` fallback) when no `unix_socket`/`driverOptions.socket` is configured. They never
  name a database server. If you set both a socket and a host, the socket wins and the host is
  ignored without a diagnostic — which is what upstream's `testInvalidHost` actually measures.
- **A misconfigured key is refused, not ignored.** The top-level `ferro` key and `read_pool` at any
  level (both shipped in earlier drafts of this project's own README and SPEC §14) raise an
  `InvalidArgumentException` naming the replacement, as does any unrecognised key inside
  `driverOptions`. Ignoring one is how a connection ends up on the pool named `default` — a different
  DSN, possibly a different database — with no diagnostic at all.
- **A pool whose BACKEND is unreachable fails at `getDatabasePlatform()`, not at connect.**
  Connecting succeeds because the Ferro handshake never depends on backend availability; the platform
  needs the server version, which does. The failure is a loud
  `Ferro\DBAL\Exception\ServerVersionUnavailable` naming the pool — never a silently-defaulted
  platform, because a wrong platform is a wrong SQL dialect. Pin `'serverVersion' => '17.10'` in the
  DBAL params if you want a zero-round-trip answer.
- **The first query against a backend that is DOWN can block for the OS connect timeout**
  (~127 s measured) rather than failing fast. Tracked in
  `docs/followups/2026-08-10-unbounded-backend-dial.md`.

---

## Identity and keys

- **`lastInsertId()` throws on PostgreSQL, always.** PG's protocol carries no such field and Ferro
  refuses to emulate it with `SELECT lastval()`, because on a transaction-mode pool the follow-up
  runs on a **different connection** and returns a silently wrong key. Use `INSERT … RETURNING id`.
  The thrown class is the SPI's own `Doctrine\DBAL\Driver\Exception\NoIdentityValue`, wrapped by DBAL
  into a `DriverException` as usual.
- **`lastInsertId()` has no sequence-name argument.** DBAL 4 removed the overload; this is upstream,
  not Ferro.
- **`lastInsertId()` is cleared by a failed statement** — a deliberate divergence from PDO. Read it
  immediately after the successful INSERT.
- **Doctrine ORM + PostgreSQL + the default IDENTITY strategy cannot insert.**
  `Doctrine\ORM\Id\IdentityGenerator::generateId()` is `(int) $conn->lastInsertId()`, and DBAL 4
  defaults PostgreSQL to `GENERATOR_TYPE_IDENTITY`. Configure the **SEQUENCE** strategy for the
  PostgreSQL platform through the ORM's `Configuration::setIdentityGenerationPreferences()`, keyed on
  `Doctrine\DBAL\Platforms\PostgreSQLPlatform::class`. (The exact constant for the strategy is ORM's
  own and is not restated here: `doctrine/orm` is not a dependency of this repository, so nothing in
  this file has been verified against it. The mechanism, and the reason it is needed, are what this
  entry is asserting.)

  **Drop-in is config-only for DBAL, and is explicitly NOT config-only for ORM on PostgreSQL.** The
  engine's pooling model is not bent to fit an ORM default; the honest one-line configuration is.
- **ORM multi-table DELETE/UPDATE on class-table inheritance needs an explicit transaction.**
  `MultiTableDeleteExecutor` issues `CREATE TEMPORARY TABLE`, `INSERT`, `DELETE` and `DROP` as four
  separate statements with no transaction; on a transaction-mode pool statements 2-4 land on
  different connections. Wrap the query in `$conn->transactional(…)`. (Read from `doctrine/orm 3`
  during M1-S8b research; like the entry above, it has **not** been re-verified at the acceptance
  gate, because the ORM suite is not run — see `docs/dbal-suite/2026-08-11-results.md`.)

---

## Transactions and session state

- **`'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class` is REQUIRED, because
  `transactional()` otherwise throws the write's fate away.** This is the entry to read if you read
  only one.

  `Doctrine\DBAL\Connection::transactional()` exempts exactly five classes — `TransactionRolledBack`,
  `UniqueConstraintViolationException`, `ForeignKeyConstraintViolationException`,
  `DeadlockException`, `ConnectionLost` — from the rollback it runs after a failed commit. Anything
  else reaches a `rollBack()` at nesting level 0 (the commit's own `finally` already decremented it),
  which raises `Doctrine\DBAL\Exception\NoActiveTransaction` **from a `finally`, replacing the
  exception in flight**. MEASURED, on the stock connection class: a lost COMMIT inside
  `$conn->transactional(fn ($c) => $c->executeStatement('INSERT …'))` reaches the application as
  **"There is no active transaction."** — a message that reads like a programming error and is
  routinely logged and ignored — while `Ferro\DBAL\IndeterminateWriteException` survives only as
  `getPrevious()`, where no `catch` block keys on it. The write may have landed.

  This is worse than what Ferro replaces: `pdo_pgsql` reports the same event as `ConnectionLost`,
  which IS exempt, so it propagates cleanly there.

  **The fix is the wrapper**, which restores the driver's verdict after DBAL's cleanup has
  substituted its own. The exception's ANCESTRY was not changed instead, and the reason is worth
  stating because it looks like the shorter fix: `ConnectionLost` is the only one of the five that
  tells the truth about an indeterminate write, and it is declared `final`; `DeadlockException`
  carries `Doctrine\DBAL\Exception\RetryableException`, which would invite a framework to replay the
  write; and the remaining three each assert a fate ("it did not apply", "a duplicate exists") that
  the engine did not report. `Ferro\DBAL\IndeterminateWriteException` therefore stays a plain
  `Doctrine\DBAL\Exception\DriverException`, catchable as `Doctrine\DBAL\Exception` and **never** as
  `RetryableException`.

  An application that must configure a different `wrapperClass` (`PrimaryReadReplicaConnection`, or
  its own) recovers the guarantee by composing `Ferro\DBAL\Wrapper\IndeterminateSafeTransactional`
  into it. An application that keeps the stock `Doctrine\DBAL\Connection` gets the masked behaviour;
  that is the incompatibility, and it is not silent — it is this entry.

  **Residual, stated rather than buried:** a driver exception raised INSIDE the closure is masked by
  a second route — the rollback DBAL issues from its first `finally` runs at nesting level 1, reaches
  the driver, and a LOUD failure there replaces the original. That route is not repaired, because
  telling it apart from an application deliberately wrapping the exception needs a chain walk, and a
  chain walk would let the wrapper overrule an application that had already handled the write. In
  practice the client swallows both reachable rollback failures (a lost frame, a tombstoned `tx_id`),
  and §19.3 classifies an in-transaction statement `Retryable`, never `Indeterminate`, so the commit
  boundary is where the branch actually arrives.
- **`setTransactionIsolation()` requires the wrapper.** Configure
  `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class`. Without it the raw
  `SET SESSION TRANSACTION ISOLATION LEVEL …` / `SET SESSION CHARACTERISTICS AS …` statement is
  **REFUSED, loudly**, with a message naming this one-line fix. That refusal is the kind treatment:
  left alone, the statement lands on an arbitrary pooled connection, reports SUCCESS, is wiped by
  hygiene before the next `BEGIN`, and your application silently gets the pool default while
  `getTransactionIsolation()` keeps reporting the level it asked for. With the wrapper, the level is
  captured as a typed enum above the SQL layer and rides `BEGIN` on the next transaction.
- **`READ UNCOMMITTED` is upgraded to `READ COMMITTED`** — never weakened. PostgreSQL treats them as
  the same level; on MySQL this is a genuine, documented **tightening**.
- **`setAutoCommit(false)` must be configured before the first connect**, on
  `Doctrine\DBAL\Configuration`. Calling `Connection::setAutoCommit(false)` on an already-connected
  connection opens nothing (DBAL's `beginTransaction()` lives in `connect()`, which returns early)
  and the next `commit()` raises `NoActiveTransaction`. That is upstream behaviour, measured.
- **`setAutoCommit(false)` pins a backend connection for the whole request**, and re-pins immediately
  after every commit — measured with `pg_current_xact_id()` identical across statements. It works;
  it just turns Ferro's central win off.
- **Savepoints work normally.** DBAL nests transactions client-side and emits ordinary savepoint SQL;
  those statements ride the same pinned `tx_id` as the transaction that opened them.

---

## MySQL/MariaDB: an implicit commit changes what a lost statement reports

**What you will see.** On MySQL and MariaDB, once a statement inside your transaction has
implicitly committed, every LATER failure in that same transaction reports
`IndeterminateWriteException` — "we cannot tell you whether your write landed" — instead of a
retryable error. **Do not add a blanket retry to make this go away.** The whole point is that
retrying is no longer safe.

**Why.** MySQL and MariaDB have no transactional DDL. A statement from the implicit-commit family
inside an explicit transaction COMMITS everything before it and ends the transaction — the server
does this silently, and it does it even when the statement then FAILS. Ferro used to report a later
in-transaction loss as retryable, on the reasoning "the transaction will never commit, so replaying
it is safe". That reasoning is false once part of the transaction has already been committed:
replaying re-applies writes that are already durable. PostgreSQL is unaffected — its DDL is
transactional — and its behaviour is byte-identical to before.

**Which statements do it.** The MySQL manual's "Statements That Cause an Implicit Commit" families,
and three that are easy to miss:

| shape | note |
|---|---|
| `CREATE` / `DROP` / `ALTER` / `RENAME` / `TRUNCATE` (non-`TEMPORARY`) | the common case — any migration |
| `LOCK TABLES` / `UNLOCK TABLES`, `START TRANSACTION`, `SET autocommit` | |
| `GRANT` / `REVOKE` / `SET PASSWORD` / **`SET DEFAULT ROLE`** | account management; `SET ROLE` does **not** commit |
| **`EXECUTE`** of a prepared DDL | the commit belongs to the statement that RUNS, not to how it was dispatched — `PREPARE` and `DEALLOCATE` alone are safe |
| **`/*! … */`** and MariaDB's **`/*M! … */`** | an *executable comment* is not a comment: the server runs it. `mysqldump` emits DDL this way, so a replayed dump hits it |
| `CALL` / `DO` | a routine may run any of the above |

**Who meets this in practice.** Doctrine Migrations on MySQL, where transactional migrations are the
default: `BEGIN` → insert the migration-log row → `ALTER TABLE` (the log row is now committed) → a
later failure. Ferro now tells you the truth about that transaction instead of inviting you to
replay it.

**What to do.** Treat `IndeterminateWriteException` as it is meant to be treated: reconcile, do not
retry. If you need retryable migrations on MySQL, the fix is upstream of Ferro — MySQL cannot give
you a transactional `ALTER`.

**One deliberate cry-wolf.** A statement that errors *without running* (a syntax error on a DDL, say)
cannot be distinguished from one that ran and committed, using only the information available before
dispatch, so it is treated as if it had committed. That direction is chosen on purpose: a spurious
`Indeterminate` costs you a reconciliation you did not need, while the opposite mistake silently
double-applies a write.

---

## Values

The driver's type boundary is a **conversion step the driver owns**, not SQL rewriting. It exists
because Doctrine's stock type layer is, measured on 4.4.4, a silently-corrupting calendar parser.

- **A value Doctrine would parse INCORRECTLY is refused, not converted.** Measured, with **no
  exception raised** by stock DBAL: `date '2026-00-05'` → `DateTime(2025-12-05)`;
  `datetime '0000-00-00 00:00:00'` → `DateTime(-0001-11-30)`; PostgreSQL's legal `time '24:00:00'` →
  `00:00:00`. Through this driver each of those raises
  `Ferro\DBAL\Exception\NonRepresentableValue` instead. **We refuse what PDO corrupts.**
  The full refused set: PG `time '24:00:00'`, PG `date`/`timestamp` `infinity`/`-infinity`, MySQL
  zero and zero-in dates, MySQL negative `TIME` intervals, sub-second `TIME`, and sub-second
  `TIMESTAMPTZ` (refused rather than truncated — silent precision loss is the same defect class).
  Read those columns through your own `Ferro\Client\Connection`, or cast them in SQL.
- **`datetimetz` is re-rendered per platform.** `DateTimeTzType` has no fallback and accepts only
  `Y-m-d H:i:sO` on PostgreSQL and `Y-m-d H:i:s` on the MySQL family, so no canonical RFC3339 form
  parses anywhere. A whole-second `TIMESTAMPTZ` is re-rendered into the platform's own format; a
  sub-second one is refused.
- **An integer parameter above `PHP_INT_MAX` is refused client-side**, not silently saturated
  (a PHP `(int)` cast saturates rather than wrapping). Bind it as a string against a `numeric`
  column, or keep it in `bigint` range.
- **The whole signed 64-bit range reads as a PHP `int`.** Measured through the driver:
  `4294967295`, `4294967296`, `9223372036854775806` and `9223372036854775807` all arrive as `int`.
  (An earlier build of `php/client` threw `ProtocolException: value tag 2: expected a int payload,
  got string` at 2^32; fixed — the turnover is `PHP_INT_MAX`, where it belongs.)
- **A MySQL `BIGINT UNSIGNED` above `PHP_INT_MAX` arrives as a decimal STRING**, because PHP has no
  wider integer: `SELECT CAST(18446744073709551615 AS UNSIGNED)` → `string('18446744073709551615')`.
  That is the `u64_overflow` policy answer, not a defect. PostgreSQL has no unsigned integer type,
  so the case cannot arise there.
- **A `LARGE_OBJECT` bind is materialised in memory** and is bounded by the 16 MiB maximum frame
  payload. A chunked bind would be a protocol change.
- **`BINARY` / `LARGE_OBJECT` are the only route to binary.** Every bare PHP string binds as text;
  the driver wraps those two `ParameterType`s in `Ferro\Bytes` for you.
- **MySQL/MariaDB sessions run at `time_zone = '+00:00'`.** `NOW()`, `CURDATE()` and `CURTIME()`
  return UTC on every Ferro MySQL connection. Doctrine and Laravel make the same choice; pooling
  determinism requires it.

---

## Schema, migrations and introspection

- **A PostgreSQL type outside Ferro's 14 canonical tags reads as PostgreSQL's own TEXT**, not as a
  typed value and not as a loud refusal (decision D-S8b-6). Measured through the driver:

  ```
  '{1,2,3}'::int[]      -> string('{1,2,3}')      '[1,10)'::int4range  -> string('[1,10)')
  inet '192.168.0.1'    -> string('192.168.0.1')  '1 day'::interval    -> string('1 day')
  '12:34:56+02'::timetz -> string('12:34:56+02')  point '(1,2)'        -> string('(1,2)')
  ```

  This is exactly what `pdo_pgsql`/libpq hand back, so PostGIS (`geometry`/`geography` as EWKB hex),
  `hstore`, `ltree`, `citext`, native enums and every future extension type work unmodified — and it
  is what makes the stock schema manager work (below). The consequence to know: **an array does not
  arrive as a PHP array.** Parse it, or cast it in SQL. The 14 canonical types keep their typed,
  policy-aware path.
- **The stock PostgreSQL schema manager works.** Directly measured: `introspectTable()` (columns
  typed through `Integer`/`Text`/`Decimal`), `listTableIndexes()` with multi-column ordering, and
  `listTableNames()`; the wider surface — sequences, schemas, views, foreign keys, comparison — is
  covered by upstream's own `Schema\*` classes, all green in the acceptance run. It was blocked
  until this branch by one catalog type (`pg_index.indkey`, an `int2vector`); the text fallback above
  is what unblocked it, together with widening the PG bind matrix so a stock `BooleanType`
  `int(1)` and the platform's `? || ' SECOND'` date arithmetic bind. Both former workarounds
  (`$conn->insert('t', ['flag' => true], ['flag' => Types::BOOLEAN])`, a `?::int` cast) are no longer
  needed. **`doctrine/migrations` is separately measured** — `testkit/migrations-e2e.sh` drives the
  real `doctrine-migrations` CLI (3.9.7, unpatched) through diff → migrate → empty diff → rollback,
  with every existence check made by `psql` inside the container.
- **The application user has no `CREATE DATABASE` privilege** in the testkit, deliberately. Anything
  that provisions databases needs its own credentials.

---

## Performance and shape

- **The streamed route is the PARAMETERLESS route, not the "read" route.** On PostgreSQL, every
  zero-parameter statement reaching `Doctrine\DBAL\Connection::executeQuery()` is streamed —
  `UPDATE`, `DELETE`, `INSERT … RETURNING`, `CREATE TABLE`, exactly as much as `SELECT`. The driver
  never looks at the verb, because charter rule 6 forbids inferring read-vs-write from SQL text, so
  it *cannot* be bounded to reads. The **ERROR** and **`rowCount()`** entries below are the
  consequences of that, and they are why the distinction is worth stating precisely rather than as
  "the read path". Everything with parameters, and everything on
  MySQL/MariaDB (where engine-side row streaming is still deferred), buffers: a streamed request's
  terminal carries no `affected` field, so streaming the parameterised path would make every
  parameterised write return `0` from `executeStatement()`.
- **Abandoning an iteration cancels the stream — for the canonical idiom.**
  `foreach ($conn->iterateAssociative($sql) as $row) { break; }` cancels: the driver `Result` is
  destroyed by refcount and frees itself. `$it = $conn->iterateAssociative($sql); foreach ($it as $row) { break; }`
  does **not**: `$it` keeps it alive, and the remainder is transferred on the next statement
  (measured: 99 975 of 100 000 rows). A live reference is indistinguishable from a caller who may
  still fetch, so this is a PHP refcount fact, not a design choice. **Iterate the call directly, or
  `unset()` the iterator.**
- **Inside an open transaction, abandoning an iteration DRAINS the remainder instead of cancelling
  it.** Same result for your code, different cost: the rows you did not read still cross the wire
  (at constant memory — they are discarded as they arrive), where in autocommit the query would have
  been cancelled outright. `Ferro\DBAL\Connection::abandonDrainedRowCount()` reports what that has
  cost on this connection: `0` outside a transaction, the size of the abandoned remainder inside one.
  It is not tunable, because the alternative is not "slower", it is *wrong*: the cancel becomes a
  real backend `CancelRequest`, which aborts the running statement, which puts an open `BEGIN` block
  into PostgreSQL's ABORTED state — so the transaction would be rolled back and every write you had
  already made in it silently lost. If the remainder is large enough for the transfer to matter,
  `LIMIT` the query.
- **A statement issued *while* an iteration is open drains the remainder into memory first** — the
  session is strictly single-in-flight. The canonical
  `foreach (iterate…) { executeStatement(…) }` idiom therefore keeps working, at the cost of
  buffering what is left, which is what PDO does unconditionally.
  `Ferro\DBAL\Connection::settledRowCount()` reports how many rows that has cost on this connection:
  `0` for pure iteration and for a properly abandoned one, non-zero only for interleaving.
- **On the streamed route, a statement's ERROR is not raised by `executeQuery()` — and if you
  discard the result it is not raised AT ALL.** The open reads only the column header, so the
  failure arrives when the rows do. Measured on PostgreSQL:

  ```php
  $c->executeQuery('INSERT INTO t VALUES (1)');   // duplicate key, result discarded
  // -> returns a Doctrine\DBAL\Result. NO exception, ever. The row is not inserted, the
  //    connection stays healthy, and nothing in the application learns the write failed.
  $c->executeQuery('INSERT INTO t VALUES (1)')->fetchAllAssociative();
  // -> UniqueConstraintViolationException, as you would expect.
  ```

  A `CREATE TABLE` that already exists behaves the same way. `executeStatement()` throws
  immediately on both families, and on **MySQL** `executeQuery()` throws immediately too (nothing
  streams there) — so this is PostgreSQL-only and route-only, i.e. invisible to a MySQL-first
  reader. **Do not issue DML or DDL through `executeQuery()` on PostgreSQL:** use
  `executeStatement()`, which is what DBAL's own API intends, or consume the result.
- **`rowCount()` diverges by ROUTE, family and verb.** The one wrong cell is PostgreSQL's streamed
  route, whose terminal carries no `affected`:

  | | PostgreSQL | MySQL/MariaDB |
  |---|---|---|
  | `executeStatement(...)`, with or without params | correct | correct |
  | `executeQuery(sql, [params])->rowCount()` on DML | correct | correct |
  | **`executeQuery(sql)->rowCount()` on DML, no params** | **`0`, always** | correct |
  | `executeQuery(...)->rowCount()` on a `SELECT` | row count buffered, `0` streamed | `0` |

  Measured: a 10-row `executeQuery('UPDATE t SET x = 1')` reports `0` on PostgreSQL and `10` on
  MySQL, having updated 10 rows on both. So a guard like
  `if ($conn->executeQuery('UPDATE … WHERE id = 7')->rowCount() === 0) { throw new NotFound(); }`
  fires on **every successful update** on PostgreSQL. This compounds with the `lastInsertId()` entry
  above, which sends PostgreSQL users to `INSERT … RETURNING id` — exactly the parameterless shape.
  Use `executeStatement()` for anything whose count you read. The `SELECT` row is not normalised
  because normalising means counting rows — the conflation §14 warns about — and DBAL itself
  documents `rowCount()`-on-a-`SELECT` as driver-specific.
- **`free()` keeps `rowCount()`** while emptying rows and columns. Upstream is split on this
  (SQLite3 keeps its count, PgSQL answers `0`); ours is a choice.
- **`Ferro\Pg\Copy`** — the first-class replacement for `pdo_pgsql` COPY hacks named in SPEC §14 —
  does not exist yet. Deferred.

---

## Daemon limits an operator should know about

M1-S9a added three `ferrod` knobs after a review found one local client could pin the host-wide
daemon's memory and file descriptors indefinitely. Defaults are chosen so an ordinary PHP-FPM fleet
never meets them, but they are limits and you should know they exist.

| knob | default | what it does | when to raise it |
|---|---|---|---|
| `max_connections` | **512** | caps concurrently accepted sessions | an fd budget sized under systemd's 1024 `DefaultLimitNOFILE`, not a round number. Raise it (and `LimitNOFILE`) if you run more than ~512 FPM workers per host |
| `frame_read_timeout` | **30 s** | a **stall** detector — how long a session may make *no progress* | it measures time since the last progress, not since the frame started, so a slow-but-progressing client is never severed. Raise it only if a legitimate client can stall longer than 30 s mid-frame |
| `idle_timeout` | **disabled** | closes a session doing nothing at all | deliberately off. The sync PHP client cannot ping while blocked between web requests, so ANY nonzero default would sever every quiet worker on the host — a new outage class, not a fix. Set it only if you know your fleet |

An idle close is silent: there is no request to fail, and the client's resilience loop reconnects on
next use. `max_connections` and `frame_read_timeout` are the actual teeth against the hazard; the
idle knob exists for operators who want old workers' connections reclaimed promptly during a rolling
deploy.

**Restart timing.** SIGTERM now drains — sessions, pools and transaction actors all wind down rather
than being cut mid-wire. The worst-case stop is `drain_deadline + session_drain_grace`, and §18's
`TimeoutStopSec` must exceed it. That relationship is enforced at compile time: a backstop that ties
with or undercuts the budget it backs fails the build.

---

## Not supported, and where it went

- **SQLite:** there is no SQLite backend. `AnyPool` is `{ Pg | Mysql }`.
- **Named parameters at the driver:** positional `?` only. DBAL expands named parameters above the
  driver for `executeQuery()`/`executeStatement()`, so this is only visible if you call
  `prepare()->bindValue(':name', …)` yourself — exactly as capable as the stock mysqli driver.
- **`read_pool` as a config key:** it does not exist, at any level, and is **refused by name** rather
  than ignored. Charter rule 6 forbids inferring read-vs-write, so the charter-compliant shape is a
  **second, explicitly configured connection** carrying `'driverOptions' => ['readonly' => true]` —
  whose two consequences are documented under *Read-only connections* above.
- **A top-level `ferro` config key:** it does not exist either, and is likewise refused. Everything
  Ferro reads lives in `driverOptions` (`socket`, `pool`, `readonly`, `connect_timeout`,
  `io_timeout`) plus the top-level `unix_socket`/`host`/`port`.
