# Phase C (M2 — the Eloquent milestone): slice plan

**Status: SCOPE ONLY. No Phase C code has been written.** This document exists so C1 is sliced
before it is started, the way B2 was — that split (fork edit → reclaim seam → bound → parkable conn
→ stream → driver flip) is why six risky changes landed without a rollback, each proven in CI before
the next built on it.

Every claim below is marked **[verified]** (checked against the code this iteration) or
**[UNVERIFIED]** (a premise the owning slice must check FIRST). Phase B produced four items whose
stated premise did not survive contact; the marking is there so that cost is not paid again.

## The shape of the milestone

§15's mechanism is the same shape as §14's and the same charter rule 6 constraint applies: register
`ferro-{mysql,pgsql,…}` via `Illuminate\Database\Connection::resolverFor()`, subclass the matching
Illuminate connection class, and override **the execution layer only** — stock Grammar, stock
Processor, stock Schema builder.

**S8a/S8b is the precedent to copy, including its split.** S8b could not start until S8a had built
the seven engine/client carries it needed. The same question decides C1's first slice: *what does
the Illuminate tier need that `ferro/client` does not have yet?*

## Carries: what the Eloquent tier needs from below

| Need (§15) | Status | Notes |
|---|---|---|
| `select()`, `statement()`, `affectingStatement()`, `unprepared()` | **[verified]** present | `fetchRaw` / `fetch:none` + `affected`, all built for S8b. |
| `cursor()` → `LazyCollection` | **[verified]** present | `streamRaw()` streams on BOTH families since Phase B item B2; the pool-kind gate is gone. This was the largest C1 dependency and it is already paid. |
| `beginTransaction`/`commit`/`rollBack` + savepoints | **[verified]** present | Imperative trio (S8a) + savepoint SQL passthrough (§22.2 (r)). |
| `lastInsertId` | **[verified]** present | On the wire since S8a; on stream terminals since B2c. |
| `getAttribute(SERVER_VERSION)` for the PDO shim | **[verified]** present | `poolInfo()` carries `server_version` (S8a, `HELLO_ACK` v2). |
| **`selectResultSets()` — MULTIPLE result sets** | **GAP [verified], but NOT the binding constraint** | `ExecOk` carries exactly ONE `cols` + ONE `rows`, so several result sets have no wire representation; §15 lists the method without noting this. **The deeper blocker, also [verified]:** a MySQL `CALL` returns no usable rows today — a prepared `CALL` declares zero result columns even when the procedure emits a result set, so the streamed path discards the rows and the buffered path yields N cell-less rows. Fixing the wire without fixing that would ship a feature that still returns nothing. See C1a. |
| `DB::transaction($fn, attempts: 3)` retry mapping | **[verified] — works, but ONLY if the tier's exception follows PDO's code convention, NOT DBAL's** | See "The `attempts:` requirement" below. Getting it backwards silently disables retry for PostgreSQL serialization failures. |
| `read`/`write` split → a second pool | **[verified] — no engine work needed** | `PoolSpec` (name, dsn, kind, pin_functions, pin_on_unknown) has NO replica or read-role concept; pools are just named DSNs, and `read-replica` appears in the tree only as an example pool NAME in a config test. That is GOOD news: Laravel's `'read' => ['pool' => 'main_ro']` is satisfied entirely client-side by selecting a different pool name per query. **The work is inheriting Illuminate's stickiness rules, not building replication:** the base `Connection` already decides read-vs-write per query (a write makes subsequent reads sticky; reads inside a transaction go to the write connection) and expresses that by picking `getPdo()` vs `getReadPdo()`. Our execution layer does not use PDO, so the subclass must read WHICH role the base class selected and map it to a pool name — inheriting the semantics rather than re-deriving them. |
| `FerroPdoShim` (`quote`, `lastInsertId`, `inTransaction`, `exec`, `getAttribute`) | **[decided] — do NOT implement `quote()` speculatively** | `lastInsertId`/`inTransaction`/`exec`/`getAttribute` are all backed by things that already exist. `quote()` is different in kind: implementing it means owning dialect-specific SQL string escaping, which is security-critical code written for no known caller. §15 lists it because ecosystem packages touch it, but WHICH packages and HOW is unknown. The slice that finds a real caller decides; until then it refuses, with a message naming the alternative (parameter binding). This follows the house stance set at S7 — *we refuse what PDO corrupts* — and charter rule 6, and it is reversible in the safe direction: a refusal can become an implementation, an unnoticed escaping bug cannot be un-shipped. |

## The `attempts:` requirement (verified against the INSTALLED `illuminate/database` v11.51.0)

**Container feasibility, checked first:** `illuminate/database ^11.0` installs cleanly here
(v11.51.0, via `env -u GITHUB_TOKEN COMPOSER_AUTH='{}' composer install`), so C1b can be built and
tested locally rather than only in CI. The three structural facts below were read out of that
installed tree, not fetched from a branch — `Connection::resolverFor($driver, Closure)` exists at
`Connection.php:1683` with a `static::$resolvers` map, `QueryException extends PDOException`
(`QueryException.php:9`), and it does `$this->code = $previous->getCode()` (line 48).

`DB::transaction($fn, attempts: 3)` retries iff `causedByConcurrencyError($e)` is true. That helper
matches on exactly two things:

1. `$e instanceof PDOException` **and** `($e->getCode() === 40001 || $e->getCode() === '40001')` —
   verbatim from the installed source, so EITHER the int or the string form satisfies it; or
2. the exception MESSAGE containing one of a fixed list of substrings — among them
   `"Deadlock found when trying to get lock"`, `"deadlock detected"` and
   `"Lock wait timeout exceeded; try restarting transaction"`.

Two consequences, and the second is the one that would have shipped silently.

**Criterion 2 already works, by faithfulness rather than by design.** Ferro preserves the raw server
message verbatim on every fate arm, so a MySQL deadlock, a PG `deadlock detected` (PostgreSQL's own
deadlock wording, and it IS in the list) and a MySQL 1205 lock-wait timeout all match those
substrings as-is. Nothing is needed for those — **which is exactly what makes the gap below easy to
miss**: the common cases pass without anyone implementing anything.

**Criterion 1 requires the tier's exception to put SQLSTATE in `getCode()` — the OPPOSITE of what the
DBAL driver does.** `Illuminate\Database\QueryException extends PDOException` and its constructor
does `$this->code = $previous->getCode()`, i.e. it inherits the code from OUR driver exception. PDO's
convention is that `getCode()` IS the SQLSTATE; DBAL's convention (and `Ferro\DBAL\Exception\DriverException`'s)
is that `getCode()` is the vendor ERRNO, with SQLSTATE in `getSQLState()`. **If the Eloquent tier
copies the DBAL tier's convention, criterion 1 never fires**, and a PostgreSQL serialization failure —
SQLSTATE `40001`, whose message `could not serialize access due to concurrent update` matches NONE of
the substrings in criterion 2 — is **never retried**, even though Ferro classified it `Retryable`
correctly. `attempts: 3` would appear to work (deadlocks retry via criterion 2) while silently not
working for the one case SERIALIZABLE workloads depend on.

So: **the Eloquent tier's driver exception MUST follow PDO's convention, and a live guard must assert
that a PG serialization failure actually re-runs the closure** — not merely that it classifies
Retryable. This is the same shape as §22.2 (ac)'s lesson: a correct classification is worthless if
the tier above cannot read it.

## Proposed slices

- **C1a — the `selectResultSets()` wire gap. DECIDED while scoping: documented incompatibility for
  M2, because the wire is not its real blocker.** The primary consumer of `selectResultSets()` is a
  stored procedure, and **a MySQL `CALL` cannot return usable rows today at all** — a prepared `CALL`
  reports ZERO result columns even when the procedure emits a result set at run time. Verified in
  code (`ferro-backend-mysql/src/stream.rs`), and the two paths round the same blind spot off
  differently: the STREAMED path takes the no-rows arm and discards the rows, while the BUFFERED path
  maps every row through the empty prepared-column list and yields **N rows with no cells**. So
  carrying multiple result sets on the wire would be building on sand: the feature would still return
  nothing usable on the backend that motivates it.
  **The dependency order is therefore CALL-blind-spot FIRST, multi-result-set SECOND**, and both are
  engine work, not tier work. Recorded so the tier is not blocked on a `/proto` slice that would not
  have helped. The blind-spot fix is its own investigation (likely: read column metadata from the
  EXECUTE response rather than the PREPARE response, or route `CALL` over the text protocol) and is
  not scheduled here.
- **C1b — package skeleton + service provider + one connection class, `select()` only.** The
  smallest thing that can execute a real query through a real Illuminate connection. Its exit gate
  is the S8b lesson: a HARD CONTACT ASSERTION (`getNativeConnection() instanceof …` + a round-tripped
  `SELECT 1`) before a single suite test runs. Upstream's `TestUtil` silently fell back to SQLite
  and reported a green 105-test run with zero Ferro contact; that must not be re-learned.
- **C1c — writes + transactions. DONE.** `statement()`/`affectingStatement()`/`unprepared()` over
  `fetch:none`; transactions over a minimal `FerroPdoShim`. The exit gate landed as specified and the
  requirement was **mutation-proven live**: swapping `FerroQueryException` to the sibling Doctrine
  tier's errno convention makes a real PG `40001` propagate OUT of `transaction(attempts: 3)` instead
  of retrying — `attempts:` silently inert, exactly as predicted.
  **One design change fell out of building it, and it moves a later slice earlier:** the PDO shim is
  NOT merely a compatibility layer for ecosystem packages, as §15 frames it. `ManagesTransactions` —
  which owns the transaction counter, savepoint naming through the stock grammar, the connection
  events and the `attempts:` retry loop — is written entirely against
  `getPdo()->beginTransaction()/commit()/rollBack()/inTransaction()/exec()`. Supplying those five
  methods inherits all of that unchanged; the alternative was copying the trait's body and keeping it
  in step with Laravel forever. It is possible because `Connection::getPdo()` has NO return type, so
  the shim is duck-typed and need not extend `\PDO`. **C1e is therefore already half-built**, and what
  remains of it is the question of which further PDO methods any real package needs — with `quote()`
  still refused (see the table above).
- **C1d — `cursor()`/`LazyCollection`**, which should be small given B2.
- **C1e — the PDO shim**, scoped by what C1b–C1d actually turn out to need, not by §15's list
  up front.
- **C2 — the `illuminate/database` suite**, modelled on `testkit/dbal-suite.sh`.

## The bar, stated honestly up front

§15's acceptance is "the `illuminate/database` integration test suite green on MySQL, PG, SQLite via
Ferro connections", plus a demo app. **SQLite has no backend (C3), so the SQLite column is
unreachable until C3 lands** — exactly the situation §14's bar was in, and the honest recording rule
from the S8b close applies: report the columns that ran, name the one that could not, and do not
restate the bar as if it were met. Whether C3 should therefore come BEFORE C2 is an open sequencing
question, not a settled one.
