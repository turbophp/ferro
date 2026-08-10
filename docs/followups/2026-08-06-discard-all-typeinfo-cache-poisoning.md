# Follow-up: `DISCARD ALL` poisons tokio-postgres's typeinfo statement cache

**Status: RESOLVED 2026-08-10** (M1-S8a whole-branch review, finding F1). Kept as the standing
record of the defect, the two false claims it carried, and the one residual window the fix does not
close. See "Resolution" at the bottom.

**Found:** M1-S7 Task 9 (live acceptance), 2026-08-06. Independently reproduced and re-characterised
by the S7 acceptance review, then re-characterised again — as **routine, not rare** — by the S8a
whole-branch review.
**Belongs to:** M1-S3 (conditional hygiene at checkout) — **not an S7 regression.**
**Severity:** was medium (bounded blast radius, safety intact, permanent for the affected
connection); the S8a bind surface raised it to **high** before it was fixed, see "What changed in
S8a".

## What happens

The **full** hygiene profile runs `DISCARD ALL` on a tainted connection at checkout. `DISCARD ALL`
includes `DEALLOCATE ALL`, which destroys the prepared statements `tokio-postgres` caches internally
for **typeinfo lookups** — the `pg_type` queries it issues to resolve an OID it does not know
natively.

The driver keeps its own handles to those statements and does not learn they are gone. The next
typeinfo lookup on that connection fails:

```
SQLSTATE 26000 — prepared statement "s8" does not exist
```

## Reproduction (deterministic)

The trigger needs **two distinct custom OIDs** across a taint:

1. Create two custom types with distinct OIDs (e.g. two enums, or an enum and a composite — or,
   since S8a, two DOMAINs used as PARAMETERS).
2. Query/bind the first — the driver performs a typeinfo lookup and caches the statement.
3. Taint the connection (anything that selects the full hygiene profile) and return it to the pool.
4. Check the connection out again and query/bind the **second** custom type.
5. → `26000`.

One thing masks it if you get the setup wrong, which is why it took a precise repro:

- **Repeating the *same* OID does not trigger it** — `tokio-postgres` short-circuits on its
  OID→`Type` cache and never re-issues the typeinfo query.

## Corrected characterisation

Task 9 initially described this as "order-dependent". It is worse than that: it is **permanent
connection poisoning**.

- Rounds 3, 4, … also fail.
- A custom type created *after* the poisoning also fails.
- It fails **with no further tainting** — the connection sits in the pool broken indefinitely.

## What changed in S8a (and the two sentences here that became FALSE)

The S8a whole-branch review found this ticket had two claims that its own slice had invalidated:

1. > "**A domain over a builtin base does not trigger it** — PG resolves a domain to its base type
   > in the `RowDescription`, so no typeinfo lookup happens at all."

   True of the **read** side only. PG does **not** resolve a domain in `Statement::params()`, which
   reports a parameter's own domain OID verbatim — the very asymmetry S8a's `bind::resolve_domain`
   exists for. So a DOMAIN-typed **parameter** forces a typeinfo lookup at prepare, and since S8a
   made such a bind SUPPORTED (it was `Unsupported` before), an **ordinary supported write** now
   re-arms the poisoning.

2. > "**Every type M1-S7 added is safe.** … no shipped type is affected, all 14 map to builtin OIDs"

   Still true of the type *tags*, but no longer true of the *bind surface*: the shipped surface now
   includes a domain over any of those builtin bases.

### Where the lookup actually fires (measured, not assumed)

The S8a review's stated justification was that `information_schema` introspection would hit this
constantly. That is only half right, and the half that is wrong matters for anyone reasoning about
the blast radius, so it was measured on the testkit's PG 17 via
`pg_prepared_statements.parameter_types`:

| statement                              | reported param type |
| -------------------------------------- | ------------------- |
| `INSERT INTO t (domcol) VALUES ($1)`   | the **DOMAIN**      |
| `UPDATE t SET domcol = $1`             | the **DOMAIN**      |
| `SELECT $1::domain`                    | the **DOMAIN**      |
| `SELECT ... WHERE domcol = $1`         | the **base** type   |

So the trigger is a **write into a domain-typed column** (or an explicit cast), not a comparison —
and *reading* `information_schema` triggers nothing at all, since PG resolves domains to their base
in `RowDescription`. `information_schema` does define five distinct domains (`cardinal_number` int4,
`character_data` varchar, `sql_identifier` name, `time_stamp` timestamptz, `yes_or_no` varchar), but
an introspection query's `WHERE table_name = $1` param is inferred as plain `name` — measured — so
it neither triggers this nor, as it happens, binds at all today (Ferro's `TEXT` bind accepts
`text`/`varchar`/`bpchar`, not `name` — a separate S8b item).

What does make this routine is the ordinary case: two writes into two DIFFERENT domain columns
across one connection recycle. That is normal ORM traffic for any schema using domains, and S8a is
exactly the slice that made it supported instead of refused.

## What was NOT affected

- **Safety properties held throughout.** The failure is `NonRetryable` — never `Indeterminate`,
  never `ConnectionLost`. No silent miscast, no double-apply, no cross-tenant data leak.

## Why it mattered

Charter rule 6's contract is a *loud, diagnosable* failure. An operator hitting an unsupported type
should get `unsupported type for column "x": PG type foo (OID nnnnn)` — the message M1-S7 Task 4b
deliberately improved. Instead they got a bare `26000` naming an internal statement handle, on a
connection that was now permanently broken. The diagnosis pointed at the wrong layer entirely.

## Fix directions (as originally listed, with the verdicts)

1. **Reset the driver's statement cache alongside `DISCARD ALL`.** ← **CHOSEN.** Turned out to be a
   ~10-line, purely additive accessor on the vendored fork.
2. **Narrow the profile** (omit `DEALLOCATE ALL`). **Rejected**: trades a cache-coherence bug for a
   §7.4 leak class, on exactly the connections already known to be dirty. The full profile exists
   precisely because the targeted one was proven insufficient for tainted connections.
3. **Retire the connection instead of recycling it.** **Rejected**: a reconnect on a hot path
   (charter rule 5), *and* it would not have covered the user-issued-`DISCARD ALL` arm below, which
   recycles NON-tainted through the `Targeted` profile.

## Resolution (2026-08-10)

- **Fork:** `vendor/tokio-postgres/src/client.rs` gained
  `Client::clear_typeinfo_statement_cache()` (and its `InnerClient` counterpart), which drops the
  three cached typeinfo `Statement` handles so the next lookup re-prepares. It deliberately leaves
  the `types` oid→`Type` map alone — `DEALLOCATE ALL` destroys statements, not type definitions.
  Documented in `/UPSTREAM_PR.md`'s M1-S8a addendum as a third, independently upstreamable change.
- **Pool:** `ferro-backend-pg/src/conn.rs`'s `PoolBackend::reset` calls it **before** the reset
  batch, on **both** profiles.
  - `Full` because `DISCARD ALL` ⇒ `DEALLOCATE ALL` is the pool-caused poisoning.
  - `Targeted` because the pool is not the only party that deallocates: `ferro-classify` safe-lists
    a **user-issued** `DISCARD ALL` by design (`RESET`/`DISCARD` move session state toward
    default), so a connection whose own SQL ran one is recycled NON-tainted through `Targeted`
    carrying the same dead handles. This second arm was not in the original ticket; it was found
    while fixing the first and is equally permanent. (A user `DEALLOCATE ALL` *does* taint —
    `PinTrigger::Prepare` — so it lands on `Full`; and `DISCARD PLANS` deallocates nothing at all,
    verified on PG 17: it drops cached plans and leaves the statements.)
  - *Before* the batch because each dropped handle sends its usual `Close`, which is a real
    deallocation while the statement exists and an accepted no-op once it does not, so the driver can
    never be left holding a handle to a statement the server dropped — not even if the batch fails
    partway.
- **Cost:** one re-prepare of the typeinfo statement on the first custom OID a checkout resolves that
  the connection's oid→`Type` map has not already cached. Nothing for the 14 shipped tags, which are
  all builtin and never look anything up.

### Acceptance (all met)

- Two distinct custom OIDs across a taint, on a recycled connection: `pg_types_it.rs`
  `s8a_f1_distinct_domain_oids_survive_a_full_hygiene_reset` — RED with `26000` before the fix,
  green after.
- Rounds 3+ with no further taint (the "permanent" characterisation): round 3 of the same test.
- The user-issued-`DISCARD ALL` / `Targeted` arm: `s8a_f1_distinct_domain_oids_survive_a_user_issued_discard_all`
  — RED under a mutation that narrows the fix to the `Full` profile only, which is what proves the
  second arm is load-bearing rather than defensive.
- The §7.4 leak classes the full profile closes are all still closed: `pg_pool_it.rs`'s M1-S3
  hygiene block re-run green (the profiles' SQL is unchanged; only the driver-side cache is
  invalidated).
- An unsupported custom type still produces the loud `Unsupported` naming the column and its native
  type, not `26000`: `deferred_column_types_are_refused_before_execution` and the domain-over-`timetz`
  clause of `s8a_narrowing_and_domain_binds_round_trip`.

### RESIDUAL — one window remains open (deliberate)

A **user statement that deallocates** (`DISCARD ALL` or `DEALLOCATE ALL`) followed by
a custom-OID lookup **within the same checkout** still poisons that checkout: there is no reset
between the two. Closing it needs a per-statement hook (e.g. routing `PinTrigger::Prepare` through a
new `PoolBackend` callback) rather than a reset-time one — real plumbing across the generic pool for
a self-inflicted, single-request window. Under `ferrod` an autocommit request takes a fresh checkout
per statement, so the window is only reachable inside one explicit transaction that first nukes its
own prepared statements. Recorded here and in `conn.rs`'s `reset` docblock rather than fixed.
