# Follow-up: `DISCARD ALL` poisons tokio-postgres's typeinfo statement cache

**Found:** M1-S7 Task 9 (live acceptance), 2026-08-06. Independently reproduced and re-characterised
by the S7 acceptance review.
**Belongs to:** M1-S3 (conditional hygiene at checkout) — **not an S7 regression.**
**Severity:** medium. Bounded blast radius, safety intact, but permanent for the affected connection.
**Blocks:** nothing today. **Will ambush M1-S8** (the Doctrine DBAL-4 driver), which exercises
`information_schema` and custom types constantly.

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

1. Create two custom types with distinct OIDs (e.g. two enums, or an enum and a composite).
2. Query the first — the driver performs a typeinfo lookup and caches the statement.
3. Taint the connection (anything that selects the full hygiene profile) and return it to the pool.
4. Check the connection out again and query the **second** custom type.
5. → `26000`.

Two things mask it if you get the setup wrong, which is why it took a precise repro:

- **Repeating the *same* OID does not trigger it** — `tokio-postgres` short-circuits on its
  OID→`Type` cache and never re-issues the typeinfo query.
- **A domain over a builtin base does not trigger it** — PG resolves a domain to its base type in
  the `RowDescription`, so no typeinfo lookup happens at all.

## Corrected characterisation

Task 9 initially described this as "order-dependent". It is worse than that: it is **permanent
connection poisoning**.

- Rounds 3, 4, … also fail.
- A custom type created *after* the poisoning also fails.
- It fails **with no further tainting** — the connection sits in the pool broken indefinitely.

## What is NOT affected

- **Every type M1-S7 added is safe.** All eight canonical tags map to builtin OIDs
  (`numeric`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`), and builtin OIDs
  never perform a typeinfo lookup.
- **Safety properties hold.** The failure is `NonRetryable` — never `Indeterminate`, never
  `ConnectionLost`. No silent miscast, no double-apply, no cross-tenant data leak.

## Why it still matters

Charter rule 6's contract is a *loud, diagnosable* failure. An operator hitting an unsupported type
should get `unsupported type for column "x": PG type foo (OID nnnnn)` — the message M1-S7 Task 4b
deliberately improved. Instead they get a bare `26000` naming an internal statement handle, on a
connection that is now permanently broken. The diagnosis points at the wrong layer entirely.

And S8's DBAL suite will hit this: `information_schema` columns are domains, and enum/composite
support is exactly the surface a real ORM exercises.

## Fix directions (not yet chosen)

1. **Reset the driver's statement cache alongside `DISCARD ALL`.** The hygiene profile and the
   driver's cache must agree; today the pool invalidates server state behind the driver's back.
   Likely needs a small addition to the vendored `tokio-postgres` fork (a `clear_type_cache()` /
   `clear_statement_cache()` entry point) — the fork already exists for the RFQ work.
2. **Narrow the profile:** use `DISCARD ALL EXCEPT PLANS`-style semantics, or compose the discard
   from its parts (`CLOSE ALL; RESET ALL; UNLISTEN *; …`) omitting `DEALLOCATE ALL`. Cheaper, but
   must be checked against every leak class §7.4 lists — the full profile exists precisely because
   the targeted one was proven insufficient for tainted connections.
3. **Retire the connection instead of recycling it** when the full profile runs. Simplest and
   safest; costs a reconnect on every tainted checkout, which is a real throughput regression.

Option 1 is the most likely correct answer. Option 2 needs care: do not reopen a leak class to fix
a cache-coherence bug.

## Acceptance for the fix

- The reproduction above passes: two distinct custom OIDs across a taint, on a recycled connection.
- Rounds 3+ and a post-poisoning custom type also pass.
- The §7.4 leak classes the full profile closes are all still closed (re-run M1-S3's hygiene suite).
- An unsupported custom type still produces the loud `Unsupported` naming the column and its native
  type — not `26000`.
