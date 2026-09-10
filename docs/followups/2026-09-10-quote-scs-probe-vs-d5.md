# Follow-up: `quote()`'s `standard_conforming_strings` probe is a round trip, and D5 says there is none

**Found:** M2-C2f, by me, while scoping C3 and reading SPEC §21 for an unrelated reason.
**Severity:** low as a defect (the shipped behaviour is safe and correct), **high as a process
matter** — charter ground rule 1 makes §21 decisions binding and says an impossible one is to be
RAISED, not routed around. This one is not impossible; it is satisfiable, and I did not check §21
before implementing.

## The decision

> **D5** — `quote()` implemented client-side with per-platform tables; **no engine round trip**.
> *Rationale: A network hop inside a discouraged compat API is absurd; tables are small and testable.*

## What shipped

`FerroPdoShim::quote()` (§22.2 (as)) is client-side and its rule *is* a per-platform table in effect
— double `'`, wrap. But before using it, it verifies `standard_conforming_strings` with **one
`SHOW`**, cached for the connection's lifetime.

**The verification is not decorative.** The doubling rule is complete only while that GUC is `on`;
with it `off`, PostgreSQL reads `\` as an escape and a value ending in a backslash can consume the
closing quote. Assuming it would put an unverified premise on a security-relevant path.

**And it is not what D5's rationale objects to.** D5 objects to "a network hop inside a discouraged
compat API" — a per-call hop. This is one per connection, amortised to nothing across the N bindings
`Grammar::substituteBindingsIntoRawSql()` escapes. But D5's text says *no engine round trip*, and
that is the binding part; the rationale explains it, it does not narrow it.

## The fix, which is strictly better than either alternative

**`standard_conforming_strings` is a `GUC_REPORT` parameter**, so PostgreSQL sends it in the startup
`ParameterStatus` stream and again whenever it changes. The M1-S1 vendored fork ALREADY mirrors
every reported parameter and exposes it synchronously — `Client::parameter(name) -> Option<String>`,
zero round trips (VERIFIED: `vendor/tokio-postgres/src/client.rs:277`, and
`pg_parameter_status_it.rs` is its live test). The engine therefore already holds this value.

So: advertise it in `HELLO_ACK` pool metadata alongside `name`/`kind`/`server_version`, and have the
shim read it from `poolInfo()`. That is:

- **D5-compliant** — no round trip at all, not even one.
- **More correct than the `SHOW`**, not merely cheaper: `ParameterStatus` tracks the value LIVE,
  where a cached `SHOW` is a snapshot of one checkout.
- A `/proto` change, so charter rule 2 applies: registry + golden vectors + both codecs in one
  change set. That is the only reason it was not done in C2f — it is a slice, not a tweak.

## Options considered and rejected

- **Drop the probe, document the assumption.** D5-compliant and the cheapest edit, but it puts an
  unverified premise under an escaping function. Rejected: the whole reason the probe exists is that
  "PostgreSQL's default since 9.1" is not a basis for SQL escaping.
- **Hold the C2f change until the `/proto` slice lands.** Rejected in favour of this project's own
  established pattern — ship the measured behaviour, record the gap, follow up — but that is a
  judgement call, and the reason it is written down here rather than left in a commit message is
  that the alternative was defensible.

## Status

**OPEN.** The shipped behaviour is safe; what is outstanding is the D5 conflict, and the resolution
above is a designed slice, not a sketch. It should be taken before any further `quote()`-adjacent
work, and SPEC §21 D5 should gain a pointer to it either way so the next reader does not have to
rediscover the conflict.
