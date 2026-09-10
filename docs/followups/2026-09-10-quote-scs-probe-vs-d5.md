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

### The design, settled by reading rather than left as "advertise it somehow"

**The field should NOT be named for PostgreSQL's GUC.** What a client needs is one bit — *is a
backslash inside a single-quoted literal an ordinary character?* — and both families have it:
PostgreSQL's `standard_conforming_strings`, and MySQL's `NO_BACKSLASH_ESCAPES` in `sql_mode`. A
family-independent `literals_are_standard: bool | nil` carries exactly that and keeps a PostgreSQL
GUC name out of the protocol. **MySQL needs it MORE than PostgreSQL does**, which is the argument
against deferring the shape: PostgreSQL defaults to the safe value, MySQL defaults to the unsafe one,
so a MySQL `quote()` cannot be written at all without this bit.

**`nil` is a legitimate steady state**, by the same argument `server_version`'s own contract makes:
the handshake never depends on a backend being reachable, so an unlearned value rides as `nil` and
the client must treat it as "unknown". Fail-closed is already what the shim does — anything that is
not an unambiguous yes refuses.

**It needs no new engine machinery.** `PoolInfo` is per-POOL and `Client::parameter()` is
per-CONNECTION, but the lazy, concurrent, TTL'd version probe added at M1-S8a Task 12 already has
exactly that shape, so the new value rides the probe that already runs.

### The cost this document originally understated

**`PoolInfo` is a positional fixarray of 3** (`/proto/PROTOCOL.md` §4), and both decoders reject any
other arity — the PHP one throws on `count($w) !== 3`. A fourth element is therefore a BREAKING wire
change: it needs `protocol_version` 2 → 3, which makes a skewed engine/client pair fail at the first
byte of the first frame. That is exactly what M1-S8a did when it introduced this metadata, and its
comment in `methods.toml` explains why the bump is the honest mechanism ("the lock file carries no
message shapes, so nothing else would move"). Pre-v1 that is acceptable, but it is a deployment
consequence rather than a code detail, and the first version of this document said only "a `/proto`
change", which reads cheaper than it is.

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
