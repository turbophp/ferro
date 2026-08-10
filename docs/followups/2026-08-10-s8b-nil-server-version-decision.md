# Follow-up (DECISION REQUIRED before M1-S8b): what a DBAL driver does with `server_version: nil`

**Found:** M1-S8a Task 12 declared it as a carry; the S8a whole-branch review found the *consumer*
half recorded nowhere durable (finding F19b).
**Belongs to:** M1-S8b (the Doctrine DBAL-4 driver tier). Nothing in `php/client/src` reads
`serverVersion` yet, so there is no shipped behaviour to change — only a decision to make before
one exists.
**Severity:** low as an engine matter, **high as a driver matter**: the wrong default is a silently
wrong SQL dialect rather than a clean error.

## The engine-side mechanism (already true, already documented)

`HELLO_ACK` advertises `server_version` per pool. It is `nil` whenever the engine does not currently
trust a value. `proto/PROTOCOL.md` §4 records that `nil` may mean "never learned", "learned then
expired", or "the probe did not answer inside the handshake budget", and that a client must treat it
as **unknown**, never as "old" or "absent".

Three reachable ways a **previously known** version becomes `nil` again:

1. **TTL expiry with a slow re-probe.** `VERSION_TTL` is 10 min. `PoolEntry::cached_version` refuses
   a `Known` entry past its TTL *by design* — a stale string is worse than none — so the handshake
   between "expired" and "re-probed" reports `nil`. Guarded by
   `pools::tests::an_expired_version_is_advertised_as_unknown_not_as_a_stale_string`.
2. **A transient failure after expiry.** `ProbeGuard` overwrites the state unconditionally, so once
   the TTL has lapsed a failed or budget-overrunning re-probe replaces `Known` with `Failed` and the
   version stays `nil` for one `VERSION_RETRY_BACKOFF` (5 s) window.
3. **A backend that is simply down at connect time.** By design: the handshake never depends on
   backend availability.

None of these are bugs. `nil` is a normal, recurring value on a healthy system — that is the point
of this ticket.

## The decision S8b must make

A Doctrine DBAL driver selects a **platform** from the server version string — MySQL vs MariaDB, and
which major-version branch — and the platform decides which SQL grammar DBAL emits. So on `nil` the
driver must choose one of:

- **(a) Fail the connection loudly.** Safe, but it converts a routine 5-second window (case 2, or a
  handshake that raced a rolling upgrade) into a hard outage for every PHP worker that reconnects
  during it. Given `boot_epoch` reconnect storms (SPEC §19.1), that is a real availability cost.
- **(b) Fall back to a default platform.** Never errors, and is **the dangerous one**: a MariaDB pool
  silently served by the MySQL platform emits a different dialect. Failures land far from the cause.
- **(c) Defer.** Return a driver connection whose `getServerVersion()` is only resolved on first use,
  and re-read `HELLO_ACK`'s pool metadata (or a dedicated request) at that point. Costs a round trip
  on the first query after a `nil` handshake and needs a decision about what happens if it is still
  `nil` then.
- **(d) Make it not happen.** Have `HELLO_ACK` carry the last-known version with an explicit
  `stale: true`, letting the driver pick a platform from a string it knows is old. This contradicts
  the current engine rule ("never advertise a version we no longer trust") and would need a
  `/proto` change plus a §22.2 entry — do not do it as a side effect of the driver work.

The **kind** field is never `nil` (it is inferred from the DSN scheme, so it is known even for a
backend that has never been dialled), so a driver always knows the FAMILY. What `nil` costs it is
only the version *within* that family — which is exactly the MySQL-vs-MariaDB and
major-version-branch distinction.

## Recommendation (not binding — this is the open item)

(c) then (a): defer, and if the version is still unknown when it is actually needed, fail loudly.
Never (b). Whatever is chosen must be written into SPEC §14 and asserted by a live test that
handshakes against a pool whose backend is down.
