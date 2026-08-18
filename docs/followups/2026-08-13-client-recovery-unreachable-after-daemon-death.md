# Follow-up: after a `ferrod` death the `php/client` `Connection` cannot recover — two mechanisms, both outside the fate taxonomy

**Found:** M1-S9 Task 3, on the FIRST live run of the new §20.3 kill-`ferrod` chaos harness
(`php/client/tests/Live/DaemonKillFateLiveTest.php`). Both were then reproduced INDEPENDENTLY of
the harness, in ~20 lines of ordinary caller code against PG 17.
**Belongs to:** `php/client` — `Client/ReconnectLoop.php`, `Client/Transport.php`,
`Client/Session.php`, `Client/Connection.php`.
**Severity:** HIGH. Each one leaves a `Connection` PERMANENTLY unusable after an ordinary `ferrod`
restart or crash, and each surfaces OUTSIDE the §9.2/§19.3 taxonomy — so a caller's
`catch (FerroException)` never sees it, and under PHP-FPM finding A is an uncaught `TypeError`
(a fatal → 500).
**NOT fixed here, deliberately:** M1-S9 Task 3 changes no `php/client/src` (the plan's own rule:
"if a cell fails in a way that looks like a client defect, STOP, journal the evidence, and raise
it"). Both are PINNED by tests so neither can be closed or widened silently.
**Blocks:** §19.2's transparent-recovery promise for the only failure shape a real restart
produces — the client noticing while the daemon is DOWN.

## Finding A — reconnect EXHAUSTION leaves a closed session in place; the next call raises `TypeError`

`ReconnectLoop::reconnect()` closes `$this->session` first (best-effort, `ReconnectLoop.php:82-87`),
dials `maxAttempts` times, and on exhaustion **rethrows the raw last dial error without replacing
the session** (`:108`). The loop is left holding a CLOSED session. Every later call reaches
`Transport::writeAll` → `@fwrite($closedResource, …)`; `@` suppresses warnings but **not** PHP 8's
`TypeError` for a closed resource. `TypeError` is outside the taxonomy, so
`Connection::dispatchAutocommit`'s `catch (ConnectionLostException | TransportException)` cannot act
on it and the reconnect loop is never re-entered.

Budget arithmetic makes this the COMMON case, not a corner: `RetryPolicy::default()` is
`maxAttempts = 3` with a 0.05 s base, and a dial at a dead UDS fails instantly (ECONNREFUSED) — so
exhaustion completes in **under ~0.35 s**, far less than any real `systemctl restart ferrod`.

Measured, standalone (one ferrod killed, another relaunched by a shell driver):

```
int(1)
-- killing ferrod --
kill-phase:   TransportException: connect failed to unix:///tmp/ferro-repro-HAV5.sock: Connection refused (errno 111)
-- ferrod restarted; sleeping 3s --
post-restart: TypeError: fwrite(): supplied resource is not a valid stream resource
```

## Finding B — a mid-stream wire failure poisons the `Session` forever

`Session::$streamOpen` is cleared only by a `request_id=0` session-fatal terminal (`Session.php:300`),
by a normal END (`:313`), or by a drain that reaches one. A `readFrame()`/`sendWindowUpdate()` that
THROWS clears nothing, and `Connection::stream()`'s `finally` correctly declines to run the release
over a wire it already knows is broken. The session therefore still believes a stream is open, and
every later `sendRequest` hits `assertNoOpenStream()` → `ProtocolException` instructing the caller
to "drive it to its terminal" — which is impossible, the socket is dead. `ProtocolException` is
again outside the reconnect-triggering set.

Measured, standalone (SIGKILL at row 5 of a 200 000-row stream):

```
int(1)
-- killing ferrod MID-STREAM at row 5 --
stream-phase after 2048 rows: TransportException: write failed after 0 of 23 bytes
-- ferrod restarted; sleeping 3s --
post-restart: ProtocolException: a stream (request_id=2) is open on this session; drive it to its
              terminal or call abandonStream() before sending another request
```

## Why neither was caught before — defect species (c), an unobservable vantage point

`RestartLiveTest` is the tree's only §19.1/§19.2 live proof, and it calls `restartFerrod()` **before**
issuing the read — so the daemon is already UP, the reconnect succeeds on attempt 1, and exhaustion
is never reached. The event a real crash produces (the client notices while the daemon is down) did
not exist anywhere in the repository until this harness created it. Every fate test in the tree
watches engine-side classification with the daemon ALIVE.

## Where they are pinned

`php/client/tests/Live/DaemonKillFateLiveTest.php`:
- `testReconnectExhaustionLeavesTheConnectionPermanentlyDeadPinnedDefect` (finding A)
- `testMidStreamDaemonDeathPoisonsTheSessionPinnedDefect` (finding B)

Each asserts the MEASURED broken behaviour and carries a failure message reading "if this now
RECOVERS, the client was fixed: delete this pin and restore the §19.2 recovery assertion in
<the cell>". So the fix cannot land silently, and a further regression cannot either.

## Shape of a fix (sketch only — not decided here)

- A: `ReconnectLoop::reconnect()` should not leave a closed session readable. Either keep the dead
  session and let `Transport` report a typed `TransportException` for a closed socket (an
  `is_resource` guard in `readExact`/`writeAll` is the smallest change and fixes the taxonomy
  violation on its own), or mark the loop "needs redial" so the next call retries the dial.
  **The taxonomy half should be fixed regardless of the recovery decision:** no client path should
  ever raise a raw `TypeError`.
- B: a wire failure must forget the stream. `Connection::stream()`'s `finally` already knows
  (`$wireFailed`); it needs a local, non-wire `Session::forgetStream($rid)` to call.
- Both want a live guard built the way this harness builds them (the kill proven to PRECEDE the
  classification), not a unit test over a fake transport.
