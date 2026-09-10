# Follow-up: the backend DIAL is unbounded — `checkout_timeout` does not cover it

> **STATUS: FIXED (M1 Phase B, B5) for the two dials that hold something.** `Pool::checkout` now
> bounds `backend.connect()` with `checkout_timeout` (fix direction 1, reusing the existing knob —
> see the decision below), and both backends' out-of-band `Cancel::cancel()` bound their SIDE-connection
> dial with a fixed `CANCEL_DIAL_BUDGET` (2 s). **Still open:** fix direction 3 (TCP keepalive on
> established connections), which is a different hazard — a connection going silent AFTER it is up,
> which no dial bound can reach.

**Found:** M1-S8a Task 12 (the server-version probe) self-declared it as a carry; the S8a
whole-branch review verified it against the code and promoted it out of the task report.
**Belongs to:** M0 / M1-S3 (`ferro-pool`'s checkout path) — **not an S8a regression.** S8a is only
where it became visible, because the probe is the first caller whose *whole point* is to survive an
unreachable backend.
**Severity:** medium. No correctness or safety property is at risk; the cost is a wedged wait and a
held pool permit.
**Blocks:** nothing today. Partially mitigated for the probe path only (see below).

## What happens

`Pool::checkout` bounds the **semaphore acquire** and nothing else:

```rust
// engine/crates/ferro-pool/src/pool.rs
let acquire = Arc::clone(&self.inner.semaphore).acquire_owned();
let permit = match tokio::time::timeout(self.inner.config.checkout_timeout, acquire).await { .. };
// ...
return match self.inner.backend.connect().await { .. };   // <- NOT under any timeout
```

The pool's own comment says so explicitly (`pool.rs`, the BOUNDED-recycle block: *"the
`checkout_timeout` at the top of `checkout()` wraps ONLY the permit acquire, not this
pop/rollback/reset loop"*) — and the same is true one step earlier, of the dial.

`PgBackend::connect` then calls `tokio_postgres::connect(&self.url, NoTls)` with **no connect
timeout**, and TCP keepalive is off. Against a host that neither answers nor resets — the black-holed
address, a dropped-packet firewall rule, a wedged hypervisor — the dial runs to the **OS TCP
connect timeout, measured at ~127 s on this host**. `MysqlBackend::connect` has the same shape.

`tokio_postgres::CancelToken::cancel_query` (the out-of-band CANCEL, `ferro-backend-pg/src/conn.rs`)
dials a **fresh** connection and is unbounded for the same reason.

## Why it matters

For the duration of that dial the caller holds one of the pool's `max_size` (16) permits. Sixteen
such callers exhaust the pool for two minutes with nothing running on any connection. Every path
that checks out is affected: `run_autocommit_exec`, the TX actor, the stream producer, the version
probe.

## What IS bounded today (and what is not)

`ferrod`'s version probe bounds its own dial at `VERSION_CHECKOUT_BUDGET` (5 s), its cancel at
`VERSION_CANCEL_BUDGET` (2 s) and its post-cancel drain at `VERSION_DRAIN_BUDGET` (5 s) — see
`engine/crates/ferrod/src/pools.rs` and the live guard
`pools::tests::a_black_holed_dial_is_bounded_so_the_pool_stays_probeable`. That was necessary
because a probe that never returns seals the pool permanently un-probeable (S8a review F17), and it
means the probe no longer holds a permit for ~127 s.

**It is a bandage on one caller.** The root cause is untouched: every other checkout in the engine
still dials unbounded, and each one bounding itself is duplicated policy in the wrong layer.

## Fix directions (not yet chosen)

1. **Bound the dial inside `Pool::checkout`,** i.e. wrap `backend.connect()` in a
   `config.connect_timeout` (new) or reuse `checkout_timeout` for the whole body rather than the
   acquire alone. One place, every caller fixed, and the per-caller budgets above become redundant.
   Needs a decision on whether `checkout_timeout` should mean "time to a usable connection" (it
   currently does not, despite the name).
2. **Bound it in each backend's `connect`.** `tokio_postgres` takes `connect_timeout` in the
   connection string, and `mysql_async`'s `OptsBuilder` has `tcp_connect_timeout` — so this can be
   done with no fork change, at the cost of it being a per-backend detail rather than a pool policy.
3. **Enable TCP keepalive** on pooled connections. Orthogonal (it bounds an established connection
   going silent, not the dial) but the same class of hazard and worth deciding at the same time.

## Where it is recorded

Only here, and in the doc comment on `VERSION_CHECKOUT_BUDGET`. It was previously recorded ONLY in
the Task 12 report — not in SPEC §22.2, not in `proto/PROTOCOL.md` — which is what the S8a review
flagged (finding F19a).

## What B5 actually changed, and the decision it had to make

**The choice was fix direction 1, with `checkout_timeout` reused rather than a new
`connect_timeout`.** The open question this document left — *"needs a decision on whether
`checkout_timeout` should mean 'time to a usable connection' (it currently does not, despite the
name)"* — resolved by reading what the knob already means in the shipped code rather than by
picking a new meaning. The BOUNDED-recycle block, ten lines below the dial, **already** reuses
`checkout_timeout` as a per-conn cleanup bound. So `checkout_timeout` has never been a total budget:
a checkout could always spend it on the acquire and then again on each conn it recycled. Bounding
the dial with the same value is consistent with that; inventing a second knob would have implied the
first one was a total, which it is not. This is recorded as a per-step bound in the code comment so
the next reader does not have to re-derive it.

**A timed-out dial is `PoolError::Timeout`, not `ConnectionLost`.** Both are Retryable and neither
ever sent a user statement (`sent = false`, so there is no `Indeterminate` question either way), but
the split is what lets an operator distinguish "the backend refused us" from "the backend never
answered at all" — the entire signature of the black-hole case.

**The cancel dial was worse than the checkout dial, which is why it is in scope.** `ferrod` awaits
`cancel_handle.cancel().await` INLINE on the timeout/cancel arms of `run_autocommit_exec`, before it
re-awaits the query future. An unbounded cancel therefore stalled the very teardown the deadline was
supposed to start. A fixed budget is right there rather than a config knob because a cancel is
best-effort *by contract*: its timeout and its failure have identical handling (log at debug, move
on), the actor tears the transaction down regardless, and no statement is ever re-run (charter rule 3).

**The guard asserts the released permit, not the elapsed time.** A `checkout()` that returned
`Timeout` while leaking its permit would pass a naive "did it come back?" check and still leave the
pool one slot short forever — which is the harm this document records. `b5_a_black_holed_dial_is_bounded_and_releases_its_permit`
therefore drives a `max_size: 1` pool and requires a SECOND checkout to succeed. A companion test
pins the success path, so the bound cannot silently become a ban. Both are mutation-proven (widen the
bound to an hour and the first goes red).

**The per-caller bandage is now redundant but was left in place.** `ferrod`'s
`VERSION_CHECKOUT_BUDGET` still wraps the probe's checkout; it is tighter than the pool's bound in
the shipped tuning, so it still fires first and its live guard still passes. Removing it is a
separate change and is not smuggled in here.
