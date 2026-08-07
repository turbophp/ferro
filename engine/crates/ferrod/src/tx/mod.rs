//! `TxRegistry` + the per-`tx_id` transaction actors (SPEC §4/§7; S6).
//!
//! A transaction pins a pooled connection to a `tx_id`, NOT to the client socket — the property
//! that makes Fiber-suspended / multiplexed requests correct. Each open transaction is a single
//! task (the [`actor`]) that OWNS a `Checkout` (the pinned conn) for the tx's whole life and
//! serializes [`TxCommand`]s onto it over an `mpsc` channel. The registry fronts each actor with a
//! [`TxHandle`]: who owns it, how to command it, how to abort it, and how to await its teardown.
//!
//! **Lifecycle (SPEC §7):**
//! ```text
//! BEGIN → checkout conn → Checkout::begin_tx_with(tx_id, begin_sql) → spawn actor (MOVES Checkout
//!         in) → register(tx_id) → reply tx_id
//! tx-scoped EXEC / SAVEPOINT / RELEASE / ROLLBACK_TO → lookup(tx_id, owner) → send a TxCommand +
//!         oneshot reply → actor runs it on the pinned conn → replies → handler declares outcome
//! COMMIT/ROLLBACK        → actor runs the tx-control, drops the Checkout (conn → pool), DEREGISTERs
//! deadline (idle/max)    → actor cancels any in-flight statement out-of-band, rolls back, drops the
//!                          Checkout, and TOMBSTONEs the tx_id as TxDeadline (a distinct state)
//! session death / drain  → abort_session fires each owned actor's abort token, awaits `done`
//! ```
//!
//! **Abort is a [`CancellationToken`], not a [`TxCommand`] (design note).** The actor's command
//! `mpsc` is NOT polled while a user statement is in flight (the interruptible-statement `select!`
//! has only `{ query_future, max_timer, abort_signal }` arms — polling `cmd_rx` there would consume
//! a *pipelined* command out of order and interleave it with the running statement). So a teardown
//! signal that must interrupt an in-flight statement (session death) cannot ride the command
//! channel; it is a dedicated `abort: CancellationToken` observed in BOTH the idle and the
//! interruptible `select!`. `TxCommand` therefore carries only the six real commands.

pub mod actor;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use ferro_proto::value::Value;

use crate::session::SessionId;
use crate::session::responder::Responder;

/// Process-global source of `tx_id`s. Monotonic and never reused, starting at 1 (0 is reserved as a
/// "no tx" sentinel on the wire). Contractually **bounded < 2^63** (SPEC §7 — a `tx_id` is a native
/// PHP int, NOT a full-range/`boot_epoch`-style u64); from 1 and incrementing, that bound is
/// unreachable in practice.
static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

/// Maximum number of deadline tombstones the registry retains at once (S6 hardening). A tombstone
/// lets a timed-out tx's OWNER see `TxDeadline` (retryable) rather than an opaque `Protocol` on its
/// next touch of the id. Tombstones are normally purged with the session ([`TxRegistry::abort_session`]),
/// but a single long-lived session that times out many transactions would otherwise accumulate them
/// unboundedly — so the registry keeps at most this many, evicting OLDEST-first. An evicted (very
/// old) tombstone degrades its owner's next lookup from `TxDeadline` → `Protocol`, which is
/// acceptable: the tx is long dead either way and `tx_id`s are never reused, so a stale id can never
/// be confused with a live transaction.
const TOMBSTONE_CAP: usize = 4096;

/// Draw the next `tx_id`: monotonic, distinct per call, never reused, bounded < 2^63 (see
/// [`NEXT_TX_ID`]). Called by the BEGIN handler immediately before spawning the actor.
pub fn next_tx_id() -> u64 {
    NEXT_TX_ID.fetch_add(1, Ordering::Relaxed)
}

/// A command sent to a per-`tx_id` actor over its `mpsc` channel. Each variant carries a `oneshot`
/// reply the actor sends back so the forwarding handler can declare the request's one terminal.
///
/// There is no `Abort` variant — abort is an out-of-band [`CancellationToken`] on [`TxHandle`]
/// (see this module's doc comment for why the command channel cannot carry it).
#[derive(Debug)]
pub enum TxCommand {
    /// A tx-scoped user statement. Runs via the GUARDED [`ferro_pool::pool::Checkout::query`] (a
    /// client sneaking a bare `BEGIN`/`SAVEPOINT` is still rejected). `fetch`/`readonly` are the
    /// forwarding HANDLER's concern (result shaping + the §19.3 fate classification, exactly as on
    /// the S5 autocommit path), so they stay handler-local and are NOT carried here — the actor
    /// needs only the statement and returns the raw result for the handler to map.
    ///
    /// **M1-S4 (Task 3):** `timeout_ms` and `cancel` enforce the same `ExecRequest.timeout_ms` +
    /// per-request CANCEL contract the S4 autocommit path enforces, but for a tx-scoped statement:
    /// the actor's `select!` races the query against BOTH, and on either firing rolls the whole
    /// transaction back + tombstones it (§19.3 — the safe uniform in-tx action on a client
    /// cancel/timeout is roll back, the client restarts), the SAME exit its own absolute `max_tx`
    /// deadline already uses.
    ///
    /// `cancel` is the per-REQUEST [`CancellationToken`] (the forwarding handler's own, from
    /// `session::registry`) — DISTINCT from [`TxHandle::abort`] (the session-level teardown signal
    /// `TxRegistry::abort_session` fires on session death/drain). Reusing `abort` here would be
    /// WRONG: firing it routes through `ExecStep::Abort`, which DROPS the reply with no fate at all
    /// (the forwarding handler then declares its own `Protocol`/`NonRetryable` guess) — the correct
    /// fate for a client CANCEL of an in-flight tx statement is a REPLIED `TxDeadline{Retryable}`,
    /// exactly like a deadline, not a silently dropped reply.
    Exec {
        sql: String,
        params: Vec<Value>,
        timeout_ms: Option<u32>,
        cancel: CancellationToken,
        reply: oneshot::Sender<ExecReply>,
    },
    /// A tx-scoped `fetch:stream` user statement (M1-S5 Task 5). Unlike [`TxCommand::Exec`] (which
    /// returns the whole result via an [`ExecReply`] the handler then frames), a streamed exec MOVES
    /// the request's [`Responder`] INTO the actor: the actor owns the pinned `co`, and the shared
    /// `services::sql::run_tx_streamed` producer emits HEAD/DATA off it and declares the ONE terminal
    /// through that moved `Responder` — the forwarding handler cannot, because `query_stream` borrows
    /// the actor's `&mut co`. The `done` ack tells the handler the producer has finished (so it may
    /// RETURN and let the supervisor deliver the already-declared terminal AFTER the last DATA, B4).
    ///
    /// `timeout_ms`/`cancel` carry the SAME S4 contract as [`TxCommand::Exec`]; the actor combines
    /// them with its own `abort` + `max_tx` into the single cancel/deadline the producer takes. A
    /// mid-stream cancel/timeout/max-deadline → the producer classifies `TxDeadline{Retryable}`
    /// (`in_tx: true`) and the actor rolls back + tombstones (the §19.3 uniform in-tx action); a
    /// session `abort` mid-stream takes the no-fate `TxEnd::Abort` teardown, exactly as the buffered
    /// path's `ExecStep::{Deadline,Abort}` split does. `readonly` feeds the producer's fate context.
    ExecStreamed {
        sql: String,
        params: Vec<Value>,
        timeout_ms: Option<u32>,
        readonly: bool,
        cancel: CancellationToken,
        responder: Responder,
        done: oneshot::Sender<()>,
    },
    /// Establish a savepoint. `name` is an optional client alias; the engine composes the ACTUAL
    /// savepoint name (`sp_N`) it runs on the wire (never a client string — no injection surface).
    Savepoint {
        name: Option<String>,
        reply: oneshot::Sender<CtlReply>,
    },
    /// Release a savepoint (destroying it and every one established after it). `name` resolves
    /// against client aliases + engine names; `None` → the most recent savepoint.
    Release {
        name: Option<String>,
        reply: oneshot::Sender<CtlReply>,
    },
    /// Roll back to a savepoint (destroying every one after it, keeping it). Resolution as
    /// [`TxCommand::Release`].
    RollbackTo {
        name: Option<String>,
        reply: oneshot::Sender<CtlReply>,
    },
    /// Commit and end the transaction; the actor releases the pinned conn and deregisters.
    Commit { reply: oneshot::Sender<CtlReply> },
    /// Roll back and end the transaction; the actor releases the pinned conn and deregisters.
    Rollback { reply: oneshot::Sender<CtlReply> },
}

/// The actor's reply to a [`TxCommand::Exec`].
#[derive(Debug)]
pub enum ExecReply {
    /// The statement ran to completion (successfully, or with a known/unknown-fate error). The
    /// handler maps this exactly like the S5 autocommit path — `build_terminal_body` on `Ok`, or
    /// `fate::classify_fate(.., OpContext{ readonly, sent: true, in_tx: true })` on `Err`: `in_tx`
    /// is `true` here (unlike the autocommit/control call sites) because a link-loss on an in-tx
    /// STATEMENT means the whole transaction is dead, so it is `Retryable`, never `Indeterminate`.
    /// `exec_us` times only the DB call.
    Completed {
        result: Result<QueryResult, PoolError>,
        exec_us: u64,
    },
    /// A transaction deadline fired mid-statement: the actor cancelled the server statement
    /// out-of-band, drained the query future to its erroring completion, and is rolling back +
    /// tombstoning. The handler declares ONE `TxDeadline{Retryable}` terminal — the statement is
    /// NEVER re-run (charter rule 3).
    Deadline,
}

/// The actor's reply to a tx-control [`TxCommand`] (`Commit`/`Rollback`/`Savepoint`/`Release`/
/// `RollbackTo`).
#[derive(Debug)]
pub enum CtlReply {
    /// The control statement applied. The handler declares an empty `Outcome::Ok`.
    Ok,
    /// The backend rejected/failed the control statement. The handler maps it via
    /// `fate::classify_fate` with `in_tx: false` (a control boundary, never an in-tx statement) —
    /// COMMIT loss → §19.3 `WriteUnconfirmed`; the others are not lost-writes → mapped known-fate.
    Err(PoolError),
    /// A RELEASE/ROLLBACK_TO named (or, with `None`, an empty stack implied) a savepoint that does
    /// not exist. The handler declares `Protocol` (a client-side misuse, never touched the backend).
    UnknownSavepoint,
}

/// A registered transaction's control surface: who owns it, how to command its actor, how to abort
/// it out-of-band, and how to await its teardown.
///
/// `Clone` so [`TxRegistry::lookup`] can hand a caller its own copy while the entry stays
/// registered — every field is a cheap-to-clone handle (`mpsc::Sender`, `CancellationToken`,
/// `watch::Receiver`) or `Copy` (`owner`).
#[derive(Clone, Debug)]
pub struct TxHandle {
    /// The session that opened this transaction. A lookup by any other session is indistinguishable
    /// from an unknown `tx_id` (see [`TxRegistry::lookup`]).
    pub owner: SessionId,
    /// Command channel to the actor that owns the pinned `Checkout`.
    pub cmd_tx: mpsc::Sender<TxCommand>,
    /// Out-of-band teardown signal. Firing it makes the actor cancel any in-flight statement, roll
    /// back, release the conn, and deregister — even mid-statement (the command channel cannot do
    /// this; see this module's doc comment). Fired by [`TxRegistry::abort_session`].
    pub abort: CancellationToken,
    /// Resolves `true` once the actor has finished tearing down (rolled back + released the conn).
    /// The actor holds the paired `watch::Sender`; dropping it (a panicked/finished task) also
    /// resolves an awaiter, so a dead actor never wedges `abort_session`.
    pub done: watch::Receiver<bool>,
    /// Whether the backend this tx is pinned to can stream rows
    /// (`PoolBackend::supports_row_streaming`), captured at BEGIN. The forwarding handler needs it
    /// because `TxHandle` is backend-AGNOSTIC: without it, a tx-scoped `fetch:stream` on MySQL could
    /// only be refused INSIDE the actor — i.e. after checkout + BEGIN, force-tainting the pinned
    /// connection.
    pub streaming: bool,
}

/// The reason a [`TxRegistry::lookup`] failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxLookupErr {
    /// A missing `tx_id` OR an owner mismatch — DELIBERATELY the same variant so cross-session and
    /// unknown are indistinguishable to the caller (both → wire `Protocol`), and a client can never
    /// probe another session's `tx_id` space (nor learn that another session's id was tombstoned).
    NotFoundOrForbidden,
    /// The caller's OWN transaction was torn down by a deadline (`TxDeadline`). Distinct from
    /// `NotFoundOrForbidden` so a timed-out tx is reported to its owner as a retryable
    /// `TxDeadline`, not an opaque `Protocol`.
    Tombstoned,
}

/// A registry entry: either a live actor's control surface, or a tombstone left by a deadline.
enum TxEntry {
    Active(TxHandle),
    /// A transaction the engine tore down on a deadline. Retains `owner` so the owner-vs-other
    /// distinction still holds (only the owner sees `Tombstoned`; anyone else sees the same
    /// `NotFoundOrForbidden` an unknown id yields).
    Tombstoned {
        owner: SessionId,
    },
}

impl TxEntry {
    fn owner(&self) -> SessionId {
        match self {
            TxEntry::Active(h) => h.owner,
            TxEntry::Tombstoned { owner } => *owner,
        }
    }
}

/// The `tx_id → entry` map plus the bounded tombstone-retention bookkeeping, both under ONE mutex so
/// a `tombstone` that evicts the oldest tombstone is atomic with the insertion.
#[derive(Default)]
struct TxTable {
    /// `tx_id` -> its entry (a live actor, or a deadline tombstone).
    map: HashMap<u64, TxEntry>,
    /// Insertion-ordered `tx_id`s of the tombstones currently held, for oldest-first eviction once
    /// the count would exceed [`TOMBSTONE_CAP`]. The deque length is the bound; a stale id (a
    /// tombstone already purged by [`TxRegistry::abort_session`], or superseded) may linger here
    /// harmlessly — eviction skips any id no longer present as a tombstone in `map`.
    tombstone_order: VecDeque<u64>,
}

struct Inner {
    /// Monotonic, never-reused session ids, one drawn per accepted connection.
    next_session_id: AtomicU64,
    /// The transaction table (live actors + bounded deadline tombstones).
    txs: Mutex<TxTable>,
    /// Bounds [`TxRegistry::abort_session`]'s teardown wait — wired to `config.drain_deadline`.
    abort_deadline: Duration,
}

/// A cloneable, `Arc`-backed handle to the process-global transaction registry. Cloning shares the
/// same counter + map (it is an `Arc` internally) — cloning does not create a second registry.
#[derive(Clone)]
pub struct TxRegistry {
    inner: Arc<Inner>,
}

impl TxRegistry {
    /// Build a registry whose [`TxRegistry::abort_session`] teardown wait is bounded by
    /// `abort_deadline` (wire it to `config.drain_deadline`).
    pub fn new(abort_deadline: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                next_session_id: AtomicU64::new(0),
                txs: Mutex::new(TxTable::default()),
                abort_deadline,
            }),
        }
    }

    /// Draw the next session id: monotonic, distinct per call, never reused. One per accepted
    /// connection (see `session::Session::run_with_handler`).
    pub fn next_session_id(&self) -> SessionId {
        self.inner.next_session_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register a transaction's actor control surface under `tx_id` (the BEGIN handler, right after
    /// spawning the actor).
    pub fn register(&self, tx_id: u64, handle: TxHandle) {
        self.inner
            .txs
            .lock()
            .unwrap()
            .map
            .insert(tx_id, TxEntry::Active(handle));
    }

    /// Remove a transaction from the registry entirely (a later lookup → `NotFoundOrForbidden`).
    /// Called by the actor as it tears down on a clean COMMIT/ROLLBACK or an abort.
    pub fn deregister(&self, tx_id: u64) {
        self.inner.txs.lock().unwrap().map.remove(&tx_id);
    }

    /// Replace a transaction's entry with a `TxDeadline` tombstone, preserving its owner (a later
    /// owner lookup → `Tombstoned`; anyone else → `NotFoundOrForbidden`). Called by the actor as it
    /// tears down on a deadline. A no-op if the id is already gone.
    ///
    /// Tombstone retention is BOUNDED by [`TOMBSTONE_CAP`]: recording this tombstone may evict the
    /// OLDEST one (a very old timed-out id then degrades from `Tombstoned` → `NotFoundOrForbidden`
    /// on its owner's next touch — acceptable, see the const's doc), so a long-lived session that
    /// times out many transactions can never accumulate tombstones without bound.
    pub fn tombstone(&self, tx_id: u64) {
        let t = &mut *self.inner.txs.lock().unwrap();
        if let Some(owner) = t.map.get(&tx_id).map(TxEntry::owner) {
            t.map.insert(tx_id, TxEntry::Tombstoned { owner });
            t.tombstone_order.push_back(tx_id);
            // Evict oldest-first until back within the cap. Skip (but still drop from the order
            // deque) any id that is no longer a live tombstone — already purged by a session abort,
            // so the deque, not the map, is what stays bounded.
            while t.tombstone_order.len() > TOMBSTONE_CAP {
                if let Some(oldest) = t.tombstone_order.pop_front()
                    && matches!(t.map.get(&oldest), Some(TxEntry::Tombstoned { .. }))
                {
                    t.map.remove(&oldest);
                }
            }
        }
    }

    /// Look up a transaction by `tx_id` ON BEHALF OF `caller`. On success returns a clone of the
    /// live `TxHandle` (the entry stays registered). Otherwise:
    /// - a missing id, OR an owner other than `caller` (of a live OR tombstoned entry) →
    ///   [`TxLookupErr::NotFoundOrForbidden`] — cross-session and unknown are indistinguishable;
    /// - the caller's OWN tombstoned tx → [`TxLookupErr::Tombstoned`] (→ wire `TxDeadline`).
    pub fn lookup(&self, tx_id: u64, caller: SessionId) -> Result<TxHandle, TxLookupErr> {
        let txs = self.inner.txs.lock().unwrap();
        match txs.map.get(&tx_id) {
            Some(TxEntry::Active(h)) if h.owner == caller => Ok(h.clone()),
            Some(TxEntry::Tombstoned { owner }) if *owner == caller => Err(TxLookupErr::Tombstoned),
            _ => Err(TxLookupErr::NotFoundOrForbidden),
        }
    }

    /// Signal every transaction owned by `sid` to abort and await their teardown, BOUNDED by the
    /// registry's `abort_deadline`, then purge any entries still owned by `sid`.
    ///
    /// Called on every session-end route, between the in-flight registry's `cancel_all()` and the
    /// supervisor drain (see `session::Session::run_with_handler`): `cancel_all()` only fires each
    /// request's `CancellationToken`, which a tx-scoped handler blocked on a `oneshot` recv does
    /// NOT observe — so firing each actor's abort token here makes the actor drop its reply sender,
    /// the in-flight handler's recv returns `Err`, it declares its one terminal, and the supervisor
    /// delivers the `END` inside the drain window.
    ///
    /// The final purge (`retain`) drops both any aborted-active entry that has not yet finished
    /// deregistering AND any deadline tombstone from this session — so nothing leaks per session.
    pub async fn abort_session(&self, sid: SessionId) {
        // Collect the owned live handles under the lock, then release it BEFORE any await (never
        // hold a std::sync::Mutex across an await point).
        let handles: Vec<TxHandle> = {
            let txs = self.inner.txs.lock().unwrap();
            txs.map
                .values()
                .filter_map(|e| match e {
                    TxEntry::Active(h) if h.owner == sid => Some(h.clone()),
                    _ => None,
                })
                .collect()
        };

        if !handles.is_empty() {
            let teardown = async {
                // Fire every owned actor's abort token first (cheap + sync; interrupts even an
                // in-flight statement)...
                for handle in &handles {
                    handle.abort.cancel();
                }
                // ...then await each actor's teardown-complete: the value flips to `true`, OR the
                // actor's `watch::Sender` drops (a panicked/finished task) — both resolve `wait_for`.
                for mut handle in handles {
                    let _ = handle.done.wait_for(|torn_down| *torn_down).await;
                }
            };
            let _ = tokio::time::timeout(self.inner.abort_deadline, teardown).await;
        }

        // Purge anything still owned by this session (belt-and-suspenders against a lingering
        // aborted-active whose actor has not finished, plus this session's deadline tombstones).
        // The `tombstone_order` deque is left as-is: it stays bounded by `TOMBSTONE_CAP` regardless,
        // and eviction already skips any id no longer present as a tombstone in `map`.
        self.inner
            .txs
            .lock()
            .unwrap()
            .map
            .retain(|_, e| e.owner() != sid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_handle(owner: SessionId) -> TxHandle {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<TxCommand>(1);
        let (_done_tx, done_rx) = watch::channel(false);
        TxHandle {
            owner,
            cmd_tx,
            abort: CancellationToken::new(),
            done: done_rx,
            // Registry/lookup fixtures only; `true` is the trait default (PG's real value).
            streaming: true,
        }
    }

    #[tokio::test]
    async fn abort_session_on_empty_registry_returns_promptly() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let sid = reg.next_session_id();
        tokio::time::timeout(Duration::from_millis(100), reg.abort_session(sid))
            .await
            .expect("abort_session on an empty registry must return promptly, not hang");
    }

    #[test]
    fn next_session_id_is_monotonic_and_distinct() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let ids: Vec<SessionId> = (0..8).map(|_| reg.next_session_id()).collect();
        for window in ids.windows(2) {
            assert!(
                window[1] > window[0],
                "session ids must be monotonic: {ids:?}"
            );
        }
        let mut deduped = ids.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            ids.len(),
            "session ids must be distinct: {ids:?}"
        );
    }

    #[test]
    fn next_tx_id_is_monotonic_distinct_and_bounded() {
        let ids: Vec<u64> = (0..8).map(|_| next_tx_id()).collect();
        for window in ids.windows(2) {
            assert!(window[1] > window[0], "tx ids must be monotonic: {ids:?}");
        }
        let mut deduped = ids.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(deduped.len(), ids.len(), "tx ids must be distinct: {ids:?}");
        assert!(
            ids.iter().all(|&id| id < (1u64 << 63)),
            "tx ids bounded < 2^63"
        );
    }

    #[test]
    fn lookup_owner_mismatch_and_missing_both_not_found_or_forbidden() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();
        let other = reg.next_session_id();

        assert_eq!(
            reg.lookup(999, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );

        reg.register(42, dummy_handle(owner));
        assert!(reg.lookup(42, owner).is_ok(), "the owner finds its own tx");
        // A DIFFERENT session gets the SAME NotFoundOrForbidden a missing id yields.
        assert_eq!(
            reg.lookup(42, other).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );
    }

    #[test]
    fn tombstone_is_owner_scoped_and_distinct_from_unknown() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();
        let other = reg.next_session_id();

        reg.register(7, dummy_handle(owner));
        reg.tombstone(7);

        // The owner sees Tombstoned (→ TxDeadline); anyone else sees NotFoundOrForbidden (→
        // Protocol), indistinguishable from an unknown id — a tombstone never leaks across sessions.
        assert_eq!(reg.lookup(7, owner).unwrap_err(), TxLookupErr::Tombstoned);
        assert_eq!(
            reg.lookup(7, other).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );

        // Deregister then a re-tombstone of a gone id is a no-op (stays gone, not resurrected).
        reg.deregister(7);
        reg.tombstone(7);
        assert_eq!(
            reg.lookup(7, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );
    }

    #[tokio::test]
    async fn abort_session_purges_owned_tombstones() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();
        reg.register(1, dummy_handle(owner));
        reg.tombstone(1); // a timed-out tx this session left behind
        assert_eq!(reg.lookup(1, owner).unwrap_err(), TxLookupErr::Tombstoned);

        reg.abort_session(owner).await;
        // The tombstone is purged with the session (no per-session leak).
        assert_eq!(
            reg.lookup(1, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );
    }

    #[test]
    fn tombstone_retention_is_bounded_evicting_oldest() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();

        // Fill exactly to the cap: all CAP tombstones are retained.
        for id in 1..=TOMBSTONE_CAP as u64 {
            reg.register(id, dummy_handle(owner));
            reg.tombstone(id);
        }
        assert_eq!(
            reg.inner.txs.lock().unwrap().map.len(),
            TOMBSTONE_CAP,
            "the map holds exactly CAP tombstones at the cap"
        );
        assert_eq!(reg.lookup(1, owner).unwrap_err(), TxLookupErr::Tombstoned);

        // One more tombstone pushes over the cap → the OLDEST (id 1) is evicted; the map stays
        // bounded at CAP (a long-lived session that times out many txs cannot grow it unboundedly).
        let extra = TOMBSTONE_CAP as u64 + 1;
        reg.register(extra, dummy_handle(owner));
        reg.tombstone(extra);
        assert_eq!(
            reg.inner.txs.lock().unwrap().map.len(),
            TOMBSTONE_CAP,
            "the map stays bounded at CAP after oldest-eviction"
        );

        // The evicted oldest id degrades TxDeadline → Protocol (acceptable — the tx is long dead
        // and ids are never reused); more-recent tombstones stay retryable.
        assert_eq!(
            reg.lookup(1, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden,
            "the oldest tombstone is evicted"
        );
        assert_eq!(reg.lookup(2, owner).unwrap_err(), TxLookupErr::Tombstoned);
        assert_eq!(
            reg.lookup(extra, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );
    }
}
