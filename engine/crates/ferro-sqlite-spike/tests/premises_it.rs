//! C3-1: the four premises SPEC D13 and `docs/dev-loop/C3-SQLITE-SCOPE.md` rest on, each proven
//! against real `rusqlite` and a real on-disk WAL database. See the crate docs: this is a SPIKE,
//! not the backend.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rusqlite::{Connection, ffi};

/// `SQLITE_BUSY_SNAPSHOT` = `SQLITE_BUSY | (2<<8)` = 517. Spelled out rather than taken from a
/// constant so the number this spike asserts is the number a reader can look up.
const SQLITE_BUSY_SNAPSHOT: i32 = 517;

/// `SQLITE_READONLY` = 8. Spelled out for the same reason as `SQLITE_BUSY_SNAPSHOT`.
const SQLITE_READONLY: i32 = 8;

fn open_wal(path: &Path) -> Connection {
    let conn = Connection::open(path).expect("open");
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .expect("set WAL");
    assert_eq!(
        mode.to_lowercase(),
        "wal",
        "the whole premise set is about WAL mode; a db that silently stayed in rollback mode \
         would prove nothing"
    );
    conn
}

fn extended_code(err: &rusqlite::Error) -> i32 {
    match err {
        rusqlite::Error::SqliteFailure(ffi::Error { extended_code, .. }, _) => *extended_code,
        other => panic!("expected a SqliteFailure, got {other:?}"),
    }
}

/// The PRIMARY error as rusqlite's typed enum.
///
/// Deliberately not `extended_code & 0xff` compared against `ErrorCode::X as i32`: `ffi::ErrorCode`
/// is a plain Rust enum whose discriminants are its own declaration order, NOT SQLite's numeric
/// codes (`DatabaseBusy` is discriminant 3 while SQLITE_BUSY is 5; `OperationInterrupted` is 7
/// while SQLITE_INTERRUPT is 9). Casting it to an integer and comparing against a SQLite code
/// compiles cleanly and is always wrong — this spike wrote that bug and caught it here.
fn primary_code(err: &rusqlite::Error) -> ffi::ErrorCode {
    match err {
        rusqlite::Error::SqliteFailure(ffi::Error { code, .. }, _) => *code,
        other => panic!("expected a SqliteFailure, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// P4 — the crux of the REJECTED option (A), and the reason D13 takes the lock at BEGIN.
//
// D13's revisit note requires this be REPRODUCED, not inherited: the decision going against
// Option A is not a licence to skip proving A's failure mode is real.
// ---------------------------------------------------------------------------------------------

/// **P4a: `busy_timeout` does NOT retry a deferred upgrade.** A transaction that begins as a reader
/// and later writes, after another connection has committed, gets `SQLITE_BUSY_SNAPSHOT` — and gets
/// it IMMEDIATELY, with a 5-second `busy_timeout` armed on the very connection that fails.
///
/// The elapsed-time assertion is the load-bearing half. Merely observing the error would be
/// consistent with "busy_timeout retried for 5s and then gave up", which is a completely different
/// (and far more benign) claim. Returning in milliseconds proves the retry loop was never entered.
#[test]
fn p4a_busy_timeout_does_not_retry_a_deferred_upgrade() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p4a.db");

    let a = open_wal(&path);
    let b = open_wal(&path);
    let timeout = Duration::from_secs(5);
    a.busy_timeout(timeout).expect("busy_timeout on A");
    b.busy_timeout(timeout).expect("busy_timeout on B");

    a.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
         INSERT INTO t VALUES (1, 1);",
    )
    .expect("seed");

    // A begins DEFERRED and READS — this is what takes the read snapshot. A deferred BEGIN with no
    // statement after it would acquire nothing and prove nothing.
    a.execute_batch("BEGIN DEFERRED").expect("A begins");
    let seen: i64 = a
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .expect("A reads");
    assert_eq!(seen, 1, "A's snapshot is the pre-write state");

    // B writes and COMMITS, moving the database past A's snapshot.
    b.execute_batch("BEGIN IMMEDIATE; UPDATE t SET v = 2 WHERE id = 1; COMMIT;")
        .expect("B writes and commits");

    // A now tries to UPGRADE to a writer. This is the deferred-upgrade case.
    let started = Instant::now();
    let err = a
        .execute("UPDATE t SET v = 3 WHERE id = 1", [])
        .expect_err("A's upgrade MUST fail — its snapshot is stale");
    let elapsed = started.elapsed();

    assert_eq!(
        extended_code(&err),
        SQLITE_BUSY_SNAPSHOT,
        "the deferred-upgrade failure is SQLITE_BUSY_SNAPSHOT (517), not a plain SQLITE_BUSY: {err:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "THE LOAD-BEARING ASSERTION: a 5s busy_timeout was armed on this very connection and the \
         call still returned in {elapsed:?}. busy_timeout never retried — which is exactly why \
         Option A leaves a failure class the engine is forbidden to resolve (charter rule 3 \
         forbids the rollback-and-replay this needs)."
    );

    // And the transaction is left for the caller to roll back — the engine cannot replay it.
    a.execute_batch("ROLLBACK").expect("A rolls back");
}

/// **P6: `is_autocommit()` tracks an AUTOMATIC rollback — the case engine-side tracking misses.**
///
/// This is the premise SPEC §7.1's SQLite paragraph rests on, so it is proven rather than asserted.
/// §7.1's rule is that pin decisions come from the backend's own authoritative state report, never
/// from lexing the statement text. For SQLite that report is `sqlite3_get_autocommit()` via
/// `Connection::is_autocommit()` — and the reason it must be READ AFTER EVERY STATEMENT, rather
/// than the engine simply remembering that it sent a `BEGIN`, is that SQLite can roll a transaction
/// back on its own. A constraint declared `ON CONFLICT ROLLBACK` does exactly that: the statement
/// fails AND the transaction ends, with nothing in the SQL text saying so.
///
/// An engine that tracked `BEGIN`/`COMMIT` itself would believe a transaction was still open, hold
/// the pin, and hand the next tenant a connection it thinks is mid-transaction — the §7.1 hazard,
/// arrived at from the other direction.
///
/// The CONTROL is the second half: the same duplicate insert against a PLAIN unique constraint
/// (SQLite's default is `ON CONFLICT ABORT`) fails the statement and leaves the transaction OPEN,
/// so `is_autocommit()` stays false. Without it, the first half would be equally consistent with
/// "the transaction never opened" or "any error ends a transaction".
#[test]
fn p6_is_autocommit_tracks_an_automatic_rollback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p6.db");
    let conn = open_wal(&path);

    conn.execute_batch(
        "CREATE TABLE auto(id INTEGER PRIMARY KEY, v INTEGER UNIQUE ON CONFLICT ROLLBACK);
         CREATE TABLE abort(id INTEGER PRIMARY KEY, v INTEGER UNIQUE);",
    )
    .expect("seed");

    assert!(
        conn.is_autocommit(),
        "baseline: a connection with no transaction open reports autocommit"
    );

    // (a) ON CONFLICT ROLLBACK — the transaction ends underneath the engine.
    conn.execute_batch("BEGIN IMMEDIATE").expect("begin");
    conn.execute("INSERT INTO auto VALUES (1, 1)", [])
        .expect("first insert");
    assert!(
        !conn.is_autocommit(),
        "a transaction is open, so the authority reports NOT autocommit"
    );

    conn.execute("INSERT INTO auto VALUES (2, 1)", [])
        .expect_err("duplicate v must violate the unique constraint");

    assert!(
        conn.is_autocommit(),
        "THE LOAD-BEARING ASSERTION: SQLite rolled the transaction back BY ITSELF, and the library \
         call reports it. Nothing in the SQL text said so, so an engine that tracked BEGIN/COMMIT \
         itself would still believe this connection is mid-transaction and would pin it for a \
         transaction that no longer exists."
    );

    // (b) CONTROL: a plain unique constraint (ON CONFLICT ABORT) fails the STATEMENT only. Without
    //     this half, (a) would be equally consistent with "any error ends a transaction".
    conn.execute_batch("BEGIN IMMEDIATE").expect("begin again");
    conn.execute("INSERT INTO abort VALUES (1, 1)", [])
        .expect("first insert");
    conn.execute("INSERT INTO abort VALUES (2, 1)", [])
        .expect_err("duplicate v must violate the unique constraint");
    assert!(
        !conn.is_autocommit(),
        "THE CONTROL: an ordinary constraint failure leaves the transaction OPEN, so the same \
         signal still reports in-transaction. The difference between (a) and (b) is SQLite's own \
         conflict resolution, which is exactly why the signal must be read rather than inferred."
    );
    conn.execute_batch("ROLLBACK").expect("explicit rollback");
    assert!(conn.is_autocommit(), "and an explicit ROLLBACK ends it too");
}

/// **P5: `PRAGMA query_only` closes the hole D13's own readonly arm opens.**
///
/// D13 says a transaction the client DECLARED `readonly` takes a DEFERRED reader lock. That is
/// precisely P4a's setup — so a client that declares `readonly` and then writes anyway walks
/// straight into the one failure class C3-1 proved the engine cannot resolve, since charter rule 3
/// forbids the rollback-and-replay `SQLITE_BUSY_SNAPSHOT` needs. The false declaration is the
/// client's, but the unretryable error would be the engine's to report, and it would appear only
/// under concurrency — the worst shape for a bug.
///
/// `PRAGMA query_only=ON` converts it into a different error entirely: the write is refused up
/// front with `SQLITE_READONLY` (8), which is deterministic, provably did not run (so its §9.2 fate
/// is `NonRetryable`, never `Indeterminate`), and does not depend on whether anyone else happened
/// to commit. Proven against the SAME fixture as P4a so the two are directly comparable.
///
/// This is a premise for the slice that starts composing `BEGIN DEFERRED`, NOT a claim that any
/// backend sets the pragma — no SQLite backend exists yet. It records that the mitigation is real
/// BEFORE the arm that needs it ships, so the arm is not left resting on an assumption.
#[test]
fn p5_query_only_turns_a_lying_readonly_into_a_clean_refusal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p5.db");

    let a = open_wal(&path);
    let b = open_wal(&path);
    a.busy_timeout(Duration::from_secs(5)).expect("timeout A");
    b.busy_timeout(Duration::from_secs(5)).expect("timeout B");

    b.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
         INSERT INTO t VALUES (1, 1);",
    )
    .expect("seed");

    // The declared-readonly connection. This is what the backend would arm for a `readonly` request.
    a.execute_batch("PRAGMA query_only=ON")
        .expect("arm query_only");

    // (a) The STALE-SNAPSHOT shape — byte for byte P4a's setup, which without the pragma yields the
    //     unretryable 517.
    a.execute_batch("BEGIN DEFERRED").expect("A begins");
    let seen: i64 = a
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .expect("query_only must NOT interfere with reading — that is the whole point of it");
    assert_eq!(seen, 1);

    b.execute_batch("BEGIN IMMEDIATE; UPDATE t SET v = 2 WHERE id = 1; COMMIT;")
        .expect("B moves the db past A's snapshot");

    let started = Instant::now();
    let err = a
        .execute("UPDATE t SET v = 3 WHERE id = 1", [])
        .expect_err("a query_only connection must refuse the write");
    let elapsed = started.elapsed();

    assert_eq!(
        extended_code(&err),
        SQLITE_READONLY,
        "THE LOAD-BEARING ASSERTION: under exactly P4a's conditions the failure is now READONLY \
         (8), NOT the unretryable BUSY_SNAPSHOT (517). Compare p4a, which asserts 517 on this same \
         fixture without the pragma — that pair is the proof the mitigation is the pragma and not \
         some accident of timing: {err:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "refused up front, not after a busy wait: {elapsed:?}"
    );
    a.execute_batch("ROLLBACK").expect("A rolls back");

    // (b) And it is DETERMINISTIC, not a race won: with NO concurrent writer and no stale snapshot
    //     at all, the same write is refused identically. Without this half, (a) would be equally
    //     consistent with "READONLY happens to win the race against BUSY_SNAPSHOT here".
    let err = a
        .execute("UPDATE t SET v = 4 WHERE id = 1", [])
        .expect_err("still refused with no contention whatsoever");
    assert_eq!(
        extended_code(&err),
        SQLITE_READONLY,
        "the refusal is a property of the connection, not of contention: {err:?}"
    );

    // The pragma is reversible on the connection, which is what makes it usable on a POOLED one:
    // a backend arms it per-checkout for a declared-readonly request and disarms it on recycle.
    a.execute_batch("PRAGMA query_only=OFF").expect("disarm");
    a.execute("UPDATE t SET v = 5 WHERE id = 1", [])
        .expect("the same connection writes once the pragma is off");
}

/// **P4b: `BEGIN IMMEDIATE` removes the class by construction — and busy_timeout DOES work there.**
///
/// This is the other half of the argument, and it is what makes D13 a fix rather than a trade. With
/// the write lock taken at BEGIN, the upgrade never happens; contention moves to the OTHER
/// connection, where it surfaces as a plain `SQLITE_BUSY` that `busy_timeout` genuinely retries —
/// proven here by the elapsed time being AT LEAST the timeout, the mirror of P4a's assertion.
#[test]
fn p4b_begin_immediate_removes_the_class_and_busy_timeout_applies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p4b.db");

    let a = open_wal(&path);
    let b = open_wal(&path);
    a.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
         INSERT INTO t VALUES (1, 1);",
    )
    .expect("seed");

    let b_timeout = Duration::from_millis(600);
    b.busy_timeout(b_timeout).expect("busy_timeout on B");

    // A takes the writer lock AT BEGIN — the D13 rule for an undeclared statement.
    a.execute_batch("BEGIN IMMEDIATE")
        .expect("A begins immediate");
    let seen: i64 = a
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .expect("A reads inside its write transaction");
    assert_eq!(seen, 1);

    // B now attempts to become a writer and must WAIT, then fail with a plain busy.
    let started = Instant::now();
    let err = b
        .execute_batch("BEGIN IMMEDIATE")
        .expect_err("B cannot take a second writer lock");
    let elapsed = started.elapsed();

    assert_eq!(
        primary_code(&err),
        ffi::ErrorCode::DatabaseBusy,
        "B's contention is a plain SQLITE_BUSY: {err:?}"
    );
    assert_eq!(extended_code(&err), 5, "SQLITE_BUSY is 5: {err:?}");
    assert_ne!(
        extended_code(&err),
        SQLITE_BUSY_SNAPSHOT,
        "and specifically NOT the snapshot variant — nothing upgraded"
    );
    // 90% of the armed timeout, not 100%: SQLite's busy handler sleeps in increments and may
    // return a hair early under scheduling jitter. The claim being made is "parked for roughly the
    // armed duration rather than returning immediately", and the gap it must separate is enormous
    // (600 ms vs P4a's sub-millisecond return), so a 10% floor costs the proof nothing and keeps a
    // loaded CI runner from producing a red lane this environment cannot read.
    let floor = b_timeout.mul_f64(0.9);
    assert!(
        elapsed >= floor,
        "THE MIRROR OF P4a: busy_timeout genuinely retried here, parking for {elapsed:?} (>= \
         {floor:?}, from the {b_timeout:?} armed). The same knob that is useless against a \
         deferred upgrade is effective once the lock is taken at BEGIN — so D13 converts an \
         unresolvable failure into an ordinary, bounded wait."
    );

    // A's own write succeeds: it never has to upgrade, because it never was a reader.
    a.execute("UPDATE t SET v = 3 WHERE id = 1", [])
        .expect("A writes without upgrading");
    a.execute_batch("COMMIT").expect("A commits");

    let after: i64 = b
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .expect("B reads after");
    assert_eq!(after, 3, "A's write landed exactly once");
}

// ---------------------------------------------------------------------------------------------
// P2 — `RowStream: BackendRows + Send` across `spawn_blocking` WITHOUT buffering the result.
// This is the property §14's never-buffer clause needs, and the largest single piece of C3 work.
// ---------------------------------------------------------------------------------------------

/// **P2: rows cross the `spawn_blocking` boundary incrementally, and the connection comes back.**
///
/// Two things are proven, and the second is the one the pool actually needs:
///
/// 1. **Non-buffering.** The producer sends over a capacity-1 bounded channel and counts every row
///    it hands over. After the consumer has taken 10 of 100 000 rows, the producer must have
///    produced only a handful — if `rusqlite` forced the whole result into memory first, or the
///    channel were unbounded, the count would be ~100 000.
/// 2. **The connection is recoverable.** The blocking task OWNS the `Connection` for the stream's
///    lifetime (it must — `Statement`/`Rows` borrow it) and hands it back when done. That is the
///    park/unpark shape MySQL already needed at B2b-2a, so the pool's existing `reclaim_stream`
///    seam fits; a connection that could not be handed back would mean a discarded conn per stream.
#[tokio::test(flavor = "multi_thread")]
async fn p2_rows_stream_across_spawn_blocking_without_buffering() {
    const TOTAL: usize = 100_000;
    const TAKE: usize = 10;

    fn assert_send<T: Send>() {}
    assert_send::<tokio::sync::mpsc::Receiver<i64>>();

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p2.db");
    let conn = open_wal(&path);

    let produced = Arc::new(AtomicUsize::new(0));
    let produced_in_task = Arc::clone(&produced);

    // Capacity 1: the producer parks on the very first unread row.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<i64>(1);

    let handle = tokio::task::spawn_blocking(move || {
        {
            let mut stmt = conn
                .prepare(
                    "WITH RECURSIVE seq(x) AS (
                         SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < ?1
                     ) SELECT x FROM seq",
                )
                .expect("prepare");
            let mut rows = stmt.query([TOTAL as i64]).expect("query");
            while let Some(row) = rows.next().expect("step") {
                let v: i64 = row.get(0).expect("get");
                produced_in_task.fetch_add(1, Ordering::SeqCst);
                if tx.blocking_send(v).is_err() {
                    // Consumer abandoned: stop stepping. This is the abandonment path the pool's
                    // Drop net covers for the other backends.
                    break;
                }
            }
        }
        // `stmt`/`rows` are dropped above, releasing their borrow, so the connection can be
        // returned to the pool — the premise this half of the test exists to prove.
        conn
    });

    let mut got = Vec::with_capacity(TAKE);
    for _ in 0..TAKE {
        got.push(rx.recv().await.expect("row"));
    }
    assert_eq!(
        got,
        (1..=TAKE as i64).collect::<Vec<_>>(),
        "rows arrive in order"
    );

    // THE STALL PROBE, and it is the whole test. Reading the counter straight after taking TAKE
    // rows proves NOTHING: the producer is on another thread and simply has not had time to run
    // away yet, so the assertion passes just as happily with an unbounded channel. (Measured — the
    // first version of this test did exactly that, and the mutation below passed.) Sleeping first
    // gives an unconstrained producer ample wall-clock to finish all TOTAL rows, so a counter that
    // is STILL tiny afterwards can only mean it is parked on backpressure.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let produced_now = produced.load(Ordering::SeqCst);

    assert!(
        produced_now < 64,
        "NON-BUFFERING: 250ms after taking {TAKE} of {TOTAL} rows — with the consumer idle the \
         whole time — the producer has produced only {produced_now} and is parked on the \
         capacity-1 channel. MUTATION-PROVEN: widen that channel to {TOTAL}*2 and this reads \
         {TOTAL}, because nothing then holds the producer back."
    );

    // Drain the rest and recover the connection.
    while rx.recv().await.is_some() {}
    let conn = handle.await.expect("blocking task joined");
    assert_eq!(
        produced.load(Ordering::SeqCst),
        TOTAL,
        "the full result really was {TOTAL} rows — the small count above was backpressure, not a \
         short read"
    );

    let alive: i64 = conn
        .query_row("SELECT 1", [], |r| r.get(0))
        .expect("the handed-back connection is usable");
    assert_eq!(alive, 1);
    assert!(
        conn.is_autocommit(),
        "and it comes back with no transaction open"
    );
}

// ---------------------------------------------------------------------------------------------
// P3 — `cancel_handle` / `Cancel`: PG and MySQL both cancel over a SIDE connection; SQLite's
// equivalent is `sqlite3_interrupt()` on the same handle, from another thread.
// ---------------------------------------------------------------------------------------------

/// **P3: an `InterruptHandle` is obtainable, is `Send + Sync + 'static`, and actually interrupts.**
///
/// The trait bound is checked at COMPILE time (that is the premise the scope doc marked unverified
/// — whether the handle can satisfy `Cancel`'s supertrait bounds) and the behaviour is checked at
/// RUN time, because a handle that satisfied the bounds but did not stop a running statement would
/// give the pool a cancel that silently does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn p3_interrupt_handle_is_send_static_and_actually_interrupts() {
    fn assert_send_sync_static<T: Send + Sync + 'static>() {}
    assert_send_sync_static::<rusqlite::InterruptHandle>();

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p3.db");
    let conn = open_wal(&path);
    let interrupt = conn.get_interrupt_handle();

    // Interrupt REPEATEDLY, not once. `sqlite3_interrupt` only affects a statement that is already
    // running, so a single well-timed call is a race: if `spawn_blocking` is slow to start on a
    // loaded runner the interrupt lands first, does nothing, and the test HANGS on a query with
    // 1e11 iterations rather than failing. Re-firing makes the test robust to that scheduling
    // without weakening what it proves.
    let fired = tokio::task::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            interrupt.interrupt();
        }
    });

    let started = Instant::now();
    let err = tokio::task::spawn_blocking(move || {
        // Unbounded enough that it cannot finish on its own inside the test.
        conn.query_row(
            "WITH RECURSIVE seq(x) AS (
                 SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 100000000000
             ) SELECT count(*) FROM seq",
            [],
            |r| r.get::<_, i64>(0),
        )
    })
    .await
    .expect("blocking task joined")
    .expect_err("the statement MUST be interrupted, not complete");
    let elapsed = started.elapsed();

    fired.abort();

    assert_eq!(
        primary_code(&err),
        ffi::ErrorCode::OperationInterrupted,
        "interrupting yields SQLITE_INTERRUPT: {err:?}"
    );
    assert_eq!(extended_code(&err), 9, "SQLITE_INTERRUPT is 9: {err:?}");
    assert!(
        elapsed < Duration::from_secs(10),
        "and it took effect promptly ({elapsed:?}), rather than the query merely ending on its own"
    );
}

// ---------------------------------------------------------------------------------------------
// P1 — "no vendored driver fork is needed". Stated honestly: every capability the `PoolBackend`
// seam demands is reachable on rusqlite's PUBLIC API. That is a checkable claim; "no fork will
// ever be needed" is not.
// ---------------------------------------------------------------------------------------------

/// **P1: the seam's capabilities are all public API — no fork required for what C3 needs.**
///
/// The assumption this checks is the one that was proven FALSE for `mysql_async` at M1-S6 (stock
/// never negotiates `CLIENT_SESSION_TRACK`, so the pin authority would have been dead at the
/// handshake) and for `tokio-postgres` at M1-S1 (the RFQ byte is parsed and discarded). Both times
/// it was checked before any backend code existed. This is that check for SQLite, and the test
/// COMPILING is most of the proof — a capability that needed a fork would not type.
#[test]
fn p1_every_capability_the_seam_needs_is_on_the_public_api() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p1.db");
    let conn = open_wal(&path);
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
        .expect("seed");

    // tx_status authority — the `ReadyForQuery`/`SERVER_STATUS_IN_TRANS` analogue. Synchronous and
    // round-trip-free, exactly as the trait documents, because SQLite is a library not a protocol.
    assert!(conn.is_autocommit(), "no tx open yet");
    conn.execute_batch("BEGIN IMMEDIATE").expect("begin");
    assert!(
        !conn.is_autocommit(),
        "sqlite3_get_autocommit() is a real pin signal"
    );
    conn.execute_batch("COMMIT").expect("commit");
    assert!(conn.is_autocommit(), "and it clears on commit");

    // cancel (P3 proves it works; this proves it is reachable from the seam's shape).
    let _handle: rusqlite::InterruptHandle = conn.get_interrupt_handle();

    // busy handling.
    conn.busy_timeout(Duration::from_millis(250))
        .expect("busy_timeout is public");

    // last_insert_id + affected, the two the OK-packet/command-tag paths already carry.
    conn.execute("INSERT INTO t(v) VALUES ('a')", [])
        .expect("insert");
    assert_eq!(conn.last_insert_rowid(), 1, "last_insert_id is public");
    let affected = conn
        .execute("UPDATE t SET v = 'b' WHERE id = 1", [])
        .expect("update");
    assert_eq!(affected, 1, "affected-rows is public");

    // Extended error codes — needed to tell SQLITE_BUSY_SNAPSHOT from a plain busy, which is the
    // distinction D13 is built on. P4a/P4b already depend on this; asserted here so P1 is a
    // complete list rather than an implicit one.
    let err = conn
        .execute("INSERT INTO t(id, v) VALUES (1, 'dup')", [])
        .expect_err("primary key violation");
    assert_eq!(
        extended_code(&err),
        ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
        "extended codes reach the caller — the §9.2 fate matrix keys on them: {err:?}"
    );
}
