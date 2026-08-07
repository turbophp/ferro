//! M1-S6 Task 7 — the §7.1 tracker-verification HARD GATE (real, read-back) + live MySQL/MariaDB
//! pin/hygiene parity with pin-cause assertions, driven through a real `Pool<MysqlBackend>`.
//!
//! This is the S6 acceptance test. It proves, against real Dockerized MySQL 8 + MariaDB 11, that the
//! split pin signal (T3) + the CALL/DO assist backstop (T6) + the conditional hygiene at checkout
//! (S3) together:
//!
//!   1. **DETECT** a session mutation performed INSIDE a stored program (`CALL p_set_session()` runs
//!      `SET SESSION sort_buffer_size` in its body — invisible to the single-statement assist lexer,
//!      which sees only `CALL p_set_session()`) — the conn ends up **tainted**, via the OK-packet
//!      tracker (`PinCause::SessionTracker`) and/or the unconditional CALL backstop
//!      (`PinCause::Unknown`); and
//!   2. **CLOSE the leak** — the NEXT lease on the SAME physical connection ran
//!      `COM_RESET_CONNECTION` at checkout and reads the session variable back at the server DEFAULT,
//!      never the mutated value. A value that survives to the next tenant is a hard FAIL.
//!
//! Plus the pin-cause DoD parity (mirroring PG's `pg_pool_it.rs`): a bare `BEGIN`/`COMMIT` pins one
//! conn (`PinCause::Tx`) and unpins; a direct `SET SESSION` taints (tracker authority); a plain
//! autocommit `SELECT` never pins.
//!
//! **The read-back discriminator (a live subtlety this gate exposes).** `p_set_session()` sets
//! `sort_buffer_size = 262144`, which on MySQL 8 is *exactly the server default* (verified live:
//! `@@global.sort_buffer_size = 262144`), so the in-proc value is INDISTINGUISHABLE from the default
//! — a bare read-back could not tell "reset happened" from "leak survived". On MariaDB 11 the default
//! is 2097152, so the in-proc 262144 is directly discriminating. To make the hygiene proof
//! non-vacuous on BOTH engines, `co1` additionally installs a guaranteed-non-default probe
//! (`default * 2`, always a valid block-size multiple) and the gate asserts the next lease reads the
//! DEFAULT (never the probe). See §22 for the divergence note.
//!
//! Every test SKIPS (does not fail) when its env var is unset, so `cargo test --workspace` stays
//! green offline (S2 convention):
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferro-backend-mysql --test pin_hygiene_it -- --nocapture
//! ```

use std::time::Duration;

use ferro_backend_mysql::MysqlBackend;
use ferro_pool::backend::PoolBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::{Checkout, Pool};
use mysql_async::prelude::Queryable;

/// A pool config with an explicit `max_size` and a generous checkout timeout (the recycle path runs
/// a real `COM_RESET_CONNECTION` under this bound). `max_size = 1` forces the SAME physical
/// connection to come back at the next checkout — the reuse the hard gate depends on.
fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(10),
        ..PoolConfig::default()
    }
}

/// A verification-only raw read of `@@session.sort_buffer_size` off the checked-out connection.
/// Goes straight at `MysqlConn::mysql` (like PG's tests hit `PgConn::client`) — this BYPASSES the
/// pin authority, which is exactly right for OBSERVING server state without perturbing the taint
/// under test. `sort_buffer_size` is an unsigned integer variable → `u64`.
async fn read_sbs(co: &mut Checkout<MysqlBackend>) -> u64 {
    co.conn_mut()
        .mysql
        .query_first::<u64, _>("SELECT @@session.sort_buffer_size")
        .await
        .expect("read @@session.sort_buffer_size")
        .expect("sort_buffer_size returned one row")
}

/// A verification-only raw read of `@@session.sort_buffer_size` off a bare (non-pooled) backend
/// connection — the same read as [`read_sbs`], but for the raw `MysqlConn` the tracker-authority
/// proof and the parity `(b1)` open directly via `pool.backend().connect()`.
async fn read_sbs_raw(raw: &mut ferro_backend_mysql::MysqlConn) -> u64 {
    raw.mysql
        .query_first::<u64, _>("SELECT @@session.sort_buffer_size")
        .await
        .expect("read @@session.sort_buffer_size (raw)")
        .expect("sort_buffer_size returned one row (raw)")
}

/// The physical connection id (the MySQL protocol handshake thread id off the driver — NOT
/// `CONNECTION_ID()`, which is a server-side value that a `COM_RESET_CONNECTION` also preserves but
/// which would need a query round trip to read; the driver already holds the handshake id.
/// Its `BIGINT UNSIGNED` type is no longer a blocker — M1-S7 admits it as `U64` — but the protocol
/// id remains the more direct signal).
/// `COM_RESET_CONNECTION` preserves this id (it re-initializes session state on the same TCP conn),
/// so equal ids across two checkouts proves the SAME physical conn was reused.
fn conn_id(co: &Checkout<MysqlBackend>) -> u32 {
    co.conn().mysql.id()
}

// -------------------------------------------------------------------------------------------------
// THE HARD GATE (R2, M1-D5): tracker-verification + read-back leak-closed.
// -------------------------------------------------------------------------------------------------

async fn run_hard_gate(url: &str, label: &str) {
    // max_size = 1: the ONLY connection this pool creates MUST be the one reused at the next
    // checkout — that reuse is what lets the read-back observe whether the mutation leaked.
    let pool = Pool::new(MysqlBackend::new(url), config(1));

    // ---- R2 HEADLINE PROPERTY, proven SELF-CONTAINED (the TRACKER, not the CALL backstop) --------
    // The gate's raison d'être (§7.1): the OK-packet tracker sees a session mutation performed INSIDE
    // a stored program. The `Checkout::exec("CALL …")` path below proves DETECTION + hygiene, but its
    // taint can come from the T6 unconditional CALL/DO lexer backstop — which would MASK a dead
    // tracker (the gate would still pass). So prove the tracker DIRECTLY here, on a RAW backend conn:
    // `take_session_mutated` at the backend level reflects ONLY the OK-packet tracker (it never runs
    // `apply_classify`). Pre-set a NON-default value first so the proc's `262144` is a REAL change on
    // MySQL 8 too (whose default IS 262144 — a no-op SET might be tracker-suppressed; matching
    // conn_it.rs), DRAIN that pre-set's taint (read-and-clear), THEN CALL and assert the tracker fired.
    {
        let mut raw = pool
            .backend()
            .connect()
            .await
            .expect("raw connect (tracker proof)");
        let raw_default = read_sbs_raw(&mut raw).await;
        pool.backend()
            .simple_query(
                &mut raw,
                &format!("SET SESSION sort_buffer_size = {}", raw_default * 2),
            )
            .await
            .expect(
                "pre-set a non-default sort_buffer_size (so the proc's 262144 is a real change)",
            );
        // Drain the pre-set's taint so the assertion below is about the CALL alone, not the SET.
        let _ = pool.backend().take_session_mutated(&mut raw);
        pool.backend()
            .simple_query(&mut raw, "CALL p_set_session()")
            .await
            .expect("CALL p_set_session() (raw)");
        let tracker_fired = pool.backend().take_session_mutated(&mut raw);
        assert!(
            tracker_fired,
            "[{label}] R2: the OK-packet tracker MUST fire for a SET SESSION inside a stored program \
             (proven independent of the CALL lexer backstop) — a dead tracker fails HERE"
        );
        println!(
            "[{label}] TRACKER PROOF: take_session_mutated after CALL p_set_session() \
             (pre-set {} → proc set 262144) = {tracker_fired} — the tracker saw inside the stored program",
            raw_default * 2
        );
        drop(raw);
    }

    // ---- co1: run the in-proc mutation, prove DETECTION, install a discriminating probe ----------
    let (co1_id, default_sbs, in_proc_val, probe) = {
        let mut co1 = pool.checkout().await.expect("checkout co1");
        let id = conn_id(&co1);

        // A fresh (never-recycled) conn is at the server default (connect ran only the baselined
        // tracker-setup SETs, never sort_buffer_size).
        let default_sbs = read_sbs(&mut co1).await;

        // THE in-proc mutation: `SET SESSION sort_buffer_size` buried in a stored program. The assist
        // lexer sees only `CALL p_set_session()`; the OK-packet tracker sees the SET.
        co1.exec("CALL p_set_session()")
            .await
            .expect("CALL p_set_session()");

        // DETECTION: the mechanism tainted the conn. Cause is EITHER the OK-packet tracker
        // (SessionTracker) OR the unconditional CALL backstop (Unknown) — both close the leak. Since
        // `apply_classify` (the CALL→Unknown rule) composes AFTER `apply_session_tracker`, the
        // surfaced label is Unknown on both engines, but the assertion accepts either.
        let cause = co1.last_pin_cause();
        assert!(
            co1.tainted(),
            "[{label}] HARD GATE: CALL p_set_session() must TAINT the conn (in-proc mutation detected)"
        );
        assert!(
            matches!(
                cause,
                Some(PinCause::SessionTracker) | Some(PinCause::Unknown)
            ),
            "[{label}] HARD GATE: CALL taint cause must be SessionTracker or Unknown, got {cause:?}"
        );

        // The value the proc actually installed on this conn (== 262144 on both engines).
        let in_proc_val = read_sbs(&mut co1).await;

        // Make the read-back DISCRIMINATING on BOTH engines. On MariaDB the in-proc 262144 already
        // differs from the default (2097152); on MySQL 8 it COINCIDES with the default (262144), so a
        // bare read-back cannot distinguish reset-happened from leak-survived. `default * 2` is a
        // guaranteed-non-default, block-size-valid probe (the same `* 2` trick T3's conn_it uses).
        let probe = default_sbs * 2;
        co1.exec(&format!("SET SESSION sort_buffer_size = {probe}"))
            .await
            .expect("install the non-default probe");
        let after_set = read_sbs(&mut co1).await;
        assert_eq!(
            after_set, probe,
            "[{label}] the probe SET must take effect on co1"
        );

        println!(
            "[{label}] GATE co1: tainted={} cause={:?} default_sbs={} in_proc_val={} probe={}",
            co1.tainted(),
            cause,
            default_sbs,
            in_proc_val,
            probe
        );

        (id, default_sbs, in_proc_val, probe)
        // co1 drops HERE → returns to the idle stack with `tainted = true`.
    };

    // ---- co2: the SAME physical conn, recycled via COM_RESET_CONNECTION ---------------------------
    let mut co2 = pool.checkout().await.expect("checkout co2");
    assert_eq!(
        conn_id(&co2),
        co1_id,
        "[{label}] max_size=1 must hand back the SAME physical conn (else the read-back is vacuous)"
    );

    // A fresh Checkout must NOT inherit taint from the previous holder.
    assert!(
        !co2.tainted(),
        "[{label}] co2 is a fresh lease — it must not inherit co1's taint"
    );

    let after_reset = read_sbs(&mut co2).await;

    // HYGIENE — the reuse-safety property. The recycle's COM_RESET_CONNECTION restored the session
    // default; NEITHER the in-proc mutation NOR the probe leaked to the next tenant. Discriminating
    // on both engines: `probe` (default*2) is never equal to `default_sbs`.
    assert_eq!(
        after_reset, default_sbs,
        "[{label}] LEAK: next lease must read the DEFAULT sort_buffer_size ({default_sbs}), got \
         {after_reset} (probe was {probe}) — the session mutation SURVIVED the recycle"
    );
    assert_ne!(
        after_reset, probe,
        "[{label}] LEAK: the probe value {probe} must NOT survive the recycle"
    );

    println!(
        "[{label}] GATE PASS: co1 tainted (in-proc mutation DETECTED); co2 sort_buffer_size={after_reset} \
         (DEFAULT — probe {probe} cleared) → leak CLOSED"
    );
    if in_proc_val != default_sbs {
        println!(
            "[{label}] DIVERGENCE: in-proc value {in_proc_val} != default {default_sbs} → the CALL \
             leak is DIRECTLY observable on this engine (read-back alone suffices)"
        );
    } else {
        println!(
            "[{label}] DIVERGENCE: in-proc value {in_proc_val} == default {default_sbs} → hygiene \
             observed via the default*2 probe (the in-proc value coincides with the default here)"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// Live pin-cause parity (charter DoD), mirroring PG's pg_pool_it.rs pin-cause tests.
// -------------------------------------------------------------------------------------------------

async fn run_pin_cause_parity(url: &str, label: &str) {
    // max_size >= 2 so a concurrent checkout has somewhere else to go (pinning is observable).
    let pool = Pool::new(MysqlBackend::new(url), config(2));

    // (A) A bare BEGIN..COMMIT pins exactly one conn (PinCause::Tx) and unpins on commit.
    //     isolation=None per the T5 §22.2 note → the composed SQL is the bare `BEGIN` (MySQL's
    //     START TRANSACTION synonym); the isolation/readonly-composed BEGIN is deferred to S7.
    {
        let mut co = pool.checkout().await.expect("checkout (tx)");
        let id = conn_id(&co);
        co.begin_tx(TxId(1)).await.expect("begin_tx (bare BEGIN)");
        assert_eq!(
            co.pin_state(),
            PinState::PinnedTx(TxId(1)),
            "[{label}] BEGIN pins the tx to TxId(1)"
        );
        assert_eq!(
            co.last_pin_cause(),
            Some(PinCause::Tx),
            "[{label}] pin-cause DoD: BEGIN → PinCause::Tx (RFQ/status-flag authority)"
        );
        // An in-tx statement stays on the SAME physical conn (the tx is pinned to one conn id).
        co.query("SELECT 1", &[]).await.expect("in-tx SELECT 1");
        assert_eq!(
            conn_id(&co),
            id,
            "[{label}] the tx stays pinned to exactly one conn id"
        );
        co.commit_tx().await.expect("commit");
        assert_eq!(
            co.pin_state(),
            PinState::Unpinned,
            "[{label}] COMMIT unpins the conn"
        );
        println!("[{label}] parity (A) BEGIN..COMMIT: PinCause::Tx, one conn, unpinned on commit");
    }

    // (B) A direct `SET SESSION` mid-lease taints. Two proofs:
    //   (b1) the TRACKER AUTHORITY catches it, isolated from the lexer: a RAW backend SET followed by
    //        `take_session_mutated` == true (the same OK-packet tracker T3 proves — SessionTracker).
    //   (b2) through the guarded `Checkout::exec` path BOTH signals fire; `apply_classify` (the SET
    //        lexer rule → Set) composes LAST, so the surfaced Checkout label is `Set` — but the
    //        tracker independently tainted it too. The pin-cause DoD accepts either mutation cause.
    {
        // (b1) tracker authority, lexer-independent.
        let mut raw = pool.backend().connect().await.expect("raw connect");
        pool.backend()
            .simple_query(&mut raw, "SET SESSION sort_buffer_size = 524288")
            .await
            .expect("raw direct SET SESSION");
        assert!(
            pool.backend().take_session_mutated(&mut raw),
            "[{label}] the OK-packet tracker (AUTHORITY) catches a direct SET SESSION"
        );
        drop(raw);

        // (b2) Checkout-level pin-cause.
        let mut co = pool.checkout().await.expect("checkout (set)");
        let default_sbs = read_sbs(&mut co).await;
        co.exec(&format!(
            "SET SESSION sort_buffer_size = {}",
            default_sbs * 2
        ))
        .await
        .expect("direct SET SESSION via exec");
        assert!(
            co.tainted(),
            "[{label}] a direct SET SESSION taints the conn"
        );
        let cause = co.last_pin_cause();
        assert!(
            matches!(cause, Some(PinCause::SessionTracker) | Some(PinCause::Set)),
            "[{label}] direct-SET pin-cause must be SessionTracker or Set, got {cause:?}"
        );
        println!(
            "[{label}] parity (B) direct SET SESSION: tracker authority fired; Checkout cause={cause:?} \
             (lexer `Set` composes last over the tracker's `SessionTracker`)"
        );
    }

    // (C) A plain autocommit SELECT never pins and never taints (the mechanism does not over-pin a
    //     clean read): no tx (status Idle), no tracker mutation, no lexer trigger.
    {
        let mut co = pool.checkout().await.expect("checkout (select)");
        co.query("SELECT 1", &[])
            .await
            .expect("autocommit SELECT 1");
        assert!(
            !co.tainted(),
            "[{label}] a plain autocommit SELECT must NOT taint"
        );
        assert_eq!(
            co.pin_state(),
            PinState::Unpinned,
            "[{label}] a plain SELECT does not pin"
        );
        assert_eq!(
            co.last_pin_cause(),
            None,
            "[{label}] a plain SELECT has NO pin cause"
        );
        println!("[{label}] parity (C) autocommit SELECT: not tainted, Unpinned, cause None");
    }

    println!("[{label}] pin-cause parity PASSED");
}

// -------------------------------------------------------------------------------------------------
// Per-engine entry points (skip cleanly without the env var; run against BOTH where both are set).
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_hard_gate_and_pin_cause_parity() {
    let Ok(url) = std::env::var("FERRO_TEST_MYSQL_URL") else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL unset (mysql_hard_gate_and_pin_cause_parity)");
        return;
    };
    run_hard_gate(&url, "MYSQL").await;
    run_pin_cause_parity(&url, "MYSQL").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_hard_gate_and_pin_cause_parity() {
    let Ok(url) = std::env::var("FERRO_TEST_MARIADB_URL") else {
        eprintln!("skip: FERRO_TEST_MARIADB_URL unset (mariadb_hard_gate_and_pin_cause_parity)");
        return;
    };
    run_hard_gate(&url, "MARIADB").await;
    run_pin_cause_parity(&url, "MARIADB").await;
}
