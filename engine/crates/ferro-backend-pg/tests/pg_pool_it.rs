//! Live `ferro-backend-pg` integration tests (S4 Task 5) against a real Postgres.
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset, so `cargo test --workspace`
//! stays green offline (S2 convention). Point it at the S2 Dockerized Postgres:
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg
//! ```

use std::time::Duration;

use ferro_backend_pg::{PgBackend, Value};
use ferro_pool::backend::{PoolBackend, ResetProfile, TxStatus};
use ferro_pool::config::PoolConfig;
use ferro_pool::error::{Branch, PoolError};
use ferro_pool::pin::{PinCause, PinState, TxId};
use ferro_pool::pool::{Checkout, Pool};

/// Returns the test DSN, or `None` (after printing a skip notice) if `FERRO_TEST_PG_URL` is
/// unset. Every test below returns immediately when this is `None` — that early return IS the
/// skip.
fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// Runs `sql` (a single-row, single-column query) on `co`'s connection and returns the `i32`
/// result. Goes straight at the raw `tokio_postgres::Client` (`PgConn::client` is `pub` for
/// exactly this) since the pool-internal `exec`/`simple_query` surface only reports
/// success/failure, never row data.
async fn query_i32(co: &mut Checkout<PgBackend>, sql: &str) -> i32 {
    let row = co
        .conn_mut()
        .client
        .query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("query {sql:?} failed: {e}"));
    row.get(0)
}

async fn backend_pid(co: &mut Checkout<PgBackend>) -> i32 {
    query_i32(co, "SELECT pg_backend_pid()").await
}

/// Runs `sql` through the GUARDED, row-returning `Checkout::query` (so the M1-S1 RFQ pin AUTHORITY
/// fires after the statement) and returns the first cell as an `i64` (cast the projection to `int8`
/// so it hydrates to `Value::I64`). Used to prove pinning while the RFQ read runs post-statement.
async fn query_first_i64(co: &mut Checkout<PgBackend>, sql: &str) -> i64 {
    let result = co
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("Checkout::query {sql:?} failed: {e:?}"));
    match result
        .rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
    {
        Some(Value::I64(v)) => v,
        other => panic!("expected an i64 cell from {sql:?}, got {other:?}"),
    }
}

/// Runs `sql` through the GUARDED, row-returning `Checkout::query` (the real S6 tx-scoped exec
/// path) and returns the first cell as a `String` (panicking if it is not a text cell). Used to
/// OBSERVE server state (e.g. `current_setting('transaction_isolation')`) via the pool's public
/// query surface rather than the raw client.
async fn query_first_text(co: &mut Checkout<PgBackend>, sql: &str) -> String {
    let result = co
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("Checkout::query {sql:?} failed: {e:?}"));
    match result
        .rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
    {
        Some(Value::Text(s)) => s,
        other => panic!("expected a text cell from {sql:?}, got {other:?}"),
    }
}

/// Runs `sql` through the GUARDED, row-returning `Checkout::query` and returns the first cell as a
/// `bool` (panicking if it is not a bool cell). Used for `IS NULL`-style yes/no probes (M1-S2 Task
/// 3's temp-table hygiene proof).
async fn query_first_bool(co: &mut Checkout<PgBackend>, sql: &str) -> bool {
    let result = co
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("Checkout::query {sql:?} failed: {e:?}"));
    match result
        .rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
    {
        Some(Value::Bool(b)) => b,
        other => panic!("expected a bool cell from {sql:?}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_checkout_select1_release() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(2));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        let one = query_i32(&mut co, "SELECT 1").await;
        assert_eq!(one, 1);
        backend_pid(&mut co).await
        // `co` drops at the end of this block -> released back to the pool.
    };

    let pid2 = {
        let mut co = pool.checkout().await.expect("checkout again");
        backend_pid(&mut co).await
    };

    assert_eq!(
        pid1, pid2,
        "expected the released connection to be reused, not a fresh one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_tx_pins_single_backend_pid() {
    let Some(url) = test_url() else {
        return;
    };
    // v2/m1: max_size >= 2 so a concurrent checkout has somewhere else to go, proving pinning
    // actually kept it off the pinned connection (rather than there being nothing else to give).
    let pool = Pool::new(PgBackend::new(url), config(2));

    let mut a = pool.checkout().await.expect("checkout A");
    a.begin_tx(TxId(1)).await.expect("begin tx on A");
    assert_eq!(a.pin_state(), PinState::PinnedTx(TxId(1)));
    assert_eq!(a.last_pin_cause(), Some(PinCause::Tx));

    let pid_a1 = backend_pid(&mut a).await;
    let pid_a2 = backend_pid(&mut a).await;
    assert_eq!(
        pid_a1, pid_a2,
        "both statements inside the tx must stay pinned to the same backend pid"
    );

    // While A is still pinned, a concurrent checkout must land on a DIFFERENT backend pid --
    // proof that pinning kept the load off the pinned connection rather than there being no
    // other connection to hand out.
    let mut b = pool.checkout().await.expect("checkout B while A is pinned");
    let pid_b = backend_pid(&mut b).await;
    assert_ne!(
        pid_a1, pid_b,
        "a concurrent checkout must not be handed the pinned tx connection"
    );
    drop(b);

    a.commit_tx().await.expect("commit A");
    assert_eq!(a.pin_state(), PinState::Unpinned);
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_release_hygiene_leaves_conn_clean() {
    let Some(url) = test_url() else {
        return;
    };
    // max_size = 1: the only connection this pool will ever create MUST be the one reused below,
    // so the defensive-rollback assertion isn't at the mercy of which idle conn gets popped.
    let pool = Pool::new(PgBackend::new(url), config(1));

    {
        let mut co = pool.checkout().await.expect("checkout");
        co.begin_tx(TxId(42)).await.expect("begin tx");
        co.exec("CREATE TEMP TABLE ferro_s4_hygiene_probe (id int)")
            .await
            .expect("create temp table inside tx");
        // Dropped here WITHOUT commit/rollback: `tx_open` stays set on release, so the *next*
        // checkout performs the defensive ROLLBACK (v2/B1) before handing this conn out again.
        // DDL is transactional in Postgres, so rolling back also undoes the temp table create.
    }

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (defensive rollback should have run)");
    let result = co2
        .conn_mut()
        .client
        .simple_query("SELECT count(*) FROM ferro_s4_hygiene_probe")
        .await;
    assert!(
        result.is_err(),
        "temp table created inside the uncommitted tx should be gone after the defensive \
         ROLLBACK, got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_killed_backend_evicted_no_retry() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url.clone()), config(2));

    let mut a = pool.checkout().await.expect("checkout A");
    let pid_a = backend_pid(&mut a).await;

    // Deterministic kill from a SEPARATE connection (v2/M4): `pg_terminate_backend(pg_backend_pid())`
    // races its own result on the killing connection, so use a second, independent connection.
    let (killer, killer_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect killer");
    tokio::spawn(async move {
        let _ = killer_conn.await;
    });
    let terminated: bool = killer
        .query_one("SELECT pg_terminate_backend($1)", &[&pid_a])
        .await
        .expect("pg_terminate_backend")
        .get(0);
    assert!(terminated, "pg_terminate_backend should report success");

    // Detect death via a ROUND TRIP on A's next use (v2/M4), not via `is_closed()` timing: the
    // backend needs a moment to actually die once the terminate signal lands, so retry briefly.
    let mut last_err = None;
    let mut detected = false;
    for _ in 0..100 {
        match a.exec("SELECT 1").await {
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(e) => {
                last_err = Some(e);
                detected = true;
                break;
            }
        }
    }
    assert!(
        detected,
        "expected the round trip to eventually detect the killed backend"
    );
    assert_eq!(
        last_err,
        Some(PoolError::ConnectionLost),
        "a killed backend must surface as ConnectionLost (Retryable), never a silent success"
    );
    assert_eq!(
        last_err.as_ref().map(PoolError::taxonomy_branch),
        Some(Branch::Retryable),
        "ConnectionLost must classify as Retryable -- the arm the pg_syntax_error_... test below \
         proves is distinct from the Backend/NonRetryable arm"
    );

    // Charter rule 3 (no transparent retry): the loop above made exactly one *user* statement
    // attempt per iteration and stopped at the first error -- there is no hidden retry loop
    // inside `exec`/`checkout` that could have silently re-issued "SELECT 1" on a fresh
    // connection in place of the failed one.

    drop(a);
    // Give the connection-driver task a moment to flip its `closed` flag, so the next checkout's
    // eviction check isn't itself racing that async update.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut b = pool
        .checkout()
        .await
        .expect("checkout after the kill should reconnect fresh");
    let pid_b = backend_pid(&mut b).await;
    assert_ne!(
        pid_a, pid_b,
        "the pool must have evicted the dead connection and reconnected to a fresh backend pid"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_max_lifetime_recycles_live() {
    let Some(url) = test_url() else {
        return;
    };
    let mut cfg = config(1);
    cfg.max_lifetime = Duration::from_millis(50);
    let pool = Pool::new(PgBackend::new(url), cfg);

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        backend_pid(&mut co).await
    };

    tokio::time::sleep(Duration::from_millis(150)).await;

    let pid2 = {
        let mut co = pool
            .checkout()
            .await
            .expect("checkout after max_lifetime elapsed");
        backend_pid(&mut co).await
    };

    assert_ne!(
        pid1, pid2,
        "a connection past max_lifetime should be recycled (fresh pid), not reused"
    );
}

/// IMPORTANT 2 (S4 review), UPDATED (M1-S4 M4b whole-branch final review): the FATAL-severity-vs-
/// DbError classification in `conn.rs`'s `simple_query` had zero coverage of the non-fatal arm --
/// only the `ConnectionLost`/`Retryable` arm (a killed backend, `pg_killed_backend_evicted_no_retry`
/// above) was exercised live. A genuine SQL error (severity ERROR, not FATAL/PANIC) must classify as
/// a NonRetryable statement-level error, never be misclassified as the retryable `ConnectionLost` --
/// and the connection itself must stay alive and reusable afterward, since nothing about the session
/// actually ended.
///
/// M4b changed `simple_query`'s non-fatal arm from the coarse `PoolError::Backend(String)` (SQLSTATE
/// discarded) to the SQLSTATE-preserving `PoolError::Sql{code, branch, sqlstate, message}` --
/// `Checkout::query`'s path (`error_map::map`) already produced this shape; `simple_query` now
/// matches it exactly, which is what makes a commit-time `40001`/`40P01` classify correctly (see
/// `ferrod`'s `chaos_fate_it.rs`'s `commit_time_serialization_write_skew_is_retryable_live`). This
/// test's core property -- NonRetryable, never the retryable `ConnectionLost`, connection survives --
/// is unchanged; only the exact `PoolError` variant + the now-preserved SQLSTATE are asserted.
#[tokio::test(flavor = "multi_thread")]
async fn pg_syntax_error_classifies_as_sql_nonretryable() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let mut co = pool.checkout().await.expect("checkout");

    let err = co
        .exec("SELECT * FROM nonexistent_table_xyz")
        .await
        .expect_err("querying a nonexistent table must fail");
    match &err {
        PoolError::Sql { sqlstate, .. } => assert_eq!(
            sqlstate.as_deref(),
            Some("42P01"),
            "undefined table must preserve its SQLSTATE, got {err:?}"
        ),
        other => panic!(
            "a genuine SQL error (undefined table) must classify as PoolError::Sql, got {other:?}"
        ),
    }
    assert_eq!(
        err.taxonomy_branch(),
        Branch::NonRetryable,
        "a statement-level SQL error must be NonRetryable -- misclassifying it as the retryable \
         ConnectionLost would license a caller to blindly retry a query that will always fail"
    );

    // A second, independent flavor of statement-level error (a syntax error) must classify the
    // same way -- this isn't specific to "undefined table".
    let err2 = co
        .exec("SEL ECT 1")
        .await
        .expect_err("a syntax error must fail");
    match &err2 {
        PoolError::Sql { sqlstate, .. } => assert_eq!(
            sqlstate.as_deref(),
            Some("42601"),
            "a syntax error must preserve its SQLSTATE, got {err2:?}"
        ),
        other => panic!("a syntax error must also classify as PoolError::Sql, got {other:?}"),
    }
    assert_eq!(err2.taxonomy_branch(), Branch::NonRetryable);

    // The connection itself must still be alive and usable afterward: proof the errors above were
    // statement-level, not connection-level. A FATAL/PANIC (ConnectionLost) misclassification
    // would have ended the session and this would fail.
    let one = query_i32(&mut co, "SELECT 1").await;
    assert_eq!(
        one, 1,
        "the connection must still be alive and usable after plain statement-level SQL errors"
    );
}

/// S6 live: `begin_tx_with` a COMPOSED BEGIN opens the tx at the composed isolation level (observed
/// via the guarded `Checkout::query`), and the pinned conn keeps the SAME `pg_backend_pid` across
/// statements inside the tx.
#[tokio::test(flavor = "multi_thread")]
async fn pg_begin_tx_with_composed_isolation_and_same_pid() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let mut co = pool.checkout().await.expect("checkout");
    co.begin_tx_with(TxId(1), "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY")
        .await
        .expect("begin_tx_with a composed BEGIN");
    assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(1)));
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));

    // `current_setting('transaction_isolation')` is the query-friendly equivalent of `SHOW
    // transaction_isolation` — it reflects the composed level inside the open tx.
    let pid_before = backend_pid(&mut co).await;
    let iso = query_first_text(&mut co, "SELECT current_setting('transaction_isolation')").await;
    assert_eq!(
        iso, "serializable",
        "the composed BEGIN must set the tx isolation to serializable, got {iso:?}"
    );
    let pid_after = backend_pid(&mut co).await;
    assert_eq!(
        pid_before, pid_after,
        "every statement inside the tx must stay pinned to the SAME backend pid"
    );

    co.rollback_tx().await.expect("rollback the read-only tx");
    assert_eq!(co.pin_state(), PinState::Unpinned);
}

/// S6 live: `tx_control` runs engine-composed SAVEPOINT/RELEASE on a pinned conn via the UNGUARDED
/// passthrough, and the conn stays on the same backend pid throughout. Since M1-S8a the guarded
/// `query` ALSO admits a savepoint inside an open transaction (SPEC §22.2 (r)) — the tail of this
/// test pins the difference that remains: boundary verbs and compound savepoints stay refused.
#[tokio::test(flavor = "multi_thread")]
async fn pg_tx_control_savepoint_roundtrip() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let mut co = pool.checkout().await.expect("checkout");
    co.begin_tx(TxId(9)).await.expect("begin tx");
    let pid = backend_pid(&mut co).await;

    co.tx_control("SAVEPOINT s1")
        .await
        .expect("SAVEPOINT s1 via tx_control");
    co.tx_control("RELEASE s1")
        .await
        .expect("RELEASE s1 via tx_control");

    // A fresh savepoint + ROLLBACK TO also succeeds on the pinned conn.
    co.tx_control("SAVEPOINT s2")
        .await
        .expect("SAVEPOINT s2 via tx_control");
    co.tx_control("ROLLBACK TO s2")
        .await
        .expect("ROLLBACK TO s2 via tx_control");

    assert_eq!(
        backend_pid(&mut co).await,
        pid,
        "the savepoint statements must all run on the SAME pinned backend pid"
    );

    // M1-S8a (SPEC §22.2 (r)): the guarded `query()` no longer rejects a savepoint on a conn that
    // HAS a transaction open — that is the Doctrine nested-transaction passthrough. What it still
    // rejects on the very same pinned conn is a transaction-BOUNDARY verb (the pin authority), and
    // a COMPOUND statement leading with a savepoint (which the text protocol would run whole).
    co.query("SAVEPOINT s3", &[])
        .await
        .expect("M1-S8a: a savepoint passes through the guarded query() inside a transaction");
    assert!(
        co.tx_open(),
        "a savepoint must not close the transaction (PG's RFQ byte does not flip)"
    );
    for refused in ["COMMIT", "ROLLBACK", "BEGIN", "SAVEPOINT s4; COMMIT"] {
        assert!(
            matches!(co.query(refused, &[]).await, Err(PoolError::Unsupported(_))),
            "the guarded query() must still reject {refused:?} on a pinned conn"
        );
    }
    assert_eq!(
        backend_pid(&mut co).await,
        pid,
        "the passthrough savepoint and every refusal stayed on the SAME pinned backend pid"
    );

    co.commit_tx().await.expect("commit the tx");
}

/// **M1-S8a review F1, live on PG: a savepoint passthrough must NOT taint — and the proof is a
/// prepared statement that SURVIVES the recycle.**
///
/// Savepoint verbs are not on `ferro-classify`'s safe list, so feeding a passthrough savepoint to
/// the assist lexer returns `Unknown` under the default `pin_on_unknown` and TAINTS. On Postgres
/// that is not "one extra hygiene reset" — it is the LESS safe direction, and this test is what
/// makes that concrete: a taint selects `ResetProfile::Full` (`DISCARD ALL`), whose `DEALLOCATE ALL`
/// is the exact trigger for the tokio-postgres typeinfo-cache poisoning recorded in SPEC §22.2 (m)
/// and ticketed in `docs/followups/2026-08-06-discard-all-typeinfo-cache-poisoning.md`. Doctrine
/// emits `SAVEPOINT DOCTRINE_<n>` for EVERY nested transaction, so tainting here would poison
/// essentially every pooled PG connection that has ever served one.
///
/// **Why a prepared statement is the right instrument.** `ResetProfile::Targeted` is DEFINED as
/// `DISCARD ALL` minus its two prepare-affecting statements (`DEALLOCATE ALL`, which destroys them,
/// and `DISCARD PLANS`, which only drops their cached plans — measured on PG 17), so
/// a server-side prepared statement is the one observable that distinguishes the two profiles at
/// recycle. It is created through `tx_control` — the raw, UNGUARDED text leaf, which never runs the
/// assist lexer — because a `PREPARE` through `exec()` would taint as `PinCause::Prepare` all by
/// itself and make the profile question moot. It is read back through the raw client's TEXT
/// protocol, so the read cannot itself re-prepare anything.
///
/// **Falsifiable, and falsified.** With the `apply_classify_for` skip mutated away
/// (`SavepointPassthrough => self.apply_classify(sql)`) this goes RED at the first `tainted()`
/// assertion; with those assertions also removed, the `EXECUTE zz_p` below fails with SQLSTATE
/// `26000` ("prepared statement \"zz_p\" does not exist") — measured, `DISCARD ALL` ran.
#[tokio::test(flavor = "multi_thread")]
async fn pg_s8a_savepoint_passthrough_does_not_taint_and_keeps_prepares_across_recycle() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        // Raw, unguarded, un-lexed: create the observable without tainting.
        co.tx_control("DEALLOCATE ALL").await.expect("clean slate");
        co.tx_control("PREPARE zz_p AS SELECT 1")
            .await
            .expect("PREPARE zz_p via the unguarded tx_control leaf");
        assert!(
            !co.tainted(),
            "pre-condition: the raw tx_control PREPARE must not taint, or this test would be \
             measuring the wrong thing"
        );

        co.begin_tx(TxId(41)).await.expect("begin tx");
        let pid = backend_pid(&mut co).await;

        // THE STATEMENTS UNDER TEST — Doctrine's own nested-transaction SQL, verbatim, through the
        // guarded `query()` entry (the M1-S8a passthrough).
        for sql in [
            "SAVEPOINT DOCTRINE_1",
            "RELEASE SAVEPOINT DOCTRINE_1",
            "SAVEPOINT DOCTRINE_2",
            "ROLLBACK TO SAVEPOINT DOCTRINE_2",
        ] {
            co.query(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("passthrough {sql:?}: {e:?}"));
            assert!(
                !co.tainted(),
                "F1: a passthrough savepoint must leave the conn UNTAINTED: {sql:?}"
            );
            assert_eq!(
                co.last_pin_cause(),
                Some(PinCause::Tx),
                "the cause stays the RFQ tx AUTHORITY, never the lexer's Unknown: {sql:?}"
            );
        }

        co.commit_tx().await.expect("commit the tx");
        assert!(
            !co.tainted(),
            "the whole nested-transaction round trip left the conn untainted"
        );
        pid
        // `co` drops -> back to idle UNTAINTED. PG's `clean_reset_profile()` is `Some(Targeted)`, so
        // a reset still runs at the next checkout -- the TARGETED one, which spares prepares.
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (recycle runs)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid — on a fresh connection `zz_p` would not exist \
         either, which would make the assertion below a false RED"
    );

    // TEXT protocol read-back: this cannot itself create a prepared statement.
    co2.conn_mut()
        .client
        .simple_query("EXECUTE zz_p")
        .await
        .unwrap_or_else(|e| {
            // Print the SQLSTATE, not just "db error": the whole point is that the failure mode is
            // a bare `26000` (prepared statement does not exist), i.e. `DEALLOCATE ALL` ran.
            let code = e.code().map(|c| c.code().to_string());
            panic!(
                "the TARGETED profile must have preserved `zz_p` across the recycle, but EXECUTE \
                 failed (sqlstate {code:?}: {e}) — DISCARD ALL ran, i.e. the passthrough savepoint \
                 tainted the conn"
            )
        });

    co2.tx_control("DEALLOCATE ALL").await.expect("cleanup");
}

/// **Charter rule 6, live on PG: the F1 skip is per-statement, NOT a lexer off-switch — and the
/// taint it leaves behind really does route the recycle to the FULL profile.**
///
/// The savepoint passthrough opts out of `apply_classify` by the GUARD's verdict, so anything else
/// running on the same checkout — including inside the same transaction, immediately after a
/// savepoint — is still lexed and still taints. Without this, F1's fix would have widened into a
/// cross-tenant session-state leak.
///
/// **This is the exact MIRROR of
/// `pg_s8a_savepoint_passthrough_does_not_taint_and_keeps_prepares_across_recycle`, and it has to
/// be.** Both PG reset profiles run `RESET ALL` (`Targeted` is DEFINED as `DISCARD ALL` minus
/// `DEALLOCATE ALL` / `DISCARD PLANS`), so "the next tenant does not inherit the `search_path`"
/// cannot tell Full from Targeted and would stay GREEN under any taint-routing defect — it was the
/// closing claim of an earlier version of this test and it was unearned. The ONE observable that
/// distinguishes the two profiles at recycle is a server-side prepared statement: the sibling test
/// proves untainted -> `zz_p` SURVIVES, this one proves tainted -> `zz_skip_p` is GONE.
///
/// The statement is created through `tx_control` — the raw, UNGUARDED text leaf, which never runs
/// the assist lexer — because a `PREPARE` through `exec()` would taint as `PinCause::Prepare` all
/// by itself and make the routing question moot. Nothing else in this test can deallocate it: the
/// tx is COMMITted (so the pool's pre-reset `ROLLBACK` never runs) and neither `SET` nor a
/// savepoint touches prepared statements. It is read back through the raw client's TEXT protocol,
/// so the read cannot itself re-prepare anything.
///
/// **Falsifiable, and falsified.** With `ferro-pool`'s `pool.rs` profile selection mutated so a
/// tainted conn no longer selects `Full` (`let profile = self.inner.backend.clean_reset_profile();`)
/// the `EXECUTE zz_skip_p` below SUCCEEDS and this test goes RED — measured, `Targeted` ran and the
/// prepare survived.
#[tokio::test(flavor = "multi_thread")]
async fn pg_s8a_savepoint_skip_does_not_disable_the_lexer_for_other_statements() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        // Raw, unguarded, un-lexed: create the profile-distinguishing observable without tainting.
        co.tx_control("DEALLOCATE ALL").await.expect("clean slate");
        co.tx_control("PREPARE zz_skip_p AS SELECT 1")
            .await
            .expect("PREPARE zz_skip_p via the unguarded tx_control leaf");
        assert!(
            !co.tainted(),
            "pre-condition: the raw tx_control PREPARE must not taint, or this test would be \
             measuring the wrong thing"
        );

        co.begin_tx(TxId(43)).await.expect("begin tx");
        co.query("SAVEPOINT DOCTRINE_1", &[])
            .await
            .expect("passthrough savepoint");
        assert!(!co.tainted(), "the savepoint itself must not taint");

        co.exec("SET search_path TO ferro_test_s8a")
            .await
            .expect("SET inside the same transaction");
        assert!(
            co.tainted(),
            "charter rule 6: a user SET inside the SAME tx, right after a savepoint, must STILL \
             taint"
        );
        assert_eq!(co.last_pin_cause(), Some(PinCause::Set), "pin-cause DoD");

        // A savepoint AFTER the taint must never CLEAR it (the skip is a no-op, not a reset).
        co.query("SAVEPOINT DOCTRINE_2", &[])
            .await
            .expect("second passthrough savepoint");
        assert!(
            co.tainted(),
            "a savepoint must never clear a taint another statement already set"
        );

        co.commit_tx().await.expect("commit the tx");
        assert!(
            co.tainted(),
            "the taint must survive COMMIT all the way to release — it is what selects the FULL \
             profile at the next checkout"
        );
        backend_pid(&mut co).await
        // `co` drops -> back to idle TAINTED -> the recycle below must run ResetProfile::Full.
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (FULL reset runs)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid — on a fresh connection `zz_skip_p` would not \
         exist either, which would make the assertion below a false GREEN"
    );

    // THE PROFILE-DISTINGUISHING ASSERTION. TEXT protocol read-back: cannot itself prepare anything.
    let err = co2
        .conn_mut()
        .client
        .simple_query("EXECUTE zz_skip_p")
        .await
        .expect_err(
            "the taint must have selected the FULL profile at recycle, whose DISCARD ALL runs \
             DEALLOCATE ALL — `zz_skip_p` SURVIVING means the TARGETED profile ran instead, i.e. \
             the tainted conn was not routed to Full",
        );
    // `tokio_postgres::Error`'s own `Display` is a fixed "db error" label; the SQLSTATE and the real
    // message live on the wrapped `DbError`. Assert the SQLSTATE so a *different* failure (a dropped
    // connection, a syntax error) cannot masquerade as the proof.
    assert_eq!(
        err.code().map(|c| c.code().to_string()).as_deref(),
        Some("26000"),
        "expected 26000 (invalid_sql_statement_name — 'prepared statement \"zz_skip_p\" does not \
         exist'), got {err:?}"
    );

    // Secondary, and deliberately NOT the proof: both profiles run RESET ALL, so this only shows
    // that SOME reset ran. It is kept because the leak class it names (the next tenant inheriting a
    // `search_path`) is the reason charter rule 6 matters here.
    let search_path = query_first_text(&mut co2, "SELECT current_setting('search_path')").await;
    assert_ne!(
        search_path, "ferro_test_s8a",
        "a reset must have run at recycle, so the next tenant must NOT inherit the search_path; \
         got {search_path:?}"
    );

    co2.tx_control("DEALLOCATE ALL").await.expect("cleanup");
}

/// S6 live: the out-of-band `cancel_handle` cancels an in-flight `Checkout::query` (`pg_sleep`),
/// which errors with SQLSTATE 57014 (query_canceled) — NOT a hang and NOT a silent success — and
/// the pinned conn is usable again after a `rollback_tx`. This is the deadline/abort mechanism the
/// TX actor (next task) uses: grab the handle BEFORE the query, fire the cancel from a side task.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cancel_handle_cancels_in_flight_query() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let mut co = pool.checkout().await.expect("checkout");
    co.begin_tx(TxId(5)).await.expect("begin tx");

    // Grab the cancel handle BEFORE starting the query — it borrows nothing from `co`, so it can
    // fire while `co.query(..)` (which holds `&mut co`) is still live.
    let cancel = co.cancel_handle();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        // Fire via the `Cancel` trait — exactly what the TX actor does with `B::CancelHandle`.
        ferro_pool::backend::Cancel::cancel(cancel).await;
    });

    // A ~2s statement via the GUARDED query() path (the int8 projection keeps the result column an
    // M0-supported type while `pg_sleep` runs in FROM). The out-of-band cancel makes it error 57014.
    let res = co.query("SELECT 42::int8 FROM pg_sleep(2)", &[]).await;
    canceller.await.expect("canceller task");

    match res {
        Err(PoolError::Sql { sqlstate, .. }) => assert_eq!(
            sqlstate.as_deref(),
            Some("57014"),
            "an out-of-band cancel must surface as query_canceled (57014)"
        ),
        other => panic!("expected a 57014 Sql error from the cancelled query, got {other:?}"),
    }

    // The tx is now in an aborted state; rollback_tx brings the conn back to usable.
    co.rollback_tx().await.expect("rollback after cancel");
    let one = query_i32(&mut co, "SELECT 1").await;
    assert_eq!(
        one, 1,
        "the pinned conn must be usable again after cancel + rollback_tx"
    );
}

// -------------------------------------------------------------------------------------------------
// M1-S1 Task 4 LIVE ACCEPTANCE: the RFQ status byte (I/T/E) off the real wire IS the pin authority.
// -------------------------------------------------------------------------------------------------

/// S1 acceptance (a) + (d): `begin_tx_with("BEGIN")` → two in-tx statements → `commit_tx`, all on
/// the SAME `pg_backend_pid` (pinned by the real RFQ `T`), pin-cause `Tx`, and after COMMIT the RFQ
/// is `I` → `!tx_open`. The two statements ride the GUARDED `Checkout::query`, so the RFQ authority
/// runs after each and must KEEP `tx_open` set.
#[tokio::test(flavor = "multi_thread")]
async fn pg_rfq_tx_lifecycle_same_pid_and_unpins_on_commit() {
    let Some(url) = test_url() else {
        return;
    };
    // max_size >= 2 so a second backend connection is available; pid1 == pid2 below is tautological
    // at THIS (single-held-Checkout) level, not proof of multiplexed pinning — that genuine proof
    // (one tx pins one backend pid across requests going through the real session/multiplexing
    // layer) lives in `ferrod::tests::tx_it::tx_pins_one_backend_pid`. This test's real job is the
    // RFQ-authority checks below (tx_open/PinnedTx/cause==Tx/the raw I/T/E bytes).
    let pool = Pool::new(PgBackend::new(url), config(2));
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx_with(TxId(1), "BEGIN").await.expect("begin");
    assert!(
        co.tx_open(),
        "real RFQ T after BEGIN => tx_open (the authority)"
    );
    assert!(!co.tainted(), "a clean BEGIN taints nothing");
    assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(1)));
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx), "pin-cause DoD (d)");
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::InTx,
        "the RFQ byte reads T inside the tx"
    );

    // Two in-tx statements via the guarded query() path — RFQ authority runs after each; both must
    // land on the SAME pinned backend pid, and tx_open must stay set.
    let pid1 = query_first_i64(&mut co, "SELECT pg_backend_pid()::int8").await;
    assert!(
        co.tx_open(),
        "still pinned after the first in-tx statement (RFQ T)"
    );
    let pid2 = query_first_i64(&mut co, "SELECT pg_backend_pid()::int8").await;
    assert_eq!(
        pid1, pid2,
        "both in-tx statements ran on the SAME pinned backend (pid {pid1} vs {pid2})"
    );
    assert!(
        co.tx_open() && !co.tainted(),
        "a clean, still-open tx after two statements"
    );
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(1)),
        "the real TxId is preserved across the in-tx statements"
    );

    co.commit_tx().await.expect("commit");
    assert!(!co.tx_open(), "real RFQ I after COMMIT => !tx_open");
    assert!(!co.tainted(), "a clean commit leaves no taint");
    assert_eq!(co.pin_state(), PinState::Unpinned, "commit unpins");
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Idle,
        "the RFQ byte reads I after COMMIT"
    );
}

/// S1 acceptance (b): a FAILED statement mid-tx (`SELECT 1/0`, SQLSTATE 22012) aborts the tx block
/// → the real RFQ flips to `E`; the pin is HELD (`tx_open && tainted`, the real `TxId` never
/// clobbered) until an explicit `rollback_tx`, after which the RFQ is `I` again. The
/// `is_err() && tx_open` guard makes `tainted` hold regardless of any Err-arm atomic staleness.
#[tokio::test(flavor = "multi_thread")]
async fn pg_rfq_failed_stmt_holds_pin_until_rollback() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.begin_tx(TxId(2)).await.expect("begin");
    assert!(co.tx_open());
    assert_eq!(pool.backend().tx_status(co.conn()), TxStatus::InTx);

    // A failing statement mid-tx aborts the block. The guarded query() reads the RFQ after; on the
    // Err arm the guard forces the taint even if the atomic were momentarily stale.
    let _err = co
        .query("SELECT 1 / 0", &[])
        .await
        .expect_err("SELECT 1/0 must error (division_by_zero)");
    assert!(
        co.tx_open(),
        "a failed in-tx statement keeps the tx OPEN (aborted block)"
    );
    assert!(
        co.tainted(),
        "a failed in-tx statement TAINTS — the conn needs a ROLLBACK before reuse"
    );
    assert_eq!(
        co.pin_state(),
        PinState::PinnedTx(TxId(2)),
        "the real pool-opened TxId is NEVER clobbered by the failure (E)"
    );
    assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));
    // NOTE: we do NOT assert the raw `tx_status()` byte here. This is an Err arm, where the RFQ
    // atomic is stale-untrustworthy (Err surfaces at `ErrorResponse` before the trailing
    // `ReadyForQuery` is decoded — fragile under response fragmentation on non-loopback networks).
    // The real safety property — the pin is HELD armed-for-cleanup — is the `tx_open()`/`tainted()`
    // pair asserted just above, which the unconditional Err-arm fail-safe guarantees regardless of
    // any atomic staleness. The Ok-arm byte read after ROLLBACK below stays (post-drain, trustworthy).

    // The pin is held until an EXPLICIT rollback (the engine never auto-rolls-back a user tx).
    co.rollback_tx().await.expect("rollback the aborted tx");
    assert!(!co.tx_open(), "real RFQ I after ROLLBACK => !tx_open");
    assert_eq!(co.pin_state(), PinState::Unpinned, "rollback unpins");
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Idle,
        "the RFQ byte reads I after ROLLBACK"
    );
}

/// S1 acceptance (c): an AUTOCOMMIT `Checkout::query("SELECT 1")` (no BEGIN) NEVER pins — the real
/// RFQ stays `I` → `!tx_open`, `Unpinned`, no `Tx` pin-cause.
#[tokio::test(flavor = "multi_thread")]
async fn pg_rfq_autocommit_never_pins() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.query("SELECT 1", &[]).await.expect("autocommit query");
    assert!(
        !co.tx_open(),
        "an autocommit query NEVER opens a tx (RFQ stays I)"
    );
    assert!(!co.tainted());
    assert_eq!(co.pin_state(), PinState::Unpinned);
    assert_eq!(
        co.last_pin_cause(),
        None,
        "no tx observed => no Tx pin-cause"
    );
    assert_eq!(
        pool.backend().tx_status(co.conn()),
        TxStatus::Idle,
        "the RFQ byte reads I after an autocommit statement"
    );
}

/// LIVE end-to-end proof of the Err-arm cross-tenant-leak fix: `exec` forwards a multi-statement
/// batch to `batch_execute`, and `is_bare_tx_control` only checks the LEADING keyword — so
/// `SELECT 1; BEGIN; SELECT 1/0` PASSES the guard, opens a tx mid-batch from autocommit, then errors
/// (`22012`), leaving an OPEN, ABORTED tx on the pooled conn. The unconditional Err-arm fail-safe
/// must arm the checkout-time cleanup (`tx_open && tainted`) so the NEXT tenant gets a CLEAN conn —
/// never an inherited `25P02`.
///
/// This is an end-to-end GREEN proof; it does NOT reliably go RED against the pre-fix conditional
/// guard on LOOPBACK. On localhost the connection task decodes the trailing `ReadyForQuery(E)`
/// before the pool reads the atomic, so the pre-fix `apply_tx_status(Failed)` already sets
/// `tx_open=true` and the old conditional guard accidentally fires. The leak needs the Err-arm
/// atomic to be stale-`Idle` (`ErrorResponse`-then-`RFQ` fragmented across reads on a real network)
/// — that condition is reproduced DETERMINISTICALLY in the fake unit test
/// `rfq_pin::err_arm_forces_cleanup_even_when_status_reads_idle`, which is the RED→GREEN guard.
#[tokio::test(flavor = "multi_thread")]
async fn pg_rfq_err_arm_batch_leak_is_cleaned_before_reuse() {
    let Some(url) = test_url() else {
        return;
    };
    // max_size=1 so the NEXT checkout is GUARANTEED to reuse the same (possibly-poisoned) conn.
    let pool = Pool::new(PgBackend::new(url), config(1));

    {
        let mut co = pool.checkout().await.expect("checkout");
        // A batch whose LEADING keyword (SELECT) passes the guard, but which opens a tx mid-batch
        // then errors — the exact cross-tenant-leak shape.
        let err = co
            .exec("SELECT 1; BEGIN; SELECT 1/0")
            .await
            .expect_err("the mid-batch division-by-zero must error");
        // The Err-arm fail-safe armed the cleanup, REGARDLESS of the stale Err-arm RFQ byte.
        assert!(
            co.tx_open(),
            "the Err-arm fail-safe must arm the defensive ROLLBACK (possibly-open tx), got err {err:?}"
        );
        assert!(
            co.tainted(),
            "the Err-arm fail-safe must arm the DISCARD ALL reset"
        );
        // `co` drops here -> returns to the idle stack with tx_open && tainted set.
    }

    // The NEXT checkout recycles the conn (ROLLBACK then DISCARD ALL) BEFORE handing it out, so it
    // is CLEAN: a fresh autocommit SELECT succeeds and the RFQ reads Idle — no inherited aborted tx.
    let mut co2 = pool.checkout().await.expect("checkout again");
    let one = query_i32(&mut co2, "SELECT 1").await;
    assert_eq!(
        one, 1,
        "the next tenant must inherit a CLEAN conn, not an aborted tx (25P02)"
    );
    assert_eq!(
        pool.backend().tx_status(co2.conn()),
        TxStatus::Idle,
        "the recycled conn is Idle — the mid-batch tx was rolled back before reuse"
    );
}

// -------------------------------------------------------------------------------------------------
// M1-S2 Task 3 LIVE: the assist lexer (`ferro-classify`) taints protocol-invisible session
// mutations, closing the S1-deferred SET search_path/LISTEN/temp/advisory leaks. Every test below
// builds a `max_size=1` pool so the SAME backend connection is guaranteed to come back at the next
// checkout, and asserts `pg_backend_pid()` is IDENTICAL across both checkouts before trusting the
// hygiene assertion that follows — without that pid assertion a fresh/different connection (which
// also starts at PG's defaults) would make the test a false green even if the taint/reset never ran.
// -------------------------------------------------------------------------------------------------

/// (a)+(b): a non-local `SET` is PROTOCOL-INVISIBLE to the RFQ byte (it stays `Idle`/`Unpinned`) —
/// only the assist lexer catches it as `PinCause::Set`. This closes the S1-deferred `SET
/// search_path` leak: without `apply_classify` wiring the connection would never be `tainted`, the
/// checkout-time recycle would skip `DISCARD ALL`, and the NEXT tenant on this pooled connection
/// would inherit `ferro_test_s2` on its `search_path`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_classify_set_search_path_taints_and_hygiene_resets_on_recycle() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("SET search_path TO ferro_test_s2")
            .await
            .expect("SET search_path");

        // (a) the assist signal fires while the RFQ authority stays exactly as an ordinary
        // autocommit statement would leave it.
        assert!(!co.tx_open(), "a SET never opens an RFQ transaction");
        assert_eq!(co.pin_state(), PinState::Unpinned);
        assert!(co.tainted(), "a non-local SET must taint (assist signal)");
        assert_eq!(co.last_pin_cause(), Some(PinCause::Set), "pin-cause DoD");

        backend_pid(&mut co).await
        // `co` drops here -> returns to idle with tainted=true; the recycle (DISCARD ALL) runs at
        // the START of the NEXT checkout, not here (v2/B1 recycle-on-next-checkout model).
    };

    // (b) hygiene end-to-end: the SAME connection (asserted below) comes back recycled.
    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (recycle should have run DISCARD ALL)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (also starting at \
         the default search_path) would make the assertion below a false green"
    );

    let search_path = query_first_text(&mut co2, "SELECT current_setting('search_path')").await;
    assert_ne!(
        search_path, "ferro_test_s2",
        "DISCARD ALL at recycle must have reset search_path away from what the tainted \
         connection set it to; got {search_path:?}"
    );
}

/// (c): a session-scoped advisory lock function taints `PinCause::AdvisoryLock`. The release proof
/// is checked from an INDEPENDENT second connection (a fresh raw `tokio_postgres` client, NOT the
/// pooled one) asserting `pg_try_advisory_lock(1)` returns `true` — a same-session re-acquire on
/// the pooled connection would trivially succeed too (session advisory locks are re-entrant within
/// the SAME session), so that would be a false green; only a genuinely DIFFERENT PG backend session
/// successfully acquiring the same lock id proves the pooled session's hold was actually released
/// by `DISCARD ALL` at recycle.
#[tokio::test(flavor = "multi_thread")]
async fn pg_classify_advisory_lock_taints_and_released_by_recycle() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url.clone()), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        // `pg_advisory_lock` returns `void` (PG OID 2278), outside the M0 row-hydration scalar set
        // (`rowmap::oid_to_tag`), so this goes through `exec()` (which discards any result via
        // `simple_query`/`batch_execute`) rather than the row-returning `query()` -- `apply_classify`
        // runs identically in both, so the assist-signal behavior under test is unaffected.
        co.exec("SELECT pg_advisory_lock(1)")
            .await
            .expect("pg_advisory_lock");

        assert!(
            !co.tx_open(),
            "a session advisory lock never opens an RFQ transaction"
        );
        assert_eq!(co.pin_state(), PinState::Unpinned);
        assert!(
            co.tainted(),
            "a session advisory lock must taint (assist signal)"
        );
        assert_eq!(
            co.last_pin_cause(),
            Some(PinCause::AdvisoryLock),
            "pin-cause DoD"
        );

        backend_pid(&mut co).await
    };

    // Force the recycle (DISCARD ALL) to run by checking out again on the SAME (max_size=1) conn.
    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (recycle should have run DISCARD ALL)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- the release proof below only means \
         something if this is the SAME session that held the lock"
    );
    drop(co2);

    // INDEPENDENT second connection: a separate tokio_postgres client, not the pooled one.
    let (indep, indep_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect independent probe");
    tokio::spawn(async move {
        let _ = indep_conn.await;
    });

    let acquired: bool = indep
        .query_one("SELECT pg_try_advisory_lock(1)", &[])
        .await
        .expect("pg_try_advisory_lock from the independent session")
        .get(0);
    assert!(
        acquired,
        "an INDEPENDENT session must be able to acquire lock id 1 -- proving DISCARD ALL released \
         the pooled connection's hold, not merely that the SAME session re-acquired it re-entrantly"
    );

    // Idempotent cleanup: release the lock from the SAME (independent) session that just acquired
    // it, so a rerun of this test against the persistent testkit DB starts from a clean slate.
    let released: bool = indep
        .query_one("SELECT pg_advisory_unlock(1)", &[])
        .await
        .expect("pg_advisory_unlock from the independent session")
        .get(0);
    assert!(
        released,
        "cleanup: the independent session must release lock id 1 it just acquired"
    );
}

/// (d): `LISTEN` taints `PinCause::Listen`; after recycle the NEXT same-pid checkout is NOT
/// subscribed to the channel (`DISCARD ALL` runs `UNLISTEN *` among its resets).
#[tokio::test(flavor = "multi_thread")]
async fn pg_classify_listen_taints_and_unsubscribed_by_recycle() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.query("LISTEN ferro_test_chan", &[])
            .await
            .expect("LISTEN");

        assert!(!co.tx_open(), "LISTEN never opens an RFQ transaction");
        assert_eq!(co.pin_state(), PinState::Unpinned);
        assert!(co.tainted(), "LISTEN must taint (assist signal)");
        assert_eq!(co.last_pin_cause(), Some(PinCause::Listen), "pin-cause DoD");

        backend_pid(&mut co).await
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (recycle should have run DISCARD ALL)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (also never \
         subscribed) would make the assertion below a false green"
    );

    let subscribed =
        query_first_i64(&mut co2, "SELECT count(*) FROM pg_listening_channels()").await;
    assert_eq!(
        subscribed, 0,
        "DISCARD ALL at recycle must have UNLISTENed the channel, got count={subscribed}"
    );
}

/// (e): `CREATE TEMP TABLE` taints `PinCause::Temp`; after recycle the NEXT same-pid checkout does
/// NOT see the temp table (`DISCARD ALL` drops all temp objects).
#[tokio::test(flavor = "multi_thread")]
async fn pg_classify_temp_table_taints_and_dropped_by_recycle() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("CREATE TEMP TABLE IF NOT EXISTS ferro_test_tmp (x int)")
            .await
            .expect("CREATE TEMP TABLE");

        assert!(
            !co.tx_open(),
            "CREATE TEMP TABLE never opens an RFQ transaction"
        );
        assert_eq!(co.pin_state(), PinState::Unpinned);
        assert!(co.tainted(), "CREATE TEMP TABLE must taint (assist signal)");
        assert_eq!(co.last_pin_cause(), Some(PinCause::Temp), "pin-cause DoD");

        backend_pid(&mut co).await
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (recycle should have run DISCARD ALL)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (which also never \
         saw the temp table) would make the assertion below a false green"
    );

    let gone = query_first_bool(&mut co2, "SELECT to_regclass('ferro_test_tmp') IS NULL").await;
    assert!(
        gone,
        "DISCARD ALL at recycle must have dropped the temp table (to_regclass should be NULL)"
    );
}

// -------------------------------------------------------------------------------------------------
// M1-S3 Task 3 LIVE: the conditional-hygiene acceptance bar. Tasks 1-2 (already merged) taught the
// pool's checkout-time recycle a `ResetProfile`: `tainted` -> `Full` (`DISCARD ALL`); NOT tainted but
// recycled (PG's `clean_reset_profile()`) -> `Targeted` (`CLOSE ALL; SET SESSION AUTHORIZATION
// DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD
// SEQUENCES;`). Every test below is `max_size=1` + a same-`pg_backend_pid` assertion across the two
// checkouts (the same false-green guard the M1-S2 block above uses): without it, a fresh connection
// (which also starts at PG's defaults) would make the "leak closed" assertions pass even if the
// targeted reset never ran.
// -------------------------------------------------------------------------------------------------

/// (a) HEADLINE — the §7.4 assist-lexer blind-spot backstop, and S3's entire value proposition.
/// `set_config('search_path', ..., false)` mutates SESSION state (persists past the current
/// statement/transaction, exactly like a bare `SET`) but does so THROUGH A FUNCTION CALL inside an
/// otherwise-safe-listed leading `SELECT` -- `ferro-classify`'s rule 7 (the only pre-safe-list content
/// check) only looks for the `pg_advisory_lock` family, not `set_config`, so this statement falls
/// through to rule 8 (`SELECT` is safe) and returns `None`: the connection is NEVER tainted. Before
/// M1-S3, the pool's recycle guard was `if idle_conn.tx_open || idle_conn.tainted` (see git blame on
/// `pool.rs`'s pre-S3 revision) -- a `!tainted` conn skipped hygiene ENTIRELY, so this exact mutation
/// leaked to the very next tenant of this pooled connection. S3 widens the guard to also run the
/// backend's `clean_reset_profile()` (PG: `Targeted`) on a non-tainted recycled conn, whose `RESET
/// ALL` resets every GUC (including one set via `set_config(..., false)`) back to its configured
/// default. This is a genuine RED-before/GREEN-after test: temporarily reverting the S3 `pool.rs`
/// recycle-guard change (back to the pre-S3 `tx_open || tainted` guard) and rerunning this test makes
/// checkout 2 below observe `search_path` STILL `ferro_s3_leak`, failing the `assert_ne!` with
/// `left: "ferro_s3_leak"`. (Empirically confirmed: the recycle guard was temporarily reverted, this
/// test was rerun and observed exactly that failure, then the guard was restored -- not merely
/// inferred by re-reading the diff.)
#[tokio::test(flavor = "multi_thread")]
async fn pg_s3_set_config_search_path_blind_spot_closed_by_targeted_reset() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.query(
            "SELECT set_config('search_path', 'ferro_s3_leak', false)",
            &[],
        )
        .await
        .expect("set_config via a safe-listed leading SELECT");

        assert!(
            !co.tainted(),
            "set_config(...) inside a leading SELECT is the S2 lexer's documented blind spot -- it \
             must NOT taint (that is exactly what makes this the §7.4 backstop case, not an ordinary \
             tainted-path reset)"
        );
        assert_eq!(co.pin_state(), PinState::Unpinned);

        backend_pid(&mut co).await
        // `co` drops here -> idle with tainted=false. The recycle at the START of the NEXT
        // checkout now ALSO runs (S3): tainted=false but `clean_reset_profile() == Some(Targeted)`.
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (the targeted reset should have run on this non-tainted conn)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (also starting at \
         the default search_path) would make the assertion below a false green"
    );

    let search_path = query_first_text(&mut co2, "SHOW search_path").await;
    assert_ne!(
        search_path, "ferro_s3_leak",
        "the targeted profile's RESET ALL must have reset search_path away from the blind-spot \
         set_config mutation; got {search_path:?} -- a `!tainted` conn that skipped hygiene (the \
         pre-S3 behavior) would still read back ferro_s3_leak here"
    );
}

/// (b) the advisory-lock blind spot: `pg_advisory_lock` called from INSIDE a `DO $$ ... $$` body.
/// `ferro-classify`'s scanner deliberately masks dollar-quoted regions (`ci_false_inside_dollar_quote`
/// in `ferro-classify/src/scan.rs` is the unit proof), and `DO` is itself a safe-listed leading
/// keyword (rules.rs's documented §7.4 limitation) -- so the lexer never sees the lock acquisition and
/// the connection is NOT tainted. The release proof MUST come from an INDEPENDENT connection: session
/// advisory locks are RE-ENTRANT within the same session, so re-checking `pg_try_advisory_lock` on the
/// SAME pooled connection would trivially return true regardless of whether the reset ran at all --
/// only a genuinely different PG backend session acquiring the SAME lock id proves this session's
/// hold was actually released by the targeted profile's `pg_advisory_unlock_all()`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_s3_advisory_lock_in_do_body_blind_spot_released_by_targeted_reset() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url.clone()), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("DO $$ BEGIN PERFORM pg_advisory_lock(4242); END $$")
            .await
            .expect("DO block acquiring an advisory lock in-body");

        assert!(
            !co.tainted(),
            "pg_advisory_lock called from inside a DO body is masked from the lexer (dollar-quote \
             region) -- this must NOT taint, which is exactly why the targeted profile (not the \
             already-proven tainted->Full path) is what has to release it"
        );
        assert_eq!(co.pin_state(), PinState::Unpinned);

        backend_pid(&mut co).await
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (targeted reset should have released the advisory lock)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- the independent-connection release proof \
         below only means something if this is the SAME session that acquired the lock"
    );
    drop(co2);

    // INDEPENDENT second connection: a separate tokio_postgres client, NOT the pooled one.
    let (indep, indep_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect independent probe");
    tokio::spawn(async move {
        let _ = indep_conn.await;
    });

    let acquired: bool = indep
        .query_one("SELECT pg_try_advisory_lock(4242)", &[])
        .await
        .expect("pg_try_advisory_lock from the independent session")
        .get(0);
    assert!(
        acquired,
        "an INDEPENDENT session must be able to acquire lock id 4242 -- proving the targeted \
         profile's pg_advisory_unlock_all() released the pooled connection's hold, not merely that \
         the SAME session re-acquired it re-entrantly (which would false-green even with no reset)"
    );

    // Idempotent cleanup: release from the SAME (independent) session that just acquired it, so a
    // rerun of this test against the persistent testkit DB starts from a clean slate.
    let released: bool = indep
        .query_one("SELECT pg_advisory_unlock(4242)", &[])
        .await
        .expect("pg_advisory_unlock from the independent session")
        .get(0);
    assert!(
        released,
        "cleanup: the independent session must release lock id 4242 it just acquired"
    );
}

/// (c) temp table + LISTEN, cleared by the TARGETED profile specifically (not merely the
/// already-proven tainted->Full path). Created via the raw `conn_mut()` side door -- which
/// deliberately bypasses `Checkout::exec`/`query` entirely, so `apply_classify` never runs and this
/// checkout stays genuinely `!tainted()` -- isolating the assertion to the targeted profile's OWN
/// `DISCARD TEMP` / `UNLISTEN *` coverage, independent of the M1-S2 tainted-path tests already in
/// this file (`pg_classify_listen_taints_...` / `pg_classify_temp_table_taints_...`), which exercise
/// the SAME leak classes but via the `tainted -> Full` route.
#[tokio::test(flavor = "multi_thread")]
async fn pg_s3_temp_and_listen_cleared_by_targeted_reset_on_non_tainted_conn() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.conn_mut()
            .client
            .batch_execute("CREATE TEMP TABLE ferro_s3_tmp (x int); LISTEN ferro_s3_chan;")
            .await
            .expect("create temp table + LISTEN via the raw conn_mut() side door");

        assert!(
            !co.tainted(),
            "the raw side door bypasses apply_classify entirely -- this checkout must stay \
             !tainted so the recycle below takes the TARGETED path, not the already-proven Full path"
        );

        backend_pid(&mut co).await
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (targeted reset should have cleared temp + listen)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (which also never \
         saw the temp table/listen) would make the assertions below a false green"
    );

    let temp_gone = query_first_bool(&mut co2, "SELECT to_regclass('ferro_s3_tmp') IS NULL").await;
    assert!(
        temp_gone,
        "the targeted profile's DISCARD TEMP must have dropped the temp table"
    );

    let listening = query_first_i64(&mut co2, "SELECT count(*) FROM pg_listening_channels()").await;
    assert_eq!(
        listening, 0,
        "the targeted profile's UNLISTEN * must have unsubscribed the channel, got count={listening}"
    );
}

/// (d) the verification-BLOCKER fix, live: a `WITH HOLD` cursor is session-scoped -- it survives its
/// own transaction's `COMMIT` and stays open for the rest of the session until explicitly `CLOSE`d.
/// `DECLARE` is NOT in `ferro-classify`'s `SAFE_LEADING_KEYWORDS`, so under the DEFAULT
/// `pin_on_unknown=true` it would taint as `Unknown` -- which would route this conn through `Full`
/// (`DISCARD ALL`, which itself starts with `CLOSE ALL`) and never actually exercise the TARGETED
/// profile's own `CLOSE ALL`. So this test builds the pool with `pin_on_unknown=false` (per the task
/// brief: "set the PoolConfig field directly"), under which an unrecognized statement like `DECLARE`
/// classifies `None` (not tainted) -- routing this conn through Targeted specifically. Without
/// `CLOSE ALL` in the targeted batch, this holdable cursor would survive the reset and the NEXT
/// tenant on this pooled connection could `FETCH` it -- a cross-tenant data leak (the exact gap the
/// plan's adversarial review caught and fixed by adding `CLOSE ALL` to the targeted profile).
///
/// Uses the sanctioned `begin_tx`/`exec`/`commit_tx` lifecycle (NOT a raw multi-statement
/// `"BEGIN; DECLARE ...; COMMIT;"` batch through `Checkout::exec`): `is_bare_tx_control` inspects
/// ONLY the leading keyword, and a batch literally starting with the word `BEGIN` is bare
/// tx-control by that check, so `exec` would reject it up front with `PoolError::Unsupported` before
/// anything ran. `begin_tx`/`commit_tx` reach the raw, unguarded `simple_query` for exactly this
/// reason (see `Checkout::begin_tx_with`'s doc comment) and are the pool's own sanctioned way to
/// open/close a transaction around an in-between statement -- the live effect (an open tx, a
/// `WITH HOLD` DECLARE inside it, a clean COMMIT) is identical to the brief's literal SQL.
#[tokio::test(flavor = "multi_thread")]
async fn pg_s3_with_hold_cursor_closed_by_targeted_close_all() {
    let Some(url) = test_url() else {
        return;
    };
    let mut cfg = config(1);
    cfg.pin_on_unknown = false;
    let pool = Pool::new(PgBackend::new(url), cfg);

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.begin_tx(TxId(1001)).await.expect("begin tx");
        co.exec("DECLARE ferro_s3_cur CURSOR WITH HOLD FOR SELECT 1")
            .await
            .expect("DECLARE a WITH HOLD cursor inside the tx");
        co.commit_tx()
            .await
            .expect("commit (the WITH HOLD cursor survives)");

        assert!(
            !co.tainted(),
            "with pin_on_unknown=false, an unrecognized DECLARE must NOT taint -- this is what \
             routes the conn through Targeted (not Full) below, actually exercising CLOSE ALL"
        );
        assert_eq!(co.pin_state(), PinState::Unpinned, "commit_tx unpins");

        backend_pid(&mut co).await
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (targeted CLOSE ALL should have closed the held cursor)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (which never \
         declared the cursor) would make the assertion below a false green"
    );

    let cursor_count = query_first_i64(
        &mut co2,
        "SELECT count(*) FROM pg_cursors WHERE name = 'ferro_s3_cur'",
    )
    .await;
    assert_eq!(
        cursor_count, 0,
        "the targeted profile's CLOSE ALL must have closed the WITH HOLD cursor -- without it, the \
         NEXT tenant on this pooled connection could FETCH a previous tenant's held cursor \
         (a cross-tenant data leak)"
    );
}

/// (e) GENUINE prepares-survive-vs-destroyed contrast -- the entire rationale for the conditional
/// model. Drives `PoolBackend::reset` DIRECTLY (via the sanctioned `conn_mut()` raw-client side door
/// + `pool.backend()`), bypassing the pool's own recycle decision entirely: this test is about the
/// RESET PRIMITIVE's own effect on a named prepared statement, which the other live tests in this
/// file don't otherwise exercise (they only ever observe SESSION STATE, never a prepared statement's
/// survival). A named `PREPARE` survives a `Targeted` reset (EXECUTE still succeeds -- Targeted
/// omits `DEALLOCATE ALL`/`DISCARD PLANS`) but NOT a `Full` reset (`DISCARD ALL` destroys it).
#[tokio::test(flavor = "multi_thread")]
async fn pg_s3_targeted_reset_preserves_prepares_full_reset_destroys_them() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.conn_mut()
        .client
        .batch_execute("PREPARE ferro_s3_ps AS SELECT 1")
        .await
        .expect("PREPARE ferro_s3_ps");

    pool.backend()
        .reset(co.conn_mut(), ResetProfile::Targeted)
        .await
        .expect("Targeted reset must succeed");

    co.conn_mut()
        .client
        .batch_execute("EXECUTE ferro_s3_ps")
        .await
        .expect(
            "EXECUTE must SUCCEED after a Targeted reset -- Targeted deliberately omits \
             DEALLOCATE ALL/DISCARD PLANS, so a named prepared statement survives it (the whole \
             reason Targeted exists instead of always running Full)",
        );

    // Contrast: re-PREPARE the SAME name (DEALLOCATE first, since Targeted just proved it survives
    // and a second PREPARE of an existing name errors), then Full -- must NOT survive.
    co.conn_mut()
        .client
        .batch_execute("DEALLOCATE ferro_s3_ps; PREPARE ferro_s3_ps AS SELECT 1")
        .await
        .expect("re-PREPARE ferro_s3_ps for the Full contrast");

    pool.backend()
        .reset(co.conn_mut(), ResetProfile::Full)
        .await
        .expect("Full reset must succeed");

    let err = co
        .conn_mut()
        .client
        .batch_execute("EXECUTE ferro_s3_ps")
        .await
        .expect_err(
            "EXECUTE must FAIL after a Full reset -- DISCARD ALL deallocates every prepared \
             statement, proving the Targeted-vs-Full distinction actually matters",
        );
    // `tokio_postgres::Error`'s own `Display` is just a fixed `"db error"` label (the real server
    // message lives on the wrapped `DbError`, reached via `as_db_error()`), so assert on THAT
    // message rather than `err.to_string()`.
    let msg = err
        .as_db_error()
        .map(|e| e.message().to_lowercase())
        .unwrap_or_default();
    assert!(
        msg.contains("ferro_s3_ps") || msg.contains("prepared statement"),
        "expected a 'prepared statement ferro_s3_ps does not exist' style error, got: {err:?}"
    );
}

/// (f) a tainted conn is still fully cleaned for the next tenant: a non-local `SET` taints via
/// `PinCause::Set` -- the SAME lexer signal the existing M1-S2
/// `pg_classify_set_search_path_taints_and_hygiene_resets_on_recycle` test above exercises -- and
/// the recycle clears the mutation. Note this only proves a reset ran (both the Full and Targeted
/// profiles reset `search_path`); the deterministic Full-vs-Targeted selection for a tainted conn
/// is proven by the fake unit test in `ferro-pool`.
/// Uses a distinct search_path value (`ferro_s3_full`) and its own pool so it is independently
/// idempotent against the persistent testkit DB.
#[tokio::test(flavor = "multi_thread")]
async fn pg_s3_tainted_conn_still_gets_full_reset() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));

    let pid1 = {
        let mut co = pool.checkout().await.expect("checkout");
        co.exec("SET search_path TO ferro_s3_full")
            .await
            .expect("SET search_path");

        assert!(
            co.tainted(),
            "a non-local SET must taint (PinCause::Set) -- this conn must take the Full path, not \
             Targeted"
        );
        assert_eq!(co.last_pin_cause(), Some(PinCause::Set));

        backend_pid(&mut co).await
    };

    let mut co2 = pool
        .checkout()
        .await
        .expect("checkout again (Full reset should have run)");
    let pid2 = backend_pid(&mut co2).await;
    assert_eq!(
        pid1, pid2,
        "max_size=1 must reuse the SAME backend pid -- otherwise a fresh conn (also starting at \
         the default search_path) would make the assertion below a false green"
    );

    let search_path = query_first_text(&mut co2, "SELECT current_setting('search_path')").await;
    assert_ne!(
        search_path, "ferro_s3_full",
        "DISCARD ALL (the Full profile, for a tainted conn) must have reset search_path; got \
         {search_path:?}"
    );
}
