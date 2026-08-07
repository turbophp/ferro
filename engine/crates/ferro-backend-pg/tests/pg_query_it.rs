//! Live `ferro-backend-pg` row-returning `Checkout::query` tests (S5 Task 2) against a real
//! Postgres. Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — mirrors
//! `pg_pool_it.rs` so `cargo test --workspace` stays green offline.
//!
//! **Type-coverage contract:** this file covers the M0 scalar set plus the pre-flight refusal of a
//! type outside the supported set. The M1-S7 canonical tags (`DECIMAL`/`DATE`/`TIME`/`TIMESTAMP`/
//! `TIMESTAMPTZ`/`UUID`/`JSON`) have their own live round-trip suite in `pg_types_it.rs`.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg
//! ```

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;
use ferro_proto::consts::{branch, errc, tag};
use ferro_proto::value::Value;

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

/// THE HEADLINE: `SELECT 1` returns INT4, so OID-strict extraction MUST read it as `i32` and widen
/// to `Value::I64(1)` — a naive `try_get::<i64>` would fail. One column, tag I64, one row `[I64(1)]`.
#[tokio::test(flavor = "multi_thread")]
async fn query_select1_oid_strict_int4() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let r = co
        .query("SELECT 1", &[])
        .await
        .expect("SELECT 1 should succeed");

    assert_eq!(r.cols.len(), 1, "SELECT 1 has exactly one column");
    assert_eq!(
        r.cols[0].tag,
        tag::I64,
        "an int4 column maps to the canonical I64 tag"
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::I64(1)]],
        "OID-strict int4 extraction must widen to Value::I64(1)"
    );
    // PG's SELECT command tag carries the retrieved row count, so `rows_affected()` == 1 here. The
    // service (Task 3) uses `rows` for fetch:rows and `affected` for fetch:none — `query` faithfully
    // returns both. The point of asserting it is that it is NEVER a hardcoded 0 (the S4 defect).
    assert_eq!(
        r.affected, 1,
        "a 1-row SELECT reports affected == row count via the command tag"
    );
}

/// A parameterized query round-trips the full M0 scalar set incl. NULL and BYTES. Explicit casts
/// pin each `$n` to the canonical type the binder produces (I64→bigint, F64→double precision, …);
/// the type-agnostic NULL bind is exercised on the `::text` column.
#[tokio::test(flavor = "multi_thread")]
async fn query_params_round_trip_m0_scalars() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let params = [
        Value::I64(-200),
        Value::F64(1.5),
        Value::Text("hi".to_string()),
        Value::Bool(true),
        Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
        Value::Null,
    ];
    let r = co
        .query(
            "SELECT ?::bigint, ?::double precision, ?::text, ?::boolean, ?::bytea, ?::text",
            &params,
        )
        .await
        .expect("parameterized query should succeed");

    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![
            tag::I64,
            tag::F64,
            tag::TEXT,
            tag::BOOL,
            tag::BYTES,
            tag::TEXT
        ],
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("hi".to_string()),
            Value::Bool(true),
            Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
            Value::Null,
        ]],
    );
}

/// A syntax error classifies as `Sql { Syntax, NonRetryable }` preserving the raw SQLSTATE, and the
/// connection stays usable afterward (it was a statement-level error, not a session end).
#[tokio::test(flavor = "multi_thread")]
async fn query_syntax_error_classifies_and_conn_survives() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let err = co
        .query("SELCT 1", &[])
        .await
        .expect_err("a syntax error must fail");
    match err {
        PoolError::Sql {
            code,
            branch: b,
            ref sqlstate,
            ..
        } => {
            assert_eq!(code, errc::SYNTAX, "a syntax error maps to the Syntax code");
            assert_eq!(b, branch::NON_RETRYABLE);
            assert!(
                sqlstate.as_deref().is_some_and(|s| s.starts_with("42")),
                "the raw SQLSTATE (42xxx) must be preserved, got {sqlstate:?}"
            );
        }
        other => panic!("expected PoolError::Sql{{Syntax}}, got {other:?}"),
    }

    // The connection must still be usable — proof the error was statement-level, not a lost session.
    let ok = co.query("SELECT 1", &[]).await.expect("conn still usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

/// A still-deferred column type (`interval`) is a loud `Unsupported`, raised before the query runs,
/// and the connection stays clean.
///
/// Was `query_out_of_m0_column_is_unsupported` / `SELECT now()` until M1-S7 implemented
/// `timestamptz` — the assertion is REPOINTED at a genuinely-deferred type, not deleted, because
/// the property under test (a column type outside the supported set is a pre-flight `Unsupported`,
/// never a silent miscast) is unchanged; only its witness moved.
#[tokio::test(flavor = "multi_thread")]
async fn query_deferred_column_type_is_unsupported() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let err = co
        .query("SELECT '1 day'::interval", &[])
        .await
        .expect_err("interval is deferred past M1-S7");
    assert!(
        matches!(err, PoolError::Unsupported(_)),
        "a deferred column type must be Unsupported, got {err:?}"
    );

    // `timestamptz` — this test's PREVIOUS witness — is now genuinely supported, so the repointing
    // above is a real coverage move rather than a silently-weakened assertion.
    let now = co
        .query("SELECT now()", &[])
        .await
        .expect("timestamptz is supported as of M1-S7");
    assert_eq!(now.cols[0].tag, tag::TIMESTAMPTZ);

    // Conn stays clean/usable (we errored during cols-build, before running the query).
    let ok = co.query("SELECT 1", &[]).await.expect("conn still usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

/// A wrong param COUNT is a KNOWN-FATE bind error, NOT the fate-unknown `ConnectionLost` (§19.3
/// safety — the S5 Task-2 review defect). The statement (`SELECT $1::bigint` needs one param) is
/// given zero: pre-validation catches the arity mismatch BEFORE anything is sent, so the fate is
/// known (never executed) and the connection is untouched → still usable afterward. Were this
/// classified `ConnectionLost`, the service would emit a false `WriteUnconfirmed{Indeterminate}`.
#[tokio::test(flavor = "multi_thread")]
async fn query_wrong_param_count_is_known_fate_not_connection_lost() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let err = co
        .query("SELECT ?::bigint", &[])
        .await
        .expect_err("one placeholder, zero params supplied must fail");
    match err {
        PoolError::Sql {
            code,
            branch: b,
            sqlstate,
            errno,
            ..
        } => {
            assert_eq!(
                code,
                errc::UNSUPPORTED,
                "a bind arity mismatch is a known-fate Unsupported Sql error"
            );
            assert_eq!(b, branch::NON_RETRYABLE);
            assert_eq!(
                sqlstate, None,
                "the server never saw the statement, so there is no SQLSTATE"
            );
            // M1-S8a: a PRE-SEND rejection must never FABRICATE a vendor errno either. The errno is
            // populated at exactly ONE site (the MySQL `error_map`, from a real `ServerError`); PG
            // has no integer errno at all, and no server answered here. Mirrors the MySQL twin at
            // `ferro-backend-mysql/src/bind.rs`.
            assert_eq!(
                errno, None,
                "a pre-send bind rejection has no vendor errno — no server answered"
            );
        }
        PoolError::ConnectionLost => panic!(
            "REGRESSION: a wrong param count was classified ConnectionLost \
             (fate-unknown) — this is the false-Indeterminate defect"
        ),
        other => panic!("expected known-fate PoolError::Sql{{Unsupported}}, got {other:?}"),
    }

    // The connection was never touched by a bad bind (we rejected before query_raw) — still usable.
    let ok = co.query("SELECT 1", &[]).await.expect("conn still usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

/// **M1-S8a, live**: an IN-RANGE `Value::I64` now BINDS an `int4` PK column (the narrowing bind),
/// and an OUT-OF-RANGE one is still a KNOWN-FATE bind error, NOT `ConnectionLost` (§19.3).
///
/// Under M0 this test pinned the opposite for the in-range case — `I64` boxed as `int8`, which does
/// not `accept` the `int4` a serial-style PK infers for the parameter, so every DBAL insert into a
/// `serial` PK was refused. `PgInt` now writes the native `int4`. The §19.3 property the test exists
/// for is UNCHANGED and is carried by the out-of-range half: a value the target width cannot hold is
/// refused by the VALUE-aware pre-flight, before anything is sent, so it is a diagnosable
/// `Sql{Unsupported}` and can never be MISCLASSIFIED as the fate-unknown `ConnectionLost` that
/// §19.3 turns into a false `WriteUnconfirmed{Indeterminate}` on a write.
#[tokio::test(flavor = "multi_thread")]
async fn query_i64_against_int4_is_known_fate_not_connection_lost() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // int4 PK — PG infers the INSERT's `$1` parameter type as int4 from the target column.
    co.query("CREATE TEMP TABLE ferro_s5_pk (id int4 primary key)", &[])
        .await
        .expect("create temp table");

    // (a) IN RANGE: the narrowing bind lands the row, natively, against a real server.
    let ins = co
        .query("INSERT INTO ferro_s5_pk (id) VALUES (?)", &[Value::I64(7)])
        .await
        .expect("M1-S8a: an in-range I64 binds an int4 column");
    assert_eq!(ins.affected, 1, "the narrowing bind actually inserted");
    let back = co
        .query("SELECT id FROM ferro_s5_pk WHERE id = ?", &[Value::I64(7)])
        .await
        .expect("read back through the same narrowing bind");
    assert_eq!(back.rows, vec![vec![Value::I64(7)]]);

    // (b) OUT OF RANGE: a value int4 cannot hold is refused PRE-SEND, known-fate.
    let too_big = i64::from(i32::MAX) + 1;
    let err = co
        .query(
            "INSERT INTO ferro_s5_pk (id) VALUES (?)",
            &[Value::I64(too_big)],
        )
        .await
        .expect_err("an out-of-range I64 cannot bind an int4 column");
    match err {
        PoolError::Sql {
            code,
            branch: b,
            ref sqlstate,
            errno,
            ref message,
        } => {
            assert_eq!(
                code,
                errc::UNSUPPORTED,
                "an out-of-range bind is a known-fate Unsupported Sql error"
            );
            assert_eq!(b, branch::NON_RETRYABLE);
            assert_eq!(
                *sqlstate, None,
                "the server never saw the statement, so there is no SQLSTATE"
            );
            // M1-S8a: the NEW narrowing rejection is a pre-send `bind_error` too, so it must not
            // FABRICATE a vendor errno. The errno is populated at exactly ONE site (the MySQL
            // `error_map`, from a real `ServerError`); PG has none, and no server answered here.
            assert_eq!(
                errno, None,
                "a pre-send range rejection has no vendor errno — no server answered"
            );
            assert!(
                message.contains("out of range") && message.contains(&too_big.to_string()),
                "the refusal must name the reason and the offending value: {message}"
            );
        }
        PoolError::ConnectionLost => panic!(
            "REGRESSION: an out-of-range I64-vs-int4 bind was classified ConnectionLost \
             (fate-unknown) — this is the exact false-Indeterminate the pre-validation prevents"
        ),
        other => panic!("expected known-fate PoolError::Sql{{Unsupported}}, got {other:?}"),
    }

    // The out-of-range row provably never inserted (bind rejected pre-send) and the conn is clean.
    let rows = co
        .query("SELECT count(*)::int8 FROM ferro_s5_pk", &[])
        .await
        .expect("conn still usable");
    assert_eq!(
        rows.rows,
        vec![vec![Value::I64(1)]],
        "only the in-range insert landed"
    );
}

/// A DML statement reports `affected` from the command tag — NEVER a hardcoded 0 (the S4
/// `batch_execute` defect). Two inserted rows → `affected == 2`, with an empty row set.
#[tokio::test(flavor = "multi_thread")]
async fn query_insert_reports_affected() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // bigint column so the canonical I64→int8 bind matches the inferred param type.
    co.query("CREATE TEMP TABLE ferro_s5_q (id bigint)", &[])
        .await
        .expect("create temp table");

    let r = co
        .query(
            "INSERT INTO ferro_s5_q (id) VALUES (?), (?)",
            &[Value::I64(1), Value::I64(2)],
        )
        .await
        .expect("insert should succeed");

    assert_eq!(
        r.affected, 2,
        "affected must come from the command tag, not a hardcoded 0"
    );
    assert!(
        r.rows.is_empty(),
        "an INSERT without RETURNING yields no rows"
    );
}

/// PostgreSQL has no integer error code — its error identity is the five-character SQLSTATE — so
/// `PoolError::Sql.errno` is `None` on PG **by construction**, not by omission (M1-S8a).
///
/// This is deliberately driven by a real server error rather than a hand-built `PoolError`: an
/// assertion over an input the test itself constructed with `errno: None` cannot fail. Here the
/// value comes off `error_map::map` on a genuine `42601`, so wiring any PG errno — a fabricated one,
/// a hash of the SQLSTATE — turns this RED.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_pg_server_error_carries_no_errno() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let err = co
        .query("SELEKT 1", &[])
        .await
        .expect_err("a syntax error must fail");
    match err {
        PoolError::Sql {
            ref sqlstate,
            errno,
            ..
        } => {
            assert_eq!(
                sqlstate.as_deref(),
                Some("42601"),
                "PG identifies this error by SQLSTATE"
            );
            assert_eq!(
                errno, None,
                "PG has no integer errno — None by construction, off the REAL error_map path"
            );
        }
        other => panic!("expected a known-fate Sql error, got {other:?}"),
    }
}
