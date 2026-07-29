//! Live `ferro-backend-pg` row-returning `Checkout::query` tests (S5 Task 2) against a real
//! Postgres. Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — mirrors
//! `pg_pool_it.rs` so `cargo test --workspace` stays green offline.
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

/// An out-of-M0 column type (`now()` → timestamptz) is a loud `Unsupported`, raised before the
/// query runs, and the connection stays clean.
#[tokio::test(flavor = "multi_thread")]
async fn query_out_of_m0_column_is_unsupported() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let err = co
        .query("SELECT now()", &[])
        .await
        .expect_err("timestamptz is out of the M0 scalar set");
    assert!(
        matches!(err, PoolError::Unsupported(_)),
        "an out-of-M0 column type must be Unsupported, got {err:?}"
    );

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
            code, branch: b, ..
        } => {
            assert_eq!(
                code,
                errc::UNSUPPORTED,
                "a bind arity mismatch is a known-fate Unsupported Sql error"
            );
            assert_eq!(b, branch::NON_RETRYABLE);
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

/// `Value::I64` bound against an `int4` PK column is a KNOWN-FATE bind error, NOT `ConnectionLost`
/// (§19.3). This is the EXACT input that would have produced a false `WriteUnconfirmed`: the
/// canonical `I64` boxes as `int8`, which does not `accept` the `int4` the column (a serial-style
/// PK) inferred for the parameter. Pre-validation rejects it before the INSERT is sent, so the row
/// provably never inserted and the connection stays clean.
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

    let err = co
        .query("INSERT INTO ferro_s5_pk (id) VALUES (?)", &[Value::I64(1)])
        .await
        .expect_err("I64 (int8) cannot bind an int4 column in M0");
    match err {
        PoolError::Sql {
            code, branch: b, ..
        } => {
            assert_eq!(
                code,
                errc::UNSUPPORTED,
                "an uncastable bind is a known-fate Unsupported Sql error"
            );
            assert_eq!(b, branch::NON_RETRYABLE);
        }
        PoolError::ConnectionLost => panic!(
            "REGRESSION: an I64-vs-int4 bind was classified ConnectionLost (fate-unknown) — \
             this is the exact false-Indeterminate the pre-validation fix prevents"
        ),
        other => panic!("expected known-fate PoolError::Sql{{Unsupported}}, got {other:?}"),
    }

    // Nothing was inserted (bind rejected pre-send) and the conn is clean: still usable.
    let ok = co.query("SELECT 1", &[]).await.expect("conn still usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
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
