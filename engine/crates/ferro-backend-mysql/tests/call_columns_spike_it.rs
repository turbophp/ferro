//! **SPIKE (M2): where does a `CALL`'s column metadata actually live?**
//!
//! `docs/followups/2026-09-10-mysql-call-returns-no-columns.md` reads the source and concludes that
//! the blind spot C1a recorded is not a MySQL wall but a choice about WHICH metadata Ferro reads:
//! `query.rs` builds `cols` from the PREPARED statement (`stmt.columns()`) and then maps each row's
//! cells by iterating that same list, so an empty list yields ZERO-CELL rows.
//!
//! That reading cannot be confirmed without a live MySQL, and the container this was written in has
//! none. This file is the confirmation, and it deliberately goes at the DRIVER rather than through
//! `PoolBackend::query`: the question is what MySQL and `mysql_async` do, and routing it through
//! Ferro's own mapping would answer a different one.
//!
//! It asserts nothing about the FIX. It answers the three questions the follow-up marks UNVERIFIED,
//! and prints everything it sees, so a downstream design rests on output rather than on this file's
//! expectations:
//!
//!   1. is a prepared `CALL`'s `stmt.columns()` empty?
//!   2. is the EXECUTED result set's `columns()` populated for the same procedure?
//!   3. how many result sets does a `CALL` produce? (measured for ONE and for TWO `SELECT`s —
//!      the two-`SELECT` half was added at S2, since S1 measured only N=1 and S4 is designed
//!      against the answer)
//!
//! **Failure here invalidates the follow-up's plan** — which is the point of running it first.
//!
//! ```text
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//!   cargo test -p ferro-backend-mysql --test call_columns_spike_it -- --nocapture
//! ```

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};

/// A procedure name unique per run, so a crashed earlier run cannot leave one behind that this one
/// then silently reuses with a different body.
fn proc_name() -> String {
    format!(
        "ferro_call_spike_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[tokio::test]
async fn a_prepared_call_reports_its_columns_only_after_execution() {
    let Some(url) = std::env::var("FERRO_TEST_MYSQL_URL").ok() else {
        eprintln!(
            "skip: FERRO_TEST_MYSQL_URL unset (a_prepared_call_reports_its_columns_only_after_execution)"
        );
        return;
    };

    let opts = Opts::from_url(&url).expect("FERRO_TEST_MYSQL_URL is a valid mysql:// url");
    let mut conn = Conn::new(opts).await.expect("connect to live MySQL");
    let p = proc_name();

    conn.query_drop(format!(
        "CREATE PROCEDURE {p}() BEGIN SELECT 7 AS seven, 'x' AS letter; END"
    ))
    .await
    .expect("create the procedure");

    // ---- (1) PREPARE-time metadata -------------------------------------------------------------
    let stmt = conn
        .prep(format!("CALL {p}()"))
        .await
        .expect("prepare the CALL");
    let prepared_cols: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    println!(
        "[spike] prepare-time columns: {} -> {:?}",
        prepared_cols.len(),
        prepared_cols
    );

    // ---- (2) EXECUTION-time metadata, and (3) how many sets -------------------------------------
    let mut result = conn.exec_iter(&stmt, ()).await.expect("execute the CALL");

    let mut sets = 0usize;
    loop {
        let exec_cols: Vec<String> = result
            .columns()
            .map(|cs| cs.iter().map(|c| c.name_str().to_string()).collect())
            .unwrap_or_default();
        let rows: Vec<mysql_async::Row> = result.collect().await.expect("collect the set");
        println!(
            "[spike] set #{sets}: execution-time columns {} -> {:?}; {} row(s), first row cells = {:?}",
            exec_cols.len(),
            exec_cols,
            rows.len(),
            rows.first().map(|r| r.len()),
        );
        sets += 1;
        // `collect()` consumes ONE result set and `is_empty()` reports whether another follows —
        // the driver's own doc says so ("`SELECT 'foo'; SELECT 'foo','bar';` will produce a
        // QueryResult with two result sets in it. One can use QueryResult::is_empty …"). The
        // `next_set`/`more_results_exists` pair is PRIVATE; the follow-up named them and was wrong
        // about the public surface, which this test corrected before any design leaned on it.
        if result.is_empty() {
            break;
        }
    }
    println!("[spike] total result sets seen: {sets}");

    conn.query_drop(format!("DROP PROCEDURE {p}"))
        .await
        .expect("drop the procedure");

    // ---- (3b) N > 1: a TWO-`SELECT` procedure --------------------------------------------------
    // The S1 run measured only the N=1 case, and the follow-up flagged that as the open half: the
    // trailing OK packet is not surfaced as a set by `collect()`/`is_empty()`, so N `SELECT`s were
    // EXPECTED to yield N sets — expected, not measured. S4 (`selectResultSets()`) is designed
    // against this number, so it is measured here rather than assumed there.
    let p2 = proc_name();
    conn.query_drop(format!(
        "CREATE PROCEDURE {p2}() BEGIN SELECT 1 AS a; SELECT 2 AS b, 3 AS c; END"
    ))
    .await
    .expect("create the two-SELECT procedure");

    let stmt2 = conn
        .prep(format!("CALL {p2}()"))
        .await
        .expect("prepare the two-SELECT CALL");
    println!(
        "[spike] two-SELECT prepare-time columns: {}",
        stmt2.columns().len()
    );

    let mut result2 = conn
        .exec_iter(&stmt2, ())
        .await
        .expect("execute the two-SELECT CALL");
    let mut sets2 = 0usize;
    loop {
        let cols: Vec<String> = result2
            .columns()
            .map(|cs| cs.iter().map(|c| c.name_str().to_string()).collect())
            .unwrap_or_default();
        let rows: Vec<mysql_async::Row> = result2.collect().await.expect("collect the set");
        println!(
            "[spike] two-SELECT set #{sets2}: columns {} -> {:?}; {} row(s), first row cells = {:?}",
            cols.len(),
            cols,
            rows.len(),
            rows.first().map(|r| r.len()),
        );
        sets2 += 1;
        if result2.is_empty() {
            break;
        }
    }
    println!("[spike] two-SELECT total result sets seen: {sets2}");
    drop(result2);

    conn.query_drop(format!("DROP PROCEDURE {p2}"))
        .await
        .expect("drop the two-SELECT procedure");

    // The ONE assertion, and it is the follow-up's whole thesis: the prepare-time list is NOT where
    // a CALL's columns live. If this fails, the plan in
    // docs/followups/2026-09-10-mysql-call-returns-no-columns.md is wrong and must be rewritten
    // before any engine change — which is why this test exists before that change.
    assert!(
        prepared_cols.is_empty(),
        "the follow-up assumes a prepared CALL declares no columns; it declared {prepared_cols:?}, \
         so the diagnosis needs revisiting rather than the code"
    );
    assert!(
        sets >= 1,
        "a CALL whose body SELECTs must yield at least one result set"
    );
    // Asserted as `> 1` rather than `== 2` deliberately: what S4 needs to know is that a
    // multi-`SELECT` procedure really does surface more than one set (otherwise there is nothing
    // for a multi-result-set wire change to carry, and S4's premise collapses). Whether the exact
    // count is 2 or 3 — i.e. whether the trailing OK packet surfaces as a set on some engine — is
    // PRINTED above for S4 to design against, and pinning it here would only turn one engine red
    // without telling anyone more than the printed line already does.
    assert!(
        sets2 > 1,
        "a CALL with two SELECTs must surface more than one result set (saw {sets2}); \
         S4's multi-result-set design rests on this"
    );
}
