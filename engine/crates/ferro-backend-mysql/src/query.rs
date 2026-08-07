//! `PoolBackend::query` for MySQL/MariaDB (M1-S6 Task 4): the buffered, param-bound, row-returning
//! path — the MySQL counterpart of `ferro-backend-pg`'s `query::run`.
//!
//! Flow (buffered; MySQL row streaming is deferred — SPEC §22.2 (n)):
//! 1. `COM_STMT_PREPARE` the SQL (`?` is MySQL's NATIVE placeholder — no `?`→`$n` normalization,
//!    unlike PG). One round trip that yields the result-column metadata.
//! 2. Build `Vec<ColMeta>` from the prepared statement's columns via [`rowmap::column_to_tag`] — a
//!    loud `Unsupported` for any out-of-scope column type, raised BEFORE the query runs (the
//!    connection stays clean and usable), exactly like PG's cols-build.
//! 3. **Bind pre-flight**, both halves BEFORE anything is sent: the arity check
//!    ([`bind::validate_arity`], `params.len()` vs the statement's `num_params()`) and the per-param
//!    canonical-shape check ([`bind::to_params`]). Either failure is a KNOWN-FATE
//!    `Sql{Unsupported}`, NEVER `ConnectionLost` — the statement provably never ran, so it can never
//!    mint a false §19.3 `Indeterminate`. (MySQL prepares expose NO inferred param types, so unlike
//!    PG there is no server-side type to check the payload against — the canonical grammar is.)
//! 4. Bind + `exec_iter`, fully draining the single result set (BUFFERED — constant-memory streaming
//!    is S7). Each cell maps through [`rowmap::extract_value`]. `affected` and `last_insert_id` come
//!    off the OK packet (via the driver's per-conn cache).
//!
//! On ANY prepare/exec/transport error → `Err` (via [`MysqlConn::map_stmt_error`], which marks the
//! conn `closed` on a session-fatal failure and routes through `error_map`). The `Err` is
//! PROPAGATED, never swallowed to `Ok` — that is what makes `ferro-pool`'s backend-agnostic Rule-A
//! (`Checkout::query`'s `if r.is_err() { tx_open = true; tainted = true }`) force-taint the conn.
//! Nothing here re-runs the statement (charter rule 3).
//!
//! After a successful drain, [`MysqlConn::record_session_mutation`] records the §7.1 session-mutation
//! taint from the resulting OK packet — the SAME taint the leaf `simple_query` records, so a mutation
//! on the row-returning path also taints.

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row, Statement};

use crate::Value;
use crate::conn::MysqlConn;
use crate::{bind, rowmap};

/// Runs `sql` (with native `?` placeholders) + `params` against `conn`, buffering the full result.
/// See the module docs for the flow and the §19.3 safety invariants.
pub async fn run(
    conn: &mut MysqlConn,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, PoolError> {
    // (1) prepare.
    let stmt = match conn.mysql.prep(sql).await {
        Ok(s) => s,
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };

    // (2) cols from the prepared statement's columns — correct even for a zero-row result, and an
    // out-of-scope column type errors here (Unsupported) BEFORE the query runs, conn stays clean.
    let columns = stmt.columns();
    let mut cols = Vec::with_capacity(columns.len());
    for col in columns.iter() {
        cols.push(ColMeta {
            name: col.name_str().into_owned(),
            tag: rowmap::column_to_tag(col)?,
        });
    }

    // (3) bind PRE-FLIGHT, both halves raised before anything is sent, both KNOWN-FATE (never
    // ConnectionLost): the arity check, then the per-param canonical-shape check. A payload MySQL
    // cannot represent (a PG `infinity`/`NaN`, an over-range TIME) is refused HERE — passing it
    // through for the server to reject is a silent coercion under a permissive `sql_mode`.
    bind::validate_arity(params, stmt.num_params() as usize)?;
    let bound = bind::to_params(params)?;

    // (4) bind + buffered exec. `drain` confines the driver's `&mut Conn` borrow entirely to itself
    // and returns OWNED rows + affected + last_insert_id, so `conn` is free again afterward.
    let (raw_rows, affected, _last_insert_id) = match drain(conn, &stmt, bound).await {
        Ok(t) => t,
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };

    // Post-drain: record the §7.1 session-mutation taint (same as `simple_query`). `last_insert_id`
    // is intentionally NOT carried on `QueryResult` (a shared type; S7's DBAL reads it off the conn
    // via `MysqlConn::last_insert_id`, which the drain above leaves populated).
    conn.record_session_mutation();

    // Map each cell through the SAME classifier `cols` used, so rows and cols never disagree.
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(raw_rows.len());
    for row in &raw_rows {
        let mut out = Vec::with_capacity(columns.len());
        for (idx, col) in columns.iter().enumerate() {
            let cell = row.as_ref(idx).ok_or_else(|| {
                PoolError::Backend(format!(
                    "row cell index {idx} out of range (row has {} cells, statement has {} columns)",
                    row.len(),
                    columns.len()
                ))
            })?;
            out.push(rowmap::extract_value(cell, col)?);
        }
        rows.push(out);
    }

    Ok(QueryResult {
        cols,
        rows,
        affected,
    })
}

/// Bind + execute + fully drain a single result set, returning owned rows and the OK-packet
/// `affected`/`last_insert_id`. Isolates the driver's `&mut Conn` borrow so `run` can touch `conn`
/// (error mapping, taint recording) once this returns.
async fn drain(
    conn: &mut MysqlConn,
    stmt: &Statement,
    params: Params,
) -> Result<(Vec<Row>, u64, Option<u64>), mysql_async::Error> {
    let mut result = conn.mysql.exec_iter(stmt, params).await?;
    let rows = result.collect::<Row>().await?;
    let affected = result.affected_rows();
    let last_insert_id = result.last_insert_id();
    // Drain any trailing state so the conn is clean for the next statement and `last_ok_packet`
    // reflects THIS statement (what `record_session_mutation` reads).
    result.drop_result().await?;
    Ok((rows, affected, last_insert_id))
}
