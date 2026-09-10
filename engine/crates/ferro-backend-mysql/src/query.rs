//! `PoolBackend::query` for MySQL/MariaDB (M1-S6 Task 4): the buffered, param-bound, row-returning
//! path — the MySQL counterpart of `ferro-backend-pg`'s `query::run`.
//!
//! Flow (buffered; MySQL row streaming is deferred — SPEC §22.2 (n)):
//! 1. `COM_STMT_PREPARE` the SQL (`?` is MySQL's NATIVE placeholder — no `?`→`$n` normalization,
//!    unlike PG). One round trip that yields the result-column metadata.
//! 2. Build `Vec<ColMeta>` from the prepared statement's columns via [`rowmap::column_to_tag`] — a
//!    loud `Unsupported` for any out-of-scope column type, raised BEFORE the query runs (the
//!    connection stays clean and usable), exactly like PG's cols-build.
//!
//!    **Unless the prepared statement declares NO columns**, in which case the executed result
//!    set's metadata is used instead (step 4a). A prepared `CALL` is the case that matters: MySQL
//!    reports its columns only at EXECUTION time, so the prepare-time list is empty even when the
//!    procedure emits a result set — and since the row-mapping loop below iterates that same list,
//!    an empty one yielded rows with ZERO CELLS. Measured live before this was written:
//!    `call_columns_spike_it.rs` (prepare-time 0, execution-time 2, row cells `Some(2)`).
//!
//!    The prepare-time list stays PREFERRED wherever the server provides one, and that is a fate
//!    decision rather than an optimization — see the §19.3 note on `run` itself.
//! 3. **Bind pre-flight**, both halves BEFORE anything is sent: the arity check
//!    ([`bind::validate_arity`], `params.len()` vs the statement's `num_params()`) and the per-param
//!    canonical-shape check ([`bind::to_params`]). Either failure is a KNOWN-FATE
//!    `Sql{Unsupported}`, NEVER `ConnectionLost` — the statement provably never ran, so it can never
//!    mint a false §19.3 `Indeterminate`. (MySQL prepares expose NO inferred param types, so unlike
//!    PG there is no server-side type to check the payload against — the canonical grammar is.)
//! 4. Bind + `exec_iter`, fully draining the single result set (BUFFERED — constant-memory streaming
//!    is S7). Each cell maps through [`rowmap::extract_value`]. `affected` and `last_insert_id` come
//!    off THIS statement's OK packet and both land on the returned `QueryResult` (M1-S8a).
//!    The executed set's own column metadata is read here too, and it is the fallback for step 2.
//!    It MUST be read before the drain — see the ordering note in [`drain`], which points the
//!    OPPOSITE way from the one governing `affected`/`last_insert_id` two lines below it.
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

use std::sync::Arc;

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Params, Row, Statement};

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
    let stmt = match conn.driver_mut().prep(sql).await {
        Ok(s) => s,
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };

    // (2) cols from the prepared statement's columns — correct even for a zero-row result, and an
    // out-of-scope column type errors here (Unsupported) BEFORE the query runs, conn stays clean.
    //
    // Borrowed, never cloned: `stmt` outlives every use below, and this is the hot path for every
    // buffered MySQL read.
    let prepared = stmt.columns();
    let pre_cols = if prepared.is_empty() {
        // The server declared no columns. Either the statement genuinely returns none (an INSERT,
        // and then the executed set is empty too, so this changes nothing), or it is a `CALL`,
        // whose columns MySQL reports only at execution time. Resolved after the drain, below.
        None
    } else {
        Some(build_cols(&prepared)?)
    };

    // (3) bind PRE-FLIGHT, both halves raised before anything is sent, both KNOWN-FATE (never
    // ConnectionLost): the arity check, then the per-param canonical-shape check. A payload MySQL
    // cannot represent (a PG `infinity`/`NaN`, an over-range TIME) is refused HERE — passing it
    // through for the server to reject is a silent coercion under a permissive `sql_mode`.
    bind::validate_arity(params, stmt.num_params() as usize)?;
    let bound = bind::to_params(params)?;

    // (4) bind + buffered exec. `drain` confines the driver's `&mut Conn` borrow entirely to itself
    // and returns the executed set's columns + OWNED rows + affected + last_insert_id, so `conn` is
    // free again afterward.
    let (executed, raw_rows, affected, last_insert_id) = match drain(conn, &stmt, bound).await {
        Ok(t) => t,
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };

    // Post-drain: record the §7.1 session-mutation taint (same as `simple_query`). `last_insert_id`
    // IS carried on `QueryResult` as of M1-S8a — reading it back off the conn is not an option for a
    // transaction-mode pool, where the caller's next statement lands on a different connection.
    conn.record_session_mutation();

    // (4a) Resolve which column list describes these rows. `cols` and the mapping list below are
    // produced together and are ALWAYS the same list — that is what keeps rows and cols in step.
    //
    // §19.3 / SPEC §22.2: on the `None` arm the out-of-scope-column refusal necessarily lands AFTER
    // the statement has run, because the metadata does not exist until then — "never sent" becomes
    // "sent, then refused". That is why the prepared list stays preferred rather than being dropped
    // for one uniform post-drain read: it confines the changed fate to statements the server
    // declares no columns for, which is precisely the `CALL` shape that returned cell-less rows
    // before this. The refusal is still KNOWN-FATE and never `Indeterminate` — it is raised here,
    // after a FULL drain, so the outcome is settled and the conn is clean and reusable.
    let (cols, columns): (Vec<ColMeta>, &[Column]) = match pre_cols {
        Some(cols) => (cols, &prepared),
        None => {
            let executed = executed.as_deref().unwrap_or(&[]);
            (build_cols(executed)?, executed)
        }
    };

    // Map each cell through the SAME classifier `cols` used, so rows and cols never disagree.
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(raw_rows.len());
    for row in &raw_rows {
        let mut out = Vec::with_capacity(columns.len());
        for (idx, col) in columns.iter().enumerate() {
            let cell = row.as_ref(idx).ok_or_else(|| {
                PoolError::Backend(format!(
                    "row cell index {idx} out of range (row has {} cells, result has {} columns)",
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
        last_insert_id,
    })
}

/// Builds the wire `ColMeta` list from a driver column list, refusing any out-of-scope column type
/// with a loud `Unsupported` that names the column (§9.1 — never a silent miscast).
fn build_cols(columns: &[Column]) -> Result<Vec<ColMeta>, PoolError> {
    let mut cols = Vec::with_capacity(columns.len());
    for col in columns.iter() {
        cols.push(ColMeta {
            name: col.name_str().into_owned(),
            tag: rowmap::column_to_tag(col)?,
        });
    }
    Ok(cols)
}

/// Bind + execute + fully drain a single result set, returning the EXECUTED set's column metadata,
/// owned rows, and the OK-packet `affected`/`last_insert_id`. Isolates the driver's `&mut Conn`
/// borrow so `run` can touch `conn` (error mapping, taint recording) once this returns.
///
/// The columns come back as the driver's own `Arc<[Column]>`, so handing them out is a refcount
/// bump rather than a copy of the metadata — this is on the hot path for every buffered read.
pub(crate) async fn drain(
    conn: &mut MysqlConn,
    stmt: &Statement,
    params: Params,
) -> Result<(Option<Arc<[Column]>>, Vec<Row>, u64, Option<u64>), mysql_async::Error> {
    let mut result = conn.driver_mut().exec_iter(stmt, params).await?;
    // ORDER IS LOAD-BEARING IN THE OPPOSITE DIRECTION FROM THE TWO READS BELOW, which is why this
    // is not grouped with them. `columns()` reads the driver's PENDING result set, and draining
    // clears it (`next_row` calls `set_pending_result(None)` at the terminator), so after
    // `collect()` this no longer describes the set that was just drained — it is `None` for a
    // single-set statement, and for a multi-set one (a `CALL` with several `SELECT`s) it has already
    // moved on to the NEXT set. Either way it is the wrong answer for the rows `collect()` returned.
    // Read it before the drain; read `affected`/`last_insert_id` after. Both constraints are real
    // and they point opposite ways.
    let columns = result.columns();
    let rows = result.collect::<Row>().await?;
    // ORDER IS LOAD-BEARING — read these AFTER `collect()` has drained the set. Before the drain,
    // the driver is still reporting the PREVIOUS statement's OK packet, so hoisting either read
    // above `collect()` makes a SELECT report the prior INSERT's key. Note the engines disagree
    // about how loudly: hoisting it turns MariaDB 11 RED (`left: Some(1) right: None`) while
    // MySQL 8 stays GREEN, so this is only caught because BOTH engines run in the integration
    // lane — do not "simplify" by grouping the reads next to `exec_iter`. (M1-S8a Task 2 review.)
    let affected = result.affected_rows();
    let last_insert_id = result.last_insert_id();
    // Drain any trailing state so the conn is clean for the next statement and `last_ok_packet`
    // reflects THIS statement (what `record_session_mutation` reads).
    result.drop_result().await?;
    Ok((columns, rows, affected, last_insert_id))
}
