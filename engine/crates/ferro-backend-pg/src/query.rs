//! `PoolBackend::query` for Postgres (S5): the typed, OID-strict, row-returning path.
//!
//! Flow (D-S5-1 — buffered, not streamed, in M0):
//! 1. `?`→`$n` normalize the SQL (cached).
//! 2. `prepare` it — one round trip that yields the column OIDs (so `cols` is correct even for a
//!    zero-row result) and lets PG infer the `$n` param types.
//! 3. Build `Vec<ColMeta>` from the prepared statement's columns via `rowmap::oid_to_tag` — a loud
//!    `Unsupported` for any out-of-M0 column type, raised BEFORE the query runs (the connection
//!    stays clean and usable).
//! 4. Bind params and run `query_raw`, collecting ALL rows (buffered) with OID-strict extraction.
//! 5. `affected` comes from the command tag (`RowStream::rows_affected()`) — NEVER a hardcoded 0
//!    (the S4 `batch_execute` defect). A SELECT reports 0 affected with its rows; a DML reports its
//!    row count with an empty row set. `query` always returns rows + affected + cols; the
//!    SELECT-vs-DML shaping (`fetch:none`) is the SERVICE's job (Task 3).
//!
//! All server-side errors go through `error_map::map` (`as_db_error()`-first). Nothing here
//! re-runs the statement (charter rule 3).

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use futures_util::{TryStreamExt, pin_mut};
use tokio_postgres::Client;

use crate::Value;
use crate::{error_map, placeholder, rowmap};

/// Runs `sql` (with `?` placeholders) and `params` against `client`, buffering the full result.
pub async fn run(client: &Client, sql: &str, params: &[Value]) -> Result<QueryResult, PoolError> {
    let normalized = placeholder::normalize(sql);

    let stmt = client
        .prepare(&normalized)
        .await
        .map_err(|e| error_map::map(&e))?;

    // Build cols from the prepared statement's columns — correct even for a zero-row result, and
    // detects an out-of-M0 column type before running the query (Unsupported, conn stays clean).
    let mut cols = Vec::with_capacity(stmt.columns().len());
    for col in stmt.columns() {
        let tag = rowmap::oid_to_tag(col.type_().oid())?;
        cols.push(ferro_proto::messages::sql::ColMeta {
            name: col.name().to_string(),
            tag,
        });
    }

    let boxed = crate::bind::to_boxed_params(params);
    let stream = client
        .query_raw(&stmt, boxed)
        .await
        .map_err(|e| error_map::map(&e))?;
    pin_mut!(stream);

    let mut rows: Vec<Vec<Value>> = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(|e| error_map::map(&e))? {
        let mut out_row = Vec::with_capacity(row.columns().len());
        for (idx, col) in row.columns().iter().enumerate() {
            out_row.push(rowmap::extract_value(&row, idx, col.type_().oid())?);
        }
        rows.push(out_row);
    }

    // `rows_affected()` is `None` until the stream is exhausted; the loop above exhausts it, so a
    // remaining `None` (e.g. an empty-query response) means zero rows affected.
    let affected = stream.rows_affected().unwrap_or(0);

    Ok(QueryResult {
        cols,
        rows,
        affected,
    })
}
