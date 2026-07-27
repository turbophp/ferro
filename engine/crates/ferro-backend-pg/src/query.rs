//! `PoolBackend::query` for Postgres (S5): the typed, OID-strict, row-returning path.
//!
//! Flow (D-S5-1 — buffered, not streamed, in M0):
//! 1. `?`→`$n` normalize the SQL (cached).
//! 2. `prepare` it — one round trip that yields the column OIDs (so `cols` is correct even for a
//!    zero-row result) and lets PG infer the `$n` param types.
//! 3. Build `Vec<ColMeta>` from the prepared statement's columns via `rowmap::oid_to_tag` — a loud
//!    `Unsupported` for any out-of-M0 column type, raised BEFORE the query runs (the connection
//!    stays clean and usable).
//! 4. **Pre-validate the bind (S5 review fix — §19.3 safety).** BEFORE sending anything, check the
//!    param arity (`params.len() == stmt.params().len()`) and that each param's `ToSql` impl
//!    `accepts` the statement's inferred parameter type. A failure here is a client-side BIND error
//!    whose `tokio_postgres::Error` would carry `as_db_error() == None` — which `error_map::map`
//!    (correctly, for a real transport failure) classifies as the fate-unknown `ConnectionLost`.
//!    But a bind error means the statement **provably never executed**, so surfacing it as
//!    `ConnectionLost` would let the SQL service mint a FALSE `WriteUnconfirmed{Indeterminate}` for
//!    a write that never happened (§19.3). We instead raise a KNOWN-FATE `PoolError::Sql` with the
//!    `Unsupported` code — never `ConnectionLost`. (Mirrors `rowmap`, which routes `try_get`
//!    client-side decode errors to `Backend`, not `error_map`, for the same reason.)
//! 5. Bind params and run `query_raw`, collecting ALL rows (buffered) with OID-strict extraction.
//!    A remaining `as_db_error() == None` error HERE is now genuinely a post-send transport failure
//!    (the pre-validation above already excluded every client-side bind fault) → `ConnectionLost`
//!    is the correct, honest classification.
//! 6. `affected` comes from the command tag (`RowStream::rows_affected()`) — NEVER a hardcoded 0
//!    (the S4 `batch_execute` defect). Note PG's command tag reports the ROW COUNT for a SELECT too
//!    (so a `SELECT 1` yields `affected == 1` alongside its one row); a DML reports its affected-row
//!    count with an empty row set. `query` always returns rows + affected + cols; the SELECT-vs-DML
//!    shaping (`fetch:none` drops the rows, keeps `affected`) is the SERVICE's job (Task 3).
//!
//! **M0 bind mapping (deliberate, fails LOUDLY):** the canonical scalars map DIRECTLY —
//! `I64`→`int8`, `F64`→`float8`, `Bool`→`bool`, `Text`→`text`, `Bytes`→`bytea`. A narrower target
//! column (`int4`/`int2`/`float4`/`serial`) needs a client-side cast; without one, step 4 rejects
//! the bind as a known-fate `Unsupported` — a clear, diagnosable error, NEVER a silent miscast and
//! NEVER a fate-unknown `Indeterminate`. Widening these binds (client cast injection, or binding
//! `I64` as `int4` when the target is narrower) is post-M0.
//!
//! All server-side errors go through `error_map::map` (`as_db_error()`-first). Nothing here
//! re-runs the statement (charter rule 3).

use ferro_pool::backend::QueryResult;
use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use futures_util::{TryStreamExt, pin_mut};
use tokio_postgres::Client;

use crate::Value;
use crate::{bind, error_map, placeholder, rowmap};

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

    // (4) Pre-validate the bind BEFORE sending, so a client-side bind fault is known-fate, never
    // the fate-unknown ConnectionLost that a post-send transport failure produces (§19.3). See the
    // module docs: this is the exact predicate `query_raw`'s own `to_sql_checked` would apply, run
    // one step earlier so its failure is diagnosable rather than mistaken for a lost connection.
    let expected = stmt.params();
    if params.len() != expected.len() {
        return Err(bind_error(format!(
            "parameter count mismatch: got {}, statement expects {}",
            params.len(),
            expected.len()
        )));
    }
    for (i, (v, ty)) in params.iter().zip(expected).enumerate() {
        if !bind::accepts(v, ty) {
            return Err(bind_error(format!(
                "parameter {i} type mismatch: canonical {} cannot bind to PG type {} \
                 (M0 maps I64->int8 / F64->float8 directly; a narrower column needs a client cast)",
                bind::value_kind(v),
                ty.name()
            )));
        }
    }

    let boxed = bind::to_boxed_params(params);
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

/// A KNOWN-FATE bind error: a `PoolError::Sql` carrying the `Unsupported` code/branch and a clear
/// message. Deliberately NOT `PoolError::ConnectionLost` — a bind fault (wrong arity / uncastable
/// param) is caught before the statement is ever sent, so its fate is known (it never executed).
/// The service maps a `Sql` error verbatim and applies NO `readonly`→`Indeterminate` override, so
/// this can never surface as a false `WriteUnconfirmed{Indeterminate}` (§19.3). `sqlstate` is
/// `None`: there is no server SQLSTATE because the server never saw the statement.
fn bind_error(message: String) -> PoolError {
    PoolError::Sql {
        code: errc::UNSUPPORTED,
        branch: errc::UNSUPPORTED_BRANCH,
        sqlstate: None,
        message,
    }
}
