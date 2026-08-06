//! `PoolBackend::query` for Postgres (S5): the typed, OID-strict, row-returning path.
//!
//! Flow (D-S5-1 — buffered, not streamed, in M0):
//! 1. `?`→`$n` normalize the SQL (cached).
//! 2. `prepare` it — one round trip that yields the column OIDs (so `cols` is correct even for a
//!    zero-row result) and lets PG infer the `$n` param types.
//! 3. Build `Vec<ColMeta>` from the prepared statement's columns via `rowmap::oid_to_tag` — a loud
//!    `Unsupported` for any column type outside the supported set, raised BEFORE the query runs
//!    (the connection stays clean and usable). This is the cols-build half of the two-gate pair —
//!    `rowmap::extract_value` (step 5) is the mid-stream half, and both read the one
//!    `oid_extract_type` table so `HEAD` can never promise a tag the producer cannot fill.
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

use std::pin::Pin;

use async_trait::async_trait;
use ferro_pool::backend::{BackendRows, QueryResult};
use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use ferro_proto::messages::sql::ColMeta;
use futures_util::{TryStreamExt, pin_mut};
use tokio_postgres::Client;
use tokio_postgres::types::Oid;

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
    // detects an unsupported column type before running the query (Unsupported, conn stays clean).
    let mut cols = Vec::with_capacity(stmt.columns().len());
    for col in stmt.columns() {
        // The column name + resolved `Type` (not a bare OID) so an `Unsupported` refusal names the
        // column and PG's own type name — a custom OID alone is database-local and unactionable.
        let tag = rowmap::oid_to_tag(col.name(), col.type_())?;
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

/// Runs `sql` (with `?` placeholders) + `params` against `client` and returns the prepared
/// statement's `cols` plus a LAZY [`PgRowStream`] — the incremental, constant-memory counterpart to
/// [`run`] (S5 Task 3, the `fetch:stream` producer path).
///
/// Steps 1-4 are IDENTICAL to [`run`] (see this module's flow docs): `?`→`$n` normalize; `prepare`
/// for OID-strict `cols` + PG-inferred `$n` param types (an unsupported column type is a loud
/// `Unsupported` raised BEFORE the query runs, conn stays clean); and the §19.3 bind pre-validation
/// that keeps a client-side bind fault KNOWN-FATE (`Sql{Unsupported}`) rather than the fate-unknown
/// `ConnectionLost` a post-send transport failure produces (which would mint a false
/// `WriteUnconfirmed{Indeterminate}`). The difference is step 5: the `RowStream` is BOX-PINNED
/// (`tokio_postgres::RowStream` is `!Unpin`) and handed back to be drained one row at a time by the
/// caller, NOT collected into a `Vec`.
///
/// This DELIBERATELY duplicates `run`'s prepare/pre-validate preamble rather than refactoring a
/// shared helper into it — the buffered `run` is a proven, correctness-critical path (S5 Task 2)
/// and this task's charter is to add the streaming path WITHOUT touching it.
pub async fn stream(
    client: &Client,
    sql: &str,
    params: &[Value],
) -> Result<(Vec<ColMeta>, PgRowStream), PoolError> {
    let normalized = placeholder::normalize(sql);

    let stmt = client
        .prepare(&normalized)
        .await
        .map_err(|e| error_map::map(&e))?;

    // cols + per-column OIDs, driven off the prepared statement — correct even for a zero-row
    // result, and an unsupported column type errors here (Unsupported) before the query runs.
    let mut cols = Vec::with_capacity(stmt.columns().len());
    let mut oids = Vec::with_capacity(stmt.columns().len());
    for col in stmt.columns() {
        // Same as `run`: name + resolved `Type`, so the refusal is diagnosable (see `run`).
        let tag = rowmap::oid_to_tag(col.name(), col.type_())?;
        cols.push(ColMeta {
            name: col.name().to_string(),
            tag,
        });
        oids.push(col.type_().oid());
    }

    // (4) §19.3 bind pre-validation — IDENTICAL to `run`'s step 4: reject a client-side bind fault
    // as known-fate BEFORE sending, so it is never mistaken for the fate-unknown ConnectionLost.
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
    let row_stream = client
        .query_raw(&stmt, boxed)
        .await
        .map_err(|e| error_map::map(&e))?;

    Ok((
        cols,
        PgRowStream {
            // `RowStream` is `!Unpin` (it `pin_project!`s an internal `Responses`), so it MUST be
            // box-pinned to be stored and polled across `next()` calls. It is channel-backed and
            // driven by the spawned connection task — it does NOT borrow `client`/`stmt`, and it
            // keeps its OWN `Statement` clone internally, so nothing here needs to outlive it.
            stream: Box::pin(row_stream),
            oids,
            done: false,
        },
    ))
}

/// The incremental [`BackendRows`] for Postgres (S5 Task 3). Owns a BOX-PINNED
/// `tokio_postgres::RowStream` and maps each row's cells to canonical `Value`s OID-strictly (via
/// `rowmap::extract_value`, exactly as [`run`] does), LAZILY — one row per `next()` poll. A
/// mid-stream server error maps through `error_map::map` (SQLSTATE-preserving); a client-side
/// decode mismatch surfaces as `Backend` — both mark the stream `done` (a real backend cannot
/// continue a statement past its error). `rows_affected` reads the command tag, valid only AFTER
/// `next()` has returned `None` (the post-drain rule: the `CommandComplete` count arrives in the
/// message just before the terminating `ReadyForQuery`).
pub struct PgRowStream {
    stream: Pin<Box<tokio_postgres::RowStream>>,
    oids: Vec<Oid>,
    done: bool,
}

#[async_trait]
impl BackendRows for PgRowStream {
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>> {
        if self.done {
            return None;
        }
        match self.stream.as_mut().try_next().await {
            Ok(Some(row)) => {
                let mut out = Vec::with_capacity(self.oids.len());
                for (idx, &oid) in self.oids.iter().enumerate() {
                    match rowmap::extract_value(&row, idx, oid) {
                        Ok(v) => out.push(v),
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    }
                }
                Some(Ok(out))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(error_map::map(&e)))
            }
        }
    }

    fn rows_affected(&self) -> u64 {
        self.stream.rows_affected().unwrap_or(0)
    }
}
