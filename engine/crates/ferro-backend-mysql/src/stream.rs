//! `PoolBackend::query_stream` for MySQL/MariaDB (dev-loop B2b-2b): the INCREMENTAL, constant-memory
//! row path that SPEC §22.2 (n) deferred out of M1-S6, now buildable on the three pieces the earlier
//! slices landed.
//!
//! ## Why this needed three slices to become possible
//!
//! `PoolBackend::RowStream` is an associated type with NO lifetime, so a streaming row source must
//! OWN its connection — but every `mysql_async` streaming entry point either borrows the `Conn` or
//! consumes it with no way to get it back, and dropping the stream CLOSES the connection. A pool
//! that must stream a result set and then RECYCLE the same server session was therefore locked out.
//! What unlocked it:
//!
//! * **B2a** — the vendored fork learned [`ResultSetStream::into_conn`], which drains whatever
//!   remains and hands the owned `Conn` back instead of closing it.
//! * **B2b-1** — `ferro-pool` grew `PoolBackend::reclaim_stream`, called by `RowStreamHandle::finish`
//!   AFTER the drain and BEFORE `finalize_stream` reads `tx_status(&conn)` — the one moment a
//!   conn-owning backend can put its connection back before the pin engine needs it.
//! * **B2b-2a** — [`MysqlConn`] became PARKABLE, and every method reachable while parked answers
//!   safely (`is_closed` → dead above all, which is what makes the pool DISCARD a husk rather than
//!   recycle it — the FB-2 contract).
//!
//! ## The two shapes, and where the dispatch reads them (M2/S3)
//!
//! `stream_and_drop()` answers `None` for a statement with no result set — **and consumes the
//! `QueryResult`, closing the connection with it**. So a statement that returns no rows must never
//! reach that call. What decides it is the result-column metadata, and MySQL publishes that in two
//! different places depending on the statement:
//!
//! * **The prepared statement declares columns** (every ordinary `SELECT`). Dispatch on that, before
//!   anything runs: park, run, `stream_and_drop`. This is the hot path and S3 left it untouched.
//! * **The prepared statement declares none.** That covers BOTH a genuine no-result-set statement
//!   (an `INSERT`) and a `CALL`, whose columns MySQL reports only at EXECUTION time — and they need
//!   opposite handling. Nothing available before the statement runs separates them, so this arm
//!   parks, RUNS, and reads `QueryResult::columns_ref()`: a non-empty executed set streams (a `CALL`
//!   now yields real rows, incrementally), an empty one hands the connection straight back.
//!
//! Parking before knowing which shape it is only became safe with the B2a fork's
//! [`mysql_async::QueryResult::into_conn`], whose docblock names exactly this case: the owned-route
//! exit for a statement that produced no result set. Without it, guessing wrong ate the connection.
//!
//! **S3 closed the `CALL` blind spot on this path; S2 closed it on the buffered one** (SPEC §22.2
//! (av)). Both pay the same narrow §19.3 trade, and only on the empty-prepared-list arm: the
//! metadata does not exist until the statement has run, so an out-of-scope column type is refused
//! AFTER the send rather than before it. The refusal stays KNOWN-FATE, never `Indeterminate`, and
//! the connection is handed back clean rather than discarded. `ferro-classify` pins unconditionally
//! on `CALL`/`DO` regardless, so the SESSION is safe on every arm.
//!
//! **Still open (S4):** a multi-`SELECT` procedure streams only its FIRST result set; the rest are
//! drained by `into_conn`. `ExecOk` carries exactly one `cols`+`rows`, so representing more is a
//! `/proto` change.
//!
//! ## The affected-row rule (§22.2 (n), measured)
//!
//! `ResultSetStream::affected_rows()` reports the ok-packet captured at stream SETUP — the PREVIOUS
//! statement's count. The truthful count is read from the CONNECTION after the drain, which is
//! exactly where [`reclaim`] reads it. [`MysqlRowStream::rows_affected`] therefore answers `0` and
//! is never the authority for this backend. Note the documented cross-backend divergence: a MySQL
//! SELECT's post-drain packet reports `affected = 0`, where PostgreSQL's command tag reports the row
//! count.

use futures_util::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::prelude::{Query, WithParams};
use mysql_async::{BinaryProtocol, Column, Conn, ResultSetStream, Row};

use async_trait::async_trait;
use ferro_pool::backend::{BackendRows, Reclaimed};
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;

use crate::Value;
use crate::conn::MysqlConn;
use crate::{bind, rowmap};

/// The owned stream handed to `ferro-pool` (B2b-2b). `'static` throughout: it owns the `Conn`, so it
/// borrows nothing and satisfies `BackendRows: Send` for the producer's `tokio::spawn`.
type OwnedRowStream = ResultSetStream<'static, 'static, 'static, Row, BinaryProtocol>;

/// The incremental MySQL/MariaDB row source. Two shapes — see the module docs for why the no-rows
/// one must never take the owned route.
pub enum MysqlRowStream {
    /// A real result set. **Owns the driver connection** (parked out of [`MysqlConn`]) until
    /// [`reclaim`] recovers it; the per-cell classifier needs `columns`, which is ALWAYS the very
    /// list `cols` was built from, so rows and cols can never disagree. Since M2/S3 that list is
    /// the prepared metadata for an ordinary `SELECT` and the EXECUTED metadata for a `CALL` — the
    /// invariant is that the two are produced together, not that they come from one source.
    Rows {
        stream: Box<OwnedRowStream>,
        columns: Vec<Column>,
        /// Latched once the stream yields `None`/`Err`, mirroring `PgRowStream`: a backend stream
        /// never resumes after its terminal.
        done: bool,
    },
    /// No result set (e.g. an INSERT sent with `fetch:stream`). This yields no rows and carries the
    /// count AND THE GENERATED KEY straight through to [`reclaim`]. The key matters precisely here:
    /// an `INSERT` is the no-result-set shape, and `lastInsertId()` is what reads it.
    ///
    /// **Contract corrected at M2/S3:** this used to be decided on the PREPARED column count, and
    /// ran buffered on a never-parked connection. It is now decided on the EXECUTED set — because
    /// an empty prepared list also describes a `CALL`, which does return rows — so reaching this
    /// variant means the connection WAS parked, ran, reported no result set, and was handed back
    /// via `into_conn` before `open` returned. Both values below were read from that recovered
    /// connection post-drain, the same rule [`reclaim`] uses.
    NoRows {
        affected: u64,
        last_insert_id: Option<u64>,
    },
}

#[async_trait]
impl BackendRows for MysqlRowStream {
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>> {
        let MysqlRowStream::Rows {
            stream,
            columns,
            done,
        } = self
        else {
            return None;
        };
        if *done {
            return None;
        }
        match stream.next().await {
            Some(Ok(row)) => {
                let mut out = Vec::with_capacity(columns.len());
                for (idx, col) in columns.iter().enumerate() {
                    let cell = match row.as_ref(idx) {
                        Some(c) => c,
                        None => {
                            *done = true;
                            return Some(Err(PoolError::Backend(format!(
                                "row cell index {idx} out of range (row has {} cells, result has {} columns)",
                                row.len(),
                                columns.len()
                            ))));
                        }
                    };
                    match rowmap::extract_value(cell, col) {
                        Ok(v) => out.push(v),
                        Err(e) => {
                            // A cell we cannot represent ends the stream: the row is already off the
                            // wire, and continuing would silently skip it.
                            *done = true;
                            return Some(Err(e));
                        }
                    }
                }
                Some(Ok(out))
            }
            Some(Err(e)) => {
                *done = true;
                Some(Err(crate::error_map::map(&e)))
            }
            None => {
                *done = true;
                None
            }
        }
    }

    /// Always `0` for this backend — deliberately. The stream's own `affected_rows()` reports the
    /// PREVIOUS statement's ok-packet (§22.2 (n), measured), and the truthful post-drain count is
    /// read from the CONNECTION in [`reclaim`], which is what `RowStreamHandle::finish` uses. Never
    /// a fabricated value: `0` here is "ask the reclaim hook", not "no rows were affected".
    fn rows_affected(&self) -> u64 {
        0
    }
}

/// Builds the wire `ColMeta` list from a driver column list, refusing an out-of-scope column type
/// with a loud `Unsupported` naming the column (§9.1 — never a silent miscast).
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

/// A second concurrent stream on one checkout — the conn is already parked, so there is nothing to
/// hand to this one.
fn park_conflict() -> PoolError {
    PoolError::Backend(
        "query_stream on an already-parked MySQL connection (a second concurrent stream on one \
         checkout)"
            .to_string(),
    )
}

/// `PoolBackend::query_stream` for MySQL/MariaDB. See the module docs for the dispatch and the
/// ownership handshake.
pub async fn open(
    conn: &mut MysqlConn,
    sql: &str,
    params: &[Value],
) -> Result<(Vec<ColMeta>, MysqlRowStream), PoolError> {
    // (1) prepare — one round trip, on the still-borrowed conn.
    let stmt = match conn.driver_mut().prep(sql).await {
        Ok(s) => s,
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };

    // (2) cols from the prepared statement (correct even for a zero-row result); an out-of-scope
    // column type is a loud `Unsupported` HERE, before anything runs, with the conn still clean.
    let columns: Vec<Column> = stmt.columns().to_vec();
    let cols = build_cols(&columns)?;

    // (3) bind PRE-FLIGHT, both halves before anything is sent, both KNOWN-FATE (never
    // ConnectionLost) — identical to the buffered path, so a payload MySQL cannot represent is
    // refused here rather than silently coerced by a permissive `sql_mode`.
    bind::validate_arity(params, stmt.num_params() as usize)?;
    let bound = bind::to_params(params)?;

    // (4) The prepared statement declared NO result columns. That is TWO different statements —
    // one with genuinely no result set (an INSERT), and a `CALL`, whose columns MySQL reports only
    // at EXECUTION time — and they need opposite handling. Nothing available before the statement
    // runs can tell them apart, so this arm parks, RUNS, and asks the executed result which it is.
    //
    // Parking first is safe precisely because the B2a fork exposes `QueryResult::into_conn`, whose
    // own docblock names this case: it is the owned-route exit for a statement that produced no
    // result set, where `stream_and_drop` would answer `None` and close the connection with it.
    if columns.is_empty() {
        let owned: Conn = conn.park().ok_or_else(park_conflict)?;
        let qr = match stmt.with(bound).run(owned).await {
            Ok(qr) => qr,
            // The owned conn was consumed by the failed run; the wrapper stays parked and the pool
            // discards the husk (the FB-2 contract).
            Err(e) => return Err(conn.map_stmt_error(&e)),
        };

        // Read the EXECUTED metadata before anything consumes the `QueryResult`.
        let executed: Vec<Column> = qr.columns_ref().to_vec();

        if executed.is_empty() {
            // Genuinely no result set. Hand the connection straight back rather than streaming
            // nothing: `into_conn` drains and returns it, and its post-drain packet is THIS
            // statement's, so both values are read here on the same rule `reclaim` uses.
            let recovered = match qr.into_conn().await {
                Ok(c) => c,
                Err(e) => return Err(crate::error_map::map(&e)),
            };
            let affected = recovered.affected_rows();
            let last_insert_id = recovered.last_insert_id();
            conn.unpark(recovered);
            conn.record_session_mutation();
            return Ok((
                cols,
                MysqlRowStream::NoRows {
                    affected,
                    last_insert_id,
                },
            ));
        }

        // A `CALL` that really did emit a result set. Build `cols` from the executed metadata — the
        // §19.3 trade SPEC §22.2 (av) records for the buffered path applies here for the same
        // reason and only on this arm: the metadata does not exist until the statement has run, so
        // an out-of-scope column type is refused AFTER the send rather than before it. The refusal
        // stays KNOWN-FATE, and the connection is handed back CLEAN rather than discarded.
        let call_cols = match build_cols(&executed) {
            Ok(c) => c,
            Err(e) => {
                // On an `Err` here the conn cannot be recovered, so it is left PARKED and the
                // pool discards the husk. Either way the caller sees the `Unsupported`, which is
                // the more informative of the two errors and the one that is actionable.
                if let Ok(recovered) = qr.into_conn().await {
                    conn.unpark(recovered);
                    conn.record_session_mutation();
                }
                return Err(e);
            }
        };

        let stream = match qr.stream_and_drop::<Row>().await {
            Ok(Some(s)) => s,
            // Unreachable: `columns_ref` just reported a non-empty executed set. Handled as a real
            // error rather than `unreachable!` so a surprising server never panics ferrod.
            Ok(None) => {
                return Err(PoolError::Backend(
                    "MySQL reported no result set for a statement whose EXECUTED metadata declared \
                     result columns"
                        .to_string(),
                ));
            }
            Err(e) => return Err(conn.map_stmt_error(&e)),
        };

        return Ok((
            call_cols,
            MysqlRowStream::Rows {
                stream: Box::new(stream),
                columns: executed,
                done: false,
            },
        ));
    }

    // (5) PARK: the driver handle moves into the stream for the duration. From here on, any failure
    // leaves the conn parked — which reads `is_closed`-dead, so the pool DISCARDS the husk instead
    // of recycling a connection whose session is gone (the FB-2 contract, charter rule 6).
    let owned: Conn = conn.park().ok_or_else(park_conflict)?;

    let qr = match stmt.with(bound).run(owned).await {
        Ok(qr) => qr,
        // The owned conn was consumed by the failed run and is gone; the wrapper stays parked.
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };
    let stream = match qr.stream_and_drop::<Row>().await {
        Ok(Some(s)) => s,
        // Unreachable: we only take this route when the prepared statement HAS result columns.
        // Handled as a real error (not `unreachable!`) so a surprising server never panics ferrod;
        // the conn is gone with the consumed QueryResult, and the parked wrapper is discarded.
        Ok(None) => {
            return Err(PoolError::Backend(
                "MySQL reported no result set for a statement whose prepared metadata declared \
                 result columns"
                    .to_string(),
            ));
        }
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };

    Ok((
        cols,
        MysqlRowStream::Rows {
            stream: Box::new(stream),
            columns,
            done: false,
        },
    ))
}

/// `PoolBackend::reclaim_stream` for MySQL/MariaDB: drain whatever remains, put the connection BACK
/// (ending the parked window), and answer the affected count from the CONNECTION's post-drain packet
/// — the §22.2 (n) rule. Called by `RowStreamHandle::finish` before `finalize_stream` reads
/// `tx_status`, and itself bounded by the pool (FB-3), so a hung restore cannot strand the terminal.
pub async fn reclaim(conn: &mut MysqlConn, rows: MysqlRowStream) -> Result<Reclaimed, PoolError> {
    match rows {
        // Never parked: both values were already read post-drain by the buffered path.
        MysqlRowStream::NoRows {
            affected,
            last_insert_id,
        } => Ok(Reclaimed {
            affected,
            last_insert_id,
        }),
        MysqlRowStream::Rows { stream, .. } => {
            // `into_conn` (the B2a fork edit) drains every unconsumed row and pending result set,
            // then returns the owned `Conn` instead of closing it.
            let recovered = match stream.into_conn().await {
                Ok(c) => c,
                // The connection could not be recovered. Leave the wrapper PARKED: `is_closed`
                // reports it dead, so the pool discards it rather than handing on a husk. This is
                // the arm the reclaim contract exists for.
                Err(e) => return Err(crate::error_map::map(&e)),
            };
            // Post-drain, and only now: the connection's own accessor carries THIS statement's final
            // packet (the stream's would carry the previous statement's — §22.2 (n)).
            let affected = recovered.affected_rows();
            // Same post-drain rule for the generated key: read from the CONNECTION, whose final
            // packet is this statement's. A row-returning statement normally has none, but reading
            // it here rather than assuming `None` keeps the two arms honest and identical.
            let last_insert_id = recovered.last_insert_id();
            conn.unpark(recovered);
            // The §7.1 assist taint, at the same post-statement point the buffered path records it.
            conn.record_session_mutation();
            Ok(Reclaimed {
                affected,
                last_insert_id,
            })
        }
    }
}
