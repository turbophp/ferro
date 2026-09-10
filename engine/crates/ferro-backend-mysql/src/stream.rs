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
//! ## The two shapes, and why the dispatch is on the PREPARED columns
//!
//! `stream_and_drop()` answers `None` for a statement with no result set — **and consumes the
//! `QueryResult`, closing the connection with it**. So a statement that returns no rows must never
//! take the owned route. The prepared statement tells us which shape we have before anything runs:
//! zero result columns means no result set (measured on both engines by
//! `stream_recovery_it.rs`), and that path runs BUFFERED on the borrowed conn — never parking —
//! and hands back a stream that yields nothing.
//!
//! **Known consequence, recorded rather than hidden:** a prepared `CALL` reports zero result columns
//! even when the procedure emits a result set at run time, so a `fetch:stream` `CALL` takes the
//! no-rows path and its rows are discarded with the buffered drain. The BUFFERED path has the same
//! blind spot from the same cause (it maps every row through the empty prepared-column list, so it
//! yields rows with no cells), so this is a pre-existing MySQL-backend limitation surfaced here, not
//! one introduced by streaming — the two paths merely round it off differently (0 rows vs N empty
//! rows). `ferro-classify` pins unconditionally on `CALL`/`DO` regardless, so the SESSION stays
//! safe either way.
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
use ferro_pool::backend::BackendRows;
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
    /// [`reclaim`] recovers it; the per-cell classifier needs `columns`, which is the same
    /// prepared-statement metadata `cols` was built from, so rows and cols can never disagree.
    Rows {
        stream: Box<OwnedRowStream>,
        columns: Vec<Column>,
        /// Latched once the stream yields `None`/`Err`, mirroring `PgRowStream`: a backend stream
        /// never resumes after its terminal.
        done: bool,
    },
    /// No result set (e.g. an INSERT sent with `fetch:stream`). The statement already ran BUFFERED on
    /// the borrowed conn — nothing was ever parked — so this yields no rows and carries the count
    /// straight through to [`reclaim`].
    NoRows { affected: u64 },
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
                                "row cell index {idx} out of range (row has {} cells, statement has {} columns)",
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
    let mut cols = Vec::with_capacity(columns.len());
    for col in columns.iter() {
        cols.push(ColMeta {
            name: col.name_str().into_owned(),
            tag: rowmap::column_to_tag(col)?,
        });
    }

    // (3) bind PRE-FLIGHT, both halves before anything is sent, both KNOWN-FATE (never
    // ConnectionLost) — identical to the buffered path, so a payload MySQL cannot represent is
    // refused here rather than silently coerced by a permissive `sql_mode`.
    bind::validate_arity(params, stmt.num_params() as usize)?;
    let bound = bind::to_params(params)?;

    // (4) NO RESULT SET → never park (see the module docs: `stream_and_drop` would answer `None`
    // and eat the connection). Run it buffered on the borrowed conn and yield an empty stream.
    if columns.is_empty() {
        let (_rows, affected, _lii) = match crate::query::drain(conn, &stmt, bound).await {
            Ok(t) => t,
            Err(e) => return Err(conn.map_stmt_error(&e)),
        };
        conn.record_session_mutation();
        return Ok((cols, MysqlRowStream::NoRows { affected }));
    }

    // (5) PARK: the driver handle moves into the stream for the duration. From here on, any failure
    // leaves the conn parked — which reads `is_closed`-dead, so the pool DISCARDS the husk instead
    // of recycling a connection whose session is gone (the FB-2 contract, charter rule 6).
    let owned: Conn = conn.park().ok_or_else(|| {
        PoolError::Backend(
            "query_stream on an already-parked MySQL connection (a second concurrent stream on one \
             checkout)"
                .to_string(),
        )
    })?;

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
pub async fn reclaim(conn: &mut MysqlConn, rows: MysqlRowStream) -> Result<u64, PoolError> {
    match rows {
        // Never parked: the count was already read post-drain by the buffered path.
        MysqlRowStream::NoRows { affected } => Ok(affected),
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
            conn.unpark(recovered);
            // The §7.1 assist taint, at the same post-statement point the buffered path records it.
            conn.record_session_mutation();
            Ok(affected)
        }
    }
}
