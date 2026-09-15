//! **C3-5: the incremental row stream.** The producer half of `fetch:stream` for SQLite, and the
//! `reclaim_stream` that hands the connection back afterwards.
//!
//! # Why the stream OWNS the connection
//!
//! It has no choice. `rusqlite::Statement` and `Rows` both BORROW the `Connection`, so a stream
//! that yields rows one at a time must keep the connection alive and borrowed for its whole life —
//! and `Connection` is `Send` but not `Sync`, so that life has to happen on ONE blocking thread.
//! The connection is therefore moved out of [`SqliteConn`] at `query_stream` and moved back at
//! `reclaim_stream`, which is precisely the conn-owning shape MySQL needed at B2b-2a and the reason
//! `PoolBackend::reclaim_stream` exists at all (SPEC §22.2 (n)).
//!
//! # Non-buffering is a property of the channel, and it is the point
//!
//! The blocking task sends each row over a **capacity-1** bounded channel and parks on the first
//! unread one. That is C3-1's `p2` shape unchanged, including its lesson: `p2`'s first version
//! asserted the producer's row count immediately after taking a few rows, which proves nothing
//! (the producer simply had not had time to run away) and passed under an unbounded-channel
//! mutation. The live test here uses the same stall probe `p2` ended up with.
//!
//! # Abandonment
//!
//! A dropped receiver makes `blocking_send` fail, the loop breaks, and the task returns the
//! connection as usual — so an abandoned stream costs nothing extra. Dropping the whole
//! [`SqliteRowStream`] without reclaiming is the other arm: the join handle goes with it, the
//! detached task finishes and drops the `Connection`, and `SqliteConn` is left handle-less, which
//! `is_closed` reports as dead. That is exactly what `RowStreamHandle::finish` relies on when a
//! drain times out — it drops the stream and calls that "the kill" — so the pool discards the husk
//! instead of recycling a mid-result-set session (charter rule 6).

use ferro_pool::backend::{BackendRows, Reclaimed};
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;
use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::conn::SqliteConn;

/// Rows in flight between the blocking producer and the async consumer.
///
/// **One, deliberately.** The producer parks on the first row the consumer has not taken, which is
/// what makes "constant engine memory" true of the backend and not merely of the frames above it.
/// A larger buffer would be a throughput knob traded against exactly the property §14's never-buffer
/// clause asks for, and charter rule 5 says not to make that trade without a bench number.
const ROW_CHANNEL_CAPACITY: usize = 1;

/// What the blocking task hands back when it is done: the connection plus the counters that can
/// only be read on the connection AFTER the statement has finished.
struct StreamReturn {
    conn: Connection,
    affected: u64,
    last_insert_id: Option<u64>,
}

/// An incremental SQLite row stream (C3-5).
///
/// Until this slice the type wrapped [`std::convert::Infallible`] so that `query_stream`'s
/// `Unsupported` was enforced by the type system rather than by a runtime panic (§22.2 (bg)). It is
/// real now, and `supports_row_streaming` flipped with it — both halves in one change, as (bh) said
/// they must.
pub struct SqliteRowStream {
    rx: mpsc::Receiver<Result<Vec<Value>, PoolError>>,
    /// `None` once reclaimed. Dropping it while `Some` detaches the producer, which then drops the
    /// connection — the abandonment kill described in the module docs.
    join: Option<JoinHandle<Option<StreamReturn>>>,
    /// Filled by `reclaim_stream` from the connection's own post-drain counters. The trait's
    /// `rows_affected()` reads this, and it is 0 until then — which is correct rather than a
    /// placeholder: SQLite's `changes()` reports the LAST data-modifying statement and goes stale
    /// (§22.2 (be)), so a value read before the statement finished would be the previous one's.
    affected: u64,
}

#[async_trait::async_trait]
impl BackendRows for SqliteRowStream {
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>> {
        self.rx.recv().await
    }

    fn rows_affected(&self) -> u64 {
        self.affected
    }
}

/// Start an incremental query. Moves the connection out of `conn` and into a blocking task that
/// owns it until [`reclaim`] takes it back.
///
/// `cols` is resolved BEFORE the first row, from the prepared statement, and awaited here — so a
/// prepare or bind failure surfaces as an ordinary `Err` from `query_stream` rather than as a
/// first-row error the session layer would have to unwind mid-stream.
pub async fn start(
    conn: &mut SqliteConn,
    sql: &str,
    params: &[Value],
) -> Result<(Vec<ColMeta>, SqliteRowStream), PoolError> {
    let bound: Vec<rusqlite::types::Value> = params
        .iter()
        .map(crate::rowmap::param_from_value)
        .collect::<Result<_, _>>()?;

    let Some(c) = conn.park_for_stream() else {
        // No live handle: the connection is parked by another call or was lost to a panicking
        // blocking task. Either way there is nothing to stream from, and `is_closed` already
        // reports it, so the pool will discard rather than recycle.
        return Err(PoolError::ConnectionLost);
    };

    let sql = sql.to_string();
    let (row_tx, rx) = mpsc::channel::<Result<Vec<Value>, PoolError>>(ROW_CHANNEL_CAPACITY);
    let (col_tx, col_rx) = oneshot::channel::<Result<Vec<ColMeta>, PoolError>>();

    let join = tokio::task::spawn_blocking(move || {
        let before_changes = c.total_changes();
        let before_rowid = c.last_insert_rowid();

        // The producing work is a separate fn taking `&Connection` so that `stmt`/`rows` — and with
        // them the borrow of the connection — are provably gone before it is moved into the return
        // value below. Written inline first, every early-return arm was a borrow-check error: the
        // connection cannot be handed back from inside the scope that borrows it, which is the same
        // constraint that makes this a conn-owning backend in the first place.
        produce(&c, &sql, &bound, col_tx, row_tx);

        // Counters read AFTER the statement finished, on the connection, exactly as the buffered
        // path does: the `total_changes()` delta says whether anything changed and `changes()` says
        // how many, because on its own each is wrong in a different direction (§22.2 (be)).
        let affected = if c.total_changes() == before_changes {
            0
        } else {
            c.changes()
        };
        let rowid = c.last_insert_rowid();
        let last_insert_id = if rowid != before_rowid && rowid > 0 {
            Some(rowid as u64)
        } else {
            None
        };
        Some(StreamReturn {
            conn: c,
            affected,
            last_insert_id,
        })
    });

    match col_rx.await {
        Ok(Ok(cols)) => Ok((
            cols,
            SqliteRowStream {
                rx,
                join: Some(join),
                affected: 0,
            },
        )),
        // The statement failed before producing columns. Reclaim the connection here rather than
        // leaving it with the detached task, so an ordinary syntax error costs no connection.
        Ok(Err(e)) => {
            if let Some(ret) = join.await.ok().flatten() {
                conn.unpark_from_stream(ret.conn);
            }
            Err(e)
        }
        // The task died without sending — a panic in the blocking closure. The connection went
        // with it, and `conn` is left handle-less, which `is_closed` reports.
        Err(_) => Err(PoolError::ConnectionLost),
    }
}

/// Join the producer, put the connection back, and answer `affected`/`last_insert_id` from the
/// connection's own post-drain counters.
///
/// An `Err` here must leave `conn` reporting dead, which it does by construction: the handle is
/// only ever restored on the success arm, so a producer that panicked or vanished leaves
/// `SqliteConn` handle-less and the pool discards it (the trait's stated contract).
pub async fn reclaim(
    conn: &mut SqliteConn,
    mut rows: SqliteRowStream,
) -> Result<Reclaimed, PoolError> {
    // Drop the receiver first: if the producer is parked on a full channel it would otherwise never
    // observe the abandonment and the join below would wait for a consumer that is gone.
    rows.rx.close();

    let Some(join) = rows.join.take() else {
        return Err(PoolError::ConnectionLost);
    };
    match join.await {
        Ok(Some(ret)) => {
            conn.unpark_from_stream(ret.conn);
            Ok(Reclaimed {
                affected: ret.affected,
                last_insert_id: ret.last_insert_id,
            })
        }
        Ok(None) | Err(_) => Err(PoolError::ConnectionLost),
    }
}

/// The blocking producer body. Takes the connection by REFERENCE so every borrow it creates is
/// released when it returns, which is what lets the caller move the connection into its return
/// value; see the comment at the call site.
fn produce(
    c: &Connection,
    sql: &str,
    bound: &[rusqlite::types::Value],
    col_tx: oneshot::Sender<Result<Vec<ColMeta>, PoolError>>,
    row_tx: mpsc::Sender<Result<Vec<Value>, PoolError>>,
) {
    let mut stmt = match c.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            let _ = col_tx.send(Err(crate::error_map::map(&e)));
            return;
        }
    };
    let ncol = stmt.column_count();
    let names: Vec<String> = (0..ncol)
        .map(|i| stmt.column_name(i).unwrap_or_default().to_string())
        .collect();

    let mut rows = match stmt.query(rusqlite::params_from_iter(bound.iter())) {
        Ok(r) => r,
        Err(e) => {
            let _ = col_tx.send(Err(crate::error_map::map(&e)));
            return;
        }
    };

    // `tag: NULL` on every column, and that is not a shortfall. C3-3c established that the decode
    // authority is the PER-CELL tag and that `ColMeta.tag` has NO consumer — the PHP client drops
    // it in both paths by design. The buffered path fills it from the first row because it happens
    // to have one in hand; a stream does not, and peeking a row just to populate a field nobody
    // reads would buy nothing and cost the first row's latency. Documented there as advisory;
    // documented here as why the two differ.
    let cols: Vec<ColMeta> = names
        .into_iter()
        .map(|name| ColMeta {
            name,
            tag: ferro_proto::consts::tag::NULL,
        })
        .collect();
    if col_tx.send(Ok(cols)).is_err() {
        // The caller went away before the first row: nothing to produce for.
        return;
    }

    loop {
        let item = match rows.next() {
            Ok(None) => break,
            Ok(Some(r)) => {
                let mut row = Vec::with_capacity(ncol);
                let mut bad = None;
                for i in 0..ncol {
                    match r.get_ref(i) {
                        Ok(v) => row.push(crate::rowmap::value_from_ref(v)),
                        Err(e) => {
                            bad = Some(crate::error_map::map(&e));
                            break;
                        }
                    }
                }
                match bad {
                    Some(e) => Err(e),
                    None => Ok(row),
                }
            }
            // A step error ends the stream. `SQLITE_INTERRUPT` (9) arrives here when the request's
            // cancel fired — the ONLY mechanism that can stop a statement on this backend (§22.2
            // (bg)) — and rides the channel as an ordinary Err, so the handle records `errored` and
            // the §19.3 matrix classifies it as it would any other statement failure.
            Err(e) => Err(crate::error_map::map(&e)),
        };
        let fatal = item.is_err();
        if row_tx.blocking_send(item).is_err() {
            // Consumer abandoned: stop stepping rather than running the result to completion for
            // nobody.
            break;
        }
        if fatal {
            break;
        }
    }
}
