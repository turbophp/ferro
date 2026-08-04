//! `ferro-backend-mysql`: the MySQL/MariaDB `PoolBackend` (M1-S6).
//!
//! This crate is where `ferro-pool`'s hand-rolled pool mechanics will connect to a real MySQL
//! server. The full `MysqlBackend` skeleton lands in the next task; **this** slice's task 1 is the
//! load-bearing FORK that everything else gates on.
//!
//! ## The fork this crate exercises (M1-S6 task 1)
//!
//! Ferro's MySQL pin engine reads MySQL's **OK-packet session trackers** as the authoritative
//! signal for protocol-invisible session mutations — the direct analog of Postgres's
//! `ReadyForQuery` transaction-status byte (M1-S1). Those trackers only appear on the wire when the
//! client advertised the `CLIENT_SESSION_TRACK` capability at handshake. Stock `mysql_async` never
//! advertises that bit and exposes no `Opts`/`OptsBuilder` hook to add it, so
//! [`mysql_async::OkPacket::session_state_info`] is *always empty* on a stock connection.
//!
//! Ferro therefore vendors a one-bit fork of `mysql_async` (`vendor/mysql-async`, wired via the root
//! `Cargo.toml` `[patch.crates-io]`; see `/UPSTREAM_PR_MYSQL_ASYNC.md`) that ORs `CLIENT_SESSION_TRACK`
//! into `Opts::get_capabilities()`. This crate depends on that patched build; the behavioral spike in
//! `tests/tracker_spike_it.rs` proves — live — that the trackers actually fire.

use async_trait::async_trait;

use ferro_pool::backend::{
    BackendRows, Cancel, Dialect, PoolBackend, QueryResult, ResetProfile, TxStatus,
};
use ferro_pool::error::PoolError;
use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

// Re-export the OK-packet tracker surface the MySQL pin engine (task 2+) reads, so downstream
// modules name it without reaching through `mysql_async`'s re-export path. These are the types the
// `CLIENT_SESSION_TRACK` fork makes *non-empty*.
pub use mysql_async::{
    OkPacket, SessionStateChange, SessionStateInfo, SystemVariable, TransactionState,
};

/// The upstream SQL dialect this backend speaks — a per-backend constant (the pin engine keys the
/// assist lexer off it, M1-S2).
pub const DIALECT: Dialect = Dialect::MySql;

/// The MySQL OK-packet `SERVER_STATUS_IN_TRANS` flag (0x0001) — set while a transaction block is
/// open. The MySQL analog of Postgres's `T` (in-transaction) `ReadyForQuery` byte; the pin engine
/// (task 2+) reads it off [`OkPacket::status_flags`] as the transaction-state authority.
pub const SERVER_STATUS_IN_TRANS: mysql_common::constants::StatusFlags =
    mysql_common::constants::StatusFlags::SERVER_STATUS_IN_TRANS;

/// Decode every session-state tracker on an OK packet into typed [`SessionStateChange`] values.
///
/// On the FORKED (`CLIENT_SESSION_TRACK`-negotiating) build this is non-empty after a session
/// mutation (e.g. `SET SESSION …`); on stock `mysql_async` it is always empty. Errors from a
/// malformed tracker blob are dropped here (best-effort decode) — task 2 decides the pin policy.
pub fn session_state_changes(ok: &OkPacket<'_>) -> Vec<SessionStateChange<'static>> {
    let Ok(infos) = ok.session_state_info() else {
        return Vec::new();
    };
    infos
        .iter()
        .filter_map(|info| info.decode().ok().map(SessionStateChange::into_owned))
        .collect()
}

/// True iff this OK packet reports an open transaction block (`SERVER_STATUS_IN_TRANS`).
pub fn in_transaction(ok: &OkPacket<'_>) -> bool {
    ok.status_flags().contains(SERVER_STATUS_IN_TRANS)
}

// -------------------------------------------------------------------------------------------------
// `MysqlBackend`: the `PoolBackend` skeleton (M1-S6 task 2).
//
// This stands the crate up at COMPILING `PoolBackend` parity so `ferro-pool`'s hand-rolled pool can
// name `Pool<MysqlBackend>` — every trait method is present, but only `dialect()` is wired for real
// and `query_stream()` returns `Unsupported`. The load-bearing bodies (connect / simple_query /
// query / tx_status off `SERVER_STATUS_IN_TRANS` / reset / the OK-packet
// `take_session_mutated` override) land in later M1-S6 tasks. It connects to nothing yet.
// -------------------------------------------------------------------------------------------------

/// `PoolBackend` impl over a single MySQL/MariaDB DSN (M1-S6). SKELETON — see the module note above.
pub struct MysqlBackend {
    /// The `mysql://` DSN a later task's `connect()` will dial. Held now so the public constructor
    /// surface matches `PgBackend::new`; read by the `connect()` skeleton's diagnostic.
    url: String,
}

impl MysqlBackend {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

/// Placeholder incremental row stream (M1-S6 skeleton). MySQL streaming is a later slice (M1-S7), so
/// [`MysqlBackend::query_stream`] returns `PoolError::Unsupported` and this type is NEVER
/// constructed; it exists only to satisfy the `PoolBackend::RowStream: BackendRows` bound at compile
/// time. Its methods are `unreachable!` by construction.
pub struct MysqlRowStream;

#[async_trait]
impl BackendRows for MysqlRowStream {
    async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>> {
        unreachable!("MysqlRowStream is a compile-time placeholder; MySQL streaming lands in M1-S7")
    }

    fn rows_affected(&self) -> u64 {
        unreachable!("MysqlRowStream is a compile-time placeholder; MySQL streaming lands in M1-S7")
    }
}

/// Placeholder out-of-band cancel handle (M1-S6 skeleton). The real handle — built from the
/// mysql_async connection's server-side connection id, cancelling via a SIDE connection's `KILL
/// QUERY` (the MySQL analog of `PgCancel`) — lands with the interruptible query path in a later
/// task; this exists only to satisfy the `PoolBackend::CancelHandle: Cancel` bound.
pub struct MysqlCancel;

#[async_trait]
impl Cancel for MysqlCancel {
    async fn cancel(self) {
        unreachable!(
            "MysqlCancel is a compile-time placeholder; MySQL cancel lands in a later M1-S6 task"
        )
    }
}

#[async_trait]
impl PoolBackend for MysqlBackend {
    type Conn = mysql_async::Conn;
    type RowStream = MysqlRowStream;
    type CancelHandle = MysqlCancel;

    fn cancel_handle(&self, _conn: &Self::Conn) -> Self::CancelHandle {
        todo!(
            "MySQL out-of-band cancel (KILL QUERY over a side connection) lands in a later M1-S6 task"
        )
    }

    async fn connect(&self) -> Result<Self::Conn, PoolError> {
        todo!(
            "MySQL connect lands in the next M1-S6 task (will dial self.url = {:?})",
            self.url
        )
    }

    async fn ping(&self, _conn: &mut Self::Conn) -> Result<(), PoolError> {
        todo!("MySQL ping lands in a later M1-S6 task")
    }

    fn is_closed(&self, _conn: &Self::Conn) -> bool {
        todo!("MySQL is_closed lands in a later M1-S6 task")
    }

    /// Wired for real in the skeleton: this backend always speaks MySQL, so the assist lexer (M1-S2)
    /// picks the MySQL keyword rule set. A per-backend constant — no round trip, no `conn`.
    fn dialect(&self) -> Dialect {
        DIALECT
    }

    fn tx_status(&self, _conn: &Self::Conn) -> TxStatus {
        todo!(
            "MySQL tx_status (off the OK-packet SERVER_STATUS_IN_TRANS flag) lands in a later M1-S6 task"
        )
    }

    // `take_session_mutated` deliberately inherits the trait's default `false` in the SKELETON — the
    // real OK-packet session-tracker drain (`session_state_info`, the M1-S6 raison d'être) overrides
    // it in a later task once this backend can actually connect and read OK packets.

    async fn reset(&self, _conn: &mut Self::Conn, _profile: ResetProfile) -> Result<(), PoolError> {
        todo!("MySQL reset lands in a later M1-S6 task")
    }

    fn clean_reset_profile(&self) -> Option<ResetProfile> {
        todo!("MySQL clean_reset_profile lands in a later M1-S6 task")
    }

    async fn simple_query(&self, _conn: &mut Self::Conn, _sql: &str) -> Result<u64, PoolError> {
        todo!("MySQL simple_query lands in a later M1-S6 task")
    }

    async fn query(
        &self,
        _conn: &mut Self::Conn,
        _sql: &str,
        _params: &[Value],
    ) -> Result<QueryResult, PoolError> {
        todo!("MySQL query lands in a later M1-S6 task")
    }

    async fn query_stream(
        &self,
        _conn: &mut Self::Conn,
        _sql: &str,
        _params: &[Value],
    ) -> Result<(Vec<ColMeta>, Self::RowStream), PoolError> {
        // MySQL streaming is a later slice; the buffered `query` path is what M1-S6 delivers.
        Err(PoolError::Unsupported(
            "MySQL streaming lands in M1-S7".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `MysqlBackend::dialect()` is a pure, synchronous constant — no live MySQL needed (skeleton
    /// parity check, mirroring `PgBackend`'s `dialect_is_postgres`).
    #[test]
    fn dialect_is_mysql() {
        let backend = MysqlBackend::new("mysql://unused/unused");
        assert_eq!(backend.dialect(), Dialect::MySql);
    }
}
