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

use ferro_pool::backend::Dialect;

pub mod bind;
pub mod conn;
pub mod error_map;
pub mod mytext;
pub mod query;
pub mod rowmap;
pub mod tracker;

pub use conn::{MysqlBackend, MysqlCancel, MysqlConn, MysqlRowStream};

/// Re-exported so this crate's modules (and downstream tests) can name the canonical scalar type
/// without reaching into `ferro-proto`'s module path (parity with `ferro-backend-pg`).
pub use ferro_proto::value::Value;

// Re-export the OK-packet tracker surface the MySQL pin engine reads, so downstream modules name it
// without reaching through `mysql_async`'s re-export path. These are the types the
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
