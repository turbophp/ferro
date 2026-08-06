//! `ferro-backend-pg`: the Postgres `PoolBackend` (S4 Task 5) — `tokio-postgres` with `NoTls`
//! (no TLS in M0). This is where `ferro-pool`'s hand-rolled pool mechanics connect to a real
//! database; the SQL EXEC service that actually *uses* this pool for user statements is S5.

pub mod bind;
pub mod conn;
pub mod error_map;
pub mod pgtext;
pub mod placeholder;
pub mod query;
pub mod rowmap;

pub use conn::{PgBackend, PgConn};

/// Re-exported so this crate's modules (and downstream tests) can name the canonical scalar type
/// without reaching into `ferro-proto`'s module path.
pub use ferro_proto::value::Value;
