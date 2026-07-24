//! `ferro-backend-pg`: the Postgres `PoolBackend` (S4 Task 5) — `tokio-postgres` with `NoTls`
//! (no TLS in M0). This is where `ferro-pool`'s hand-rolled pool mechanics connect to a real
//! database; the SQL EXEC service that actually *uses* this pool for user statements is S5.

pub mod conn;

pub use conn::{PgBackend, PgConn};
