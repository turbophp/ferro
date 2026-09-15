//! `ferro-backend-sqlite` — the SQLite arm of the `ferro_pool::backend::PoolBackend` seam
//! (SPEC §7.6, D13). C3-3a: the crate, the connection wrapper, and connection setup.
//!
//! **This is the backend. It is NOT `ferro-sqlite-spike`**, which stays where it is as the premises
//! suite (`p1`–`p6`) and must not be grown into this crate or deleted: those tests would fail loudly
//! if a future SQLite release changed the behaviour D13 rests on, which is worth keeping long after
//! the backend exists.
//!
//! ## What C3-3a deliberately does NOT do
//!
//! **There is no `impl PoolBackend` yet.** The trait has a dozen required methods and this slice
//! builds four, so implementing it now would mean eight bodies returning `Unsupported` — and a stub
//! that returns `Unsupported` is indistinguishable, at the type level, from a finished method. The
//! incompleteness would stop being visible exactly when it matters. Leaving the impl out means the
//! compiler states the obvious: this is not yet a backend the pool can hold. The methods below
//! carry the trait's signatures so the impl is a mechanical step in C3-3b/c, and the compiler
//! checks them against the trait at that point.
//!
//! Nothing here is reachable from `ferrod`: the third `AnyPool` arm is C3-3e and is last by design,
//! so a failure in this crate can never be confused with a failure in the daemon.

pub mod conn;

pub use conn::{SqliteBackend, SqliteConn, resolve_path};
