//! `ferro-backend-sqlite` — the SQLite arm of the `ferro_pool::backend::PoolBackend` seam
//! (SPEC §7.6, D13) — complete against the trait as of C3-5.
//!
//! ## Slice history, because the shape of this crate was decided incrementally
//!
//! C3-3a built the crate, the connection wrapper and setup, and deliberately shipped **no**
//! `impl PoolBackend`: eight of the trait's twelve methods would have been `Unsupported` stubs, and
//! a stub returning `Unsupported` is indistinguishable at the type level from a finished method, so
//! the incompleteness would have stopped being visible exactly when it mattered. C3-3b and C3-3c
//! filled in the pin authority, hygiene and the row mapping; **C3-3d added the impl**, at the line
//! C3-3b drew — when only the streaming pair was left; and **C3-5 closed that pair**, so the backend
//! is now complete against the trait.
//!
//! **This is the backend. It is NOT `ferro-sqlite-spike`**, which stays where it is as the premises
//! suite (`p1`–`p6`) and must not be grown into this crate or deleted: those tests would fail loudly
//! if a future SQLite release changed the behaviour D13 rests on, which is worth keeping long after
//! the backend exists.
//!
pub mod conn;
pub mod error_map;
pub mod rowmap;
pub mod stream;

pub use conn::{SqliteBackend, SqliteConn, resolve_path};
pub use stream::SqliteRowStream;
