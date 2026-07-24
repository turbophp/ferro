//! Request-bearing service handlers — the spawn-per-request tasks `session::mod`'s dispatch hands
//! each `Route::Request` frame to. M0 implements SQL `EXEC` ([`sql`]); TX (BEGIN/COMMIT over the
//! pin, S6) and STREAM (post-M0) land later.

pub mod sql;
