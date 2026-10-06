//! Request-bearing service handlers — the spawn-per-request tasks `session::mod`'s dispatch hands
//! each `Route::Request` frame to. M0 implements SQL `EXEC` ([`sql`]); TX (BEGIN/COMMIT over the
//! pin, S6) and STREAM (post-M0) land later. The ADMIN service's `BACKUP` ([`admin`], M2-C3-7b) is
//! dispatched from the same per-connection handler, after the session's D15 gate.

pub mod admin;
pub mod copy;
pub mod fate;
#[cfg(feature = "http")]
pub mod http;
pub mod sql;
