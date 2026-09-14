//! C3-1 — the SQLite premise spike. **This is NOT the backend.**
//!
//! `ferro-backend-sqlite` does not exist and this crate is not it. The crate ships no library code
//! at all: everything lives in `tests/premises_it.rs`, whose only job is to PROVE, against the real
//! `rusqlite` and a real on-disk database, the four premises that `docs/dev-loop/C3-SQLITE-SCOPE.md`
//! marks UNVERIFIED and that SPEC D13 rests on. Any one of them coming back false invalidates the
//! C3 plan, which is why they are proven before a backend crate is created rather than discovered
//! halfway through building one.
//!
//! The habit is deliberate and has paid twice: the "no vendored driver fork is needed" assumption
//! was proven FALSE for `mysql_async` at M1-S6 and for `tokio-postgres` at M1-S1, both times before
//! any backend code was written.
//!
//! SQLite is the first backend whose spike needs no server, so unlike the PG and MySQL lanes every
//! assertion here runs in an ordinary container — no Docker, no `testkit` compose file.
//!
//! When the real backend lands, the P4 guards (the `SQLITE_BUSY_SNAPSHOT` pair) are worth KEEPING
//! rather than deleting: they are the executable statement of why D13 chose the lock-at-BEGIN rule,
//! and they would fail loudly if a future SQLite release changed that behaviour.
