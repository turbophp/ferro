//! Ferro Queue's pure core (SPEC §24, M7; this crate lands at M7-G1a). **No I/O.**
//!
//! SPEC §24.16 C6 puts the queue's store model, per-dialect statement builders, shape checks and
//! fence-result decoding in a crate that cannot touch a socket, so every engine-authored statement
//! and every rule that decides what one may do is testable — and MUTATION-testable — without a
//! database. That is D22's stated mitigation for the scope it grants: a defect in a statement
//! builder can now damage application rows, so the builders live where a test can pin them.
//!
//! What lives here at G1a:
//!
//! - [`config`] — `FERRO_QUEUE_STORES` and the per-store keys of §24.3, with every refusal. A refusal
//!   disables ONE store and is reported by store name and key, never with a value.
//! - [`ident`] — the `[schema.]identifier` rule for `TABLE`/`DEDUP_TABLE`, and how a validated
//!   identifier is quoted per dialect.
//! - [`sql`] — the `sql` kind's two encodings: the canonical decimal `job_id` (with its strict
//!   decode, §24.3 prerequisite (a)) and the 8-byte token.
//! - [`version`] — the version gate (PostgreSQL ≥ 12, MySQL ≥ 8.0.1, MariaDB ≥ 10.6).
//! - [`shape`] — shape verification: the `information_schema` statement and the verdict on its rows.
//! - [`checks`] — the per-request refusals made before any checkout (payload, queue name, the
//!   fields v1 refuses).
//!
//! The seven verbs' statement builders and the fence land at G1b (SPEC §24.14).

pub mod checks;
pub mod config;
pub mod ident;
pub mod shape;
pub mod sql;
pub mod version;

/// A pool's backend family as configured — the queue's own copy of `ferrod`'s `PoolKind`, so this
/// crate does not depend on the daemon. MySQL and MariaDB share one pool kind; the version string
/// tells them apart ([`version::gate`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolFamily {
    Postgres,
    Mysql,
    Sqlite,
}

/// The SQL dialect a store's statements are composed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    /// MySQL 8 and MariaDB: one dialect for everything G1 composes.
    Mysql,
}
