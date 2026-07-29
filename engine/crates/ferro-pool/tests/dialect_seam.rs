//! M1-S2 Task 2 TDD: the `PoolBackend::dialect()` seam on `FakeBackend`.
//!
//! `PgBackend::dialect() == Dialect::Postgres` is covered in-crate (`ferro-backend-pg/src/conn.rs`
//! `#[cfg(test)]`) since it needs no live Postgres either. These two prove (a) `FakeBackend`'s
//! `#[derive(Default)]` still compiles AND resolves to `Dialect::Postgres` now that it carries a
//! `Dialect` field (only possible because `Dialect: Default` with `#[default] Postgres` — the
//! T1b/verification-#3 requirement), and (b) the `set_dialect` test hook actually flips what
//! `dialect()` reports.

use ferro_pool::backend::{Dialect, PoolBackend};
use ferro_pool::fake::FakeBackend;

#[test]
fn fake_backend_default_dialect_is_postgres() {
    let backend = FakeBackend::default();
    assert_eq!(backend.dialect(), Dialect::Postgres);
}

#[test]
fn fake_backend_set_dialect_overrides_the_reported_dialect() {
    let backend = FakeBackend::new();
    assert_eq!(backend.dialect(), Dialect::Postgres, "default is Postgres");

    backend.set_dialect(Dialect::MySql);
    assert_eq!(backend.dialect(), Dialect::MySql);
}
