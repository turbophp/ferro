//! **C4b-2a: §13's hygiene counters, counted EXACTLY, through a real `Pool` on the fake backend.**
//!
//! The daemon-level e2e (`ferrod`'s `metrics_it.rs`) can only assert `>=`: HELLO's version probe
//! and the session's own traffic also recycle connections. Its adversarial review showed what that
//! costs — counting on the fresh-dial exit too, or counting a connection that is then EVICTED,
//! both left it green. These tests own the pool outright, so every count is exact and each of
//! those mutations fails one of them.

use ferro_pool::config::PoolConfig;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pin::{HygieneOutcome, TxId};
use ferro_pool::pool::Pool;

fn pool_of(backend: FakeBackend) -> Pool<FakeBackend> {
    Pool::new(
        backend,
        PoolConfig {
            max_size: 1,
            ..Default::default()
        },
    )
}

fn counts(pool: &Pool<FakeBackend>) -> (u64, u64, u64) {
    let m = pool.hygiene_metrics();
    (
        m.get(HygieneOutcome::SkippedClean),
        m.get(HygieneOutcome::Targeted),
        m.get(HygieneOutcome::Full),
    )
}

/// A fresh dial has no previous tenant: it is not a hygiene event.
#[tokio::test]
async fn a_fresh_dial_is_not_counted() {
    let pool = pool_of(FakeBackend::new());
    let c = pool.checkout().await.expect("checkout");
    drop(c);
    assert_eq!(counts(&pool), (0, 0, 0));
}

/// A clean recycle gets the backend's clean profile — `Targeted` on the fake, as on PostgreSQL.
#[tokio::test]
async fn a_clean_recycle_is_counted_once_as_targeted() {
    let pool = pool_of(FakeBackend::new());
    let mut c = pool.checkout().await.expect("checkout");
    c.exec("SELECT 1").await.expect("plain SELECT");
    drop(c);
    let c = pool.checkout().await.expect("recycle");
    drop(c);
    assert_eq!(counts(&pool), (0, 1, 0));
}

/// A tainted recycle gets the full reset.
#[tokio::test]
async fn a_tainted_recycle_is_counted_once_as_full() {
    let pool = pool_of(FakeBackend::new());
    let mut c = pool.checkout().await.expect("checkout");
    c.exec("SET search_path TO app")
        .await
        .expect("tainting SET");
    assert!(c.tainted());
    drop(c);
    let next = pool.checkout().await.expect("recycle");
    assert!(next.conn().recorded.iter().any(|s| s == "RESET:Full"));
    drop(next);
    assert_eq!(counts(&pool), (0, 0, 1));
}

/// `skipped_clean` is reachable the moment a backend reports no clean profile — the case no real
/// backend reaches today, and the reason the series is exported at all.
#[tokio::test]
async fn a_backend_with_no_clean_profile_is_counted_as_skipped_clean() {
    let backend = FakeBackend::new();
    backend.set_clean_reset_profile(None);
    let pool = pool_of(backend);
    let mut c = pool.checkout().await.expect("checkout");
    c.exec("SELECT 1").await.expect("plain SELECT");
    drop(c);
    let c = pool.checkout().await.expect("recycle");
    drop(c);
    assert_eq!(counts(&pool), (1, 0, 0));
}

/// **The documented meaning of `skipped_clean`: no reset PROFILE ran.** A recycled connection
/// that was still in a transaction gets a defensive `ROLLBACK` — transaction cleanup, which the tx
/// authority owns — but no session hygiene, so it is filed here. Pinned so the definition cannot
/// drift silently (adversarial review F4).
#[tokio::test]
async fn a_rollback_only_recycle_is_skipped_clean_by_definition() {
    let backend = FakeBackend::new();
    backend.set_clean_reset_profile(None);
    let pool = pool_of(backend);
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    drop(c);
    let next = pool.checkout().await.expect("recycle");
    assert!(next.conn().recorded.iter().any(|s| s == "ROLLBACK"));
    assert!(!next.conn().recorded.iter().any(|s| s.starts_with("RESET:")));
    drop(next);
    assert_eq!(counts(&pool), (1, 0, 0));
}

/// A connection whose cleanup FAILS is evicted and never handed out — it received no hygiene
/// anyone will run on, so it is not counted. The checkout that follows is a fresh dial, which is
/// not counted either.
#[tokio::test]
async fn an_evicted_connection_is_not_counted() {
    let pool = pool_of(FakeBackend::new());
    let mut c = pool.checkout().await.expect("checkout");
    c.begin_tx(TxId(1)).await.expect("begin");
    c.conn_mut().arm_fail_next_simple_query();
    let first_id = c.conn().id;
    drop(c);
    let next = pool.checkout().await.expect("evict + fresh dial");
    assert_ne!(
        next.conn().id,
        first_id,
        "the failed-cleanup connection must have been evicted"
    );
    drop(next);
    assert_eq!(counts(&pool), (0, 0, 0));
}

/// Every outcome is reported, including the zeroes, through the public accessor the exporter uses.
#[tokio::test]
async fn the_snapshot_carries_every_outcome() {
    let pool = pool_of(FakeBackend::new());
    let snap = pool.hygiene_metrics().snapshot();
    assert_eq!(snap.len(), HygieneOutcome::COUNT);
}
