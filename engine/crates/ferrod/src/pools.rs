//! The daemon pool registry (S5, heterogeneous since M1-S6): one [`AnyPool`] per configured
//! `[[pool]]` — a `Pool<PgBackend>` or a `Pool<MysqlBackend>` depending on the DSN scheme — resolved
//! by the logical name a client puts in `ExecRequest.pool`.
//!
//! Built at startup by [`PoolRegistry::build`], **after the tokio runtime is up** — `Pool::new`
//! spawns the background liveness reaper (`tokio::spawn`; the S4 reaper `max_size` fix is in place),
//! so constructing a pool off-runtime would panic. `main` builds the registry inside its
//! `#[tokio::main]` body; the integration tests build it inside a `#[tokio::test]`.
//!
//! The registry is deliberately heterogeneous (an `AnyPool` enum, not a `Pool<dyn Backend>`): the
//! pool's pin state machine and `Checkout` are monomorphized over the concrete backend `B`, so the
//! SQL/TX handlers dispatch by matching `AnyPool` and calling the SAME generic body (`run_exec_on_pool`
//! / `begin_on_pool` in `services::sql`) for whichever variant — no `dyn` in the hot path.
//!
//! DSNs come from `config.pools` (env/secret refs per SPEC §12): they are the daemon's secret, never
//! sent to a client and never logged — only the pool NAME is ever emitted here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ferro_backend_mysql::MysqlBackend;
use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;

use crate::config::{Config, PoolKind, PoolSpec};

/// One resolved pool of either supported backend. The SQL/TX handlers `match` on this and call the
/// same generic handler body with the concrete `Pool<B>` — both arms monomorphize, and the request
/// terminal is declared via the `Responder` (no typed return), so the arms unify cleanly.
pub enum AnyPool {
    Pg(Pool<PgBackend>),
    Mysql(Pool<MysqlBackend>),
}

/// Daemon per-pool defaults (M0). Deliberately modest, not tuned: correctness over throughput
/// until the D12 bench (charter rule 5). The reaper IS enabled (`Some`) so a backend killed out
/// from under an idle connection is evicted rather than handed to the next tenant.
const DEFAULT_POOL_MAX_SIZE: usize = 16;
const DEFAULT_POOL_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_POOL_MAX_LIFETIME: Duration = Duration::from_secs(30 * 60);
const DEFAULT_POOL_REAP_INTERVAL: Duration = Duration::from_secs(30);

/// The set of live pools the SQL EXEC handler resolves by name. Cheap to clone-share behind an
/// `Arc` (each `Pool` is itself an `Arc` handle — cloning shares connections, it does not fork a
/// second pool).
pub struct PoolRegistry {
    by_name: HashMap<String, AnyPool>,
}

impl PoolRegistry {
    /// Build one [`AnyPool`] per configured pool — a `Pool<PgBackend>` or a `Pool<MysqlBackend>`
    /// per `spec.kind` (inferred from the DSN scheme, `config::infer_pool_kind`). MUST be called
    /// with a tokio runtime already running (see the module docs — the pool's reaper `tokio::spawn`s).
    /// Logs only the pool NAME (+ kind), never the DSN (§12).
    pub fn build(config: &Config) -> Arc<Self> {
        let mut by_name = HashMap::with_capacity(config.pools.len());
        for spec in &config.pools {
            let cfg = daemon_pool_config(spec);
            let pool = match spec.kind {
                PoolKind::Postgres => AnyPool::Pg(Pool::new(PgBackend::new(spec.dsn.clone()), cfg)),
                PoolKind::Mysql => {
                    AnyPool::Mysql(Pool::new(MysqlBackend::new(spec.dsn.clone()), cfg))
                }
            };
            tracing::info!(pool = %spec.name, kind = ?spec.kind, "ferrod: built connection pool");
            by_name.insert(spec.name.clone(), pool);
        }
        Arc::new(Self { by_name })
    }

    /// Resolve a pool by the name a client referenced in `ExecRequest.pool`. `None` ⇒ the handler
    /// answers `Unsupported: unknown pool`.
    pub fn get(&self, name: &str) -> Option<&AnyPool> {
        self.by_name.get(name)
    }

    /// The configured pool names (order unspecified) — for HELLO_ACK's `pools` advertisement and
    /// diagnostics.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(String::as_str)
    }

    /// Number of configured pools.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether no pools are configured (the EXEC handler then answers every request `Unsupported`).
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// Build a pool's `PoolConfig` from its `PoolSpec`: fixed daemon defaults (size/timeout/lifetime/
/// reap) plus the per-pool pin-engine escape hatch (`pin_functions`/`pin_on_unknown`, M1-S2 Task 4)
/// carried verbatim from the spec parsed out of `FERRO_POOL_<NAME>_PIN_FUNCTIONS`/
/// `FERRO_POOL_<NAME>_PIN_ON_UNKNOWN` (see `config::parse_pool_pin_config`).
fn daemon_pool_config(spec: &PoolSpec) -> PoolConfig {
    PoolConfig {
        max_size: DEFAULT_POOL_MAX_SIZE,
        checkout_timeout: DEFAULT_POOL_CHECKOUT_TIMEOUT,
        max_lifetime: DEFAULT_POOL_MAX_LIFETIME,
        reap_interval: Some(DEFAULT_POOL_REAP_INTERVAL),
        pin_functions: spec.pin_functions.clone(),
        pin_on_unknown: spec.pin_on_unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::PoolSpec;

    fn spec(name: &str, dsn: &str) -> PoolSpec {
        PoolSpec {
            name: name.to_string(),
            dsn: dsn.to_string(),
            kind: crate::config::infer_pool_kind(dsn),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }
    }

    /// An empty pool config yields an empty registry that resolves nothing — the EXEC handler's
    /// `unknown pool` path. No runtime needed (no pool is built), so this stays a plain unit test.
    #[test]
    fn empty_config_builds_empty_registry() {
        let config = Config::default();
        let registry = PoolRegistry::build(&config);
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.get("default").is_none());
    }

    /// `build` picks the concrete `AnyPool` variant per `spec.kind`. `Pool::new` is LAZY (it only
    /// spawns the reaper; no connection is dialed until checkout), so this builds real pools against
    /// never-dialed DSNs and just inspects the variant — needs a runtime (the reaper spawns), hence
    /// `#[tokio::test]`. Proves the mysql:// DSN yields `AnyPool::Mysql` and postgres:// yields
    /// `AnyPool::Pg` — the monomorphic-registry → heterogeneous-registry fix (M1-S6 Task 5).
    #[tokio::test]
    async fn build_selects_backend_variant_per_kind() {
        let config = Config {
            pools: vec![
                spec("pg", "postgres://user@127.0.0.1:5432/app"),
                spec("my", "mysql://user@127.0.0.1:3306/app"),
            ],
            ..Config::default()
        };
        let registry = PoolRegistry::build(&config);
        assert_eq!(registry.len(), 2);
        assert!(matches!(registry.get("pg"), Some(AnyPool::Pg(_))));
        assert!(matches!(registry.get("my"), Some(AnyPool::Mysql(_))));
        assert!(registry.get("nope").is_none());
    }
}
