//! The daemon pool registry (S5): one `Pool<PgBackend>` per configured `[[pool]]`, resolved by the
//! logical name a client puts in `ExecRequest.pool`.
//!
//! Built at startup by [`PoolRegistry::build`], **after the tokio runtime is up** — `Pool::new`
//! spawns the background liveness reaper (`tokio::spawn`; the S4 reaper `max_size` fix is in place),
//! so constructing a pool off-runtime would panic. `main` builds the registry inside its
//! `#[tokio::main]` body; the integration tests build it inside a `#[tokio::test]`.
//!
//! DSNs come from `config.pools` (env/secret refs per SPEC §12): they are the daemon's secret, never
//! sent to a client and never logged — only the pool NAME is ever emitted here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;

use crate::config::Config;

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
    by_name: HashMap<String, Pool<PgBackend>>,
}

impl PoolRegistry {
    /// Build one `Pool<PgBackend>` per configured pool. MUST be called with a tokio runtime already
    /// running (see the module docs — the pool's reaper `tokio::spawn`s). Logs only the pool NAME,
    /// never the DSN (§12).
    pub fn build(config: &Config) -> Arc<Self> {
        let mut by_name = HashMap::with_capacity(config.pools.len());
        for spec in &config.pools {
            let pool = Pool::new(PgBackend::new(spec.dsn.clone()), daemon_pool_config());
            tracing::info!(pool = %spec.name, "ferrod: built connection pool");
            by_name.insert(spec.name.clone(), pool);
        }
        Arc::new(Self { by_name })
    }

    /// Resolve a pool by the name a client referenced in `ExecRequest.pool`. `None` ⇒ the handler
    /// answers `Unsupported: unknown pool`.
    pub fn get(&self, name: &str) -> Option<&Pool<PgBackend>> {
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

fn daemon_pool_config() -> PoolConfig {
    PoolConfig {
        max_size: DEFAULT_POOL_MAX_SIZE,
        checkout_timeout: DEFAULT_POOL_CHECKOUT_TIMEOUT,
        max_lifetime: DEFAULT_POOL_MAX_LIFETIME,
        reap_interval: Some(DEFAULT_POOL_REAP_INTERVAL),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
