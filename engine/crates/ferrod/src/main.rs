//! `ferrod` binary edge: load config, init tracing, bind the UDS listener, wire a real signal
//! watcher to the injectable `shutdown::Drain`, then hand everything to `serve` (the testable
//! accept loop). All the actual behavior lives in the library (`ferrod::{serve, session, ...}`);
//! this file is deliberately thin so `tests/shutdown.rs`/`tests/peercred.rs` can drive `serve`
//! directly with an injected `Drain` instead of a real OS signal.

use std::sync::Arc;

use ferrod::config::Config;
use ferrod::epoch::{EpochSource, RandomEpoch};
use ferrod::listener::bind_uds;
use ferrod::pools::PoolRegistry;
use ferrod::serve::serve;
use ferrod::services::sql;
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // product-vision §5 wants the SPEC §13 slow log as "structured JSON to stdout/journald".
    // `FERRO_LOG_FORMAT=json` selects it for the whole stream rather than for one target, because
    // a single `fmt` subscriber has one formatter — and a log stream that is JSON for some lines
    // and prose for others is worse to parse than either. Default stays TEXT: an operator reading
    // a terminal should not have to opt out of machine output.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if std::env::var("FERRO_LOG_FORMAT").is_ok_and(|v| v.trim().eq_ignore_ascii_case("json")) {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let config = Config::from_env();
    config.validate()?;
    let listener = bind_uds(&config)?;
    tracing::info!(socket = %config.socket_path.display(), "ferrod listening");

    // Build the connection pools now that the tokio runtime is up (each pool spawns a background
    // reaper — see `PoolRegistry::build`). DSNs live in `config.pools` (§12 server secret); only
    // pool names are logged. The EXEC handler resolves pools by name out of this registry.
    let registry = PoolRegistry::build(&config);
    tracing::info!(pools = registry.len(), "ferrod: pool registry ready");

    // One process-global transaction registry, shared by every connection `serve` spawns (S6
    // seam). Its `abort_session` teardown wait mirrors the graceful-drain deadline.
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );

    // Drawn once per running instance and handed to every connection `serve` spawns (SPEC
    // §19.1: every connection served by this instance observes the identical `boot_epoch`).
    let epoch = RandomEpoch.epoch();

    let drain = Drain::new();
    spawn_signal_watchers(drain.clone())?;

    // SPEC §13's Prometheus endpoint (M2-C4b), only when the operator asked for it. Binding is
    // fallible and must NOT take the daemon down: the engine's job is on the UDS, and refusing to
    // start because a metrics port is already taken would turn an observability problem into an
    // outage. It is logged loudly instead, at `error`, because a silently absent endpoint is the
    // failure mode an operator discovers from an empty dashboard days later.
    let metrics_shutdown = if let Some(addr) = config.metrics_addr.clone() {
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => {
                let bound = l.local_addr().ok();
                // product-vision §5: admin/metrics bind loopback/UDS by default, and anything
                // crossing hosts wants mTLS/bearer — which this endpoint does not have. It carries
                // no DSNs and no SQL (only pool NAMES and counters), but pool names and pin causes
                // still describe an operator's topology, so a non-loopback bind is said out loud
                // rather than quietly honoured.
                if bound.is_some_and(|a| !a.ip().is_loopback()) {
                    tracing::warn!(
                        addr = ?bound,
                        "ferrod: metrics endpoint is NOT on loopback and is unauthenticated \
                         (product-vision §5) — put it behind a proxy or bind it to localhost",
                    );
                }
                tracing::info!(addr = ?bound, "ferrod: metrics endpoint listening");
                let (tx, rx) = tokio::sync::watch::channel(false);
                tokio::spawn(ferrod::metrics::serve(l, registry.clone(), epoch.0, rx));
                Some(tx)
            }
            Err(e) => {
                tracing::error!(%addr, error = %e, "ferrod: metrics endpoint failed to bind");
                None
            }
        }
    } else {
        None
    };

    serve(
        listener,
        config,
        epoch,
        drain,
        registry,
        tx_registry,
        factory,
    )
    .await;

    if let Some(tx) = metrics_shutdown {
        let _ = tx.send(true);
    }
    tracing::info!("ferrod exiting");
    Ok(())
}

/// Spawn the real OS-signal watchers (`SIGTERM`, plus `Ctrl-C`/`SIGINT` for interactive manual
/// runs) that trigger `drain` — the ONLY place a real signal is ever touched; `serve` and every
/// test know only about `shutdown::Drain`.
fn spawn_signal_watchers(drain: Drain) -> anyhow::Result<()> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let sigterm_drain = drain.clone();
    tokio::spawn(async move {
        sigterm.recv().await;
        tracing::info!("SIGTERM received: starting graceful drain");
        sigterm_drain.trigger();
    });

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received: starting graceful drain");
            drain.trigger();
        }
    });

    Ok(())
}
