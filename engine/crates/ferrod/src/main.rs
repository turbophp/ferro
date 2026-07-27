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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env();
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
        registry,
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

    serve(listener, config, epoch, drain, tx_registry, factory).await;

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
