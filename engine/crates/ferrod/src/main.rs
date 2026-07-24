//! `ferrod` binary edge: load config, init tracing, bind the UDS listener, wire a real signal
//! watcher to the injectable `shutdown::Drain`, then hand everything to `serve` (the testable
//! accept loop). All the actual behavior lives in the library (`ferrod::{serve, session, ...}`);
//! this file is deliberately thin so `tests/shutdown.rs`/`tests/peercred.rs` can drive `serve`
//! directly with an injected `Drain` instead of a real OS signal.

use ferrod::config::Config;
use ferrod::epoch::{EpochSource, RandomEpoch};
use ferrod::listener::bind_uds;
use ferrod::serve::serve;
use ferrod::session::default_handler_fn;
use ferrod::shutdown::Drain;

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

    // Drawn once per running instance and handed to every connection `serve` spawns (SPEC
    // §19.1: every connection served by this instance observes the identical `boot_epoch`).
    let epoch = RandomEpoch.epoch();

    let drain = Drain::new();
    spawn_signal_watchers(drain.clone())?;

    serve(listener, config, epoch, drain, default_handler_fn()).await;

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
