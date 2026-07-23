//! UDS bind with stale-socket-unlink. The peercred-gated accept loop is wired in later S3
//! tasks (`main`); this module only provides the bound listener.

use crate::config::Config;

/// Bind a `tokio::net::UnixListener` at `cfg.socket_path`, unlinking a pre-existing (stale)
/// socket file first and creating the parent directory if it does not exist.
pub fn bind_uds(cfg: &Config) -> std::io::Result<tokio::net::UnixListener> {
    if let Some(parent) = cfg.socket_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    // Stale-unlink: a leftover socket file from a previous (crashed/killed) instance must not
    // block bind(). Ignore a missing file; propagate any other removal error.
    if cfg.socket_path.exists() {
        std::fs::remove_file(&cfg.socket_path)?;
    }

    tokio::net::UnixListener::bind(&cfg.socket_path)
}
