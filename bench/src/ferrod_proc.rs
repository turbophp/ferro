//! [V8] A panic-safe `ferrod` process handle. `FerrodProc` owns the child process AND its socket
//! path; its `Drop` impl guarantees teardown — SIGTERM, poll `try_wait()` to a deadline, SIGKILL,
//! then unlink the socket — so a panic anywhere in aggregation (percentile math, JSON assembly,
//! `validate()`) can NEVER leak a running ferrod, an orphaned socket file, or the upstream Postgres
//! connections that ferrod's pool holds. The teardown runs from `Drop`, which unwinding executes.

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

/// How long to wait for a graceful SIGTERM exit before escalating to SIGKILL.
const SIGTERM_GRACE: Duration = Duration::from_secs(5);
/// Poll interval while waiting for the child to exit.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// A launched `ferrod` (or, in tests, any child) plus the socket file it binds. Dropping it tears
/// the process and socket down unconditionally.
pub struct FerrodProc {
    child: Child,
    socket_path: PathBuf,
    /// Path the child's stdout+stderr were redirected to, for surfacing on a readiness failure.
    log_path: PathBuf,
}

impl FerrodProc {
    /// Launch a RELEASE `ferrod` configured entirely by env (verified recipe, matches
    /// `LiveTestCase`/`config.rs`): `FERRO_SOCK`, `FERRO_POOLS=default`,
    /// `FERRO_POOL_DEFAULT_DSN=<dsn>`. stdout+stderr are redirected to `log_path`. The current
    /// environment is inherited so a `RUST_LOG` set by the operator still applies.
    pub fn spawn(bin: &Path, socket_path: &Path, dsn: &str, log_path: &Path) -> io::Result<Self> {
        let log = std::fs::File::create(log_path)?;
        let log_err = log.try_clone()?;
        let child = Command::new(bin)
            .env("FERRO_SOCK", socket_path)
            .env("FERRO_POOLS", "default")
            .env("FERRO_POOL_DEFAULT_DSN", dsn)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()?;
        Ok(FerrodProc {
            child,
            socket_path: socket_path.to_path_buf(),
            log_path: log_path.to_path_buf(),
        })
    }

    /// Wrap an already-spawned child (used by the Drop unit test with a stand-in process).
    #[cfg(test)]
    fn from_child(child: Child, socket_path: PathBuf, log_path: PathBuf) -> Self {
        FerrodProc {
            child,
            socket_path,
            log_path,
        }
    }

    /// The child's OS pid.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The captured stdout+stderr log, or a placeholder if unreadable — surfaced on a readiness
    /// failure alongside the PHP stderr so both halves of the path are diagnosable [V7].
    pub fn read_log(&self) -> String {
        match std::fs::read_to_string(&self.log_path) {
            Ok(s) if !s.is_empty() => s,
            Ok(_) => "(ferrod log empty)".to_string(),
            Err(_) => "(ferrod log unreadable)".to_string(),
        }
    }

    /// `Some(exit)` if the child has already exited (non-blocking), else `None`. On an errored
    /// `try_wait` we conservatively report "still running" so teardown still signals it.
    pub fn exited(&mut self) -> Option<std::process::ExitStatus> {
        // An errored `try_wait` is treated as "still running" (`None`) so teardown still signals it.
        self.child.try_wait().unwrap_or_default()
    }
}

impl Drop for FerrodProc {
    fn drop(&mut self) {
        let pid = Pid::from_raw(self.child.id() as i32);

        // If it already exited, just reap and unlink.
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            // SIGTERM -> graceful drain (ferrod's signal watcher triggers `Drain`).
            let _ = signal::kill(pid, Signal::SIGTERM);

            let deadline = Instant::now() + SIGTERM_GRACE;
            let mut exited = false;
            while Instant::now() < deadline {
                match self.child.try_wait() {
                    Ok(Some(_)) => {
                        exited = true;
                        break;
                    }
                    Ok(None) => std::thread::sleep(POLL_INTERVAL),
                    Err(_) => break,
                }
            }

            if !exited {
                // Graceful window elapsed — hard kill and reap so no zombie is left.
                let _ = signal::kill(pid, Signal::SIGKILL);
                let _ = self.child.wait();
            }
        }

        // Unlink the socket unconditionally (ferrod stale-unlinks at bind, but be explicit so a
        // panic-teardown leaves no dangling file for the next run).
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Drop guard must SIGTERM the child (a `sleep` dies on the default SIGTERM disposition)
    /// AND unlink the socket file — even though no explicit stop was called. This exercises the
    /// nix::kill + unlink path that protects a panicking aggregation.
    #[test]
    fn drop_terminates_child_and_unlinks_socket() {
        let dir = std::env::temp_dir();
        let sock = dir.join(format!("ferro-bench-drop-test-{}.sock", std::process::id()));
        std::fs::write(&sock, b"stand-in socket file").unwrap();
        assert!(sock.exists());

        // A long sleep stands in for ferrod; it terminates on SIGTERM.
        let child = Command::new("sleep")
            .arg("120")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id() as i32;
        let log = dir.join(format!("ferro-bench-drop-test-{}.log", std::process::id()));
        let proc = FerrodProc::from_child(child, sock.clone(), log);

        drop(proc); // triggers SIGTERM -> reap -> unlink

        assert!(!sock.exists(), "Drop must unlink the socket file");
        // The pid must no longer be a live, signalable process (kill(0) errors once reaped).
        let alive = signal::kill(Pid::from_raw(pid), None).is_ok();
        assert!(!alive, "Drop must terminate the child process");
    }
}
