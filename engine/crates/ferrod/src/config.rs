//! Daemon configuration: env-loaded with defaults (charter rule: no hand-rolled protocol
//! constants — `credit_frames`/`credit_bytes` default from `ferro_proto::consts`;
//! `session_cap_bytes` is its OWN literal, a distinct concept from `MAX_FRAME_PAYLOAD`).

use std::path::PathBuf;
use std::time::Duration;

/// Default UDS bind path when `FERRO_SOCK` is unset.
const DEFAULT_SOCKET_PATH: &str = "/run/ferro/dev.sock";

/// Default per-session aggregate credit cap in bytes. A distinct concept from
/// `ferro_proto::consts::MAX_FRAME_PAYLOAD` (the codec's per-frame ceiling) — do NOT couple
/// the two.
const DEFAULT_SESSION_CAP_BYTES: usize = 16 * 1024 * 1024;

/// Default cap on concurrently in-flight requests per session.
const DEFAULT_MAX_INFLIGHT: usize = 1024;

/// Default deadline for a graceful (SIGTERM) drain before hard-closing remaining sessions.
const DEFAULT_DRAIN_DEADLINE: Duration = Duration::from_secs(5);

/// Default deadline for the mandatory first frame (`core/HELLO`) to arrive on a newly accepted
/// connection. Without this bound, a peer that passes the `SO_PEERCRED` gate and then simply
/// never sends anything pins an fd, a session task, and a writer task forever — a slowloris /
/// fd-exhaustion vector, not just a wasted connection.
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Config {
    /// UDS bind path. From `FERRO_SOCK`, default `/run/ferro/dev.sock`.
    pub socket_path: PathBuf,
    /// Peer-uid allow-list for `SO_PEERCRED` gating. Empty means "allow only the daemon's own
    /// uid" (see `uid_allowed`). From `FERRO_ALLOW_UIDS` (comma-separated), default empty.
    pub peer_allow_uids: Vec<u32>,
    /// Default per-request credit, in frames.
    pub credit_frames: u32,
    /// Default per-request credit, in bytes.
    pub credit_bytes: u32,
    /// Per-session aggregate credit cap in bytes (own literal, see `DEFAULT_SESSION_CAP_BYTES`).
    pub session_cap_bytes: usize,
    /// Max concurrently in-flight requests per session.
    pub max_inflight: usize,
    /// Deadline for a graceful drain (SIGTERM) before hard-closing remaining sessions.
    pub drain_deadline: Duration,
    /// Deadline for the mandatory first frame (`core/HELLO`) to arrive before the connection is
    /// dropped silently (no reply — there was never a valid session to fail).
    pub handshake_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            socket_path: PathBuf::from(DEFAULT_SOCKET_PATH),
            peer_allow_uids: Vec::new(),
            credit_frames: ferro_proto::consts::DEFAULT_CREDIT_FRAMES,
            credit_bytes: ferro_proto::consts::DEFAULT_CREDIT_BYTES,
            session_cap_bytes: DEFAULT_SESSION_CAP_BYTES,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            drain_deadline: DEFAULT_DRAIN_DEADLINE,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        }
    }
}

impl Config {
    /// Load configuration from the process environment, falling back to defaults for any
    /// variable that is unset or fails to parse.
    pub fn from_env() -> Self {
        let mut cfg = Config::default();

        if let Ok(path) = std::env::var("FERRO_SOCK") {
            cfg.socket_path = PathBuf::from(path);
        }

        if let Ok(list) = std::env::var("FERRO_ALLOW_UIDS") {
            cfg.peer_allow_uids = parse_allow_uids(&list);
        }

        cfg
    }

    /// The daemon's own uid, via the safe `nix` wrapper around `getuid(2)`.
    pub fn own_uid() -> u32 {
        nix::unistd::getuid().as_raw()
    }

    /// Whether `uid` is allowed to connect. An empty `peer_allow_uids` means "self only" — the
    /// daemon's own uid, per `own_uid()`. A non-empty list is an explicit allow-list membership
    /// check (the daemon's own uid is NOT implicitly included once the list is non-empty).
    pub fn uid_allowed(&self, uid: u32) -> bool {
        if self.peer_allow_uids.is_empty() {
            uid == Self::own_uid()
        } else {
            self.peer_allow_uids.contains(&uid)
        }
    }
}

/// Parse a comma-separated `FERRO_ALLOW_UIDS` value into the uids it names. An unparseable token
/// (empty after trimming aside) is `tracing::warn!`-ed and skipped, NOT silently discarded: a
/// wrong delimiter (e.g. `"33;44"`, a single token that fails to parse as `u32`) would otherwise
/// yield an empty allow-list, which falls back to self-only (`uid_allowed`) — a silent,
/// security-relevant surprise for an operator who intended to allow those other uids. Parsing
/// continues past a bad token (fail-fast is not required here, per the charter's "when uncertain"
/// guidance — a warn is the minimum).
fn parse_allow_uids(list: &str) -> Vec<u32> {
    list.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| match s.parse::<u32>() {
            Ok(uid) => Some(uid),
            Err(err) => {
                tracing::warn!(
                    token = s,
                    error = %err,
                    "FERRO_ALLOW_UIDS: skipping unparseable uid token"
                );
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_allow_uids_skips_malformed_tokens_and_keeps_valid_ones() {
        // "33;44" is a single token with the wrong delimiter -- not parseable as a u32 -- and
        // must not silently swallow the whole list: 55 and 66 on either side of it still make it
        // into the result.
        let uids = parse_allow_uids("55, 33;44 ,66,not-a-uid,");
        assert_eq!(uids, vec![55, 66]);
    }

    #[test]
    fn parse_allow_uids_all_malformed_yields_empty_not_a_panic() {
        assert_eq!(parse_allow_uids("nope;nope"), Vec::<u32>::new());
    }

    #[test]
    fn parse_allow_uids_empty_string_yields_empty() {
        assert_eq!(parse_allow_uids(""), Vec::<u32>::new());
    }
}
