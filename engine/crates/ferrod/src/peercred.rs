//! `SO_PEERCRED` access via the safe `nix` wrapper — no raw `libc`, per the workspace's
//! `unsafe_code = "forbid"` lint (inherited by this crate).

use nix::sys::socket::{getsockopt, sockopt::PeerCredentials};

/// Returns the effective uid of the peer on a connected `AF_UNIX` socket.
pub fn peer_uid(fd: &impl std::os::fd::AsFd) -> std::io::Result<u32> {
    let cred = getsockopt(fd, PeerCredentials).map_err(std::io::Error::from)?;
    Ok(cred.uid())
}
