//! The out-of-band large-payload path (M3-D3, SPEC §5.1, `/proto/PROTOCOL.md` §1.1).
//!
//! When the client advertised `MEMFD_RX` in its HELLO and a request's buffered success terminal is
//! at least the configured threshold (`FERRO_MEMFD_THRESHOLD_BYTES`, default 1 MiB), the payload
//! that terminal would have carried inline — the whole `Outcome::Ok` envelope — is written into a
//! **sealed** memfd instead, and the terminal frame (`END | OOB_FD`) carries an [`OobRef`] naming it.
//! The fd rides `SCM_RIGHTS` on the session's own Unix socket.
//!
//! **What moves and what does not, and WHEN.** The handler still builds the encoded `ExecOk` body
//! in the heap, and the supervisor still builds the ordinary inline terminal from it, merely marking
//! it `oob` (`ControlMsg::oob`). The memfd is made by the WRITER, immediately before the terminal is
//! sent: [`seal_payload`] copies the frame's payload into it, the frame goes out with the fd, and
//! the engine's copy of the fd is closed. So a terminal waiting in the control channel is heap,
//! exactly as an inline one is, and the engine holds at most ONE memfd per session at any moment —
//! a memfd made when the handler finished would sit in the engine's fd table for as long as the
//! client took to read, and one non-reading session could then exhaust `RLIMIT_NOFILE` and lock
//! every other tenant out of `accept` (review F2, reproduced). What the engine gets back is its
//! writer: a multi-megabyte terminal leaves the socket as one short frame, so the other requests
//! multiplexed on the session are not queued behind the client draining it. The memfd's pages are
//! kernel shmem, not `ferrod` RSS, and they live as long as SOME process holds the fd: once sent,
//! that is the client's socket receive queue, then the client — so they survive the session's
//! teardown and even `ferrod`'s exit until the client reads or closes its socket (review F3). The
//! result ceiling is unchanged (a buffered result still has to fit one frame's
//! `MAX_FRAME_PAYLOAD`), because §5.1 permits no difference but throughput.
//!
//! **Associating the fd with its frame is the classic bug, and the rule here makes it impossible to
//! get wrong on either side.** On a `SOCK_STREAM` Unix socket ancillary data is attached to the
//! bytes of the `sendmsg` that carried it, but a receiver's `recvmsg` can return those bytes TOGETHER
//! with earlier, fd-less bytes (measured in PHP: `"XX"` written plainly, then `"HDR1body"` sent with
//! an fd, arrive as ONE `recvmsg` of `"XXHDR1body"` carrying the fd) — so "the fd belongs to the
//! first frame in this read" is wrong. The writer therefore (1) finishes writing every earlier
//! frame, (2) sends the `OOB_FD` frame's bytes in a `sendmsg` that starts at the frame's FIRST byte
//! and carries exactly ONE fd, and never attaches an fd to anything else. Fds then arrive in the
//! same order as the `OOB_FD` frames, and each no later than its frame's first byte, so a receiver
//! that reads every byte with `recvmsg` and queues the fds FIFO pairs the n-th `OOB_FD` frame with
//! the n-th fd. A receiver must never read this socket with plain `read(2)`: the kernel discards
//! (closes) any fd attached to bytes read that way.

use std::fs::File;
use std::io::{self, IoSlice, Seek, SeekFrom, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering};

use nix::fcntl::{FcntlArg, SealFlag, fcntl};
use nix::sys::memfd::{MemFdCreateFlag, memfd_create};
use nix::sys::socket::{ControlMessage, MsgFlags, sendmsg};
use tokio::io::Interest;
use tokio::net::UnixStream;

use ferro_proto::consts::oob_encoding;
use ferro_proto::messages::OobRef;

/// The seals every OOB memfd carries before its fd leaves the engine: its size can neither shrink
/// nor grow, its bytes cannot be written, and no seal can be removed or added. A receiver can
/// therefore trust `len` and read at leisure — nothing the engine (or anyone else holding the fd)
/// does afterwards can change what it reads.
pub const SEALS: SealFlag = SealFlag::F_SEAL_SHRINK
    .union(SealFlag::F_SEAL_GROW)
    .union(SealFlag::F_SEAL_WRITE)
    .union(SealFlag::F_SEAL_SEAL);

/// How many bytes the `Outcome::Ok` envelope adds in front of a success body — what makes a body
/// of `n` bytes an inline payload of `n + ENVELOPE_LEN`. Pinned against the codec by a test.
pub const ENVELOPE_LEN: usize = 2;

/// Copy `payload` — a terminal frame's exact inline payload — into a fresh memfd, seal it, and
/// rewind it. Returns the sealed fd. The fd is positioned at offset 0, because an `SCM_RIGHTS` fd
/// shares its OPEN FILE DESCRIPTION — and therefore its file offset — with the engine's copy: a
/// receiver that reads "from the current position" would otherwise read nothing.
///
/// Synchronous: it runs in the writer task right before the send, the same order of work as the
/// inline path's copy of the payload into the writer's frame buffer, which it replaces.
pub fn seal_payload(payload: &[u8]) -> io::Result<OwnedFd> {
    let fd = memfd_create(
        c"ferro-oob",
        MemFdCreateFlag::MFD_CLOEXEC | MemFdCreateFlag::MFD_ALLOW_SEALING,
    )
    .map_err(io::Error::from)?;
    let mut file = File::from(fd);
    // `set_len` first so the kernel allocates the size once, instead of growing it write by write.
    file.set_len(payload.len() as u64)?;
    file.write_all(payload)?;
    fcntl(file.as_raw_fd(), FcntlArg::F_ADD_SEALS(SEALS)).map_err(io::Error::from)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(OwnedFd::from(file))
}

/// The `OobRef` an `OOB_FD` terminal carries for a payload of `len` bytes.
pub fn oob_ref(len: u64) -> OobRef {
    OobRef {
        fd_index: 0,
        len,
        encoding: oob_encoding::FRAME_PAYLOAD,
    }
}

/// Send `bytes` — which MUST begin at an `OOB_FD` frame's first byte — with `fd` attached as the
/// only `SCM_RIGHTS` fd, and return how many bytes the kernel took (at least 1). The caller writes
/// the rest plainly: the fd is attached to the first byte, which is what the receiver keys on.
///
/// The caller must have flushed every earlier frame first; nothing here can check that, which is why
/// the one caller is the writer task, the only thing that writes to the socket.
pub async fn send_with_fd(
    stream: &UnixStream,
    bytes: &[u8],
    fd: BorrowedFd<'_>,
) -> io::Result<usize> {
    debug_assert!(!bytes.is_empty(), "an OOB_FD frame always has a header");
    let fds = [fd.as_raw_fd()];
    loop {
        stream.writable().await?;
        let sent = stream.try_io(Interest::WRITABLE, || {
            sendmsg::<()>(
                stream.as_raw_fd(),
                &[IoSlice::new(bytes)],
                &[ControlMessage::ScmRights(&fds)],
                MsgFlags::MSG_NOSIGNAL,
                None,
            )
            .map_err(io::Error::from)
        });
        match sent {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Whether a failed `sendmsg` refused the FD rather than the connection: `ETOOMANYREFS` is the
/// kernel's per-user limit on fds in flight (`RLIMIT_NOFILE`, skipped for a privileged sender). The
/// call sent nothing, so the writer can still deliver the same terminal inline.
pub fn is_fd_refusal(e: &io::Error) -> bool {
    e.raw_os_error() == Some(nix::errno::Errno::ETOOMANYREFS as i32)
}

/// The seals an fd carries (`F_GET_SEALS`). For tests and diagnostics.
pub fn seals_of(fd: impl AsFd) -> io::Result<SealFlag> {
    let bits = fcntl(fd.as_fd().as_raw_fd(), FcntlArg::F_GET_SEALS).map_err(io::Error::from)?;
    Ok(SealFlag::from_bits_truncate(bits))
}

/// SPEC §13-style counters for the OOB path, exported by `metrics::render`.
pub struct OobCounters {
    sent: AtomicU64,
    bytes: AtomicU64,
    fallbacks: AtomicU64,
}

impl OobCounters {
    const fn new() -> Self {
        OobCounters {
            sent: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            fallbacks: AtomicU64::new(0),
        }
    }
    /// A terminal whose fd the kernel accepted.
    pub fn record_sent(&self, len: u64) {
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(len, Ordering::Relaxed);
    }
    /// A terminal that qualified for the OOB path but went inline (memfd creation failed, or the
    /// kernel refused the fd).
    pub fn record_fallback(&self) {
        self.fallbacks.fetch_add(1, Ordering::Relaxed);
    }
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub fn fallbacks(&self) -> u64 {
        self.fallbacks.load(Ordering::Relaxed)
    }
}

pub static COUNTERS: OobCounters = OobCounters::new();

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::messages::Outcome;
    use std::io::Read;

    /// The memfd holds EXACTLY the inline payload, is rewound, and carries all four seals — and
    /// the seals are real: a write, a grow and a shrink through the very fd that wrote it all fail.
    #[test]
    fn a_sealed_payload_is_the_inline_payload_and_cannot_be_changed() {
        let body: Vec<u8> = {
            let mut b = vec![0xc6, 0, 0, 0x10, 0]; // bin32 header for 4096 bytes
            b.extend((0..4096u32).map(|i| (i % 251) as u8));
            b
        };
        let payload = Outcome::Ok(body).encode();
        let fd = seal_payload(&payload).expect("seal");
        let len = payload.len() as u64;
        let seals = seals_of(&fd).expect("F_GET_SEALS");
        // Against the four seals spelled out, not against `SEALS` — comparing a constant with
        // itself would pass whatever it held.
        for (seal, name) in [
            (SealFlag::F_SEAL_SHRINK, "SHRINK"),
            (SealFlag::F_SEAL_GROW, "GROW"),
            (SealFlag::F_SEAL_WRITE, "WRITE"),
            (SealFlag::F_SEAL_SEAL, "SEAL"),
        ] {
            assert!(seals.contains(seal), "F_SEAL_{name} is not set: {seals:?}");
        }

        let mut file = File::from(fd);
        assert_eq!(
            file.stream_position().unwrap(),
            0,
            "rewound for the receiver"
        );
        let mut got = Vec::new();
        file.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload);

        // An OVERWRITE inside the current size, so only F_SEAL_WRITE can refuse it — a write at
        // EOF would also be refused by F_SEAL_GROW and prove nothing about the write seal (a
        // mutation removing F_SEAL_WRITE once passed this test that way).
        file.seek(SeekFrom::Start(0)).unwrap();
        assert!(
            file.write_all(b"x").is_err(),
            "F_SEAL_WRITE refuses a write"
        );
        assert!(file.set_len(len + 1).is_err(), "F_SEAL_GROW refuses a grow");
        assert!(
            file.set_len(len - 1).is_err(),
            "F_SEAL_SHRINK refuses a shrink"
        );
        assert!(
            fcntl(file.as_raw_fd(), FcntlArg::F_ADD_SEALS(SealFlag::empty())).is_err(),
            "F_SEAL_SEAL refuses any further seal change"
        );
    }

    #[test]
    fn the_oob_ref_names_fd_zero_and_the_frame_payload_encoding() {
        let r = oob_ref(1234);
        assert_eq!(r.fd_index, 0);
        assert_eq!(r.len, 1234);
        assert_eq!(r.encoding, oob_encoding::FRAME_PAYLOAD);
    }
}
