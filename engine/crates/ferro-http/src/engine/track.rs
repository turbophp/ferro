//! The SPEC §23.7.1 write tracker: what decides `sent`.
//!
//! It wraps the connection's PLAINTEXT I/O, below `hyper` (and, from slice F5, above TLS). Built on
//! spike F1a's measured tracker (`ferro-http-spike/tests/common`), with the four rules that spike
//! proved load-bearing kept as they were, each pinned by a test here:
//!
//! 1. **`sent` is set by an ACCEPTED byte, never by a `poll_write` call** (M-P1a): a connection that
//!    dies at its first write reports `sent = false`.
//! 2. **Vectored writes are counted** (M-P1b): `hyper` writes vectored on TCP, so a tracker counting
//!    only `poll_write` is blind to every byte.
//! 3. **Every per-exchange count starts at zero at [`TrackState::arm`]** (review F-4): on a reused
//!    keep-alive connection the previous exchange's bytes, reads and first error never leak into
//!    this one — the read count that separates `eof_empty` from `eof_partial_head` is dispatch-
//!    relative.
//! 4. **`sent` is the OR of a plaintext half and a ciphertext half** (review F-1). F4a is plaintext,
//!    so nothing writes the ciphertext half yet; it exists so F5's `CipherTap` below TLS only has
//!    to feed it, and the `sent` rule does not change shape when TLS arrives.
//!
//! The engine reads `sent` only AFTER it has aborted `hyper`'s connection task and dropped the I/O
//! (§23.7.1 as amended by F1a): that task writes independently of the response future, so before
//! that point a later byte could still follow the reading.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Which direction an I/O error was first observed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Read,
    Write,
}

/// What the tracker has observed, shared (`Arc`) between the tracker inside `hyper` and the engine.
#[derive(Debug, Default)]
pub struct TrackState {
    armed: AtomicBool,
    plain_sent: AtomicBool,
    written_armed: AtomicU64,
    written_unarmed: AtomicU64,
    /// Ciphertext bytes the socket accepted below TLS since dispatch (review F-1). Fed by F5's
    /// `CipherTap`; zero on plaintext.
    cipher_armed: AtomicU64,
    read_armed: AtomicU64,
    read_total: AtomicU64,
    first_error: Mutex<Option<(Dir, io::ErrorKind)>>,
}

impl TrackState {
    /// Dispatch (§23.6 step 6). Every per-exchange count starts at zero here.
    pub fn arm(&self) {
        self.read_armed.store(0, Ordering::SeqCst);
        self.plain_sent.store(false, Ordering::SeqCst);
        self.written_armed.store(0, Ordering::SeqCst);
        self.cipher_armed.store(0, Ordering::SeqCst);
        *self.first_error.lock().unwrap_or_else(|p| p.into_inner()) = None;
        self.armed.store(true, Ordering::SeqCst);
    }

    /// §23.7.1: sent ⇔ the plaintext layer accepted a byte since dispatch OR the socket accepted a
    /// ciphertext byte since dispatch.
    pub fn sent(&self) -> bool {
        self.plain_sent.load(Ordering::SeqCst) || self.cipher_armed.load(Ordering::SeqCst) > 0
    }

    /// Plaintext request bytes accepted since dispatch (`HttpStats.bytes_sent`, §23.5.4).
    pub fn written_armed(&self) -> u64 {
        self.written_armed.load(Ordering::SeqCst)
    }

    /// Bytes accepted while unarmed (P1: must stay 0 between handshake and dispatch on HTTP/1.1).
    pub fn written_unarmed(&self) -> u64 {
        self.written_unarmed.load(Ordering::SeqCst)
    }

    /// Response bytes delivered to `hyper` since dispatch (`HttpStats.bytes_received`, §23.5.4).
    pub fn read(&self) -> u64 {
        self.read_armed.load(Ordering::SeqCst)
    }

    /// Response bytes over the connection's life (diagnostic only; never classifies).
    pub fn read_total(&self) -> u64 {
        self.read_total.load(Ordering::SeqCst)
    }

    /// The first I/O error since dispatch, and its direction.
    pub fn first_error(&self) -> Option<(Dir, io::ErrorKind)> {
        *self.first_error.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn note_error(&self, dir: Dir, kind: io::ErrorKind) {
        let mut slot = self.first_error.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some((dir, kind));
        }
    }

    fn note_read(&self, n: usize) {
        self.read_total.fetch_add(n as u64, Ordering::SeqCst);
        if self.armed.load(Ordering::SeqCst) {
            self.read_armed.fetch_add(n as u64, Ordering::SeqCst);
        }
    }

    fn note_written(&self, n: usize) {
        if n == 0 {
            return;
        }
        if self.armed.load(Ordering::SeqCst) {
            self.plain_sent.store(true, Ordering::SeqCst);
            self.written_armed.fetch_add(n as u64, Ordering::SeqCst);
        } else {
            self.written_unarmed.fetch_add(n as u64, Ordering::SeqCst);
        }
    }
}

/// The tracker: an I/O wrapper feeding a shared [`TrackState`].
pub struct Tracker<T> {
    inner: T,
    state: Arc<TrackState>,
}

impl<T> Tracker<T> {
    pub fn new(inner: T) -> (Self, Arc<TrackState>) {
        let state = Arc::new(TrackState::default());
        (
            Tracker {
                inner,
                state: state.clone(),
            },
            state,
        )
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Tracker<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        match &res {
            Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - before;
                self.state.note_read(n);
            }
            Poll::Ready(Err(e)) => self.state.note_error(Dir::Read, e.kind()),
            Poll::Pending => {}
        }
        res
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Tracker<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        match &res {
            Poll::Ready(Ok(n)) => self.state.note_written(*n),
            Poll::Ready(Err(e)) => self.state.note_error(Dir::Write, e.kind()),
            Poll::Pending => {}
        }
        res
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        match &res {
            Poll::Ready(Ok(n)) => self.state.note_written(*n),
            Poll::Ready(Err(e)) => self.state.note_error(Dir::Write, e.kind()),
            Poll::Pending => {}
        }
        res
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let res = Pin::new(&mut self.inner).poll_flush(cx);
        if let Poll::Ready(Err(e)) = &res {
            self.state.note_error(Dir::Write, e.kind());
        }
        res
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// Writes FAIL; reads never complete — P1's deterministic dead-at-first-write control.
    struct DeadOnWrite;
    impl AsyncRead for DeadOnWrite {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for DeadOnWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_failed_write_is_not_sent_and_names_the_write_side() {
        let (mut t, s) = Tracker::new(DeadOnWrite);
        s.arm();
        assert!(t.write_all(b"POST / HTTP/1.1\r\n").await.is_err());
        assert!(!s.sent());
        assert_eq!(s.written_armed(), 0);
        assert_eq!(s.first_error().map(|(d, _)| d), Some(Dir::Write));
    }

    #[tokio::test]
    async fn vectored_and_plain_writes_both_count_and_arming_resets() {
        let (mut t, s) = Tracker::new(tokio::io::sink());
        t.write_all(b"before").await.unwrap();
        assert_eq!((s.written_unarmed(), s.sent()), (6, false));
        s.arm();
        let bufs = [io::IoSlice::new(b"ab"), io::IoSlice::new(b"cd")];
        let n = t.write_vectored(&bufs).await.unwrap();
        assert!(n > 0);
        assert!(s.sent());
        assert_eq!(s.written_armed(), n as u64);
        s.arm();
        assert!(!s.sent(), "arming starts the exchange's counts at zero");
        assert_eq!(s.written_armed(), 0);
    }
}
