//! The SPEC §23.7.1 write tracker: what decides `sent`.
//!
//! It wraps the connection's PLAINTEXT I/O, below `hyper` (and, on `https`, above TLS). Built on
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
//! 4. **`sent` is measured where the bytes meet the SOCKET** (review F-1, M6-F5a). On `http` the
//!    plaintext layer IS the socket, so `sent` is the plaintext count. On `https` it is the
//!    [`CipherTap`]'s count below TLS ALONE: `tokio-rustls` 0.26.6 can fail a plaintext write after
//!    earlier encrypted records of that same write reached the socket (F-1, so the plaintext count
//!    can read zero while the upstream decrypted the head), AND it can ACCEPT a plaintext write into
//!    `rustls`'s buffer while the socket took nothing (`poll_write` returns `Ok(n)` once `n > 0`
//!    plaintext bytes are buffered and the socket write is `Pending`). Those buffered bytes never
//!    reach the wire once the connection is discarded, so they are UNSENT; F1a's "plaintext OR
//!    ciphertext" counted them as sent (§23.7.1 as amended by M6-F5a, SPEC §22.2 (dc)).
//! 5. **Ferro never sends `close_notify`** (M6-F5a). On `https` the plaintext [`Tracker`] does not
//!    forward `poll_shutdown` to TLS: `hyper` shuts its I/O down on its own after delivering some
//!    errors (rule above), and the alert `rustls` would then write is a ciphertext byte after
//!    dispatch carrying no request byte — it would turn a request that never left into "sent". The
//!    socket closes when the connection is dropped, as every discard does.
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
    /// The connection runs TLS: `sent` is the ciphertext count (rule 4), and the plaintext
    /// tracker never forwards a shutdown (rule 5). Fixed at construction.
    tls: bool,
    armed: AtomicBool,
    plain_sent: AtomicBool,
    written_armed: AtomicU64,
    written_unarmed: AtomicU64,
    /// Ciphertext bytes the socket accepted below TLS since dispatch (review F-1). Fed by the
    /// [`CipherTap`]; zero on plaintext.
    cipher_armed: AtomicU64,
    /// Ciphertext bytes the socket accepted while unarmed (the handshake; diagnostic).
    cipher_unarmed: AtomicU64,
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

    /// §23.7.1 (as amended by M6-F5a): sent ⇔ the SOCKET accepted a byte since dispatch — the
    /// plaintext count on `http`, the ciphertext count below TLS on `https` (module docs, rule 4).
    pub fn sent(&self) -> bool {
        if self.tls {
            self.cipher_armed.load(Ordering::SeqCst) > 0
        } else {
            self.plain_sent.load(Ordering::SeqCst)
        }
    }

    /// Whether the connection runs TLS.
    pub fn is_tls(&self) -> bool {
        self.tls
    }

    /// Ciphertext bytes the socket accepted since dispatch.
    pub fn cipher_armed(&self) -> u64 {
        self.cipher_armed.load(Ordering::SeqCst)
    }

    /// Ciphertext bytes the socket accepted while unarmed (the handshake, before dispatch).
    pub fn cipher_unarmed(&self) -> u64 {
        self.cipher_unarmed.load(Ordering::SeqCst)
    }

    /// Whether the plaintext layer accepted a byte since dispatch (on `https`: into `rustls`'s
    /// buffer, which is not the wire — diagnostics and tests only).
    pub fn plaintext_sent(&self) -> bool {
        self.plain_sent.load(Ordering::SeqCst)
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

    fn note_cipher(&self, n: usize) {
        if n == 0 {
            return;
        }
        if self.armed.load(Ordering::SeqCst) {
            self.cipher_armed.fetch_add(n as u64, Ordering::SeqCst);
        } else {
            self.cipher_unarmed.fetch_add(n as u64, Ordering::SeqCst);
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
    /// The plaintext tracker of an `http` connection: it sits directly on the socket.
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

    /// The plaintext tracker of an `https` connection, above TLS, sharing the state its
    /// [`CipherTap`] below TLS feeds.
    pub fn over_tls(inner: T, state: Arc<TrackState>) -> Self {
        debug_assert!(state.tls, "a TLS tracker shares a CipherTap's state");
        Tracker { inner, state }
    }

    /// The wrapped I/O (tests: the controls that bypass the tracker's rules).
    #[cfg(test)]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

/// The ciphertext half (review F-1): wraps the TRANSPORT below TLS and counts the encrypted bytes
/// the socket accepts into the same [`TrackState`] the plaintext [`Tracker`] above TLS uses. On
/// `https` this count alone decides `sent` (module docs, rule 4). Reads pass through uncounted;
/// write errors are left to the plaintext layer, which sees `rustls`'s report of them.
#[derive(Debug)]
pub struct CipherTap<T> {
    inner: T,
    state: Arc<TrackState>,
}

impl<T> CipherTap<T> {
    /// Below TLS, with a fresh TLS-mode state for [`Tracker::over_tls`].
    pub fn new(inner: T) -> (Self, Arc<TrackState>) {
        let state = Arc::new(TrackState {
            tls: true,
            ..TrackState::default()
        });
        (
            CipherTap {
                inner,
                state: state.clone(),
            },
            state,
        )
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for CipherTap<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for CipherTap<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &res {
            self.state.note_cipher(*n);
        }
        res
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(n)) = &res {
            self.state.note_cipher(*n);
        }
        res
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
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
        if self.state.tls {
            // Rule 5: no `close_notify`. Forwarding would make `rustls` write an alert record —
            // a ciphertext byte after dispatch that carries no request byte. The socket closes when
            // the connection is dropped.
            return Poll::Ready(Ok(()));
        }
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

    /// Both of the [`CipherTap`]'s write paths count (rule 4). `tokio-rustls` 0.26.6 writes only
    /// vectored, so the non-vectored `poll_write` is unexercised by every TLS test — and a
    /// version that routes through it would, uncounted, report a delivered request as unsent (the
    /// HIGH direction). This drives each path directly (review L4).
    #[tokio::test]
    async fn the_cipher_tap_counts_plain_and_vectored_socket_writes() {
        let (mut tap, s) = CipherTap::new(tokio::io::sink());
        // `write` is `poll_write`; `write_vectored` is `poll_write_vectored`.
        let n = tap.write(b"handshake").await.unwrap();
        assert_eq!(
            (s.cipher_unarmed(), s.cipher_armed(), s.sent()),
            (n as u64, 0, false)
        );
        s.arm();
        let n = tap.write(b"record").await.unwrap();
        assert!(n > 0);
        assert_eq!(s.cipher_armed(), n as u64, "poll_write counts");
        assert!(s.sent());
        s.arm();
        assert!(!s.sent());
        let bufs = [io::IoSlice::new(b"ab"), io::IoSlice::new(b"cd")];
        let n = tap.write_vectored(&bufs).await.unwrap();
        assert!(n > 0);
        assert_eq!(s.cipher_armed(), n as u64, "poll_write_vectored counts");
        assert!(s.sent());
    }

    /// Writes are HELD (pending) until `open`; reads deliver `head` only after the first write
    /// attempt, so a head arrives while the request is still buffered inside `hyper`.
    struct HeldWrites {
        head: Option<&'static [u8]>,
        open: Arc<AtomicBool>,
        waker: Arc<Mutex<Option<std::task::Waker>>>,
        tried: AtomicBool,
    }
    impl AsyncRead for HeldWrites {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.tried.load(Ordering::SeqCst)
                && let Some(h) = self.head.take()
            {
                buf.put_slice(h);
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        }
    }
    impl AsyncWrite for HeldWrites {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            b: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.open.load(Ordering::SeqCst) {
                return Poll::Ready(Ok(b.len()));
            }
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            if !self.tried.swap(true, Ordering::SeqCst) {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// **Why `sent` is read only after the connection task is aborted and awaited (§23.7.1,
    /// review round 2 on R8).** `hyper` 1.11.1 delivers a head-read error to the request's future
    /// and then KEEPS FLUSHING the buffered request: `close()` after the error
    /// (`proto/h1/dispatch.rs` 334–341) does not stop the same loop's `poll_flush`, and
    /// `Buffered::poll_shutdown` (`proto/h1/io.rs` 330) flushes before shutting down. So at the
    /// moment the error is seen `sent` can be false, and become true afterwards if the task is left
    /// running — a read taken before the discard could report a POST that reached the upstream as
    /// unsent (Retryable), against charter rule 3. This pins the `hyper` premise the engine's
    /// discard-first rule rests on; if a `hyper` upgrade stops flushing, this fails and the rule's
    /// stated reason must be re-derived (the rule itself stays).
    #[tokio::test]
    async fn hyper_keeps_writing_the_request_after_delivering_a_head_error() {
        let open = Arc::new(AtomicBool::new(false));
        let waker = Arc::new(Mutex::new(None));
        let io = HeldWrites {
            head: Some(b"garbage garbage\r\n\r\n"),
            open: open.clone(),
            waker: waker.clone(),
            tried: AtomicBool::new(false),
        };
        let (t, s) = Tracker::new(io);
        let (mut sender, conn) = hyper::client::conn::http1::handshake::<
            _,
            crate::engine::body::OneChunk,
        >(hyper_util::rt::TokioIo::new(t))
        .await
        .unwrap();
        let task = tokio::spawn(conn);
        s.arm();
        let req = http::Request::builder()
            .method("POST")
            .uri("/")
            .header("host", "x")
            .header("content-length", "6")
            .body(crate::engine::body::OneChunk::new(
                bytes::Bytes::from_static(b"charge"),
            ))
            .unwrap();
        let err = sender
            .try_send_request(req)
            .await
            .expect_err("a malformed head is an error");
        assert!(
            err.message().is_none(),
            "the request was dispatched, not handed back"
        );
        assert!(!s.sent(), "nothing had left when the error was delivered");
        // Release the held writes without aborting the connection task: hyper flushes the request.
        open.store(true, Ordering::SeqCst);
        if let Some(w) = waker.lock().unwrap().take() {
            w.wake();
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
        assert!(
            s.sent(),
            "hyper wrote the request after the head error had been delivered"
        );
    }
}
