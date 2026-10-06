//! Shared scaffolding for the F1a premise tests: the plaintext write tracker the SPEC §23.7.1 design
//! names, a one-chunk request body, an HTTP/1.1 client built on `hyper::client::conn::http1`, and
//! loopback fake upstreams that misbehave on purpose.
//!
//! Compiled into every `tests/*.rs` binary that declares `mod common;`, each using a subset.
#![allow(dead_code)]

use std::convert::Infallible;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Which direction an I/O error was first observed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Read,
    Write,
}

/// What the tracker has observed. Shared (`Arc`) between the tracker inside `hyper` and the test.
#[derive(Debug, Default)]
pub struct TrackState {
    /// Armed at dispatch, by the engine (here: the test), immediately before handing the request to
    /// `hyper`. Bytes accepted while unarmed are counted separately so the "no byte before dispatch"
    /// half of P1 is an observation, not an assumption.
    armed: AtomicBool,
    /// SPEC §23.7.1: "the first `poll_write` that accepts one or more bytes sets `sent = true`".
    sent: AtomicBool,
    written_armed: AtomicU64,
    written_unarmed: AtomicU64,
    /// Response bytes the plaintext layer delivered to `hyper` (HttpStats `bytes_received`).
    read: AtomicU64,
    first_error: Mutex<Option<(Dir, io::ErrorKind)>>,
}

impl TrackState {
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    pub fn sent(&self) -> bool {
        self.sent.load(Ordering::SeqCst)
    }
    pub fn written_armed(&self) -> u64 {
        self.written_armed.load(Ordering::SeqCst)
    }
    pub fn written_unarmed(&self) -> u64 {
        self.written_unarmed.load(Ordering::SeqCst)
    }
    pub fn read(&self) -> u64 {
        self.read.load(Ordering::SeqCst)
    }
    pub fn first_error(&self) -> Option<(Dir, io::ErrorKind)> {
        *self.first_error.lock().unwrap()
    }
    fn note_error(&self, dir: Dir, kind: io::ErrorKind) {
        let mut slot = self.first_error.lock().unwrap();
        if slot.is_none() {
            *slot = Some((dir, kind));
        }
    }
    fn note_written(&self, n: usize) {
        if n == 0 {
            return;
        }
        if self.armed.load(Ordering::SeqCst) {
            // MUTATION SITE (M-P1a): setting `sent` on the poll_write CALL rather than on an
            // accepted byte makes the dies-before-first-write control report `sent = true`.
            self.sent.store(true, Ordering::SeqCst);
            self.written_armed.fetch_add(n as u64, Ordering::SeqCst);
        } else {
            self.written_unarmed.fetch_add(n as u64, Ordering::SeqCst);
        }
    }
}

/// The SPEC §23.7.1 write tracker: wraps the connection's PLAINTEXT I/O, below `hyper` and above
/// TLS (for `https` it wraps the `TlsStream`, whose reads and writes are plaintext).
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
                self.state.read.fetch_add(n as u64, Ordering::SeqCst);
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
            // MUTATION SITE (M-P1b): dropping this count (hyper writes vectored on TCP) makes the
            // tracker blind to every byte, and P1's exactness assertion fails.
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

/// An I/O whose reads never complete and whose every write FAILS: a connection that dies at the
/// first write. The deterministic form of P1's required control.
pub struct DeadOnWrite;

impl AsyncRead for DeadOnWrite {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for DeadOnWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// An in-memory upstream whose READS are scripted chunk by chunk, so what `hyper` sees per
/// `poll_read` is decided by the test, not by TCP segmentation. Writes are accepted and discarded.
/// Nothing is readable until the request has been written (a real upstream answers only after it
/// has a request; hyper's idle connection treats early bytes as an unexpected message). Once the
/// script is exhausted, reads stay pending (the connection stays open).
pub struct Scripted {
    chunks: std::collections::VecDeque<Vec<u8>>,
    written: bool,
    read_waker: Option<std::task::Waker>,
}

impl Scripted {
    /// Deliver `bytes` in reads of at most `chunk` bytes each.
    pub fn new(bytes: &[u8], chunk: usize) -> Self {
        Scripted {
            chunks: bytes.chunks(chunk).map(<[u8]>::to_vec).collect(),
            written: false,
            read_waker: None,
        }
    }
    /// Deliver `bytes` split at the given offsets (each read returns exactly one piece, or less if
    /// hyper offers less room).
    pub fn split_at(bytes: &[u8], cuts: &[usize]) -> Self {
        let mut chunks = std::collections::VecDeque::new();
        let mut start = 0;
        for &c in cuts.iter().chain(std::iter::once(&bytes.len())) {
            if c > start {
                chunks.push_back(bytes[start..c].to_vec());
                start = c;
            }
        }
        Scripted {
            chunks,
            written: false,
            read_waker: None,
        }
    }
}

impl AsyncRead for Scripted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.written {
            self.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let Some(front) = self.chunks.front_mut() else {
            return Poll::Pending;
        };
        let n = front.len().min(buf.remaining());
        buf.put_slice(&front[..n]);
        front.drain(..n);
        if front.is_empty() {
            self.chunks.pop_front();
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Scripted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !buf.is_empty() {
            self.written = true;
            if let Some(w) = self.read_waker.take() {
                w.wake();
            }
        }
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A complete request body in one frame — the shape §23.4.4/§23.9.3 give every v1 request body
/// (inline in the `REQUEST` frame). Hand-rolled so the spike's dependency set stays D20's.
pub struct OneChunk(Option<Bytes>, u64);

impl OneChunk {
    pub fn new(b: impl Into<Bytes>) -> Self {
        let b = b.into();
        let len = b.len() as u64;
        OneChunk(if b.is_empty() { None } else { Some(b) }, len)
    }
    pub fn empty() -> Self {
        OneChunk(None, 0)
    }
}

impl Body for OneChunk {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|b| Ok(Frame::data(b))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.1)
    }
}

pub fn post(body: impl Into<Bytes>) -> http::Request<OneChunk> {
    http::Request::post("/v1/charge")
        .header(http::header::HOST, "upstream.test")
        .body(OneChunk::new(body))
        .unwrap()
}

pub fn get() -> http::Request<OneChunk> {
    http::Request::get("/v1/things")
        .header(http::header::HOST, "upstream.test")
        .body(OneChunk::empty())
        .unwrap()
}

/// The SPEC §23.9.1 response-head limits, applied to an HTTP/1.1 client builder.
pub const H1_MAX_BUF_SIZE: usize = 256 * 1024;
pub const H1_MAX_HEADERS: usize = 256;

pub fn h1_builder() -> http1::Builder {
    let mut b = http1::Builder::new();
    b.max_buf_size(H1_MAX_BUF_SIZE).max_headers(H1_MAX_HEADERS);
    b
}

pub type ConnTask = JoinHandle<Result<(), hyper::Error>>;

/// Handshake `hyper`'s HTTP/1.1 client over `io` wrapped in a tracker; the connection future is
/// spawned (it must be polled for anything to happen) and returned so a test can await its end.
pub async fn h1_over<T>(
    io: T,
    builder: &http1::Builder,
) -> (http1::SendRequest<OneChunk>, Arc<TrackState>, ConnTask)
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tracked, state) = Tracker::new(io);
    let (send, conn) = builder
        .handshake(TokioIo::new(tracked))
        .await
        .expect("http1 handshake (no I/O happens here)");
    let task = tokio::spawn(conn);
    (send, state, task)
}

/// A loopback upstream that serves exactly ONE connection with `script`, and reports how many
/// request bytes it received in total.
pub struct FakeUpstream {
    pub addr: std::net::SocketAddr,
    pub received: Arc<AtomicU64>,
    pub done: JoinHandle<()>,
}

pub async fn fake_upstream<F, Fut>(script: F) -> FakeUpstream
where
    F: FnOnce(TcpStream, Arc<AtomicU64>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let received = Arc::new(AtomicU64::new(0));
    let r = received.clone();
    let done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        script(stream, r).await;
    });
    FakeUpstream {
        addr,
        received,
        done,
    }
}

/// Read until the end of the request head (`\r\n\r\n`), then `body_len` more bytes, counting every
/// byte into `received`. Returns early (without panicking) on EOF or error.
pub async fn read_request(s: &mut TcpStream, received: &AtomicU64, body_len: usize) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut seen: Vec<u8> = Vec::new();
    let mut head_end = None;
    loop {
        if let Some(end) = head_end
            && seen.len() >= end + body_len
        {
            return;
        }
        let n = match s.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        received.fetch_add(n as u64, Ordering::SeqCst);
        if head_end.is_none() {
            seen.extend_from_slice(&buf[..n]);
            if let Some(p) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
                head_end = Some(p + 4);
            }
        } else {
            // Past the head we only need the count, not the bytes.
            seen.resize(seen.len() + n, 0);
        }
    }
}

/// Close with a TCP RST instead of a FIN (`SO_LINGER = 0`). tokio deprecates `set_linger` because a
/// NON-zero linger blocks the thread on drop; a zero linger never blocks (the kernel discards and
/// sends RST at once), which is exactly the fault this harness injects.
#[allow(deprecated)]
pub fn reset(s: TcpStream) {
    s.set_linger(Some(Duration::ZERO)).unwrap();
    drop(s);
}

pub async fn dial(addr: std::net::SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

/// A real-time bound so a hung exchange fails the test instead of hanging CI.
pub async fn within<F: std::future::Future>(what: &str, f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .unwrap_or_else(|_| panic!("timed out: {what}"))
}
