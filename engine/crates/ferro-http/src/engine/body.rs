//! The request body `hyper` sends: one complete chunk, because a v1 request body rides inline in the
//! `REQUEST` frame (§23.4.4, §23.9.3) — which is what makes `sent` well defined and lets the engine
//! drop `Expect`. Hand-rolled (no `http-body-util`, which D20 does not adopt), as in spike F1a.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

/// A whole request body in at most one data frame.
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
