use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::query::extract_row_affected;
use crate::{Error, Statement, query, slice_iter};
use bytes::Bytes;
use futures_util::Stream;
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

pub async fn copy_out(client: &InnerClient, statement: Statement) -> Result<CopyOutStream, Error> {
    debug!("executing copy out statement {}", statement.name());

    let buf = query::encode(client, &statement, slice_iter(&[]))?;
    let responses = start(client, buf).await?;
    Ok(CopyOutStream {
        responses,
        rows_affected: None,
        pending_error: None,
        done: false,
    })
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    match responses.next().await? {
        Message::CopyOutResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pin_project! {
    /// A stream of `COPY ... TO STDOUT` query data.
    ///
    /// FERRO M3-D4 fork (see `/UPSTREAM_PR.md`, drop when upstream merges): the stream ends at
    /// `ReadyForQuery`, not at `CopyDone`. Upstream ended at `CopyDone`, so the `CommandComplete`
    /// row count was unreachable and — for a writing `COPY (INSERT … RETURNING …) TO STDOUT` in an
    /// implicit transaction — a COMMIT failure (an `ErrorResponse` after `CopyDone`) was never
    /// reported: the caller saw a clean end. A server error is reported only after `ReadyForQuery`
    /// has been read, so the connection is in step when the caller sees it.
    #[project(!Unpin)]
    pub struct CopyOutStream {
        responses: Responses,
        rows_affected: Option<u64>,
        pending_error: Option<Error>,
        done: bool,
    }
}

impl CopyOutStream {
    /// FERRO M3-D4 fork: the `COPY n` row count, once the stream has ended cleanly; `None` before.
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}

impl Stream for CopyOutStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        loop {
            match ready!(this.responses.poll_next(cx)) {
                Ok(Message::CopyData(body)) if this.pending_error.is_none() => {
                    return Poll::Ready(Some(Ok(body.into_bytes())));
                }
                Ok(Message::CopyDone) | Ok(Message::CopyData(_)) => {}
                Ok(Message::CommandComplete(body)) => {
                    if this.pending_error.is_none() {
                        match extract_row_affected(&body) {
                            Ok(n) => *this.rows_affected = Some(n),
                            Err(e) => *this.pending_error = Some(e),
                        }
                    }
                }
                Ok(Message::ReadyForQuery(_)) => {
                    *this.done = true;
                    return Poll::Ready(this.pending_error.take().map(Err));
                }
                Ok(_) => {
                    *this.done = true;
                    return Poll::Ready(Some(Err(Error::unexpected_message())));
                }
                Err(e) if e.as_db_error().is_some() => {
                    // Keep the FIRST error; read on to `ReadyForQuery`.
                    *this.rows_affected = None;
                    if this.pending_error.is_none() {
                        *this.pending_error = Some(e);
                    }
                }
                Err(e) => {
                    *this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}
