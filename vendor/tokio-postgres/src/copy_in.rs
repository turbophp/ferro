use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::query::extract_row_affected;
use crate::{Error, Statement, query, slice_iter};
use bytes::{Buf, BufMut, BytesMut};
use futures_channel::mpsc;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

enum CopyInMessage {
    Message(FrontendMessage),
    Done,
    /// FERRO M3-D4 fork (see `/UPSTREAM_PR.md`): the statement failed BEFORE the server entered
    /// copy-in mode, so the `Sync` already sent with `Bind`/`Execute` has ended it. End this
    /// request's message stream WITHOUT the `CopyFail` + `Sync` a dropped sender produces: that
    /// second `Sync` draws a second `ReadyForQuery` the connection has no request for, and the
    /// connection dies (`unexpected message from server`).
    Abandon,
}

pub struct CopyInReceiver {
    receiver: mpsc::Receiver<CopyInMessage>,
    done: bool,
}

impl CopyInReceiver {
    fn new(receiver: mpsc::Receiver<CopyInMessage>) -> CopyInReceiver {
        CopyInReceiver {
            receiver,
            done: false,
        }
    }
}

impl Stream for CopyInReceiver {
    type Item = FrontendMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<FrontendMessage>> {
        if self.done {
            return Poll::Ready(None);
        }

        match ready!(self.receiver.poll_next_unpin(cx)) {
            Some(CopyInMessage::Message(message)) => Poll::Ready(Some(message)),
            Some(CopyInMessage::Abandon) => {
                self.done = true;
                Poll::Ready(None)
            }
            Some(CopyInMessage::Done) => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_done(&mut buf);
                frontend::sync(&mut buf);
                Poll::Ready(Some(FrontendMessage::Raw(buf.freeze())))
            }
            None => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Poll::Ready(Some(FrontendMessage::Raw(buf.freeze())))
            }
        }
    }
}

enum SinkState {
    Active,
    Closing,
    Reading,
    /// FERRO M3-D4 fork (see `/UPSTREAM_PR.md`, drop when upstream merges): `CommandComplete`
    /// arrived with this row count; the copy is NOT done until `ReadyForQuery`, because an implicit
    /// transaction commits at the `Sync` that follows — and a commit-time failure (a deferred
    /// constraint) arrives as an `ErrorResponse` between the two.
    Draining(u64),
    /// FERRO M3-D4 fork: a server error arrived while finishing; keep reading to `ReadyForQuery` so
    /// the connection is in step, then report it.
    Failed(Option<Error>),
    /// FERRO M3-D4 fork: `abort` closed the data channel (the driver then sends `CopyFail` +
    /// `Sync`); reading the acknowledgement up to `ReadyForQuery`.
    Aborting,
    /// FERRO M3-D4 fork: `ReadyForQuery` consumed; the request is over.
    Done,
}

pin_project! {
    /// A sink for `COPY ... FROM STDIN` query data.
    ///
    /// The copy *must* be explicitly completed via the `Sink::close` or `finish` methods. If it is
    /// not, the copy will be aborted.
    #[project(!Unpin)]
    pub struct CopyInSink<T> {
        #[pin]
        sender: mpsc::Sender<CopyInMessage>,
        responses: Responses,
        buf: BytesMut,
        state: SinkState,
        _p2: PhantomData<T>,
    }
}

impl<T> CopyInSink<T>
where
    T: Buf + 'static + Send,
{
    /// A poll-based version of `finish`.
    pub fn poll_finish(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<u64, Error>> {
        loop {
            match self.state {
                SinkState::Active => {
                    ready!(self.as_mut().poll_flush(cx))?;
                    let mut this = self.as_mut().project();
                    ready!(this.sender.as_mut().poll_ready(cx)).map_err(|_| Error::closed())?;
                    this.sender
                        .start_send(CopyInMessage::Done)
                        .map_err(|_| Error::closed())?;
                    *this.state = SinkState::Closing;
                }
                SinkState::Closing => {
                    let this = self.as_mut().project();
                    ready!(this.sender.poll_close(cx)).map_err(|_| Error::closed())?;
                    *this.state = SinkState::Reading;
                }
                SinkState::Reading => {
                    let this = self.as_mut().project();
                    match ready!(this.responses.poll_next(cx)) {
                        Ok(Message::CommandComplete(body)) => {
                            let rows = extract_row_affected(&body)?;
                            // FERRO M3-D4 fork: upstream returned `Ok(rows)` HERE — before the
                            // implicit transaction's COMMIT, whose failure then went unreported.
                            *this.state = SinkState::Draining(rows);
                        }
                        Ok(_) => return Poll::Ready(Err(Error::unexpected_message())),
                        Err(e) => *this.state = Self::failed_or_return(e)?,
                    }
                }
                SinkState::Draining(rows) => {
                    let this = self.as_mut().project();
                    match ready!(this.responses.poll_next(cx)) {
                        Ok(Message::ReadyForQuery(_)) => {
                            *this.state = SinkState::Done;
                            return Poll::Ready(Ok(rows));
                        }
                        Ok(_) => return Poll::Ready(Err(Error::unexpected_message())),
                        Err(e) => *this.state = Self::failed_or_return(e)?,
                    }
                }
                SinkState::Failed(_) => {
                    let this = self.as_mut().project();
                    match ready!(this.responses.poll_next(cx)) {
                        Ok(Message::ReadyForQuery(_)) => {
                            let prev = std::mem::replace(this.state, SinkState::Done);
                            let SinkState::Failed(e) = prev else {
                                unreachable!("state checked above")
                            };
                            return Poll::Ready(Err(e.unwrap_or_else(Error::unexpected_message)));
                        }
                        // Anything else before `ReadyForQuery` is skipped: the first error is
                        // the one worth reporting.
                        Ok(_) => {}
                        Err(e) if e.as_db_error().is_some() => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                SinkState::Aborting | SinkState::Done => {
                    return Poll::Ready(Err(Error::closed()));
                }
            }
        }
    }

    /// FERRO M3-D4 fork: a SERVER error (an `ErrorResponse`) is followed by `ReadyForQuery`, which
    /// is read before it is reported; any other error (the connection closed) is returned at once.
    fn failed_or_return(e: Error) -> Result<SinkState, Error> {
        if e.as_db_error().is_some() {
            Ok(SinkState::Failed(Some(e)))
        } else {
            Err(e)
        }
    }

    /// FERRO M3-D4 fork (see `/UPSTREAM_PR.md`): abandon the copy BEFORE its end-of-data and wait
    /// for the server to acknowledge it. Closes the data channel — the connection then sends
    /// `CopyFail` + `Sync`, exactly what dropping the sink does upstream — and reads the server's
    /// answer up to `ReadyForQuery`, so the caller learns that the copy is over, that nothing of it
    /// can commit (PostgreSQL completes a `COPY FROM STDIN` only on `CopyDone`), and that the
    /// connection is back in step. Data buffered but not yet flushed is discarded.
    ///
    /// Upstream offers only the drop, which sends the same messages and reads nothing: the caller
    /// cannot tell when the connection is usable again, and the transaction-status byte it reads
    /// next is stale. Called after `finish` has begun, it does not abort anything — `CopyDone` has
    /// already been sent — and returns `Error::closed()` once that finish has ended.
    pub fn poll_abort(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            match self.state {
                SinkState::Active => {
                    let this = self.as_mut().project();
                    this.buf.clear();
                    ready!(this.sender.poll_close(cx)).map_err(|_| Error::closed())?;
                    *this.state = SinkState::Aborting;
                }
                SinkState::Aborting => {
                    let this = self.as_mut().project();
                    match ready!(this.responses.poll_next(cx)) {
                        Ok(Message::ReadyForQuery(_)) => {
                            *this.state = SinkState::Done;
                            return Poll::Ready(Ok(()));
                        }
                        // The expected `ErrorResponse` (`COPY from stdin failed`, or whatever error
                        // ended the copy first) and anything else ahead of `ReadyForQuery`.
                        Ok(_) => {}
                        Err(e) if e.as_db_error().is_some() => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                SinkState::Closing
                | SinkState::Reading
                | SinkState::Draining(_)
                | SinkState::Failed(_) => {
                    return self.poll_finish(cx).map_ok(|_| ());
                }
                SinkState::Done => return Poll::Ready(Err(Error::closed())),
            }
        }
    }

    /// FERRO M3-D4 fork: see [`CopyInSink::poll_abort`].
    pub async fn abort(mut self: Pin<&mut Self>) -> Result<(), Error> {
        future::poll_fn(|cx| self.as_mut().poll_abort(cx)).await
    }

    /// Completes the copy, returning the number of rows inserted.
    ///
    /// The `Sink::close` method is equivalent to `finish`, except that it does not return the
    /// number of rows.
    pub async fn finish(mut self: Pin<&mut Self>) -> Result<u64, Error> {
        future::poll_fn(|cx| self.as_mut().poll_finish(cx)).await
    }
}

impl<T> Sink<T> for CopyInSink<T>
where
    T: Buf + 'static + Send,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.project();
        // FERRO M3-D4 fork (see `/UPSTREAM_PR.md`): while data is still being sent, the only thing
        // the server can say is that the copy FAILED (a malformed row, a constraint, a cancel). It
        // stops reading the data then and discards it up to the `Sync` the driver sends only at
        // `finish`/`CopyFail`, so upstream kept sending — possibly gigabytes — into a copy that had
        // already failed, and learned of it only at `finish`. Report it to the next send instead;
        // the caller then aborts.
        if let SinkState::Active = this.state {
            match this.responses.poll_next(cx) {
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(_)) => return Poll::Ready(Err(Error::unexpected_message())),
                Poll::Pending => {}
            }
        }
        this.sender.poll_ready(cx).map_err(|_| Error::closed())
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Error> {
        let this = self.project();

        let data: Box<dyn Buf + Send> = if item.remaining() > 4096 {
            if this.buf.is_empty() {
                Box::new(item)
            } else {
                Box::new(this.buf.split().freeze().chain(item))
            }
        } else {
            this.buf.put(item);
            if this.buf.len() > 4096 {
                Box::new(this.buf.split().freeze())
            } else {
                return Ok(());
            }
        };

        let data = CopyData::new(data).map_err(Error::encode)?;
        this.sender
            .start_send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .map_err(|_| Error::closed())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let mut this = self.project();

        if !this.buf.is_empty() {
            ready!(this.sender.as_mut().poll_ready(cx)).map_err(|_| Error::closed())?;
            let data: Box<dyn Buf + Send> = Box::new(this.buf.split().freeze());
            let data = CopyData::new(data).map_err(Error::encode)?;
            this.sender
                .as_mut()
                .start_send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
                .map_err(|_| Error::closed())?;
        }

        this.sender.poll_flush(cx).map_err(|_| Error::closed())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.poll_finish(cx).map_ok(|_| ())
    }
}

#[derive(PartialEq)]
enum Expect {
    BindComplete,
    CopyInResponse,
}

pub async fn copy_in<T>(client: &InnerClient, statement: Statement) -> Result<CopyInSink<T>, Error>
where
    T: Buf + 'static + Send,
{
    debug!("executing copy in statement {}", statement.name());

    let buf = query::encode(client, &statement, slice_iter(&[]))?;

    let (mut sender, receiver) = mpsc::channel(1);
    let receiver = CopyInReceiver::new(receiver);
    let mut responses = client.send(RequestMessages::CopyIn(receiver))?;

    sender
        .send(CopyInMessage::Message(FrontendMessage::Raw(buf)))
        .await
        .map_err(|_| Error::closed())?;

    // FERRO M3-D4 fork: a server error before copy-in mode (an unknown table, a bind failure) is
    // followed by the `ReadyForQuery` of the `Sync` already sent; abandon the data stream so no
    // second `Sync` follows it (see `CopyInMessage::Abandon`). Upstream killed the connection here.
    for expected in [Expect::BindComplete, Expect::CopyInResponse] {
        match responses.next().await {
            Ok(Message::BindComplete) if expected == Expect::BindComplete => {}
            Ok(Message::CopyInResponse(_)) if expected == Expect::CopyInResponse => {}
            Ok(_) => return Err(Error::unexpected_message()),
            Err(e) => {
                if e.as_db_error().is_some() {
                    let _ = sender.send(CopyInMessage::Abandon).await;
                }
                return Err(e);
            }
        }
    }

    Ok(CopyInSink {
        sender,
        responses,
        buf: BytesMut::new(),
        state: SinkState::Active,
        _p2: PhantomData,
    })
}
