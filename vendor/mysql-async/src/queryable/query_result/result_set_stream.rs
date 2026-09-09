// Copyright (c) 2021 mysql_async developers.
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::any::type_name;
use std::borrow::Cow;
use std::sync::Arc;
use std::task::Poll;
use std::{fmt, marker::PhantomData};

use futures_core::FusedStream;
use futures_core::{future::BoxFuture, Stream};
use futures_util::FutureExt;
use mysql_common::packets::{Column, OkPacket};

use crate::{
    conn::PendingResult,
    prelude::{FromRow, Protocol},
    QueryResult, Row,
};

enum CowMut<'r, 'a: 'r, 't: 'a, P> {
    Borrowed(&'r mut QueryResult<'a, 't, P>),
    Owned(QueryResult<'a, 't, P>),
}

impl<'r, 'a: 'r, 't: 'a, P> fmt::Debug for CowMut<'r, 'a, 't, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Borrowed(arg0) => f.debug_tuple("Borrowed").field(arg0).finish(),
            Self::Owned(arg0) => f.debug_tuple("Owned").field(arg0).finish(),
        }
    }
}

impl<'r, 'a: 'r, 't: 'a, P> AsMut<QueryResult<'a, 't, P>> for CowMut<'r, 'a, 't, P> {
    fn as_mut(&mut self) -> &mut QueryResult<'a, 't, P> {
        match self {
            CowMut::Borrowed(q) => q,
            CowMut::Owned(q) => q,
        }
    }
}

enum ResultSetStreamState<'r, 'a: 'r, 't: 'a, P> {
    Idle(CowMut<'r, 'a, 't, P>),
    NextFut(BoxFuture<'r, (crate::Result<Option<Row>>, CowMut<'r, 'a, 't, P>)>),
}

impl<'r, 'a: 'r, 't: 'a, P> fmt::Debug for ResultSetStreamState<'r, 'a, 't, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle(arg0) => f.debug_tuple("Idle").field(arg0).finish(),
            Self::NextFut(_arg0) => f
                .debug_tuple("NextFut")
                .field(&type_name::<
                    BoxFuture<'r, (crate::Result<Option<Row>>, CowMut<'r, 'a, 't, P>)>,
                >())
                .finish(),
        }
    }
}

#[derive(Debug)]
/// Rows stream for a single result set.
pub struct ResultSetStream<'r, 'a: 'r, 't: 'a, T, P> {
    query_result: Option<ResultSetStreamState<'r, 'a, 't, P>>,
    ok_packet: Option<OkPacket<'static>>,
    columns: Arc<[Column]>,
    /// FERRO FORK (see `UPSTREAM_PR_MYSQL_ASYNC.md`): set once the stream has
    /// yielded its terminal `None` (or an error). The state — and with it an
    /// OWNED connection — is retained instead of being dropped at that point,
    /// so exhaustion no longer closes an owned connection out from under the
    /// caller; [`ResultSetStream::into_conn`] can recover it. The connection
    /// now closes when the stream itself is dropped (or is recovered via
    /// `into_conn`), rather than at the terminal `None`/`Err`. For a caller
    /// that drops the stream right after the last row this is unobservable;
    /// a caller that keeps the exhausted stream alive to read the trailing
    /// accessors (`affected_rows`/`last_insert_id`/`info`/`get_warnings`) and
    /// does more work before dropping it now holds the connection open for
    /// that window — recover it promptly with `into_conn` to avoid that.
    done: bool,
    __from_row_type: PhantomData<T>,
}

impl<'r, 'a: 'r, 't: 'a, T, P> FusedStream for ResultSetStream<'r, 'a, 't, T, P>
where
    P: Protocol + Unpin,
    T: FromRow + Unpin + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        self.done || self.query_result.is_none()
    }
}

impl<'r, 'a: 'r, 't: 'a, T, P> ResultSetStream<'r, 'a, 't, T, P> {
    /// See [`Conn::last_insert_id`][1].
    ///
    /// [1]: crate::Conn::last_insert_id
    pub fn last_insert_id(&self) -> Option<u64> {
        self.ok_packet.as_ref().and_then(|ok| ok.last_insert_id())
    }

    /// See [`Conn::affected_rows`][1].
    ///
    /// [1]: crate::Conn::affected_rows
    pub fn affected_rows(&self) -> u64 {
        self.ok_packet
            .as_ref()
            .map(|ok| ok.affected_rows())
            .unwrap_or_default()
    }

    /// See [`QueryResult::columns_ref`].
    pub fn columns_ref(&self) -> &[Column] {
        &self.columns[..]
    }

    /// See [`QueryResult::columns`].
    pub fn columns(&self) -> Arc<[Column]> {
        self.columns.clone()
    }

    /// See [`Conn::info`][1].
    ///
    /// [1]: crate::Conn::info
    pub fn info(&self) -> Cow<'_, str> {
        self.ok_packet
            .as_ref()
            .and_then(|ok| ok.info_str())
            .unwrap_or_default()
    }

    /// See [`Conn::get_warnings`][1].
    ///
    /// [1]: crate::Conn::get_warnings
    pub fn get_warnings(&self) -> u16 {
        self.ok_packet
            .as_ref()
            .map(|ok| ok.warnings())
            .unwrap_or_default()
    }

    /// Returns an `OkPacket` corresponding to this result set.
    ///
    /// Will be `None` if there is no OK packet (result set contains an error).
    pub fn ok_packet(&self) -> Option<&OkPacket<'static>> {
        self.ok_packet.as_ref()
    }
}

impl<'r, 'a: 'r, 't: 'a, T, P> ResultSetStream<'r, 'a, 't, T, P>
where
    P: Protocol + Unpin,
    T: FromRow + Unpin + Send + 'static,
{
    /// FERRO FORK ADDITION — see `UPSTREAM_PR_MYSQL_ASYNC.md`.
    ///
    /// Consumes the stream and returns the OWNED [`crate::Conn`] it was
    /// driving, after draining every unconsumed row and pending result set so
    /// the connection is protocol-clean and immediately reusable. This is the
    /// missing exit from the owned-connection route: without it, the only way
    /// out of a `ResultSetStream` built over an owned connection is to drop
    /// it, which closes the connection — unusable for a connection pool that
    /// must stream a result set and then RECYCLE the same server session.
    ///
    /// Only a stream that owns its connection can be recovered — one built
    /// through [`QueryResult::stream_and_drop`] on an owned-connection
    /// `QueryResult` (e.g. the [`crate::prelude::Query::stream`] route). A
    /// stream over a borrowed connection or a transaction returns
    /// [`crate::DriverError::StreamDoesNotOwnConn`]; the caller still holds
    /// the connection in those cases, so there is nothing to recover.
    ///
    /// Post-drain, the connection's own accessors (`Conn::affected_rows`,
    /// `Conn::last_ok_packet`) reflect the FINAL packet of the drained
    /// statement — the value a caller must use for row counts, since this
    /// stream's [`ResultSetStream::affected_rows`] reports the ok-packet
    /// captured at stream SETUP (the previous statement's).
    ///
    /// An error while resolving an in-flight row future or during the drain
    /// propagates and the connection is dropped (closed), exactly as if the
    /// stream itself had been dropped at that error.
    pub async fn into_conn(mut self) -> crate::Result<crate::Conn> {
        let cow = match self.query_result.take() {
            Some(ResultSetStreamState::Idle(cow)) => cow,
            Some(ResultSetStreamState::NextFut(fut)) => {
                // Resolve the in-flight row future so the state machine hands
                // the connection back; the row it fetched (if any) is part of
                // the drain and is discarded.
                let (row, cow) = fut.await;
                row?;
                cow
            }
            None => return Err(crate::DriverError::StreamDoesNotOwnConn.into()),
        };
        match cow {
            CowMut::Owned(query_result) => query_result.into_conn().await,
            CowMut::Borrowed(_) => Err(crate::DriverError::StreamDoesNotOwnConn.into()),
        }
    }
}

impl<'r, 'a: 'r, 't: 'a, T, P> Stream for ResultSetStream<'r, 'a, 't, T, P>
where
    P: Protocol + Unpin,
    T: FromRow + Unpin + Send + 'static,
{
    type Item = crate::Result<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            let columns = this.columns.clone();
            match this.query_result.take() {
                Some(ResultSetStreamState::Idle(mut query_result)) => {
                    let fut = Box::pin(async move {
                        let row = query_result.as_mut().next_row_or_next_set2(columns).await;
                        (row, query_result)
                    });
                    this.query_result = Some(ResultSetStreamState::NextFut(fut));
                }
                Some(ResultSetStreamState::NextFut(mut fut)) => match fut.poll_unpin(cx) {
                    Poll::Ready((row, query_result)) => match row {
                        Ok(Some(row)) => {
                            this.query_result = Some(ResultSetStreamState::Idle(query_result));
                            return Poll::Ready(Some(Ok(crate::from_row(row))));
                        }
                        // FERRO FORK: retain the state on both terminal arms
                        // (see the `done` field) instead of dropping it — and
                        // with it an owned connection — here.
                        Ok(None) => {
                            this.done = true;
                            this.query_result =
                                Some(ResultSetStreamState::Idle(query_result));
                            return Poll::Ready(None);
                        }
                        Err(err) => {
                            this.done = true;
                            this.query_result =
                                Some(ResultSetStreamState::Idle(query_result));
                            return Poll::Ready(Some(Err(err)));
                        }
                    },
                    Poll::Pending => {
                        this.query_result = Some(ResultSetStreamState::NextFut(fut));
                        return Poll::Pending;
                    }
                },
                None => return Poll::Ready(None),
            }
        }
    }
}

impl<'a, 't: 'a, P> QueryResult<'a, 't, P>
where
    P: Protocol + Unpin,
{
    async fn setup_stream(
        &mut self,
    ) -> crate::Result<Option<(Option<OkPacket<'static>>, Arc<[Column]>)>> {
        match self.conn.as_mut().use_pending_result()? {
            Some(PendingResult::Taken(meta)) => {
                let meta = (*meta).clone();
                self.skip_taken(meta).await?;
            }
            Some(_) => (),
            None => return Ok(None),
        }

        let ok_packet = self.conn.last_ok_packet().cloned();
        let columns = match self.conn.as_mut().take_pending_result()? {
            Some(meta) => meta.columns().clone(),
            None => return Ok(None),
        };

        Ok(Some((ok_packet, columns)))
    }

    /// Returns a [`Stream`] for the current result set.
    ///
    /// The returned stream satisfies [`futures_util::TryStream`],
    /// so you can use [`futures_util::TryStreamExt`] functions on it.
    ///
    /// # Behavior
    ///
    /// ## Conversion
    ///
    /// This stream will convert each row into `T` using [`FromRow`] implementation.
    /// If the row type is unknown please use the [`Row`] type for `T`
    /// to make this conversion infallible.
    ///
    /// ## Consumption
    ///
    /// The call to [`QueryResult::stream`] entails the consumption of the current result set,
    /// practically this means that the second call to [`QueryResult::stream`] will return
    /// the next result set stream even if the stream returned from the first call wasn't
    /// explicitly consumed:
    ///
    /// ```rust
    /// # use mysql_async::test_misc::get_opts;
    /// # #[tokio::main]
    /// # async fn main() -> mysql_async::Result<()> {
    /// # use mysql_async::*;
    /// # use mysql_async::prelude::*;
    /// # use futures_util::StreamExt;
    /// let mut conn = Conn::new(get_opts()).await?;
    ///
    /// // This query result will contain two result sets.
    /// let mut result = conn.query_iter("SELECT 1; SELECT 2;").await?;
    ///
    /// // The first result set stream is dropped here without being consumed,
    /// let _ = result.stream::<u8>().await?;
    /// // so it will be implicitly consumed here.
    /// let mut stream = result.stream::<u8>().await?.expect("the second result set must be here");
    /// assert_eq!(2_u8, stream.next().await.unwrap()?);
    ///
    /// # drop(stream); drop(result); conn.disconnect().await }
    /// ```
    ///
    /// ## Errors
    ///
    /// Note, that [`QueryResult::stream`] may error if:
    ///
    /// - current result set contains an error,
    /// - previously unconsumed result set stream contained an error.
    ///
    /// ```rust
    /// # use mysql_async::test_misc::get_opts;
    /// # #[tokio::main]
    /// # async fn main() -> mysql_async::Result<()> {
    /// # use mysql_async::*;
    /// # use mysql_async::prelude::*;
    /// # use futures_util::StreamExt;
    /// let mut conn = Conn::new(get_opts()).await?;
    ///
    /// // The second result set of this query will contain an error.
    /// let mut result = conn.query_iter("SELECT 1; SELECT FOO(); SELECT 2;").await?;
    ///
    /// // First result set stream is dropped here without being consumed,
    /// let _ = result.stream::<Row>().await?;
    /// // so it will be implicitly consumed on the second call to `QueryResult::stream`
    /// // that will error complaining about unknown FOO
    /// assert!(result.stream::<Row>().await.unwrap_err().to_string().contains("FOO"));
    ///
    /// # drop(result); conn.disconnect().await }
    /// ```
    pub fn stream<'r, T: Unpin + FromRow + Send + 'static>(
        &'r mut self,
    ) -> BoxFuture<'r, crate::Result<Option<ResultSetStream<'r, 'a, 't, T, P>>>> {
        async move {
            Ok(self
                .setup_stream()
                .await?
                .map(
                    move |(ok_packet, columns)| ResultSetStream::<'r, 'a, 't, T, P> {
                        ok_packet,
                        columns,
                        query_result: Some(ResultSetStreamState::Idle(CowMut::Borrowed(self))),
                        done: false,
                        __from_row_type: PhantomData,
                    },
                ))
        }
        .boxed()
    }

    /// Owned version of the [`QueryResult::stream`].
    ///
    /// Returned stream will stop iteration on the first result set boundary.
    ///
    /// See also [`Query::stream`][query_stream], [`Queryable::query_stream`][queryable_query_stream],
    /// [`Queryable::exec_stream`][queryable_exec_stream].
    ///
    /// The following example uses the [`Query::stream`][query_stream] function
    /// that is based on the [`QueryResult::stream_and_drop`]:
    ///
    /// ```rust
    /// # use mysql_async::test_misc::get_opts;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use mysql_async::*;
    /// # use mysql_async::prelude::*;
    /// # use futures_util::TryStreamExt;
    /// let pool = Pool::new(get_opts());
    /// let mut conn = pool.get_conn().await?;
    ///
    /// // This example uses the `Query::stream` function that is based on `QueryResult::stream_and_drop`:
    /// let mut stream = "SELECT 1 UNION ALL SELECT 2".stream::<u8, _>(&mut conn).await?;
    /// let rows = stream.try_collect::<Vec<_>>().await.unwrap();
    /// assert_eq!(vec![1, 2], rows);
    ///
    /// // Only the first result set will go into the stream:
    /// let mut stream = r"
    ///     SELECT 'foo' UNION ALL SELECT 'bar';
    ///     SELECT 'baz' UNION ALL SELECT 'quux';".stream::<String, _>(&mut conn).await?;
    /// let rows = stream.try_collect::<Vec<_>>().await.unwrap();
    /// assert_eq!(vec!["foo".to_owned(), "bar".to_owned()], rows);
    ///
    /// // We can also build a `'static` stream by giving away the connection:
    /// let stream = "SELECT 2 UNION ALL SELECT 3".stream::<u8, _>(conn).await?;
    /// // `tokio::spawn` requires `'static`
    /// let handle = tokio::spawn(async move {
    ///     stream.try_collect::<Vec<_>>().await.unwrap()
    /// });
    /// assert_eq!(vec![2, 3], handle.await?);
    ///
    /// # Ok(()) }
    /// ```
    ///
    /// [queryable_query_stream]: crate::prelude::Queryable::query_stream
    /// [queryable_exec_stream]: crate::prelude::Queryable::exec_stream
    /// [query_stream]: crate::prelude::Query::stream
    pub fn stream_and_drop<T: Unpin + FromRow + Send + 'static>(
        mut self,
    ) -> BoxFuture<'a, crate::Result<Option<ResultSetStream<'a, 'a, 't, T, P>>>> {
        async move {
            Ok(self
                .setup_stream()
                .await?
                .map(|(ok_packet, columns)| ResultSetStream::<'a, 'a, 't, T, P> {
                    ok_packet,
                    columns,
                    query_result: Some(ResultSetStreamState::Idle(CowMut::Owned(self))),
                    done: false,
                    __from_row_type: PhantomData,
                }))
        }
        .boxed()
    }
}
