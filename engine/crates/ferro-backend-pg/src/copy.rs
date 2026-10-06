//! PostgreSQL's COPY sub-protocol behind `ferro-pool`'s `BackendCopyIn`/`BackendCopyOut` (M3-D4,
//! SPEC §6.1), over the vendored `tokio-postgres`'s `copy_in`/`copy_out`.
//!
//! The statement is passed to the server unmodified — no placeholder normalisation (COPY takes no
//! parameters, and a `?` inside a COPY option is the user's). Both halves rely on the M3-D4 fork
//! changes (`/UPSTREAM_PR.md`): `finish` and the out-stream read through `ReadyForQuery`, so a
//! failure of the implicit COMMIT is reported rather than lost and the transaction-status byte the
//! pin engine reads next is fresh; `send` reports a server error as soon as it arrives; and `abort`
//! waits for the server's acknowledgement.

use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use ferro_pool::backend::{BackendCopyIn, BackendCopyOut};
use ferro_pool::error::PoolError;
use futures_util::{SinkExt, StreamExt};
use tokio_postgres::{Client, CopyInSink, CopyOutStream};

use crate::error_map;

pub struct PgCopyIn {
    sink: Pin<Box<CopyInSink<Bytes>>>,
}

pub struct PgCopyOut {
    stream: Pin<Box<CopyOutStream>>,
    done: bool,
}

pub async fn copy_in(client: &Client, sql: &str) -> Result<PgCopyIn, PoolError> {
    let sink = client
        .copy_in::<str, Bytes>(sql)
        .await
        .map_err(|e| error_map::map(&e))?;
    Ok(PgCopyIn {
        sink: Box::pin(sink),
    })
}

pub async fn copy_out(client: &Client, sql: &str) -> Result<PgCopyOut, PoolError> {
    let stream = client
        .copy_out::<str>(sql)
        .await
        .map_err(|e| error_map::map(&e))?;
    Ok(PgCopyOut {
        stream: Box::pin(stream),
        done: false,
    })
}

#[async_trait]
impl BackendCopyIn for PgCopyIn {
    async fn send(&mut self, chunk: Bytes) -> Result<(), PoolError> {
        self.sink
            .send(chunk)
            .await
            .map_err(|e| error_map::map(&e))
    }

    async fn finish(&mut self) -> Result<u64, PoolError> {
        self.sink
            .as_mut()
            .finish()
            .await
            .map_err(|e| error_map::map(&e))
    }

    async fn abort(&mut self) -> Result<(), PoolError> {
        self.sink
            .as_mut()
            .abort()
            .await
            .map_err(|e| error_map::map(&e))
    }
}

#[async_trait]
impl BackendCopyOut for PgCopyOut {
    async fn next(&mut self) -> Option<Result<Bytes, PoolError>> {
        if self.done {
            return None;
        }
        match self.stream.next().await {
            Some(Ok(b)) => Some(Ok(b)),
            Some(Err(e)) => {
                self.done = true;
                Some(Err(error_map::map(&e)))
            }
            None => {
                self.done = true;
                None
            }
        }
    }

    fn rows_affected(&self) -> u64 {
        self.stream.rows_affected().unwrap_or(0)
    }
}
