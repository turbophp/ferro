//! A MINIMAL standalone Ferro wire client for the `ferro-e2e` demo — the Rust cousin of what the
//! PHP client (S7) will do in its own language.
//!
//! It speaks the real protocol over a `tokio::net::UnixStream` framed by
//! **`ferrod::session::codec::{FrameCodec, InFrame, OutFrame}`** through `tokio_util::codec::Framed`
//! (mirroring `ferrod`'s own `tests/common/mod.rs`). There is NO framing codec in `ferro-proto`:
//! that crate exposes only `header::{Header, HEADER_LEN}` + per-message `encode`/`decode`; the
//! length-framing itself lives in `ferrod::session::codec`, so we reuse it here rather than
//! re-implement the wire.
//!
//! Dev/demo tool, not shipped runtime.

use ferro_proto::consts::{TYPE_REGISTRY_HASH, flags, method_core, method_sql, service};
use ferro_proto::header::Header;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::{Hello, HelloAck, Outcome};
use ferro_proto::value::Value;
use ferrod::session::codec::{FrameCodec, InFrame, OutFrame};
use futures::{SinkExt, StreamExt};
use std::path::Path;
use tokio::net::UnixStream;
use tokio_util::codec::Framed;

/// Boxed error so the demo can `?` over both `FrameError` (framing/IO) and `CodecError` (decode)
/// without pulling in an error crate.
pub type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// The HELLO/HELLO_ACK result the demo prints: the daemon's `boot_epoch` and advertised pools.
pub struct Handshake {
    pub boot_epoch: u64,
    pub pools: Vec<String>,
}

/// A framed connection to a `ferrod` session server over a UDS socket.
pub struct DemoClient {
    framed: Framed<UnixStream, FrameCodec>,
}

impl DemoClient {
    /// Connect to the `ferrod` UDS socket at `path` and wrap it in the shared `FrameCodec`.
    pub async fn connect(path: &Path) -> Result<Self, BoxErr> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self {
            framed: Framed::new(stream, FrameCodec),
        })
    }

    async fn send(&mut self, frame: OutFrame) -> Result<(), BoxErr> {
        self.framed.send(frame).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<InFrame, BoxErr> {
        match self.framed.next().await {
            Some(frame) => Ok(frame?),
            None => Err("connection closed before a frame arrived".into()),
        }
    }

    fn frame(service: u16, method: u16, request_id: u32, payload: Vec<u8>) -> OutFrame {
        OutFrame {
            header: Header {
                flags: 0,
                service,
                method,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.into(),
        }
    }

    /// HELLO handshake: send `core/HELLO` with `features = 0` and the daemon's own
    /// `TYPE_REGISTRY_HASH` (so the daemon's hard type-registry check passes), read the
    /// `HELLO_ACK`, and return its `boot_epoch` + advertised pools.
    pub async fn hello(&mut self, request_id: u32) -> Result<Handshake, BoxErr> {
        let hello = Hello {
            client_version: 1,
            type_registry_hash: TYPE_REGISTRY_HASH.to_string(),
            manifest_hash: None,
            pid: std::process::id(),
            features: 0,
        };
        self.send(Self::frame(
            service::CORE,
            method_core::HELLO,
            request_id,
            hello.encode(),
        ))
        .await?;

        let frame = self.recv().await?;
        if frame.header.service != service::CORE || frame.header.method != method_core::HELLO_ACK {
            return Err(format!(
                "expected HELLO_ACK, got service={} method={}",
                frame.header.service, frame.header.method
            )
            .into());
        }
        let ack = HelloAck::decode(&frame.payload)?;
        Ok(Handshake {
            boot_epoch: ack.boot_epoch,
            pools: ack.pools,
        })
    }

    /// Send one `SQL/EXEC` against the "default" pool and read back its single terminal `Outcome`.
    /// `fetch`: 0 = rows, 1 = none (affected only). Awaits the terminal before returning.
    pub async fn exec(
        &mut self,
        request_id: u32,
        sql: &str,
        params: Vec<Value>,
        fetch: u8,
        readonly: bool,
    ) -> Result<Outcome, BoxErr> {
        self.send_exec(request_id, sql, params, fetch, readonly)
            .await?;
        let (_rid, outcome) = self.recv_terminal().await?;
        Ok(outcome)
    }

    /// Send an `EXEC` WITHOUT awaiting its terminal — for the multiplexing demo (fire several, then
    /// collect). Defaults to a read-only, fetch=rows request.
    pub async fn send_exec(
        &mut self,
        request_id: u32,
        sql: &str,
        params: Vec<Value>,
        fetch: u8,
        readonly: bool,
    ) -> Result<(), BoxErr> {
        let req = ExecRequest {
            pool: "default".to_string(),
            sql: Some(sql.to_string()),
            query_id: None,
            params,
            timeout_ms: None,
            readonly,
            fetch,
        };
        self.send(Self::frame(
            service::SQL,
            method_sql::EXEC,
            request_id,
            req.encode(),
        ))
        .await
    }

    /// Read the next terminal frame and return `(request_id, Outcome)`. Asserts the exactly-one-END
    /// invariant (`flags::END` set on the terminal).
    pub async fn recv_terminal(&mut self) -> Result<(u32, Outcome), BoxErr> {
        let terminal = self.recv().await?;
        if terminal.header.flags & flags::END != flags::END {
            return Err(format!(
                "terminal for request {} did not carry flags::END",
                terminal.header.request_id
            )
            .into());
        }
        Ok((
            terminal.header.request_id,
            Outcome::decode(&terminal.payload)?,
        ))
    }
}
