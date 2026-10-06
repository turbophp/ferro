//! Service `HTTP` (= 6), method `REQUEST`: Ferro HTTP's request handler (SPEC §23.6, M6-F4a).
//!
//! The engine itself — validation, DNS, the address guard, the keep-alive pool, the write tracker,
//! the fate table — lives in `ferro-http` (`ferro_http::engine`), behind this crate's `http` feature
//! (D20, §23.13). What lives HERE is only the seam between that engine and the chassis:
//!
//! - [`ResponderSink`] sends the engine's `HEAD` and `BODY` frames through the request's
//!   `Responder`, i.e. through the same credit debit, session cap and single ordered control channel
//!   every streamed SQL frame takes (`Responder::send_stream_frame`, which takes the service id —
//!   §23.5's one chassis change). `HEAD` is not `STREAM`-flagged and still debits credit; `BODY` is.
//! - the engine's [`Terminal`] is declared on the `Responder` exactly once, so exactly one `END`
//!   follows every `REQUEST` (charter rule 4) — including a sink failure, a cancel or a deadline,
//!   which the engine classifies (§23.7.1) and returns like any other terminal.
//!
//! Nothing here interprets HTTP. The session and dispatch layers carry only the route.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use ferro_http::engine::{HttpEngine, ResponseSink, SinkError, SinkFrame, Terminal};
use ferro_proto::consts::{flags, method_http, service};
use ferro_proto::messages::HttpRequest;
use tokio_util::sync::CancellationToken;

use crate::session::flow::WaitAborted;
use crate::session::responder::{Responder, StreamSendError};

/// The engine's frames, sent through the request's `Responder`.
pub struct ResponderSink<'r> {
    responder: &'r Responder,
    cancel: CancellationToken,
}

impl ResponseSink for ResponderSink<'_> {
    fn send<'a>(
        &'a self,
        frame: SinkFrame,
        payload: Vec<u8>,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        let (method, frame_flags) = match frame {
            SinkFrame::Head => (method_http::HEAD, 0),
            SinkFrame::Body => (method_http::BODY, flags::STREAM),
        };
        Box::pin(async move {
            self.responder
                .send_stream_frame(
                    service::HTTP,
                    method,
                    frame_flags,
                    payload,
                    &self.cancel,
                    Some(deadline),
                )
                .await
                .map(|_| ())
                .map_err(|e| match e {
                    StreamSendError::Aborted(WaitAborted::Cancelled) => SinkError::Cancelled,
                    StreamSendError::Aborted(WaitAborted::Deadline) => SinkError::Deadline,
                    StreamSendError::LinkLost => SinkError::LinkLost,
                    StreamSendError::Oversized => SinkError::Oversized,
                })
        })
    }
}

/// Serve one decoded `REQUEST` and declare its ONE terminal. `started` is when its frame was decoded
/// (the total deadline runs from there, §23.6 step 4).
pub async fn handle_request(
    engine: &Arc<HttpEngine>,
    req: HttpRequest,
    peer_uid: Option<u32>,
    started: Instant,
    responder: Responder,
    cancel: CancellationToken,
) {
    // §23.5.1 field 13: `ExecRequest` field 9's rule — interpreted once, a malformed value dropped
    // and counted (`ferro_traceparent_invalid_total`), never refused. It is not forwarded upstream,
    // and nothing else consumes it until slice F7's span.
    let _trace = crate::trace::from_request(req.traceparent.as_deref());
    let terminal = {
        let sink = ResponderSink {
            responder: &responder,
            cancel: cancel.clone(),
        };
        engine
            .exchange(&req, peer_uid, started, &cancel, &sink)
            .await
    };
    match terminal {
        Terminal::Done(done) => responder.end_ok(Bytes::from(done.encode())),
        Terminal::Error(ep) => responder.end_error(ep),
        Terminal::Cancelled => responder.end_cancelled(),
    }
}
