//! Consuming-typestate `Responder`: a handler DECLARES its terminal outcome by consuming `self`
//! in exactly one of `end_ok`/`end_error`/`end_cancelled` — since each takes `self` by value, a
//! second call is a compile error, not a runtime assertion ("at most one declaration" is enforced
//! by the type system, not by a check).
//!
//! Declaring does NOT send anything to the wire: it only stores the outcome into a `cell` that
//! the supervisor holds a clone of (see `session::supervisor`), which reads it back — exactly
//! once — after the handler's spawned task joins. The supervisor, not the handler, is the sole
//! terminal-sender (SPEC's "Terminal-delivery refinement (v2.1)"). A handler that returns (or
//! panics) without calling any `end_*` leaves the cell `None`; the supervisor treats that
//! identically to a panic — a synthesized, distinctly-marked error — so exactly-one-`END` holds
//! even when a handler has a bug.
//!
//! **Streaming sink (M1-S5 Task 4a — additive).** ON TOP of the one-shot terminal role, a
//! `Responder` is also the sink a `fetch:stream` handler pushes result frames through, BEFORE it
//! declares its terminal: `send_head` (once, the column metadata) then any number of `send_data`
//! (row batches). These are NON-consuming (`&self`) — the producer sends many, then exactly one
//! `end_*` — and each one runs the full flow-control gauntlet: it debits the request's per-request
//! credit window (B3), reserves against the per-session byte cap (M6), and only then enqueues the
//! frame (carrying the cap guard) on the SAME ordered control channel the terminal uses (B4 — the
//! terminal, sent later by the supervisor, can never overtake a DATA frame). `HandlerFn` is
//! unchanged: the streaming resources are threaded in at construction (`new_streaming`), and a
//! non-streamed handler simply never calls `send_head`/`send_data` (so `new_pair`, which builds a
//! `Responder` with inert sink resources, keeps every existing terminal-only call site working).

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use ferro_proto::consts::{MAX_FRAME_PAYLOAD, flags, method_stream, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{ColMeta, ErrorPayload, StreamData, StreamHead};
use ferro_proto::value::Value;

use super::codec::{ControlMsg, OutFrame};
use super::flow::{CreditCell, SessionCap, WaitAborted};

/// A handler's declared outcome, as read back by the supervisor. Distinct from
/// `ferro_proto::messages::Outcome` because `Ok` here still holds the handler's owned `Bytes` —
/// the supervisor converts it to the wire `Outcome::Ok(Vec<u8>)` at send time.
#[derive(Debug, Clone)]
pub enum Terminal {
    Ok(Bytes),
    Error(ErrorPayload),
    Cancelled,
}

/// Why a streamed `send_head`/`send_data` did NOT enqueue its frame. The producer (Task 4b) turns
/// any of these into the request's ONE terminal — it never keeps streaming past a failed send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSendError {
    /// The flow-control wait (credit window or session cap) unwound because the request was
    /// cancelled or its deadline passed — nothing was reserved net (B3/M6 bail clean).
    Aborted(WaitAborted),
    /// The encoded frame payload exceeds `MAX_FRAME_PAYLOAD` — a single row too large for one frame
    /// (the §5.2/large-row ceiling). Nothing was debited or reserved.
    Oversized,
    /// The control channel is closed: the writer task is gone (peer disconnected / session tearing
    /// down). Any cap reservation made for this frame is released with the dropped message.
    LinkLost,
}

/// The streaming sink resources a `Responder` carries so a `fetch:stream` handler can emit
/// HEAD/DATA frames. Bundled so `new_pair` (inert) and `new_streaming` (real) differ only in what
/// they put here. A non-streamed handler never touches any of it.
struct StreamSink {
    /// The wire `request_id` stamped on every HEAD/DATA frame (correlates them with the eventual
    /// terminal, which the supervisor sends on the same id).
    request_id: u32,
    /// This request's per-request credit window (B3 backpressure): every frame debits it.
    credit: Arc<CreditCell>,
    /// The per-session aggregate byte cap (M6): every frame reserves against it, releasing on the
    /// message drop after the write.
    session_cap: Arc<SessionCap>,
    /// A clone of the connection's ONE ordered control channel — DATA frames and the terminal share
    /// it, which is what keeps the terminal from overtaking a DATA frame (B4).
    control_tx: mpsc::Sender<ControlMsg>,
}

/// A handler's one-shot terminal declaration AND (M1-S5) its streaming sink.
pub struct Responder {
    cell: Arc<Mutex<Option<Terminal>>>,
    sink: StreamSink,
}

impl Responder {
    /// Construct a linked `(Responder, cell)` pair with INERT sink resources — for terminal-only
    /// handlers and for tests that never stream. The caller hands the `Responder` to the handler
    /// task and keeps `cell` (cloning the `Arc` first if it also hands a copy to a supervisor task)
    /// to read back whatever the handler declares. The sink is real-typed but wired to a
    /// permissive credit window + cap and a CLOSED control channel, so a stray `send_head`/
    /// `send_data` fails fast with `LinkLost` (it never parks on flow control and never silently
    /// succeeds) — a non-streamed handler simply never calls them.
    pub fn new_pair() -> (Responder, Arc<Mutex<Option<Terminal>>>) {
        // Drop the receiver at once so the channel is closed: flow control passes (permissive
        // window/cap), then the send hits the closed channel → `LinkLost`, never a hang.
        let (control_tx, rx) = mpsc::channel::<ControlMsg>(1);
        drop(rx);
        let sink = StreamSink {
            request_id: 0,
            credit: Arc::new(CreditCell::new(super::flow::Credit::new(
                u32::MAX,
                u32::MAX,
            ))),
            session_cap: Arc::new(SessionCap::new(u64::MAX)),
            control_tx,
        };
        Self::from_parts(sink)
    }

    /// Construct a linked `(Responder, cell)` pair with REAL streaming sink resources: this
    /// request's `request_id`, its credit window (`credit`), the shared per-session cap
    /// (`session_cap`), and a clone of the connection's ordered control channel (`control_tx`). A
    /// `fetch:stream` handler pushes HEAD/DATA through these; every handler still declares exactly
    /// one terminal via `end_*` regardless.
    pub fn new_streaming(
        request_id: u32,
        credit: Arc<CreditCell>,
        session_cap: Arc<SessionCap>,
        control_tx: mpsc::Sender<ControlMsg>,
    ) -> (Responder, Arc<Mutex<Option<Terminal>>>) {
        Self::from_parts(StreamSink {
            request_id,
            credit,
            session_cap,
            control_tx,
        })
    }

    fn from_parts(sink: StreamSink) -> (Responder, Arc<Mutex<Option<Terminal>>>) {
        let cell = Arc::new(Mutex::new(None));
        (
            Responder {
                cell: cell.clone(),
                sink,
            },
            cell,
        )
    }

    /// Declare success with `body` — the method-specific opaque result bytes (must already be a
    /// single complete MessagePack value, or empty; see `Outcome::encode`'s contract).
    pub fn end_ok(self, body: Bytes) {
        *self.cell.lock().unwrap() = Some(Terminal::Ok(body));
    }

    /// Declare failure.
    pub fn end_error(self, ep: ErrorPayload) {
        *self.cell.lock().unwrap() = Some(Terminal::Error(ep));
    }

    /// Declare cancellation. SPEC: flag-based `CANCEL` is advisory — the handler observes the
    /// cancel flag and calls this itself (or races it against its own natural completion); the
    /// supervisor never synthesizes `Cancelled` on its own (its synthetic path is reserved for
    /// the panic/no-terminal bug case and always uses `Terminal::Error`).
    pub fn end_cancelled(self) {
        *self.cell.lock().unwrap() = Some(Terminal::Cancelled);
    }

    /// Stream the result's column metadata as one `STREAM/HEAD` frame — sent once, before any
    /// `DATA`. Runs the FULL flow-control gauntlet exactly like `send_data` (MINOR-12 — HEAD is not
    /// special-cased: it debits credit and reserves cap too), so a client's credit window governs
    /// HEAD delivery as uniformly as it governs rows.
    pub async fn send_head(
        &self,
        cols: &[ColMeta],
        cancel: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), StreamSendError> {
        let payload = StreamHead {
            cols: cols.to_vec(),
        }
        .encode();
        self.send_stream_frame(method_stream::HEAD, 0, payload, cancel, deadline)
            .await
    }

    /// Stream a batch of result rows as one `STREAM/DATA` frame (flag `STREAM` set). Debits the
    /// per-request credit window and reserves against the per-session cap before enqueueing.
    pub async fn send_data(
        &self,
        rows: Vec<Vec<Value>>,
        cancel: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), StreamSendError> {
        let payload = StreamData { rows }.encode();
        self.send_stream_frame(
            method_stream::DATA,
            flags::STREAM,
            payload,
            cancel,
            deadline,
        )
        .await
    }

    /// The shared sink core for `send_head`/`send_data`. Ordering is load-bearing:
    /// 1. oversized check (nothing reserved yet — a single row too big for one frame);
    /// 2. debit the per-request credit window (B3 — bails clean on cancel/deadline, reserves
    ///    nothing);
    /// 3. reserve against the per-session cap (M6 — the `CapReserve` guard);
    /// 4. build the frame and enqueue it with the guard riding IN the `ControlMsg`, so a cancelled
    ///    or link-lost enqueue drops the message and releases the reservation (no leak, exactly one
    ///    release, on the writer's post-write drop).
    async fn send_stream_frame(
        &self,
        method: u16,
        frame_flags: u16,
        payload: Vec<u8>,
        cancel: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), StreamSendError> {
        let len = payload.len();

        // 1. Oversized: a single frame's payload cannot exceed the codec ceiling. Mirrors the
        // buffered `build_terminal_body` cap; only trips for a genuinely huge single row.
        if len > MAX_FRAME_PAYLOAD as usize {
            return Err(StreamSendError::Oversized);
        }

        // 2. Credit (B3). Debits nothing on abort.
        self.sink
            .credit
            .debit_or_wait(len as u32, cancel, deadline)
            .await
            .map_err(StreamSendError::Aborted)?;

        // 3. Session cap (M6). The returned guard releases the reservation exactly once, on drop.
        let cap = self
            .sink
            .session_cap
            .reserve_or_wait(len as u64, cancel, deadline)
            .await
            .map_err(StreamSendError::Aborted)?;

        // 4. Enqueue. The guard travels IN the message: if this send is cancelled or the channel is
        // closed, the dropped `ControlMsg` releases the reservation — no pre-reserved DATA permit
        // and no leak. The writer drops the message (releasing the cap) only after the write flushes.
        let frame = OutFrame {
            header: Header {
                flags: frame_flags,
                service: service::STREAM,
                method,
                request_id: self.sink.request_id,
                payload_len: len as u32,
            },
            payload: Bytes::from(payload),
        };
        let msg = ControlMsg {
            frame,
            cap: Some(cap),
        };
        // The FINAL channel send is cancel/deadline-aware too (carried Task-4a review fix). Steps 2-3
        // already returned, so without this a slow-but-connected client filling the channel's slack
        // could park the producer HERE past its cancel/deadline. Race the send against the same
        // `cancel`/`deadline`; on an abort arm the `send(msg)` future is dropped, dropping `msg` —
        // which releases the `CapReserve` guard it carries (no leak, exactly one release) — and the
        // frame is never enqueued. `biased` so cancel/deadline win a tie over a just-freed slot.
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(StreamSendError::Aborted(WaitAborted::Cancelled)),
            () = sleep_until_opt(deadline) => Err(StreamSendError::Aborted(WaitAborted::Deadline)),
            res = self.sink.control_tx.send(msg) => res.map_err(|_| StreamSendError::LinkLost),
        }
    }
}

/// Await `deadline` if there is one, else never resolve — so the final-send race in
/// [`Responder::send_stream_frame`] has a deadline arm even when the request set no `timeout_ms`.
/// Mirrors `flow`'s identical private helper (kept local rather than reaching into another module's
/// private item).
async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use ferro_proto::messages::Outcome;
    use tokio::time::timeout;

    use super::super::flow::Credit;
    use crate::session::registry::Registry;
    use crate::session::supervisor;

    const REQ_ID: u32 = 42;
    const SOME_METHOD: u16 = 1;

    fn small_rows() -> Vec<Vec<Value>> {
        vec![
            vec![Value::I64(1), Value::Text("alpha".into())],
            vec![Value::I64(2), Value::Text("beta".into())],
        ]
    }

    fn cell(frames: u32, bytes: u32) -> Arc<CreditCell> {
        Arc::new(CreditCell::new(Credit::new(frames, bytes)))
    }

    // --- 1. send_data enqueues a cap-carrying ControlMsg; the writer's drop releases the cap ---
    #[tokio::test]
    async fn send_data_enqueues_and_cap_releases_on_writer_drop() {
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        let (tx, mut rx) = mpsc::channel::<ControlMsg>(4);
        let (responder, _cell) =
            Responder::new_streaming(REQ_ID, cell(10, 10_000_000), Arc::clone(&session_cap), tx);
        let cancel = CancellationToken::new();

        let rows = small_rows();
        let expected_len = StreamData { rows: rows.clone() }.encode().len() as u64;

        responder
            .send_data(rows, &cancel, None)
            .await
            .expect("send_data must enqueue");

        // Reserved but not yet released — the guard rides inside the still-buffered ControlMsg.
        assert_eq!(
            session_cap.used(),
            expected_len,
            "the frame's bytes stay reserved while it is buffered"
        );

        // The stand-in "writer" takes the message off the channel...
        let msg = rx.try_recv().expect("exactly one DATA frame enqueued");
        assert_eq!(msg.frame.header.service, service::STREAM);
        assert_eq!(msg.frame.header.method, method_stream::DATA);
        assert_eq!(msg.frame.header.flags, flags::STREAM);
        assert_eq!(msg.frame.header.request_id, REQ_ID);
        assert!(msg.cap.is_some(), "a DATA frame carries its cap guard");
        assert_eq!(
            session_cap.used(),
            expected_len,
            "still reserved: the writer holds the message (thus the guard)"
        );

        // ...and drops it after the write → the cap returns to baseline (M6 release-on-drop).
        drop(msg);
        assert_eq!(
            session_cap.used(),
            0,
            "dropping the ControlMsg must release the cap reservation exactly once"
        );

        assert!(rx.try_recv().is_err(), "exactly one frame was enqueued");
    }

    // --- 2. an oversized single row is the Oversized error, reserving/debiting nothing ---
    #[tokio::test]
    async fn send_data_oversized_row_is_oversized_error() {
        let session_cap = Arc::new(SessionCap::new(u64::MAX));
        let credit = cell(10, u32::MAX);
        let (tx, mut rx) = mpsc::channel::<ControlMsg>(4);
        let (responder, _cell) =
            Responder::new_streaming(REQ_ID, Arc::clone(&credit), Arc::clone(&session_cap), tx);
        let cancel = CancellationToken::new();

        // One row whose single BYTES cell alone exceeds the per-frame ceiling.
        let big = vec![vec![Value::Bytes(vec![
            0u8;
            MAX_FRAME_PAYLOAD as usize + 16
        ])]];

        let err = timeout(
            Duration::from_secs(5),
            responder.send_data(big, &cancel, None),
        )
        .await
        .expect("oversized must return promptly, never hang")
        .expect_err("an oversized frame is rejected");
        assert_eq!(err, StreamSendError::Oversized);

        // The oversized check runs BEFORE any debit/reserve, so both are untouched.
        assert_eq!(session_cap.used(), 0, "oversized reserves nothing");
        assert_eq!(
            credit.snapshot(),
            Credit::new(10, u32::MAX),
            "oversized debits nothing"
        );
        assert!(rx.try_recv().is_err(), "oversized enqueues nothing");
    }

    // --- 3. MINOR-12: send_head debits credit exactly like send_data ---
    #[tokio::test]
    async fn send_head_debits_credit_like_data_minor_12() {
        let session_cap = Arc::new(SessionCap::new(64 * 1024 * 1024));
        // Exactly ONE frame of credit: send_head must consume it, leaving send_data to block.
        let credit = cell(1, 10_000_000);
        let (tx, _rx) = mpsc::channel::<ControlMsg>(8);
        let (responder, _cell) =
            Responder::new_streaming(REQ_ID, Arc::clone(&credit), Arc::clone(&session_cap), tx);
        let responder = Arc::new(responder);
        let cancel = CancellationToken::new();

        let cols = vec![ColMeta {
            name: "id".into(),
            tag: 2,
        }];

        // HEAD consumes the one frame of credit (MINOR-12: it is NOT special-cased).
        responder
            .send_head(&cols, &cancel, None)
            .await
            .expect("send_head must succeed on a 1-frame window");
        assert_eq!(
            credit.snapshot().frames(),
            0,
            "send_head must debit a frame exactly like a DATA frame"
        );

        // A following send_data now has no frames → it must PARK on debit_or_wait.
        let waiter = {
            let responder = Arc::clone(&responder);
            let cancel = cancel.clone();
            tokio::spawn(async move { responder.send_data(small_rows(), &cancel, None).await })
        };
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiter.is_finished(),
            "send_data must block once send_head has exhausted the credit window"
        );

        // Replenishing the window (as a routed WINDOW_UPDATE would) unblocks it.
        credit.replenish(1, 1_000_000);
        let res = timeout(Duration::from_secs(5), waiter)
            .await
            .expect("replenish must unblock send_data, never hang")
            .expect("task join");
        assert_eq!(res, Ok(()), "send_data proceeds once credit is replenished");
    }

    // --- 4. a pre-cancelled request aborts a credit-starved send_data, reserving nothing ---
    #[tokio::test]
    async fn send_data_precancelled_aborts_and_reserves_nothing() {
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // No credit: send_data would park — but the pre-cancelled token unwinds it at once.
        let (tx, _rx) = mpsc::channel::<ControlMsg>(4);
        let (responder, _cell) =
            Responder::new_streaming(REQ_ID, cell(0, 0), Arc::clone(&session_cap), tx);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = timeout(
            Duration::from_secs(5),
            responder.send_data(small_rows(), &cancel, None),
        )
        .await
        .expect("a cancelled send must return promptly, never hang")
        .expect_err("a cancelled send does not enqueue");
        assert_eq!(err, StreamSendError::Aborted(WaitAborted::Cancelled));
        assert_eq!(
            session_cap.used(),
            0,
            "an aborted send reserves nothing net"
        );
    }

    // --- carried Task-4a review fix: the FINAL channel send is cancel/deadline-aware too ---
    // A `send_data` parked on a FULL control channel (debit + reserve already passed) with a fired
    // `cancel` must return `Aborted` PROMPTLY (never park past the request's cancel/deadline while a
    // slow-but-connected client fills the channel slack), and the dropped guard-carrying `ControlMsg`
    // must release the cap reservation — no leak.
    #[tokio::test]
    async fn send_data_final_channel_send_aborts_on_cancel_and_releases_cap() {
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        // Capacity 1, pre-filled: the producer's own send then blocks on a FULL channel.
        let (tx, mut rx) = mpsc::channel::<ControlMsg>(1);
        tx.send(ControlMsg::bare(dummy_stream_frame()))
            .await
            .expect("prefill the one channel slot");
        // Generous credit + cap: the block MUST be on the channel send, not on debit/reserve.
        let credit = cell(10, u32::MAX);
        let (responder, _cell) =
            Responder::new_streaming(REQ_ID, Arc::clone(&credit), Arc::clone(&session_cap), tx);
        let responder = Arc::new(responder);
        let cancel = CancellationToken::new();

        let rows = small_rows();
        let expected_len = StreamData { rows: rows.clone() }.encode().len() as u64;

        let waiter = {
            let responder = Arc::clone(&responder);
            let cancel = cancel.clone();
            tokio::spawn(async move { responder.send_data(rows, &cancel, None).await })
        };
        // Let it debit+reserve and park on the full channel's send.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiter.is_finished(),
            "send_data must park on the full channel (debit + reserve already passed)"
        );
        assert_eq!(
            session_cap.used(),
            expected_len,
            "the frame's cap is reserved while it is parked on the channel send"
        );

        // Fire cancel -> the final-send race unwinds promptly; the dropped ControlMsg releases the cap.
        cancel.cancel();
        let res = timeout(Duration::from_secs(5), waiter)
            .await
            .expect("cancel must unwind a full-channel send promptly, never hang")
            .expect("task join");
        assert_eq!(res, Err(StreamSendError::Aborted(WaitAborted::Cancelled)));
        assert_eq!(
            session_cap.used(),
            0,
            "the aborted send's cap reservation is released on the dropped ControlMsg (no leak)"
        );

        // Only the prefilled bare message was ever enqueued — the cancelled frame never went in.
        let first = rx.recv().await.expect("the prefilled message");
        assert!(first.cap.is_none(), "the prefilled message carries no cap");
        assert!(
            rx.try_recv().is_err(),
            "the cancelled frame was never enqueued"
        );
    }

    fn dummy_stream_frame() -> OutFrame {
        OutFrame {
            header: Header {
                flags: 0,
                service: service::STREAM,
                method: method_stream::DATA,
                request_id: REQ_ID,
                payload_len: 0,
            },
            payload: Bytes::new(),
        }
    }

    // --- a stray stream send on an inert `new_pair` Responder fails fast, never hangs ---
    #[tokio::test]
    async fn new_pair_stream_send_fails_fast_with_link_lost() {
        let (responder, _cell) = Responder::new_pair();
        let cancel = CancellationToken::new();
        let err = timeout(
            Duration::from_secs(5),
            responder.send_data(small_rows(), &cancel, None),
        )
        .await
        .expect("an inert Responder must fail fast on the closed channel, never park")
        .expect_err("a non-streamed Responder has no live channel to send on");
        assert_eq!(err, StreamSendError::LinkLost);
    }

    // --- 5. B4: a streamed DATA frame arrives BEFORE the terminal on the one ordered channel ---
    #[tokio::test]
    async fn data_arrives_before_terminal_b4() {
        let (control_tx, mut control_rx) = mpsc::channel::<ControlMsg>(8);
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        let registry = Arc::new(Registry::new(4));
        registry
            .insert(REQ_ID, Credit::new(10, 10_000_000))
            .unwrap();

        // Reserve the terminal's slot BEFORE the handler runs — exactly as the session layer does.
        let permit = control_tx.clone().reserve_owned().await.unwrap();

        let (responder, cell) = Responder::new_streaming(
            REQ_ID,
            cell(10, 10_000_000),
            Arc::clone(&session_cap),
            control_tx.clone(),
        );

        // The "handler": emit one DATA frame DURING its run, then declare its terminal.
        let handle = tokio::spawn(async move {
            let cancel = CancellationToken::new();
            responder
                .send_data(small_rows(), &cancel, None)
                .await
                .expect("send_data");
            responder.end_ok(Bytes::new());
        });

        // The supervisor sends the terminal only AFTER handle.await — same ordered channel → FIFO.
        supervisor::supervise(
            REQ_ID,
            service::SQL,
            SOME_METHOD,
            permit,
            cell,
            handle,
            registry.clone(),
        )
        .await;

        // First off the channel: the DATA frame.
        let first = control_rx.recv().await.expect("a DATA frame");
        assert_eq!(first.frame.header.service, service::STREAM);
        assert_eq!(first.frame.header.method, method_stream::DATA);
        assert_eq!(first.frame.header.flags, flags::STREAM);
        assert!(first.cap.is_some(), "DATA carries its cap guard");

        // Then the terminal — never before the DATA (invariant B4).
        let terminal = control_rx.recv().await.expect("the terminal");
        assert_eq!(terminal.frame.header.flags, flags::END);
        assert_eq!(terminal.frame.header.service, service::SQL);
        assert!(terminal.cap.is_none(), "the terminal carries no cap guard");
        match Outcome::decode(&terminal.frame.payload).expect("decode Outcome") {
            Outcome::Ok(body) => assert!(body.is_empty()),
            other => panic!("expected Outcome::Ok, got {other:?}"),
        }

        assert_eq!(registry.len(), 0, "the supervisor removed the entry");
    }
}
