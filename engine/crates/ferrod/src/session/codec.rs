use bytes::{Buf, Bytes, BytesMut};
use ferro_proto::consts::MAX_FRAME_PAYLOAD;
use ferro_proto::header::{HEADER_LEN, Header};
use tokio_util::codec::{Decoder, Encoder};

use super::flow::CapReserve;

/// The codec's error type. tokio-util requires `Decoder::Error: From<std::io::Error>` (and same for
/// Encoder), which `ferro_proto::CodecError` does NOT satisfy — and adding a `From<io::Error>` to
/// CodecError from here is an orphan-rule violation. So we wrap both in a ferrod-local error.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("codec: {0}")]
    Codec(#[from] ferro_proto::CodecError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct InFrame {
    pub header: Header,
    pub payload: Bytes,
}
#[derive(Debug, Clone)]
pub struct OutFrame {
    pub header: Header,
    pub payload: Bytes,
}

/// The writer channel item: one `OutFrame` to serialize to the socket, plus an OPTIONAL
/// per-session cap reservation (`CapReserve`, M6) that must stay alive until AFTER the frame has
/// been written. The writer sends `frame`, then drops the `ControlMsg` — releasing `cap` only once
/// the write has flushed, so the reserved bytes bound the actually-buffered bytes rather than
/// merely the enqueued ones (releasing at enqueue would defeat that bound). `OutFrame` itself stays
/// a pure codec type (the golden-vector tests encode it directly); the cap guard rides ALONGSIDE it
/// here, never inside it.
///
/// Every non-streamed send — the reader loop's HELLO_ACK/PONG/diagnostics, the supervisor's
/// terminal — carries `cap: None`. A streamed `Responder::send_head`/`send_data` frame carries
/// `Some(guard)`; because the guard travels IN the message, a cancelled/failed enqueue drops the
/// message and releases the reservation (no leak), and there is exactly one release, on the drop.
#[derive(Debug)]
pub struct ControlMsg {
    pub frame: OutFrame,
    pub cap: Option<CapReserve>,
}

impl ControlMsg {
    /// A control/liveness/terminal frame with no cap reservation to release (the common,
    /// non-streamed case).
    pub fn bare(frame: OutFrame) -> Self {
        ControlMsg { frame, cap: None }
    }
}

/// Shared decode-progress counters (M1-S9a finding 5c). The session task cannot reach its own
/// codec once `Framed` has been `.split()` (`session/mod.rs` — the reader half owns it), so the
/// only way the session's tick arm can tell "a frame is half-here and no bytes are arriving" from
/// "the peer is simply quiet" is a handle created WITH the codec and read from outside it.
///
/// The counters describe the frame currently being reassembled:
/// - `started` — number of frames that have gone PARTIAL (a whole frame delivered in one read
///   never touches these counters at all);
/// - `completed` — how many of those have since finished, so `started > completed` means exactly
///   "a partial frame is outstanding right now";
/// - `buffered` — bytes held for that partial frame. **This is the field that makes the tick arm a
///   stall detector rather than a per-frame completion deadline**: it advances on every read that
///   delivers bytes, so a slow-but-progressing client keeps changing the snapshot and is never
///   killed. See `ReadSnapshot`.
///
/// The fields are atomics because the codec is unreachable BY NAME after the split, not because
/// there is a data race to defend against: the reader half and the tick arm are two arms of one
/// `tokio::select!` in ONE task, so the stores and the loads are ordered by that task's own program
/// order and a snapshot can never be torn. `Relaxed` is therefore the honest ordering.
#[derive(Debug, Default)]
pub struct ReadProgress {
    started: std::sync::atomic::AtomicU64,
    completed: std::sync::atomic::AtomicU64,
    buffered: std::sync::atomic::AtomicU64,
}

impl ReadProgress {
    /// Read all three counters (see the type doc for why this cannot tear).
    pub fn snapshot(&self) -> ReadSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        ReadSnapshot {
            started: self.started.load(Relaxed),
            completed: self.completed.load(Relaxed),
            buffered: self.buffered.load(Relaxed),
        }
    }
}

/// One reading of [`ReadProgress`]. Compared for EQUALITY by the session's tick arm: two equal
/// snapshots `frame_read_timeout` apart, with a partial frame outstanding in both, mean no byte of
/// that frame arrived in the whole window — a stalled partial frame. Any byte arriving changes
/// `buffered`; a frame finishing changes `completed`; the next frame going partial changes
/// `started`. So equality is exactly "nothing happened on the read side".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSnapshot {
    pub started: u64,
    pub completed: u64,
    pub buffered: u64,
}

impl ReadSnapshot {
    /// Whether a frame is half-received right now — a header seen with its body incomplete, or the
    /// header itself still incomplete. Both count: "send three bytes and stop" holds a session
    /// exactly as effectively as "send a 16 MiB header and stop".
    pub fn is_partial(self) -> bool {
        self.started > self.completed
    }
}

/// The inbound/outbound frame codec. Carries an OPTIONAL [`ReadProgress`] handle: `Default`
/// (no handle) is what every non-session construction site wants — the one-shot `deny_connection`
/// framer, the test harness client, the fuzz target — and the session builds its own with
/// [`FrameCodec::with_progress`].
#[derive(Debug, Default)]
pub struct FrameCodec {
    progress: Option<std::sync::Arc<ReadProgress>>,
    /// True while the current frame is only partially buffered. Local to the codec (the decoder is
    /// `&mut self`), and what makes `started`/`completed` count FRAMES rather than decode calls.
    mid_frame: bool,
}

impl FrameCodec {
    /// A codec that publishes its partial-frame progress to `progress`, for a caller that will
    /// `.split()` the `Framed` and therefore lose all access to the codec itself.
    pub fn with_progress(progress: std::sync::Arc<ReadProgress>) -> Self {
        FrameCodec {
            progress: Some(progress),
            mid_frame: false,
        }
    }

    /// The current frame is incomplete with `buffered` of its bytes in hand. Bumps `started` ONCE
    /// per frame (so the counter identifies WHICH frame is outstanding, not how many times the
    /// decoder was polled) and republishes `buffered` every time, which is what the stall detector
    /// keys progress off.
    fn note_partial(&mut self, buffered: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let first = !self.mid_frame;
        self.mid_frame = true;
        if let Some(p) = &self.progress {
            if first {
                p.started.fetch_add(1, Relaxed);
            }
            p.buffered.store(buffered as u64, Relaxed);
        }
    }

    /// A frame completed. Only counts if it had gone partial first — a frame delivered whole in one
    /// read was never outstanding and must not perturb the counters.
    fn note_complete(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        if !std::mem::take(&mut self.mid_frame) {
            return;
        }
        if let Some(p) = &self.progress {
            p.completed.fetch_add(1, Relaxed);
        }
    }
}

/// M1-S9a (finding 5): when a frame is incomplete, reserve at most this much ahead of the bytes
/// actually received. `payload_len` is CLIENT-DECLARED — pre-reserving it in full let one local
/// connection pin 16 MiB with a 17-byte send, held until the frame completed or the socket
/// closed (measured). 64 KiB amortizes reallocation for ordinary frames; a large frame's buffer
/// still grows to size as its bytes genuinely arrive.
///
/// This bounds the reserve AHEAD of `src.len()`, not the buffer as a whole: `BytesMut` grows
/// geometrically, so a partial frame pins roughly `received × 2 + 64 KiB`. That is the property
/// worth having — an attacker pays for what they pin — and it is what the guards assert. It is
/// deliberately NOT a total cap on the buffer, which would break reassembly of a legitimate
/// large frame.
const PARTIAL_FRAME_RESERVE_STEP: usize = 64 * 1024;

impl Decoder for FrameCodec {
    type Item = InFrame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<InFrame>, FrameError> {
        if src.is_empty() {
            // NOT a partial frame — a quiet peer. `Framed` polls the decoder once more with an
            // empty buffer after every frame it yields, so counting this as "half a frame here"
            // would leave every healthy session permanently mid-frame (measured: the idle reaper
            // was vetoed forever, and the stall detector armed on a session with nothing to stall).
            return Ok(None);
        }
        if src.len() < HEADER_LEN {
            // A partial HEADER is a partial frame too (M1-S9a): "send three bytes, then stop" holds
            // the session exactly as effectively as a declared-16-MiB header does, and costs the
            // attacker even less.
            self.note_partial(src.len());
            return Ok(None);
        }
        let header = Header::decode(&src[..HEADER_LEN])?; // CodecError -> FrameError via #[from]
        let need = HEADER_LEN + header.payload_len as usize;
        if src.len() < need {
            self.note_partial(src.len());
            src.reserve((need - src.len()).min(PARTIAL_FRAME_RESERVE_STEP));
            return Ok(None);
        }
        self.note_complete();
        src.advance(HEADER_LEN);
        let payload = src.split_to(header.payload_len as usize).freeze();
        Ok(Some(InFrame { header, payload }))
    }
}

impl Encoder<OutFrame> for FrameCodec {
    type Error = FrameError;
    fn encode(&mut self, item: OutFrame, dst: &mut BytesMut) -> Result<(), FrameError> {
        debug_assert_eq!(item.header.payload_len as usize, item.payload.len());
        if item.payload.len() > MAX_FRAME_PAYLOAD as usize {
            return Err(FrameError::Codec(ferro_proto::CodecError::FrameTooLarge {
                len: item.payload.len() as u32,
                max: MAX_FRAME_PAYLOAD,
            }));
        }
        dst.extend_from_slice(&item.header.encode());
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*; // codec.rs already imports Header + MAX_FRAME_PAYLOAD at the top

    fn header(payload_len: u32) -> Header {
        Header {
            flags: 0,
            service: 0,
            method: 0,
            request_id: 7,
            payload_len,
        }
    }

    /// M1-S9a finding 5: a header DECLARING the 16 MiB maximum plus one body byte must not make
    /// the decoder pre-reserve the whole declared payload — that is the one-local-client
    /// memory-amplification vector (N connections × 16 MiB for N × 17 bytes sent). The buffer may
    /// only grow in bounded steps as bytes actually arrive.
    #[test]
    fn a_partial_frame_reserves_in_bounded_steps_not_the_declared_payload() {
        let mut codec = FrameCodec::default();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(MAX_FRAME_PAYLOAD).encode());
        src.extend_from_slice(&[0u8]); // one body byte of 16 MiB declared

        let r = codec
            .decode(&mut src)
            .expect("partial frame is NeedMore, not an error");
        assert!(r.is_none());
        assert!(
            src.capacity() < 256 * 1024,
            "the decoder reserved {} bytes for a frame of which 1 body byte has arrived — the \
             16 MiB-per-header amplification is back",
            src.capacity()
        );
    }

    /// The attack shape the finding actually describes, which the single-shot test above cannot
    /// distinguish: a client trickles bytes at a stalled frame while the reader loop calls `decode`
    /// on EVERY wakeup. A per-call reserve that compounded (capacity climbing one step per decode
    /// while nothing arrives) would satisfy the test above and still be an amplification vector.
    /// The pinned bytes must track what was RECEIVED, not what was DECLARED.
    ///
    /// The bound is absolute rather than a multiple of `PARTIAL_FRAME_RESERVE_STEP` on purpose: a
    /// relative bound would keep passing if someone raised the step to the declared payload, which
    /// is the very defect this guards. It is loose (`BytesMut` grows geometrically, so one step
    /// ahead of a small `len` can allocate ~2 steps) but still 32x below the 16 MiB failure.
    #[test]
    fn a_trickled_partial_frame_pins_only_what_was_sent() {
        let mut codec = FrameCodec::default();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(MAX_FRAME_PAYLOAD).encode());

        for kib in 0..8usize {
            for _ in 0..4 {
                assert!(codec.decode(&mut src).expect("NeedMore").is_none());
            }
            assert!(
                src.capacity() <= 512 * 1024,
                "after {kib} KiB of a 16 MiB declared frame arrived, the decoder had pinned {} bytes",
                src.capacity()
            );
            src.extend_from_slice(&[0xcd; 1024]);
        }
    }

    /// The step-reserve must not break reassembly: a frame fed in chunks decodes byte-identically.
    #[test]
    fn a_chunked_large_frame_still_decodes_whole() {
        let payload = vec![0xabu8; 300 * 1024]; // several reserve steps
        let mut codec = FrameCodec::default();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(payload.len() as u32).encode());

        for chunk in payload.chunks(10 * 1024) {
            assert!(codec.decode(&mut src).expect("NeedMore").is_none());
            src.extend_from_slice(chunk);
        }
        let frame = codec
            .decode(&mut src)
            .expect("decode")
            .expect("the complete frame decodes");
        assert_eq!(frame.header.payload_len as usize, payload.len());
        assert_eq!(&frame.payload[..], &payload[..]);
        assert!(src.is_empty(), "no trailing bytes left behind");
    }

    // -------------------------------------------------------------------------------------------
    // M1-S9a Task 11: `ReadProgress`, the handle the session's tick arm reads to tell a STALLED
    // half-frame from a quiet peer. The properties below are what make the tick arm a stall
    // detector; `availability_it.rs` proves the end-to-end consequences over a real socket.
    // -------------------------------------------------------------------------------------------

    fn progress_codec() -> (FrameCodec, std::sync::Arc<ReadProgress>) {
        let p = std::sync::Arc::new(ReadProgress::default());
        (FrameCodec::with_progress(std::sync::Arc::clone(&p)), p)
    }

    /// A frame that arrives whole in one read was never outstanding, so it must leave the counters
    /// completely untouched — otherwise a busy, perfectly healthy session would look mid-frame to
    /// the tick arm forever.
    #[test]
    fn a_whole_frame_never_touches_the_progress_counters() {
        let (mut codec, progress) = progress_codec();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(4).encode());
        src.extend_from_slice(&[1, 2, 3, 4]);

        assert!(codec.decode(&mut src).expect("decode").is_some());
        let snap = progress.snapshot();
        assert_eq!((snap.started, snap.completed), (0, 0));
        assert!(!snap.is_partial());
    }

    /// The load-bearing one: while a frame trickles in, `buffered` tracks what has ARRIVED. That is
    /// what distinguishes "slow client" from "stalled client" — under the plan's original
    /// two-counter shape every one of these snapshots would have been identical, and a slow client
    /// would be indistinguishable from a dead one.
    #[test]
    fn buffered_advances_with_every_arriving_byte_of_a_partial_frame() {
        let (mut codec, progress) = progress_codec();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(64).encode());

        assert!(codec.decode(&mut src).expect("NeedMore").is_none());
        let mut prev = progress.snapshot();
        assert!(
            prev.is_partial(),
            "a header with no body is a partial frame"
        );
        assert_eq!(prev.started, 1);

        // 7 x 8 = 56 of the 64 declared body bytes: the frame stays partial throughout.
        for _ in 0..7 {
            src.extend_from_slice(&[0u8; 8]);
            assert!(codec.decode(&mut src).expect("NeedMore").is_none());
            let snap = progress.snapshot();
            assert_ne!(
                snap, prev,
                "the snapshot must change when bytes arrive, or a progressing client reads as \
                 stalled"
            );
            assert_eq!(
                snap.started, 1,
                "one partial FRAME, however many decode calls it took"
            );
            prev = snap;
        }
    }

    /// Completion is observable, and a fresh partial frame after it is distinguishable from the
    /// previous one even when it happens to hold the same number of bytes.
    #[test]
    fn completion_clears_partial_and_the_next_partial_frame_is_distinguishable() {
        let (mut codec, progress) = progress_codec();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(4).encode());
        assert!(codec.decode(&mut src).expect("NeedMore").is_none());
        let first = progress.snapshot();

        src.extend_from_slice(&[1, 2, 3, 4]);
        assert!(codec.decode(&mut src).expect("decode").is_some());
        let done = progress.snapshot();
        assert!(!done.is_partial(), "the frame completed");
        assert_eq!((done.started, done.completed), (1, 1));

        // A second frame goes partial with the SAME byte count as the first did: only `started`
        // separates the two snapshots.
        src.extend_from_slice(&header(4).encode());
        assert!(codec.decode(&mut src).expect("NeedMore").is_none());
        let second = progress.snapshot();
        assert!(second.is_partial());
        assert_eq!(second.buffered, first.buffered);
        assert_ne!(second, first);
    }

    /// A truncated HEADER counts as a partial frame. The plan's shape returned early before any
    /// bookkeeping, which left "send three bytes and go silent" invisible to the stall detector —
    /// the cheapest possible way to hold a session.
    #[test]
    fn a_truncated_header_is_a_partial_frame_too() {
        let (mut codec, progress) = progress_codec();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(0).encode()[..3]);

        assert!(codec.decode(&mut src).expect("NeedMore").is_none());
        let snap = progress.snapshot();
        assert!(
            snap.is_partial(),
            "3 of {HEADER_LEN} header bytes is partial"
        );
        assert_eq!(snap.buffered, 3);
    }

    /// The inverse, and the one this implementation got WRONG first: an EMPTY buffer is a quiet
    /// peer, not half a frame. `Framed` polls the decoder once more with an empty buffer after
    /// every frame it yields, so counting that as partial leaves every healthy session permanently
    /// "mid-frame" — which vetoes the idle reaper forever and arms the stall detector on a session
    /// that has nothing outstanding. Caught live, by the idle test failing to reap a quiet session.
    #[test]
    fn an_empty_buffer_is_a_quiet_peer_not_a_partial_frame() {
        let (mut codec, progress) = progress_codec();
        let mut src = bytes::BytesMut::new();

        // Cold: nothing has ever arrived.
        assert!(codec.decode(&mut src).expect("NeedMore").is_none());
        assert!(!progress.snapshot().is_partial());

        // And after a whole frame has been consumed, which is where `Framed` actually does this.
        src.extend_from_slice(&header(2).encode());
        src.extend_from_slice(&[7, 7]);
        assert!(codec.decode(&mut src).expect("decode").is_some());
        assert!(src.is_empty());
        assert!(codec.decode(&mut src).expect("NeedMore").is_none());
        assert!(
            !progress.snapshot().is_partial(),
            "a drained buffer after a complete frame is not a partial frame"
        );
    }
}
