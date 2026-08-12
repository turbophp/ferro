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

#[derive(Default)]
pub struct FrameCodec;

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
        if src.len() < HEADER_LEN {
            return Ok(None);
        }
        let header = Header::decode(&src[..HEADER_LEN])?; // CodecError -> FrameError via #[from]
        let need = HEADER_LEN + header.payload_len as usize;
        if src.len() < need {
            src.reserve((need - src.len()).min(PARTIAL_FRAME_RESERVE_STEP));
            return Ok(None);
        }
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
        let mut codec = FrameCodec;
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
        let mut codec = FrameCodec;
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
        let mut codec = FrameCodec;
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
}
