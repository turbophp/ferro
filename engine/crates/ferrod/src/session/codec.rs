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
            src.reserve(need - src.len());
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
