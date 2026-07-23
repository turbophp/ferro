use bytes::{Buf, Bytes, BytesMut};
use ferro_proto::consts::MAX_FRAME_PAYLOAD;
use ferro_proto::header::{HEADER_LEN, Header};
use tokio_util::codec::{Decoder, Encoder};

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
