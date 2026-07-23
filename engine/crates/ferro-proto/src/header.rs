use crate::CodecError;
use crate::consts::{MAGIC, MAX_FRAME_PAYLOAD, PROTOCOL_VERSION};

pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub flags: u16,
    pub service: u16,
    pub method: u16,
    pub request_id: u32,
    pub payload_len: u32,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0] = MAGIC;
        b[1] = PROTOCOL_VERSION;
        b[2..4].copy_from_slice(&self.flags.to_le_bytes());
        b[4..6].copy_from_slice(&self.service.to_le_bytes());
        b[6..8].copy_from_slice(&self.method.to_le_bytes());
        b[8..12].copy_from_slice(&self.request_id.to_le_bytes());
        b[12..16].copy_from_slice(&self.payload_len.to_le_bytes());
        b
    }

    /// Decode + validate the header ONLY. Rejects an oversize `payload_len` before any payload
    /// is read (the zero-allocation DoS guard). Does not validate flags (caller decides when).
    pub fn decode(buf: &[u8]) -> Result<Header, CodecError> {
        if buf.len() < HEADER_LEN {
            return Err(CodecError::Truncated {
                need: HEADER_LEN,
                have: buf.len(),
            });
        }
        if buf[0] != MAGIC {
            return Err(CodecError::BadMagic {
                expected: MAGIC,
                got: buf[0],
            });
        }
        if buf[1] != PROTOCOL_VERSION {
            return Err(CodecError::BadVersion {
                expected: PROTOCOL_VERSION,
                got: buf[1],
            });
        }
        let payload_len = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        if payload_len > MAX_FRAME_PAYLOAD {
            return Err(CodecError::FrameTooLarge {
                len: payload_len,
                max: MAX_FRAME_PAYLOAD,
            });
        }
        Ok(Header {
            flags: u16::from_le_bytes([buf[2], buf[3]]),
            service: u16::from_le_bytes([buf[4], buf[5]]),
            method: u16::from_le_bytes([buf[6], buf[7]]),
            request_id: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            payload_len,
        })
    }
}
