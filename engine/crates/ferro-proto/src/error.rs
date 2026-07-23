use thiserror::Error;

/// Every decode failure is a protocol violation the caller maps to `NonRetryable{Protocol}`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("bad magic: expected 0x{expected:02X}, got 0x{got:02X}")]
    BadMagic { expected: u8, got: u8 },
    #[error("unsupported protocol version: expected {expected}, got {got}")]
    BadVersion { expected: u8, got: u8 },
    #[error("frame payload_len {len} exceeds MAX_FRAME_PAYLOAD {max}")]
    FrameTooLarge { len: u32, max: u32 },
    #[error("unknown flag bits set: 0x{bits:04X}")]
    UnknownFlags { bits: u16 },
    #[error("frame sets a reserved flag (OOB_FD/COMPRESSED) unsupported in M0")]
    UnsupportedFlag,
    #[error("buffer too short: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("malformed messagepack payload: {0}")]
    Malformed(String),
    #[error("trailing bytes after payload: {0} extra")]
    TrailingBytes(usize),
}
