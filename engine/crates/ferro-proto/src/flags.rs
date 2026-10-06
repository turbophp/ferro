use crate::CodecError;
use crate::consts::flags::{CANCEL, COMPRESSED, END, OOB_FD, STREAM};

/// All bits defined in M0 (known set). OOB_FD/COMPRESSED are known-but-reserved.
pub const KNOWN: u16 = STREAM | END | CANCEL | OOB_FD | COMPRESSED;
/// Reserved bits that are illegal to *set* in a frame the engine READS. Since M3-D3 the engine
/// itself sets `OOB_FD` on frames it SENDS to a `MEMFD_RX` client (`/proto/PROTOCOL.md` §1.1); a
/// client never may, so `validate` — the inbound check — still refuses it.
pub const RESERVED: u16 = OOB_FD | COMPRESSED;

#[inline]
pub fn has(bits: u16, mask: u16) -> bool {
    bits & mask != 0
}

/// Unknown bits -> Protocol; reserved-but-known bits actually set -> Unsupported.
pub fn validate(bits: u16) -> Result<(), CodecError> {
    if bits & !KNOWN != 0 {
        return Err(CodecError::UnknownFlags {
            bits: bits & !KNOWN,
        });
    }
    if bits & RESERVED != 0 {
        return Err(CodecError::UnsupportedFlag);
    }
    Ok(())
}
