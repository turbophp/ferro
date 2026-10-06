//! COPY wire messages (M3-D4; SPEC §6.1, `/proto/PROTOCOL.md` §12).
//!
//! `CopyRequest` is the request body of SQL/`COPY_IN` and SQL/`COPY_OUT`. It is `Value`-free, so it
//! rides the `msg!`/rmp-serde positional layout the TX and ADMIN messages use.
//!
//! `CopyData` (STREAM/`COPY_DATA`, either direction) carries one chunk of RAW COPY bytes as a
//! MessagePack `bin`. It is hand-rolled rather than `msg!`, because rmp-serde writes a `Vec<u8>` as an
//! ARRAY of integers, not a `bin` — and because the engine must be able to take the chunk out of a
//! received frame WITHOUT copying it ([`CopyData::data_range`]). `CopyDone` (STREAM/`COPY_DONE`,
//! client → engine) is an empty fixarray: the frame IS the message.
//!
//! The bytes are opaque to Ferro: they are whatever PostgreSQL's COPY sub-protocol carries in the
//! format the statement names (text, CSV, binary). Formatting rows is the caller's job.

use super::{from_slice, to_vec};
use crate::CodecError;
use rmp::decode as dec;
use rmp::encode as enc;
use serde::{Deserialize, Serialize};

msg!(
    /// The body of SQL/`COPY_IN` and SQL/`COPY_OUT`: a positional fixarray of 5.
    ///
    /// `readonly` is the CLIENT's §19.3 declaration, never inferred (charter rule 6). It matters for
    /// `COPY_OUT` only — `COPY (DELETE … RETURNING *) TO STDOUT` writes — and a `COPY_IN` declared
    /// `readonly` is refused, because the method itself is the write.
    CopyRequest {
        pool: String,
        sql: String,
        readonly: bool,
        timeout_ms: Option<u32>,
        tx_id: Option<u64>
    }
);

/// One chunk of raw COPY bytes: `[data: bin]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyData {
    pub data: Vec<u8>,
}

/// The bytes `CopyData` adds around its chunk, at most: the fixarray(1) marker and a `bin32` header.
pub const COPY_DATA_OVERHEAD: usize = 1 + 5;

impl CopyData {
    /// Encode `[bin(data)]` — the canonical smallest `bin` width, as every codec here writes it.
    pub fn encode(&self) -> Vec<u8> {
        Self::encode_slice(&self.data)
    }

    /// [`CopyData::encode`] for a borrowed chunk, so the engine's producer never builds a `CopyData`.
    pub fn encode_slice(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len() + COPY_DATA_OVERHEAD);
        enc::write_array_len(&mut out, 1).expect("in-memory");
        enc::write_bin(&mut out, data).expect("in-memory");
        out
    }

    /// Where the chunk lies inside an encoded `CopyData` payload, so the receiver can slice it out
    /// of the frame it already holds instead of copying it. Strict: exactly one `bin`, no trailing
    /// byte, and a declared length that the payload actually carries.
    pub fn data_range(b: &[u8]) -> Result<std::ops::Range<usize>, CodecError> {
        let mut rd: &[u8] = b;
        let top = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("CopyData array: {e:?}")))?;
        if top != 1 {
            return Err(CodecError::Malformed(format!("CopyData len {top} != 1")));
        }
        let len = dec::read_bin_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("CopyData data: {e:?}")))?
            as usize;
        let start = b.len() - rd.len();
        if rd.len() < len {
            return Err(CodecError::Malformed(format!(
                "CopyData declares {len} bytes, carries {}",
                rd.len()
            )));
        }
        if rd.len() > len {
            return Err(CodecError::TrailingBytes(rd.len() - len));
        }
        Ok(start..start + len)
    }

    pub fn decode(b: &[u8]) -> Result<CopyData, CodecError> {
        let r = Self::data_range(b)?;
        Ok(CopyData {
            data: b[r].to_vec(),
        })
    }
}

msg!(
    /// STREAM/`COPY_DONE`'s body: an empty fixarray. Client → engine only.
    CopyDone {}
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_request_is_a_fixarray_of_five_and_roundtrips() {
        for (timeout_ms, tx_id) in [(Some(30_000), Some(7)), (None, None)] {
            let r = CopyRequest {
                pool: "main".into(),
                sql: "COPY t FROM STDIN".into(),
                readonly: false,
                timeout_ms,
                tx_id,
            };
            let b = r.encode();
            assert_eq!(b[0], 0x95);
            assert_eq!(CopyRequest::decode(&b).unwrap(), r);
        }
    }

    #[test]
    fn copy_data_is_a_bin_not_an_array_of_ints_and_roundtrips_at_every_width() {
        for n in [0usize, 1, 255, 256, 65_535, 65_536] {
            let data: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
            let b = CopyData { data: data.clone() }.encode();
            assert_eq!(b[0], 0x91);
            let marker = b[1];
            let want = if n < 256 {
                0xc4
            } else if n < 65_536 {
                0xc5
            } else {
                0xc6
            };
            assert_eq!(marker, want, "smallest bin width for {n}");
            assert_eq!(CopyData::decode(&b).unwrap().data, data);
            assert_eq!(&b[CopyData::data_range(&b).unwrap()], &data[..]);
        }
    }

    #[test]
    fn copy_data_refuses_a_short_payload_trailing_bytes_and_a_str() {
        let mut b = CopyData {
            data: b"abc".to_vec(),
        }
        .encode();
        b.push(0);
        assert!(matches!(
            CopyData::data_range(&b),
            Err(CodecError::TrailingBytes(1))
        ));
        let short = [0x91, 0xc4, 0x05, b'a'];
        assert!(CopyData::data_range(&short).is_err());
        // A `str` is not a `bin`: the chunk is bytes, never text.
        let s = [0x91, 0xa3, b'a', b'b', b'c'];
        assert!(CopyData::data_range(&s).is_err());
        assert!(CopyData::data_range(&[0x92, 0xc4, 0x00, 0xc0]).is_err());
    }

    #[test]
    fn copy_done_is_an_empty_fixarray() {
        assert_eq!(CopyDone {}.encode(), vec![0x90]);
        assert!(CopyDone::decode(&[0x91, 0xc0]).is_err());
    }
}
