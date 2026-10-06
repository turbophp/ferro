//! Content decoding (SPEC §23.9.2; slice M6-F4b).
//!
//! When a request carries `decode = true` and the response's `Content-Encoding` is exactly ONE of
//! `gzip`, `x-gzip` or `deflate` (zlib, with a raw-deflate fallback), the engine decodes the body
//! incrementally and removes `content-encoding` and `content-length` from the head, reporting both
//! in `HttpHead.decoded`. Every other encoding — `br`, `zstd`, `identity`, a list (`gzip, br`), or
//! two `Content-Encoding` fields — passes through with `decoded = nil`.
//!
//! **The bounds §23.9.2 names, and how each is held:**
//!
//! - **Memory is bounded by the window.** The decoder emits at most [`super::MAX_BODY_CHUNK`] bytes
//!   per step and holds no more than one step's output and the unconsumed tail of one input chunk.
//!   The engine sends each step as one `BODY` frame through the credit gauntlet BEFORE asking for
//!   the next, so a frame parked on credit stops decompression. That is why this uses the low-level
//!   [`flate2::Decompress`] with a bounded output buffer rather than `flate2::write::GzDecoder`,
//!   which writes ALL the output for one input chunk at once (a 256 KiB gzip chunk of zeros is about
//!   256 MiB out).
//! - **CPU is bounded by the deadline.** The engine checks its cancel token and total deadline
//!   between steps, so a bomb that is never short of credit still ends at the request's deadline.
//! - **A corrupt stream is `ResponseIncomplete` (`decode`)**: a bad gzip header, a CRC32 or length
//!   mismatch, an inflate error, a stream truncated at the end of the body, or bytes after the end
//!   of the compressed stream that do not begin another gzip member.
//!
//! An entirely EMPTY body is not an error under any encoding: a `HEAD` response, a 204 or a 304
//! routinely carries `Content-Encoding: gzip` with no body at all.

use bytes::Bytes;
use flate2::{Crc, Decompress, FlushDecompress, Status};

/// The longest gzip header the decoder buffers (the optional `FEXTRA`, `FNAME` and `FCOMMENT`
/// fields make its length unbounded on the wire). A header that has not ended by here is refused.
pub const MAX_GZIP_HEADER: usize = 64 * 1024;

/// Which decoding a response gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coding {
    Gzip,
    Deflate,
}

/// The response's coding, if the engine decodes it: exactly one `Content-Encoding` field whose
/// whole value is (ASCII case-insensitively) `gzip`, `x-gzip` or `deflate`.
pub fn coding_of(headers: &http::HeaderMap) -> Option<(Coding, String)> {
    let mut all = headers.get_all(http::header::CONTENT_ENCODING).iter();
    let value = all.next()?;
    if all.next().is_some() {
        return None; // stacked across fields: pass through
    }
    let s = value.to_str().ok()?.trim();
    let coding = if s.eq_ignore_ascii_case("gzip") || s.eq_ignore_ascii_case("x-gzip") {
        Coding::Gzip
    } else if s.eq_ignore_ascii_case("deflate") {
        Coding::Deflate
    } else {
        return None;
    };
    Some((coding, s.to_string()))
}

/// Why a body could not be decoded. Carries no data from the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Header,
    Inflate,
    Checksum,
    Truncated,
    TrailingData,
}

#[derive(Debug)]
enum State {
    /// Gzip: waiting for (another) member header. `first` is false after a complete member, where
    /// running out of input is a clean end.
    GzHeader { first: bool },
    /// Deflate: the first two bytes decide zlib or raw.
    Sniff,
    /// Inflating a member (gzip: raw deflate) or the whole stream (deflate).
    Inflate,
    /// Gzip: waiting for the 8-byte trailer (CRC32, ISIZE).
    GzTrailer,
    /// Deflate: the stream ended; nothing may follow.
    Done,
}

/// An incremental, bounded decoder. Feed it each body chunk with [`Decoder::feed`], then drain it
/// with [`Decoder::next_chunk`] until that returns `Ok(None)` (more input needed); at the end of the
/// body, [`Decoder::finish`].
pub struct Decoder {
    coding: Coding,
    state: State,
    inflate: Decompress,
    crc: Crc,
    /// Unconsumed input. Only a short tail (a partial header, trailer or sniff) is ever carried into
    /// the next `feed`; inflate consumes what it is given.
    pending: Bytes,
    pos: usize,
    seen_input: bool,
    /// The one output buffer, `max_chunk` long.
    buf: Vec<u8>,
}

impl Decoder {
    pub fn new(coding: Coding, max_chunk: usize) -> Self {
        Decoder {
            coding,
            state: match coding {
                Coding::Gzip => State::GzHeader { first: true },
                Coding::Deflate => State::Sniff,
            },
            inflate: Decompress::new(false),
            crc: Crc::new(),
            pending: Bytes::new(),
            pos: 0,
            seen_input: false,
            buf: vec![0u8; max_chunk.max(1)],
        }
    }

    /// Hand the decoder the next body chunk.
    pub fn feed(&mut self, input: Bytes) {
        if input.is_empty() {
            return;
        }
        self.seen_input = true;
        if self.pos >= self.pending.len() {
            self.pending = input;
        } else {
            // A short tail the previous step could not use yet: join it to the new input.
            let mut joined = Vec::with_capacity(self.pending.len() - self.pos + input.len());
            joined.extend_from_slice(&self.pending[self.pos..]);
            joined.extend_from_slice(&input);
            self.pending = Bytes::from(joined);
        }
        self.pos = 0;
    }

    fn rest(&self) -> &[u8] {
        &self.pending[self.pos..]
    }

    /// The next decoded chunk (non-empty, at most `max_chunk` bytes), or `Ok(None)` when the decoder
    /// needs more input.
    pub fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, DecodeError> {
        // A fixed-size buffer, allocated once: one step can never emit more than `max_chunk`,
        // whatever the ratio, and only the filled part is copied out.
        let mut out = std::mem::take(&mut self.buf);
        let mut filled = 0usize;
        let r = self.step(&mut out, &mut filled);
        let chunk = (filled > 0).then(|| out[..filled].to_vec());
        self.buf = out;
        r.map(|()| chunk)
    }

    fn step(&mut self, out: &mut [u8], filled: &mut usize) -> Result<(), DecodeError> {
        loop {
            match self.state {
                State::GzHeader { .. } => {
                    if self.rest().is_empty() {
                        return Ok(());
                    }
                    match parse_gzip_header(self.rest())? {
                        Some(len) => {
                            self.pos += len;
                            self.inflate = Decompress::new(false);
                            self.crc = Crc::new();
                            self.state = State::Inflate;
                        }
                        None if self.rest().len() >= MAX_GZIP_HEADER => {
                            return Err(DecodeError::Header);
                        }
                        None => return Ok(()),
                    }
                }
                State::Sniff => {
                    if self.rest().len() < 2 {
                        return Ok(());
                    }
                    // RFC 1950: CM = 8, CINFO <= 7, FCHECK makes the pair a multiple of 31, and no
                    // preset dictionary. Anything else is taken as raw deflate (the fallback servers
                    // that send RFC 1951 under `deflate` need).
                    let (cmf, flg) = (self.rest()[0], self.rest()[1]);
                    let zlib = cmf & 0x0f == 8
                        && cmf >> 4 <= 7
                        && ((u16::from(cmf) << 8) | u16::from(flg)) % 31 == 0
                        && flg & 0x20 == 0;
                    self.inflate = Decompress::new(zlib);
                    self.state = State::Inflate;
                }
                State::Inflate => {
                    if *filled == out.len() {
                        return Ok(());
                    }
                    let in_before = self.inflate.total_in();
                    let out_before = self.inflate.total_out();
                    let status = self
                        .inflate
                        .decompress(
                            &self.pending[self.pos..],
                            &mut out[*filled..],
                            FlushDecompress::None,
                        )
                        .map_err(|_| DecodeError::Inflate)?;
                    let consumed = usize::try_from(self.inflate.total_in() - in_before)
                        .map_err(|_| DecodeError::Inflate)?;
                    let produced = usize::try_from(self.inflate.total_out() - out_before)
                        .map_err(|_| DecodeError::Inflate)?;
                    self.pos += consumed;
                    if self.coding == Coding::Gzip {
                        self.crc.update(&out[*filled..*filled + produced]);
                    }
                    *filled += produced;
                    match status {
                        Status::StreamEnd => {
                            self.state = match self.coding {
                                Coding::Gzip => State::GzTrailer,
                                Coding::Deflate => State::Done,
                            };
                        }
                        Status::Ok | Status::BufError => {
                            if produced == 0 && consumed == 0 {
                                // No progress: inflate wants input (room is handled above).
                                return Ok(());
                            }
                        }
                    }
                }
                State::GzTrailer => {
                    if self.rest().len() < 8 {
                        return Ok(());
                    }
                    let t = &self.rest()[..8];
                    let crc = u32::from_le_bytes([t[0], t[1], t[2], t[3]]);
                    let isize = u32::from_le_bytes([t[4], t[5], t[6], t[7]]);
                    if crc != self.crc.sum() || isize != self.crc.amount() {
                        return Err(DecodeError::Checksum);
                    }
                    self.pos += 8;
                    // RFC 1952 §2.2: a gzip file is a series of members.
                    self.state = State::GzHeader { first: false };
                }
                State::Done => {
                    if !self.rest().is_empty() {
                        return Err(DecodeError::TrailingData);
                    }
                    return Ok(());
                }
            }
        }
    }

    /// The body ended. `Ok` only at a clean end: no input at all, or a complete stream (gzip: at a
    /// member boundary) with nothing left over.
    pub fn finish(&self) -> Result<(), DecodeError> {
        if !self.seen_input {
            return Ok(());
        }
        match self.state {
            State::GzHeader { first: false } if self.rest().is_empty() => Ok(()),
            State::Done if self.rest().is_empty() => Ok(()),
            State::GzHeader { first: false } | State::Done => Err(DecodeError::TrailingData),
            _ => Err(DecodeError::Truncated),
        }
    }
}

/// RFC 1952 §2.3 member header. `Ok(Some(len))`: complete, `len` bytes long. `Ok(None)`: incomplete.
fn parse_gzip_header(b: &[u8]) -> Result<Option<usize>, DecodeError> {
    const FHCRC: u8 = 0x02;
    const FEXTRA: u8 = 0x04;
    const FNAME: u8 = 0x08;
    const FCOMMENT: u8 = 0x10;
    const RESERVED: u8 = 0xe0;
    // Check the magic as soon as it is visible, so a non-gzip body fails at its first bytes.
    for (i, want) in [0x1f, 0x8b, 8].into_iter().enumerate() {
        match b.get(i) {
            None => return Ok(None),
            Some(&v) if v != want => return Err(DecodeError::Header),
            Some(_) => {}
        }
    }
    let Some(&flg) = b.get(3) else {
        return Ok(None);
    };
    if flg & RESERVED != 0 {
        return Err(DecodeError::Header);
    }
    let mut i = 10; // ID1 ID2 CM FLG MTIME(4) XFL OS
    if b.len() < i {
        return Ok(None);
    }
    if flg & FEXTRA != 0 {
        let Some(x) = b.get(i..i + 2) else {
            return Ok(None);
        };
        i += 2 + usize::from(u16::from_le_bytes([x[0], x[1]]));
        if b.len() < i {
            return Ok(None);
        }
    }
    for flag in [FNAME, FCOMMENT] {
        if flg & flag != 0 {
            match b[i..].iter().position(|&c| c == 0) {
                Some(z) => i += z + 1,
                None => return Ok(None),
            }
        }
    }
    if flg & FHCRC != 0 {
        if b.len() < i + 2 {
            return Ok(None);
        }
        let mut crc = Crc::new();
        crc.update(&b[..i]);
        if (crc.sum() & 0xffff) as u16 != u16::from_le_bytes([b[i], b[i + 1]]) {
            return Err(DecodeError::Header);
        }
        i += 2;
    }
    Ok(Some(i))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }
    fn zlib(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }
    fn raw(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::DeflateEncoder::new(Vec::new(), Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    /// Decode `wire` delivered in `piece`-byte chunks; returns the output and every chunk's size.
    fn run(
        coding: Coding,
        wire: &[u8],
        piece: usize,
        max: usize,
    ) -> Result<(Vec<u8>, usize), DecodeError> {
        let mut d = Decoder::new(coding, max);
        let mut out = Vec::new();
        let mut biggest = 0;
        for c in wire.chunks(piece.max(1)) {
            d.feed(Bytes::copy_from_slice(c));
            while let Some(chunk) = d.next_chunk()? {
                assert!(!chunk.is_empty() && chunk.len() <= max);
                biggest = biggest.max(chunk.len());
                out.extend_from_slice(&chunk);
            }
        }
        d.finish()?;
        Ok((out, biggest))
    }

    fn sample() -> Vec<u8> {
        (0..200_000u32)
            .flat_map(|i| (i % 251).to_le_bytes())
            .collect()
    }

    #[test]
    fn gzip_zlib_and_raw_deflate_round_trip_at_every_chunking() {
        let data = sample();
        for piece in [1, 2, 7, 4096, 1 << 20] {
            for (coding, wire) in [
                (Coding::Gzip, gzip(&data)),
                (Coding::Deflate, zlib(&data)),
                (Coding::Deflate, raw(&data)),
            ] {
                if piece == 1 && wire.len() > 300_000 {
                    continue;
                }
                let (out, _) = run(coding, &wire, piece, 64 * 1024).unwrap();
                assert!(out == data, "{coding:?} piece={piece}");
            }
        }
    }

    /// The memory bound: one step never emits more than `max_chunk`, however compressible the input
    /// (a 64 MiB run of zeros is ~64 KiB of gzip — fed in ONE chunk here).
    #[test]
    fn a_bomb_is_emitted_in_bounded_steps() {
        let zeros = vec![0u8; 64 * 1024 * 1024];
        let wire = gzip(&zeros);
        assert!(wire.len() < 128 * 1024);
        let mut d = Decoder::new(Coding::Gzip, 256 * 1024);
        d.feed(Bytes::from(wire));
        let mut total = 0usize;
        let mut steps = 0usize;
        while let Some(c) = d.next_chunk().unwrap() {
            assert!(c.len() <= 256 * 1024);
            total += c.len();
            steps += 1;
        }
        d.finish().unwrap();
        assert_eq!(total, zeros.len());
        assert!(steps >= 256, "{steps} steps");
    }

    #[test]
    fn multi_member_gzip_and_header_fields_are_accepted() {
        let mut wire = gzip(b"hello ");
        wire.extend(gzip(b"world"));
        assert_eq!(run(Coding::Gzip, &wire, 3, 16).unwrap().0, b"hello world");
        // FEXTRA + FNAME + FCOMMENT + FHCRC.
        let mut e = flate2::GzBuilder::new()
            .extra(vec![1, 2, 3])
            .filename("f.txt")
            .comment("c")
            .operating_system(3)
            .write(Vec::new(), Compression::fast());
        e.write_all(b"fields").unwrap();
        let wire = e.finish().unwrap();
        assert_eq!(run(Coding::Gzip, &wire, 1, 16).unwrap().0, b"fields");
    }

    #[test]
    fn corrupt_truncated_and_trailing_streams_are_refused() {
        let data = sample();
        let good = gzip(&data);
        // Truncated anywhere after the header.
        for cut in [5, 12, good.len() / 2, good.len() - 4, good.len() - 1] {
            let e = run(Coding::Gzip, &good[..cut], 4096, 65536).unwrap_err();
            assert!(matches!(e, DecodeError::Truncated), "cut {cut}: {e:?}");
        }
        // A flipped CRC byte.
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 6] ^= 0xff;
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Checksum
        );
        // A wrong ISIZE.
        let mut bad = good.clone();
        bad[n - 1] ^= 0x01;
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Checksum
        );
        // Not gzip at all; reserved flag bits.
        assert_eq!(
            run(Coding::Gzip, b"plain text", 4, 16).unwrap_err(),
            DecodeError::Header
        );
        let mut bad = good.clone();
        bad[3] |= 0x80;
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Header
        );
        // Garbage after a complete member / after a complete deflate stream.
        let mut bad = good.clone();
        bad.extend_from_slice(b"junk");
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Header
        );
        let mut bad = zlib(&data);
        bad.extend_from_slice(b"junk");
        assert_eq!(
            run(Coding::Deflate, &bad, 4096, 65536).unwrap_err(),
            DecodeError::TrailingData
        );
        // Inflate garbage.
        let mut bad = zlib(&data);
        for b in &mut bad[2..40] {
            *b = 0xff;
        }
        assert!(run(Coding::Deflate, &bad, 4096, 65536).is_err());
        // An endless header (FNAME never terminated) stops at the cap.
        let mut hdr = vec![0x1f, 0x8b, 8, 0x08, 0, 0, 0, 0, 0, 3];
        hdr.extend(std::iter::repeat_n(b'a', MAX_GZIP_HEADER));
        assert_eq!(
            run(Coding::Gzip, &hdr, 4096, 65536).unwrap_err(),
            DecodeError::Header
        );
    }

    #[test]
    fn an_empty_body_is_clean_under_every_coding() {
        for c in [Coding::Gzip, Coding::Deflate] {
            assert_eq!(run(c, b"", 1, 16).unwrap().0, b"");
        }
    }

    #[test]
    fn only_a_single_known_coding_is_decoded() {
        let h = |vals: &[&str]| {
            let mut m = http::HeaderMap::new();
            for v in vals {
                m.append(http::header::CONTENT_ENCODING, v.parse().unwrap());
            }
            coding_of(&m)
        };
        assert_eq!(h(&["gzip"]), Some((Coding::Gzip, "gzip".into())));
        assert_eq!(h(&["X-Gzip"]), Some((Coding::Gzip, "X-Gzip".into())));
        assert_eq!(h(&["Deflate"]), Some((Coding::Deflate, "Deflate".into())));
        for none in [
            &[][..],
            &["br"],
            &["zstd"],
            &["identity"],
            &["gzip, br"],
            &["gzip", "gzip"],
        ] {
            assert_eq!(h(none), None, "{none:?}");
        }
    }
}
